// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M17/P1 S5 — the transaction control operations drain the binding's send accumulator before the
/// core sees them (D3), and commit and abort on the async surface wait for the completion barrier
/// (D4).
/// </summary>
/// <remarks>
/// <para>
/// <b>Records stay buffered until something drains them.</b> Most tests build the producer through
/// the <c>NativeProducer.CreateMock(bool, SendAccumulatorSettings)</c> seam with a 60 s batch window
/// and a slot threshold above their record count, so the batch thread takes nothing on its own
/// within a test. <c>AdmittedRecordCount</c> reads the accumulator's chain without forcing a drain,
/// and is the "still buffered" witness before an operation. The "drained" witness after it is
/// <c>DrainPendingSends(TimeSpan.Zero)</c>, which is <see langword="true"/> only when the
/// accumulator is empty and idle at that instant (when the accumulator is not idle it forces a drain
/// as a side effect, so it is read only after the operation).
/// </para>
/// <para>
/// <b>The operations are driven through <c>NativeProducer</c>.</b> The public members forward to
/// these at CP5. The mock's transaction semantics are the core's: a send inside a transaction is
/// staged and reaches <c>HistoryCount()</c> only on commit, a send outside one reaches it at once,
/// and commit and abort flush first.
/// </para>
/// <para>
/// <b>Mutations.</b> The runs are recorded in
/// <c>design/history/M17/P1-producer-transactions/gate/CP4.txt</c>.
/// </para>
/// </remarks>
public sealed class ProducerTransactionDrainTests
{
    private const string Topic = "txn-drain-topic";

    private const string GroupId = "txn-drain-group";

    private const string ExpiryPrefix =
        "The producer's send accumulator did not drain within 0 seconds, so records buffered in the " +
        "binding have not reached the core and ";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Longer than s_deadline, so a mutation that waits behind a held callback fails at the deadline
    // with the hold still in force.
    private static readonly TimeSpan s_hold = TimeSpan.FromSeconds(60);

    // The failure-path bound: a failed commit or abort must complete well inside it while the pump
    // is parked for s_hold.
    private static readonly TimeSpan s_failureBound = TimeSpan.FromSeconds(10);

    // The sanctioned barrier hold window (PLAN §5.1 item 3): it only gives the "no barrier" mutation
    // room to show; the pass condition is the probe.
    private static readonly TimeSpan s_barrierHold = TimeSpan.FromMilliseconds(500);

    private static readonly SendAccumulatorSettings s_holding =
        new SendAccumulatorSettings(slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100);

    private static readonly SendAccumulatorSettings s_prompt =
        new SendAccumulatorSettings(slotThreshold: 1000, batchWindowMs: 10, batchChunk: 1100);

    /// <summary>
    /// The settings seams reach the accumulator: <c>NativeProducer.CreateMock(bool,
    /// SendAccumulatorSettings)</c> and the internal <c>AsyncMockProducer</c> constructor overload
    /// both build it with the settings given, not the environment's.
    /// </summary>
    [Fact]
    public async Task SettingsSeams_BuildTheAccumulatorWithTheSettingsGiven()
    {
        NativeProducer native = NativeProducer.CreateMock(autoComplete: true, s_holding);
        AsyncMockProducer<byte[], byte[]> mock = new AsyncMockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: true, s_holding);
        try
        {
            Task<RecordMetadata> nativeSend = Send(native, 0x01);
            Task<RecordMetadata> mockSend = mock.Send(new ProducerRecord<byte[], byte[]>(Topic, new byte[] { 0x02 }));

            Assert.Equal(60_000, native.StartedAccumulator!.Settings.BatchWindowMs);
            Assert.Equal(1000, native.StartedAccumulator.Settings.SlotThreshold);

            NativeProducer mockNative = (NativeProducer)typeof(AsyncMockProducer<byte[], byte[]>)
                .GetField("_native", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(mock)!;
            Assert.Equal(60_000, mockNative.StartedAccumulator!.Settings.BatchWindowMs);

            await native.FlushWithCallback();
            await mock.Flush();
            await TestTimeout.Run(() => nativeSend, s_deadline);
            await TestTimeout.Run(() => mockSend, s_deadline);
        }
        finally
        {
            native.Dispose();
            mock.Dispose();
        }
    }

    /// <summary>
    /// S5 (i): commit includes the sends that returned before it. Three sends are still in the
    /// accumulator when <c>CommitTransaction</c> is called; as soon as it completes the core's
    /// history holds all three.
    /// </summary>
    /// <remarks>
    /// Mutation: drop the async drain (the four <see cref="Task"/> operations share it) — HistoryCount
    /// reads 0.
    /// </remarks>
    [Fact]
    public async Task CommitTransaction_IncludesTheSendsThatReturnedBeforeIt()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_holding);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            producer.BeginTransaction();
            Task<RecordMetadata>[] sends = { Send(producer, 0x11), Send(producer, 0x12), Send(producer, 0x13) };
            Assert.Equal(3, producer.StartedAccumulator!.AdmittedRecordCount);

            await TestTimeout.Run(() => producer.CommitTransactionWithCallback(), s_deadline);

            Assert.Equal(3, producer.MockHistoryCount());
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5 (ii): abort discards the sends that returned before it. After the abort and a flush the
    /// history is empty, and the three sends complete, because the mock's abort flushes first (D7).
    /// </summary>
    /// <remarks>
    /// Mutation: drop the async drain — the records reach the core after the abort, outside any
    /// transaction, and HistoryCount reads 3.
    /// </remarks>
    [Fact]
    public async Task AbortTransaction_DiscardsTheSendsThatReturnedBeforeIt()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_holding);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            producer.BeginTransaction();
            Task<RecordMetadata>[] sends = { Send(producer, 0x21), Send(producer, 0x22), Send(producer, 0x23) };
            Assert.Equal(3, producer.StartedAccumulator!.AdmittedRecordCount);

            await TestTimeout.Run(() => producer.AbortTransactionWithCallback(), s_deadline);
            await TestTimeout.Run(() => producer.FlushWithCallback(), s_deadline);

            Assert.Equal(0, producer.MockHistoryCount());
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5 (iii): a send issued before <c>BeginTransaction</c> is not swept into the transaction that
    /// begin opens. It reaches the core outside any transaction, so it is in the history although the
    /// transaction is then aborted.
    /// </summary>
    /// <remarks>
    /// Mutation: drop the drain in <c>BeginTransaction</c> — the record reaches the core after begin,
    /// is staged, and is discarded by the abort, so HistoryCount reads 0.
    /// </remarks>
    [Fact]
    public async Task BeginTransaction_DoesNotSweepAnEarlierSendIntoTheTransaction()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_holding);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            Task<RecordMetadata> send = Send(producer, 0x31);
            Assert.Equal(1, producer.StartedAccumulator!.AdmittedRecordCount);

            producer.BeginTransaction();
            await TestTimeout.Run(() => producer.AbortTransactionWithCallback(), s_deadline);

            Assert.Equal(1, producer.MockHistoryCount());
            await TestTimeout.Run(() => send, s_deadline);
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5: each of the five operations, as the async producer types reach them, returns with the
    /// accumulator empty and idle — a send made just before it is never still buffered after it.
    /// </summary>
    /// <remarks>
    /// Mutations: drop the async drain — the first assertion, <c>initTransactions</c>'s, fails; drop
    /// <c>BeginTransaction</c>'s drain — its assertion fails. The other blocking forms are covered by
    /// the expiry test below.
    /// </remarks>
    [Fact]
    public async Task EachControlOperation_ReturnsWithTheAccumulatorEmptyAndIdle()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_holding);
        try
        {
            List<Task<RecordMetadata>> sends = new List<Task<RecordMetadata>>();

            sends.Add(SendBuffered(producer, 0x41));
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            AssertDrained(producer, "initTransactions");

            sends.Add(SendBuffered(producer, 0x42));
            producer.BeginTransaction();
            AssertDrained(producer, "beginTransaction");

            sends.Add(SendBuffered(producer, 0x43));
            await TestTimeout.Run(
                () => producer.SendOffsetsToTransactionWithCallback(Offsets(7), GroupMetadata()), s_deadline);
            AssertDrained(producer, "sendOffsetsToTransaction");

            sends.Add(SendBuffered(producer, 0x44));
            await TestTimeout.Run(() => producer.CommitTransactionWithCallback(), s_deadline);
            AssertDrained(producer, "commitTransaction");

            producer.BeginTransaction();
            sends.Add(SendBuffered(producer, 0x45));
            await TestTimeout.Run(() => producer.AbortTransactionWithCallback(), s_deadline);
            AssertDrained(producer, "abortTransaction");

            // 0x41 and 0x42 went outside a transaction, 0x43 and 0x44 were committed, 0x45 aborted.
            Assert.Equal(4, producer.MockHistoryCount());
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5: a producer driven only through the sync surface never starts an accumulator — through a
    /// sync send and a whole transaction of the five blocking operations.
    /// </summary>
    [Fact]
    public void SyncSurface_NeverStartsAnAccumulator_AcrossAWholeTransaction()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        try
        {
            producer.InitTransactions();
            producer.BeginTransaction();
            _ = producer.Send(new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x51 }), null);
            producer.SendOffsetsToTransaction(Offsets(9), GroupMetadata());
            producer.CommitTransaction();
            producer.BeginTransaction();
            producer.AbortTransaction();

            Assert.Null(producer.StartedAccumulator);
            Assert.Equal(1, producer.MockHistoryCount());
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5: the bounded drain of each blocking operation expires deterministically, with D3's message,
    /// <c>Code</c> 0, <c>IsRetriable</c> false, and no native call. <c>beginTransaction()</c> is the
    /// row the async producer types reach; the others are the same helper through the sync forms.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The hold is <c>SendAccumulatorTests</c>' flush-expiry hold: the core producer is closed, so
    /// <c>send_batch</c> rejects the record and the batch thread fires its delivery callback, which
    /// parks it with the accumulator still draining. A zero bound then expires at once. Because the
    /// core is closed, a native call would have failed with the core's closed-producer error
    /// instead, so the message itself shows that no native call was made.
    /// </para>
    /// <para>
    /// Mutation: drop the drain in one blocking form — its row fails with the core's closed-producer
    /// error.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("initTransactions()")]
    [InlineData("beginTransaction()")]
    [InlineData("sendOffsetsToTransaction()")]
    [InlineData("commitTransaction()")]
    [InlineData("abortTransaction()")]
    public async Task BlockingControlOperation_WhenTheDrainExpires_ThrowsWithoutANativeCall(string javaMethod)
    {
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        Task<RecordMetadata>? send = null;
        try
        {
            NativeMethods.ProducerClose(producer.Handle.DangerousGetHandle(), out IntPtr closeError);
            _ = KafkaException.FromHandle(closeError);

            send = producer.SendViaPump(
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x61 }),
                new DeliveryRegistration(new GatedDeliveryCallback(entered, release), Topic, 0));
            Assert.True(entered.Wait(s_deadline), "the batch thread never entered the delivery callback");

            Action operation = javaMethod switch
            {
                "initTransactions()" => () => producer.InitTransactionsWithAccumulatorDrainBound(TimeSpan.Zero),
                "beginTransaction()" => () => producer.BeginTransactionWithAccumulatorDrainBound(TimeSpan.Zero),
                "sendOffsetsToTransaction()" => () => producer.SendOffsetsToTransactionWithAccumulatorDrainBound(
                    Offsets(1), GroupMetadata(), TimeSpan.Zero),
                "commitTransaction()" => () => producer.CommitTransactionWithAccumulatorDrainBound(TimeSpan.Zero),
                _ => () => producer.AbortTransactionWithAccumulatorDrainBound(TimeSpan.Zero),
            };

            KafkaException failure = Assert.Throws<KafkaException>(operation);
            Assert.Equal(ExpiryPrefix + javaMethod + " was not attempted.", failure.Message);
            Assert.Equal(0, failure.Code);
            Assert.False(failure.IsRetriable);
        }
        finally
        {
            release.Set();
        }

        Assert.NotNull(send);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send!, s_deadline));
        producer.Dispose();
    }

    /// <summary>
    /// S5, D4 on commit: the commit's <see cref="Task"/> completes only after every delivery callback
    /// of the sends ahead of it has returned. The pump is parked in the first callback; the native
    /// commit is observed done; the <see cref="Task"/> stays pending through the hold window; once the
    /// callbacks are released, a probe in its continuation sees all of them returned.
    /// </summary>
    /// <remarks>
    /// Mutations: drop the barrier await, or the barrier enqueue — the <see cref="Task"/> completes
    /// inside the hold window.
    /// </remarks>
    [Fact]
    public async Task CommitTransaction_OnSuccess_CompletesOnlyAfterTheEarlierDeliveryCallbacksReturned()
    {
        const int Sends = 3;
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        GatedDeliveryCallback callback = new GatedDeliveryCallback(entered, release);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            producer.BeginTransaction();
            Task<RecordMetadata>[] sends = SendGated(producer, callback, Sends);
            Assert.True(entered.Wait(s_deadline), "the pump never entered the first delivery callback");

            Task commit = producer.CommitTransactionWithCallback();
            await PollUntil(() => producer.MockHistoryCount() == Sends, s_deadline, "the native commit did not complete");

            Task<(int Returned, bool SendsDone)> probe = Probe(commit, callback, sends);
            Assert.NotSame(commit, await Task.WhenAny(commit, Task.Delay(s_barrierHold)));
            Assert.False(commit.IsCompleted, "the commit completed while a delivery callback ahead of it was held");

            release.Set();
            await TestTimeout.Run(() => commit, s_deadline);
            (int returned, bool sendsDone) = await TestTimeout.Run(() => probe, s_deadline);
            Assert.Equal(Sends, returned);
            Assert.True(sendsDone, "a send ahead of the commit was not complete when the commit completed");
        }
        finally
        {
            release.Set();
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5, D4 on commit's failure path: with the commit-transaction error installed and the pump
    /// still parked, the commit faults at once with the core's error — the barrier is not awaited on
    /// failure.
    /// </summary>
    /// <remarks>Mutation: await the barrier on the failure path too — the fault waits for the hold and the bound expires.</remarks>
    [Fact]
    public async Task CommitTransaction_OnFailure_FaultsWithoutWaitingForTheBarrier()
    {
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        GatedDeliveryCallback callback = new GatedDeliveryCallback(entered, release);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            producer.BeginTransaction();
            producer.MockSetCommitTransactionError(120, "commit failed abortably");
            _ = SendGated(producer, callback, 2);
            Assert.True(entered.Wait(s_deadline), "the pump never entered the first delivery callback");

            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => producer.CommitTransactionWithCallback(), s_failureBound));
            Assert.Equal(120, failure.Code);
            Assert.Equal("commit failed abortably", failure.Message);
            Assert.False(release.IsSet);
        }
        finally
        {
            release.Set();
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5, D4 on abort: the abort's <see cref="Task"/> completes only after every delivery callback of
    /// the sends ahead of it has returned (MPT 364's shape). The mock is manual, so the sends are
    /// resolved only by the abort's own flush: the first callback being entered shows the native abort
    /// ran, and the <see cref="Task"/> stays pending while that callback is held.
    /// </summary>
    /// <remarks>
    /// Mutations: drop the barrier await, or the barrier enqueue — the <see cref="Task"/> completes
    /// inside the hold window.
    /// </remarks>
    [Fact]
    public async Task AbortTransaction_OnSuccess_CompletesOnlyAfterTheEarlierDeliveryCallbacksReturned()
    {
        const int Sends = 3;
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        GatedDeliveryCallback callback = new GatedDeliveryCallback(entered, release);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: false, s_prompt);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            producer.BeginTransaction();
            Task<RecordMetadata>[] sends = SendGated(producer, callback, Sends);

            Task abort = producer.AbortTransactionWithCallback();
            Assert.True(entered.Wait(s_deadline), "the abort's flush did not resolve the sends");

            Task<(int Returned, bool SendsDone)> probe = Probe(abort, callback, sends);
            Assert.NotSame(abort, await Task.WhenAny(abort, Task.Delay(s_barrierHold)));
            Assert.False(abort.IsCompleted, "the abort completed while a delivery callback ahead of it was held");

            release.Set();
            await TestTimeout.Run(() => abort, s_deadline);
            (int returned, bool sendsDone) = await TestTimeout.Run(() => probe, s_deadline);
            Assert.Equal(Sends, returned);
            Assert.True(sendsDone, "a send ahead of the abort was not complete when the abort completed");
            Assert.Equal(0, producer.MockHistoryCount());
        }
        finally
        {
            release.Set();
            producer.Dispose();
        }
    }

    /// <summary>
    /// S5, D4 on abort's failure path: with the pump parked, an abort the core rejects — here there
    /// is no open transaction — faults at once with the core's error, without awaiting the barrier.
    /// </summary>
    /// <remarks>Mutation: await the barrier on the failure path too — the fault waits for the hold and the bound expires.</remarks>
    [Fact]
    public async Task AbortTransaction_OnFailure_FaultsWithoutWaitingForTheBarrier()
    {
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        GatedDeliveryCallback callback = new GatedDeliveryCallback(entered, release);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        try
        {
            await TestTimeout.Run(() => producer.InitTransactionsWithCallback(), s_deadline);
            _ = SendGated(producer, callback, 2);
            Assert.True(entered.Wait(s_deadline), "the pump never entered the first delivery callback");

            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => producer.AbortTransactionWithCallback(), s_failureBound));
            Assert.Equal(-4, failure.Code);
            Assert.Equal("There is no open transaction.", failure.Message);
            Assert.False(release.IsSet);
        }
        finally
        {
            release.Set();
            producer.Dispose();
        }
    }

    private static Task<RecordMetadata> Send(NativeProducer producer, byte tag) =>
        producer.SendViaPump(new SerializedProducerRecord(Topic, 0, null, null, new byte[] { tag }), delivery: null);

    // A send that the accumulator still holds when this returns (the 60 s window), so the operation
    // that follows is what hands it to the core.
    private static Task<RecordMetadata> SendBuffered(NativeProducer producer, byte tag)
    {
        Task<RecordMetadata> send = Send(producer, tag);
        Assert.Equal(1, producer.StartedAccumulator!.AdmittedRecordCount);
        return send;
    }

    private static Task<RecordMetadata>[] SendGated(NativeProducer producer, GatedDeliveryCallback callback, int count)
    {
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[count];
        for (int i = 0; i < count; i++)
        {
            sends[i] = producer.SendViaPump(
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { (byte)(0x70 + i) }),
                new DeliveryRegistration(callback, Topic, 0));
        }

        return sends;
    }

    private static void AssertDrained(NativeProducer producer, string operation) =>
        Assert.True(
            producer.DrainPendingSends(TimeSpan.Zero),
            $"{operation} returned with a send still in the accumulator");

    // Reads, at the moment `operation` completes, how many callbacks have returned and whether every
    // send's Task is complete.
    private static Task<(int Returned, bool SendsDone)> Probe(
        Task operation, GatedDeliveryCallback callback, Task<RecordMetadata>[] sends) =>
        operation.ContinueWith(
            _ =>
            {
                bool sendsDone = true;
                foreach (Task<RecordMetadata> send in sends)
                {
                    sendsDone &= send.Status == TaskStatus.RanToCompletion;
                }

                return (callback.Returned, sendsDone);
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(offset, "drain-metadata"),
        };

    private static ConsumerGroupMetadata GroupMetadata() =>
        ConsumerGroupMetadata.FromCopiedValues(GroupId, -1, string.Empty, null);

    private static async Task PollUntil(Func<bool> condition, TimeSpan timeout, string because)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!condition() && elapsed.Elapsed < timeout)
        {
            await Task.Delay(2);
        }

        Assert.True(condition(), because);
    }

    /// <summary>
    /// A delivery callback that parks the thread that fires it until the test releases it, then
    /// counts itself returned. Every send of a test shares one instance, so the first fire parks the
    /// thread and the later ones pass straight through once released. The wait is bounded, and the
    /// body runs inside <c>DeliveryRegistration.Fire</c>'s no-throw boundary.
    /// </summary>
    private sealed class GatedDeliveryCallback : IDeliveryCallback
    {
        private readonly ManualResetEventSlim _entered;
        private readonly ManualResetEventSlim _release;
        private int _returned;

        internal GatedDeliveryCallback(ManualResetEventSlim entered, ManualResetEventSlim release)
        {
            _entered = entered;
            _release = release;
        }

        internal int Returned => Volatile.Read(ref _returned);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            _entered.Set();
            _ = _release.Wait(s_hold);
            Interlocked.Increment(ref _returned);
        }
    }
}
