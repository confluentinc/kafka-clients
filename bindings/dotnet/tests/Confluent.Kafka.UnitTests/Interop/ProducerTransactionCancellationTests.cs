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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M17/P1 S6 — the four <see cref="Task"/> control operations' cancellation table (D5) and the
/// lifetime of what they hand native (D12), through the <c>NativeSubmit</c> seam.
/// </summary>
/// <remarks>
/// <para>
/// <b>The seam.</b> Each async operation has an internal overload taking its native submit. The
/// fake here counts calls, and either forwards to the real P/Invoke or captures the callback and
/// its user data without firing them, so a test decides when (and with what) the "native"
/// operation completes.
/// </para>
/// <para>
/// <b>Runs in the serial collection.</b> Row 3 checks that the completion context becomes
/// collectable, which needs a full collection that other tests' allocations cannot disturb.
/// </para>
/// <para>
/// <b>Mutations.</b> The runs are recorded in
/// <c>design/history/M17/P1-producer-transactions/gate/CP4.txt</c>.
/// </para>
/// </remarks>
[Collection(SerialMemoryMeasurementCollection.Name)]
public sealed class ProducerTransactionCancellationTests
{
    private const string Topic = "txn-cancel-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_hold = TimeSpan.FromSeconds(60);

    private static readonly SendAccumulatorSettings s_prompt =
        new SendAccumulatorSettings(slotThreshold: 1000, batchWindowMs: 10, batchChunk: 1100);

    /// <summary>The four <see cref="Task"/> control operations, by their Java names.</summary>
    public static IEnumerable<object[]> Operations() => new[]
    {
        new object[] { "initTransactions" },
        new object[] { "sendOffsetsToTransaction" },
        new object[] { "commitTransaction" },
        new object[] { "abortTransaction" },
    };

    /// <summary>The four operations, with and without a started accumulator.</summary>
    public static IEnumerable<object[]> OperationsWithAndWithoutAccumulator()
    {
        foreach (object[] operation in Operations())
        {
            yield return new object[] { operation[0], false };
            yield return new object[] { operation[0], true };
        }
    }

    /// <summary>The four operations, each fired late with success and with a live error handle.</summary>
    public static IEnumerable<object[]> OperationsWithLateOutcome()
    {
        foreach (object[] operation in Operations())
        {
            yield return new object[] { operation[0], false };
            yield return new object[] { operation[0], true };
        }
    }

    /// <summary>
    /// D5 row 1: an already-canceled token throws <see cref="OperationCanceledException"/>
    /// synchronously — not a canceled <see cref="Task"/> — before any drain or submit, whether or not
    /// an accumulator exists.
    /// </summary>
    /// <remarks>
    /// Mutation: move the token check into the <c>async</c> half — the accumulator rows return a
    /// canceled <see cref="Task"/> instead of throwing.
    /// </remarks>
    [Theory]
    [MemberData(nameof(OperationsWithAndWithoutAccumulator))]
    public async Task AlreadyCanceledToken_ThrowsSynchronously_WithoutASubmit(string operation, bool withAccumulator)
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        try
        {
            if (withAccumulator)
            {
                await TestTimeout.Run(() => Send(producer, 0x01), s_deadline);
                Assert.NotNull(producer.StartedAccumulator);
            }

            SubmitFake fake = new SubmitFake(forwardTo: null);
            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            Assert.Throws<OperationCanceledException>(() => { _ = Invoke(producer, operation, cts.Token, fake); });
            Assert.Equal(0, fake.Count);
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// D5 row 2: a token canceled while the D3 drain is held cancels the <see cref="Task"/>, and the
    /// operation is never submitted; the drain itself carries on, so the buffered sends still reach
    /// the core once the hold is released.
    /// </summary>
    /// <remarks>
    /// The hold is <c>SendAccumulatorTests</c>' flush-expiry hold (closed core, batch thread parked in
    /// a delivery callback). A second send is buffered behind it. Mutation: swallow the drain's
    /// cancellation and submit without the token — the <see cref="Task"/> is never canceled, and the
    /// wait for it times out.
    /// </remarks>
    [Theory]
    [MemberData(nameof(Operations))]
    public async Task CanceledDuringTheDrain_CancelsTheTask_WithoutASubmit_AndTheDrainCarriesOn(string operation)
    {
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        SubmitFake fake = new SubmitFake(forwardTo: null);
        try
        {
            NativeMethods.ProducerClose(producer.Handle.DangerousGetHandle(), out IntPtr closeError);
            _ = KafkaException.FromHandle(closeError);

            Task<RecordMetadata> parked = producer.SendViaPump(
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x21 }),
                new DeliveryRegistration(new GatedDeliveryCallback(entered, release), Topic, 0));
            Assert.True(entered.Wait(s_deadline), "the batch thread never entered the delivery callback");
            Task<RecordMetadata> buffered = Send(producer, 0x22);

            using CancellationTokenSource cts = new CancellationTokenSource();
            Task control = Invoke(producer, operation, cts.Token, fake);
            Assert.False(control.IsCompleted, "the operation completed although its drain was held");

            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => control, s_deadline));
            Assert.True(control.IsCanceled);
            Assert.Equal(0, fake.Count);

            release.Set();
            Assert.True(producer.DrainPendingSends(s_deadline), "the drain stopped with the canceled waiter");
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => parked, s_deadline));
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => buffered, s_deadline));
            Assert.Equal(0, fake.Count);
        }
        finally
        {
            release.Set();
            producer.Dispose();
        }
    }

    /// <summary>
    /// D5 row 3 and D12: a token canceled after the submit cancels the awaiter while the operation
    /// runs on. The late callback — with success, or with a live error handle — neither throws nor
    /// changes the canceled <see cref="Task"/>. The span-the-op reference keeps the native producer
    /// alive past <c>Dispose()</c> until that callback, and the completion context is collectable
    /// after it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Differential witness: after <c>Dispose()</c> and before the late callback the handle is not
    /// closed (the reference is outstanding, so <c>Producer_destroy</c> has not run); after it, the
    /// handle is closed. The late error handle is freed by <c>OperationCompletionSource.Complete</c>
    /// through <c>KafkaException.FromHandle</c> before its cancellation check; that free is reviewed,
    /// not measured (a <c>DllImport</c> cannot be swapped to count it, as Python 1587 does).
    /// </para>
    /// <para>
    /// Mutations: release the span-the-op reference as soon as it is taken — the handle is closed
    /// right after <c>Dispose()</c>; drop <c>RegisterCancellation</c> — the <see cref="Task"/> is not
    /// canceled, and the wait for it times out.
    /// </para>
    /// </remarks>
    [Theory]
    [MemberData(nameof(OperationsWithLateOutcome))]
    public async Task CanceledAfterTheSubmit_CancelsTheAwaiter_AndTheLateCallbackIsHarmless(
        string operation, bool lateError)
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SubmitFake fake = new SubmitFake(forwardTo: null);
        using CancellationTokenSource cts = new CancellationTokenSource();

        Task control = Invoke(producer, operation, cts.Token, fake);
        Assert.Equal(1, fake.Count);
        Assert.False(control.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => control, s_deadline));
        Assert.True(control.IsCanceled);

        WeakReference context = TargetOf(fake.UserData);
        SafeProducerHandle handle = producer.Handle;
        producer.Dispose();
        Assert.False(handle.IsClosed, "the native producer was destroyed with the operation's reference outstanding");

        Exception? thrown = Record.Exception(() => fake.Fire(lateError ? NewError(120, "late failure") : IntPtr.Zero));
        Assert.Null(thrown);
        Assert.True(control.IsCanceled);
        Assert.True(handle.IsClosed, "the late callback did not release the operation's reference");

        fake.Forget();
        AssertCollected(context);
    }

    /// <summary>
    /// D5 row 4: a token canceled while commit or abort awaits the D4 barrier completes the
    /// <see cref="Task"/> <b>successfully</b> — the operation happened, and only the callback
    /// ordering is given up. The pump is parked, so the barrier never completes during the test.
    /// </summary>
    /// <remarks>
    /// The native callback is fired first, then the token: <c>Complete</c> disposes the token
    /// registration before setting the result, so the cancellation can only reach the barrier wait.
    /// Mutation: throw on cancellation after the barrier wait — the <see cref="Task"/> is canceled.
    /// </remarks>
    [Theory]
    [InlineData("commitTransaction")]
    [InlineData("abortTransaction")]
    public async Task CanceledWhileAwaitingTheBarrier_CompletesTheTaskSuccessfully(string operation)
    {
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        GatedDeliveryCallback callback = new GatedDeliveryCallback(entered, release);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true, s_prompt);
        SubmitFake fake = new SubmitFake(forwardTo: null);
        try
        {
            _ = producer.SendViaPump(
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x41 }),
                new DeliveryRegistration(callback, Topic, 0));
            Assert.True(entered.Wait(s_deadline), "the pump never entered the delivery callback");

            using CancellationTokenSource cts = new CancellationTokenSource();
            Task control = Invoke(producer, operation, cts.Token, fake);
            await PollUntil(() => fake.Count == 1, s_deadline, "the operation was never submitted");

            fake.Fire(IntPtr.Zero);
            cts.Cancel();

            await TestTimeout.Run(() => control, s_deadline);
            Assert.Equal(TaskStatus.RanToCompletion, control.Status);
            Assert.Equal(0, callback.Returned);
        }
        finally
        {
            release.Set();
            producer.Dispose();
        }
    }

    /// <summary>
    /// The Python 1557 analogue (<c>test_async_txn_ops_route_through_async_ffi</c>): each of the four
    /// async operations submits exactly once through its seam, and the real P/Invoke it forwards to
    /// completes the operation against the mock — the resolution proof S1 defers here.
    /// </summary>
    [Fact]
    public async Task EachAsyncOperation_SubmitsOnceThroughItsAsyncSymbol()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        try
        {
            SubmitFake init = new SubmitFake(forwardTo: "initTransactions");
            await TestTimeout.Run(() => Invoke(producer, "initTransactions", default, init), s_deadline);
            Assert.Equal(1, init.Count);

            producer.BeginTransaction();
            SubmitFake sendOffsets = new SubmitFake(forwardTo: "sendOffsetsToTransaction");
            await TestTimeout.Run(() => Invoke(producer, "sendOffsetsToTransaction", default, sendOffsets), s_deadline);
            Assert.Equal(1, sendOffsets.Count);
            Assert.True(producer.MockSentOffsets());

            SubmitFake commit = new SubmitFake(forwardTo: "commitTransaction");
            await TestTimeout.Run(() => Invoke(producer, "commitTransaction", default, commit), s_deadline);
            Assert.Equal(1, commit.Count);

            producer.BeginTransaction();
            SubmitFake abort = new SubmitFake(forwardTo: "abortTransaction");
            await TestTimeout.Run(() => Invoke(producer, "abortTransaction", default, abort), s_deadline);
            Assert.Equal(1, abort.Count);
        }
        finally
        {
            producer.Dispose();
        }
    }

    /// <summary>
    /// D12 marshalling: inside the send-offsets submit, the five arrays decode to the map's values
    /// (a missing leader epoch as <c>-1</c>), and the transient <c>ConsumerGroupMetadata_t</c> reads
    /// back through the bound accessors with the managed values. That the handle and the pins end with
    /// the submit is a review item (same-frame <c>finally</c>), not a measurement.
    /// </summary>
    [Fact]
    public async Task SendOffsetsToTransaction_MarshalsTheOffsetsAndTheGroupMetadataIntoTheSubmit()
    {
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition("txn-cancel-a", 0)] = new OffsetAndMetadata(5, "first", 3),
            [new TopicPartition("txn-cancel-b", 2)] = new OffsetAndMetadata(11, "zweite é", null),
        };
        ConsumerGroupMetadata groupMetadata =
            ConsumerGroupMetadata.FromCopiedValues("txn-cancel-group", 7, "member-1", "instance-1");

        List<string> decoded = new List<string>();
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        try
        {
            ProducerCallbacks.OperationCallback? callback = null;
            IntPtr userData = IntPtr.Zero;
            Task control = producer.SendOffsetsToTransactionWithCallback(
                offsets,
                groupMetadata,
                default,
                (p, topics, partitions, offsetValues, leaderEpochs, metadata, count, nativeGroupMetadata, cb, ud) =>
                {
                    decoded.Add($"count={count}");
                    for (int i = 0; i < count; i++)
                    {
                        decoded.Add(
                            $"{Utf8Marshal.PtrToString(topics[i])}/{partitions[i]}@{offsetValues[i]}" +
                            $" epoch={leaderEpochs[i]} metadata={Utf8Marshal.PtrToString(metadata[i])}");
                    }

                    decoded.Add(
                        $"group={Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(nativeGroupMetadata))}" +
                        $" generation={NativeMethods.ConsumerGroupMetadataGenerationId(nativeGroupMetadata)}" +
                        $" member={Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataMemberId(nativeGroupMetadata))}" +
                        $" instance={Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupInstanceId(nativeGroupMetadata))}");
                    callback = cb;
                    userData = ud;
                });

            Assert.NotNull(callback);
            callback!(IntPtr.Zero, userData);
            await TestTimeout.Run(() => control, s_deadline);
        }
        finally
        {
            producer.Dispose();
        }

        List<string> expected = new List<string> { "count=2" };
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
            expected.Add(
                $"{entry.Key.Topic}/{entry.Key.Partition}@{entry.Value.Offset}" +
                $" epoch={entry.Value.LeaderEpoch ?? -1} metadata={entry.Value.Metadata}");
        }

        expected.Add("group=txn-cancel-group generation=7 member=member-1 instance=instance-1");
        Assert.Equal(expected, decoded);
    }

    /// <summary>
    /// D13: an empty offsets map is not short-circuited on either surface. The async form submits it
    /// once, with <c>count == 0</c>, and the core's answer completes the <see cref="Task"/>. The sync
    /// form, called after the commit so no transaction is open, reaches the core, which rejects it
    /// with its state error; a managed early return would succeed instead. Inside the transaction the
    /// sync form succeeds and leaves <c>SentOffsets()</c> false, as Java's mock does for an empty map.
    /// </summary>
    /// <remarks>
    /// Mutations: return early on an empty map in the async form — the submit count reads 0; in the
    /// sync form — the call after the commit returns without the core's error.
    /// </remarks>
    [Fact]
    public async Task SendOffsetsToTransaction_WithAnEmptyMap_IsStillSubmitted()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        try
        {
            producer.InitTransactions();
            producer.BeginTransaction();

            int submits = 0;
            int submittedCount = -1;
            await TestTimeout.Run(
                () => producer.SendOffsetsToTransactionWithCallback(
                    new Dictionary<TopicPartition, OffsetAndMetadata>(),
                    ConsumerGroupMetadata.FromCopiedValues("txn-cancel-group", -1, string.Empty, null),
                    default,
                    (p, topics, partitions, offsets, leaderEpochs, metadata, count, groupMetadata, cb, ud) =>
                    {
                        submits++;
                        submittedCount = count;
                        NativeMethods.ProducerSendOffsetsToTransactionAsync(
                            p, topics, partitions, offsets, leaderEpochs, metadata, count, groupMetadata, cb, ud);
                    }),
                s_deadline);

            Assert.Equal(1, submits);
            Assert.Equal(0, submittedCount);

            producer.SendOffsetsToTransaction(
                new Dictionary<TopicPartition, OffsetAndMetadata>(),
                ConsumerGroupMetadata.FromCopiedValues("txn-cancel-group", -1, string.Empty, null));
            Assert.False(producer.MockSentOffsets());

            producer.CommitTransaction();
            KafkaException failure = Assert.Throws<KafkaException>(
                () => producer.SendOffsetsToTransaction(
                    new Dictionary<TopicPartition, OffsetAndMetadata>(),
                    ConsumerGroupMetadata.FromCopiedValues("txn-cancel-group", -1, string.Empty, null)));
            Assert.Equal(-4, failure.Code);
            Assert.Equal("There is no open transaction.", failure.Message);
        }
        finally
        {
            producer.Dispose();
        }
    }

    private static Task Invoke(NativeProducer producer, string operation, CancellationToken token, SubmitFake fake) =>
        operation switch
        {
            "initTransactions" => producer.InitTransactionsWithCallback(token, fake.Submit),
            "sendOffsetsToTransaction" => producer.SendOffsetsToTransactionWithCallback(
                new Dictionary<TopicPartition, OffsetAndMetadata>
                {
                    [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(3, "cancel-metadata"),
                },
                ConsumerGroupMetadata.FromCopiedValues("txn-cancel-group", -1, string.Empty, null),
                token,
                fake.SubmitSendOffsets),
            "commitTransaction" => producer.CommitTransactionWithCallback(token, fake.Submit),
            "abortTransaction" => producer.AbortTransactionWithCallback(token, fake.Submit),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Not a control operation."),
        };

    private static Task<RecordMetadata> Send(NativeProducer producer, byte tag) =>
        producer.SendViaPump(new SerializedProducerRecord(Topic, 0, null, null, new byte[] { tag }), delivery: null);

    private static IntPtr NewError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        return NativeMethods.KafkaErrorNew(code, pinned.Pointer);
    }

    // Not inlined, so no local of this frame keeps the context reachable.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference TargetOf(IntPtr userData) =>
        new WeakReference(GCHandle.FromIntPtr(userData).Target);

    private static void AssertCollected(WeakReference reference)
    {
        for (int i = 0; i < 10 && reference.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.False(reference.IsAlive, "the completion context is still reachable after the late callback");
    }

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
    /// The submit fake: counts calls, and either forwards to the real P/Invoke of the operation it
    /// is named for or keeps the callback and user data for the test to fire.
    /// </summary>
    private sealed class SubmitFake
    {
        private readonly string? _forwardTo;
        private int _count;
        private ProducerCallbacks.OperationCallback? _callback;
        private IntPtr _userData;

        // null: capture only. Otherwise the Java name of the operation whose async symbol to call.
        internal SubmitFake(string? forwardTo) => _forwardTo = forwardTo;

        internal int Count => Volatile.Read(ref _count);

        internal IntPtr UserData => _userData;

        internal void Submit(IntPtr producer, ProducerCallbacks.OperationCallback callback, IntPtr userData)
        {
            Capture(callback, userData);
            switch (_forwardTo)
            {
                case null:
                    break;
                case "initTransactions":
                    NativeMethods.ProducerInitTransactionsAsync(producer, callback, userData);
                    break;
                case "commitTransaction":
                    NativeMethods.ProducerCommitTransactionAsync(producer, callback, userData);
                    break;
                case "abortTransaction":
                    NativeMethods.ProducerAbortTransactionAsync(producer, callback, userData);
                    break;
                default:
                    throw new InvalidOperationException($"{_forwardTo} does not take the void submit.");
            }
        }

        internal void SubmitSendOffsets(
            IntPtr producer,
            IntPtr[] topics,
            int[] partitions,
            long[] offsets,
            int[] leaderEpochs,
            IntPtr[] metadata,
            int count,
            IntPtr groupMetadata,
            ProducerCallbacks.OperationCallback callback,
            IntPtr userData)
        {
            Capture(callback, userData);
            if (_forwardTo is not null)
            {
                NativeMethods.ProducerSendOffsetsToTransactionAsync(
                    producer, topics, partitions, offsets, leaderEpochs, metadata, count, groupMetadata, callback, userData);
            }
        }

        internal void Fire(IntPtr error) => _callback!(error, _userData);

        internal void Forget()
        {
            _callback = null;
            _userData = IntPtr.Zero;
        }

        private void Capture(ProducerCallbacks.OperationCallback callback, IntPtr userData)
        {
            Interlocked.Increment(ref _count);
            _callback = callback;
            _userData = userData;
        }
    }

    /// <summary>
    /// A delivery callback that parks the thread that fires it until the test releases it, then
    /// counts itself returned. The wait is bounded, and the body runs inside
    /// <c>DeliveryRegistration.Fire</c>'s no-throw boundary.
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
