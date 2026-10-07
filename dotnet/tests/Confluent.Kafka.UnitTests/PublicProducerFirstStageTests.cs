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
using System.Globalization;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The serial collection for tests that configure a public producer's send accumulator through its
/// environment variables (<see cref="SendAccumulatorSettings.FromEnvironment"/>, read once, at the
/// producer's first async send). Process-wide state, so nothing may run beside these tests: xUnit
/// runs a collection with parallelization disabled after every parallel one has finished.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class AccumulatorEnvironmentCollection
{
    /// <summary>The collection name — see <see cref="PublicProducerFirstStageTests"/>.</summary>
    internal const string Name = "producer-accumulator-environment";
}

/// <summary>
/// M11/P3.5 S3 — the two-stage async send's <b>first stage</b> through the public
/// <see cref="AsyncMockProducer{TKey, TValue}"/>: what <see cref="IAsyncProducer{TKey, TValue}.Flush"/>,
/// teardown and a caller's token do to sends whose first stage is still pending. The accumulator
/// seam's half is <c>Interop.SendAccumulatorFirstStageTests</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Async flavor only, deliberately.</b> ffi §A6's "both flavors" rule is applied through the
/// <c>PublicProducerDeliveryCallbackTests.Flavor</c> seam (sync <see cref="MockProducer{TKey, TValue}"/>
/// vs async <see cref="AsyncMockProducer{TKey, TValue}"/>), and its sync side has nothing here to run
/// on: the synchronous <c>Send</c> returns the <see cref="RecordMetadata"/> itself, so it has no first
/// stage, takes no token, and never reaches the admission bound — the COMMENTS.DONE.72 precedent.
/// Within the async flavor every test covers both <c>Send</c> overloads (with and without an
/// <see cref="IDeliveryCallback"/>), and the flood tests both kinds of pending first stage (a
/// cancelable token, and none).
/// </para>
/// <para>
/// The bound and the batch window come from the environment, so every test sets them (a 60 s window
/// holds the batch thread back) inside an <see cref="AccumulatorEnvironment"/> scope that restores the
/// previous values, and observes the accumulator through reflection on the producer's own instance.
/// </para>
/// </remarks>
[Collection(AccumulatorEnvironmentCollection.Name)]
public sealed class PublicProducerFirstStageTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // "Promptly": room for a loaded runner, well short of the 60 s window, so anything that is in
    // fact waiting for the window (or for nothing) fails here instead of being rescued by it.
    private static readonly TimeSpan s_prompt = TimeSpan.FromSeconds(5);

    // How long a callback has to fire a second time before "exactly once" is believed.
    private static readonly TimeSpan s_settle = TimeSpan.FromMilliseconds(250);

    private const string Topic = "first-stage-topic";

    private readonly ITestOutputHelper _output;

    public PublicProducerFirstStageTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task Flush_CompletesEveryPendingFirstStage_AndEveryDelivery()
    {
        // M11/P3.5 T5 — Flush drains the accumulator before it flushes the core, and a drain returns
        // the permits every pending first stage is waiting for. So after Flush, every first stage
        // completes — eventually, since its continuation is queued to the pool — successfully, with
        // its own delivery task, and every delivery completes. A Flush that skipped the accumulator
        // leaves all of it waiting on the 60 s window, and fails the prompt bound below.
        const int Bound = 2;
        const int Sends = 10;

        using AccumulatorEnvironment environment = new AccumulatorEnvironment(Bound);
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource live = new CancellationTokenSource();
        CountingCallback callback = new CountingCallback();

        Task<AsyncKafkaFuture<RecordMetadata>>[] stages = new Task<AsyncKafkaFuture<RecordMetadata>>[Sends];
        int withCallback = 0;
        for (int i = 0; i < Sends; i++)
        {
            // Both overloads, and both kinds of pending stage: a live token on half the sends.
            CancellationToken token = i % 2 == 0 ? live.Token : CancellationToken.None;
            bool hasCallback = i % 4 < 2;
            withCallback += hasCallback ? 1 : 0;
            ValueTask<AsyncKafkaFuture<RecordMetadata>> send = hasCallback
                ? producer.Send(Record(i), callback, token)
                : producer.Send(Record(i), token);
            stages[i] = send.AsTask();
        }

        Assert.Equal(Sends - Bound, CountPending(stages));
        Task<RecordMetadata>[] deliveries = DeliveriesOf(producer, Sends);
        Assert.Equal(0, producer.HistoryCount());

        await TestTimeout.Run(() => producer.Flush(), s_deadline);
        Assert.Equal(Sends, producer.HistoryCount());

        await SettleWithin(Task.WhenAll(stages), s_prompt, "a first stage was still pending after Flush");
        for (int i = 0; i < Sends; i++)
        {
            Assert.True(
                stages[i].Status == TaskStatus.RanToCompletion,
                $"first stage {i} ended {stages[i].Status} after Flush");
            Assert.Same(deliveries[i], (await stages[i]).Get());
        }

        RecordMetadata[] metadata = await TestTimeout.Run(() => Task.WhenAll(deliveries), s_prompt);
        for (int i = 0; i < Sends; i++)
        {
            Assert.Equal(i, metadata[i].Offset);
        }

        SendAccumulator accumulator = AccumulatorOf(producer);
        await WaitUntil(() => accumulator.AvailableAdmissions == Bound, s_prompt);
        Assert.Equal(Bound, accumulator.AvailableAdmissions);

        await WaitUntil(() => callback.Count == withCallback, s_prompt);
        await Task.Delay(s_settle);
        Assert.Equal(withCallback, callback.Count);
        Assert.False(live.IsCancellationRequested);
    }

    [Theory]
    [InlineData("Close")]
    [InlineData("Dispose")]
    [InlineData("DisposeAsync")]
    public async Task Teardown_WithANonAwaitingFloodPastTheBound_SettlesBothStagesOfEverySendExactlyOnce(string teardown)
    {
        // M11/P3.5 T9 — a caller that floods past the bound without awaiting, then tears the producer
        // down. Teardown cancels the admission gate before its final drain, so every pending first
        // stage's wait ends CANCELLED underneath — and D3 says the stage completes SUCCESSFULLY anyway,
        // because its record was appended before it waited and teardown sends it. So: every first
        // stage RanToCompletion with its own delivery task (not Canceled, not Faulted, not pending),
        // every record drained into the still-open pump gate, and every delivery callback fired exactly
        // once.
        //
        // ⚠ Which way each wait ends is a RACE here: teardown cancels the gate and then lets the final
        // drain run, and the drain's releases usually grant the waits before their cancellation lands
        // (measured with a D3 mutant — a stage continuation run only on a granted permit — this test
        // went red in some runs and flavors and not others). So this is the end-to-end check, not the
        // deterministic one: Interop.SendAccumulatorFirstStageTests.Admission_CancelableFirstStage_IsCompletedSuccessfullyByStopsCancel_WhenTheBatchThreadCannotDrain
        // parks the batch thread so the cancel is the ONLY way out, and pins D3 there. For the same
        // reason this test cannot see a Stop that forgot to cancel the gate: the final drain then
        // releases every wait anyway (each appended record returns one permit), so the outcome here is
        // identical — the parked-thread tests are the ones that see it.
        //
        // The one tolerated exception is pump-teardown residual 2 (PublicProducerAccumulatorTeardownTests):
        // a delivery the pump's teardown faults with the "closed" KafkaException, which fires no
        // callback — so for such an index the callback count must be 0, never 1 and never 2.
        const int Bound = 4;
        const int Flood = 40;

        using AccumulatorEnvironment environment = new AccumulatorEnvironment(Bound);
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource live = new CancellationTokenSource();

        (Task<AsyncKafkaFuture<RecordMetadata>>[] stages, Task<RecordMetadata>[] deliveries, CountingCallback?[] callbacks) =
            FloodPastTheBound(producer, live.Token, Bound, Flood);

        await TearDown(producer, teardown);

        await SettleWithin(Task.WhenAll(stages), s_prompt, $"{teardown} left a first stage pending");
        for (int i = 0; i < Flood; i++)
        {
            Assert.True(
                stages[i].Status == TaskStatus.RanToCompletion,
                $"{teardown}: first stage {i} ended {stages[i].Status} — teardown must complete it successfully (D3)");
            Assert.Same(deliveries[i], (await stages[i]).Get());
        }

        Assert.Equal(Flood, producer.DrainedSendCount);

        await SettleWithin(Task.WhenAll(deliveries), s_prompt, $"{teardown} left a delivery pending", observeFaults: true);
        await Task.Delay(s_settle);

        int faulted = 0;
        for (int i = 0; i < Flood; i++)
        {
            Task<RecordMetadata> delivery = deliveries[i];
            CountingCallback? callback = callbacks[i];
            if (delivery.Status == TaskStatus.RanToCompletion)
            {
                Assert.Equal(i, (await delivery).Offset);
                if (callback is not null)
                {
                    Assert.True(callback.Count == 1, $"{teardown}: send {i}'s callback fired {callback.Count} times");
                    Assert.Null(callback.LastException);
                    Assert.Equal(i, callback.LastMetadata!.Offset);
                }
            }
            else
            {
                faulted++;
                KafkaException failure = Assert.IsType<KafkaException>(delivery.Exception?.InnerException);
                Assert.Contains("closed", failure.Message, StringComparison.OrdinalIgnoreCase);
                if (callback is not null)
                {
                    Assert.True(callback.Count == 0, $"{teardown}: send {i} faulted at teardown but its callback fired {callback.Count} times");
                }
            }
        }

        _output.WriteLine($"{teardown}: {faulted} of {Flood} deliveries took the pump-teardown residual");
        producer.Dispose();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Send_CallerTokenFiresWhileTheFirstStageWaits_CancelsBothStages_AndTheRecordIsStillSent(bool withCallback)
    {
        // M11/P3.5 T17 — D2 (c) end to end. The caller's token fires while its send's first stage is
        // pending: the first stage ends Canceled with the token (the accumulator's half, T16), the
        // delivery task ends Canceled with the token too (SendViaPump's own registration) — so the
        // caller is fully released — and still the record goes out, and its delivery callback, which
        // is not a Task and is not cancelled by anything, fires exactly once with the real metadata.
        // The delivery task stays Canceled after the record lands: the pump's later TrySetResult is a
        // no-op, never a second outcome.
        const int Bound = 2;

        using AccumulatorEnvironment environment = new AccumulatorEnvironment(Bound);
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource cancellation = new CancellationTokenSource();
        CountingCallback callback = new CountingCallback();

        Task<RecordMetadata> first = producer.Send(Record(0)).Delivery();
        Task<RecordMetadata> second = producer.Send(Record(1)).Delivery();
        SendAccumulator accumulator = AccumulatorOf(producer);
        Assert.Equal(0, accumulator.AvailableAdmissions);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> send = withCallback
            ? producer.Send(Record(2), callback, cancellation.Token)
            : producer.Send(Record(2), cancellation.Token);
        Assert.False(send.IsCompleted, "the first stage completed although the bound was full");
        Task<AsyncKafkaFuture<RecordMetadata>> stage = send.AsTask();
        Task<RecordMetadata> delivery = DeliveriesOf(producer, 3)[2];

        cancellation.Cancel();

        OperationCanceledException stageCanceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => stage, s_prompt));
        Assert.Equal(cancellation.Token, stageCanceled.CancellationToken);
        OperationCanceledException deliveryCanceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => delivery, s_prompt));
        Assert.Equal(cancellation.Token, deliveryCanceled.CancellationToken);

        // Released BEFORE capacity freed: nothing has drained, nothing reached the core.
        Assert.Equal(0, accumulator.SendBatchRecordCount);
        Assert.Equal(0, producer.HistoryCount());

        // ...and the record is still sent — counted on the core, not inferred from an awaiter.
        producer.WaitForSendsToReachCore(s_deadline);
        Assert.Equal(3, producer.HistoryCount());
        await TestTimeout.Run(() => Task.WhenAll(first, second), s_deadline);
        await WaitUntil(() => producer.DrainedSendCount == 3, s_prompt);
        Assert.Equal(3, producer.DrainedSendCount);

        if (withCallback)
        {
            await WaitUntil(() => callback.Count > 0, s_prompt);
            await Task.Delay(s_settle);
            Assert.Equal(1, callback.Count);
            Assert.Null(callback.LastException);
            Assert.Equal(2, callback.LastMetadata!.Offset);
        }
        else
        {
            await Task.Delay(s_settle);
            Assert.Equal(0, callback.Count);
        }

        Assert.Equal(TaskStatus.Canceled, delivery.Status);
        Assert.Equal(TaskStatus.Canceled, stage.Status);
    }

    [Theory]
    [InlineData("Close")]
    [InlineData("Dispose")]
    [InlineData("DisposeAsync")]
    public async Task Teardown_WithANonAwaitingFloodPastTheBound_LeavesNoUnobservedTaskFault(string teardown)
    {
        // M11/P3.5 T12 — teardown of a flood with pending first stages ends every admission wait
        // CANCELLED underneath its stage, and each stage's continuation then runs on the pool with no
        // one awaiting it. Any fault there (a Set* where a TrySet* belongs, a throw out of a
        // continuation) is invisible to the caller, who sees only a completed first stage; it surfaces
        // only as an unobserved task fault when the faulted continuation is collected. So this test
        // observes everything the CALLER holds, drops it, forces the collections, and requires that no
        // unobserved fault raised from the producer's own code arrived.
        //
        // The listener is installed after a collection that flushes earlier tests' garbage (this
        // collection runs with nothing beside it), and it only counts faults whose trace names the
        // producer's code, so a stray fault from elsewhere cannot fail it.
        const int Bound = 4;
        const int Flood = 40;

        CollectAndFinalize();
        List<string> unobserved = new List<string>();
        EventHandler<UnobservedTaskExceptionEventArgs> listener = (_, e) =>
        {
            string text = e.Exception.ToString();
            if (text.Contains("Confluent.Kafka.Internal", StringComparison.Ordinal))
            {
                lock (unobserved)
                {
                    unobserved.Add(text);
                }
            }
        };

        TaskScheduler.UnobservedTaskException += listener;
        try
        {
            await FloodTearDownAndObserve(teardown, Bound, Flood);
            CollectAndFinalize();
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= listener;
        }

        lock (unobserved)
        {
            Assert.True(
                unobserved.Count == 0,
                $"{teardown}: {unobserved.Count} unobserved task fault(s) from the producer:\n" + string.Join("\n", unobserved));
        }
    }

    private async Task FloodTearDownAndObserve(string teardown, int bound, int flood)
    {
        using AccumulatorEnvironment environment = new AccumulatorEnvironment(bound);
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource live = new CancellationTokenSource();

        (Task<AsyncKafkaFuture<RecordMetadata>>[] stages, Task<RecordMetadata>[] deliveries, _) =
            FloodPastTheBound(producer, live.Token, bound, flood);

        await TearDown(producer, teardown);

        // Observe all of it: the caller's own tasks must not be what the listener hears about.
        await SettleWithin(Task.WhenAll(stages), s_prompt, $"{teardown} left a first stage pending", observeFaults: true);

        // That observation also hides a first stage that teardown FAULTED (or cancelled) from the
        // listener, so require the outcome directly: a teardown-cancelled admission wait still
        // completes its stage with the record's delivery Task (D3), and the live token never fired.
        // A caller that drops its stage would otherwise meet each such fault only as an unobserved
        // one (the M11/P3.5 S3 mutant that faulted the stage on a cancelled wait passed without this).
        int unsuccessful = Array.FindAll(stages, stage => stage.Status != TaskStatus.RanToCompletion).Length;
        Assert.True(
            unsuccessful == 0,
            $"{teardown}: {unsuccessful} first stage(s) did not complete successfully at teardown");

        await SettleWithin(Task.WhenAll(deliveries), s_prompt, $"{teardown} left a delivery pending", observeFaults: true);
        foreach (Task<RecordMetadata> delivery in deliveries)
        {
            _ = delivery.Exception;
        }

        await Task.Delay(s_settle);
        producer.Dispose();
    }

    /// <summary>
    /// Fires <paramref name="flood"/> sends without awaiting any first stage, into a bound of
    /// <paramref name="bound"/> held back by the 60 s window, over every combination of overload
    /// (index even: with a callback) and pending-stage kind (index mod 4 below 2: the live token).
    /// Returns each send's first stage, its delivery task (read from the pending node, in call order)
    /// and its callback, if it has one.
    /// </summary>
    private static (Task<AsyncKafkaFuture<RecordMetadata>>[] Stages, Task<RecordMetadata>[] Deliveries, CountingCallback?[] Callbacks)
        FloodPastTheBound(AsyncMockProducer<byte[], byte[]> producer, CancellationToken live, int bound, int flood)
    {
        Task<AsyncKafkaFuture<RecordMetadata>>[] stages = new Task<AsyncKafkaFuture<RecordMetadata>>[flood];
        CountingCallback?[] callbacks = new CountingCallback?[flood];
        for (int i = 0; i < flood; i++)
        {
            CancellationToken token = i % 4 < 2 ? live : CancellationToken.None;
            ValueTask<AsyncKafkaFuture<RecordMetadata>> send;
            if (i % 2 == 0)
            {
                CountingCallback callback = new CountingCallback();
                callbacks[i] = callback;
                send = producer.Send(Record(i), callback, token);
            }
            else
            {
                send = producer.Send(Record(i), token);
            }

            stages[i] = send.AsTask();
        }

        Assert.Equal(flood - bound, CountPending(stages));
        Task<RecordMetadata>[] deliveries = DeliveriesOf(producer, flood);
        Assert.Equal(0, producer.HistoryCount());
        return (stages, deliveries, callbacks);
    }

    private static async Task TearDown(AsyncMockProducer<byte[], byte[]> producer, string teardown)
    {
        switch (teardown)
        {
            case "Close":
                await TestTimeout.Run(() => producer.Close(), s_deadline);
                break;
            case "Dispose":
                TestTimeout.Run(producer.Dispose, s_deadline);
                break;
            case "DisposeAsync":
                await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(teardown), teardown, "unknown teardown");
        }
    }

    private static ProducerRecord<byte[], byte[]> Record(int index) =>
        new ProducerRecord<byte[], byte[]>(Topic, new byte[] { (byte)index, 0xAA }, partition: 0);

    private static int CountPending(Task<AsyncKafkaFuture<RecordMetadata>>[] stages)
    {
        int pending = 0;
        foreach (Task<AsyncKafkaFuture<RecordMetadata>> stage in stages)
        {
            pending += stage.IsCompleted ? 0 : 1;
        }

        return pending;
    }

    /// <summary>
    /// The delivery tasks of the first <paramref name="count"/> records in the producer's pending
    /// accumulator node, in call order — the only handle on the delivery of a send whose first stage
    /// has not completed (or never will, when its token fires).
    /// </summary>
    private static Task<RecordMetadata>[] DeliveriesOf(AsyncMockProducer<byte[], byte[]> producer, int count)
    {
        TaskCompletionSource<RecordMetadata>?[] slots =
            SendAccumulatorTests.Harness.PendingCompletionsOf(AccumulatorOf(producer));
        Task<RecordMetadata>[] deliveries = new Task<RecordMetadata>[count];
        for (int i = 0; i < count; i++)
        {
            deliveries[i] = (slots[i] ?? throw new InvalidOperationException($"pending slot {i} is empty")).Task;
        }

        return deliveries;
    }

    /// <summary>The public producer's own send accumulator, created by its first async send.</summary>
    private static SendAccumulator AccumulatorOf(AsyncMockProducer<byte[], byte[]> producer)
    {
        FieldInfo nativeField = typeof(AsyncMockProducer<byte[], byte[]>).GetField(
                "_native", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("AsyncMockProducer._native was renamed; update this test");
        NativeProducer native = (NativeProducer)nativeField.GetValue(producer)!;
        FieldInfo accumulatorField = typeof(NativeProducer).GetField(
                "_accumulator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("NativeProducer._accumulator was renamed; update this test");
        return (SendAccumulator?)accumulatorField.GetValue(native)
            ?? throw new InvalidOperationException("the producer has no accumulator yet — its first async send creates it");
    }

    private static async Task SettleWithin(Task all, TimeSpan timeout, string message, bool observeFaults = false)
    {
        Task settled = await Task.WhenAny(all, Task.Delay(timeout));
        Assert.True(ReferenceEquals(all, settled), message);
        if (observeFaults)
        {
            _ = all.Exception;
        }
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (!condition() && watch.Elapsed < timeout)
        {
            await Task.Delay(10);
        }
    }

    private static void CollectAndFinalize()
    {
        for (int i = 0; i < 2; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        GC.Collect();
    }

    private sealed class CountingCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal RecordMetadata? LastMetadata { get; private set; }

        internal KafkaException? LastException { get; private set; }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            LastMetadata = metadata;
            LastException = exception;
            _ = Interlocked.Increment(ref _count);
        }
    }

    /// <summary>
    /// Sets the accumulator's environment variables for one test — threshold 1000, a 60 s window,
    /// chunk 1100, and the given admission bound — and restores whatever was there before.
    /// </summary>
    private sealed class AccumulatorEnvironment : IDisposable
    {
        private static readonly string[] s_variables =
        {
            SendAccumulatorSettings.ThresholdVariable,
            SendAccumulatorSettings.WindowVariable,
            SendAccumulatorSettings.ChunkVariable,
            SendAccumulatorSettings.MaxAdmittedVariable,
        };

        private readonly string?[] _previous = new string?[s_variables.Length];

        internal AccumulatorEnvironment(int maxAdmitted)
        {
            for (int i = 0; i < s_variables.Length; i++)
            {
                _previous[i] = Environment.GetEnvironmentVariable(s_variables[i]);
            }

            Environment.SetEnvironmentVariable(SendAccumulatorSettings.ThresholdVariable, "1000");
            Environment.SetEnvironmentVariable(SendAccumulatorSettings.WindowVariable, "60000");
            Environment.SetEnvironmentVariable(SendAccumulatorSettings.ChunkVariable, "1100");
            Environment.SetEnvironmentVariable(
                SendAccumulatorSettings.MaxAdmittedVariable, maxAdmitted.ToString(CultureInfo.InvariantCulture));
        }

        public void Dispose()
        {
            for (int i = 0; i < s_variables.Length; i++)
            {
                Environment.SetEnvironmentVariable(s_variables[i], _previous[i]);
            }
        }
    }
}
