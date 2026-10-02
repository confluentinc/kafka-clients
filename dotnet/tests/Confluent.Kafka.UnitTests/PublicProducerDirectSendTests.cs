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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async producer's <b>direct</b> send path (send-approach-2 POC,
/// <c>NativeProducer.SendDirect</c>): caller-thread <c>send_with_callback_cancellable</c>, push
/// delivery on the core's dispatcher thread, no accumulator and no pump. Every producer here is
/// pinned to that path (<see cref="ProducerSendPath.Direct{T}"/>), so these tests hold on the
/// <c>pump</c> run of the suite too.
/// </summary>
/// <remarks>
/// What each group pins, against Java's <c>KafkaProducer</c>:
/// <list type="bullet">
/// <item><b>Delivery</b> — exactly one callback per accepted record, fired before the record's task
/// completes (<c>completeFutureAndFireCallbacks</c>), off the caller's thread, and in send order for
/// one caller.</item>
/// <item><b>Flush / close</b> — both return only after every earlier record's callback has run
/// (Java's <c>flush()</c> / <c>close()</c>), on the async and the sync teardown alike.</item>
/// <item><b>Failures</b> — a failure <c>send()</c> itself reports throws and fires nothing (Java's
/// <c>catch (KafkaException)</c>); an API-level rejection fires the callback with placeholder
/// metadata and faults the task (Java's <c>catch (ApiException)</c>).</item>
/// <item><b>Cancellation</b> — the token interrupts a send blocked on metadata the way a thread
/// interrupt does in Java (<c>InterruptException</c>, nothing appended, no callback), and a close
/// wakes a blocked send; while one send is blocked, other operations are not.</item>
/// </list>
/// The blocking tests use a real producer whose bootstrap server refuses connections
/// (<c>127.0.0.1:1</c>), so a send parks on metadata until <c>max.block.ms</c>.
/// </remarks>
public sealed class PublicProducerDirectSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Well under the parked send's max.block.ms (30 s), so "returned promptly" is unambiguous.
    private static readonly TimeSpan s_prompt = TimeSpan.FromSeconds(10);

    // Long enough for a freshly started send to reach its metadata wait.
    private static readonly TimeSpan s_settle = TimeSpan.FromMilliseconds(500);

    private const string Topic = "direct-send-topic";

    private const int InterruptCode = -12;      // kafka_common_ErrorCode_INTERRUPT
    private const int IllegalStateCode = -4;    // kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE
    private const int RequestTimedOutCode = 7;  // kafka_common_ErrorCode_REQUEST_TIMED_OUT

    // ---- Path selection ----

    [Fact]
    public void DirectPath_IsSelectedByTheOverride_AndPumpPathStaysAvailable()
    {
        using NativeProducer direct = ProducerSendPath.Direct(() => NativeProducer.CreateMock());
        using NativeProducer pump = ProducerSendPath.Pump(() => NativeProducer.CreateMock());

        Assert.True(direct.UsesDirectSend);
        Assert.False(pump.UsesDirectSend);
    }

    [Fact]
    public void DirectPath_IsTheDefault_UnlessTheEnvironmentSelectsThePump()
    {
        using NativeProducer producer = NativeProducer.CreateMock();

        string? raw = Environment.GetEnvironmentVariable(NativeProducer.AsyncSendPathVariable);
        bool expectDirect = !string.Equals(raw?.Trim(), "pump", StringComparison.OrdinalIgnoreCase);
        Assert.Equal(expectDirect, producer.UsesDirectSend);
    }

    // ---- Delivery: exactly once, before the task, off the caller's thread, in order ----

    [Fact]
    public async Task Send_AutoCompleteMock_ResolvesWithMetadata_AndCallsBackOncePerRecord()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock();
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata>[] sends = Enumerable.Range(0, 20)
            .Select(i => producer.Send(Record(i), callback))
            .ToArray();
        RecordMetadata[] results = await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        for (int i = 0; i < results.Length; i++)
        {
            Assert.Equal(Topic, results[i].Topic);
            Assert.Equal(0, results[i].Partition);
            Assert.Equal(i, results[i].Offset);
        }

        await producer.Flush();
        Assert.Equal(20, callback.Count);
        Assert.All(callback.Exceptions, e => Assert.Null(e));
        Assert.Equal(Enumerable.Range(0, 20).Select(i => (long)i), callback.Offsets.OrderBy(o => o));
    }

    [Fact]
    public async Task Send_FromOneCaller_ReachesTheCoreInCallOrder()
    {
        // Each call returns only once its record is appended, so one caller's records cannot be
        // reordered on the way in (Java's ordering guarantee, ProducerConfig.java:274).
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock();

        const int Count = 200;
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Count];
        for (int i = 0; i < Count; i++)
        {
            sends[i] = producer.Send(Record(i));
        }

        RecordMetadata[] results = await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        Assert.Equal(Enumerable.Range(0, Count).Select(i => (long)i), results.Select(r => r.Offset));
    }

    [Fact]
    public async Task DeliveryCallback_RunsBeforeTheTaskCompletes_AndOffTheCallersThread()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);
        TaskObservingCallback callback = new TaskObservingCallback();

        Task<RecordMetadata> send = producer.Send(Record(0), callback);
        callback.Observe(send);
        Assert.False(send.IsCompleted, "a manual mock holds the send");

        int callerThread = Environment.CurrentManagedThreadId;
        Assert.True(producer.CompleteNext());
        RecordMetadata metadata = await TestTimeout.Run(() => send, s_deadline);

        Assert.True(callback.Fired.Wait(s_deadline));
        Assert.False(callback.TaskWasCompletedInsideCallback, "D3: the callback runs before the task completes");
        Assert.NotEqual(callerThread, callback.ThreadId);
        Assert.Equal(0, metadata.Offset);
    }

    [Fact]
    public async Task ConcurrentSenders_EveryRecordCallsBackExactlyOnce()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock();
        CountingCallback callback = new CountingCallback();

        const int Threads = 8;
        const int PerThread = 250;
        Task<RecordMetadata>[][] sends = await Task.WhenAll(Enumerable.Range(0, Threads).Select(t => Task.Run(() =>
            Enumerable.Range(0, PerThread)
                .Select(i => producer.Send(Record(t * PerThread + i), callback))
                .ToArray())));

        await TestTimeout.Run(() => Task.WhenAll(sends.SelectMany(s => s)), s_deadline);
        await producer.Flush();

        Assert.Equal(Threads * PerThread, callback.Count);
        Assert.Equal(Threads * PerThread, callback.DistinctValues);
        Assert.Equal(Threads * PerThread, producer.HistoryCount());
    }

    [Fact]
    public async Task ThrowingDeliveryCallback_IsSwallowed_AndLaterSendsStillComplete()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock();

        RecordMetadata first = await TestTimeout.Run(() => producer.Send(Record(0), new ThrowingCallback()), s_deadline);
        RecordMetadata second = await TestTimeout.Run(() => producer.Send(Record(1)), s_deadline);

        Assert.Equal(0, first.Offset);
        Assert.Equal(1, second.Offset);
    }

    // ---- Flush / close post-conditions (Gap 1) ----

    [Fact]
    public async Task Flush_ReturnsOnlyAfterEveryEarlierCallbackRan()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);
        SlowCallback callback = new SlowCallback(TimeSpan.FromMilliseconds(10));

        Task<RecordMetadata>[] sends = Enumerable.Range(0, 10).Select(i => producer.Send(Record(i), callback)).ToArray();
        Assert.Equal(0, callback.Count);

        // The mock's flush completes every pending send; the flush's own completion is queued on the
        // dispatcher behind the record callbacks, so all of them have run when the await resumes.
        await TestTimeout.Run(() => producer.Flush(), s_deadline);

        Assert.Equal(10, callback.Count);
        Assert.All(sends, s => Assert.True(s.IsCompleted));
    }

    [Fact]
    public void SyncDispose_ReturnsOnlyAfterEveryEarlierCallbackRan()
    {
        AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);
        SlowCallback callback = new SlowCallback(TimeSpan.FromMilliseconds(10));

        Task<RecordMetadata>[] sends = Enumerable.Range(0, 10).Select(i => producer.Send(Record(i), callback)).ToArray();

        // The sync teardown uses the SYNC core flush + close, which now wait for the dispatcher too.
        TestTimeout.Run(producer.Dispose, s_deadline);

        Assert.Equal(10, callback.Count);
        Assert.All(sends, s => Assert.Equal(TaskStatus.RanToCompletion, s.Status));
    }

    [Fact]
    public async Task DisposeAsync_ReturnsOnlyAfterEveryEarlierCallbackRan()
    {
        AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);
        SlowCallback callback = new SlowCallback(TimeSpan.FromMilliseconds(10));

        Task<RecordMetadata>[] sends = Enumerable.Range(0, 10).Select(i => producer.Send(Record(i), callback)).ToArray();
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        Assert.Equal(10, callback.Count);
        Assert.All(sends, s => Assert.Equal(TaskStatus.RanToCompletion, s.Status));
    }

    // ---- Mock completion control ----

    [Fact]
    public async Task ManualMock_CompleteNext_ResolvesTheOldestPendingSend()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);

        Task<RecordMetadata> first = producer.Send(Record(0));
        Task<RecordMetadata> second = producer.Send(Record(1));
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        Assert.True(producer.CompleteNext());
        Assert.Equal(0, (await TestTimeout.Run(() => first, s_deadline)).Offset);
        Assert.False(second.IsCompleted);

        Assert.True(producer.CompleteNext());
        Assert.Equal(1, (await TestTimeout.Run(() => second, s_deadline)).Offset);
        Assert.False(producer.CompleteNext());
    }

    [Fact]
    public async Task ManualMock_ErrorNext_FaultsTheTask_AndCallsBackWithThePlaceholder()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> send = producer.Send(Record(0, partition: 3), callback);
        Assert.True(producer.ErrorNext(RequestTimedOutCode, "broker says no"));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, s_deadline));
        Assert.Equal(RequestTimedOutCode, failure.Code);
        Assert.Equal("broker says no", failure.Message);

        await producer.Flush();
        (RecordMetadata metadata, KafkaException? exception) = Assert.Single(callback.Calls);
        Assert.Same(failure, exception);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(3, metadata.Partition);
        Assert.Equal(-1, metadata.Offset);
    }

    // ---- Failures reported by send() itself: thrown, no callback ----

    [Fact]
    public void Send_OnAClosedCore_ThrowsSynchronously_AndFiresNoCallback()
    {
        using NativeProducer producer = ProducerSendPath.Direct(() => NativeProducer.CreateMock());
        NativeMethods.ProducerClose(producer.Handle.DangerousGetHandle(), out IntPtr closeError);
        _ = KafkaException.FromHandle(closeError);
        RecordingCallback callback = new RecordingCallback();

        // Thrown synchronously out of the call, not through the returned task.
        KafkaException failure = Assert.IsType<KafkaException>(Capture(() => producer.SendViaPump(
            Serialized(0), new DeliveryRegistration(callback, Topic, 0))));

        Assert.Equal(IllegalStateCode, failure.Code);
        Assert.Equal("MockProducer is already closed.", failure.Message);
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task Send_WithAnAlreadyCanceledToken_ThrowsBeforeSending()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock();
        RecordingCallback callback = new RecordingCallback();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        OperationCanceledException canceled = Assert.IsAssignableFrom<OperationCanceledException>(
            Capture(() => producer.Send(Record(0), callback, cts.Token)));

        Assert.Equal(cts.Token, canceled.CancellationToken);
        Assert.Equal(0, producer.HistoryCount());
        await producer.Flush();
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task CancelAfterTheRecordIsAppended_CancelsOnlyTheWait_TheRecordIsStillDelivered()
    {
        await using AsyncMockProducer<byte[], byte[]> producer = DirectMock(autoComplete: false);
        RecordingCallback callback = new RecordingCallback();
        using CancellationTokenSource cts = new CancellationTokenSource();

        Task<RecordMetadata> send = producer.Send(Record(0), callback, cts.Token);
        cts.Cancel();

        OperationCanceledException canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => send, s_deadline));
        Assert.Equal(cts.Token, canceled.CancellationToken);

        Assert.Equal(1, producer.HistoryCount());
        Assert.True(producer.CompleteNext());
        await producer.Flush();
        (RecordMetadata metadata, KafkaException? exception) = Assert.Single(callback.Calls);
        Assert.Null(exception);
        Assert.Equal(0, metadata.Offset);
    }

    // ---- Blocking sends against an unreachable broker ----

    [Fact]
    public async Task Send_WhenMetadataNeverArrives_FaultsAfterMaxBlock_ThroughTheCallback()
    {
        // Java's catch (ApiException) arm: the send returns a failed future and the callback fires with
        // placeholder metadata — it does not throw.
        await using AsyncKafkaProducer<byte[], byte[]> producer = UnreachableProducer(maxBlockMs: 200);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> send = producer.Send(Record(0), callback);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, s_deadline));
        Assert.Equal(RequestTimedOutCode, failure.Code);
        Assert.Equal($"Topic {Topic} not present in metadata after 200 ms.", failure.Message);

        (RecordMetadata metadata, KafkaException? exception) = Assert.Single(callback.Calls);
        Assert.Same(failure, exception);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(-1, metadata.Offset);
    }

    [Fact]
    public async Task CancelWhileBlockedOnMetadata_ThrowsCanceled_NothingSent_NoCallback()
    {
        await using AsyncKafkaProducer<byte[], byte[]> producer = UnreachableProducer(maxBlockMs: 30_000);
        RecordingCallback callback = new RecordingCallback();
        using CancellationTokenSource cts = new CancellationTokenSource();

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<Exception?> sender = Task.Run(() => Capture(() => producer.Send(Record(0), callback, cts.Token)));
        await Task.Delay(s_settle);
        Assert.False(sender.IsCompleted, "the send blocks on metadata");

        cts.Cancel();
        Exception? thrown = await TestTimeout.Run(() => sender, s_prompt);
        elapsed.Stop();

        OperationCanceledException canceled = Assert.IsAssignableFrom<OperationCanceledException>(thrown);
        Assert.Equal(cts.Token, canceled.CancellationToken);
        KafkaException interrupt = Assert.IsType<KafkaException>(canceled.InnerException);
        Assert.Equal(InterruptCode, interrupt.Code);
        Assert.Equal("Send was cancelled while waiting for metadata or buffer memory", interrupt.Message);
        Assert.True(elapsed.Elapsed < s_prompt, $"cancellation took {elapsed.Elapsed}");

        await producer.Flush();
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task WhileOneSendIsBlocked_OtherOperationsProceed_AndOnlyItsOwnTokenCancelsIt()
    {
        await using AsyncKafkaProducer<byte[], byte[]> producer = UnreachableProducer(maxBlockMs: 30_000);
        using CancellationTokenSource first = new CancellationTokenSource();
        using CancellationTokenSource second = new CancellationTokenSource();

        Task<Exception?> blocked = Task.Run(() => Capture(() => producer.Send(Record(0), first.Token)));
        await Task.Delay(s_settle);
        Assert.False(blocked.IsCompleted);

        // Change 1: the parked send holds no producer-wide lock, so these return promptly.
        await TestTimeout.Run(() => producer.Flush(), s_prompt);
        Assert.NotNull(producer.Metrics());

        // A second parked send, cancelled on its own token, leaves the first one parked.
        Task<Exception?> other = Task.Run(() => Capture(() => producer.Send(Record(1), second.Token)));
        await Task.Delay(s_settle);
        second.Cancel();
        Assert.IsAssignableFrom<OperationCanceledException>(await TestTimeout.Run(() => other, s_prompt));
        Assert.False(blocked.IsCompleted, "cancelling the second send left the first parked");

        first.Cancel();
        Assert.IsAssignableFrom<OperationCanceledException>(await TestTimeout.Run(() => blocked, s_prompt));
    }

    [Fact]
    public async Task DisposeAsync_WakesABlockedSend_WhichThrows_AndFiresNoCallback()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = UnreachableProducer(maxBlockMs: 30_000);
        RecordingCallback callback = new RecordingCallback();

        Task<Exception?> sender = Task.Run(() => Capture(() => producer.Send(Record(0), callback)));
        await Task.Delay(s_settle);
        Assert.False(sender.IsCompleted);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_prompt);
        Exception? thrown = await TestTimeout.Run(() => sender, s_prompt);

        KafkaException closed = Assert.IsType<KafkaException>(thrown);
        Assert.Equal("Producer closed while send in progress", closed.Message);
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task SyncDispose_WakesABlockedSend_WhichThrows_AndFiresNoCallback()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = UnreachableProducer(maxBlockMs: 30_000);
        RecordingCallback callback = new RecordingCallback();

        Task<Exception?> sender = Task.Run(() => Capture(() => producer.Send(Record(0), callback)));
        await Task.Delay(s_settle);
        Assert.False(sender.IsCompleted);

        TestTimeout.Run(producer.Dispose, s_prompt);
        Exception? thrown = await TestTimeout.Run(() => sender, s_prompt);

        KafkaException closed = Assert.IsType<KafkaException>(thrown);
        Assert.Equal("Producer closed while send in progress", closed.Message);
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task ConcurrentSendAndDispose_DoesNotHangOrCrash_AndEverySendIsAccountedFor()
    {
        // The direct-path twin of the pump's ConcurrentSendAndDispose test. A send racing teardown
        // either (a) completes normally, (b) is refused by the disposed guard, or (c) passed the guard
        // and reached a core that teardown had just closed — Java's "Cannot perform operation after
        // producer has been closed", a synchronous throw with no callback.
        for (int round = 0; round < 20; round++)
        {
            AsyncMockProducer<byte[], byte[]> producer = DirectMock();
            CountingCallback callback = new CountingCallback();
            int thrownSynchronously = 0;
            ConcurrentBag<Task<RecordMetadata>> accepted = new ConcurrentBag<Task<RecordMetadata>>();

            Task senders = Task.WhenAll(Enumerable.Range(0, 4).Select(t => Task.Run(() =>
            {
                for (int i = 0; i < 200; i++)
                {
                    try
                    {
                        accepted.Add(producer.Send(Record(t * 1000 + i), callback));
                    }
                    catch (ObjectDisposedException)
                    {
                        Interlocked.Increment(ref thrownSynchronously);
                    }
                    catch (KafkaException e) when (e.Code == IllegalStateCode)
                    {
                        Interlocked.Increment(ref thrownSynchronously);
                    }
                }
            })));

            await Task.Delay(1);
            await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
            await TestTimeout.Run(() => senders, s_deadline);

            // Every accepted send settled, and its callback ran exactly once.
            await TestTimeout.Run(() => Task.WhenAll(accepted.Select(s => s.ContinueWith(_ => { }, TaskScheduler.Default))), s_deadline);
            Assert.Equal(accepted.Count, callback.Count);
            Assert.Equal(4 * 200, accepted.Count + thrownSynchronously);
        }
    }

    // ---- Helpers ----

    private static AsyncMockProducer<byte[], byte[]> DirectMock(bool autoComplete = true) =>
        ProducerSendPath.Direct(() => new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete));

    private static AsyncKafkaProducer<byte[], byte[]> UnreachableProducer(int maxBlockMs) =>
        ProducerSendPath.Direct(() => new AsyncKafkaProducer<byte[], byte[]>(
            new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "127.0.0.1:1",
                ["max.block.ms"] = maxBlockMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            },
            Serdes.ByteArray,
            Serdes.ByteArray));

    private static ProducerRecord<byte[], byte[]> Record(int i, int partition = 0) =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: partition);

    private static SerializedProducerRecord Serialized(int i) =>
        new SerializedProducerRecord(Topic, 0, null, null, Encoding.UTF8.GetBytes($"v-{i}"));

    /// <summary>Runs a send on this thread and returns what it threw synchronously, if anything.</summary>
    private static Exception? Capture(Func<Task<RecordMetadata>> send)
    {
        try
        {
            _ = send();
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private sealed class RecordingCallback : IDeliveryCallback
    {
        private readonly ConcurrentQueue<(RecordMetadata, KafkaException?)> _calls =
            new ConcurrentQueue<(RecordMetadata, KafkaException?)>();

        internal int Count => _calls.Count;

        internal IReadOnlyList<(RecordMetadata Metadata, KafkaException? Exception)> Calls => _calls.ToArray();

        internal IEnumerable<KafkaException?> Exceptions => _calls.Select(c => c.Item2);

        internal IEnumerable<long> Offsets => _calls.Select(c => c.Item1.Offset);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception) =>
            _calls.Enqueue((metadata, exception));
    }

    private sealed class CountingCallback : IDeliveryCallback
    {
        private readonly ConcurrentDictionary<string, int> _seen = new ConcurrentDictionary<string, int>();
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int DistinctValues => _seen.Count;

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            Interlocked.Increment(ref _count);
            _seen.AddOrUpdate($"{metadata.Partition}/{metadata.Offset}", 1, (_, n) => n + 1);
        }
    }

    private sealed class SlowCallback : IDeliveryCallback
    {
        private readonly TimeSpan _delay;
        private int _count;

        internal SlowCallback(TimeSpan delay) => _delay = delay;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            Thread.Sleep(_delay);
            Interlocked.Increment(ref _count);
        }
    }

    private sealed class ThrowingCallback : IDeliveryCallback
    {
        public void OnCompletion(RecordMetadata metadata, KafkaException? exception) =>
            throw new InvalidOperationException("user callback bug");
    }

    private sealed class TaskObservingCallback : IDeliveryCallback
    {
        private Task? _task;

        internal ManualResetEventSlim Fired { get; } = new ManualResetEventSlim(false);

        internal bool TaskWasCompletedInsideCallback { get; private set; }

        internal int ThreadId { get; private set; }

        internal void Observe(Task task) => Volatile.Write(ref _task, task);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            TaskWasCompletedInsideCallback = Volatile.Read(ref _task)?.IsCompleted ?? true;
            ThreadId = Environment.CurrentManagedThreadId;
            Fired.Set();
        }
    }
}
