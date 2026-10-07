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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P4.2 S4 — the callback and error semantics of the sync <see cref="KafkaFuture{T}"/> that
/// <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/> returns (PLAN §7, C1–C12;
/// C3 extends <c>PublicProducerDeliveryCallbackTests.Ordering_Sync_*</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>What is pinned.</b> <c>Send</c> returns once the core has accepted the record (D13); the delivery
/// callback runs on the producer's send-completion pump, never on the caller (D3), and one producer
/// never enters a shared callback instance concurrently; a failure after acceptance surfaces from
/// <see cref="KafkaFuture{T}.Get"/> after the callback fired, as the same instance on every call (D2);
/// a <c>Get</c> on the pump thread for a pending send of the same producer throws rather than deadlocks
/// (D9); the sync-only producer starts exactly one thread on its first send and joins it at dispose
/// (D4); and a discarded future still fires its callback (D8 (b)).
/// </para>
/// <para>
/// <b>Every wait is bounded</b> (<see cref="TestTimeout"/>, or an explicit deadline), so a regression —
/// in particular the D9 guard's removal, which would deadlock the pump — fails rather than hangs. The
/// pump-thread and accumulator observations go through <see cref="NativeProducer"/>'s read-only test
/// probes (<see cref="NativeProducer.StartedPumpThread"/>, <see cref="NativeProducer.AccumulatorStarted"/>),
/// reached through the mock's private <c>_native</c> field as <c>PublicProducerFirstStageTests</c> does.
/// </para>
/// </remarks>
public sealed class PublicSyncProducerKafkaFutureTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-future-topic";

    private const string PumpThreadName = "confluent-kafka-producer-send-pump";

    // D9's message, verbatim (PLAN §2 D9) — asserted exactly, so a reworded guard fails here.
    private const string GetOnOwningPumpMessage =
        "KafkaFuture.Get() was called on this producer's send-completion thread — from inside a delivery callback — " +
        "for a send that has not completed; that would deadlock. Wait for it from another thread.";

    // The pump's teardown fault (residuals 1 and 2), verbatim.
    private const string TeardownMessage = "The producer was closed before the send completed.";

    private readonly ITestOutputHelper _output;

    public PublicSyncProducerKafkaFutureTests(ITestOutputHelper output)
    {
        _output = output;
    }

    // ---- C1 (D13): Send returns before delivery; one thread sends, completes, then waits ----

    [Fact]
    public void Send_ReturnsBeforeDelivery_OnAManualMock_ThenGetAfterCompleteNext()
    {
        // The single-thread mock pattern D13 adopts: no second thread is blocked in Get, so this
        // sequence deadlocks if Send still waited for delivery. It runs on ONE pool thread, bounded.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();
        try
        {
            TestTimeout.Run(
                () =>
                {
                    KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);

                    // The manual mock has not completed the record, so nothing can have read it yet.
                    Assert.False(IsDone(future), "the future was done before the mock completed its send");
                    Assert.Equal(0, callback.Count);

                    Assert.True(producer.CompleteNext(), "Send returned without registering a pending send");
                    RecordMetadata metadata = future.Get();

                    // The callback ran before Get returned (Java's set → callbacks → open order), with
                    // the very metadata Get returns.
                    Assert.Equal(1, callback.Count);
                    Assert.Same(metadata, callback.Metadata);
                    Assert.Null(callback.Exception);
                    Assert.Equal(Topic, metadata.Topic);
                    Assert.Equal(0, metadata.Partition);
                    Assert.Equal(0L, metadata.Offset);
                },
                s_deadline);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // ---- C2 (D3): the callback runs on the pump thread, not the caller ----

    [Fact]
    public void Callback_Sync_RunsOnThePumpThread_NotTheCaller()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        int callerThreadId = 0;
        TestTimeout.Run(
            () =>
            {
                callerThreadId = Environment.CurrentManagedThreadId;
                _ = producer.Send(Record(), callback).Get();
            },
            s_deadline);

        Thread pump = NativeOf(producer).StartedPumpThread
            ?? throw new XunitException("the sync Send did not start a send-completion pump");

        Assert.Equal(1, callback.Count);
        Assert.Equal(PumpThreadName, callback.ThreadName);
        Assert.NotEqual(callerThreadId, callback.ThreadId);
        Assert.Equal(pump.ManagedThreadId, callback.ThreadId);
    }

    // ---- C4 (D3): one producer never enters a shared callback instance concurrently ----

    [Fact]
    public async Task Callback_Sync_SharedInstance_IsNeverEnteredConcurrently_ByOneProducer()
    {
        // The retired sync sub-divergence (one instance entered on N caller threads at once), turned into
        // a guarantee: N threads × M sends through ONE producer and ONE callback instance. The probe spins
        // inside the callback so an overlap — two threads inside at once — would actually be observed.
        const int Threads = 8;
        const int SendsPerThread = 50;

        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        ConcurrencyProbeCallback callback = new ConcurrencyProbeCallback();
        using Barrier start = new Barrier(Threads);

        Task[] senders = new Task[Threads];
        for (int t = 0; t < Threads; t++)
        {
            senders[t] = Task.Factory.StartNew(
                () =>
                {
                    if (!start.SignalAndWait(s_deadline))
                    {
                        throw new TimeoutException("the sender threads did not all start");
                    }

                    KafkaFuture<RecordMetadata>[] futures = new KafkaFuture<RecordMetadata>[SendsPerThread];
                    for (int i = 0; i < SendsPerThread; i++)
                    {
                        futures[i] = producer.Send(Record(), callback);
                    }

                    foreach (KafkaFuture<RecordMetadata> future in futures)
                    {
                        _ = future.Get();
                    }
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        await TestTimeout.Run(() => Task.WhenAll(senders), s_deadline);

        Assert.Equal(Threads * SendsPerThread, callback.Count);
        Assert.Equal(0, callback.Failures);
        Assert.Equal(1, callback.MaxConcurrency);
    }

    // ---- C5 (D2): an ApiException after acceptance — Send returns, the callback fires, Get throws ----

    [Fact]
    public void ApiFailure_Sync_SendReturns_CallbackFires_GetThrowsTheSameInstance()
    {
        // The core's ApiException path — a FAILED future with a null out_error (PLAN F7) — reached
        // broker-free through a REAL producer: its bootstrap refuses connections, so the send waits
        // max.block.ms for metadata and the resulting timeout (an ApiException in Java, is_api_error in
        // the core) comes back as a failed future, Java doSend's `catch (ApiException)` (callback +
        // `return new FutureFailure(e)`) — not as a synchronous out_error. So Send itself must NOT throw.
        const string ApiTopic = "sync-future-api-failure-topic";
        Dictionary<string, string> config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["max.block.ms"] = "200",
        };

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();
        try
        {
            KafkaFuture<RecordMetadata> future = default;
            TestTimeout.Run(
                () => future = producer.Send(
                    new ProducerRecord<byte[], byte[]>(ApiTopic, Encoding.UTF8.GetBytes("v")), callback),
                s_deadline);

            Exception first = null!;
            Exception second = null!;
            TestTimeout.Run(() => first = Capture(() => future.Get()), s_deadline);
            TestTimeout.Run(() => second = Capture(() => future.Get()), s_deadline);

            // Unwrapped (no AggregateException / ExecutionException analog) and the SAME instance.
            KafkaException failure = Assert.IsAssignableFrom<KafkaException>(first);
            Assert.Same(first, second);
            Assert.Equal($"Topic {ApiTopic} not present in metadata after 200 ms.", failure.Message);
            Assert.Equal(7, failure.Code);

            // The callback fired once, on the pump, before Get returned, with the same failure and Java's
            // placeholder metadata (topic, -1 partition since the record chose none, -1 offset).
            Assert.Equal(1, callback.Count);
            Assert.Same(failure, callback.Exception);
            Assert.Equal(PumpThreadName, callback.ThreadName);
            RecordMetadata placeholder = callback.Metadata ?? throw new XunitException("the callback saw null metadata");
            Assert.Equal(ApiTopic, placeholder.Topic);
            Assert.Equal(-1, placeholder.Partition);
            Assert.Equal(-1L, placeholder.Offset);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // ---- C6 (D9): Get inside a callback on the same producer throws; the send still completes ----

    [Fact]
    public void GetInsideCallback_SameProducer_ThrowsInvalidOperation_AndTheSendStillCompletes()
    {
        // The first record's callback calls Get on the SECOND record's future, which the manual mock has
        // not completed. Without the guard that Get blocks the pump — the only thread that could ever
        // complete it — so the first Get never returns either: TestTimeout turns that into a failure, and
        // the dispose in `finally` is bounded too, so the deadlock fails the test rather than hanging it.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        GetOtherFutureCallback callback = new GetOtherFutureCallback();
        try
        {
            TestTimeout.Run(
                () =>
                {
                    KafkaFuture<RecordMetadata> first = producer.Send(Record(), callback);
                    KafkaFuture<RecordMetadata> second = producer.Send(Record());
                    callback.Other = second;

                    Assert.True(producer.CompleteNext(), "no pending send to complete");
                    RecordMetadata firstMetadata = first.Get();

                    Assert.Equal(1, callback.Count);
                    InvalidOperationException guard = Assert.IsType<InvalidOperationException>(callback.Captured);
                    Assert.Equal(GetOnOwningPumpMessage, guard.Message);
                    Assert.Equal(PumpThreadName, callback.ThreadName);
                    Assert.Equal(0L, firstMetadata.Offset);

                    // The guarded send was not harmed: it completes normally once the mock completes it.
                    Assert.True(producer.CompleteNext(), "the second send was not pending");
                    Assert.Equal(1L, second.Get().Offset);
                },
                s_deadline);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // ---- C7: a send from inside a callback, waited on from another thread ----

    [Fact]
    public void SendInsideCallback_GetFromAnotherThread_Completes()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        ReentrantSendCallback callback = new ReentrantSendCallback(() => producer.Send(Record()));

        TestTimeout.Run(
            () =>
            {
                RecordMetadata first = producer.Send(Record(), callback).Get();

                // The callback ran on the pump before Get returned, so its re-entrant send has returned.
                Assert.Null(callback.SendFailure);
                KafkaFuture<RecordMetadata> reentrant = callback.Reentrant;
                Assert.NotEqual(default, reentrant);
                Assert.Equal(PumpThreadName, callback.ThreadName);

                // Waited on from THIS thread, not the pump: completes normally.
                RecordMetadata second = reentrant.Get();
                Assert.Equal(0L, first.Offset);
                Assert.Equal(1L, second.Offset);
            },
            s_deadline);
    }

    // ---- C8: Dispose with a pending future releases a blocked Get ----

    [Fact]
    public async Task Dispose_WithAPendingFuture_ReleasesGet()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);
        Thread? waiter = null;
        Task<RecordMetadata> blocked = Task.Factory.StartNew(
            () =>
            {
                Volatile.Write(ref waiter, Thread.CurrentThread);
                return future.Get();
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        // Let the waiter reach its blocking wait first (bounded), so teardown races a Get that is
        // actually parked rather than one not yet called.
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!IsParked(Volatile.Read(ref waiter)) && elapsed.Elapsed < s_deadline)
        {
            Thread.Yield();
        }

        Assert.True(IsParked(Volatile.Read(ref waiter)), "the waiter never parked in Get");
        Assert.False(blocked.IsCompleted, "Get returned before the mock completed the send");

        TestTimeout.Run(producer.Dispose, s_deadline);

        RecordMetadata? result = null;
        KafkaException? teardown = null;
        try
        {
            result = await TestTimeout.Run(() => blocked, s_deadline);
        }
        catch (KafkaException exception)
        {
            teardown = exception;
        }

        // The callback fired if and only if the completion was read. Teardown's flush resolves the
        // manual mock's pending send, so the pump reads it (result + callback) unless teardown faulted
        // the send while still queued (residual 2: the teardown message, and no callback). The pump is
        // joined by now, so a callback cannot still arrive later.
        if (result is not null)
        {
            Assert.Equal(1, callback.Count);
            Assert.Same(result, callback.Metadata);
            Assert.Null(callback.Exception);
            Assert.Equal(0L, result.Offset);
            _output.WriteLine("C8 outcome: the completion was read (result + callback).");
        }
        else
        {
            Assert.NotNull(teardown);
            Assert.Equal(TeardownMessage, teardown!.Message);
            Assert.Equal(0, callback.Count);
            _output.WriteLine("C8 outcome: teardown faulted the queued send (teardown message, no callback).");
        }
    }

    // ---- C9 (D4): the sync-only producer starts the pump on its first Send and joins it at Dispose ----

    [Fact]
    public void SyncOnlyProducer_StartsThePumpOnFirstSend_AndJoinsItAtDispose()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        NativeProducer native = NativeOf(producer);

        // Lazy (D4 (a)): the constructor starts no thread.
        Assert.Null(native.StartedPumpThread);
        Assert.False(native.AccumulatorStarted);

        TestTimeout.Run(() => _ = producer.Send(Record()).Get(), s_deadline);

        Thread pump = native.StartedPumpThread
            ?? throw new XunitException("the first sync Send did not start a send-completion pump");
        Assert.True(pump.IsAlive, "the started pump's thread is not running");
        Assert.Equal(PumpThreadName, pump.Name);

        // Exactly one thread: a later send reuses the same pump, and the sync surface never starts the
        // send accumulator's send-batch thread.
        TestTimeout.Run(() => _ = producer.Send(Record()).Get(), s_deadline);
        Assert.Same(pump, native.StartedPumpThread);
        Assert.False(native.AccumulatorStarted, "the sync surface started the send accumulator");

        TestTimeout.Run(producer.Dispose, s_deadline);

        // Joined: Stop's Join has returned, so the thread has terminated.
        Assert.False(pump.IsAlive, "Dispose did not join the sync-only producer's pump");
        Assert.False(native.AccumulatorStarted);
    }

    // ---- C10: Send after Dispose throws ObjectDisposedException and starts no pump ----

    [Fact]
    public void SendAfterDispose_ThrowsObjectDisposed_AndStartsNoPump()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        NativeProducer native = NativeOf(producer);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        TestTimeout.Run(producer.Dispose, s_deadline);

        ObjectDisposedException plain = Assert.Throws<ObjectDisposedException>(() => producer.Send(Record()));
        ObjectDisposedException withCallback =
            Assert.Throws<ObjectDisposedException>(() => producer.Send(Record(), callback));

        string expected = new ObjectDisposedException(nameof(NativeProducer)).Message;
        Assert.Equal(expected, plain.Message);
        Assert.Equal(expected, withCallback.Message);
        Assert.Equal(nameof(NativeProducer), plain.ObjectName);

        Assert.Null(native.StartedPumpThread);
        Assert.False(native.AccumulatorStarted);
        Assert.Equal(0, callback.Count);
    }

    // ---- C10 (seam): the pump's creation path itself refuses once the close latch is won ----

    [Fact]
    public void EnsurePump_AfterTheCloseLatch_Throws_AndStartsNoPump()
    {
        // C10 cannot see EnsurePump's re-check under _pumpLock, because Send's leading ThrowIfClosed throws
        // first; C11 sees it only when a Dispose lands between the two checks, which it does not do in every
        // run. Calling the creation path directly on a producer that never sent and is already disposed makes
        // a creation path that skips the re-check start a pump here, every time.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        NativeProducer native = NativeOf(producer);

        TestTimeout.Run(producer.Dispose, s_deadline);

        ObjectDisposedException refused = Assert.Throws<ObjectDisposedException>(() => native.EnsurePump());

        Assert.Equal(new ObjectDisposedException(nameof(NativeProducer)).Message, refused.Message);
        Assert.Equal(nameof(NativeProducer), refused.ObjectName);
        Assert.Null(native.StartedPumpThread);
        Assert.False(native.AccumulatorStarted);
    }

    // ---- C11: a first Send racing Dispose never leaves a running pump ----

    [Fact]
    public async Task RacingFirstSendAndDispose_NeverLeavesARunningPump()
    {
        // The first Send creates the pump, so it is the one that can race teardown into leaving a pump
        // nobody joins (EnsurePump's ThrowIfClosed under _pumpLock is what forbids it). Each iteration
        // releases a fresh producer's first Send and its Dispose together; the skew spreads the
        // interleavings across iterations.
        const int Iterations = 200;
        string safeHandleClosed = SafeHandleClosedMessage();

        // The P/Invoke marshaller's own refusal of the released SafeProducerHandle that Send passes to
        // Producer_send: on net8.0 and net10.0 it names the handle's type, and it is not DangerousAddRef's
        // message above (observed by mutating EnsurePump's re-check away, M11/P4.2 S4m). DangerousAddRef's
        // stays accepted because this project also builds for net462, where the marshaller was not measured.
        string releasedHandle = new ObjectDisposedException(
            typeof(Confluent.Kafka.Internal.Interop.SafeProducerHandle).FullName).Message;
        int sent = 0;
        int disposed = 0;
        int coreClosed = 0;

        for (int i = 0; i < Iterations; i++)
        {
            MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            NativeProducer native = NativeOf(producer);
            using Barrier start = new Barrier(2);
            int skew = (i % 16) * 32;
            bool skewSender = i % 2 == 0;

            Task<string> sender = Task.Factory.StartNew(
                () =>
                {
                    WaitAtStart(start);
                    if (skewSender)
                    {
                        Thread.SpinWait(skew);
                    }

                    KafkaFuture<RecordMetadata> future;
                    try
                    {
                        future = producer.Send(Record());
                    }
                    catch (ObjectDisposedException exception)
                    {
                        // The closed check (NativeProducer) or the handle's own guard (released handle).
                        Assert.True(
                            exception.Message == new ObjectDisposedException(nameof(NativeProducer)).Message
                                || exception.Message == releasedHandle
                                || exception.Message == safeHandleClosed,
                            $"unexpected ObjectDisposedException message: {exception.Message}");
                        return "disposed";
                    }
                    catch (KafkaException exception)
                    {
                        // The latch was not yet won at the closed check, but the core was closed by the
                        // time the record reached it: the core's own closed rejection, before acceptance.
                        Assert.Equal("MockProducer is already closed.", exception.Message);
                        return "core-closed";
                    }

                    // Accepted: the future resolves (read, or faulted by teardown) — it never strands.
                    try
                    {
                        _ = future.Get();
                    }
                    catch (KafkaException exception)
                    {
                        Assert.Equal(TeardownMessage, exception.Message);
                    }

                    return "sent";
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);

            Task disposer = Task.Factory.StartNew(
                () =>
                {
                    WaitAtStart(start);
                    if (!skewSender)
                    {
                        Thread.SpinWait(skew);
                    }

                    producer.Dispose();
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);

            await TestTimeout.Run(() => Task.WhenAll(sender, disposer), s_deadline);

            switch (await sender)
            {
                case "sent":
                    sent++;
                    break;
                case "disposed":
                    disposed++;
                    break;
                default:
                    coreClosed++;
                    break;
            }

            Thread? pump = native.StartedPumpThread;
            Assert.True(
                pump is null || !pump.IsAlive,
                $"iteration {i}: a send-completion pump was left running after Dispose returned");
            Assert.False(native.AccumulatorStarted, $"iteration {i}: the sync surface started the accumulator");
        }

        _output.WriteLine(
            $"C11 outcomes over {Iterations} iterations: sent={sent}, disposed={disposed}, core-closed={coreClosed}");
    }

    // ---- C12 (D8 (b)): discarded futures still fire every callback ----

    [Fact]
    public async Task FireAndForget_DiscardedFutures_StillFireEveryCallback()
    {
        const int Sends = 200;
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        ConcurrencyProbeCallback callback = new ConcurrencyProbeCallback();
        try
        {
            TestTimeout.Run(
                () =>
                {
                    for (int i = 0; i < Sends; i++)
                    {
                        // Discarded on purpose: fire-and-forget is the point of this test.
                        _ = producer.Send(Record(), callback);
                    }

                    producer.Flush();
                },
                s_deadline);

            // D8 (b): the callbacks may trail Flush by the pump's lag, so wait for them — bounded.
            Stopwatch elapsed = Stopwatch.StartNew();
            while (callback.Count < Sends && elapsed.Elapsed < s_deadline)
            {
                await Task.Delay(2);
            }

            Assert.Equal(Sends, callback.Count);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }

        // The pump is joined: no callback can still arrive, so the count is final — exactly once each.
        Assert.Equal(Sends, callback.Count);
        Assert.Equal(0, callback.Failures);
    }

    // ---- Helpers ----

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0);

    /// <summary>The mock's own <see cref="NativeProducer"/>, for its read-only test probes.</summary>
    private static NativeProducer NativeOf(MockProducer<byte[], byte[]> producer)
    {
        FieldInfo nativeField = typeof(MockProducer<byte[], byte[]>).GetField(
                "_native", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("MockProducer._native was renamed; update this test");
        return (NativeProducer)nativeField.GetValue(producer)!;
    }

    /// <summary>Whether the future's latch has completed — its internal state, read without blocking.</summary>
    private static bool IsDone(KafkaFuture<RecordMetadata> future)
    {
        FieldInfo completionField = typeof(KafkaFuture<RecordMetadata>).GetField(
                "_completion", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("KafkaFuture._completion was renamed; update this test");
        SyncCompletion<RecordMetadata> completion = (SyncCompletion<RecordMetadata>?)completionField.GetValue(future)
            ?? throw new InvalidOperationException("the future is a default value");
        return completion.IsDone;
    }

    private static bool IsParked(Thread? thread) =>
        thread is not null && (thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0;

    private static void WaitAtStart(Barrier start)
    {
        if (!start.SignalAndWait(s_deadline))
        {
            throw new TimeoutException("the racing threads did not both start");
        }
    }

    private static Exception Capture(Action action)
    {
        try
        {
            action();
        }
        catch (Exception exception)
        {
            return exception;
        }

        throw new XunitException("expected the call to throw");
    }

    /// <summary>The runtime's own message for a SafeHandle used after release.</summary>
    private static string SafeHandleClosedMessage()
    {
        using Microsoft.Win32.SafeHandles.SafeWaitHandle handle =
            new Microsoft.Win32.SafeHandles.SafeWaitHandle(IntPtr.Zero, ownsHandle: false);
        handle.Dispose();
        try
        {
            bool added = false;
            handle.DangerousAddRef(ref added);
        }
        catch (ObjectDisposedException exception)
        {
            return exception.Message;
        }

        throw new InvalidOperationException("a released SafeHandle accepted DangerousAddRef");
    }

    // ---- Fixtures ----

    /// <summary>Records each invocation's arguments and the thread it ran on. Asserts nothing itself.</summary>
    private sealed class ThreadRecordingCallback : IDeliveryCallback
    {
        private readonly object _gate = new object();

        private int _count;

        private RecordMetadata? _metadata;

        private KafkaException? _exception;

        private int _threadId;

        private string? _threadName;

        internal int Count
        {
            get
            {
                lock (_gate)
                {
                    return _count;
                }
            }
        }

        internal RecordMetadata? Metadata
        {
            get
            {
                lock (_gate)
                {
                    return _metadata;
                }
            }
        }

        internal KafkaException? Exception
        {
            get
            {
                lock (_gate)
                {
                    return _exception;
                }
            }
        }

        internal int ThreadId
        {
            get
            {
                lock (_gate)
                {
                    return _threadId;
                }
            }
        }

        internal string? ThreadName
        {
            get
            {
                lock (_gate)
                {
                    return _threadName;
                }
            }
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            lock (_gate)
            {
                _count++;
                _metadata = metadata;
                _exception = exception;
                _threadId = Environment.CurrentManagedThreadId;
                _threadName = Thread.CurrentThread.Name;
            }
        }
    }

    /// <summary>
    /// Counts invocations and the most threads ever inside it at once. The spin widens each
    /// invocation's window so two overlapping callers would be seen; it is not a correctness wait.
    /// </summary>
    private sealed class ConcurrencyProbeCallback : IDeliveryCallback
    {
        private const int SpinIterations = 10000;

        private int _inside;

        private int _maxConcurrency;

        private int _count;

        private int _failures;

        internal int Count => Volatile.Read(ref _count);

        internal int Failures => Volatile.Read(ref _failures);

        internal int MaxConcurrency => Volatile.Read(ref _maxConcurrency);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            int inside = Interlocked.Increment(ref _inside);
            int seen;
            while (inside > (seen = Volatile.Read(ref _maxConcurrency))
                && Interlocked.CompareExchange(ref _maxConcurrency, inside, seen) != seen)
            {
            }

            Thread.SpinWait(SpinIterations);

            if (exception is not null)
            {
                Interlocked.Increment(ref _failures);
            }

            Interlocked.Increment(ref _count);
            Interlocked.Decrement(ref _inside);
        }
    }

    /// <summary>Calls <see cref="KafkaFuture{T}.Get"/> on another send's future from inside the callback.</summary>
    private sealed class GetOtherFutureCallback : IDeliveryCallback
    {
        private readonly object _gate = new object();

        private KafkaFuture<RecordMetadata> _other;

        private Exception? _captured;

        private int _count;

        private string? _threadName;

        internal KafkaFuture<RecordMetadata> Other
        {
            set
            {
                lock (_gate)
                {
                    _other = value;
                }
            }
        }

        internal Exception? Captured
        {
            get
            {
                lock (_gate)
                {
                    return _captured;
                }
            }
        }

        internal int Count
        {
            get
            {
                lock (_gate)
                {
                    return _count;
                }
            }
        }

        internal string? ThreadName
        {
            get
            {
                lock (_gate)
                {
                    return _threadName;
                }
            }
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            KafkaFuture<RecordMetadata> other;
            lock (_gate)
            {
                _count++;
                _threadName = Thread.CurrentThread.Name;
                other = _other;
            }

            // Outside the lock: without the D9 guard this blocks the pump for good.
            Exception? captured = null;
            try
            {
                _ = other.Get();
            }
            catch (Exception caught)
            {
                captured = caught;
            }

            lock (_gate)
            {
                _captured = captured;
            }
        }
    }

    /// <summary>Sends again from inside the callback and keeps the re-entrant send's future.</summary>
    private sealed class ReentrantSendCallback : IDeliveryCallback
    {
        private readonly object _gate = new object();

        private readonly Func<KafkaFuture<RecordMetadata>> _send;

        private KafkaFuture<RecordMetadata> _reentrant;

        private Exception? _sendFailure;

        private string? _threadName;

        internal ReentrantSendCallback(Func<KafkaFuture<RecordMetadata>> send)
        {
            _send = send;
        }

        internal KafkaFuture<RecordMetadata> Reentrant
        {
            get
            {
                lock (_gate)
                {
                    return _reentrant;
                }
            }
        }

        internal Exception? SendFailure
        {
            get
            {
                lock (_gate)
                {
                    return _sendFailure;
                }
            }
        }

        internal string? ThreadName
        {
            get
            {
                lock (_gate)
                {
                    return _threadName;
                }
            }
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            KafkaFuture<RecordMetadata> reentrant = default;
            Exception? failure = null;
            try
            {
                reentrant = _send();
            }
            catch (Exception caught)
            {
                failure = caught;
            }

            lock (_gate)
            {
                _reentrant = reentrant;
                _sendFailure = failure;
                _threadName = Thread.CurrentThread.Name;
            }
        }
    }
}
