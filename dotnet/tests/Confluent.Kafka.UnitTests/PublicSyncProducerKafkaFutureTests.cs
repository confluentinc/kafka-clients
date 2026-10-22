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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync <see cref="KafkaFuture{T}"/> that <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/>
/// returns: Send returns before delivery, the delivery callback runs on the completion thread before Get returns
/// and is never entered concurrently by one producer, a failure after acceptance surfaces from Get as the same
/// instance, and a discarded future still fires its callback. Every wait is bounded.
/// </summary>
public sealed class PublicSyncProducerKafkaFutureTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-future-topic";

    private const string CompletionThreadName = NativeProducer.CompletionThreadName;

    private readonly ITestOutputHelper _output;

    public PublicSyncProducerKafkaFutureTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Send_ReturnsBeforeDelivery_OnAManualMock_ThenGetAfterCompleteNext()
    {
        // One thread sends, completes and waits: this deadlocks if Send waited for delivery.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();
        try
        {
            TestTimeout.Run(
                () =>
                {
                    KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);

                    Assert.False(IsDone(future), "the future was done before the mock completed its send");
                    Assert.Equal(0, callback.Count);

                    Assert.True(producer.CompleteNext(), "Send returned without registering a pending send");
                    RecordMetadata metadata = future.Get();

                    // The callback runs before Get returns, as Java's, with the metadata Get returns.
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

    [Fact]
    public void Callback_Sync_RunsOnThePumpThread_NotTheCaller()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        int callerThreadId = 0;
        TestTimeout.Run(
            () =>
            {
                callerThreadId = Environment.CurrentManagedThreadId;
                _ = producer.Send(Record(), callback).Get();
            },
            s_deadline);

        Assert.Equal(1, callback.Count);
        Assert.Equal(CompletionThreadName, callback.ThreadName);
        Assert.NotEqual(callerThreadId, callback.ThreadId);
    }

    [Fact]
    public async Task Callback_Sync_SharedInstance_IsNeverEnteredConcurrently_ByOneProducer()
    {
        // N threads x M sends through one producer and one callback instance; the probe spins inside the callback,
        // so an overlap would be seen.
        const int Threads = 8;
        const int SendsPerThread = 50;

        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
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

    [Fact]
    public void ApiFailure_Sync_SendReturns_CallbackFires_GetThrowsTheSameInstance()
    {
        // A real producer whose bootstrap refuses connections: the metadata wait times out after max.block.ms, and the
        // failure reaches the callback and Get rather than Send, as Java's doSend catch (ApiException) does.
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

            // Unwrapped, and the same instance.
            KafkaException failure = Assert.IsAssignableFrom<KafkaException>(first);
            Assert.Same(first, second);
            Assert.Equal($"Topic {ApiTopic} not present in metadata after 200 ms.", failure.Message);
            Assert.Equal(7, failure.Code);

            // The callback fired once, on the completion thread, before Get returned, with Java's placeholder metadata.
            Assert.Equal(1, callback.Count);
            Assert.Same(failure, callback.Exception);
            Assert.Equal(CompletionThreadName, callback.ThreadName);
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

    [Fact]
    public void SendInsideCallback_GetFromAnotherThread_Completes()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ReentrantSendCallback callback = new ReentrantSendCallback(() => producer.Send(Record()));

        TestTimeout.Run(
            () =>
            {
                RecordMetadata first = producer.Send(Record(), callback).Get();

                // The callback ran on the completion thread before Get returned, so its re-entrant send has returned.
                Assert.Null(callback.SendFailure);
                KafkaFuture<RecordMetadata> reentrant = callback.Reentrant;
                Assert.NotEqual(default, reentrant);
                Assert.Equal(CompletionThreadName, callback.ThreadName);

                // Waited on from this thread, not the completion thread, so it completes.
                RecordMetadata second = reentrant.Get();
                Assert.Equal(0L, first.Offset);
                Assert.Equal(1L, second.Offset);
            },
            s_deadline);
    }

    // A manual mock: Dispose flushes first, completing a pending send; Close leaves it pending, as Java's
    // MockProducer.close() does.

    [Fact]
    public void Dispose_WithAPendingFuture_CompletesIt()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);

        TestTimeout.Run(producer.Dispose, s_deadline);

        Assert.True(future.IsDone);
        Assert.Equal(0L, future.Get(s_deadline).Offset);
        Assert.Equal(1, callback.Count);
    }

    [Fact]
    public void Close_WithAPendingFuture_LeavesItPending()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);

        TestTimeout.Run(producer.Close, s_deadline);

        Assert.Throws<TimeoutException>(() => future.Get(TimeSpan.FromMilliseconds(200)));
        Assert.False(future.IsDone);
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public void SendAfterDispose_ThrowsObjectDisposed_AndStartsNoPump()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        ThreadRecordingCallback callback = new ThreadRecordingCallback();

        TestTimeout.Run(producer.Dispose, s_deadline);

        ObjectDisposedException plain = Assert.Throws<ObjectDisposedException>(() => producer.Send(Record()));
        ObjectDisposedException withCallback =
            Assert.Throws<ObjectDisposedException>(() => producer.Send(Record(), callback));

        string expected = new ObjectDisposedException(nameof(NativeProducer)).Message;
        Assert.Equal(expected, plain.Message);
        Assert.Equal(expected, withCallback.Message);
        Assert.Equal(nameof(NativeProducer), plain.ObjectName);

        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task RacingFirstSendAndDispose_NeverLeavesARunningPump()
    {
        // Each iteration releases a fresh producer's first Send and its Dispose together; the skew spreads the
        // interleavings.
        const int Iterations = 200;

        // A Send past the closed check when Dispose released the handle is refused by the marshaller's
        // DangerousAddRef; the expected text is read from a released handle on the runtime under test.
        string releasedHandle = ReleasedSafeProducerHandleMessage();
        int sent = 0;
        int disposed = 0;
        int coreClosed = 0;

        for (int i = 0; i < Iterations; i++)
        {
            MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
                        // The closed check (NativeProducer) or the marshaller's refusal of the released handle.
                        Assert.True(
                            exception.Message == new ObjectDisposedException(nameof(NativeProducer)).Message
                                || exception.Message == releasedHandle,
                            $"unexpected ObjectDisposedException message: {exception.Message}");
                        return "disposed";
                    }
                    catch (InvalidOperationException exception)
                    {
                        // Past the closed check, but the core was closed when the record reached it.
                        Assert.Equal("MockProducer is already closed.", exception.Message);
                        return "core-closed";
                    }

                    // Accepted: the future resolves; it never strands.
                    _ = future.Get();

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

        }

        _output.WriteLine(
            $"C11 outcomes over {Iterations} iterations: sent={sent}, disposed={disposed}, core-closed={coreClosed}");
    }

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

        // The completion thread is joined, so the count is final: exactly once each.
        Assert.Equal(Sends, callback.Count);
        Assert.Equal(0, callback.Failures);
    }

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0);

    private static bool IsDone(KafkaFuture<RecordMetadata> future) => future.IsDone;

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

    // The message DangerousAddRef throws for a released SafeHandle on this runtime. The probe is created at
    // IntPtr.Zero, so its Dispose never reaches ReleaseHandle and makes no native call.
    private static string ReleasedSafeProducerHandleMessage()
    {
        System.Runtime.InteropServices.SafeHandle handle =
            new Confluent.Kafka.Internal.Interop.SafeMockProducerHandle(IntPtr.Zero, IntPtr.Zero);
        if (!handle.IsInvalid)
        {
            handle.SetHandleAsInvalid();
            throw new InvalidOperationException("the probe SafeProducerHandle was created valid");
        }

        handle.Dispose();
        bool added = false;
        try
        {
            handle.DangerousAddRef(ref added);
        }
        catch (ObjectDisposedException exception)
        {
            return exception.Message;
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }
        }

        throw new InvalidOperationException("a released SafeProducerHandle accepted DangerousAddRef");
    }

    /// <summary>Records each invocation's arguments and the thread it ran on. Asserts nothing itself.</summary>
    private sealed class ThreadRecordingCallback : IDeliveryCallback
    {
        private readonly object _gate = new object();

        private int _count;

        private RecordMetadata? _metadata;

        private Exception? _exception;

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

        internal Exception? Exception
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

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
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

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
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

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
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
