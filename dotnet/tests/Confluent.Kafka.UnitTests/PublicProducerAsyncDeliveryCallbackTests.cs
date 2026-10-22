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

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async delivery callback of <see cref="AsyncKafkaProducer{TKey, TValue}"/>: it starts on the pump, the
/// producer's later completions wait behind its task, the record's future completes after that task, and the waits
/// called around it or from inside it after an <c>await</c>. Each record fails on an unreachable broker after
/// <c>max.block.ms</c>, the sends spaced so that they fail in send order.
/// </summary>
public sealed class PublicProducerAsyncDeliveryCallbackTests
{
    private const int MaxBlockMs = 500;

    private const int SpacingMs = 150;

    private const int LaterWaitMs = 300;

    private const string FlushMessage =
        "KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.";

    private const string CloseOverride =
        "Overriding close timeout 9223372036854775807 ms to 0 ms in order to prevent useless blocking due to " +
        "self-join. This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.";

    private const string Close = "Close";
    private const string Dispose = "Dispose";
    private const string DisposeAsync = "DisposeAsync";

    private const string AfterAnAwait = "after an await";
    private const string BeforeTheFirstAwait = "before the first await";

    private const string AsyncKind = "async";
    private const string SyncKind = "sync";
    private const string NoCallback = "none";

    private const string Faulted = "faulted";
    private const string Canceled = "canceled";
    private const string Throwing = "throwing";

    [Fact]
    public async Task AsyncDeliveryCallback_StartsOnThePumpThread_AndTheFutureCompletesAfterItsTask()
    {
        TaskCompletionSource<bool> gate = NewGate();
        AsyncProbe callback = new AsyncProbe(_ => gate.Task);
        string topic = Topic("async-callback-start");
        await WithProducer(
            async producer =>
            {
                AsyncKafkaFuture<RecordMetadata> future = await FirstStage(producer.Send(Record(topic), callback));
                await Within(callback.WhenStarted);
                TaskCompletionSource<bool> endedWhenTheFutureCompleted = NewGate();
                _ = future.WhenComplete((_, _) => endedWhenTheFutureCompleted.TrySetResult(callback.Ended));

                await Task.Delay(Settle);
                Assert.Equal(NativeProducer.PumpThreadName, callback.StartThread);
                Assert.Equal(1, callback.Calls);
                Assert.False(future.IsDone);

                gate.SetResult(true);
                KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(future.Get()));
                Assert.True(await Within(endedWhenTheFutureCompleted.Task), "the future completed before the callback's task ended");
                Assert.Equal(7, failure.Code);
                Assert.Equal(MetadataTimeout(topic, MaxBlockMs), failure.Message);
                Assert.Same(failure, callback.Delivered);
                Assert.Equal(topic, callback.Metadata!.Topic);
                Assert.Equal(-1L, callback.Metadata.Offset);
            },
            gate);
    }

    [Fact]
    public async Task AsyncDeliveryCallbacks_RunOneAtATime_InCompletionOrder()
    {
        TaskCompletionSource<bool> gate = NewGate();
        List<string> log = new List<string>();
        string[] topics = { Topic("async-order-0"), Topic("async-order-1"), Topic("async-order-2") };
        AsyncProbe[] callbacks =
        {
            new AsyncProbe(_ => gate.Task, log, "0"),
            new AsyncProbe(_ => Task.Delay(50), log, "1"),
            new AsyncProbe(_ => Task.Delay(50), log, "2"),
        };
        await WithProducer(
            async producer =>
            {
                AsyncKafkaFuture<RecordMetadata>[] futures = await SendSpaced(
                    producer,
                    () => producer.Send(Record(topics[0]), callbacks[0]),
                    () => producer.Send(Record(topics[1]), callbacks[1]),
                    () => producer.Send(Record(topics[2]), callbacks[2]));

                await Task.Delay(Settle);
                Assert.Equal(new[] { "start 0" }, Snapshot(log));
                Assert.Equal(2, Waiting(producer));
                Assert.Equal(0, callbacks[1].Calls);

                gate.SetResult(true);
                for (int i = 0; i < futures.Length; i++)
                {
                    KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(futures[i].Get()));
                    Assert.Equal(MetadataTimeout(topics[i], MaxBlockMs), failure.Message);
                    Assert.Same(failure, callbacks[i].Delivered);
                    Assert.Equal(1, callbacks[i].Calls);
                    Assert.Equal(NativeProducer.PumpThreadName, callbacks[i].StartThread);
                }

                Assert.Equal(new[] { "start 0", "end 0", "start 1", "end 1", "start 2", "end 2" }, Snapshot(log));
            },
            gate);
    }

    [Fact]
    public async Task SyncAndNoCallbackSends_WaitBehindARunningAsyncCallback()
    {
        TaskCompletionSource<bool> gate = NewGate();
        List<string> events = new List<string>();
        AsyncProbe first = new AsyncProbe(_ => gate.Task);
        ProbeCallback second = new ProbeCallback((_, _) => Note(events, "callback 2"));
        string[] topics = { Topic("async-behind-0"), Topic("async-behind-1"), Topic("async-behind-2") };
        await WithProducer(
            async producer =>
            {
                AsyncKafkaFuture<RecordMetadata>[] futures = await SendSpaced(
                    producer,
                    () => producer.Send(Record(topics[0]), first),
                    () => producer.Send(Record(topics[1]), second),
                    () => producer.Send(Record(topics[2])));
                TaskCompletionSource<bool> lastCompleted = NewGate();
                _ = futures[0].WhenComplete((_, _) => Note(events, "future 1"));
                _ = futures[1].WhenComplete((_, _) => Note(events, "future 2"));
                _ = futures[2].WhenComplete((_, _) =>
                {
                    Note(events, "future 3");
                    lastCompleted.TrySetResult(true);
                });

                await Task.Delay(Settle);
                Assert.Equal(2, Waiting(producer));
                Assert.False(futures[1].IsDone);
                Assert.False(futures[2].IsDone);
                Assert.Equal(0, second.Calls);
                Assert.Empty(Snapshot(events));

                gate.SetResult(true);
                await Within(lastCompleted.Task);
                string pump = NativeProducer.PumpThreadName;
                Assert.Equal(
                    new[] { "future 1 on " + pump, "callback 2 on " + pump, "future 2 on " + pump, "future 3 on " + pump },
                    Snapshot(events));
                for (int i = 0; i < futures.Length; i++)
                {
                    KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(futures[i].Get()));
                    Assert.Equal(MetadataTimeout(topics[i], MaxBlockMs), failure.Message);
                }

                Assert.Equal(1, second.Calls);
            },
            gate);
    }

    [Theory]
    [InlineData(Faulted)]
    [InlineData(Canceled)]
    [InlineData(Throwing)]
    public async Task AsyncDeliveryCallback_FaultedCanceledOrThrowing_ReportsOnce_AndKeepsTheRecordOutcome(string how)
    {
        string marker = "async-callback-" + how + "-" + Guid.NewGuid().ToString("N");
        using TraceCapture trace = new TraceCapture(marker);
        string topic = Topic("async-callback-" + how);
        IAsyncDeliveryCallback callback;
        AsyncProbe? probe = null;
        ThrowingAsyncCallback? throwing = null;
        if (how == Throwing)
        {
            throwing = new ThrowingAsyncCallback(marker);
            callback = throwing;
        }
        else
        {
            callback = probe = new AsyncProbe(async _ =>
            {
                await Task.Yield();
                throw how == Canceled
                    ? new OperationCanceledException(marker)
                    : new InvalidOperationException(marker);
            });
        }

        await WithProducer(async producer =>
        {
            AsyncKafkaFuture<RecordMetadata> future = await FirstStage(producer.Send(Record(topic), callback));
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(future.Get()));
            Assert.Equal(7, failure.Code);
            Assert.Equal(MetadataTimeout(topic, MaxBlockMs), failure.Message);
            Assert.False(future.IsCancelled);
            if (probe is not null)
            {
                Assert.Same(failure, probe.Delivered);
                Assert.Equal(1, probe.Calls);
                Assert.True(how == Canceled ? probe.Returned!.IsCanceled : probe.Returned!.IsFaulted, $"the task did not end {how}");
            }
            else
            {
                Assert.Same(failure, throwing!.Delivered);
                Assert.Equal(1, throwing.Calls);
            }

            await Within(producer.Flush());
            string line = Assert.Single(trace.Lines());
            Assert.Contains("IAsyncDeliveryCallback", line);
        });
    }

    [Fact]
    public async Task Flush_WaitsForARunningAsyncDeliveryCallback()
    {
        TaskCompletionSource<bool> gate = NewGate();
        AsyncProbe first = new AsyncProbe(_ => gate.Task);
        await WithProducer(
            async producer =>
            {
                AsyncKafkaFuture<RecordMetadata>[] futures = await SendSpaced(
                    producer,
                    () => producer.Send(Record(Topic("async-flush-0")), first),
                    () => producer.Send(Record(Topic("async-flush-1"))));
                Task flush = producer.Flush();

                await Task.Delay(Settle);
                Assert.False(flush.IsCompleted, "Flush returned while the async callback ran");
                Assert.False(futures[0].IsDone);
                Assert.False(futures[1].IsDone);

                gate.SetResult(true);
                await Within(flush);
                Assert.True(futures[0].IsDone, "Flush returned before the async callback's record completed");
                Assert.True(futures[1].IsDone, "Flush returned before the record queued behind it completed");
                Assert.True(first.Ended);
            },
            gate);
    }

    [Fact]
    public async Task Flush_FromInsideAnAsyncDeliveryCallback_AfterAnAwait_ThrowsJavasMessage()
    {
        Exception? refused = null;
        bool faultedInCall = false;
        string? threadAfterTheAwait = null;
        await WithProducer(async producer =>
        {
            AsyncProbe callback = new AsyncProbe(async _ =>
            {
                await Task.Yield();
                threadAfterTheAwait = Thread.CurrentThread.Name;
                Task flush = producer.Flush();
                faultedInCall = flush.IsFaulted;
                try
                {
                    await flush;
                }
                catch (Exception failure)
                {
                    refused = failure;
                }
            });
            _ = await FirstStage(producer.Send(Record(Topic("async-in-callback-flush")), callback));
            await Within(callback.WhenEnded);
        });

        Assert.NotEqual(NativeProducer.PumpThreadName, threadAfterTheAwait);
        Assert.True(faultedInCall, "Flush did not end at once");
        KafkaException error = Assert.IsType<KafkaException>(refused);
        Assert.Equal(FlushMessage, error.Message);
        Assert.Equal(-1, error.Code);
        Assert.False(error.IsRetriable);
    }

    [Theory]
    [InlineData(Close, AfterAnAwait)]
    [InlineData(Dispose, AfterAnAwait)]
    [InlineData(DisposeAsync, AfterAnAwait)]
    [InlineData(Close, BeforeTheFirstAwait)]
    [InlineData(Dispose, BeforeTheFirstAwait)]
    [InlineData(DisposeAsync, BeforeTheFirstAwait)]
    public async Task Close_FromInsideAnAsyncDeliveryCallback_ClosesWithZeroTimeout_AndTearsDownAfterItEnds(
        string how,
        string when)
    {
        using TraceCapture trace = new TraceCapture("Overriding close timeout");
        bool returnedCompleted = false;
        long closeMs = -1;
        int closingThread = -1;
        int destroysBeforeTheEnd = -1;
        bool endedAtTheDestroy = false;
        await WithProducer(async producer =>
        {
            AsyncProbe? callback = null;
            DestroyLog destroys = WatchDestroy(producer, () => endedAtTheDestroy = callback!.Ended);
            callback = new AsyncProbe(async _ =>
            {
                if (when == AfterAnAwait)
                {
                    await Task.Yield();
                }

                closingThread = Environment.CurrentManagedThreadId;
                Stopwatch elapsed = Stopwatch.StartNew();
                if (how == Close)
                {
                    returnedCompleted = producer.Close().IsCompleted;
                }
                else if (how == DisposeAsync)
                {
                    returnedCompleted = producer.DisposeAsync().IsCompleted;
                }
                else
                {
                    producer.Dispose();
                    returnedCompleted = true;
                }

                closeMs = elapsed.ElapsedMilliseconds;
                await Task.Delay(Settle);
                destroysBeforeTheEnd = destroys.Destroys().Count;
            });
            _ = await FirstStage(producer.Send(Record(Topic("async-in-callback-close")), callback));

            Assert.True(destroys.WaitForADestroy(Deadline), "the teardown never ran after the callback");
            await Within(callback.WhenEnded);
            (int _, string? destroyThread) = Assert.Single(destroys.Destroys());
            Assert.Equal(NativeProducer.PumpThreadName, destroyThread);
            Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record(Topic("async-after-close"))); });
        });

        Assert.True(returnedCompleted, "Close from the callback did not return a completed close");
        Assert.InRange(closeMs, 0, MaxBlockMs);
        Assert.Equal(0, destroysBeforeTheEnd);
        Assert.True(endedAtTheDestroy, "the teardown ran before the callback's task ended");
        Assert.Equal(CloseOverride, Assert.Single(trace.LinesFrom(closingThread)));
    }

    [Theory]
    [InlineData(Close)]
    [InlineData(Dispose)]
    [InlineData(DisposeAsync)]
    public async Task Close_WaitsForARunningAsyncDeliveryCallback(string how)
    {
        TaskCompletionSource<bool> gate = NewGate();
        AsyncProbe callback = new AsyncProbe(_ => gate.Task);
        bool endedAtTheDestroy = false;
        await WithProducer(
            async producer =>
            {
                DestroyLog destroys = WatchDestroy(producer, () => endedAtTheDestroy = callback.Ended);
                AsyncKafkaFuture<RecordMetadata> future =
                    await FirstStage(producer.Send(Record(Topic("async-close-waits")), callback));
                Task closing = how == Close
                    ? producer.Close()
                    : how == DisposeAsync
                        ? producer.DisposeAsync().AsTask()
                        : Task.Factory.StartNew(
                            producer.Dispose,
                            CancellationToken.None,
                            TaskCreationOptions.LongRunning,
                            TaskScheduler.Default);

                await Task.Delay(Settle);
                Assert.False(closing.IsCompleted, $"{how} returned while the async callback ran");
                Assert.False(future.IsDone);
                Assert.Empty(destroys.Destroys());

                gate.SetResult(true);
                await Within(closing);
                Assert.True(future.IsDone, $"{how} returned before the async callback's record completed");
                Assert.Single(destroys.Destroys());
                Assert.True(endedAtTheDestroy, "the teardown ran before the callback's task ended");
            },
            gate);
    }

    [Theory]
    [InlineData(AsyncKind)]
    [InlineData(SyncKind)]
    [InlineData(NoCallback)]
    public async Task AsyncDeliveryCallback_GetOfALaterRecord_TimesOut(string laterCallback)
    {
        TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>> later =
            new TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>>(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? timedOut = null;
        long waitedMs = -1;
        AsyncProbe first = new AsyncProbe(async _ =>
        {
            AsyncKafkaFuture<RecordMetadata> laterFuture = await later.Task;
            Stopwatch waited = Stopwatch.StartNew();
            try
            {
                await laterFuture.Get(TimeSpan.FromMilliseconds(LaterWaitMs));
            }
            catch (Exception failure)
            {
                timedOut = failure;
            }

            waitedMs = waited.ElapsedMilliseconds;
        });
        AsyncProbe laterAsync = new AsyncProbe(_ => Task.CompletedTask);
        ProbeCallback laterSync = new ProbeCallback((_, _) => { });
        string laterTopic = Topic("async-later-record");
        ProducerRecord<byte[], byte[]> laterRecord = Record(laterTopic);
        await WithProducer(async producer =>
        {
            AsyncKafkaFuture<RecordMetadata>[] futures = await SendSpaced(
                producer,
                () => producer.Send(Record(Topic("async-later-first")), first),
                () => laterCallback == AsyncKind
                    ? producer.Send(laterRecord, laterAsync)
                    : laterCallback == SyncKind ? producer.Send(laterRecord, laterSync) : producer.Send(laterRecord));
            Assert.Equal(1, Waiting(producer));

            later.SetResult(futures[1]);
            await Within(first.WhenEnded);
            Assert.IsType<TimeoutException>(timedOut);
            Assert.InRange(waitedMs, LaterWaitMs - 50, (long)Deadline.TotalMilliseconds);

            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(futures[1].Get()));
            Assert.Equal(MetadataTimeout(laterTopic, MaxBlockMs), failure.Message);
            if (laterCallback == AsyncKind)
            {
                Assert.Same(failure, laterAsync.Delivered);
                Assert.Equal(1, laterAsync.Calls);
            }
            else if (laterCallback == SyncKind)
            {
                Assert.True(laterSync.WaitDone(Deadline), "the later record's callback never ran");
                Assert.Same(failure, laterSync.Delivered);
                Assert.Equal(1, laterSync.Calls);
            }
        });
    }

    [Fact]
    public async Task AsyncDeliveryCallback_TokenFiresAfterTheHandOver_DoesNotCancelIt()
    {
        TaskCompletionSource<bool> gate = NewGate();
        AsyncProbe callback = new AsyncProbe(_ => gate.Task);
        string topic = Topic("async-token-after-hand-over");
        using CancellationTokenSource cts = new CancellationTokenSource();
        await WithProducer(
            async producer =>
            {
                AsyncKafkaFuture<RecordMetadata> future =
                    await FirstStage(producer.Send(Record(topic), callback, cts.Token));
                await Within(callback.WhenStarted);
                cts.Cancel();

                await Task.Delay(Settle);
                Assert.False(callback.Ended, "the token ended the async callback");
                Assert.False(future.IsDone);
                Assert.False(future.IsCancelled);

                gate.SetResult(true);
                KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(future.Get()));
                Assert.Equal(MetadataTimeout(topic, MaxBlockMs), failure.Message);
                Assert.Same(failure, callback.Delivered);
                Assert.Equal(1, callback.Calls);
                Assert.True(callback.Ended);
            },
            gate);
    }

    [Fact]
    public async Task Send_NullAsyncDeliveryCallback_ThrowsArgumentNullException()
    {
        await WithProducer(producer =>
        {
            ArgumentNullException refused = Assert.Throws<ArgumentNullException>(
                () => { _ = producer.Send(Record(Topic("async-null-callback")), (IAsyncDeliveryCallback)null!); });
            Assert.Equal("callback", refused.ParamName);
            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task ThenApply_OnAnAsyncCallbackSend_RunsOnThePumpThread()
    {
        TaskCompletionSource<bool> gate = NewGate();
        AsyncProbe callback = new AsyncProbe(_ => gate.Task);
        await WithProducer(
            async producer =>
            {
                AsyncKafkaFuture<RecordMetadata> future =
                    await FirstStage(producer.Send(Record(Topic("async-then-apply")), callback));
                TaskCompletionSource<string?> appliedOn =
                    new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
                AsyncKafkaFuture<long> offset = future.ThenApply(metadata => metadata.Offset);
                _ = offset.WhenComplete((_, _) => appliedOn.TrySetResult(Thread.CurrentThread.Name));

                await Task.Delay(Settle);
                Assert.False(appliedOn.Task.IsCompleted, "the derived future completed while the async callback ran");

                gate.SetResult(true);
                Assert.Equal(NativeProducer.PumpThreadName, await Within(appliedOn.Task));
                Assert.Equal(7, (await Assert.ThrowsAsync<KafkaException>(() => Within(offset.Get()))).Code);
            },
            gate);
    }

    [Fact]
    public async Task AsyncDeliveryCallback_ReceivesTheErrorNextInstance()
    {
        AsyncProbe callback = new AsyncProbe(_ => Task.CompletedTask);
        AsyncMockProducer<byte[], byte[]> producer = ManualMock();
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        AsyncKafkaFuture<RecordMetadata> future =
            await FirstStage(producer.Send(Record(Topic("async-error-next")), callback));
        KafkaException injected = new KafkaException(2, "the injected instance", isRetriable: true);

        Assert.True(producer.ErrorNext(injected));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Within(future.Get()));
        Assert.Same(injected, failure);
        await Within(callback.WhenEnded);
        Assert.Same(injected, callback.Delivered);
        Assert.Equal(1, callback.Calls);
        Assert.Equal(-1L, callback.Metadata!.Offset);
    }

    [Fact]
    public async Task AsyncMockProducer_CompleteNext_RunsAnAsyncDeliveryCallback()
    {
        TaskCompletionSource<bool> gate = NewGate();
        AsyncProbe callback = new AsyncProbe(_ => gate.Task);
        string topic = Topic("async-complete-next");
        AsyncMockProducer<byte[], byte[]> producer = ManualMock();
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        try
        {
            AsyncKafkaFuture<RecordMetadata> future = await FirstStage(producer.Send(Record(topic), callback));
            await Task.Delay(Settle);
            Assert.Equal(0, callback.Calls);

            Assert.True(producer.CompleteNext());
            await Within(callback.WhenStarted);
            Assert.Equal(NativeProducer.PumpThreadName, callback.StartThread);
            await Task.Delay(Settle);
            Assert.False(future.IsDone);

            gate.SetResult(true);
            RecordMetadata metadata = await Within(future.Get());
            Assert.True(callback.Ended);
            Assert.Null(callback.Delivered);
            Assert.Same(metadata, callback.Metadata);
            Assert.Equal(topic, metadata.Topic);
            Assert.Equal(0L, metadata.Offset);
            Assert.Equal(1, callback.Calls);
        }
        finally
        {
            gate.TrySetResult(true);
        }
    }

    private static AsyncMockProducer<byte[], byte[]> ManualMock() =>
        new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

    private static TaskCompletionSource<bool> NewGate() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Task Within(Task task) => TestTimeout.Run(() => task, Deadline);

    private static Task<T> Within<T>(Task<T> task) => TestTimeout.Run(() => task, Deadline);

    private static Task<AsyncKafkaFuture<RecordMetadata>> FirstStage(ValueTask<AsyncKafkaFuture<RecordMetadata>> stage) =>
        Within(stage.AsTask());

    private static async Task<AsyncKafkaFuture<RecordMetadata>[]> SendSpaced(
        AsyncKafkaProducer<byte[], byte[]> producer,
        params Func<ValueTask<AsyncKafkaFuture<RecordMetadata>>>[] sends)
    {
        Task<AsyncKafkaFuture<RecordMetadata>>[] stages = new Task<AsyncKafkaFuture<RecordMetadata>>[sends.Length];
        for (int i = 0; i < sends.Length; i++)
        {
            if (i > 0)
            {
                await Task.Delay(SpacingMs);
            }

            stages[i] = sends[i]().AsTask();
        }

        AsyncKafkaFuture<RecordMetadata>[] futures = new AsyncKafkaFuture<RecordMetadata>[sends.Length];
        for (int i = 0; i < stages.Length; i++)
        {
            futures[i] = await Within(stages[i]);
        }

        return futures;
    }

    private static async Task WithProducer(
        Func<AsyncKafkaProducer<byte[], byte[]>, Task> body,
        params TaskCompletionSource<bool>[] gates)
    {
        AsyncKafkaProducer<byte[], byte[]> producer = Async(MaxBlockMs);
        try
        {
            await body(producer);
        }
        finally
        {
            foreach (TaskCompletionSource<bool> gate in gates)
            {
                gate.TrySetResult(true);
            }

            await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), Deadline);
        }
    }

    private static int Waiting(AsyncKafkaProducer<byte[], byte[]> producer)
    {
        const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
        NativeProducer native = (NativeProducer)producer.GetType().GetField("_native", Private)!.GetValue(producer)!;
        DeliveryReporter reporter = (DeliveryReporter)typeof(NativeProducer).GetField("_reporter", Private)!.GetValue(native)!;
        object chainGate = typeof(DeliveryReporter).GetField("_chainGate", Private)!.GetValue(reporter)!;
        Queue<DeliveryState> waiting =
            (Queue<DeliveryState>)typeof(DeliveryReporter).GetField("_waiting", Private)!.GetValue(reporter)!;
        lock (chainGate)
        {
            return waiting.Count;
        }
    }

    private static void Note(List<string> log, string what)
    {
        lock (log)
        {
            log.Add(what + " on " + Thread.CurrentThread.Name);
        }
    }

    private static string[] Snapshot(List<string> log)
    {
        lock (log)
        {
            return log.ToArray();
        }
    }

    /// <summary>An async delivery callback that runs a test's body and records its start, its end and its task.</summary>
    private sealed class AsyncProbe : IAsyncDeliveryCallback
    {
        private readonly Func<Exception?, Task> _body;
        private readonly List<string>? _log;
        private readonly string _name;
        private readonly TaskCompletionSource<bool> _started = NewGate();
        private readonly TaskCompletionSource<bool> _ended = NewGate();
        private int _calls;

        internal AsyncProbe(Func<Exception?, Task> body, List<string>? log = null, string name = "")
        {
            _body = body;
            _log = log;
            _name = name;
        }

        internal int Calls => Volatile.Read(ref _calls);

        internal string? StartThread { get; private set; }

        internal RecordMetadata? Metadata { get; private set; }

        internal Exception? Delivered { get; private set; }

        internal Task? Returned { get; private set; }

        internal bool Ended => _ended.Task.IsCompleted;

        internal Task WhenStarted => _started.Task;

        internal Task WhenEnded => _ended.Task;

        public Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            StartThread = Thread.CurrentThread.Name;
            Task returned = Run(metadata, exception);
            Returned = returned;
            return returned;
        }

        private async Task Run(RecordMetadata metadata, Exception? exception)
        {
            Interlocked.Increment(ref _calls);
            Metadata = metadata;
            Delivered = exception;
            Log("start");
            _started.TrySetResult(true);
            try
            {
                await _body(exception);
            }
            finally
            {
                Log("end");
                _ended.TrySetResult(true);
            }
        }

        private void Log(string what)
        {
            if (_log is not null)
            {
                lock (_log)
                {
                    _log.Add(what + " " + _name);
                }
            }
        }
    }

    /// <summary>An async delivery callback whose <c>OnCompletion</c> throws instead of returning a task.</summary>
    private sealed class ThrowingAsyncCallback : IAsyncDeliveryCallback
    {
        private readonly string _marker;
        private int _calls;

        internal ThrowingAsyncCallback(string marker) => _marker = marker;

        internal int Calls => Volatile.Read(ref _calls);

        internal Exception? Delivered { get; private set; }

        public Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Interlocked.Increment(ref _calls);
            Delivered = exception;
            throw new InvalidOperationException(_marker);
        }
    }
}
