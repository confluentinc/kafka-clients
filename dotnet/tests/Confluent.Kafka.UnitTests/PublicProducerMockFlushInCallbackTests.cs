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
/// A manual mock's <c>Flush</c> from inside one of its delivery callbacks, wherever the callback runs: in the drain,
/// resumed by the async chain, or after an <c>await</c>. It completes the pending send and returns at once, without
/// the <c>max.block.ms</c> wait, and the completed send's callback runs once, after the calling callback. The async
/// mock's sync <c>Dispose</c> from such a callback flushes the same way before it closes.
/// </summary>
public sealed class PublicProducerMockFlushInCallbackTests
{
    private const string Drained = "a sync callback the drain runs";
    private const string ResumedSync = "a sync callback queued behind an async callback";
    private const string ResumedAsync = "an async callback's synchronous part queued behind an async callback";
    private const string AfterAnAwait = "an async callback after an await";

    private const string FlushingEnded = "flushing callback ended";
    private const string PendingRan = "pending callback ran";

    private static readonly TimeSpan s_quickly = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(SyncFlavor, Drained)]
    [InlineData(AsyncFlavor, Drained)]
    [InlineData(AsyncFlavor, ResumedSync)]
    [InlineData(AsyncFlavor, ResumedAsync)]
    [InlineData(AsyncFlavor, AfterAnAwait)]
    public async Task MockFlush_FromACallback_CompletesThePendingSend_AndReturnsWithoutWaitingForItsReport(
        string flavor,
        string cell)
    {
        Observation seen = flavor == SyncFlavor ? RunOnMockProducer() : await RunOnAsyncMockProducer(cell);

        Assert.Null(seen.Failure);
        Assert.True(seen.ReturnedCompleted, "Flush did not return a completed result in the call");
        Assert.True(seen.Elapsed < s_quickly, $"Flush took {seen.Elapsed.TotalMilliseconds} ms");
        Assert.Equal(0, seen.PendingCallsAtReturn);
        Assert.True(seen.PendingCompleted, "the pending send did not complete");
        await Task.Delay(Settle);
        Assert.Equal(1, seen.FlushingCalls);
        Assert.Equal(1, seen.PendingCalls());
        Assert.Equal(new[] { FlushingEnded, PendingRan }, seen.Snapshot());
    }

    [Theory]
    [InlineData(ResumedSync)]
    [InlineData(ResumedAsync)]
    [InlineData(AfterAnAwait)]
    public async Task AsyncMockDispose_FromACallback_CompletesThePendingSend_AndClosesWithoutWaitingForItsReport(
        string cell)
    {
        Observation seen = await RunOnAsyncMockProducer(cell, disposes: true);

        Assert.Null(seen.Failure);
        Assert.True(seen.ReturnedCompleted, "Dispose did not return");
        Assert.True(seen.Elapsed < s_quickly, $"Dispose took {seen.Elapsed.TotalMilliseconds} ms");
        Assert.Equal(0, seen.PendingCallsAtReturn);
        Assert.True(seen.PendingCompleted, "the pending send did not complete");
        Assert.IsType<ObjectDisposedException>(seen.AfterClose);
        await Task.Delay(Settle);
        Assert.Equal(1, seen.FlushingCalls);
        Assert.Equal(1, seen.PendingCalls());
        Assert.Equal(new[] { FlushingEnded, PendingRan }, seen.Snapshot());
    }

    private static Observation RunOnMockProducer()
    {
        Observation seen = new Observation();
        MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        string topic = Topic("mock-flush-in-callback");
        Recording pending = new Recording(seen);
        Flushing flushing = new Flushing(seen, () =>
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            try
            {
                producer.Flush();
                seen.ReturnedCompleted = true;
            }
            catch (Exception failure)
            {
                seen.Failure = failure;
            }

            seen.Elapsed = elapsed.Elapsed;
            seen.PendingCallsAtReturn = pending.Calls;
        });

        _ = producer.Send(Record(topic), flushing);
        KafkaFuture<RecordMetadata> pendingFuture = producer.Send(Record(topic), pending);
        seen.PendingCalls = () => pending.Calls;

        Assert.True(producer.CompleteNext());
        Assert.True(flushing.WaitEnded(Deadline), "the flushing callback did not end");
        _ = pendingFuture.Get(Deadline);
        seen.PendingCompleted = pendingFuture.IsDone;
        return seen;
    }

    private static async Task<Observation> RunOnAsyncMockProducer(string cell, bool disposes = false)
    {
        Observation seen = new Observation();
        TaskCompletionSource<bool> gate = NewGate();
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        try
        {
            string topic = Topic("async-mock-flush-in-callback");
            Recording pending = new Recording(seen);
            Action observe = () =>
            {
                Stopwatch elapsed = Stopwatch.StartNew();
                Task returned;
                try
                {
                    returned = disposes ? DisposeOf(producer) : producer.Flush();
                }
                catch (Exception failure)
                {
                    seen.Failure = failure;
                    return;
                }

                seen.Elapsed = elapsed.Elapsed;
                seen.ReturnedCompleted = returned.Status == TaskStatus.RanToCompletion;
                seen.Failure = returned.Exception?.InnerException;
                seen.PendingCallsAtReturn = pending.Calls;
            };

            bool behindAnAsyncCallback = cell == ResumedSync || cell == ResumedAsync;
            Blocker blocker = new Blocker(gate.Task);
            if (behindAnAsyncCallback)
            {
                _ = await Within(producer.Send(Record(topic), blocker).AsTask());
            }

            Task flushingEnded;
            if (cell == Drained || cell == ResumedSync)
            {
                Flushing flushing = new Flushing(seen, observe);
                _ = await Within(producer.Send(Record(topic), flushing).AsTask());
                flushingEnded = Task.Run(() => Assert.True(flushing.WaitEnded(Deadline)));
            }
            else
            {
                AsyncFlushing flushing = new AsyncFlushing(seen, observe, afterAnAwait: cell == AfterAnAwait);
                _ = await Within(producer.Send(Record(topic), flushing).AsTask());
                flushingEnded = flushing.WhenEnded;
            }

            AsyncKafkaFuture<RecordMetadata> pendingFuture =
                await Within(producer.Send(Record(topic), pending).AsTask());
            seen.PendingCalls = () => pending.Calls;

            if (behindAnAsyncCallback)
            {
                Assert.True(producer.CompleteNext());
                await Within(blocker.WhenStarted);
            }

            Assert.True(producer.CompleteNext());
            if (behindAnAsyncCallback)
            {
                WaitUntilQueuedBehind(producer, 1);
                gate.SetResult(true);
            }

            await TestTimeout.Run(() => flushingEnded, disposes ? s_quickly : Deadline);
            _ = await Within(pendingFuture.Get());
            seen.PendingCompleted = pendingFuture.IsDone;
            if (disposes)
            {
                seen.AfterClose = FailureOf(() => _ = producer.Flush());
                await Within(producer.DisposeAsync().AsTask());
            }

            return seen;
        }
        finally
        {
            gate.TrySetResult(true);
        }
    }

    private static Task DisposeOf(IDisposable producer)
    {
        producer.Dispose();
        return Task.CompletedTask;
    }

    private static Exception? FailureOf(Action call)
    {
        try
        {
            call();
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
    }

    private static void WaitUntilQueuedBehind(AsyncMockProducer<byte[], byte[]> producer, int count)
    {
        const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
        NativeProducer native = (NativeProducer)producer.GetType().GetField("_native", Private)!.GetValue(producer)!;
        DeliveryReporter reporter = (DeliveryReporter)typeof(NativeProducer).GetField("_reporter", Private)!.GetValue(native)!;
        object chainGate = typeof(DeliveryReporter).GetField("_chainGate", Private)!.GetValue(reporter)!;
        Queue<DeliveryState> waiting =
            (Queue<DeliveryState>)typeof(DeliveryReporter).GetField("_waiting", Private)!.GetValue(reporter)!;
        Stopwatch waited = Stopwatch.StartNew();
        while (true)
        {
            lock (chainGate)
            {
                if (waiting.Count == count)
                {
                    return;
                }
            }

            Assert.True(waited.Elapsed < Deadline, "the flushing record never queued behind the async callback");
            Thread.Sleep(1);
        }
    }

    private static TaskCompletionSource<bool> NewGate() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Task Within(Task task) => TestTimeout.Run(() => task, Deadline);

    private static Task<T> Within<T>(Task<T> task) => TestTimeout.Run(() => task, Deadline);

    /// <summary>
    /// What the flushing callback saw of its <c>Flush</c> or <c>Dispose</c>, and the order of the two callbacks.
    /// </summary>
    private sealed class Observation
    {
        private readonly List<string> _log = new List<string>();
        private int _flushingCalls;

        internal Exception? Failure { get; set; }

        internal bool ReturnedCompleted { get; set; }

        internal TimeSpan Elapsed { get; set; }

        internal int PendingCallsAtReturn { get; set; } = -1;

        internal bool PendingCompleted { get; set; }

        internal Func<int> PendingCalls { get; set; } = () => -1;

        internal Exception? AfterClose { get; set; }

        internal int FlushingCalls => Volatile.Read(ref _flushingCalls);

        internal void CountFlushing() => Interlocked.Increment(ref _flushingCalls);

        internal void Log(string what)
        {
            lock (_log)
            {
                _log.Add(what);
            }
        }

        internal string[] Snapshot()
        {
            lock (_log)
            {
                return _log.ToArray();
            }
        }
    }

    /// <summary>A sync delivery callback that flushes its own producer.</summary>
    private sealed class Flushing : IDeliveryCallback
    {
        private readonly Observation _seen;
        private readonly Action _observe;
        private readonly ManualResetEventSlim _ended = new ManualResetEventSlim();

        internal Flushing(Observation seen, Action observe)
        {
            _seen = seen;
            _observe = observe;
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _seen.CountFlushing();
            try
            {
                _observe();
            }
            finally
            {
                _seen.Log(FlushingEnded);
                _ended.Set();
            }
        }

        internal bool WaitEnded(TimeSpan timeout) => _ended.Wait(timeout);
    }

    /// <summary>An async delivery callback that flushes its own producer, before its first <c>await</c> or after one.</summary>
    private sealed class AsyncFlushing : IAsyncDeliveryCallback
    {
        private readonly Observation _seen;
        private readonly Action _observe;
        private readonly bool _afterAnAwait;
        private readonly TaskCompletionSource<bool> _ended = NewGate();

        internal AsyncFlushing(Observation seen, Action observe, bool afterAnAwait)
        {
            _seen = seen;
            _observe = observe;
            _afterAnAwait = afterAnAwait;
        }

        internal Task WhenEnded => _ended.Task;

        public Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _seen.CountFlushing();
            return Run();
        }

        private async Task Run()
        {
            try
            {
                if (_afterAnAwait)
                {
                    await Task.Yield();
                }

                _observe();
            }
            finally
            {
                _seen.Log(FlushingEnded);
                _ended.TrySetResult(true);
            }
        }
    }

    /// <summary>An async delivery callback whose task runs until its gate opens.</summary>
    private sealed class Blocker : IAsyncDeliveryCallback
    {
        private readonly Task _gate;
        private readonly TaskCompletionSource<bool> _started = NewGate();

        internal Blocker(Task gate) => _gate = gate;

        internal Task WhenStarted => _started.Task;

        public Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _started.TrySetResult(true);
            return _gate;
        }
    }

    /// <summary>The pending send's delivery callback: it counts its calls and logs that it ran.</summary>
    private sealed class Recording : IDeliveryCallback
    {
        private readonly Observation _seen;
        private int _calls;

        internal Recording(Observation seen) => _seen = seen;

        internal int Calls => Volatile.Read(ref _calls);

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Interlocked.Increment(ref _calls);
            _seen.Log(PendingRan);
        }
    }
}
