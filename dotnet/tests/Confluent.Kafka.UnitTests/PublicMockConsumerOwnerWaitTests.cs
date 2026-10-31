// Copyright 2026 Confluent Inc.
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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The owner wait on <see cref="MockConsumer{TKey, TValue}"/> and <see cref="AsyncMockConsumer{TKey, TValue}"/>:
/// another thread's call waits for the call that holds the mock, is refused for the whole of an
/// <see cref="AsyncMockConsumer{TKey, TValue}"/> rebalance or partition loss and while an async callback runs, and the
/// three calls Java's mock leaves unsynchronized do not wait.
/// </summary>
public sealed class PublicMockConsumerOwnerWaitTests
{
    private const string Topic = "mock-owner-wait-topic";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int WakeupCode = -18;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_bound = TimeSpan.FromSeconds(10);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void SyncMock_AnotherThreadsCall_WhileAListenerRuns_WaitsForTheRebalance_ThenReturns()
    {
        Mock mock = Mock.Create(asyncMock: false);
        try
        {
            IConsumerCommon consumer = mock.Consumer;
            consumer.Assignment();
            int[] state = new int[3];
            Thread? other = null;
            Exception? failure = null;
            IReadOnlyCollection<TopicPartition>? seen = null;
            bool returnedAfterTheListener = false;
            bool waited = false;
            mock.Subscribe(new SyncListener(() =>
            {
                other = StartThread(() =>
                {
                    Volatile.Write(ref state[0], 1);
                    failure = Record.Exception(() => seen = consumer.Assignment());
                    returnedAfterTheListener = Volatile.Read(ref state[2]) == 1;
                    Volatile.Write(ref state[1], 1);
                });
                SpinUntilWaitingOrEnded(other, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
                waited = Volatile.Read(ref state[1]) == 0;
                Volatile.Write(ref state[2], 1);
            }));

            TestTimeout.Run(mock.Rebalance, s_deadline);

            Assert.True(other!.Join(s_bound), "the other thread's call never returned");
            Assert.True(waited, "the other thread's call returned while the listener ran");
            Assert.Null(failure);
            Assert.True(returnedAfterTheListener, "the other thread's call returned before the listener ended");
            Assert.Equal(new[] { s_tp0 }, seen);
        }
        finally
        {
            TestTimeout.Run(mock.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AnotherThreadsCalls_WhileRebalanceRunsASyncListener_AreRefusedAtOnce_AndWakeupAndHandlePass()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            consumer.Assignment();
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            bool returned = false;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() =>
                    {
                        Thread other = StartThread(() =>
                        {
                            seen["Assignment"] = Record.Exception(() => consumer.Assignment());
                            seen["Position"] = Record.Exception(() => { _ = consumer.Position(s_tp0); });
                            seen["Close"] = Record.Exception(() => Block(() => CloseCatching(consumer)));
                            seen["Handle"] = Record.Exception(() => consumer.Handle().Dispose());
                            seen["Wakeup"] = Record.Exception(consumer.Wakeup);
                        });
                        returned = other.Join(s_bound);
                    })),
                s_deadline);

            Exception? rebalanced = await Record.ExceptionAsync(
                () => TestTimeout.Run(() => consumer.Rebalance(new[] { s_tp0 }), s_deadline));

            Assert.True(returned, "the other thread's calls waited for the rebalance");
            AssertRefused(seen["Assignment"]);
            AssertRefused(seen["Position"]);
            AssertRefused(seen["Close"]);
            Assert.Null(seen["Handle"]);
            Assert.Null(seen["Wakeup"]);
            Assert.True(rebalanced is null || rebalanced is KafkaException { Code: WakeupCode }, rebalanced?.ToString());
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AnotherThreadsCalls_WhileRebalanceAwaitsAnAsyncListener_AreRefused_AndWakeupAndHandlePass()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            using ManualResetEventSlim entered = new ManualResetEventSlim();
            TaskCompletionSource<bool> release =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new AsyncListener(async () =>
                    {
                        entered.Set();
                        await release.Task.ConfigureAwait(false);
                    })),
                s_deadline);
            Task rebalance = consumer.Rebalance(new[] { s_tp0 });
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            Task? close = null;
            bool pending;
            try
            {
                Assert.True(entered.Wait(s_deadline), "the listener never ran");
                RunOnAnotherThread(() =>
                {
                    seen["Assignment"] = Record.Exception(() => consumer.Assignment());
                    seen["Metrics"] = Record.Exception(() => consumer.Metrics());
                    seen["Position"] = Record.Exception(() => { _ = consumer.Position(s_tp0); });
                    close = CloseCatching(consumer);
                    seen["Handle"] = Record.Exception(() => consumer.Handle().Dispose());
                    seen["Wakeup"] = Record.Exception(consumer.Wakeup);
                });
                pending = !rebalance.IsCompleted;
            }
            finally
            {
                release.TrySetResult(true);
            }

            Exception? rebalanced = await Record.ExceptionAsync(() => TestTimeout.Run(() => rebalance, s_deadline));

            Assert.True(pending, "the rebalance ended before the async listener did");
            Assert.True(rebalanced is null || rebalanced is KafkaException { Code: WakeupCode }, rebalanced?.ToString());
            AssertRefused(seen["Assignment"]);
            AssertRefused(seen["Metrics"]);
            AssertRefused(seen["Position"]);
            AssertRefused(await Record.ExceptionAsync(() => TestTimeout.Run(() => close!, s_deadline)));
            Assert.Null(seen["Handle"]);
            Assert.Null(seen["Wakeup"]);
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AnotherThreadsCall_DuringAnAsyncListenersSynchronousPart_IsRefusedAtOnce()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            consumer.Assignment();
            Exception? failure = null;
            bool returned = false;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new AsyncListener(() =>
                    {
                        Thread other;
                        using (ExecutionContext.SuppressFlow())
                        {
                            other = StartThread(() => failure = Record.Exception(() => consumer.Assignment()));
                        }

                        returned = other.Join(s_bound);
                        return Task.CompletedTask;
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_tp0 }), s_deadline);

            Assert.True(returned, "the other thread's call waited while the listener's synchronous part ran");
            AssertRefused(failure);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_ACallWaitingForAnotherThreadsUse_IsWokenAndRefused_WhenThatThreadsRebalanceBegins()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            consumer.Assignment();
            int[] state = new int[2];
            Exception? failure = null;
            Task subscribed;
            Task rebalance;
            bool waited;
            bool woken;
            using (new ConsumerPumpStall(consumer))
            {
                subscribed = consumer.Subscribe(new[] { Topic });
                Thread other = StartThread(() =>
                {
                    Volatile.Write(ref state[0], 1);
                    failure = Record.Exception(() => consumer.Assignment());
                    Volatile.Write(ref state[1], 1);
                });
                SpinUntilWaitingOrEnded(other, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
                waited = Volatile.Read(ref state[1]) == 0;
                rebalance = consumer.Rebalance(new[] { s_tp0 });
                woken = other.Join(s_bound);
            }

            await TestTimeout.Run(() => subscribed, s_deadline);
            await TestTimeout.Run(() => rebalance, s_deadline);

            Assert.True(waited, "the other thread's call did not wait for this thread's subscribe");
            Assert.True(woken, "the waiting call was not woken when this thread's rebalance began");
            AssertRefused(failure);
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AfterARebalance_AnotherThreadsCall_WaitsForThisThreadsUseAgain()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }), s_deadline);
            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_tp0 }), s_deadline);
            int[] state = new int[2];
            Exception? failure = null;
            IReadOnlyCollection<TopicPartition>? seen = null;
            Task subscribed;
            Thread other;
            bool waited;
            using (new ConsumerPumpStall(consumer))
            {
                subscribed = consumer.Subscribe(new[] { Topic });
                other = StartThread(() =>
                {
                    Volatile.Write(ref state[0], 1);
                    failure = Record.Exception(() => seen = consumer.Assignment());
                    Volatile.Write(ref state[1], 1);
                });
                SpinUntilWaitingOrEnded(other, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
                waited = Volatile.Read(ref state[1]) == 0;
            }

            await TestTimeout.Run(() => subscribed, s_deadline);

            Assert.True(other.Join(s_bound), "the other thread's call never returned");
            Assert.True(waited, "the other thread's call did not wait for this thread's subscribe");
            Assert.Null(failure);
            Assert.NotNull(seen);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_AnotherThreadsCall_DuringLosePartitions_IsRefusedAtOnce(bool asyncListener)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            Exception? failure = null;
            bool returned = false;
            bool lost = false;
            Action onLost = () =>
            {
                Thread other;
                using (ExecutionContext.SuppressFlow())
                {
                    other = StartThread(() => failure = Record.Exception(() => consumer.Assignment()));
                }

                returned = other.Join(s_bound);
                lost = true;
            };
            Task subscribed = asyncListener
                ? consumer.Subscribe(
                    new[] { Topic },
                    new AsyncListener(
                        () => Task.CompletedTask,
                        () =>
                        {
                            onLost();
                            return Task.CompletedTask;
                        }))
                : consumer.Subscribe(new[] { Topic }, new SyncListener(() => { }, onLost));
            await TestTimeout.Run(() => subscribed, s_deadline);
            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_tp0 }), s_deadline);

            await TestTimeout.Run(() => consumer.LosePartitions(new[] { s_tp0 }), s_deadline);

            Assert.True(lost, "the listener never saw the partitions lost");
            Assert.True(returned, "the other thread's call waited while the partitions were lost");
            AssertRefused(failure);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AnotherThreadsCall_DuringAnAsyncCommitCallbacksSynchronousPart_IsRefusedAtOnce()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            Exception? failure = null;
            bool returned = false;
            bool called = false;

            await TestTimeout.Run(
                () => consumer.CommitAsync(
                    Offsets(),
                    new AsyncCommitCallback(() =>
                    {
                        Thread other;
                        using (ExecutionContext.SuppressFlow())
                        {
                            other = StartThread(() => failure = Record.Exception(() => consumer.Assignment()));
                        }

                        returned = other.Join(s_bound);
                        called = true;
                        return Task.CompletedTask;
                    })),
                s_deadline);

            Assert.True(called, "the commit callback never ran");
            Assert.True(returned, "the other thread's call waited while the callback's synchronous part ran");
            AssertRefused(failure);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AnotherThreadsCall_WaitsDuringACommitsBody_AndIsWokenAndRefusedWhenItsAsyncCallbackStarts()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        TaskCompletionSource<bool> release =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            using ManualResetEventSlim started = new ManualResetEventSlim();
            int[] state = new int[2];
            Exception? failure = null;
            Thread? other = null;
            bool refusedInTheSynchronousPart = false;
            Task commit;
            bool waited;
            using (new ConsumerPumpStall(consumer))
            {
                commit = consumer.CommitAsync(
                    Offsets(),
                    new AsyncCommitCallback(() =>
                    {
                        started.Set();
                        refusedInTheSynchronousPart = Volatile.Read(ref other)!.Join(s_bound);
                        return release.Task;
                    }));
                Volatile.Write(ref other, StartThread(() =>
                {
                    Volatile.Write(ref state[0], 1);
                    failure = Record.Exception(() => consumer.Assignment());
                    Volatile.Write(ref state[1], 1);
                }));
                SpinUntilWaitingOrEnded(other!, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
                waited = Volatile.Read(ref state[1]) == 0;
            }

            bool pending;
            try
            {
                Assert.True(started.Wait(s_deadline), "the commit callback never ran");
                Assert.True(other!.Join(s_bound), "the other thread's call never returned");
                pending = !commit.IsCompleted;
            }
            finally
            {
                release.TrySetResult(true);
            }

            await TestTimeout.Run(() => commit, s_deadline);

            Assert.True(waited, "the other thread's call did not wait while the commit ran before its callback");
            Assert.True(refusedInTheSynchronousPart, "the waiting call was not woken when the async callback began");
            Assert.True(pending, "the commit ended before its async callback did");
            AssertRefused(failure);
        }
        finally
        {
            release.TrySetResult(true);
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AnotherThreadsCall_WhileASyncCommitCallbackRuns_WaitsForTheCommit_ThenReturns()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        using ManualResetEventSlim release = new ManualResetEventSlim();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            using ManualResetEventSlim running = new ManualResetEventSlim();
            int[] state = new int[2];
            Exception? failure = null;
            IReadOnlyCollection<TopicPartition>? seen = null;
            Thread other;
            bool waited;
            Task commit = consumer.CommitAsync(
                Offsets(),
                new CommitCallback(() =>
                {
                    running.Set();
                    _ = release.Wait(s_deadline);
                }));
            try
            {
                Assert.True(running.Wait(s_deadline), "the commit callback never ran");
                other = StartThread(() =>
                {
                    Volatile.Write(ref state[0], 1);
                    failure = Record.Exception(() => seen = consumer.Assignment());
                    Volatile.Write(ref state[1], 1);
                });
                SpinUntilWaitingOrEnded(other, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
                waited = Volatile.Read(ref state[1]) == 0;
            }
            finally
            {
                release.Set();
            }

            await TestTimeout.Run(() => commit, s_deadline);

            Assert.True(other.Join(s_bound), "the other thread's call never returned");
            Assert.True(waited, "the other thread's call did not wait while the sync commit callback ran");
            Assert.Null(failure);
            Assert.Equal(new[] { s_tp0 }, seen);
        }
        finally
        {
            release.Set();
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GroupMetadataEnforceRebalanceAndCurrentLag_WhileAListenerRuns_DoNotWait_AndMeetTheCoreMock(bool asyncMock)
    {
        Mock mock = Mock.Create(asyncMock);
        try
        {
            IConsumerCommon consumer = mock.Consumer;
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            Task<long?>? lag = null;
            bool returned = false;
            mock.Subscribe(new SyncListener(() =>
            {
                Thread other = StartThread(() =>
                {
                    seen["GroupMetadata"] = Record.Exception(() => consumer.GroupMetadata());
                    seen["EnforceRebalance"] = Record.Exception(() => consumer.EnforceRebalance());
                    lag = mock.StartCurrentLag();
                });
                returned = other.Join(s_bound);
            }));

            TestTimeout.Run(mock.Rebalance, s_deadline);

            Assert.True(returned, "the other thread's calls waited for the listener");
            AssertRefused(seen["GroupMetadata"]);
            AssertRefused(seen["EnforceRebalance"]);
            AssertRefused(Record.Exception(() => Block(() => lag!)));
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            TestTimeout.Run(mock.Dispose, s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_AListenersCalls_DoNotWait_AndMeetTheCoreMock(bool asyncListener)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            Exception? metrics = null;
            IReadOnlyDictionary<MetricName, IMetric>? read = null;
            Exception? subscription = null;
            string? drain = null;
            Action calls = () =>
            {
                metrics = Record.Exception(() => read = consumer.Metrics());
                subscription = Record.Exception(() => consumer.Subscription());
            };
            Task subscribed = asyncListener
                ? consumer.Subscribe(
                    new[] { Topic },
                    new AsyncListener(async () =>
                    {
                        drain = Thread.CurrentThread.Name;
                        await Task.Yield();
                        calls();
                    }))
                : consumer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() =>
                    {
                        drain = Thread.CurrentThread.Name;
                        calls();
                    }));
            await TestTimeout.Run(() => subscribed, s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_tp0 }), s_bound);

            Assert.Equal("confluent-kafka-consumer-pump", drain);
            Assert.Null(metrics);
            Assert.NotNull(read);
            AssertRefused(subscription);
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static void AssertRefused(Exception? thrown)
    {
        InvalidOperationException refused = Assert.IsType<InvalidOperationException>(thrown);
        Assert.Equal(ConcurrentMessage, refused.Message);
    }

    private static Thread StartThread(Action body)
    {
        Thread thread = new Thread(() => body());
        thread.IsBackground = true;
        thread.Start();
        return thread;
    }

    private static void RunOnAnotherThread(Action action)
    {
        Exception? failure = null;
        Thread other = StartThread(() => failure = Record.Exception(action));
        if (!other.Join(s_bound))
        {
            throw new TimeoutException("the other thread's calls did not return");
        }

        if (failure is not null)
        {
            throw new InvalidOperationException("the other thread failed", failure);
        }
    }

    private static void SpinUntilWaitingOrEnded(Thread thread, Func<bool> calling, Func<bool> ended)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!ended() && !(calling() && (thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0))
        {
            if (elapsed.Elapsed > s_deadline)
            {
                throw new TimeoutException("the other thread's call neither waited nor ended");
            }

            Thread.Yield();
        }
    }

    private static Task CloseCatching(IAsyncConsumer<byte[], byte[]> consumer)
    {
        try
        {
            return consumer.Close();
        }
        catch (Exception e)
        {
            return Task.FromException(e);
        }
    }

    private static void Block(Func<Task> operation) => TestTimeout.Run(operation, s_deadline).GetAwaiter().GetResult();

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(1) };

    /// <summary>One of the two mocks, driven through the same calls.</summary>
    private sealed class Mock : IDisposable
    {
        private readonly MockConsumer<byte[], byte[]>? _sync;

        private readonly AsyncMockConsumer<byte[], byte[]>? _async;

        private Mock(MockConsumer<byte[], byte[]>? sync, AsyncMockConsumer<byte[], byte[]>? async)
        {
            _sync = sync;
            _async = async;
        }

        internal IConsumerCommon Consumer => _sync is not null ? _sync : _async!;

        internal static Mock Create(bool asyncMock) => asyncMock
            ? new Mock(null, NewAsyncMock())
            : new Mock(new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray), null);

        internal void Subscribe(IConsumerRebalanceListener listener)
        {
            if (_sync is not null)
            {
                _sync.Subscribe(new[] { Topic }, listener);
            }
            else
            {
                Block(() => _async!.Subscribe(new[] { Topic }, listener));
            }
        }

        internal void Rebalance()
        {
            if (_sync is not null)
            {
                _sync.Rebalance(new[] { s_tp0 });
            }
            else
            {
                Block(() => _async!.Rebalance(new[] { s_tp0 }));
            }
        }

        internal Task<long?> StartCurrentLag()
        {
            if (_async is not null)
            {
                return _async.CurrentLag(s_tp0);
            }

            try
            {
                return Task.FromResult(_sync!.CurrentLag(s_tp0));
            }
            catch (Exception e)
            {
                return Task.FromException<long?>(e);
            }
        }

        public void Dispose()
        {
            _sync?.Dispose();
            _async?.Dispose();
        }
    }

    /// <summary>A listener whose assignment and loss run the given actions.</summary>
    private sealed class SyncListener : IConsumerRebalanceListener
    {
        private readonly Action _onAssigned;

        private readonly Action? _onLost;

        internal SyncListener(Action onAssigned, Action? onLost = null)
        {
            _onAssigned = onAssigned;
            _onLost = onLost;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => _onLost?.Invoke();
    }

    /// <summary>An async listener whose assignment and loss run the given functions.</summary>
    private sealed class AsyncListener : IAsyncConsumerRebalanceListener
    {
        private readonly Func<Task> _onAssigned;

        private readonly Func<Task>? _onLost;

        internal AsyncListener(Func<Task> onAssigned, Func<Task>? onLost = null)
        {
            _onAssigned = onAssigned;
            _onLost = onLost;
        }

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            _onLost is null ? Task.CompletedTask : _onLost();
    }

    /// <summary>A commit callback that runs the given action.</summary>
    private sealed class CommitCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal CommitCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }

    /// <summary>An async commit callback that runs the given function.</summary>
    private sealed class AsyncCommitCallback : IAsyncOffsetCommitCallback
    {
        private readonly Func<Task> _onComplete;

        internal AsyncCommitCallback(Func<Task> onComplete) => _onComplete = onComplete;

        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }
}
