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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Close</c> and <c>Dispose</c> called inside a rebalance listener of the same consumer, on the sync and the
/// async mock.
/// </summary>
public sealed class PublicConsumerCloseFromACallbackTests
{
    private const string Topic = "close-from-a-callback-topic";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_stress = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan s_bound = TimeSpan.FromSeconds(10);

    private static readonly TopicPartition s_partition = new TopicPartition(Topic, 0);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SyncMock_CloseInsideAListener_Throws_AndTheConsumerStaysOpen(bool withOptions)
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        try
        {
            Exception? inside = null;
            consumer.Subscribe(
                new[] { Topic },
                new SyncListener(() => inside = Record.Exception(() => Close(consumer, withOptions))));

            TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            InvalidOperationException rejected = Assert.IsType<InvalidOperationException>(inside);
            Assert.Equal(ConcurrentMessage, rejected.Message);
            Assert.Equal(new[] { s_partition }, consumer.Assignment());
            TestTimeout.Teardown(consumer, () => consumer.Close());
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void SyncMock_DisposeInsideAListener_Throws_AndTheConsumerStaysOpen()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        try
        {
            Exception? inside = null;
            consumer.Subscribe(new[] { Topic }, new SyncListener(() => inside = Record.Exception(consumer.Dispose)));

            TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            InvalidOperationException rejected = Assert.IsType<InvalidOperationException>(inside);
            Assert.Equal(ConcurrentMessage, rejected.Message);
            Assert.Equal(new[] { s_partition }, consumer.Assignment());
            TestTimeout.Dispose(consumer);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task SyncMock_CloseAndDisposeInsideAListener_WhileAnotherThreadDisposes_DoNothing()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        Exception? inside = null;
        consumer.Subscribe(
            new[] { Topic },
            new SyncListener(() =>
            {
                entered.Set();
                Assert.True(release.Wait(s_deadline), "the listener was never released");
                inside = Record.Exception(() =>
                {
                    consumer.Close();
                    consumer.Dispose();
                });
            }));

        Task rebalance = Task.Run(() => consumer.Rebalance(new[] { s_partition }));
        Assert.True(entered.Wait(s_deadline), "the listener never ran");
        Task disposing = Task.Run(consumer.Dispose);
        try
        {
            SpinUntilClosing(consumer);
        }
        finally
        {
            release.Set();
        }

        await TestTimeout.Run(() => rebalance, s_deadline);
        await TestTimeout.Run(() => disposing, s_deadline);
        Assert.Null(inside);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public void SyncMock_CloseInsideAListener_IsRefused_WhileAnotherThreadsCloseWaits_ThenTheOtherThreadCloses()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        try
        {
            consumer.Assignment();
            using ManualResetEventSlim go = new ManualResetEventSlim(false);
            int calling = 0;
            int ended = 0;
            Exception? outside = null;
            Thread closer = new Thread(() =>
            {
                if (!go.Wait(s_deadline))
                {
                    return;
                }

                Volatile.Write(ref calling, 1);
                outside = Record.Exception(() => consumer.Close());
                Volatile.Write(ref ended, 1);
            });
            closer.IsBackground = true;
            closer.Start();

            int calls = 0;
            int returned = 0;
            bool waited = false;
            Exception? unexpected = null;
            consumer.Subscribe(
                new[] { Topic },
                new SyncListener(() =>
                {
                    go.Set();
                    SpinUntilWaitingOrEnded(
                        closer, () => Volatile.Read(ref calling) == 1, () => Volatile.Read(ref ended) == 1);
                    Stopwatch elapsed = Stopwatch.StartNew();
                    while (elapsed.Elapsed < s_stress)
                    {
                        calls++;
                        try
                        {
                            consumer.Close();
                            returned++;
                        }
                        catch (InvalidOperationException e) when (e.Message == ConcurrentMessage)
                        {
                        }
                        catch (Exception e)
                        {
                            unexpected = e;
                            break;
                        }
                    }

                    waited = Volatile.Read(ref ended) == 0;
                }));

            TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.True(closer.Join(s_deadline), "the other thread's close did not end");
            Assert.True(waited, "the other thread's close did not wait for the rebalance");
            Assert.Null(unexpected);
            Assert.True(returned == 0, $"{returned} of {calls} closes inside the listener returned without closing");
            Assert.Null(outside);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_CloseInsideASyncListener_FaultsAtOnce_AndTheConsumerStaysOpen(bool withOptions)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            Task? closing = null;
            bool completedAtOnce = false;
            Exception? metrics = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() =>
                    {
                        closing = Close(consumer, withOptions);
                        completedAtOnce = closing.IsCompleted;
                        metrics = Record.Exception(() => consumer.Metrics());
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.True(completedAtOnce, "Close waited inside the listener");
            Assert.Null(metrics);
            await AssertRejected(closing!);
            await AssertStillOpenThenCloses(consumer);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_CloseInsideAnAsyncListener_AfterAnAwait_FaultsAtOnce_AndTheConsumerStaysOpen(
        bool withOptions)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            Task? closing = null;
            bool completedAtOnce = false;
            Exception? metrics = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new AsyncListener(async () =>
                    {
                        await Task.Yield();
                        closing = Close(consumer, withOptions);
                        completedAtOnce = closing.IsCompleted;
                        metrics = Record.Exception(() => consumer.Metrics());
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.True(completedAtOnce, "Close waited inside the listener");
            Assert.Null(metrics);
            await AssertRejected(closing!);
            await AssertStillOpenThenCloses(consumer);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_DisposeInsideASyncListener_ReturnsAtOnce_ThenTheConsumerCloses(bool disposeAsync)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            Task? disposing = null;
            Exception? inside = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() => inside = Record.Exception(() =>
                    {
                        disposing = DisposeInside(consumer, disposeAsync);
                    }))),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.Null(inside);
            Assert.True(disposing!.IsCompleted, "Dispose waited inside the listener");
            SpinUntilClosing(consumer);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }

        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_DisposeInsideAnAsyncListener_AfterAnAwait_ReturnsAtOnce_ThenTheConsumerCloses(
        bool disposeAsync)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            Task? disposing = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new AsyncListener(async () =>
                    {
                        await Task.Yield();
                        disposing = DisposeInside(consumer, disposeAsync);
                        await disposing;
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.True(disposing!.IsCompleted, "Dispose waited inside the listener");
            SpinUntilClosing(consumer);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }

        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public async Task AsyncMock_CloseInsideAnAsyncListener_WhileAnotherCallDisposes_CompletesWithoutEffect()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? inside = null;
        await TestTimeout.Run(
            () => consumer.Subscribe(
                new[] { Topic },
                new AsyncListener(async () =>
                {
                    entered.Set();
                    await release.Task;
                    inside = await Record.ExceptionAsync(() => consumer.Close());
                })),
            s_deadline);

        Task rebalance = consumer.Rebalance(new[] { s_partition });
        Assert.True(entered.Wait(s_deadline), "the listener never ran");
        Task disposing = consumer.DisposeAsync().AsTask();
        try
        {
            SpinUntilClosing(consumer);
        }
        finally
        {
            release.TrySetResult(true);
        }

        await TestTimeout.Run(() => rebalance, s_deadline);
        await TestTimeout.Run(() => disposing, s_deadline);
        Assert.Null(inside);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public async Task AsyncMock_CloseInsideASyncListener_IsRefused_AndAnotherThreadsCloseIsRefusedAtOnce_TheConsumerStaysOpen()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            using ManualResetEventSlim go = new ManualResetEventSlim(false);
            using ManualResetEventSlim returned = new ManualResetEventSlim(false);
            Task? other = null;
            Thread closer = new Thread(() =>
            {
                if (go.Wait(s_deadline))
                {
                    other = consumer.Close();
                    returned.Set();
                }
            });
            closer.IsBackground = true;
            closer.Start();

            Task? inside = null;
            bool refusedAtOnce = false;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() =>
                    {
                        go.Set();
                        refusedAtOnce = returned.Wait(s_bound);
                        inside = consumer.Close();
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.True(closer.Join(s_deadline), "the other thread's close did not end");
            Assert.True(refusedAtOnce, "the other thread's close waited for the rebalance");
            Assert.NotNull(inside);
            await TestTimeout.Run(() => AssertRejected(other!), s_deadline);
            await TestTimeout.Run(() => AssertRejected(inside!), s_deadline);
            await AssertStillOpenThenCloses(consumer);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static void Close(MockConsumer<byte[], byte[]> consumer, bool withOptions)
    {
        if (withOptions)
        {
            consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(1)));
        }
        else
        {
            consumer.Close();
        }
    }

    private static Task Close(AsyncMockConsumer<byte[], byte[]> consumer, bool withOptions) =>
        withOptions ? consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(1))) : consumer.Close();

    private static Task DisposeInside(AsyncMockConsumer<byte[], byte[]> consumer, bool disposeAsync)
    {
        if (disposeAsync)
        {
            return consumer.DisposeAsync().AsTask();
        }

        consumer.Dispose();
        return Task.CompletedTask;
    }

    private static async Task AssertRejected(Task closing)
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(() => closing);
        Assert.Equal(ConcurrentMessage, rejected.Message);
    }

    private static async Task AssertStillOpenThenCloses(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        Assert.Equal(new[] { s_partition }, consumer.Assignment());
        await TestTimeout.Run(() => consumer.Close(), s_deadline);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
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

    private static void SpinUntilClosing(IConsumerCommon consumer)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                consumer.Assignment();
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (InvalidOperationException)
            {
            }

            if (elapsed.Elapsed > s_deadline)
            {
                throw new TimeoutException("the consumer never began to close");
            }

            Thread.Yield();
        }
    }

    /// <summary>A listener that runs the action it is given when partitions are assigned.</summary>
    private sealed class SyncListener : IConsumerRebalanceListener
    {
        private readonly Action _onAssigned;

        internal SyncListener(Action onAssigned) => _onAssigned = onAssigned;

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>An async listener that runs the function it is given when partitions are assigned.</summary>
    private sealed class AsyncListener : IAsyncConsumerRebalanceListener
    {
        private readonly Func<Task> _onAssigned;

        internal AsyncListener(Func<Task> onAssigned) => _onAssigned = onAssigned;

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }
}
