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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Which calls count as made inside a consumer's sync callback: only those on the thread running it, while it runs,
/// and never those on another consumer.
/// </summary>
public sealed class PublicConsumerCallbackScopeTests
{
    private const string Topic = "callback-scope-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_partition = new TopicPartition(Topic, 0);

    [Fact]
    public void SyncMock_AThreadASyncListenerStarted_ClosesTheConsumer_AfterTheListenerReturned()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using ManualResetEventSlim delivered = new ManualResetEventSlim(false);
        Thread? started = null;
        Exception? closing = null;
        bool closed = false;
        try
        {
            consumer.Subscribe(
                new[] { Topic },
                new SyncListener(() =>
                {
                    started = new Thread(() =>
                    {
                        if (delivered.Wait(s_deadline))
                        {
                            closing = Record.Exception(consumer.Close);
                            closed = closing is null;
                        }
                    });
                    started.IsBackground = true;
                    started.Start();
                }));

            TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);
            delivered.Set();

            Assert.True(started!.Join(s_deadline), "the started thread never ended");
            Assert.Null(closing);
            Assert.True(closed, "the started thread never closed the consumer");
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            delivered.Set();
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_ATaskASyncListenerStarted_ClosesTheConsumer_AfterTheRebalanceEnded()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        TaskCompletionSource<bool> delivered = NewSignal();
        Task<Task>? started = null;
        try
        {
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() => started = Task.Run(async () =>
                    {
                        await delivered.Task.ConfigureAwait(false);
                        return CloseCatching(consumer);
                    }))),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { s_partition }), s_deadline);
            delivered.TrySetResult(true);

            Task closing = await TestTimeout.Run(() => started!, s_deadline);
            await TestTimeout.Run(() => closing, s_deadline);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            delivered.TrySetResult(true);
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public void KafkaConsumer_AThreadASyncCommitCallbackStarted_ClosesTheConsumer_AfterTheDeliveringCallEnded()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        using ManualResetEventSlim delivered = new ManualResetEventSlim(false);
        Thread? started = null;
        Exception? closing = null;
        bool closed = false;
        try
        {
            consumer.CommitAsync(
                Empty(),
                new DelegateCallback(() =>
                {
                    started = new Thread(() =>
                    {
                        if (delivered.Wait(s_deadline))
                        {
                            closing = Record.Exception(consumer.Close);
                            closed = closing is null;
                        }
                    });
                    started.IsBackground = true;
                    started.Start();
                }));

            TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            Assert.NotNull(started);
            delivered.Set();

            Assert.True(started!.Join(s_deadline), "the started thread never ended");
            Assert.Null(closing);
            Assert.True(closed, "the started thread never closed the consumer");
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            delivered.Set();
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_AThreadASyncCommitCallbackStarted_ClosesTheConsumer_AfterTheCallbackReturned()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        using ManualResetEventSlim delivered = new ManualResetEventSlim(false);
        using ManualResetEventSlim ran = new ManualResetEventSlim(false);
        Thread? started = null;
        Task? closing = null;
        try
        {
            await TestTimeout.Run(
                () => consumer.CommitAsync(
                    Empty(),
                    new DelegateCallback(() =>
                    {
                        started = new Thread(() =>
                        {
                            if (delivered.Wait(s_deadline))
                            {
                                closing = CloseCatching(consumer);
                            }
                        });
                        started.IsBackground = true;
                        started.Start();
                        ran.Set();
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            Assert.True(ran.IsSet, "the commit callback did not run inside the delivering call");
            delivered.Set();

            Assert.True(started!.Join(s_deadline), "the started thread never ended");
            await TestTimeout.Run(() => closing!, s_deadline);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            delivered.Set();
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task SyncMock_AListenerOfOneConsumer_ClosesAnother()
    {
        MockConsumer<byte[], byte[]> outer = NewMock();
        AsyncMockConsumer<byte[], byte[]> other = NewAsyncMock();
        try
        {
            Task? closing = null;
            outer.Subscribe(new[] { Topic }, new SyncListener(() => closing = CloseCatching(other)));

            TestTimeout.Run(() => outer.Rebalance(new[] { s_partition }), s_deadline);

            await TestTimeout.Run(() => closing!, s_deadline);
            Assert.Throws<ObjectDisposedException>(() => other.Assignment());
            Assert.Equal(new[] { s_partition }, outer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => other.DisposeAsync().AsTask(), s_deadline);
            TestTimeout.Run(outer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_ASyncListenerOfOneConsumer_ClosesASyncMock()
    {
        AsyncMockConsumer<byte[], byte[]> outer = NewAsyncMock();
        MockConsumer<byte[], byte[]> other = NewMock();
        try
        {
            Exception? closing = null;
            bool ran = false;
            await TestTimeout.Run(
                () => outer.Subscribe(
                    new[] { Topic },
                    new SyncListener(() =>
                    {
                        closing = Record.Exception(other.Close);
                        ran = true;
                    })),
                s_deadline);

            await TestTimeout.Run(() => outer.Rebalance(new[] { s_partition }), s_deadline);

            Assert.True(ran, "the listener never ran");
            Assert.Null(closing);
            Assert.Throws<ObjectDisposedException>(() => other.Assignment());
            Assert.Equal(new[] { s_partition }, outer.Assignment());
        }
        finally
        {
            TestTimeout.Run(other.Dispose, s_deadline);
            await TestTimeout.Run(() => outer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_ASyncListenerOfOneConsumer_ClosesAnother()
    {
        AsyncMockConsumer<byte[], byte[]> outer = NewAsyncMock();
        AsyncMockConsumer<byte[], byte[]> other = NewAsyncMock();
        try
        {
            Task? closing = null;
            await TestTimeout.Run(
                () => outer.Subscribe(new[] { Topic }, new SyncListener(() => closing = CloseCatching(other))),
                s_deadline);

            await TestTimeout.Run(() => outer.Rebalance(new[] { s_partition }), s_deadline);

            await TestTimeout.Run(() => closing!, s_deadline);
            Assert.Throws<ObjectDisposedException>(() => other.Assignment());
            Assert.Equal(new[] { s_partition }, outer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => other.DisposeAsync().AsTask(), s_deadline);
            await TestTimeout.Run(() => outer.DisposeAsync().AsTask(), s_deadline);
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

    private static TaskCompletionSource<bool> NewSignal() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "callback-scope-group",
        ["enable.auto.commit"] = "false",
    };

    private static KafkaConsumer<byte[], byte[]> NewKafkaConsumer() =>
        new KafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncKafkaConsumer<byte[], byte[]> NewAsyncKafkaConsumer() =>
        new AsyncKafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

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

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal DelegateCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }
}
