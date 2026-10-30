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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IAsyncConsumer.Assign"/>, <see cref="IAsyncConsumer.Pause"/>, <see cref="IAsyncConsumer.Resume"/>,
/// <see cref="IAsyncConsumer.SeekToBeginning"/> and <see cref="IAsyncConsumer.SeekToEnd"/> on the
/// <see cref="AsyncMockConsumer"/>.
/// </summary>
/// <remarks>
/// As in Java, an empty collection clears the assignment for <c>Assign</c> and changes nothing for the other four; a
/// <see langword="null"/> collection is rejected.
/// </remarks>
public sealed class PublicConsumerPartitionOpsTests
{
    private const string Topic = "partition-ops-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(200);

    [Fact]
    public async Task Assign_EmptyCollection_ClearsTheAssignment()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, 0) }), s_deadline);
        Assert.Single(consumer.Assignment());

        await TestTimeout.Run(() => consumer.Assign(Array.Empty<TopicPartition>()), s_deadline);

        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public async Task Pause_ThenPaused_ReturnsThePausedSet()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        await TestTimeout.Run(() => consumer.Pause(new[] { tp }), s_deadline);

        TopicPartition only = Assert.Single(consumer.Paused());
        Assert.Equal(tp, only);
    }

    [Fact]
    public async Task Resume_AfterPause_ClearsThePausedSet()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);
        await TestTimeout.Run(() => consumer.Pause(new[] { tp }), s_deadline);
        Assert.Single(consumer.Paused());

        await TestTimeout.Run(() => consumer.Resume(new[] { tp }), s_deadline);

        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public async Task SeekToBeginning_ResolvesBrokerFree_NoOffsetSetup()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        await TestTimeout.Run(() => consumer.SeekToBeginning(new[] { tp }), s_deadline);
    }

    [Fact]
    public async Task SeekToEnd_ResolvesBrokerFree_NoOffsetSetup()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        await TestTimeout.Run(() => consumer.SeekToEnd(new[] { tp }), s_deadline);
    }

    [Fact]
    public async Task SeekToBeginning_ObservableViaPoll_ResetsToBeginningOffset()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        consumer.UpdateBeginningOffset(Topic, 0, offset: 5);
        for (int i = 0; i < 10; i++)
        {
            consumer.AddRecord(Topic, 0, offset: i, key: null, value: new byte[] { (byte)i });
        }

        await TestTimeout.Run(() => consumer.SeekToBeginning(new[] { tp }), s_deadline);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        long[] offsets = records.Select(r => r.Offset).OrderBy(o => o).ToArray();
        Assert.Equal(new long[] { 5, 6, 7, 8, 9 }, offsets);
    }

    [Fact]
    public async Task SeekToEnd_ObservableViaPoll_ResetsToEndOffset()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        consumer.UpdateEndOffset(Topic, 0, offset: 8);
        for (int i = 0; i < 10; i++)
        {
            consumer.AddRecord(Topic, 0, offset: i, key: null, value: new byte[] { (byte)i });
        }

        await TestTimeout.Run(() => consumer.SeekToEnd(new[] { tp }), s_deadline);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        long[] offsets = records.Select(r => r.Offset).OrderBy(o => o).ToArray();
        Assert.Equal(new long[] { 8, 9 }, offsets);
    }

    [Fact]
    public async Task Pause_EmptyCollection_IsNoOpSuccess()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Pause(Array.Empty<TopicPartition>()), s_deadline);
        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public async Task Resume_EmptyCollection_IsNoOpSuccess()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Resume(Array.Empty<TopicPartition>()), s_deadline);
    }

    [Fact]
    public async Task SeekToBeginning_EmptyCollection_IsNoOpSuccess()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.SeekToBeginning(Array.Empty<TopicPartition>()), s_deadline);
    }

    [Fact]
    public async Task SeekToEnd_EmptyCollection_IsNoOpSuccess()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.SeekToEnd(Array.Empty<TopicPartition>()), s_deadline);
    }

    [Fact]
    public async Task Pause_UnassignedPartition_FaultsWithInvalidOperationExceptionMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        TopicPartition unassigned = new TopicPartition(Topic, 99);
        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => consumer.Pause(new[] { unassigned }), s_deadline));

        Assert.Contains("No current assignment for partition", ex.Message, StringComparison.Ordinal);
        Assert.Contains(unassigned.ToString(), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pause_UnassignedPartition_FaultsThenConsumerReusable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition assigned = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { assigned }), s_deadline);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => consumer.Pause(new[] { new TopicPartition(Topic, 99) }), s_deadline));

        await TestTimeout.Run(() => consumer.Pause(new[] { assigned }), s_deadline);
        Assert.Single(consumer.Paused());
    }

    [Fact]
    public async Task Ops_NullCollection_ThrowArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Assign(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Pause(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Resume(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.SeekToBeginning(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.SeekToEnd(null!));
    }

    [Fact]
    public async Task Ops_ElementWithNullTopic_ThrowArgumentException()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TopicPartition[] withNullTopic = { default };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Assign(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Pause(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Resume(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.SeekToBeginning(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.SeekToEnd(withNullTopic));
    }

    [Fact]
    public async Task Ops_AfterDispose_ThrowObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition[] tps = { new TopicPartition(Topic, 0) };
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Assign(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Pause(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Resume(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekToBeginning(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekToEnd(tps));
    }

    [Fact]
    public async Task Ops_PreCanceledToken_ThrowOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition[] tps = { new TopicPartition(Topic, 0) };
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Assign(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Pause(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Resume(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.SeekToBeginning(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.SeekToEnd(tps, cts.Token));
    }

    [Fact]
    public async Task Ops_AfterWakeup_ConsumerStillUsable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        consumer.Wakeup();

        await TestTimeout.Run(() => consumer.Pause(new[] { tp }), s_deadline);
        Assert.Single(consumer.Paused());
    }

    private static async Task<ConsumerRecords<byte[], byte[]>> Poll(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = null!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
