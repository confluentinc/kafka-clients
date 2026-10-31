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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IAsyncConsumer.Position(TopicPartition, CancellationToken)"/> on the <see cref="AsyncMockConsumer"/>:
/// the position after a seek, the unassigned-partition fault, the preconditions, cancellation and reuse.
/// </summary>
public sealed class PublicConsumerPositionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "position-topic";
    private const int Partition = 0;

    private static async Task<AsyncMockConsumer<byte[], byte[]>> ReadyForPosition(
        long seekOffset, string topic = Topic, int partition = Partition)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(
            () => consumer.Assign(new[] { new TopicPartition(topic, partition) }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(new TopicPartition(topic, partition), seekOffset), s_deadline);
        return consumer;
    }

    [Fact]
    public async Task Position_AfterAssignAndSeek_ReturnsSoughtOffset()
    {
        const long offset = 42;
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(offset);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));

        Assert.Equal(offset, position);
    }

    [Fact]
    public async Task Position_NonAsciiTopic_ReturnsSoughtOffset()
    {
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        const long offset = 11;
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(offset, nonAsciiTopic);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        long position = await PositionOf(consumer, new TopicPartition(nonAsciiTopic, Partition));

        Assert.Equal(offset, position);
    }

    [Fact]
    public async Task Position_UnassignedPartition_FaultsWithArgumentExceptionMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition unassigned = new TopicPartition(Topic, 5);

        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(
            () => PositionOf(consumer, unassigned));

        Assert.Equal(
            "You can only check the position for partitions assigned to this consumer.",
            ex.Message);
    }

    [Fact]
    public async Task Position_UnassignedPartition_FaultsThenConsumerReusable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 3);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentException>(
            () => PositionOf(consumer, new TopicPartition(Topic, 9)));

        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));
        Assert.Equal(3, position);
    }

    [Fact]
    public async Task Position_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.Position(new TopicPartition(Topic, Partition), cts.Token));
    }

    [Fact]
    public async Task Position_AfterWakeup_StillUsable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 5);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.Wakeup();

        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));
        Assert.Equal(5, position);
    }

    [Fact]
    public async Task Position_NullTopic_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.Position(default));
    }

    [Fact]
    public async Task Position_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.Position(new TopicPartition(Topic, Partition)));
    }

    [Fact]
    public async Task Position_ReachableSeam_RoundTripsOnFreeGuard()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 8);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        long first = await PositionOf(consumer, new TopicPartition(Topic, Partition));
        long second = await PositionOf(consumer, new TopicPartition(Topic, Partition));

        Assert.Equal(8, first);
        Assert.Equal(8, second);
    }

    private static async Task<long> PositionOf(IAsyncConsumer<byte[], byte[]> consumer, TopicPartition partition)
    {
        long result = 0;
        await TestTimeout.Run(async () => result = await consumer.Position(partition), s_deadline);
        return result;
    }
}
