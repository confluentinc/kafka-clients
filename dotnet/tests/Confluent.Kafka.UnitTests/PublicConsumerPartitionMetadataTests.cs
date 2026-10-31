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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IAsyncConsumer.PartitionsFor"/> and <see cref="IAsyncConsumer.ListTopics"/> on the
/// <see cref="AsyncMockConsumer"/>: the copied-out partitions, nodes and strings, the preconditions, cancellation and
/// reuse.
/// </summary>
/// <remarks>
/// The mock gives each partition one leader, which is also its only replica and in-sync replica, with no offline
/// replica and no rack, and neither query fails on it.
/// </remarks>
public sealed class PublicConsumerPartitionMetadataTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "partition-metadata-topic";
    private const string LeaderHost = "broker-1.example.com";
    private const int LeaderId = 7;
    private const int LeaderPort = 9092;

    [Fact]
    public async Task PartitionsFor_AfterUpdatePartitions_ReturnsReachableFields()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 3, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);

        Assert.Equal(3, partitions.Count);
        for (int p = 0; p < 3; p++)
        {
            PartitionInfo info = FindPartition(partitions, p);

            Assert.Equal(Topic, info.Topic);
            Assert.Equal(p, info.Partition);

            Assert.NotNull(info.Leader);
            Node leader = info.Leader!;
            Assert.Equal(LeaderId, leader.Id);
            Assert.Equal(LeaderHost, leader.Host);
            Assert.Equal(LeaderPort, leader.Port);

            Node replica = Assert.Single(info.Replicas);
            Assert.Equal(LeaderId, replica.Id);
            Assert.Equal(LeaderHost, replica.Host);
            Assert.Equal(LeaderPort, replica.Port);

            Node isr = Assert.Single(info.InSyncReplicas);
            Assert.Equal(LeaderId, isr.Id);
            Assert.Equal(LeaderHost, isr.Host);

            Assert.Empty(info.OfflineReplicas);
            Assert.Null(leader.Rack);
        }
    }

    [Fact]
    public async Task PartitionsFor_UnregisteredTopic_ReturnsEmptyList()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, "no-such-topic");

        Assert.Empty(partitions);
    }

    [Fact]
    public async Task PartitionsFor_NonAsciiTopic_RoundTripsThroughNulScanTopicMarshalling()
    {
        const string nonAscii = "topic-grüße-Ω-🎉";
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(nonAscii, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, nonAscii);

        PartitionInfo info = Assert.Single(partitions);
        Assert.Equal(nonAscii, info.Topic);
    }

    [Fact]
    public async Task PartitionsFor_NonAsciiLeaderHost_RoundTripsThroughNulScanHostMarshalling()
    {
        const string nonAsciiHost = "hôte-Ω-🎉.example.com";
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, nonAsciiHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);

        PartitionInfo info = Assert.Single(partitions);
        Assert.Equal(nonAsciiHost, info.Leader!.Host);
        Assert.Equal(nonAsciiHost, info.Replicas[0].Host);
    }

    [Fact]
    public async Task PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        // An empty topic reaches the core, as in Java and Python; only null is rejected.
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, string.Empty);

        Assert.Empty(partitions);
    }

    [Fact]
    public async Task ListTopics_AfterUpdatePartitions_ReturnsMapKeyedByTopic()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions("topic-a", partitionCount: 2, LeaderId, LeaderHost, LeaderPort);
        consumer.UpdatePartitions("topic-b", partitionCount: 1, leaderId: 9, "broker-2", leaderPort: 9093);

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.Equal(2, map.Count);

        IReadOnlyList<PartitionInfo> a = map["topic-a"];
        Assert.Equal(2, a.Count);
        Assert.Equal("topic-a", a[0].Topic);
        Node aLeader = a[0].Leader!;
        Assert.Equal(LeaderId, aLeader.Id);
        Assert.Equal(LeaderHost, aLeader.Host);

        IReadOnlyList<PartitionInfo> b = map["topic-b"];
        PartitionInfo bInfo = Assert.Single(b);
        Assert.Equal("topic-b", bInfo.Topic);
        Node bLeader = bInfo.Leader!;
        Assert.Equal(9, bLeader.Id);
        Assert.Equal("broker-2", bLeader.Host);
        Assert.Equal(9093, bLeader.Port);
    }

    [Fact]
    public async Task ListTopics_NoTopics_ReturnsEmptyMap()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.Empty(map);
    }

    [Fact]
    public async Task ListTopics_NonAsciiTopicKey_RoundTripsThroughNulScanTopicMarshalling()
    {
        const string nonAscii = "тема-Ω-🎉";
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(nonAscii, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.True(map.ContainsKey(nonAscii));
        Assert.Equal(nonAscii, map[nonAscii][0].Topic);
    }

    [Fact]
    public async Task PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.PartitionsFor(null!));
    }

    [Fact]
    public async Task PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.PartitionsFor(Topic));
    }

    [Fact]
    public async Task ListTopics_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.ListTopics());
    }

    [Fact]
    public async Task PartitionsFor_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.PartitionsFor(Topic, cts.Token));
    }

    [Fact]
    public async Task ListTopics_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.ListTopics(cts.Token));
    }

    [Fact]
    public async Task PartitionsFor_AfterWakeup_StillUsable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        consumer.Wakeup();

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);
        Assert.Single(partitions);
    }

    [Fact]
    public async Task ListTopics_AfterFaultedPartitionsFor_StillReturnsData()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        for (int i = 0; i < 5; i++)
        {
            IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);
            Assert.Single(partitions);
        }

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);
        Assert.True(map.ContainsKey(Topic));
    }

    private static PartitionInfo FindPartition(IReadOnlyList<PartitionInfo> partitions, int partition)
    {
        foreach (PartitionInfo info in partitions)
        {
            if (info.Partition == partition)
            {
                return info;
            }
        }

        throw new Xunit.Sdk.XunitException($"No PartitionInfo for partition {partition}.");
    }

    private static async Task<IReadOnlyList<PartitionInfo>> PartitionsForOf(IAsyncConsumer<byte[], byte[]> consumer, string topic)
    {
        IReadOnlyList<PartitionInfo> result = null!;
        await TestTimeout.Run(async () => result = await consumer.PartitionsFor(topic), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopicsOf(
        IAsyncConsumer<byte[], byte[]> consumer)
    {
        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> result = null!;
        await TestTimeout.Run(async () => result = await consumer.ListTopics(), s_deadline);
        return result;
    }
}
