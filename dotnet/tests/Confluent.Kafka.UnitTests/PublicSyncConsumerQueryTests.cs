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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync queries on the <see cref="MockConsumer"/>: <see cref="IConsumer.Committed"/>,
/// <see cref="IConsumer.OffsetsForTimes"/>, <see cref="IConsumer.BeginningOffsets"/>,
/// <see cref="IConsumer.EndOffsets"/>, <see cref="IConsumer.PartitionsFor"/> and <see cref="IConsumer.ListTopics"/>,
/// each returning its copied-out result or throwing synchronously.
/// </summary>
/// <remarks>
/// The mock answers every query at once, so the tests call it directly, outside <see cref="TestTimeout"/>, which would
/// wrap a synchronous throw in an <see cref="AggregateException"/>. The mock's <c>OffsetsForTimes</c> always throws
/// Java's "Not implemented yet.", so only the throw is asserted.
/// </remarks>
public sealed class PublicSyncConsumerQueryTests
{
    private const string Topic = "sync-query-topic";
    private const string LeaderHost = "broker-1.example.com";
    private const int LeaderId = 7;
    private const int LeaderPort = 9092;

    [Fact]
    public void Committed_AfterCommit_RoundTripsOffsetMetadataAndEpoch()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        consumer.Assign(new[] { tp });

        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [tp] = new OffsetAndMetadata(42, "meta-x", 7),
        });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = consumer.Committed(new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(42, result[tp]!.Offset);
        Assert.Equal("meta-x", result[tp]!.Metadata);
        Assert.Equal(7, result[tp]!.LeaderEpoch);
    }

    [Fact]
    public void Committed_NullMetadataAndEpoch_RoundTripsAsEmptyMetadataAndNullEpoch()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 1);
        consumer.Assign(new[] { tp });

        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(100) });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = consumer.Committed(new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(100, result[tp]!.Offset);
        Assert.Equal(string.Empty, result[tp]!.Metadata);
        Assert.Null(result[tp]!.LeaderEpoch);
    }

    [Fact]
    public void Committed_UncommittedPartition_ReturnsEmptyMap()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = consumer.Committed(new[] { new TopicPartition(Topic, 0) });

        Assert.Empty(result);
    }

    [Fact]
    public void Committed_EmptyCollection_ReturnsEmptyMap()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Empty(consumer.Committed(Array.Empty<TopicPartition>()));
    }

    [Fact]
    public void OffsetsForTimes_OnMock_ThrowsNotSupportedException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = 1_000L,
        };

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => consumer.OffsetsForTimes(search));

        Assert.Equal("Not implemented yet.", ex.Message);
    }

    [Fact]
    public void OffsetsForTimes_NegativeTimestamp_Accepted_ThenThrowsOnMock()
    {
        // A negative timestamp is a valid sentinel (earliest or latest), so it reaches the mock.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = -2L, // LATEST sentinel
        };

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => consumer.OffsetsForTimes(search));

        Assert.Equal("Not implemented yet.", ex.Message);
    }

    [Fact]
    public void OffsetsForTimes_EmptyMap_StillThrowsOnMock()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Throws<NotSupportedException>(() => consumer.OffsetsForTimes(new Dictionary<TopicPartition, long>()));
    }

    [Fact]
    public void BeginningOffsets_ReturnsSetOffsets()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset(Topic, 0, 5);
        consumer.UpdateBeginningOffset(Topic, 1, 7);

        IReadOnlyDictionary<TopicPartition, long> result = consumer.BeginningOffsets(
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });

        Assert.Equal(2, result.Count);
        Assert.Equal(5, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(7, result[new TopicPartition(Topic, 1)]);
    }

    [Fact]
    public void BeginningOffsets_UnsetPartition_ThrowsInvalidOperationExceptionMessage()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal($"The partition {Topic}-0 does not have a beginning offset.", ex.Message);
    }

    [Fact]
    public void BeginningOffsets_EmptyCollection_ReturnsEmptyMap()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Empty(consumer.BeginningOffsets(Array.Empty<TopicPartition>()));
    }

    [Fact]
    public void BeginningOffsets_NonAsciiTopic_RoundTripsThroughKeyMarshalling()
    {
        const string nonAscii = "topic-grüße-Ω-🎉";
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset(nonAscii, 3, 99);

        IReadOnlyDictionary<TopicPartition, long> result = consumer.BeginningOffsets(
            new[] { new TopicPartition(nonAscii, 3) });

        Assert.Equal(99, result[new TopicPartition(nonAscii, 3)]);
    }

    [Fact]
    public void EndOffsets_ReturnsSetOffsets()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset(Topic, 0, 100);
        consumer.UpdateEndOffset(Topic, 2, 250);

        IReadOnlyDictionary<TopicPartition, long> result = consumer.EndOffsets(
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 2) });

        Assert.Equal(2, result.Count);
        Assert.Equal(100, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(250, result[new TopicPartition(Topic, 2)]);
    }

    [Fact]
    public void EndOffsets_UnsetPartition_ThrowsInvalidOperationExceptionMessage()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => consumer.EndOffsets(new[] { new TopicPartition(Topic, 4) }));

        Assert.Equal($"The partition {Topic}-4 does not have an end offset.", ex.Message);
    }

    [Fact]
    public void EndOffsets_AfterThrow_ConsumerReusable()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset(Topic, 0, 11);

        Assert.Throws<InvalidOperationException>(() => consumer.EndOffsets(new[] { new TopicPartition(Topic, 9) }));

        IReadOnlyDictionary<TopicPartition, long> result = consumer.EndOffsets(new[] { new TopicPartition(Topic, 0) });
        Assert.Equal(11, result[new TopicPartition(Topic, 0)]);
    }

    [Fact]
    public void PartitionsFor_AfterUpdatePartitions_ReturnsOwnedCopies()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 3, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = consumer.PartitionsFor(Topic);

        Assert.Equal(3, partitions.Count);
        for (int p = 0; p < 3; p++)
        {
            PartitionInfo info = FindPartition(partitions, p);
            Assert.Equal(Topic, info.Topic);
            Assert.Equal(p, info.Partition);

            Assert.NotNull(info.Leader);
            Assert.Equal(LeaderId, info.Leader!.Id);
            Assert.Equal(LeaderHost, info.Leader.Host);
            Assert.Equal(LeaderPort, info.Leader.Port);

            Node replica = Assert.Single(info.Replicas);
            Assert.Equal(LeaderId, replica.Id);
            Assert.Equal(LeaderHost, replica.Host);

            Node isr = Assert.Single(info.InSyncReplicas);
            Assert.Equal(LeaderId, isr.Id);

            Assert.Empty(info.OfflineReplicas);
            Assert.Null(info.Leader.Rack);
        }
    }

    [Fact]
    public void PartitionsFor_UnregisteredTopic_ReturnsEmptyList()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Empty(consumer.PartitionsFor("no-such-topic"));
    }

    [Fact]
    public void PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        // An empty topic reaches the core, as in Java and Python; only null is rejected.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Empty(consumer.PartitionsFor(string.Empty));
    }

    [Fact]
    public void PartitionsFor_NonAsciiLeaderHost_RoundTripsThroughNulScanHostMarshalling()
    {
        const string nonAsciiHost = "hôte-Ω-🎉.example.com";
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, nonAsciiHost, LeaderPort);

        PartitionInfo info = Assert.Single(consumer.PartitionsFor(Topic));
        Assert.Equal(nonAsciiHost, info.Leader!.Host);
        Assert.Equal(nonAsciiHost, info.Replicas[0].Host);
    }

    [Fact]
    public void ListTopics_AfterUpdatePartitions_ReturnsMapKeyedByTopic()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions("topic-a", partitionCount: 2, LeaderId, LeaderHost, LeaderPort);
        consumer.UpdatePartitions("topic-b", partitionCount: 1, leaderId: 9, "broker-2", leaderPort: 9093);

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = consumer.ListTopics();

        Assert.Equal(2, map.Count);

        IReadOnlyList<PartitionInfo> a = map["topic-a"];
        Assert.Equal(2, a.Count);
        Assert.Equal("topic-a", a[0].Topic);
        Assert.Equal(LeaderId, a[0].Leader!.Id);

        IReadOnlyList<PartitionInfo> b = map["topic-b"];
        PartitionInfo bInfo = Assert.Single(b);
        Assert.Equal("topic-b", bInfo.Topic);
        Assert.Equal(9, bInfo.Leader!.Id);
        Assert.Equal("broker-2", bInfo.Leader.Host);
        Assert.Equal(9093, bInfo.Leader.Port);
    }

    [Fact]
    public void ListTopics_NoTopics_ReturnsEmptyMap()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Empty(consumer.ListTopics());
    }

    [Fact]
    public void Committed_NullCollection_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Committed(null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Fact]
    public void BeginningOffsets_NullCollection_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.BeginningOffsets(null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Fact]
    public void EndOffsets_NullCollection_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.EndOffsets(null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Fact]
    public void OffsetsForTimes_NullMap_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.OffsetsForTimes(null!));
        Assert.Equal("timestampsToSearch", ex.ParamName);
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void Committed_NullElementTopic_ThrowsArgument()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => consumer.Committed(new[] { default(TopicPartition) }));
        Assert.Equal("partitions", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OffsetsForTimes_NullKeyTopic_ThrowsArgument()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long> { [default] = 1L };

        ArgumentException ex = Assert.Throws<ArgumentException>(() => consumer.OffsetsForTimes(search));
        Assert.Equal("timestampsToSearch", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Committed_NullCollection_OnAClosedConsumer_ReportsTheClosedConsumerFirst()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.Committed(null!));
        Assert.Throws<ObjectDisposedException>(() => consumer.Committed(null!, TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void PartitionsFor_NullTopic_OnAClosedConsumer_ReportsTheClosedConsumerFirst()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.PartitionsFor(null!));
        Assert.Throws<ObjectDisposedException>(() => consumer.PartitionsFor(null!, TimeSpan.Zero));
    }

    [Fact]
    public void Committed_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.Committed(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public void OffsetsForTimes_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.OffsetsForTimes(
            new Dictionary<TopicPartition, long> { [new TopicPartition(Topic, 0)] = 1L }));
    }

    [Fact]
    public void BeginningOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public void EndOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.EndOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public void PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.PartitionsFor(Topic));
    }

    [Fact]
    public void ListTopics_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.ListTopics());
    }

    [Fact]
    public void QueryFamily_RepeatedCalls_StayReusable()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);
        consumer.UpdateBeginningOffset(Topic, 0, 3);

        for (int i = 0; i < 5; i++)
        {
            Assert.Single(consumer.PartitionsFor(Topic));
            Assert.Single(consumer.ListTopics());
            Assert.Equal(3, consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) })[new TopicPartition(Topic, 0)]);
        }
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
}
