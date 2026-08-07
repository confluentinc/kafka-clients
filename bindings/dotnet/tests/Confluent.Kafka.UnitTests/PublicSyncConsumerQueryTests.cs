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
/// M5/P8b — the <b>synchronous</b> consumer query family exercised end to end through the
/// <b>public</b> <see cref="MockConsumer"/> / <see cref="IConsumer"/> surface (no broker):
/// <see cref="IConsumer.Committed"/> / <see cref="IConsumer.OffsetsForTimes"/> /
/// <see cref="IConsumer.BeginningOffsets"/> / <see cref="IConsumer.EndOffsets"/> /
/// <see cref="IConsumer.PartitionsFor"/> / <see cref="IConsumer.ListTopics"/>. The synchronous
/// mirror of <c>PublicConsumerOffsetQueryTests</c> + <c>PublicConsumerPartitionMetadataTests</c>:
/// each query returns its owned result directly (no <c>await</c>, no <c>CancellationToken</c>);
/// a failure is a <b>synchronous</b> <see cref="KafkaException"/> throw (not a faulted Task).
/// </summary>
/// <remarks>
/// <para>
/// <b>Reachability (mirrors the async phases' recorded ceilings).</b>
/// <see cref="IConsumer.Committed"/> is fully round-trippable broker-free now that the commit
/// family is wired (M5/P6): <see cref="IConsumer.Commit(IReadOnlyDictionary{TopicPartition, OffsetAndMetadata})"/>
/// stores into the mock's committed map, and <see cref="IConsumer.Committed"/> reads the exact
/// value back for an <b>assigned</b> partition (offset, metadata, and leader epoch).
/// <see cref="IConsumer.BeginningOffsets"/> / <see cref="IConsumer.EndOffsets"/> carry data via
/// the shipped <see cref="MockConsumer.UpdateBeginningOffset"/> /
/// <see cref="MockConsumer.UpdateEndOffset"/>; an unset partition throws <c>illegal_state</c>.
/// <see cref="IConsumer.OffsetsForTimes"/> throws <c>unsupported_version</c> unconditionally on
/// the mock (Java's not-implemented <c>MockConsumer</c>) — the honesty case: the mock CANNOT
/// round-trip it, so only the throw is asserted. <see cref="IConsumer.PartitionsFor"/> /
/// <see cref="IConsumer.ListTopics"/> carry data via <see cref="MockConsumer.UpdatePartitions"/>.
/// </para>
/// <para>
/// The query ops resolve <b>instantaneously</b> on the mock (no blocking, source-verified in
/// <c>src/consumer/mock_consumer.rs</c>), so they are called directly — no <see cref="TestTimeout"/>
/// wrapper (which would surface a synchronous throw as an <c>AggregateException</c>; the P8a
/// throwing-Poll precedent). Assembly-wide serial execution is inherited (the D8.8 gate).
/// </para>
/// </remarks>
public sealed class PublicSyncConsumerQueryTests
{
    private const string Topic = "sync-query-topic";
    private const string LeaderHost = "broker-1.example.com";
    private const int LeaderId = 7;
    private const int LeaderPort = 9092;

    // UnsupportedVersion (Errors::UnsupportedVersion) — the offsets_for_times mock fault code.
    private const int UnsupportedVersionCode = 35;

    // ---- Committed — the full 3-field round-trip (M5/P6 precedent, now sync) ----

    [Fact]
    public void Committed_AfterCommit_RoundTripsOffsetMetadataAndEpoch()
    {
        // The full round-trip: Assign (committed() returns the stored value only for an assigned
        // TP — the mock's `subscriptions.is_assigned(tp)` gate), Commit real offsets via the sync
        // 5-array marshaller, read them back via the sync OffsetMap copy-out. Offset, metadata,
        // AND leader epoch all round-trip faithfully (a TRUE round-trip, not a documented limit).
        using MockConsumer consumer = new MockConsumer();
        TopicPartition tp = new TopicPartition(Topic, 0);
        consumer.Assign(new[] { tp });

        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [tp] = new OffsetAndMetadata(42, "meta-x", 7),
        });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = consumer.Committed(new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(42, result[tp].Offset);
        Assert.Equal("meta-x", result[tp].Metadata);
        Assert.Equal(7, result[tp].LeaderEpoch);
    }

    [Fact]
    public void Committed_NullMetadataAndEpoch_RoundTripsAsEmptyMetadataAndNullEpoch()
    {
        // The null-metadata / null-epoch variant: the ctor coerces null metadata to "" and the
        // sync SnapshotCommitOffsets maps a null epoch to the -1 sentinel on the wire; the
        // copy-out maps -1 back to null. Metadata reads back as "".
        using MockConsumer consumer = new MockConsumer();
        TopicPartition tp = new TopicPartition(Topic, 1);
        consumer.Assign(new[] { tp });

        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(100) });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = consumer.Committed(new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(100, result[tp].Offset);
        Assert.Equal(string.Empty, result[tp].Metadata);
        Assert.Null(result[tp].LeaderEpoch);
    }

    [Fact]
    public void Committed_ViaIConsumerInterface_RoundTrips()
    {
        using MockConsumer mock = new MockConsumer();
        TopicPartition tp = new TopicPartition(Topic, 2);
        IConsumer consumer = mock;
        consumer.Assign(new[] { tp });
        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(5, "m") });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = consumer.Committed(new[] { tp });

        Assert.Equal(5, result[tp].Offset);
        Assert.Equal("m", result[tp].Metadata);
    }

    [Fact]
    public void Committed_UncommittedPartition_ReturnsEmptyMap()
    {
        // The mock omits TPs with no committed offset — an empty, non-null map (success).
        using MockConsumer consumer = new MockConsumer();

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = consumer.Committed(new[] { new TopicPartition(Topic, 0) });

        Assert.Empty(result);
    }

    [Fact]
    public void Committed_EmptyCollection_ReturnsEmptyMap()
    {
        using MockConsumer consumer = new MockConsumer();

        Assert.Empty(consumer.Committed(Array.Empty<TopicPartition>()));
    }

    // ---- OffsetsForTimes — the honesty case (unsupported on the mock → throws) ----

    [Fact]
    public void OffsetsForTimes_OnMock_ThrowsUnsupportedVersion()
    {
        // The mock's offsets_for_times returns unsupported_version UNCONDITIONALLY (Java's
        // not-implemented MockConsumer — verified in src/consumer/mock_consumer.rs). The sync
        // call THROWS a KafkaException (not a faulted Task). Assert the throw + the exact message
        // + the UnsupportedVersion code (35). Do NOT over-claim a success round-trip — the mock
        // cannot reach one.
        using MockConsumer consumer = new MockConsumer();
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = 1_000L,
        };

        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.OffsetsForTimes(search));

        Assert.Equal("MockConsumer::offsets_for_times is not implemented", ex.Message);
        Assert.Equal(UnsupportedVersionCode, ex.Code);
    }

    [Fact]
    public void OffsetsForTimes_NegativeTimestamp_Accepted_ThenThrowsOnMock()
    {
        // A NEGATIVE timestamp is a Kafka-valid sentinel (EARLIEST/LATEST) and must NOT be
        // rejected by a precondition — it passes through to the mock, which then throws
        // unsupported_version (proving the negative value was accepted, not thrown on).
        using MockConsumer consumer = new MockConsumer();
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = -2L, // LATEST sentinel
        };

        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.OffsetsForTimes(search));

        Assert.Equal("MockConsumer::offsets_for_times is not implemented", ex.Message);
    }

    [Fact]
    public void OffsetsForTimes_EmptyMap_StillThrowsOnMock()
    {
        // The FFI does not short-circuit empty — it always calls the mock, which throws
        // unsupported_version even for an empty request (the async precedent).
        using MockConsumer consumer = new MockConsumer();

        Assert.Throws<KafkaException>(() => consumer.OffsetsForTimes(new Dictionary<TopicPartition, long>()));
    }

    // ---- BeginningOffsets / EndOffsets — data-testable broker-free ----

    [Fact]
    public void BeginningOffsets_ReturnsSetOffsets()
    {
        using MockConsumer consumer = new MockConsumer();
        consumer.UpdateBeginningOffset(Topic, 0, 5);
        consumer.UpdateBeginningOffset(Topic, 1, 7);

        IReadOnlyDictionary<TopicPartition, long> result = consumer.BeginningOffsets(
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });

        Assert.Equal(2, result.Count);
        Assert.Equal(5, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(7, result[new TopicPartition(Topic, 1)]);
    }

    [Fact]
    public void BeginningOffsets_UnsetPartition_ThrowsKafkaExceptionMessage()
    {
        // A TP with no beginning offset set throws the mock's illegal_state message
        // (the behavioral contract, asserted per DoD §3), incl. the TP in the message.
        using MockConsumer consumer = new MockConsumer();

        KafkaException ex = Assert.Throws<KafkaException>(
            () => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal($"The partition {Topic}-0 does not have a beginning offset.", ex.Message);
    }

    [Fact]
    public void BeginningOffsets_EmptyCollection_ReturnsEmptyMap()
    {
        using MockConsumer consumer = new MockConsumer();

        Assert.Empty(consumer.BeginningOffsets(Array.Empty<TopicPartition>()));
    }

    [Fact]
    public void BeginningOffsets_NonAsciiTopic_RoundTripsThroughKeyMarshalling()
    {
        // The map KEY topic is copied out of the borrowed TopicPartition_t element (NUL-scan,
        // §B3); a non-ASCII topic exercises both the input pin AND the receive-path key copy.
        const string nonAscii = "topic-grüße-Ω-🎉";
        using MockConsumer consumer = new MockConsumer();
        consumer.UpdateBeginningOffset(nonAscii, 3, 99);

        IReadOnlyDictionary<TopicPartition, long> result = consumer.BeginningOffsets(
            new[] { new TopicPartition(nonAscii, 3) });

        Assert.Equal(99, result[new TopicPartition(nonAscii, 3)]);
    }

    [Fact]
    public void EndOffsets_ReturnsSetOffsets()
    {
        using MockConsumer consumer = new MockConsumer();
        consumer.UpdateEndOffset(Topic, 0, 100);
        consumer.UpdateEndOffset(Topic, 2, 250);

        IReadOnlyDictionary<TopicPartition, long> result = consumer.EndOffsets(
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 2) });

        Assert.Equal(2, result.Count);
        Assert.Equal(100, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(250, result[new TopicPartition(Topic, 2)]);
    }

    [Fact]
    public void EndOffsets_UnsetPartition_ThrowsKafkaExceptionMessage()
    {
        using MockConsumer consumer = new MockConsumer();

        KafkaException ex = Assert.Throws<KafkaException>(
            () => consumer.EndOffsets(new[] { new TopicPartition(Topic, 4) }));

        Assert.Equal($"The partition {Topic}-4 does not have an end offset.", ex.Message);
    }

    [Fact]
    public void EndOffsets_AfterThrow_ConsumerReusable()
    {
        // The failure is not fatal: after a thrown EndOffsets, a valid query still succeeds
        // (the error handle was freed exactly once, leaving the consumer usable).
        using MockConsumer consumer = new MockConsumer();
        consumer.UpdateEndOffset(Topic, 0, 11);

        Assert.Throws<KafkaException>(() => consumer.EndOffsets(new[] { new TopicPartition(Topic, 9) }));

        IReadOnlyDictionary<TopicPartition, long> result = consumer.EndOffsets(new[] { new TopicPartition(Topic, 0) });
        Assert.Equal(11, result[new TopicPartition(Topic, 0)]);
    }

    // ---- PartitionsFor — data-testable broker-free ----

    [Fact]
    public void PartitionsFor_AfterUpdatePartitions_ReturnsOwnedCopies()
    {
        using MockConsumer consumer = new MockConsumer();
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

            // Documented-empty mock slice — the marshaller paths are still exercised (empty / null).
            Assert.Empty(info.OfflineReplicas);
            Assert.Null(info.Leader.Rack);
        }
    }

    [Fact]
    public void PartitionsFor_ViaIConsumerInterface_ReturnsData()
    {
        using MockConsumer mock = new MockConsumer();
        mock.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IConsumer consumer = mock;
        PartitionInfo info = Assert.Single(consumer.PartitionsFor(Topic));
        Assert.Equal(Topic, info.Topic);
        Assert.Equal(LeaderId, info.Leader!.Id);
    }

    [Fact]
    public void PartitionsFor_UnregisteredTopic_ReturnsEmptyList()
    {
        using MockConsumer consumer = new MockConsumer();

        Assert.Empty(consumer.PartitionsFor("no-such-topic"));
    }

    [Fact]
    public void PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        // Java/Python-faithful: an EMPTY topic is FORWARDED to the core, not rejected client-side
        // (the binding guards only null). The mock has no partitions for "", so an empty list —
        // proving the empty topic reached the core (a rejection would have thrown instead).
        using MockConsumer consumer = new MockConsumer();

        Assert.Empty(consumer.PartitionsFor(string.Empty));
    }

    [Fact]
    public void PartitionsFor_NonAsciiLeaderHost_RoundTripsThroughLengthDelimitedMarshalling()
    {
        // Node_host is LENGTH-DELIMITED ((ptr, out_len), §B3). A non-ASCII host guards against a
        // NUL-scan over-read on the length-delimited slice.
        const string nonAsciiHost = "hôte-Ω-🎉.example.com";
        using MockConsumer consumer = new MockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, nonAsciiHost, LeaderPort);

        PartitionInfo info = Assert.Single(consumer.PartitionsFor(Topic));
        Assert.Equal(nonAsciiHost, info.Leader!.Host);
        Assert.Equal(nonAsciiHost, info.Replicas[0].Host);
    }

    // ---- ListTopics — data-testable broker-free ----

    [Fact]
    public void ListTopics_AfterUpdatePartitions_ReturnsMapKeyedByTopic()
    {
        using MockConsumer consumer = new MockConsumer();
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
    public void ListTopics_ViaIConsumerInterface_ReturnsData()
    {
        using MockConsumer mock = new MockConsumer();
        mock.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IConsumer consumer = mock;
        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = consumer.ListTopics();

        Assert.True(map.ContainsKey(Topic));
        Assert.Single(map[Topic]);
    }

    [Fact]
    public void ListTopics_NoTopics_ReturnsEmptyMap()
    {
        using MockConsumer consumer = new MockConsumer();

        Assert.Empty(consumer.ListTopics());
    }

    // ---- Preconditions (before any native call; exact messages; DoD §3) ----

    [Fact]
    public void Committed_NullCollection_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Committed(null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Fact]
    public void BeginningOffsets_NullCollection_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.BeginningOffsets(null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Fact]
    public void EndOffsets_NullCollection_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.EndOffsets(null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Fact]
    public void OffsetsForTimes_NullMap_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.OffsetsForTimes(null!));
        Assert.Equal("timestampsToSearch", ex.ParamName);
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void Committed_NullElementTopic_ThrowsArgument()
    {
        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way to
        // present a null element topic without TopicPartition's own ctor validation firing.
        using MockConsumer consumer = new MockConsumer();
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => consumer.Committed(new[] { default(TopicPartition) }));
        Assert.Equal("partitions", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OffsetsForTimes_NullKeyTopic_ThrowsArgument()
    {
        using MockConsumer consumer = new MockConsumer();
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long> { [default] = 1L };

        ArgumentException ex = Assert.Throws<ArgumentException>(() => consumer.OffsetsForTimes(search));
        Assert.Equal("timestampsToSearch", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryFamily_NegativePartition_RejectedByTopicPartitionCtor()
    {
        // TopicPartition's ctor rejects a negative partition itself, so a negative value cannot
        // reach any query through a constructed TopicPartition — assert the ctor guard is that
        // same exception type and message, which is what the query would throw were it smuggled in.
        ArgumentOutOfRangeException ex =
            Assert.Throws<ArgumentOutOfRangeException>(() => new TopicPartition(Topic, -1));
        Assert.Contains("Partition must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    // ---- Preconditions fire BEFORE the disposed check (even when closed) ----

    [Fact]
    public void Committed_NullCollection_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // The null-argument check (SnapshotPartitions) precedes ThrowIfClosed.
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ArgumentNullException>(() => consumer.Committed(null!));
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ArgumentNullException>(() => consumer.PartitionsFor(null!));
    }

    // ---- Lifecycle: post-dispose → ObjectDisposedException ----

    [Fact]
    public void Committed_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.Committed(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public void OffsetsForTimes_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.OffsetsForTimes(
            new Dictionary<TopicPartition, long> { [new TopicPartition(Topic, 0)] = 1L }));
    }

    [Fact]
    public void BeginningOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public void EndOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.EndOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public void PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.PartitionsFor(Topic));
    }

    [Fact]
    public void ListTopics_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.ListTopics());
    }

    // ---- Reusable-after-op seam (free-exactly-once): repeated queries do not leak / corrupt ----

    [Fact]
    public void QueryFamily_RepeatedCalls_StayReusable()
    {
        using MockConsumer consumer = new MockConsumer();
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
