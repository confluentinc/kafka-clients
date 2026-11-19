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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>equals</c>, <c>hashCode</c> and <c>toString</c> on <see cref="TopicDescription"/>,
/// <see cref="TopicPartitionInfo"/>, <see cref="PartitionInfo"/>, <see cref="OffsetAndTimestamp"/>,
/// <see cref="ConsumerGroupMetadata"/>, <see cref="ConsumerRecord{TKey, TValue}"/> and <see cref="KafkaMetric"/>.
/// </summary>
public sealed class PublicValueTypeJavaEqualityTests
{
    private static readonly Node s_n0 = new Node(0, "h0", 9092);

    private static readonly Node s_n1 = new Node(1, "h1", 9092, "r1");

    [Fact]
    public void TopicPartitionInfo_Equality_ComparesThePartitionLeaderAndReplicaListsInOrder()
    {
        TopicPartitionInfo info = Info(0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 });

        Assert.Equal(info, Info(0, new Node(0, "h0", 9092), new[] { s_n0, s_n1 }, new[] { s_n0 }));
        Assert.Equal(info.GetHashCode(), Info(0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 }).GetHashCode());

        Assert.NotEqual(info, Info(1, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 }));
        Assert.NotEqual(info, Info(0, s_n1, new[] { s_n0, s_n1 }, new[] { s_n0 }));
        Assert.NotEqual(info, Info(0, null, new[] { s_n0, s_n1 }, new[] { s_n0 }));
        Assert.NotEqual(info, Info(0, s_n0, new[] { s_n1, s_n0 }, new[] { s_n0 }));
        Assert.NotEqual(info, Info(0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n1 }));
        Assert.False(info.Equals(null));
    }

    [Fact]
    public void TopicPartitionInfo_Equality_TellsAnAbsentElrFromAnEmptyOne()
    {
        TopicPartitionInfo withElr = new TopicPartitionInfo(
            0, s_n0, new[] { s_n0 }, new[] { s_n0 }, Array.Empty<Node>(), Array.Empty<Node>());
        TopicPartitionInfo sameElr = new TopicPartitionInfo(
            0, s_n0, new[] { s_n0 }, new[] { s_n0 }, Array.Empty<Node>(), Array.Empty<Node>());
        TopicPartitionInfo withoutElr = Info(0, s_n0, new[] { s_n0 }, new[] { s_n0 });

        Assert.Equal(withElr, sameElr);
        Assert.Equal(withElr.GetHashCode(), sameElr.GetHashCode());
        Assert.NotEqual(withElr, withoutElr);
        Assert.NotEqual(
            withElr,
            new TopicPartitionInfo(0, s_n0, new[] { s_n0 }, new[] { s_n0 }, new[] { s_n1 }, Array.Empty<Node>()));
    }

    [Fact]
    public void TopicDescription_Equality_ComparesTheNameInternalFlagPartitionsAndOperations_NotTheTopicId()
    {
        TopicPartitionInfo[] partitions = { Info(0, s_n0, new[] { s_n0 }, new[] { s_n0 }) };
        AclOperation[] operations = { AclOperation.READ, AclOperation.WRITE };
        TopicDescription description = new TopicDescription("t", false, partitions, operations, new Uuid(1L, 2L));

        TopicDescription otherId = new TopicDescription(
            "t", false, new[] { Info(0, s_n0, new[] { s_n0 }, new[] { s_n0 }) }, operations, new Uuid(3L, 4L));
        Assert.Equal(description, otherId);
        Assert.Equal(description.GetHashCode(), otherId.GetHashCode());

        TopicDescription operationsReordered = new TopicDescription(
            "t", false, partitions, new[] { AclOperation.WRITE, AclOperation.READ, AclOperation.READ }, Uuid.ZERO_UUID);
        Assert.Equal(description, operationsReordered);
        Assert.Equal(description.GetHashCode(), operationsReordered.GetHashCode());

        Assert.NotEqual(description, new TopicDescription("u", false, partitions, operations, Uuid.ZERO_UUID));
        Assert.NotEqual(description, new TopicDescription("t", true, partitions, operations, Uuid.ZERO_UUID));
        Assert.NotEqual(
            description, new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>(), operations, Uuid.ZERO_UUID));
        Assert.NotEqual(
            description, new TopicDescription("t", false, partitions, new[] { AclOperation.READ }, Uuid.ZERO_UUID));
        Assert.NotEqual(description, new TopicDescription("t", false, partitions, null, Uuid.ZERO_UUID));
        Assert.NotEqual(
            new TopicDescription("t", false, partitions, null, Uuid.ZERO_UUID),
            new TopicDescription("t", false, partitions, Array.Empty<AclOperation>(), Uuid.ZERO_UUID));
        Assert.Equal(
            new TopicDescription("t", false, partitions),
            new TopicDescription("t", false, partitions));
    }

    [Fact]
    public void TopicPartitionInfo_AndTopicDescription_HashANullElementAsJavasListHashCodeDoes()
    {
        TopicPartitionInfo withNull = new TopicPartitionInfo(0, s_n0, new Node[] { s_n0, null! }, new Node[] { null! });
        TopicPartitionInfo sameNull = new TopicPartitionInfo(0, s_n0, new Node[] { s_n0, null! }, new Node[] { null! });

        Assert.Equal(withNull, sameNull);
        Assert.Equal(withNull.GetHashCode(), sameNull.GetHashCode());
        Assert.Contains(sameNull, new HashSet<TopicPartitionInfo> { withNull });

        TopicDescription description = new TopicDescription("t", false, new TopicPartitionInfo[] { null!, withNull });
        TopicDescription sameDescription = new TopicDescription("t", false, new TopicPartitionInfo[] { null!, sameNull });

        Assert.Equal(description, sameDescription);
        Assert.Equal(description.GetHashCode(), sameDescription.GetHashCode());
        Assert.Contains(sameDescription, new HashSet<TopicDescription> { description });
    }

    [Fact]
    public void PartitionInfo_Equality_ComparesTheTopicPartitionLeaderAndReplicaListsInOrder()
    {
        PartitionInfo info = new PartitionInfo("t", 0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 }, Array.Empty<Node>());

        PartitionInfo same = new PartitionInfo(
            "t", 0, new Node(0, "h0", 9092), new[] { s_n0, s_n1 }, new[] { s_n0 }, Array.Empty<Node>());
        Assert.Equal(info, same);
        Assert.Equal(info.GetHashCode(), same.GetHashCode());

        Assert.NotEqual(info, new PartitionInfo("u", 0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 }, Array.Empty<Node>()));
        Assert.NotEqual(info, new PartitionInfo("t", 1, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 }, Array.Empty<Node>()));
        Assert.NotEqual(info, new PartitionInfo("t", 0, null, new[] { s_n0, s_n1 }, new[] { s_n0 }, Array.Empty<Node>()));
        Assert.NotEqual(info, new PartitionInfo("t", 0, s_n0, new[] { s_n1, s_n0 }, new[] { s_n0 }, Array.Empty<Node>()));
        Assert.NotEqual(info, new PartitionInfo("t", 0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n1 }, Array.Empty<Node>()));
        Assert.NotEqual(info, new PartitionInfo("t", 0, s_n0, new[] { s_n0, s_n1 }, new[] { s_n0 }, new[] { s_n1 }));
        Assert.False(info.Equals("t"));
    }

    [Fact]
    public void OffsetAndTimestamp_Equality_ComparesTheTimestampOffsetAndLeaderEpoch()
    {
        OffsetAndTimestamp value = new OffsetAndTimestamp(5L, 100L, 2);

        Assert.Equal(value, new OffsetAndTimestamp(5L, 100L, 2));
        Assert.Equal(value.GetHashCode(), new OffsetAndTimestamp(5L, 100L, 2).GetHashCode());
        Assert.Equal(new OffsetAndTimestamp(5L, 100L, null), new OffsetAndTimestamp(5L, 100L, null));

        Assert.NotEqual(value, new OffsetAndTimestamp(6L, 100L, 2));
        Assert.NotEqual(value, new OffsetAndTimestamp(5L, 101L, 2));
        Assert.NotEqual(value, new OffsetAndTimestamp(5L, 100L, 3));
        Assert.NotEqual(value, new OffsetAndTimestamp(5L, 100L, null));
    }

    [Fact]
    public void ConsumerGroupMetadata_Equality_AndToString_FollowJava()
    {
        ConsumerGroupMetadata a = GroupMetadata("g", 1, "m", null);
        ConsumerGroupMetadata b = GroupMetadata("g", 1, "m", null);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, GroupMetadata("g", 1, "m", "i"));
        Assert.NotEqual(a, GroupMetadata("h", 1, "m", null));
        Assert.NotEqual(a, GroupMetadata("g", 2, "m", null));
        Assert.NotEqual(a, GroupMetadata("g", 1, "n", null));

        Assert.Equal("GroupMetadata(groupId = g, generationId = 1, memberId = m, groupInstanceId = )", a.ToString());
        Assert.Equal(
            "GroupMetadata(groupId = g, generationId = -1, memberId = , groupInstanceId = i)",
            GroupMetadata("g", -1, string.Empty, "i").ToString());
    }

    [Fact]
    public void ConsumerRecord_ToString_FollowsJava()
    {
        ConsumerRecord<string?, string?> empty = new ConsumerRecord<string?, string?>(
            "t", 0, 0L, -1L, TimestampType.NO_TIMESTAMP_TYPE, -1, -1, null, null, new Headers(), null);
        Assert.Equal(
            "ConsumerRecord(topic = t, partition = 0, leaderEpoch = null, offset = 0, "
            + "NoTimestampType = -1, deliveryCount = null, serialized key size = -1, "
            + "serialized value size = -1, headers = RecordHeaders(headers = [], isReadOnly = false), "
            + "key = null, value = null)",
            empty.ToString());

        ConsumerRecord<string, string> full = new ConsumerRecord<string, string>(
            "orders", 3, 42L, 1700L, TimestampType.CREATE_TIME, 1, 1, "k", "v", new Headers(), 7);
        Assert.Equal(
            "ConsumerRecord(topic = orders, partition = 3, leaderEpoch = 7, offset = 42, "
            + "CreateTime = 1700, deliveryCount = null, serialized key size = 1, "
            + "serialized value size = 1, headers = RecordHeaders(headers = [], isReadOnly = false), "
            + "key = k, value = v)",
            full.ToString());

        ConsumerRecord<string, string> appended = new ConsumerRecord<string, string>(
            "orders", 3, 42L, 1700L, TimestampType.LOG_APPEND_TIME, 1, 1, "k", "v", new Headers(), null);
        Assert.Contains(", LogAppendTime = 1700, ", appended.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void KafkaMetric_ToString_IsJavasFormWithoutANamedProvider()
    {
        MetricName name = new MetricName(
            "request-latency-avg",
            "consumer-fetch-manager-metrics",
            "The average request latency in ms",
            new Dictionary<string, string> { ["client-id"] = "consumer-1" });

        Assert.Equal(
            "KafkaMetric [metricName=MetricName [name=request-latency-avg, "
            + "group=consumer-fetch-manager-metrics, "
            + "description=The average request latency in ms, "
            + "tags={client-id=consumer-1}]]",
            new KafkaMetric(name, 0.0).ToString());
    }

    private static TopicPartitionInfo Info(int partition, Node? leader, Node[] replicas, Node[] isr) =>
        new TopicPartitionInfo(partition, leader, replicas, isr);

    // The library builds this type only from the core's metadata. Equality and ToString read only the four ids, so the
    // tests pass an invalid (zero) handle, which is never released.
    private static ConsumerGroupMetadata GroupMetadata(
        string groupId,
        int generationId,
        string memberId,
        string? groupInstanceId) =>
        new ConsumerGroupMetadata(
            groupId,
            generationId,
            memberId,
            groupInstanceId,
            new SafeConsumerGroupMetadataHandle(IntPtr.Zero));
}
