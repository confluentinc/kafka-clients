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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Every admin null-topic guard family, reached the way M15/P13.2 G3-4 newly allows — a
/// <c>new TopicPartition(null!, 0)</c>, which the relaxed constructor stores as Java's does
/// (<c>TopicPartition.java:32-35</c>) — alongside the <c>default(TopicPartition)</c> the
/// pre-existing tests use. Both must hit the <b>same</b> guard with the same exception type,
/// parameter name and message, before the native submit.
/// </summary>
/// <remarks>
/// The guards stay because a null topic cannot cross the C ABI (M15/P13.2 D1): the admin ABI
/// silently skips a NULL-topic row, or fails the whole call, rather than answering it as Java
/// would. Each test goes through the RPC's submit seam, so "never submitted" is observed
/// rather than inferred. The two constructions are value-identical, which is the point: the
/// constructor no longer throws ahead of the guard, so what each test observes is the guard's
/// own exception.
/// </remarks>
public sealed class AdminConstructedNullTopicTests
{
    private const string NotSubmitted = "a null topic must be rejected before the native call";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DeleteRecords_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete> { [NullTopic(constructed)] = RecordsToDelete.BeforeOffset(1) },
            null,
            (_, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(
            rejected,
            "recordsToDelete",
            "The records-to-delete map must not contain a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AlterPartitionReassignments_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?> { [NullTopic(constructed)] = null },
            null,
            (_, _, _, _, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(
            rejected, "reassignments", "The reassignments map must not contain a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AlterConsumerGroupOffsets_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.AlterConsumerGroupOffsets(
            "g",
            new Dictionary<TopicPartition, OffsetAndMetadata> { [NullTopic(constructed)] = new OffsetAndMetadata(1) },
            null,
            (_, _, _, _, _, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(rejected, "offsets", "The offsets map must not contain a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DeleteConsumerGroupOffsets_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DeleteConsumerGroupOffsets(
            "g",
            new[] { NullTopic(constructed) },
            null,
            (_, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(
            rejected, "partitions", "The partitions collection must not contain a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AbortTransaction_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.AbortTransaction(
            new AbortTransactionSpec(NullTopic(constructed), 1L, 1, 1),
            null,
            (_, _, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(
            rejected, "spec", "The abort transaction spec must not carry a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ListOffsets_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [NullTopic(constructed)] = OffsetSpec.Latest() },
            null,
            (_, _, _, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(
            rejected,
            "topicPartitionOffsets",
            "The offsets map must not contain a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ListConsumerGroupOffsets_RejectsANullTopic(bool constructed)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["g1"] = new ListConsumerGroupOffsetsSpec { TopicPartitions = new[] { NullTopic(constructed) } },
            },
            null,
            (_, _, _, _, _, _, _, _, _, _, _) => submitted = true));

        AssertRejected(
            rejected, "groupSpecs", "The spec for group id 'g1' must not select a topic partition with a null topic.");
        Assert.False(submitted, NotSubmitted);
    }

    /// <summary>
    /// The shared <c>DistinctPartitions</c> guard, through each of its three RPCs.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DistinctPartitions_ElectLeaders_ListReassignments_DescribeProducers_RejectANullTopic(bool constructed)
    {
        const string Message = "The partitions must not contain a topic partition with a null topic.";
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;
        TopicPartition[] partitions = { NullTopic(constructed) };

        AssertRejected(
            Assert.Throws<ArgumentException>(() => admin.ElectLeaders(
                ElectionType.Preferred, partitions, null, (_, _, _, _, _, _, _, _, _) => submitted = true)),
            "partitions",
            Message);
        AssertRejected(
            Assert.Throws<ArgumentException>(() => admin.ListPartitionReassignments(
                partitions, null, (_, _, _, _, _, _, _, _) => submitted = true)),
            "partitions",
            Message);
        AssertRejected(
            Assert.Throws<ArgumentException>(() => admin.DescribeProducers(
                partitions, null, (_, _, _, _, _, _, _, _, _) => submitted = true)),
            "partitions",
            Message);

        Assert.False(submitted, NotSubmitted);
    }

    private static TopicPartition NullTopic(bool constructed) =>
        constructed ? new TopicPartition(null!, 0) : default;

    private static void AssertRejected(ArgumentException rejected, string paramName, string message)
    {
        Assert.IsType<ArgumentException>(rejected);
        Assert.Equal(paramName, rejected.ParamName);
        Assert.StartsWith(message, rejected.Message, StringComparison.Ordinal);
    }
}
