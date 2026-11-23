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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Every admin null-topic guard family, reached through <c>new TopicPartition(null!, 0)</c>, which the constructor
/// stores as Java's does: each hits its guard, with its exception type, parameter name and message, before the native
/// submit.
/// </summary>
/// <remarks>
/// The guards stay because a null topic cannot cross the C ABI, which would skip the row or fail the whole call.
/// </remarks>
public sealed class AdminConstructedNullTopicTests
{
    [Fact]
    public void DeleteRecords_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete> { [NullTopic()] = RecordsToDelete.BeforeOffset(1) },
            new DeleteRecordsOptions()));

        AssertRejected(
            rejected,
            "recordsToDelete",
            "recordsToDelete must not contain a TopicPartition with a null topic");
    }

    [Fact]
    public void AlterPartitionReassignments_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?> { [NullTopic()] = null },
            new AlterPartitionReassignmentsOptions()));

        AssertRejected(
            rejected, "reassignments", "reassignments must not contain a TopicPartition with a null topic");
    }

    [Fact]
    public void AlterConsumerGroupOffsets_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => admin.AlterConsumerGroupOffsets(
            "g",
            new Dictionary<TopicPartition, OffsetAndMetadata> { [NullTopic()] = new OffsetAndMetadata(1) },
            new AlterConsumerGroupOffsetsOptions()));

        AssertRejected(rejected, "offsets", "offsets must not contain a TopicPartition with a null topic");
    }

    [Fact]
    public void DeleteConsumerGroupOffsets_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => admin.DeleteConsumerGroupOffsets(
            "g",
            new[] { NullTopic() },
            new DeleteConsumerGroupOffsetsOptions()));

        AssertRejected(
            rejected, "partitions", "partitions must not contain a TopicPartition with a null topic");
    }

    [Fact]
    public async Task GroupOffsetResultLookups_ANullTopic_AnswersAsJava()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        SafeAdminResultHandle alteredHandle = admin.AlterConsumerGroupOffsets(
            "g", new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("t", 0)] = new OffsetAndMetadata(1) }, new AlterConsumerGroupOffsetsOptions());
        SafeAdminResultHandle deletedHandle = admin.DeleteConsumerGroupOffsets("g", new[] { new TopicPartition("t", 0) }, new DeleteConsumerGroupOffsetsOptions());

        // Alter: Java's future fails, for a partition that was not attempted.
        const string NotAttempted = "Alter offset for partition \"null-0\" was not attempted";
        IllegalArgumentException asyncAltered = await FutureAssert.ThrowsAsync<IllegalArgumentException>(
            () => AsyncAlterConsumerGroupOffsetsResult.OfClient(alteredHandle).PartitionResult(NullTopic()).Get());
        Assert.Equal(NotAttempted, asyncAltered.Message);
        KafkaFuture syncAltered = AlterConsumerGroupOffsetsResult.OfClient(alteredHandle).PartitionResult(NullTopic());
        Assert.Equal(
            NotAttempted,
            FutureAssert.Throws<IllegalArgumentException>(() => syncAltered.Get(TimeSpan.FromSeconds(30))).Message);

        // Delete: Java's membership check throws at the call.
        const string NotIncluded = "Partition null-0 was not included in the original request";
        Assert.Equal(
            NotIncluded,
            Assert.Throws<IllegalArgumentException>(
                () => AsyncDeleteConsumerGroupOffsetsResult.OfClient(deletedHandle).PartitionResult(NullTopic())).Message);
        Assert.Equal(
            NotIncluded,
            Assert.Throws<IllegalArgumentException>(
                () => DeleteConsumerGroupOffsetsResult.OfClient(deletedHandle).PartitionResult(NullTopic())).Message);
    }

    [Fact]
    public void AbortTransaction_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => AsyncAbortTransactionResult.OfClient(admin.AbortTransaction(
            new AbortTransactionSpec(NullTopic(), 1L, 1, 1),
            new AbortTransactionOptions())));

        AssertRejected(
            rejected, "spec", "spec must not have a TopicPartition with a null topic");
    }

    [Fact]
    public void ListOffsets_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [NullTopic()] = OffsetSpec.Latest() },
            new ListOffsetsOptions()));

        AssertRejected(
            rejected,
            "topicPartitionOffsets",
            "topicPartitionOffsets must not contain a TopicPartition with a null topic");
    }

    [Fact]
    public void ListConsumerGroupOffsets_RejectsANullTopic()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["g1"] = new ListConsumerGroupOffsetsSpec().TopicPartitions(new[] { NullTopic() }),
            },
            new ListConsumerGroupOffsetsOptions()));

        AssertRejected(
            rejected, "groupSpecs", "groupSpecs must not contain a TopicPartition with a null topic");
    }

    [Fact]
    public void DistinctPartitions_ElectLeaders_ListReassignments_DescribeProducers_RejectANullTopic()
    {
        const string Message = "partitions must not contain a TopicPartition with a null topic";
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition[] partitions = { NullTopic() };

        AssertRejected(
            Assert.Throws<ArgumentNullException>(() => admin.ElectLeaders(ElectionType.PREFERRED, partitions, new ElectLeadersOptions())),
            "partitions",
            Message);
        AssertRejected(
            Assert.Throws<ArgumentNullException>(() => admin.ListPartitionReassignments(partitions, new ListPartitionReassignmentsOptions())),
            "partitions",
            Message);
        AssertRejected(
            Assert.Throws<ArgumentNullException>(() => AsyncDescribeProducersResult.OfClient(admin.DescribeProducers(partitions, new DescribeProducersOptions()))),
            "partitions",
            Message);
    }

    private static TopicPartition NullTopic() => new TopicPartition(null!, 0);

    private static void AssertRejected(ArgumentException rejected, string paramName, string message)
    {
        Assert.IsType<ArgumentNullException>(rejected);
        Assert.Equal(paramName, rejected.ParamName);
        Assert.StartsWith(message, rejected.Message, StringComparison.Ordinal);
    }
}
