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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A <see langword="null"/> <see cref="TopicPartition"/>, alone, as an element or as a key, on the four admin clients,
/// their Results and the admin value classes. An RPC refuses it at the call, on every client, after the checks that
/// run first (the closed check, an undefined enum), so no request is made. A Result lookup answers it as a
/// partition that was not requested, with Java's message printing <c>null</c>. A value class that Java lets hold it
/// keeps it, and its <c>Equals</c>, <c>GetHashCode</c> and <c>ToString</c> handle it as Java's do.
/// </summary>
public sealed class PublicAdminNullPartitionTests
{
    private const string Topic = "np";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition[] s_nullAfterTp0 = { s_tp0, null! };

    private static readonly Options[] s_everyOptions = { Options.Absent, Options.Given, Options.Null };

    // How an RPC's options are passed: the overload without them, given, or null.
    private enum Options
    {
        Absent,
        Given,
        Null,
    }

    public static IEnumerable<object[]> EveryClient() => new[]
    {
        new object[] { "KafkaAdminClient" },
        new object[] { "AsyncKafkaAdminClient" },
        new object[] { "MockAdminClient" },
        new object[] { "AsyncMockAdminClient" },
    };

    public static IEnumerable<object[]> EveryMock() => new[]
    {
        new object[] { "MockAdminClient" },
        new object[] { "AsyncMockAdminClient" },
    };

    // Java's real client puts the keys in an immutable map at the call, which rejects a null.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void DeleteRecords_ANullKey_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        EntryMap<TopicPartition, RecordsToDelete> records = NullKeyMap(RecordsToDelete.BeforeOffset(1));

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "recordsToDelete",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.DeleteRecords(records)
                        : a.DeleteRecords(records, OptionsOf(options, () => new DeleteRecordsOptions())),
                    a => options == Options.Absent
                        ? a.DeleteRecords(records)
                        : a.DeleteRecords(records, OptionsOf(options, () => new DeleteRecordsOptions()))));
        }
    }

    // After the isolation-level check, which runs first.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void ListOffsets_ANullKey_IsArgumentNullException_AfterTheIsolationLevelCheck(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        EntryMap<TopicPartition, OffsetSpec> offsets = NullKeyMap(OffsetSpec.Latest());

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "topicPartitionOffsets",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.ListOffsets(offsets)
                        : a.ListOffsets(offsets, OptionsOf(options, () => new ListOffsetsOptions())),
                    a => options == Options.Absent
                        ? a.ListOffsets(offsets)
                        : a.ListOffsets(offsets, OptionsOf(options, () => new ListOffsetsOptions()))));
        }

        ListOffsetsOptions undefined = new ListOffsetsOptions((IsolationLevel)7);
        Exception? first = Refusal(client, a => a.ListOffsets(offsets, undefined), a => a.ListOffsets(offsets, undefined));
        Assert.Equal("Unknown isolation level 7", Assert.IsType<IllegalArgumentException>(first).Message);
    }

    // Java's request build dereferences it; a null collection still means every partition. The election type is
    // checked first.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void ElectLeaders_ANullElement_IsArgumentNullException_AfterTheElectionTypeCheck(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "partitions",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.ElectLeaders(ElectionType.PREFERRED, s_nullAfterTp0)
                        : a.ElectLeaders(
                            ElectionType.PREFERRED, s_nullAfterTp0, OptionsOf(options, () => new ElectLeadersOptions())),
                    a => options == Options.Absent
                        ? a.ElectLeaders(ElectionType.PREFERRED, s_nullAfterTp0)
                        : a.ElectLeaders(
                            ElectionType.PREFERRED, s_nullAfterTp0, OptionsOf(options, () => new ElectLeadersOptions()))));
        }

        Exception? first = Refusal(
            client, a => a.ElectLeaders((ElectionType)9, s_nullAfterTp0), a => a.ElectLeaders((ElectionType)9, s_nullAfterTp0));
        Assert.Equal(
            "Value 9 must be one of [PREFERRED, UNCLEAN]", Assert.IsType<IllegalArgumentException>(first).Message);
    }

    // Java reads entry.getKey().topic() at the call; a null value stays legal.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void AlterPartitionReassignments_ANullKey_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        EntryMap<TopicPartition, NewPartitionReassignment?> reassignments = NullKeyMap<NewPartitionReassignment?>(null);

        foreach (Options options in s_everyOptions)
        {
            AssertNullArgument(
                "reassignments",
                "reassignments must not contain a null key",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.AlterPartitionReassignments(reassignments)
                        : a.AlterPartitionReassignments(
                            reassignments, OptionsOf(options, () => new AlterPartitionReassignmentsOptions())),
                    a => options == Options.Absent
                        ? a.AlterPartitionReassignments(reassignments)
                        : a.AlterPartitionReassignments(
                            reassignments, OptionsOf(options, () => new AlterPartitionReassignmentsOptions()))));
        }
    }

    // Java's real client reads tp.topic() in its validation loop, before the options; the mocks would skip it,
    // but the C ABI cannot carry it. A null options on a real client does not hide it.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void ListPartitionReassignments_ANullElement_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "partitions",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.ListPartitionReassignments(s_nullAfterTp0)
                        : a.ListPartitionReassignments(
                            s_nullAfterTp0, OptionsOf(options, () => new ListPartitionReassignmentsOptions())),
                    a => options == Options.Absent
                        ? a.ListPartitionReassignments(s_nullAfterTp0)
                        : a.ListPartitionReassignments(
                            s_nullAfterTp0, OptionsOf(options, () => new ListPartitionReassignmentsOptions()))));
        }
    }

    // Java's group future fails when the request is built; .NET refuses it at the call.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void AlterConsumerGroupOffsets_ANullKey_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        EntryMap<TopicPartition, OffsetAndMetadata> offsets = NullKeyMap(new OffsetAndMetadata(1));

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "offsets",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.AlterConsumerGroupOffsets("g", offsets)
                        : a.AlterConsumerGroupOffsets(
                            "g", offsets, OptionsOf(options, () => new AlterConsumerGroupOffsetsOptions())),
                    a => options == Options.Absent
                        ? a.AlterConsumerGroupOffsets("g", offsets)
                        : a.AlterConsumerGroupOffsets(
                            "g", offsets, OptionsOf(options, () => new AlterConsumerGroupOffsetsOptions()))));
        }
    }

    // Java's group future fails when it groups the partitions by topic.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void DeleteConsumerGroupOffsets_ANullElement_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "partitions",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.DeleteConsumerGroupOffsets("g", s_nullAfterTp0)
                        : a.DeleteConsumerGroupOffsets(
                            "g", s_nullAfterTp0, OptionsOf(options, () => new DeleteConsumerGroupOffsetsOptions())),
                    a => options == Options.Absent
                        ? a.DeleteConsumerGroupOffsets("g", s_nullAfterTp0)
                        : a.DeleteConsumerGroupOffsets(
                            "g", s_nullAfterTp0, OptionsOf(options, () => new DeleteConsumerGroupOffsetsOptions()))));
        }
    }

    // A null element of a spec's partitions; the spec itself keeps it.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void ListConsumerGroupOffsets_ANullSelectedPartition_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        Dictionary<string, ListConsumerGroupOffsetsSpec> specs = new Dictionary<string, ListConsumerGroupOffsetsSpec>
        {
            ["g"] = new ListConsumerGroupOffsetsSpec().TopicPartitions(s_nullAfterTp0),
        };

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "groupSpecs",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.ListConsumerGroupOffsets(specs)
                        : a.ListConsumerGroupOffsets(
                            specs, OptionsOf(options, () => new ListConsumerGroupOffsetsOptions())),
                    a => options == Options.Absent
                        ? a.ListConsumerGroupOffsets(specs)
                        : a.ListConsumerGroupOffsets(
                            specs, OptionsOf(options, () => new ListConsumerGroupOffsetsOptions()))));
        }
    }

    // Java's real client puts the partitions in an immutable map at the call.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void DescribeProducers_ANullElement_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));

        foreach (Options options in s_everyOptions)
        {
            AssertMustNotContainNull(
                "partitions",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.DescribeProducers(s_nullAfterTp0)
                        : a.DescribeProducers(s_nullAfterTp0, OptionsOf(options, () => new DescribeProducersOptions())),
                    a => options == Options.Absent
                        ? a.DescribeProducers(s_nullAfterTp0)
                        : a.DescribeProducers(s_nullAfterTp0, OptionsOf(options, () => new DescribeProducersOptions()))));
        }
    }

    // Java puts Collections.singleton(null) in an immutable map at the call. A null topic keeps its own message.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void AbortTransaction_ANullTopicPartition_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        AbortTransactionSpec spec = new AbortTransactionSpec(null!, 1L, 1, 1);

        foreach (Options options in s_everyOptions)
        {
            AssertNullArgument(
                "spec",
                "spec must not have a null TopicPartition",
                Refusal(
                    client,
                    a => options == Options.Absent
                        ? a.AbortTransaction(spec)
                        : a.AbortTransaction(spec, OptionsOf(options, () => new AbortTransactionOptions())),
                    a => options == Options.Absent
                        ? a.AbortTransaction(spec)
                        : a.AbortTransaction(spec, OptionsOf(options, () => new AbortTransactionOptions()))));
        }

        AbortTransactionSpec nullTopic = new AbortTransactionSpec(new TopicPartition(null!, 0), 1L, 1, 1);
        AssertNullArgument(
            "spec",
            "spec must not have a TopicPartition with a null topic",
            Refusal(client, a => a.AbortTransaction(nullTopic), a => a.AbortTransaction(nullTopic)));
    }

    // The closed check still runs first.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void AClosedClient_ReportsItsClosedCheck_BeforeANullPartition(string kind)
    {
        IDisposable client = Create(kind, out _);
        TestTimeout.Dispose(client);
        EntryMap<TopicPartition, RecordsToDelete> records = NullKeyMap(RecordsToDelete.BeforeOffset(1));
        EntryMap<TopicPartition, OffsetSpec> offsets = NullKeyMap(OffsetSpec.Latest());
        AbortTransactionSpec spec = new AbortTransactionSpec(null!, 1L, 1, 1);

        Assert.IsType<IllegalStateException>(Refusal(client, a => a.DeleteRecords(records), a => a.DeleteRecords(records)));
        Assert.IsType<IllegalStateException>(Refusal(client, a => a.ListOffsets(offsets), a => a.ListOffsets(offsets)));
        Assert.IsType<IllegalStateException>(
            Refusal(client, a => a.DescribeProducers(s_nullAfterTp0), a => a.DescribeProducers(s_nullAfterTp0)));
        Assert.IsType<IllegalStateException>(
            Refusal(
                client, a => a.ListPartitionReassignments(s_nullAfterTp0), a => a.ListPartitionReassignments(s_nullAfterTp0)));
        Assert.IsType<IllegalStateException>(Refusal(client, a => a.AbortTransaction(spec), a => a.AbortTransaction(spec)));
    }

    // Java's mock stores it with putAll; the C ABI cannot carry it, so nothing is seeded.
    [Theory]
    [MemberData(nameof(EveryMock))]
    public async Task MockSeeding_ANullKey_IsArgumentNullException_AndSeedsNothing(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        EntryMap<TopicPartition, long> newOffsets = NullKeyMap(42L);

        foreach (Action seed in new Action[]
                 {
                     () => Seed(client, newOffsets, beginning: true),
                     () => Seed(client, newOffsets, beginning: false),
                     () => SeedGroupOffsets(client, newOffsets),
                 })
        {
            AssertMustNotContainNull("newOffsets", Record.Exception(seed));
        }

        foreach (OffsetSpec spec in new[] { OffsetSpec.Earliest(), OffsetSpec.Latest() })
        {
            Assert.Equal(-1L, await OffsetOf(client, spec));
        }
    }

    // Java's future fails with its message for a partition that was not attempted.
    [Fact]
    public async Task AlterConsumerGroupOffsetsResult_PartitionResult_OfNull_IsAFailedFuture_WithJavasMessage()
    {
        const string NotAttempted = "Alter offset for partition \"null\" was not attempted";
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(1) };

        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        KafkaFuture syncFuture = sync.AlterConsumerGroupOffsets("g", offsets).PartitionResult(null!);
        Assert.True(syncFuture.IsDone());
        Assert.True(syncFuture.IsCompletedExceptionally());
        Assert.Equal(NotAttempted, FutureAssert.Throws<IllegalArgumentException>(() => syncFuture.GetNow()).Message);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AsyncKafkaFuture asyncFuture = async.AlterConsumerGroupOffsets("g", offsets).PartitionResult(null!);
        Assert.True(asyncFuture.IsDone());
        Assert.True(asyncFuture.IsCompletedExceptionally());
        IllegalArgumentException asyncFailure = await FutureAssert.ThrowsAsync<IllegalArgumentException>(
            () => TestTimeout.Run(() => asyncFuture.Get(), s_deadline));
        Assert.Equal(NotAttempted, asyncFailure.Message);
    }

    // Java's membership check throws at the call.
    [Fact]
    public void DeleteConsumerGroupOffsetsResult_PartitionResult_OfNull_IsJavasIllegalArgumentException()
    {
        const string NotIncluded = "Partition null was not included in the original request";

        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        DeleteConsumerGroupOffsetsResult syncResult = sync.DeleteConsumerGroupOffsets("g", new[] { s_tp0 });
        Assert.Equal(
            NotIncluded, Assert.Throws<IllegalArgumentException>(() => syncResult.PartitionResult(null!)).Message);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AsyncDeleteConsumerGroupOffsetsResult asyncResult = async.DeleteConsumerGroupOffsets("g", new[] { s_tp0 });
        Assert.Equal(
            NotIncluded, Assert.Throws<IllegalArgumentException>(() => asyncResult.PartitionResult(null!)).Message);
    }

    // Java's lookup in a map the user built finds nothing and throws its message; a client-built result answers
    // the same.
    [Fact]
    public void ListOffsetsResult_PartitionResult_OfNull_IsJavasIllegalArgumentException()
    {
        const string NotAttempted = "List Offsets for partition \"null\" was not attempted";
        Dictionary<TopicPartition, OffsetSpec> request = new Dictionary<TopicPartition, OffsetSpec> { [s_tp0] = OffsetSpec.Latest() };
        ListOffsetsResult.ListOffsetsResultInfo info = new ListOffsetsResult.ListOffsetsResultInfo(1L, 2L, null);

        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        ListOffsetsResult syncBuilt = new ListOffsetsResult(
            new Dictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>
            {
                [s_tp0] = KafkaFuture.CompletedFuture(info),
            });
        AsyncListOffsetsResult asyncBuilt = new AsyncListOffsetsResult(
            new Dictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>
            {
                [s_tp0] = AsyncKafkaFuture.CompletedFuture(info),
            });

        foreach (Func<object> lookup in new Func<object>[]
                 {
                     () => sync.ListOffsets(request).PartitionResult(null!),
                     () => syncBuilt.PartitionResult(null!),
                     () => async.ListOffsets(request).PartitionResult(null!),
                     () => asyncBuilt.PartitionResult(null!),
                 })
        {
            Assert.Equal(NotAttempted, Assert.Throws<IllegalArgumentException>(lookup).Message);
        }
    }

    // Java's text, "Topic partition " + partition + " was not included in the request".
    [Fact]
    public void DescribeProducersResult_PartitionResult_OfNull_IsJavasIllegalArgumentException()
    {
        const string NotIncluded = "Topic partition null was not included in the request";

        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        DescribeProducersResult syncResult = sync.DescribeProducers(new[] { s_tp0 });
        Assert.Equal(
            NotIncluded, Assert.Throws<IllegalArgumentException>(() => syncResult.PartitionResult(null!)).Message);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AsyncDescribeProducersResult asyncResult = async.DescribeProducers(new[] { s_tp0 });
        Assert.Equal(
            NotIncluded, Assert.Throws<IllegalArgumentException>(() => asyncResult.PartitionResult(null!)).Message);
    }

    // Java stores the map as given; a .NET Dictionary cannot hold the null key.
    [Fact]
    public void PublicResultConstructors_ANullKey_IsArgumentNullException()
    {
        ListOffsetsResult.ListOffsetsResultInfo info = new ListOffsetsResult.ListOffsetsResultInfo(1L, 2L, null);
        DeletedRecords deleted = new DeletedRecords(3L);

        foreach (Action construct in new Action[]
                 {
                     () => _ = new DeleteRecordsResult(NullKeyMap(KafkaFuture.CompletedFuture(deleted))),
                     () => _ = new AsyncDeleteRecordsResult(NullKeyMap(AsyncKafkaFuture.CompletedFuture(deleted))),
                     () => _ = new ListOffsetsResult(NullKeyMap(KafkaFuture.CompletedFuture(info))),
                     () => _ = new AsyncListOffsetsResult(NullKeyMap(AsyncKafkaFuture.CompletedFuture(info))),
                 })
        {
            AssertMustNotContainNull("futures", Record.Exception(construct));
        }
    }

    // Java stores it; equals and hashCode go through Objects, toString prints null.
    [Fact]
    public void AbortTransactionSpec_ANullTopicPartition_IsKept_AndPrintsNull()
    {
        AbortTransactionSpec spec = new AbortTransactionSpec(null!, 1L, 2, 3);
        AbortTransactionSpec same = new AbortTransactionSpec(null!, 1L, 2, 3);
        AbortTransactionSpec other = new AbortTransactionSpec(s_tp0, 1L, 2, 3);

        Assert.Null(spec.TopicPartition());
        Assert.True(spec.Equals(same));
        Assert.Equal(spec.GetHashCode(), same.GetHashCode());
        Assert.False(spec.Equals(other));
        Assert.False(other.Equals(spec));
        Assert.Equal(
            "AbortTransactionSpec(topicPartition=null, producerId=1, producerEpoch=2, coordinatorEpoch=3)",
            spec.ToString());
    }

    // Java stores the list; Objects.equals and Objects.hash handle the null element.
    [Fact]
    public void ListConsumerGroupOffsetsSpec_ANullElement_IsKept_AndPrintsNull()
    {
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec().TopicPartitions(s_nullAfterTp0);
        ListConsumerGroupOffsetsSpec same = new ListConsumerGroupOffsetsSpec().TopicPartitions(s_nullAfterTp0);
        ListConsumerGroupOffsetsSpec other = new ListConsumerGroupOffsetsSpec().TopicPartitions(new[] { s_tp0, s_tp0 });

        Assert.Equal(s_nullAfterTp0, spec.TopicPartitions());
        Assert.True(spec.Equals(same));
        Assert.Equal(spec.GetHashCode(), same.GetHashCode());
        Assert.False(spec.Equals(other));
        Assert.Equal("ListConsumerGroupOffsetsSpec(topicPartitions=[np-0, null])", spec.ToString());
    }

    // Java's Set.copyOf rejects it.
    [Fact]
    public void MemberAssignment_ANullElement_IsArgumentNullException()
    {
        AssertMustNotContainNull("topicPartitions", Record.Exception(() => new MemberAssignment(s_nullAfterTp0)));
    }

    // Java stores the set; its equals and hashCode handle the null element, and toString prints it.
    [Fact]
    public void TransactionDescription_ANullElement_IsKept_AndPrintsNull()
    {
        TransactionDescription description = Description(s_nullAfterTp0);
        TransactionDescription same = Description(s_nullAfterTp0);

        Assert.Equal(s_nullAfterTp0, description.TopicPartitions());
        Assert.True(description.Equals(same));
        Assert.Equal(description.GetHashCode(), same.GetHashCode());
        Assert.False(description.Equals(Description(new[] { s_tp0 })));
        Assert.EndsWith(", topicPartitions=[np-0, null])", description.ToString(), StringComparison.Ordinal);
    }

    // Java stores the map as given, and its toString prints the null key.
    [Fact]
    public void LogDirDescription_ANullKey_IsKept_AndPrintsNull()
    {
        ReplicaInfo replica = new ReplicaInfo(1L, 2L, false);
        EntryMap<TopicPartition, ReplicaInfo> replicas = new EntryMap<TopicPartition, ReplicaInfo>(
            new KeyValuePair<TopicPartition, ReplicaInfo>(null!, replica));

        LogDirDescription description = new LogDirDescription(null, replicas);

        Assert.Same(replicas, description.ReplicaInfos());
        Assert.StartsWith(
            "LogDirDescription(replicaInfos={null=ReplicaInfo(size=1, offsetLag=2, isFuture=false)}, ",
            description.ToString(),
            StringComparison.Ordinal);
    }

    private static T OptionsOf<T>(Options options, Func<T> create)
        where T : class =>
        options == Options.Given ? create() : null!;

    private static IDisposable Create(string kind, out IDisposable client)
    {
        client = kind switch
        {
            "KafkaAdminClient" => IAdmin.Create(Unreachable()),
            "AsyncKafkaAdminClient" => IAsyncAdmin.Create(Unreachable()),
            "MockAdminClient" => MockAdminClient.Create().Build(),
            _ => AsyncMockAdminClient.Create().Build(),
        };
        return client;
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = "127.0.0.1:1" };

    // What a call throws at the call; an admin RPC returns its Result at once on every client.
    private static Exception? Refusal(IDisposable client, Func<IAdmin, object> sync, Func<IAsyncAdmin, object> async) =>
        Record.Exception(() => _ = client is IAsyncAdmin asyncAdmin ? async(asyncAdmin) : sync((IAdmin)client));

    private static EntryMap<TopicPartition, TValue> NullKeyMap<TValue>(TValue value) =>
        new EntryMap<TopicPartition, TValue>(
            new KeyValuePair<TopicPartition, TValue>(s_tp0, value),
            new KeyValuePair<TopicPartition, TValue>(null!, value));

    private static void AssertMustNotContainNull(string paramName, Exception? error) =>
        AssertNullArgument(paramName, paramName + " must not contain null", error);

    private static void AssertNullArgument(string paramName, string message, Exception? error)
    {
        ArgumentNullException refused = Assert.IsType<ArgumentNullException>(error);
        Assert.Equal(paramName, refused.ParamName);
        Assert.Equal(new ArgumentNullException(paramName, message).Message, refused.Message);
    }

    private static void Seed(IDisposable client, IReadOnlyDictionary<TopicPartition, long> newOffsets, bool beginning)
    {
        if (client is MockAdminClient sync)
        {
            if (beginning)
            {
                sync.UpdateBeginningOffsets(newOffsets);
            }
            else
            {
                sync.UpdateEndOffsets(newOffsets);
            }

            return;
        }

        AsyncMockAdminClient async = (AsyncMockAdminClient)client;
        if (beginning)
        {
            async.UpdateBeginningOffsets(newOffsets);
        }
        else
        {
            async.UpdateEndOffsets(newOffsets);
        }
    }

    private static void SeedGroupOffsets(IDisposable client, IReadOnlyDictionary<TopicPartition, long> newOffsets)
    {
        if (client is MockAdminClient sync)
        {
            sync.UpdateConsumerGroupOffsets(newOffsets);
        }
        else
        {
            ((AsyncMockAdminClient)client).UpdateConsumerGroupOffsets(newOffsets);
        }
    }

    private static async Task<long> OffsetOf(IDisposable client, OffsetSpec spec)
    {
        Dictionary<TopicPartition, OffsetSpec> request = new Dictionary<TopicPartition, OffsetSpec> { [s_tp0] = spec };
        ListOffsetsResult.ListOffsetsResultInfo info = client is MockAdminClient sync
            ? await TestTimeout.Run(
                () => Task.Run(() => sync.ListOffsets(request).PartitionResult(s_tp0).Get(s_deadline)), s_deadline)
            : await TestTimeout.Run(
                () => ((AsyncMockAdminClient)client).ListOffsets(request).PartitionResult(s_tp0).Get(), s_deadline);
        return info.Offset();
    }

    private static TransactionDescription Description(IReadOnlyCollection<TopicPartition> partitions) =>
        new TransactionDescription(1, TransactionState.ONGOING, 2L, 3, 4L, 5L, partitions);
}
