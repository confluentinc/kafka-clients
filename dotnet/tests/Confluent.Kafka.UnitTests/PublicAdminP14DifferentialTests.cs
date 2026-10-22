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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The differential rows of M15/P14 S4, S5a, S5b, S5c, S5d and S5e: each converted RPC, called with the same arguments on a
/// <see cref="MockAdminClient"/> and an <see cref="AsyncMockAdminClient"/> seeded with the same mock state, gives the
/// same outcome through every member of its Result — equal values, or an exception of the same type with the same
/// message (and, for a <see cref="KafkaException"/>, the same code). Each row also pins the outcome itself, so two
/// flavours that agreed on a wrong answer would still fail.
/// </summary>
/// <remarks>
/// Every wait is bounded (<see cref="FutureProbe"/>): a sync <c>Get()</c> runs on a dedicated thread, an async one
/// under <see cref="TestTimeout"/>.
/// </remarks>
public sealed class PublicAdminP14DifferentialTests
{
    // Errors.INVALID_REPLICATION_FACTOR and Errors.TOPIC_ALREADY_EXISTS.
    private const int InvalidReplicationFactorCode = 38;
    private const int TopicAlreadyExistsCode = 36;

    // Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_ID and Errors.UNSUPPORTED_VERSION (the mock's
    // "Not implemented yet", Java's UnsupportedOperationException).
    private const int UnknownTopicOrPartitionCode = 3;
    private const int UnknownTopicIdCode = 100;
    private const int UnsupportedVersionCode = 35;

    // Errors.REPLICA_NOT_AVAILABLE, Errors.INVALID_REQUEST and Errors.KAFKA_STORAGE_ERROR (M15/P14 S5a).
    private const int ReplicaNotAvailableCode = 9;
    private const int InvalidRequestCode = 42;
    private const int KafkaStorageErrorCode = 56;

    // The core's LOCAL_ILLEGAL_STATE, which carries its absent-key error.
    private const int LocalIllegalStateCode = -4;

    private const string NotImplementedYet = "Not implemented yet";

    // Java's mock alterConsumerGroupOffsets refusal, with Java's own typo (MockAdminClient.java:1213), which the core
    // mock carries verbatim (M15/P14 S5c).
    private const string NotImplementYet = "Not implement yet";

    // ListConsumerGroupOffsetsResult's no-argument accessor over anything but one group (Java's
    // IllegalStateException, ListConsumerGroupOffsetsResult.java:51) (M15/P14 S5c).
    private const string MultipleGroupsRequested =
        "Offsets from multiple consumer groups were requested. " +
        "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.";

    // The core's default message for UNKNOWN_TOPIC_OR_PARTITION, which the core mock's reassignment of an unknown
    // partition carries (Java's mock throws a message-less UnknownTopicOrPartitionException).
    private const string UnknownTopicOrPartitionMessage = "This server does not host this topic-partition.";

    // The core's LOCAL_ILLEGAL_ARGUMENT, and the core mock's refusal of a token request without a renewer (Java's mock
    // throws IndexOutOfBoundsException from options.renewers().get(0), MockAdminClient.java:652) (M15/P14 S5e).
    private const int LocalIllegalArgumentCode = -3;
    private const string NoRenewer =
        "createDelegationToken requires at least one renewer: MockAdminClient makes the first renewer the owner";

    // Errors.DELEGATION_TOKEN_NOT_FOUND, which the core mock raises with an empty message (M15/P14 S5e).
    private const int DelegationTokenNotFoundCode = 62;
    private const string DelegationTokenNotFoundMessage = "";

    private static readonly Uuid s_missingId = new Uuid(0x7A7A7A7A7A7A7A7AL, 0x5B5B5B5B5B5B5B5BL);

    /// <summary>
    /// <c>CreateTopics</c>: a creatable topic and one whose replication factor exceeds the broker count, then the
    /// creatable one again (now existing). Values, <c>All()</c> and the four typed accessors agree on both mocks. The
    /// topic id is minted by each mock independently, so only its presence is compared.
    /// </summary>
    [Fact]
    public async Task CreateTopics_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        NewTopic[] request = { new NewTopic("diff-ok", 3, 1), new NewTopic("diff-bad", 1, 5) };

        CreateTopicsResult syncResult = sync.CreateTopics(request);
        AsyncCreateTopicsResult asyncResult = async.CreateTopics(request);

        Assert.Equal(syncResult.Values.Keys, asyncResult.Values.Keys);

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.Values["diff-ok"]), FutureProbe.Of(asyncResult.Values["diff-ok"])));
        Assert.Equal(
            (object)3,
            await Agree(FutureProbe.Of(syncResult.NumPartitions("diff-ok")), FutureProbe.Of(asyncResult.NumPartitions("diff-ok"))));
        Assert.Equal(
            (object)1,
            await Agree(
                FutureProbe.Of(syncResult.ReplicationFactor("diff-ok")), FutureProbe.Of(asyncResult.ReplicationFactor("diff-ok"))));
        Config syncConfig = Assert.IsType<Config>(await FutureProbe.Of(syncResult.Config("diff-ok")).Outcome());
        Config asyncConfig = Assert.IsType<Config>(await FutureProbe.Of(asyncResult.Config("diff-ok")).Outcome());
        Assert.Equal(Entries(syncConfig), Entries(asyncConfig));
        Assert.NotEqual((object)Uuid.Zero, await FutureProbe.Of(syncResult.TopicId("diff-ok")).Outcome());
        Assert.NotEqual((object)Uuid.Zero, await FutureProbe.Of(asyncResult.TopicId("diff-ok")).Outcome());

        const string TooLarge = "Replication factor: 5 is larger than brokers: 1";
        AssertKafka(
            InvalidReplicationFactorCode,
            TooLarge,
            await Agree(FutureProbe.Of(syncResult.Values["diff-bad"]), FutureProbe.Of(asyncResult.Values["diff-bad"])));
        AssertKafka(
            InvalidReplicationFactorCode,
            TooLarge,
            await Agree(FutureProbe.Of(syncResult.NumPartitions("diff-bad")), FutureProbe.Of(asyncResult.NumPartitions("diff-bad"))));
        AssertKafka(
            InvalidReplicationFactorCode,
            TooLarge,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        CreateTopicsResult syncAgain = sync.CreateTopics(new[] { new NewTopic("diff-ok", 3, 1) });
        AsyncCreateTopicsResult asyncAgain = async.CreateTopics(new[] { new NewTopic("diff-ok", 3, 1) });
        AssertKafka(
            TopicAlreadyExistsCode,
            "Topic diff-ok exists already.",
            await Agree(FutureProbe.Of(syncAgain.All()), FutureProbe.Of(asyncAgain.All())));
    }

    /// <summary>
    /// <c>DeleteTopics</c> by names: an existing topic and a missing one, through the name-collection overload, then
    /// the deleted topic again through the <see cref="TopicCollection"/> overload (now missing). The per-key futures and
    /// <c>All()</c> agree on both mocks, the by-id side is null on both, and the topic left behind is the same.
    /// </summary>
    [Fact]
    public async Task DeleteTopics_ByNames_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-a", 1, 1), new NewTopic("diff-b", 1, 1));

        string[] request = { "diff-a", "diff-missing" };
        DeleteTopicsResult syncResult = sync.DeleteTopics(request);
        AsyncDeleteTopicsResult asyncResult = async.DeleteTopics(request);

        Assert.Null(syncResult.TopicIdValues);
        Assert.Null(asyncResult.TopicIdValues);
        Assert.Equal(request, Sorted(syncResult.TopicNameValues!.Keys));
        Assert.Equal(request, Sorted(asyncResult.TopicNameValues!.Keys));

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.TopicNameValues["diff-a"]), FutureProbe.Of(asyncResult.TopicNameValues["diff-a"])));
        const string Missing = "Topic diff-missing does not exist.";
        AssertKafka(
            UnknownTopicOrPartitionCode,
            Missing,
            await Agree(
                FutureProbe.Of(syncResult.TopicNameValues["diff-missing"]),
                FutureProbe.Of(asyncResult.TopicNameValues["diff-missing"])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            Missing,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        DeleteTopicsResult syncAgain = sync.DeleteTopics(TopicCollection.OfTopicNames(new[] { "diff-a" }));
        AsyncDeleteTopicsResult asyncAgain = async.DeleteTopics(TopicCollection.OfTopicNames(new[] { "diff-a" }));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            "Topic diff-a does not exist.",
            await Agree(FutureProbe.Of(syncAgain.All()), FutureProbe.Of(asyncAgain.All())));

        Assert.Equal(new[] { "diff-b" }, await TopicNames(sync, async));
    }

    /// <summary>
    /// <c>DeleteTopics</c> by ids: each mock's own id for an existing topic, and an id neither mock minted. Each mock
    /// mints its own ids, so the existing topic's key is compared by role, not across mocks; the outcomes agree.
    /// </summary>
    [Fact]
    public async Task DeleteTopics_ByIds_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        (Uuid syncId, Uuid asyncId) = await Seed(sync, async, new NewTopic("diff-c", 1, 1));

        DeleteTopicsResult syncResult = sync.DeleteTopics(TopicCollection.OfTopicIds(new[] { syncId, s_missingId }));
        AsyncDeleteTopicsResult asyncResult = async.DeleteTopics(TopicCollection.OfTopicIds(new[] { asyncId, s_missingId }));

        Assert.Null(syncResult.TopicNameValues);
        Assert.Null(asyncResult.TopicNameValues);
        Assert.Equal(2, syncResult.TopicIdValues!.Count);
        Assert.Equal(2, asyncResult.TopicIdValues!.Count);

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.TopicIdValues[syncId]), FutureProbe.Of(asyncResult.TopicIdValues[asyncId])));
        string missing = "Topic " + s_missingId + " does not exist.";
        AssertKafka(
            UnknownTopicOrPartitionCode,
            missing,
            await Agree(
                FutureProbe.Of(syncResult.TopicIdValues[s_missingId]), FutureProbe.Of(asyncResult.TopicIdValues[s_missingId])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            missing,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        Assert.Empty(await TopicNames(sync, async));
    }

    /// <summary>
    /// <c>DescribeTopics</c> by names: an existing two-partition topic and a missing one through the name-collection
    /// overload, then the existing one alone through the <see cref="TopicCollection"/> overload. The descriptions agree
    /// on everything but the topic id each mock mints, which is compared for presence only.
    /// </summary>
    [Fact]
    public async Task DescribeTopics_ByNames_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-d", 2, 1));

        string[] request = { "diff-d", "diff-nope" };
        DescribeTopicsResult syncResult = sync.DescribeTopics(request);
        AsyncDescribeTopicsResult asyncResult = async.DescribeTopics(request);

        Assert.Null(syncResult.TopicIdValues);
        Assert.Null(asyncResult.TopicIdValues);
        Assert.Null(syncResult.AllTopicIds());
        Assert.Null(asyncResult.AllTopicIds());
        Assert.Equal(request, Sorted(syncResult.TopicNameValues!.Keys));
        Assert.Equal(request, Sorted(asyncResult.TopicNameValues!.Keys));

        TopicDescription described = Assert.IsType<TopicDescription>(
            await Agree(
                FutureProbe.Of(syncResult.TopicNameValues["diff-d"]),
                FutureProbe.Of(asyncResult.TopicNameValues["diff-d"]),
                static outcome => outcome.ToString()!));
        Assert.Equal("diff-d", described.Name);
        Assert.False(described.IsInternal);
        Assert.Equal(2, described.Partitions.Count);
        Assert.NotEqual(Uuid.Zero, described.TopicId);

        const string NotFound = "Topic diff-nope not found.";
        AssertKafka(
            UnknownTopicOrPartitionCode,
            NotFound,
            await Agree(
                FutureProbe.Of(syncResult.TopicNameValues["diff-nope"]),
                FutureProbe.Of(asyncResult.TopicNameValues["diff-nope"])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            NotFound,
            await Agree(FutureProbe.Of(syncResult.AllTopicNames()!.Value), FutureProbe.Of(asyncResult.AllTopicNames()!.Value)));

        DescribeTopicsResult syncOne = sync.DescribeTopics(TopicCollection.OfTopicNames(new[] { "diff-d" }));
        AsyncDescribeTopicsResult asyncOne = async.DescribeTopics(TopicCollection.OfTopicNames(new[] { "diff-d" }));
        IReadOnlyDictionary<string, TopicDescription> gathered =
            Assert.IsAssignableFrom<IReadOnlyDictionary<string, TopicDescription>>(
                await Agree(
                    FutureProbe.Of(syncOne.AllTopicNames()!.Value),
                    FutureProbe.Of(asyncOne.AllTopicNames()!.Value),
                    static outcome => string.Join(
                        ";",
                        ((IReadOnlyDictionary<string, TopicDescription>)outcome)
                            .Select(static entry => entry.Key + "=" + entry.Value)
                            .OrderBy(static entry => entry, StringComparer.Ordinal))));
        Assert.Equal(new[] { "diff-d" }, gathered.Keys);
        Assert.Equal(described.ToString(), gathered["diff-d"].ToString());
    }

    /// <summary>
    /// <c>DescribeTopics</c> by ids: each mock's own id and an id neither minted. The missing id fails with
    /// <c>UNKNOWN_TOPIC_ID</c> on both, and the gathered <c>AllTopicIds()</c> of the found one agrees.
    /// </summary>
    [Fact]
    public async Task DescribeTopics_ByIds_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        (Uuid syncId, Uuid asyncId) = await Seed(sync, async, new NewTopic("diff-e", 1, 1));

        DescribeTopicsResult syncResult = sync.DescribeTopics(TopicCollection.OfTopicIds(new[] { syncId, s_missingId }));
        AsyncDescribeTopicsResult asyncResult = async.DescribeTopics(TopicCollection.OfTopicIds(new[] { asyncId, s_missingId }));

        Assert.Null(syncResult.TopicNameValues);
        Assert.Null(asyncResult.TopicNameValues);
        Assert.Null(syncResult.AllTopicNames());
        Assert.Null(asyncResult.AllTopicNames());

        TopicDescription described = Assert.IsType<TopicDescription>(
            await Agree(
                FutureProbe.Of(syncResult.TopicIdValues![syncId]),
                FutureProbe.Of(asyncResult.TopicIdValues![asyncId]),
                static outcome => outcome.ToString()!));
        Assert.Equal("diff-e", described.Name);
        Assert.Equal(syncId, described.TopicId);

        string notFound = "Topic id" + s_missingId + " not found.";
        AssertKafka(
            UnknownTopicIdCode,
            notFound,
            await Agree(
                FutureProbe.Of(syncResult.TopicIdValues[s_missingId]), FutureProbe.Of(asyncResult.TopicIdValues[s_missingId])));
        AssertKafka(
            UnknownTopicIdCode,
            notFound,
            await Agree(FutureProbe.Of(syncResult.AllTopicIds()!.Value), FutureProbe.Of(asyncResult.AllTopicIds()!.Value)));

        DescribeTopicsResult syncOne = sync.DescribeTopics(TopicCollection.OfTopicIds(new[] { syncId }));
        AsyncDescribeTopicsResult asyncOne = async.DescribeTopics(TopicCollection.OfTopicIds(new[] { asyncId }));
        IReadOnlyDictionary<Uuid, TopicDescription> gathered =
            Assert.IsAssignableFrom<IReadOnlyDictionary<Uuid, TopicDescription>>(
                await Agree(
                    FutureProbe.Of(syncOne.AllTopicIds()!.Value),
                    FutureProbe.Of(asyncOne.AllTopicIds()!.Value),
                    // The keys are each mock's own id, so only the descriptions are compared across mocks.
                    static outcome => string.Join(
                        ";",
                        ((IReadOnlyDictionary<Uuid, TopicDescription>)outcome).Values
                            .Select(static description => description.ToString())
                            .OrderBy(static entry => entry, StringComparer.Ordinal))));
        Assert.Equal(new[] { syncId }, gathered.Keys);
        Assert.Equal(described.ToString(), gathered[syncId].ToString());
    }

    /// <summary>
    /// <c>ListTopics</c>: <c>NamesToListings()</c> and its two projections agree on both mocks over the same seeded
    /// topics. The mock's map is unordered, so names are compared sorted, and topic ids, which each mock mints, are
    /// left out.
    /// </summary>
    [Fact]
    public async Task ListTopics_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-f", 1, 1), new NewTopic("diff-g", 2, 1));

        ListTopicsResult syncResult = sync.ListTopics();
        AsyncListTopicsResult asyncResult = async.ListTopics();
        string[] expected = { "diff-f", "diff-g" };

        IReadOnlyDictionary<string, TopicListing> map = Assert.IsAssignableFrom<IReadOnlyDictionary<string, TopicListing>>(
            await Agree(
                FutureProbe.Of(syncResult.NamesToListings()),
                FutureProbe.Of(asyncResult.NamesToListings()),
                static outcome => Listed(((IReadOnlyDictionary<string, TopicListing>)outcome).Values)));
        Assert.Equal(expected, Sorted(map.Keys));
        Assert.All(map, static entry => Assert.Equal(entry.Key, entry.Value.Name));

        IReadOnlyCollection<string> names = Assert.IsAssignableFrom<IReadOnlyCollection<string>>(
            await Agree(
                FutureProbe.Of(syncResult.Names()),
                FutureProbe.Of(asyncResult.Names()),
                static outcome => string.Join(",", Sorted((IReadOnlyCollection<string>)outcome))));
        Assert.Equal(expected, Sorted(names));

        IReadOnlyCollection<TopicListing> listings = Assert.IsAssignableFrom<IReadOnlyCollection<TopicListing>>(
            await Agree(
                FutureProbe.Of(syncResult.Listings()),
                FutureProbe.Of(asyncResult.Listings()),
                static outcome => Listed((IReadOnlyCollection<TopicListing>)outcome)));
        Assert.Equal(expected, Sorted(listings.Select(static listing => listing.Name)));
        Assert.All(listings, static listing => Assert.False(listing.IsInternal));
    }

    /// <summary>
    /// <c>CreatePartitions</c>: Java's <c>MockAdminClient.createPartitions</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>, which the core mock delivers per key. Both mocks
    /// fail the key and <c>All()</c> with the same error.
    /// </summary>
    [Fact]
    public async Task CreatePartitions_OnBothMocks_FailsAlike_NotImplementedYet()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-h", 1, 1));
        Dictionary<string, NewPartitions> request = new Dictionary<string, NewPartitions>(StringComparer.Ordinal)
        {
            ["diff-h"] = NewPartitions.IncreaseTo(3),
        };

        CreatePartitionsResult syncResult = sync.CreatePartitions(request);
        AsyncCreatePartitionsResult asyncResult = async.CreatePartitions(request);

        Assert.Equal(new[] { "diff-h" }, syncResult.Values.Keys);
        Assert.Equal(new[] { "diff-h" }, asyncResult.Values.Keys);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.Values["diff-h"]), FutureProbe.Of(asyncResult.Values["diff-h"])));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>DeleteRecords</c>: Java's mock throws <c>"Not implemented yet"</c> for a non-empty request, delivered per
    /// partition on both mocks; an empty request succeeds on both with no partitions.
    /// </summary>
    [Fact]
    public async Task DeleteRecords_OnBothMocks_FailsAlike_NotImplementedYet_AndAnEmptyRequestSucceeds()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-i", 1, 1));
        TopicPartition partition = new TopicPartition("diff-i", 0);
        Dictionary<TopicPartition, RecordsToDelete> request = new Dictionary<TopicPartition, RecordsToDelete>
        {
            [partition] = RecordsToDelete.BeforeOffset(5),
        };

        DeleteRecordsResult syncResult = sync.DeleteRecords(request);
        AsyncDeleteRecordsResult asyncResult = async.DeleteRecords(request);

        Assert.Equal(new[] { partition }, syncResult.LowWatermarks.Keys);
        Assert.Equal(new[] { partition }, asyncResult.LowWatermarks.Keys);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.LowWatermarks[partition]), FutureProbe.Of(asyncResult.LowWatermarks[partition])));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        Dictionary<TopicPartition, RecordsToDelete> none = new Dictionary<TopicPartition, RecordsToDelete>();
        DeleteRecordsResult syncEmpty = sync.DeleteRecords(none);
        AsyncDeleteRecordsResult asyncEmpty = async.DeleteRecords(none);
        Assert.Empty(syncEmpty.LowWatermarks);
        Assert.Empty(asyncEmpty.LowWatermarks);
        Assert.Same(BlockingCall.Returned, await Agree(FutureProbe.Of(syncEmpty.All()), FutureProbe.Of(asyncEmpty.All())));
    }

    /// <summary>
    /// <c>DescribeCluster</c> (M15/P14 S5a): the four projections agree on both three-broker mocks — the nodes, the
    /// controller, the mock's cluster id and, when asked for, the empty-but-present authorized-operation set.
    /// </summary>
    [Fact]
    public async Task DescribeCluster_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(3);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(3);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        DescribeClusterOptions options = new DescribeClusterOptions { IncludeAuthorizedOperations = true };

        DescribeClusterResult syncResult = sync.DescribeCluster(options);
        AsyncDescribeClusterResult asyncResult = async.DescribeCluster(options);

        IReadOnlyCollection<Node> nodes = Assert.IsAssignableFrom<IReadOnlyCollection<Node>>(
            await Agree(
                FutureProbe.Of(syncResult.Nodes()),
                FutureProbe.Of(asyncResult.Nodes()),
                static outcome => string.Join(",", Sorted(((IReadOnlyCollection<Node>)outcome).Select(static node => node.ToString())))));
        Assert.Equal(new[] { 0, 1, 2 }, nodes.Select(static node => node.Id).OrderBy(static id => id));

        Node controller = Assert.IsType<Node>(
            await Agree(
                FutureProbe.Of(syncResult.Controller()),
                FutureProbe.Of(asyncResult.Controller()),
                static outcome => outcome.ToString()!));
        Assert.Equal(0, controller.Id);

        Assert.Equal(
            (object)"I4ZmrWqfT2e-upky_4fdPA",
            await Agree(FutureProbe.Of(syncResult.ClusterId()), FutureProbe.Of(asyncResult.ClusterId())));

        IReadOnlyCollection<AclOperation> operations = Assert.IsAssignableFrom<IReadOnlyCollection<AclOperation>>(
            await Agree(
                FutureProbe.Of(syncResult.AuthorizedOperations()),
                FutureProbe.Of(asyncResult.AuthorizedOperations()),
                static outcome => string.Join(",", ((IReadOnlyCollection<AclOperation>)outcome).OrderBy(static op => op))));
        Assert.Empty(operations);
    }

    /// <summary>
    /// <c>ListConfigResources</c> (M15/P14 S5a): with the same topic seeded, the topic filter yields exactly that
    /// topic's resource on both mocks, and the unfiltered call lists the same resources on both.
    /// </summary>
    [Fact]
    public async Task ListConfigResources_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-j", 1, 1));

        IReadOnlyCollection<ConfigResource> topics = Assert.IsAssignableFrom<IReadOnlyCollection<ConfigResource>>(
            await Agree(
                FutureProbe.Of(sync.ListConfigResources(new[] { ConfigResourceType.Topic }).All()),
                FutureProbe.Of(async.ListConfigResources(new[] { ConfigResourceType.Topic }).All()),
                static outcome => Resources((IReadOnlyCollection<ConfigResource>)outcome)));
        Assert.Equal(new ConfigResource(ConfigResourceType.Topic, "diff-j"), Assert.Single(topics));

        IReadOnlyCollection<ConfigResource> every = Assert.IsAssignableFrom<IReadOnlyCollection<ConfigResource>>(
            await Agree(
                FutureProbe.Of(sync.ListConfigResources().All()),
                FutureProbe.Of(async.ListConfigResources().All()),
                static outcome => Resources((IReadOnlyCollection<ConfigResource>)outcome)));
        Assert.Contains(new ConfigResource(ConfigResourceType.Topic, "diff-j"), every);
        Assert.Contains(every, static resource => resource.Type == ConfigResourceType.Broker);
    }

    /// <summary>
    /// <c>DescribeConfigs</c> (M15/P14 S5a): a seeded topic, a missing topic, an existing broker and a missing one.
    /// The per-resource futures and the gathered <c>All()</c> agree on both mocks; <c>All()</c> fails with the first
    /// failure in key order.
    /// </summary>
    [Fact]
    public async Task DescribeConfigs_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        NewTopic topic = new NewTopic("diff-k", 1, 1)
        {
            Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
        };
        await Seed(sync, async, topic);

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, "diff-k");
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "diff-k-missing");
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "0");
        ConfigResource absentBroker = new ConfigResource(ConfigResourceType.Broker, "7");
        ConfigResource[] request = { present, missing, broker, absentBroker };

        DescribeConfigsResult syncResult = sync.DescribeConfigs(request);
        AsyncDescribeConfigsResult asyncResult = async.DescribeConfigs(request);
        Assert.Equal(request, syncResult.Values.Keys);
        Assert.Equal(request, asyncResult.Values.Keys);

        Config config = Assert.IsType<Config>(
            await Agree(
                FutureProbe.Of(syncResult.Values[present]),
                FutureProbe.Of(asyncResult.Values[present]),
                static outcome => string.Join(",", Entries((Config)outcome))));
        Assert.Equal("compact", config.Get("cleanup.policy")!.Value);

        Assert.IsType<Config>(
            await Agree(
                FutureProbe.Of(syncResult.Values[broker]),
                FutureProbe.Of(asyncResult.Values[broker]),
                static outcome => string.Join(",", Entries((Config)outcome))));

        const string MissingTopic = "Resource ConfigResource(type=Topic, name='diff-k-missing') not found.";
        AssertKafka(
            UnknownTopicOrPartitionCode,
            MissingTopic,
            await Agree(FutureProbe.Of(syncResult.Values[missing]), FutureProbe.Of(asyncResult.Values[missing])));
        AssertKafka(
            InvalidRequestCode,
            "Broker 7 not found.",
            await Agree(FutureProbe.Of(syncResult.Values[absentBroker]), FutureProbe.Of(asyncResult.Values[absentBroker])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            MissingTopic,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>IncrementalAlterConfigs</c> (M15/P14 S5a): a <see cref="AlterConfigOpType.Set"/> against a seeded topic and
    /// one against a missing topic. The per-resource void futures and <c>All()</c> agree on both mocks, and the change
    /// is visible to a later describe on both.
    /// </summary>
    [Fact]
    public async Task IncrementalAlterConfigs_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-l", 1, 1));

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, "diff-l");
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "diff-l-absent");
        AlterConfigOp[] set = { new AlterConfigOp(new ConfigEntry("retention.ms", "1000"), AlterConfigOpType.Set) };
        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> request =
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> { [present] = set, [missing] = set };

        AlterConfigsResult syncResult = sync.IncrementalAlterConfigs(request);
        AsyncAlterConfigsResult asyncResult = async.IncrementalAlterConfigs(request);
        Assert.Equal(new[] { present, missing }, syncResult.Values.Keys);
        Assert.Equal(new[] { present, missing }, asyncResult.Values.Keys);

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.Values[present]), FutureProbe.Of(asyncResult.Values[present])));
        const string NoSuchTopic = "No such topic as diff-l-absent";
        AssertKafka(
            UnknownTopicOrPartitionCode,
            NoSuchTopic,
            await Agree(FutureProbe.Of(syncResult.Values[missing]), FutureProbe.Of(asyncResult.Values[missing])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            NoSuchTopic,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        Config described = Assert.IsType<Config>(
            await Agree(
                FutureProbe.Of(sync.DescribeConfigs(new[] { present }).Values[present]),
                FutureProbe.Of(async.DescribeConfigs(new[] { present }).Values[present]),
                static outcome => string.Join(",", Entries((Config)outcome))));
        Assert.Equal("1000", described.Get("retention.ms")!.Value);
    }

    /// <summary>
    /// <c>DescribeLogDirs</c> (M15/P14 S5a): a broker hosting a seeded two-partition topic and a broker the mock does
    /// not have. The per-broker futures and the gathered <c>AllDescriptions()</c> agree on both mocks; the unknown
    /// broker succeeds with an empty map on both.
    /// </summary>
    [Fact]
    public async Task DescribeLogDirs_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-m", 2, 1));
        int[] request = { 0, 5 };

        DescribeLogDirsResult syncResult = sync.DescribeLogDirs(request);
        AsyncDescribeLogDirsResult asyncResult = async.DescribeLogDirs(request);
        Assert.Equal(request, syncResult.Descriptions.Keys);
        Assert.Equal(request, asyncResult.Descriptions.Keys);

        IReadOnlyDictionary<string, LogDirDescription> hosting =
            Assert.IsAssignableFrom<IReadOnlyDictionary<string, LogDirDescription>>(
                await Agree(
                    FutureProbe.Of(syncResult.Descriptions[0]),
                    FutureProbe.Of(asyncResult.Descriptions[0]),
                    static outcome => LogDirs((IReadOnlyDictionary<string, LogDirDescription>)outcome)));
        Assert.Equal("/tmp/kafka-logs:diff-m-0;diff-m-1", LogDirs(hosting));

        IReadOnlyDictionary<string, LogDirDescription> unknown =
            Assert.IsAssignableFrom<IReadOnlyDictionary<string, LogDirDescription>>(
                await Agree(
                    FutureProbe.Of(syncResult.Descriptions[5]),
                    FutureProbe.Of(asyncResult.Descriptions[5]),
                    static outcome => LogDirs((IReadOnlyDictionary<string, LogDirDescription>)outcome)));
        Assert.Empty(unknown);

        IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>> all =
            Assert.IsAssignableFrom<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>(
                await Agree(
                    FutureProbe.Of(syncResult.AllDescriptions()),
                    FutureProbe.Of(asyncResult.AllDescriptions()),
                    static outcome => string.Join(
                        "|",
                        ((IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>)outcome)
                            .OrderBy(static entry => entry.Key)
                            .Select(static entry => entry.Key + "=" + LogDirs(entry.Value)))));
        Assert.Equal(request, all.Keys.OrderBy(static broker => broker));
    }

    /// <summary>
    /// <c>AlterReplicaLogDirs</c> (M15/P14 S5a): a move to the broker's own directory, one to a directory it does not
    /// have, and one naming a broker that does not exist. The per-replica void futures and <c>All()</c> agree on both
    /// mocks, and the accepted move is then described as in progress on both.
    /// </summary>
    [Fact]
    public async Task AlterReplicaLogDirs_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-n", 2, 1));

        TopicPartitionReplica moved = new TopicPartitionReplica("diff-n", 0, 0);
        TopicPartitionReplica offline = new TopicPartitionReplica("diff-n", 1, 0);
        TopicPartitionReplica nowhere = new TopicPartitionReplica("diff-n", 0, 7);
        Dictionary<TopicPartitionReplica, string> request = new Dictionary<TopicPartitionReplica, string>
        {
            [moved] = "/tmp/kafka-logs",
            [offline] = "/mnt/absent",
            [nowhere] = "/tmp/kafka-logs",
        };

        AlterReplicaLogDirsResult syncResult = sync.AlterReplicaLogDirs(request);
        AsyncAlterReplicaLogDirsResult asyncResult = async.AlterReplicaLogDirs(request);
        Assert.Equal(new[] { moved, offline, nowhere }, syncResult.Values.Keys);
        Assert.Equal(new[] { moved, offline, nowhere }, asyncResult.Values.Keys);

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.Values[moved]), FutureProbe.Of(asyncResult.Values[moved])));
        const string Offline = "Log directory /mnt/absent is offline";
        AssertKafka(
            KafkaStorageErrorCode,
            Offline,
            await Agree(FutureProbe.Of(syncResult.Values[offline]), FutureProbe.Of(asyncResult.Values[offline])));
        AssertKafka(
            ReplicaNotAvailableCode,
            "Can't find diff-n-0-7",
            await Agree(FutureProbe.Of(syncResult.Values[nowhere]), FutureProbe.Of(asyncResult.Values[nowhere])));
        AssertKafka(
            KafkaStorageErrorCode,
            Offline,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        object? info = await Agree(
            FutureProbe.Of(sync.DescribeReplicaLogDirs(new[] { moved }).Values[moved]),
            FutureProbe.Of(async.DescribeReplicaLogDirs(new[] { moved }).Values[moved]),
            static outcome => outcome.ToString()!);
        Assert.Equal(
            "(currentReplicaLogDir=/tmp/kafka-logs, futureReplicaLogDir=/tmp/kafka-logs, futureReplicaOffsetLag=0)",
            Assert.IsType<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(info).ToString());
    }

    /// <summary>
    /// <c>DescribeReplicaLogDirs</c> (M15/P14 S5a): a replica at rest and a replica of a topic the mock does not have,
    /// which the mock omits from its answer. The per-replica futures and the gathered <c>All()</c> agree on both mocks;
    /// the omitted replica fails on both with the core's absent-key error, and so does <c>All()</c>.
    /// </summary>
    [Fact]
    public async Task DescribeReplicaLogDirs_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-o", 1, 1));

        TopicPartitionReplica atRest = new TopicPartitionReplica("diff-o", 0, 0);
        TopicPartitionReplica omitted = new TopicPartitionReplica("diff-o-missing", 0, 0);
        TopicPartitionReplica[] request = { atRest, omitted };

        DescribeReplicaLogDirsResult syncResult = sync.DescribeReplicaLogDirs(request);
        AsyncDescribeReplicaLogDirsResult asyncResult = async.DescribeReplicaLogDirs(request);
        Assert.Equal(request, syncResult.Values.Keys);
        Assert.Equal(request, asyncResult.Values.Keys);

        object? info = await Agree(
            FutureProbe.Of(syncResult.Values[atRest]),
            FutureProbe.Of(asyncResult.Values[atRest]),
            static outcome => outcome.ToString()!);
        Assert.Equal(
            "ReplicaLogDirInfo(currentReplicaLogDir=/tmp/kafka-logs)",
            Assert.IsType<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(info).ToString());

        // The core's absent-key error (LOCAL_ILLEGAL_STATE, -4), delivered per key on both flavours.
        const string Absent = "the requested key was not present in the admin RPC's response";
        AssertKafka(
            LocalIllegalStateCode,
            Absent,
            await Agree(FutureProbe.Of(syncResult.Values[omitted]), FutureProbe.Of(asyncResult.Values[omitted])));
        AssertKafka(
            LocalIllegalStateCode,
            Absent,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// The synchronous argument guards of the cluster, configs and log-dir members (M15/P14 S5a) throw alike on both
    /// mocks — the same type, parameter name and message — before any future exists: a null argument, and a null
    /// element where the member takes a collection of references.
    /// </summary>
    [Fact]
    public void ClusterConfigsLogDirsGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, "diff-p");
        TopicPartitionReplica replica = new TopicPartitionReplica("diff-p", 0, 0);

        AssertThrowAlike(() => sync.DescribeConfigs(null!), () => async.DescribeConfigs(null!));
        AssertThrowAlike(() => sync.IncrementalAlterConfigs(null!), () => async.IncrementalAlterConfigs(null!));
        AssertThrowAlike(() => sync.DescribeLogDirs(null!), () => async.DescribeLogDirs(null!));
        AssertThrowAlike(() => sync.AlterReplicaLogDirs(null!), () => async.AlterReplicaLogDirs(null!));
        AssertThrowAlike(() => sync.DescribeReplicaLogDirs(null!), () => async.DescribeReplicaLogDirs(null!));

        AssertThrowAlike<ArgumentException>(
            () => sync.DescribeConfigs(new ConfigResource[] { null! }),
            () => async.DescribeConfigs(new ConfigResource[] { null! }));
        AssertThrowAlike<ArgumentException>(
            () => sync.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> { [resource] = null! }),
            () => async.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> { [resource] = null! }));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string> { [replica] = null! }),
            () => async.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string> { [replica] = null! }));
        AssertThrowAlike<ArgumentException>(
            () => sync.DescribeReplicaLogDirs(new TopicPartitionReplica[] { null! }),
            () => async.DescribeReplicaLogDirs(new TopicPartitionReplica[] { null! }));
    }

    /// <summary>
    /// The synchronous argument guards of the topic-family members throw alike on both mocks — the same type,
    /// parameter name and message — before any future exists.
    /// </summary>
    [Fact]
    public void TopicFamilyGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        AssertThrowAlike(() => sync.DeleteTopics((TopicCollection)null!), () => async.DeleteTopics((TopicCollection)null!));
        AssertThrowAlike(
            () => sync.DeleteTopics((IReadOnlyCollection<string>)null!),
            () => async.DeleteTopics((IReadOnlyCollection<string>)null!));
        AssertThrowAlike(() => sync.DescribeTopics((TopicCollection)null!), () => async.DescribeTopics((TopicCollection)null!));
        AssertThrowAlike(
            () => sync.DescribeTopics((IReadOnlyCollection<string>)null!),
            () => async.DescribeTopics((IReadOnlyCollection<string>)null!));
        AssertThrowAlike(() => sync.CreatePartitions(null!), () => async.CreatePartitions(null!));
        AssertThrowAlike(() => sync.DeleteRecords(null!), () => async.DeleteRecords(null!));
    }

    /// <summary>
    /// <c>ElectLeaders</c> (M15/P14 S5b): Java's <c>MockAdminClient.electLeaders</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c> (<c>MockAdminClient.java:793-797</c>), which the
    /// core mock delivers as the call's own failure. For an explicit selection, an empty one and the null "every
    /// partition" one, <c>Partitions()</c> and <c>All()</c> fail alike on both mocks.
    /// </summary>
    [Fact]
    public async Task ElectLeaders_OnBothMocks_FailsAlike_NotImplementedYet()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        IReadOnlyCollection<TopicPartition>?[] selections =
        {
            new[] { new TopicPartition("diff-q", 0) },
            Array.Empty<TopicPartition>(),
            null,
        };

        foreach (IReadOnlyCollection<TopicPartition>? selection in selections)
        {
            ElectLeadersResult syncResult = sync.ElectLeaders(ElectionType.Preferred, selection);
            AsyncElectLeadersResult asyncResult = async.ElectLeaders(ElectionType.Preferred, selection);

            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.Partitions()), FutureProbe.Of(asyncResult.Partitions())));
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
        }
    }

    /// <summary>
    /// <c>AlterPartitionReassignments</c> (M15/P14 S5b): a reassignment, a cancellation (a null entry, Java's
    /// <c>Optional.empty()</c>) and a reassignment of a partition the mock does not have. The per-partition void
    /// futures and <c>All()</c> agree on both mocks, and a later list shows the same reassignment on both — the
    /// cancelled partition absent.
    /// </summary>
    [Fact]
    public async Task AlterPartitionReassignments_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(3);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(3);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await Seed(sync, async, new NewTopic("diff-r", 2, 1));

        TopicPartition reassigned = new TopicPartition("diff-r", 0);
        TopicPartition cancelled = new TopicPartition("diff-r", 1);
        TopicPartition missing = new TopicPartition("diff-r-missing", 0);
        Dictionary<TopicPartition, NewPartitionReassignment?> request = new Dictionary<TopicPartition, NewPartitionReassignment?>
        {
            [reassigned] = new NewPartitionReassignment(new[] { 0, 2 }),
            [cancelled] = null,
            [missing] = new NewPartitionReassignment(new[] { 0 }),
        };

        AlterPartitionReassignmentsResult syncResult = sync.AlterPartitionReassignments(request);
        AsyncAlterPartitionReassignmentsResult asyncResult = async.AlterPartitionReassignments(request);
        Assert.Equal(new[] { reassigned, cancelled, missing }, syncResult.Values.Keys);
        Assert.Equal(new[] { reassigned, cancelled, missing }, asyncResult.Values.Keys);

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.Values[reassigned]), FutureProbe.Of(asyncResult.Values[reassigned])));
        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.Values[cancelled]), FutureProbe.Of(asyncResult.Values[cancelled])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            UnknownTopicOrPartitionMessage,
            await Agree(FutureProbe.Of(syncResult.Values[missing]), FutureProbe.Of(asyncResult.Values[missing])));
        AssertKafka(
            UnknownTopicOrPartitionCode,
            UnknownTopicOrPartitionMessage,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> listed =
            Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                await Agree(
                    FutureProbe.Of(sync.ListPartitionReassignments(new[] { reassigned, cancelled }).Reassignments()),
                    FutureProbe.Of(async.ListPartitionReassignments(new[] { reassigned, cancelled }).Reassignments()),
                    static outcome => ReassignmentsOf((IReadOnlyDictionary<TopicPartition, PartitionReassignment>)outcome)));
        Assert.Equal(new[] { reassigned }, listed.Keys);
        Assert.Equal(new[] { 0, 2 }, Target(listed[reassigned]));
    }

    /// <summary>
    /// <c>ListPartitionReassignments(partitions, options)</c> (M15/P14 S5b): with the same reassignment made on both
    /// mocks, an explicit selection lists it and omits the quiet partition, an empty selection lists nothing, and the
    /// zero-argument form (a null selection) lists every reassignment — alike on both mocks.
    /// </summary>
    [Fact]
    public async Task ListPartitionReassignments_WithASelection_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(3);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(3);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition reassigned = new TopicPartition("diff-s", 0);
        TopicPartition quiet = new TopicPartition("diff-s", 1);
        await Reassign(sync, async, new NewTopic("diff-s", 2, 2), reassigned, 0, 2);

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> selected =
            Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                await Agree(
                    FutureProbe.Of(sync.ListPartitionReassignments(new[] { reassigned, quiet }).Reassignments()),
                    FutureProbe.Of(async.ListPartitionReassignments(new[] { reassigned, quiet }).Reassignments()),
                    static outcome => ReassignmentsOf((IReadOnlyDictionary<TopicPartition, PartitionReassignment>)outcome)));
        Assert.Equal(new[] { reassigned }, selected.Keys);
        Assert.Equal(new[] { 0, 2 }, Target(selected[reassigned]));

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> none =
            Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                await Agree(
                    FutureProbe.Of(sync.ListPartitionReassignments(Array.Empty<TopicPartition>()).Reassignments()),
                    FutureProbe.Of(async.ListPartitionReassignments(Array.Empty<TopicPartition>()).Reassignments()),
                    static outcome => ReassignmentsOf((IReadOnlyDictionary<TopicPartition, PartitionReassignment>)outcome)));
        Assert.Empty(none);

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> every =
            Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                await Agree(
                    FutureProbe.Of(sync.ListPartitionReassignments().Reassignments()),
                    FutureProbe.Of(async.ListPartitionReassignments().Reassignments()),
                    static outcome => ReassignmentsOf((IReadOnlyDictionary<TopicPartition, PartitionReassignment>)outcome)));
        Assert.Equal(new[] { reassigned }, every.Keys);
    }

    /// <summary>
    /// <c>ListPartitionReassignments(options)</c> (M15/P14 S5b), the options-only overload, which routes on its own
    /// (a null selection, not an empty one): it lists every reassignment on both mocks, the same as
    /// <c>(null, options)</c> does.
    /// </summary>
    [Fact]
    public async Task ListPartitionReassignments_OptionsOnly_OnBothMocks_ListsEveryReassignment()
    {
        MockAdminClient sync = new MockAdminClient(3);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(3);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition reassigned = new TopicPartition("diff-t", 0);
        await Reassign(sync, async, new NewTopic("diff-t", 1, 2), reassigned, 1, 2);
        ListPartitionReassignmentsOptions options = new ListPartitionReassignmentsOptions();

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> optionsOnly =
            Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                await Agree(
                    FutureProbe.Of(sync.ListPartitionReassignments(options).Reassignments()),
                    FutureProbe.Of(async.ListPartitionReassignments(options).Reassignments()),
                    static outcome => ReassignmentsOf((IReadOnlyDictionary<TopicPartition, PartitionReassignment>)outcome)));
        Assert.Equal(new[] { reassigned }, optionsOnly.Keys);
        Assert.Equal(new[] { 1, 2 }, Target(optionsOnly[reassigned]));

        object? explicitNull = await Agree(
            FutureProbe.Of(sync.ListPartitionReassignments(null, options).Reassignments()),
            FutureProbe.Of(async.ListPartitionReassignments(null, options).Reassignments()),
            static outcome => ReassignmentsOf((IReadOnlyDictionary<TopicPartition, PartitionReassignment>)outcome));
        Assert.Equal(
            ReassignmentsOf(optionsOnly),
            ReassignmentsOf(Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(explicitNull)));
    }

    /// <summary>
    /// <c>ListOffsets</c> (M15/P14 S5b): with the same offsets seeded on both mocks, an earliest query, a latest query
    /// and a timestamp query — which Java's mock does not implement (<c>MockAdminClient.java:1230-1231</c> throws from
    /// the call; the core mock fails that partition instead). The per-partition futures and the gathered <c>All()</c>
    /// agree on both mocks, and
    /// <c>PartitionResult</c> rejects an unrequested partition alike, at the call.
    /// </summary>
    [Fact]
    public async Task ListOffsets_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition earliest = new TopicPartition("diff-u", 0);
        TopicPartition latest = new TopicPartition("diff-u", 1);
        TopicPartition byTime = new TopicPartition("diff-u", 2);
        Dictionary<TopicPartition, long> beginning = new Dictionary<TopicPartition, long> { [earliest] = 5L };
        Dictionary<TopicPartition, long> end = new Dictionary<TopicPartition, long> { [latest] = 42L };
        sync.UpdateBeginningOffsets(beginning);
        async.UpdateBeginningOffsets(beginning);
        sync.UpdateEndOffsets(end);
        async.UpdateEndOffsets(end);

        Dictionary<TopicPartition, OffsetSpec> request = new Dictionary<TopicPartition, OffsetSpec>
        {
            [earliest] = OffsetSpec.Earliest(),
            [latest] = OffsetSpec.Latest(),
            [byTime] = OffsetSpec.ForTimestamp(1000L),
        };

        ListOffsetsResult syncResult = sync.ListOffsets(request);
        AsyncListOffsetsResult asyncResult = async.ListOffsets(request);

        ListOffsetsResult.ListOffsetsResultInfo fromEarliest = Assert.IsType<ListOffsetsResult.ListOffsetsResultInfo>(
            await Agree(
                FutureProbe.Of(syncResult.PartitionResult(earliest)),
                FutureProbe.Of(asyncResult.PartitionResult(earliest)),
                static outcome => Offsets((ListOffsetsResult.ListOffsetsResultInfo)outcome)));
        Assert.Equal("5/-1/none", Offsets(fromEarliest));
        ListOffsetsResult.ListOffsetsResultInfo fromLatest = Assert.IsType<ListOffsetsResult.ListOffsetsResultInfo>(
            await Agree(
                FutureProbe.Of(syncResult.PartitionResult(latest)),
                FutureProbe.Of(asyncResult.PartitionResult(latest)),
                static outcome => Offsets((ListOffsetsResult.ListOffsetsResultInfo)outcome)));
        Assert.Equal("42/-1/none", Offsets(fromLatest));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.PartitionResult(byTime)), FutureProbe.Of(asyncResult.PartitionResult(byTime))));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        TopicPartition unrequested = new TopicPartition("diff-u", 7);
        AssertThrowAlike<ArgumentException>(
            () => { _ = syncResult.PartitionResult(unrequested); },
            () => { _ = asyncResult.PartitionResult(unrequested); });
        ArgumentException rejected = Assert.Throws<ArgumentException>(() => { _ = syncResult.PartitionResult(unrequested); });
        Assert.Equal("partition", rejected.ParamName);
        Assert.Equal(
            new ArgumentException("List Offsets for partition \"diff-u-7\" was not attempted", "partition").Message,
            rejected.Message);
    }

    /// <summary>
    /// The synchronous argument guards of the election, reassignment and offset members (M15/P14 S5b) throw alike on
    /// both mocks — the same type, parameter name and message — before any future exists: a null argument where the
    /// member takes a required reference, and a null element (a null topic, a null offset spec) where it takes a
    /// collection. <c>ElectLeaders</c> and <c>ListPartitionReassignments</c> take a null selection as "every
    /// partition", so only their null-element guard applies.
    /// </summary>
    [Fact]
    public void ElectionsReassignmentsOffsetsGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition nullTopic = default;
        TopicPartition partition = new TopicPartition("diff-v", 0);

        AssertThrowAlike(() => sync.AlterPartitionReassignments(null!), () => async.AlterPartitionReassignments(null!));
        AssertThrowAlike(() => sync.ListOffsets(null!), () => async.ListOffsets(null!));

        AssertThrowAlike<ArgumentException>(
            () => sync.ElectLeaders(ElectionType.Preferred, new[] { nullTopic }),
            () => async.ElectLeaders(ElectionType.Preferred, new[] { nullTopic }));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?> { [nullTopic] = new NewPartitionReassignment(new[] { 0 }) }),
            () => async.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?> { [nullTopic] = new NewPartitionReassignment(new[] { 0 }) }));
        AssertThrowAlike<ArgumentException>(
            () => sync.ListPartitionReassignments(new[] { nullTopic }),
            () => async.ListPartitionReassignments(new[] { nullTopic }));
        AssertThrowAlike<ArgumentException>(
            () => sync.ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [nullTopic] = OffsetSpec.Latest() }),
            () => async.ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [nullTopic] = OffsetSpec.Latest() }));
        AssertThrowAlike<ArgumentException>(
            () => sync.ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [partition] = null! }),
            () => async.ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [partition] = null! }));
    }

    /// <summary>
    /// <c>ListGroups</c> (M15/P14 S5c): with the same groups seeded on both mocks — a group config each, which is what
    /// makes either mock list a group (Java's mock lists its <c>groupConfigs</c> keys,
    /// <c>MockAdminClient.java:727-731</c>) — <c>Valid()</c> and <c>All()</c> agree on both mocks and list exactly the
    /// seeded groups, and <c>Errors()</c> is empty on both.
    /// </summary>
    [Fact]
    public async Task ListGroups_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await SeedGroups(sync, async, "diff-w-b", "diff-w-a");

        ListGroupsResult syncResult = sync.ListGroups();
        AsyncListGroupsResult asyncResult = async.ListGroups();

        const string Seeded = "diff-w-a/Consumer/Stable/consumer,diff-w-b/Consumer/Stable/consumer";
        IReadOnlyCollection<GroupListing> valid = Assert.IsAssignableFrom<IReadOnlyCollection<GroupListing>>(
            await Agree(
                FutureProbe.Of(syncResult.Valid()),
                FutureProbe.Of(asyncResult.Valid()),
                static outcome => Listings((IReadOnlyCollection<GroupListing>)outcome)));
        Assert.Equal(Seeded, Listings(valid));
        IReadOnlyCollection<GroupListing> all = Assert.IsAssignableFrom<IReadOnlyCollection<GroupListing>>(
            await Agree(
                FutureProbe.Of(syncResult.All()),
                FutureProbe.Of(asyncResult.All()),
                static outcome => Listings((IReadOnlyCollection<GroupListing>)outcome)));
        Assert.Equal(Seeded, Listings(all));
        IReadOnlyCollection<KafkaException> errors = Assert.IsAssignableFrom<IReadOnlyCollection<KafkaException>>(
            await Agree(
                FutureProbe.Of(syncResult.Errors()),
                FutureProbe.Of(asyncResult.Errors()),
                static outcome => string.Join(",", ((IReadOnlyCollection<KafkaException>)outcome).Select(static e => e.Message))));
        Assert.Empty(errors);
    }

    /// <summary>
    /// <c>DescribeConsumerGroups</c> (M15/P14 S5c), which Java's mock does not implement
    /// (<c>MockAdminClient.java:735</c> throws from the call; the core mock fails every group instead): both mocks key
    /// the result by the requested ids, in request order, and each group's future and the gathered <c>All()</c> fail
    /// alike.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] groups = { "diff-x-b", "diff-x-a" };

        DescribeConsumerGroupsResult syncResult = sync.DescribeConsumerGroups(groups);
        AsyncDescribeConsumerGroupsResult asyncResult = async.DescribeConsumerGroups(groups);
        Assert.Equal(groups, syncResult.DescribedGroups.Keys);
        Assert.Equal(groups, asyncResult.DescribedGroups.Keys);

        foreach (string group in groups)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.DescribedGroups[group]), FutureProbe.Of(asyncResult.DescribedGroups[group])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>DescribeClassicGroups</c> (M15/P14 S5c), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1480</c> throws from the call; the core mock fails every group instead): both mocks key
    /// the result by the requested ids, in request order, and each group's future and the gathered <c>All()</c> fail
    /// alike.
    /// </summary>
    [Fact]
    public async Task DescribeClassicGroups_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] groups = { "diff-xc-b", "diff-xc-a" };

        DescribeClassicGroupsResult syncResult = sync.DescribeClassicGroups(groups);
        AsyncDescribeClassicGroupsResult asyncResult = async.DescribeClassicGroups(groups);
        Assert.Equal(groups, syncResult.DescribedGroups.Keys);
        Assert.Equal(groups, asyncResult.DescribedGroups.Keys);

        foreach (string group in groups)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.DescribedGroups[group]), FutureProbe.Of(asyncResult.DescribedGroups[group])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>ListConsumerGroupOffsets(groupId)</c> (M15/P14 S5c): with the same offsets committed on both mocks, the
    /// single-group path Java's mock implements (<c>MockAdminClient.java:753-759</c>). The no-argument accessor, the
    /// by-id accessor and <c>All()</c> agree on both mocks and carry every committed partition, and an unrequested
    /// group is rejected alike, at the call.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_ByGroupId_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Dictionary<TopicPartition, long> committed = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition("diff-y", 1)] = 9L,
            [new TopicPartition("diff-y", 0)] = 7L,
        };
        sync.UpdateConsumerGroupOffsets(committed);
        async.UpdateConsumerGroupOffsets(committed);

        ListConsumerGroupOffsetsResult syncResult = sync.ListConsumerGroupOffsets("diff-y-group");
        AsyncListConsumerGroupOffsetsResult asyncResult = async.ListConsumerGroupOffsets("diff-y-group");

        const string Committed = "diff-y-0=7/none/'',diff-y-1=9/none/''";
        Assert.Equal(
            Committed,
            GroupOffsets(Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                await Agree(
                    FutureProbe.Of(syncResult.PartitionsToOffsetAndMetadata()),
                    FutureProbe.Of(asyncResult.PartitionsToOffsetAndMetadata()),
                    static outcome => GroupOffsets((IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>)outcome)))));
        Assert.Equal(
            Committed,
            GroupOffsets(Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                await Agree(
                    FutureProbe.Of(syncResult.PartitionsToOffsetAndMetadata("diff-y-group")),
                    FutureProbe.Of(asyncResult.PartitionsToOffsetAndMetadata("diff-y-group")),
                    static outcome => GroupOffsets((IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>)outcome)))));
        Assert.Equal(
            "diff-y-group:" + Committed,
            AllGroupOffsets(
                Assert.IsAssignableFrom<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(
                    await Agree(
                        FutureProbe.Of(syncResult.All()),
                        FutureProbe.Of(asyncResult.All()),
                        static outcome => AllGroupOffsets(
                            (IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>)outcome)))));

        AssertThrowAlike<ArgumentException>(
            () => { _ = syncResult.PartitionsToOffsetAndMetadata("diff-y-absent"); },
            () => { _ = asyncResult.PartitionsToOffsetAndMetadata("diff-y-absent"); });
        ArgumentException unknown = Assert.Throws<ArgumentException>(
            () => { _ = syncResult.PartitionsToOffsetAndMetadata("diff-y-absent"); });
        Assert.Equal("groupId", unknown.ParamName);
        Assert.Equal(
            new ArgumentException("Offsets for consumer group 'diff-y-absent' were not requested.", "groupId").Message,
            unknown.Message);
    }

    /// <summary>
    /// <c>ListConsumerGroupOffsets(groupSpecs)</c> (M15/P14 S5c), with the same offsets committed on both mocks. One
    /// spec selecting one partition takes the single-group path (<c>MockAdminClient.java:753-759</c>, filtered by the
    /// selection) and agrees on both mocks; several specs are what Java's mock refuses
    /// (<c>MockAdminClient.java:750-751</c> throws from the call; the core mock fails every group instead), so each
    /// group's future and <c>All()</c> fail alike, and the no-argument accessor is rejected alike, at the call.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_BySpecs_OnBothMocks_GivesEqualOutcomes()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition selected = new TopicPartition("diff-z", 0);
        Dictionary<TopicPartition, long> committed = new Dictionary<TopicPartition, long>
        {
            [selected] = 3L,
            [new TopicPartition("diff-z", 1)] = 4L,
        };
        sync.UpdateConsumerGroupOffsets(committed);
        async.UpdateConsumerGroupOffsets(committed);

        Dictionary<string, ListConsumerGroupOffsetsSpec> one = new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
        {
            ["diff-z-one"] = new ListConsumerGroupOffsetsSpec { TopicPartitions = new[] { selected } },
        };
        ListConsumerGroupOffsetsResult syncOne = sync.ListConsumerGroupOffsets(one);
        AsyncListConsumerGroupOffsetsResult asyncOne = async.ListConsumerGroupOffsets(one);
        Assert.Equal(
            "diff-z-0=3/none/''",
            GroupOffsets(Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                await Agree(
                    FutureProbe.Of(syncOne.PartitionsToOffsetAndMetadata("diff-z-one")),
                    FutureProbe.Of(asyncOne.PartitionsToOffsetAndMetadata("diff-z-one")),
                    static outcome => GroupOffsets((IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>)outcome)))));

        string[] groups = { "diff-z-b", "diff-z-a" };
        Dictionary<string, ListConsumerGroupOffsetsSpec> several = new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
        {
            [groups[0]] = new ListConsumerGroupOffsetsSpec(),
            [groups[1]] = new ListConsumerGroupOffsetsSpec(),
        };
        ListConsumerGroupOffsetsResult syncSeveral = sync.ListConsumerGroupOffsets(several);
        AsyncListConsumerGroupOffsetsResult asyncSeveral = async.ListConsumerGroupOffsets(several);
        foreach (string group in groups)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(
                    FutureProbe.Of(syncSeveral.PartitionsToOffsetAndMetadata(group)),
                    FutureProbe.Of(asyncSeveral.PartitionsToOffsetAndMetadata(group))));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncSeveral.All()), FutureProbe.Of(asyncSeveral.All())));

        InvalidOperationException syncNoArgument = Assert.Throws<InvalidOperationException>(
            () => { _ = syncSeveral.PartitionsToOffsetAndMetadata(); });
        InvalidOperationException asyncNoArgument = Assert.Throws<InvalidOperationException>(
            () => { _ = asyncSeveral.PartitionsToOffsetAndMetadata(); });
        Assert.Equal(MultipleGroupsRequested, syncNoArgument.Message);
        Assert.Equal(syncNoArgument.Message, asyncNoArgument.Message);
    }

    /// <summary>
    /// <c>DeleteConsumerGroups</c> (M15/P14 S5c), which Java's mock does not implement
    /// (<c>MockAdminClient.java:774</c> throws from the call; the core mock fails every group instead): both mocks key
    /// the result by the requested ids, in request order, and each group's future and <c>All()</c> fail alike.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroups_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] groups = { "diff-aa-b", "diff-aa-a" };

        DeleteConsumerGroupsResult syncResult = sync.DeleteConsumerGroups(groups);
        AsyncDeleteConsumerGroupsResult asyncResult = async.DeleteConsumerGroups(groups);
        Assert.Equal(groups, syncResult.DeletedGroups.Keys);
        Assert.Equal(groups, asyncResult.DeletedGroups.Keys);

        foreach (string group in groups)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.DeletedGroups[group]), FutureProbe.Of(asyncResult.DeletedGroups[group])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>RemoveMembersFromConsumerGroup</c> (M15/P14 S5c), which Java's mock does not implement
    /// (<c>MockAdminClient.java:802</c> throws from the call; the core mock fails the request instead), in both modes.
    /// Naming members, each member's future and <c>All()</c> fail alike, and an unrequested or null member is rejected
    /// alike, at the call. In <c>removeAll</c> mode <c>All()</c> fails alike and <c>MemberResult</c> is rejected alike,
    /// at the call.
    /// </summary>
    [Fact]
    public async Task RemoveMembersFromConsumerGroup_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        MemberToRemove[] members = { new MemberToRemove("diff-ab-2"), new MemberToRemove("diff-ab-1") };

        RemoveMembersFromConsumerGroupOptions named = new RemoveMembersFromConsumerGroupOptions(members);
        RemoveMembersFromConsumerGroupResult syncResult = sync.RemoveMembersFromConsumerGroup("diff-ab-group", named);
        AsyncRemoveMembersFromConsumerGroupResult asyncResult = async.RemoveMembersFromConsumerGroup("diff-ab-group", named);
        foreach (MemberToRemove member in members)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.MemberResult(member)), FutureProbe.Of(asyncResult.MemberResult(member))));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        MemberToRemove unrequested = new MemberToRemove("diff-ab-3");
        AssertThrowAlike<ArgumentException>(
            () => { _ = syncResult.MemberResult(unrequested); },
            () => { _ = asyncResult.MemberResult(unrequested); });
        Assert.Equal(
            "Member diff-ab-3 was not included in the original request",
            Assert.Throws<ArgumentException>(() => { _ = syncResult.MemberResult(unrequested); }).Message);
        AssertThrowAlike(() => { _ = syncResult.MemberResult(null!); }, () => { _ = asyncResult.MemberResult(null!); });
        Assert.Equal("member", Assert.Throws<ArgumentNullException>(() => { _ = syncResult.MemberResult(null!); }).ParamName);

        RemoveMembersFromConsumerGroupOptions everyone = new RemoveMembersFromConsumerGroupOptions();
        RemoveMembersFromConsumerGroupResult syncAll = sync.RemoveMembersFromConsumerGroup("diff-ab-group", everyone);
        AsyncRemoveMembersFromConsumerGroupResult asyncAll = async.RemoveMembersFromConsumerGroup("diff-ab-group", everyone);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncAll.All()), FutureProbe.Of(asyncAll.All())));
        AssertThrowAlike<ArgumentException>(
            () => { _ = syncAll.MemberResult(members[0]); },
            () => { _ = asyncAll.MemberResult(members[0]); });
        Assert.Equal(
            "The method: memberResult is not applicable in 'removeAll' mode",
            Assert.Throws<ArgumentException>(() => { _ = syncAll.MemberResult(members[0]); }).Message);
    }

    /// <summary>
    /// <c>AlterConsumerGroupOffsets</c> (M15/P14 S5c), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1213</c> throws from the call, with its own "Not implement yet" typo; the core mock
    /// fails the request instead): the partition's future and <c>All()</c> fail alike. An unrequested partition is
    /// accepted at the call on both, and its future fails alike at <c>Get()</c> with the request's failure, which wins
    /// over the not-attempted guard.
    /// </summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition partition = new TopicPartition("diff-ac", 0);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [partition] = new OffsetAndMetadata(10L),
        };

        AlterConsumerGroupOffsetsResult syncResult = sync.AlterConsumerGroupOffsets("diff-ac-group", offsets);
        AsyncAlterConsumerGroupOffsetsResult asyncResult = async.AlterConsumerGroupOffsets("diff-ac-group", offsets);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementYet,
            await Agree(FutureProbe.Of(syncResult.PartitionResult(partition)), FutureProbe.Of(asyncResult.PartitionResult(partition))));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        TopicPartition unrequested = new TopicPartition("diff-ac", 7);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementYet,
            await Agree(FutureProbe.Of(syncResult.PartitionResult(unrequested)), FutureProbe.Of(asyncResult.PartitionResult(unrequested))));
    }

    /// <summary>
    /// <c>DeleteConsumerGroupOffsets</c> (M15/P14 S5c), which Java's mock does not implement
    /// (<c>MockAdminClient.java:784</c> throws from the call; the core mock fails the request instead): the
    /// partition's future and <c>All()</c> fail alike, and an unrequested partition is rejected alike, at the call.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition partition = new TopicPartition("diff-ad", 0);

        DeleteConsumerGroupOffsetsResult syncResult = sync.DeleteConsumerGroupOffsets("diff-ad-group", new[] { partition });
        AsyncDeleteConsumerGroupOffsetsResult asyncResult = async.DeleteConsumerGroupOffsets("diff-ad-group", new[] { partition });
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.PartitionResult(partition)), FutureProbe.Of(asyncResult.PartitionResult(partition))));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        TopicPartition unrequested = new TopicPartition("diff-ad", 7);
        AssertThrowAlike<ArgumentException>(
            () => { _ = syncResult.PartitionResult(unrequested); },
            () => { _ = asyncResult.PartitionResult(unrequested); });
        Assert.Equal(
            "Partition diff-ad-7 was not included in the original request",
            Assert.Throws<ArgumentException>(() => { _ = syncResult.PartitionResult(unrequested); }).Message);
    }

    /// <summary>
    /// The synchronous argument guards of the group and group-offset members (M15/P14 S5c) throw alike on both
    /// mocks — the same type, parameter name and message — before any future exists: an undefined enum value in the
    /// <c>ListGroups</c> options, a null argument where a member takes a required reference, and a null element (a null
    /// group id, a null spec, a null topic, a null offset) where it takes a collection.
    /// </summary>
    [Fact]
    public void GroupsGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition nullTopic = default;
        TopicPartition partition = new TopicPartition("diff-ae", 0);
        string?[] nullGroup = { "diff-ae-group", null };
        RemoveMembersFromConsumerGroupOptions everyone = new RemoveMembersFromConsumerGroupOptions();
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [partition] = new OffsetAndMetadata(0L),
        };

        AssertThrowAlike<ArgumentOutOfRangeException>(
            () => sync.ListGroups(new ListGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }),
            () => async.ListGroups(new ListGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }));

        AssertThrowAlike(() => sync.DescribeConsumerGroups(null!), () => async.DescribeConsumerGroups(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.DescribeConsumerGroups(nullGroup!), () => async.DescribeConsumerGroups(nullGroup!));
        AssertThrowAlike(() => sync.DescribeClassicGroups(null!), () => async.DescribeClassicGroups(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.DescribeClassicGroups(nullGroup!), () => async.DescribeClassicGroups(nullGroup!));
        AssertThrowAlike(() => sync.DeleteConsumerGroups(null!), () => async.DeleteConsumerGroups(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.DeleteConsumerGroups(nullGroup!), () => async.DeleteConsumerGroups(nullGroup!));

        AssertThrowAlike(
            () => sync.ListConsumerGroupOffsets((string)null!), () => async.ListConsumerGroupOffsets((string)null!));
        AssertThrowAlike(
            () => sync.ListConsumerGroupOffsets((IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec>)null!),
            () => async.ListConsumerGroupOffsets((IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec>)null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal) { ["diff-ae-group"] = null! }),
            () => async.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal) { ["diff-ae-group"] = null! }));
        AssertThrowAlike<ArgumentException>(
            () => sync.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                {
                    ["diff-ae-group"] = new ListConsumerGroupOffsetsSpec { TopicPartitions = new[] { nullTopic } },
                }),
            () => async.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                {
                    ["diff-ae-group"] = new ListConsumerGroupOffsetsSpec { TopicPartitions = new[] { nullTopic } },
                }));

        AssertThrowAlike(
            () => sync.RemoveMembersFromConsumerGroup(null!, everyone),
            () => async.RemoveMembersFromConsumerGroup(null!, everyone));
        AssertThrowAlike(
            () => sync.RemoveMembersFromConsumerGroup("diff-ae-group", null!),
            () => async.RemoveMembersFromConsumerGroup("diff-ae-group", null!));

        AssertThrowAlike(
            () => sync.AlterConsumerGroupOffsets(null!, offsets), () => async.AlterConsumerGroupOffsets(null!, offsets));
        AssertThrowAlike(
            () => sync.AlterConsumerGroupOffsets("diff-ae-group", null!),
            () => async.AlterConsumerGroupOffsets("diff-ae-group", null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterConsumerGroupOffsets(
                "diff-ae-group", new Dictionary<TopicPartition, OffsetAndMetadata> { [nullTopic] = new OffsetAndMetadata(0L) }),
            () => async.AlterConsumerGroupOffsets(
                "diff-ae-group", new Dictionary<TopicPartition, OffsetAndMetadata> { [nullTopic] = new OffsetAndMetadata(0L) }));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterConsumerGroupOffsets(
                "diff-ae-group", new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = null! }),
            () => async.AlterConsumerGroupOffsets(
                "diff-ae-group", new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = null! }));

        AssertThrowAlike(
            () => sync.DeleteConsumerGroupOffsets(null!, new[] { partition }),
            () => async.DeleteConsumerGroupOffsets(null!, new[] { partition }));
        AssertThrowAlike(
            () => sync.DeleteConsumerGroupOffsets("diff-ae-group", null!),
            () => async.DeleteConsumerGroupOffsets("diff-ae-group", null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.DeleteConsumerGroupOffsets("diff-ae-group", new[] { nullTopic }),
            () => async.DeleteConsumerGroupOffsets("diff-ae-group", new[] { nullTopic }));
    }

    /// <summary>
    /// <c>CreateAcls</c> (M15/P14 S5d), which Java's mock does not implement (<c>MockAdminClient.java:806-808</c> throws
    /// from the call; the core mock fails every binding instead): both mocks key the result by the requested bindings,
    /// in request order and once per distinct binding, and each binding's future and <c>All()</c> fail alike.
    /// </summary>
    [Fact]
    public async Task CreateAcls_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AclBinding[] bindings = { Acl("diff-af-b", "User:bob"), Acl("diff-af-a", "User:alice") };
        AclBinding[] requested = { bindings[0], bindings[1], Acl("diff-af-b", "User:bob") };

        CreateAclsResult syncResult = sync.CreateAcls(requested);
        AsyncCreateAclsResult asyncResult = async.CreateAcls(requested);
        Assert.Equal(bindings, syncResult.Values.Keys);
        Assert.Equal(bindings, asyncResult.Values.Keys);

        foreach (AclBinding binding in bindings)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.Values[binding]), FutureProbe.Of(asyncResult.Values[binding])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>DeleteAcls</c> (M15/P14 S5d), which Java's mock does not implement (<c>MockAdminClient.java:816-818</c> throws
    /// from the call; the core mock fails every filter instead): both mocks key the result by the requested filters, in
    /// request order and once per distinct filter, and each filter's future and <c>All()</c> fail alike.
    /// </summary>
    [Fact]
    public async Task DeleteAcls_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AclBindingFilter[] filters = { AclFilter("diff-ag-b"), AclFilter("diff-ag-a") };
        AclBindingFilter[] requested = { filters[0], filters[1], AclFilter("diff-ag-b") };

        DeleteAclsResult syncResult = sync.DeleteAcls(requested);
        AsyncDeleteAclsResult asyncResult = async.DeleteAcls(requested);
        Assert.Equal(filters, syncResult.Values.Keys);
        Assert.Equal(filters, asyncResult.Values.Keys);

        foreach (AclBindingFilter filter in filters)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.Values[filter]), FutureProbe.Of(asyncResult.Values[filter])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>DescribeAcls</c> (M15/P14 S5d), which Java's mock does not implement (<c>MockAdminClient.java:811-813</c>
    /// throws from the call; the core mock fails the request instead): <c>Values()</c> fails alike.
    /// </summary>
    [Fact]
    public async Task DescribeAcls_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AclBindingFilter filter = AclFilter("diff-ah");

        DescribeAclsResult syncResult = sync.DescribeAcls(filter);
        AsyncDescribeAclsResult asyncResult = async.DescribeAcls(filter);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.Values()), FutureProbe.Of(asyncResult.Values())));
    }

    /// <summary>
    /// <c>DescribeClientQuotas</c> (M15/P14 S5d), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1244-1246</c> throws from the call, with its own "Not implement yet" typo; the core mock
    /// fails the request instead): <c>Entities()</c> fails alike.
    /// </summary>
    [Fact]
    public async Task DescribeClientQuotas_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        ClientQuotaFilter filter = ClientQuotaFilter.ContainsOnly(
            new[] { ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "diff-ai") });

        DescribeClientQuotasResult syncResult = sync.DescribeClientQuotas(filter);
        AsyncDescribeClientQuotasResult asyncResult = async.DescribeClientQuotas(filter);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementYet,
            await Agree(FutureProbe.Of(syncResult.Entities()), FutureProbe.Of(asyncResult.Entities())));
    }

    /// <summary>
    /// <c>AlterClientQuotas</c> (M15/P14 S5d), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1249-1251</c> throws from the call, with its own "Not implement yet" typo; the core mock
    /// fails every entity instead): both mocks key the result by the altered entities, in request order and once per
    /// distinct entity (a second alteration of one entity is sent, not keyed again), and each entity's future and
    /// <c>All()</c> fail alike.
    /// </summary>
    [Fact]
    public async Task AlterClientQuotas_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        ClientQuotaEntity[] entities = { QuotaEntity("diff-aj"), QuotaEntity(null) };
        ClientQuotaAlteration[] alterations =
        {
            QuotaAlteration(entities[0], "producer_byte_rate", 1024d),
            QuotaAlteration(entities[1], "consumer_byte_rate", 2048d),
            QuotaAlteration(QuotaEntity("diff-aj"), "consumer_byte_rate", null),
        };

        AlterClientQuotasResult syncResult = sync.AlterClientQuotas(alterations);
        AsyncAlterClientQuotasResult asyncResult = async.AlterClientQuotas(alterations);
        Assert.Equal(entities, syncResult.Values.Keys);
        Assert.Equal(entities, asyncResult.Values.Keys);

        foreach (ClientQuotaEntity entity in entities)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementYet,
                await Agree(FutureProbe.Of(syncResult.Values[entity]), FutureProbe.Of(asyncResult.Values[entity])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// The synchronous argument guards of the ACL and quota members (M15/P14 S5d) throw alike on both mocks — the same
    /// type, parameter name and message — before any future exists: a null argument where a member takes a required
    /// reference, a null element where it takes a collection, and a request string the native client cannot take
    /// unchanged (an embedded NUL).
    /// </summary>
    [Fact]
    public void AclsAndQuotasGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AclBinding?[] nullBinding = { Acl("diff-ak", "User:alice"), null };
        AclBinding[] nulBinding = { Acl("diff\0ak", "User:alice") };
        AclBindingFilter?[] nullFilter = { AclFilter("diff-ak"), null };
        AclBindingFilter[] nulFilter = { AclFilter("diff\0ak") };
        ClientQuotaFilter nulQuotaFilter = ClientQuotaFilter.ContainsOnly(
            new[] { ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "diff\0ak") });
        ClientQuotaAlteration?[] nullAlteration = { QuotaAlteration(QuotaEntity("diff-ak"), "producer_byte_rate", 1d), null };
        ClientQuotaAlteration[] nulAlteration = { QuotaAlteration(QuotaEntity("diff-ak"), "producer\0byte_rate", 1d) };

        AssertThrowAlike(() => sync.CreateAcls(null!), () => async.CreateAcls(null!));
        AssertThrowAlike<ArgumentException>(() => sync.CreateAcls(nullBinding!), () => async.CreateAcls(nullBinding!));
        AssertThrowAlike<ArgumentException>(() => sync.CreateAcls(nulBinding), () => async.CreateAcls(nulBinding));

        AssertThrowAlike(() => sync.DeleteAcls(null!), () => async.DeleteAcls(null!));
        AssertThrowAlike<ArgumentException>(() => sync.DeleteAcls(nullFilter!), () => async.DeleteAcls(nullFilter!));
        AssertThrowAlike<ArgumentException>(() => sync.DeleteAcls(nulFilter), () => async.DeleteAcls(nulFilter));

        AssertThrowAlike(() => sync.DescribeAcls(null!), () => async.DescribeAcls(null!));
        AssertThrowAlike<ArgumentException>(() => sync.DescribeAcls(nulFilter[0]), () => async.DescribeAcls(nulFilter[0]));

        AssertThrowAlike(() => sync.DescribeClientQuotas(null!), () => async.DescribeClientQuotas(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.DescribeClientQuotas(nulQuotaFilter), () => async.DescribeClientQuotas(nulQuotaFilter));

        AssertThrowAlike(() => sync.AlterClientQuotas(null!), () => async.AlterClientQuotas(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterClientQuotas(nullAlteration!), () => async.AlterClientQuotas(nullAlteration!));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterClientQuotas(nulAlteration), () => async.AlterClientQuotas(nulAlteration));

        // The rows' own pins: the null-element and request-string guards are the core's, with its messages.
        AssertArgument("The ACL bindings must not contain a null element.", "acls", () => sync.CreateAcls(nullBinding!));
        AssertArgument("The ACL filters must not contain a null element.", "filters", () => sync.DeleteAcls(nullFilter!));
        AssertArgument(
            "The client quota alterations must not contain a null element.",
            "entries",
            () => sync.AlterClientQuotas(nullAlteration!));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "acls", () => sync.CreateAcls(nulBinding));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "filters", () => sync.DeleteAcls(nulFilter));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "filter", () => sync.DescribeAcls(nulFilter[0]));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "filter", () => sync.DescribeClientQuotas(nulQuotaFilter));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "entries", () => sync.AlterClientQuotas(nulAlteration));
    }

    /// <summary>
    /// <c>DescribeUserScramCredentials</c> (M15/P14 S5e), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1254-1255</c> throws from the call; the core mock fails the one completion instead):
    /// <c>All()</c>, <c>Users()</c> and a <c>Description</c> fail alike, as Java derives all three from the one
    /// future.
    /// </summary>
    [Fact]
    public async Task DescribeUserScramCredentials_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] users = { "diff-al-alice", "diff-al-bob" };

        DescribeUserScramCredentialsResult syncResult = sync.DescribeUserScramCredentials(users);
        AsyncDescribeUserScramCredentialsResult asyncResult = async.DescribeUserScramCredentials(users);
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.Users()), FutureProbe.Of(asyncResult.Users())));
        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(
                FutureProbe.Of(syncResult.Description(users[0])), FutureProbe.Of(asyncResult.Description(users[0]))));
    }

    /// <summary>
    /// <c>AlterUserScramCredentials</c> (M15/P14 S5e), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1259-1260</c> throws from the call; the core mock fails every user instead): both mocks key
    /// the result by the altered users, in request order and once per distinct user, and each user's future and
    /// <c>All()</c> fail alike.
    /// </summary>
    [Fact]
    public async Task AlterUserScramCredentials_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] users = { "diff-am-bob", "diff-am-alice" };
        UserScramCredentialAlteration[] alterations =
        {
            new UserScramCredentialDeletion(users[0], ScramMechanism.ScramSha256),
            new UserScramCredentialDeletion(users[1], ScramMechanism.ScramSha512),
            new UserScramCredentialDeletion(users[0], ScramMechanism.ScramSha512),
        };

        AlterUserScramCredentialsResult syncResult = sync.AlterUserScramCredentials(alterations);
        AsyncAlterUserScramCredentialsResult asyncResult = async.AlterUserScramCredentials(alterations);
        Assert.Equal(users, syncResult.Values.Keys);
        Assert.Equal(users, asyncResult.Values.Keys);

        foreach (string user in users)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.Values[user]), FutureProbe.Of(asyncResult.Values[user])));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
    }

    /// <summary>
    /// <c>CreateDelegationToken</c> (M15/P14 S5e), which Java's mock implements (<c>MockAdminClient.java:641</c>):
    /// both mocks mint a token owned by the first renewer, with the requested maximum lifetime and the ABI's <c>-1</c>
    /// expiry (token ids, MACs and issue times are each mock's own, so the token is compared through
    /// <see cref="TokenOf"/>); and both fail a request without a renewer alike, with the core mock's message.
    /// </summary>
    [Fact]
    public async Task CreateDelegationToken_OnBothMocks_Agree()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        object? created = await Agree(
            FutureProbe.Of(sync.CreateDelegationToken(Renewers("diff-an-alice", "diff-an-bob")).DelegationToken()),
            FutureProbe.Of(async.CreateDelegationToken(Renewers("diff-an-alice", "diff-an-bob")).DelegationToken()),
            token => TokenOf((DelegationToken)token));
        Assert.Equal(
            "owner=User:diff-an-alice;renewers=User:diff-an-alice,User:diff-an-bob;max=86400000;expiry=-1",
            TokenOf(Assert.IsType<DelegationToken>(created)));

        AssertKafka(
            LocalIllegalArgumentCode,
            NoRenewer,
            await Agree(
                FutureProbe.Of(sync.CreateDelegationToken().DelegationToken()),
                FutureProbe.Of(async.CreateDelegationToken().DelegationToken())));
    }

    /// <summary>
    /// <c>RenewDelegationToken</c> (M15/P14 S5e), which Java's mock implements (<c>MockAdminClient.java:661</c>):
    /// renewing each mock's own token by its MAC gives the renew period as the new expiry on both, and an unknown MAC
    /// fails alike with <c>DELEGATION_TOKEN_NOT_FOUND</c>.
    /// </summary>
    [Fact]
    public async Task RenewDelegationToken_OnBothMocks_Agree()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        byte[] syncMac = (await Minted(FutureProbe.Of(sync.CreateDelegationToken(Renewers("diff-ao")).DelegationToken()))).Hmac;
        byte[] asyncMac =
            (await Minted(FutureProbe.Of(async.CreateDelegationToken(Renewers("diff-ao")).DelegationToken()))).Hmac;
        RenewDelegationTokenOptions options = new RenewDelegationTokenOptions { RenewTimePeriodMs = 3_600_000L };

        object? renewed = await Agree(
            FutureProbe.Of(sync.RenewDelegationToken(syncMac, options).ExpiryTimestamp()),
            FutureProbe.Of(async.RenewDelegationToken(asyncMac, options).ExpiryTimestamp()));
        Assert.Equal(3_600_000L, Assert.IsType<long>(renewed));

        byte[] unknown = { 0xDE, 0x00, 0xAD };
        AssertKafka(
            DelegationTokenNotFoundCode,
            DelegationTokenNotFoundMessage,
            await Agree(
                FutureProbe.Of(sync.RenewDelegationToken(unknown).ExpiryTimestamp()),
                FutureProbe.Of(async.RenewDelegationToken(unknown).ExpiryTimestamp())));
    }

    /// <summary>
    /// <c>ExpireDelegationToken</c> (M15/P14 S5e), which Java's mock implements (<c>MockAdminClient.java:683</c>):
    /// expiring each mock's own token now gives the expiry period on both and removes the token from both; an unknown
    /// MAC fails alike with <c>DELEGATION_TOKEN_NOT_FOUND</c>.
    /// </summary>
    [Fact]
    public async Task ExpireDelegationToken_OnBothMocks_Agree()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        byte[] syncMac = (await Minted(FutureProbe.Of(sync.CreateDelegationToken(Renewers("diff-ap")).DelegationToken()))).Hmac;
        byte[] asyncMac =
            (await Minted(FutureProbe.Of(async.CreateDelegationToken(Renewers("diff-ap")).DelegationToken()))).Hmac;
        ExpireDelegationTokenOptions options = new ExpireDelegationTokenOptions { ExpiryTimePeriodMs = 0L };

        object? expired = await Agree(
            FutureProbe.Of(sync.ExpireDelegationToken(syncMac, options).ExpiryTimestamp()),
            FutureProbe.Of(async.ExpireDelegationToken(asyncMac, options).ExpiryTimestamp()));
        Assert.Equal(0L, Assert.IsType<long>(expired));
        object? remaining = await Agree(
            FutureProbe.Of(sync.DescribeDelegationToken().DelegationTokens()),
            FutureProbe.Of(async.DescribeDelegationToken().DelegationTokens()),
            tokens => TokensOf((IReadOnlyList<DelegationToken>)tokens));
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyList<DelegationToken>>(remaining));

        byte[] unknown = { 0xDE, 0x00, 0xAD };
        AssertKafka(
            DelegationTokenNotFoundCode,
            DelegationTokenNotFoundMessage,
            await Agree(
                FutureProbe.Of(sync.ExpireDelegationToken(unknown).ExpiryTimestamp()),
                FutureProbe.Of(async.ExpireDelegationToken(unknown).ExpiryTimestamp())));
    }

    /// <summary>
    /// <c>DescribeDelegationToken</c> (M15/P14 S5e), which Java's mock implements (<c>MockAdminClient.java:709</c>):
    /// with no owners filter both mocks describe every token they minted, compared through <see cref="TokensOf"/>.
    /// </summary>
    [Fact]
    public async Task DescribeDelegationToken_OnBothMocks_Agree()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        foreach (string renewer in new[] { "diff-aq-alice", "diff-aq-bob" })
        {
            _ = await Minted(FutureProbe.Of(sync.CreateDelegationToken(Renewers(renewer)).DelegationToken()));
            _ = await Minted(FutureProbe.Of(async.CreateDelegationToken(Renewers(renewer)).DelegationToken()));
        }

        object? described = await Agree(
            FutureProbe.Of(sync.DescribeDelegationToken().DelegationTokens()),
            FutureProbe.Of(async.DescribeDelegationToken().DelegationTokens()),
            tokens => TokensOf((IReadOnlyList<DelegationToken>)tokens));
        Assert.Equal(
            "owner=User:diff-aq-alice;renewers=User:diff-aq-alice;max=86400000;expiry=-1|" +
            "owner=User:diff-aq-bob;renewers=User:diff-aq-bob;max=86400000;expiry=-1",
            TokensOf(Assert.IsAssignableFrom<IReadOnlyList<DelegationToken>>(described)));
    }

    /// <summary>
    /// <c>DescribeFeatures</c> (M15/P14 S5e), which Java's mock implements (<c>MockAdminClient.java:1269</c>):
    /// both mocks describe the same seeded feature levels, compared through <see cref="FeaturesOf"/>.
    /// </summary>
    [Fact]
    public async Task DescribeFeatures_OnBothMocks_Agree()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Dictionary<string, (short Level, short MinSupported, short MaxSupported)> levels =
            new Dictionary<string, (short Level, short MinSupported, short MaxSupported)>(StringComparer.Ordinal)
            {
                ["metadata.version"] = (7, 1, 20),
                ["group.version"] = (1, 0, 1),
            };
        sync.SetFeatureLevels(levels);
        async.SetFeatureLevels(levels);

        object? described = await Agree(
            FutureProbe.Of(sync.DescribeFeatures().FeatureMetadata()),
            FutureProbe.Of(async.DescribeFeatures().FeatureMetadata()),
            metadata => FeaturesOf((FeatureMetadata)metadata));
        Assert.Equal(
            "finalized=group.version:1-1,metadata.version:7-7;epoch=123;" +
            "supported=group.version:0-1,metadata.version:1-20",
            FeaturesOf(Assert.IsType<FeatureMetadata>(described)));
    }

    /// <summary>
    /// <c>UpdateFeatures</c> (M15/P14 S5e), which Java's mock implements (<c>MockAdminClient.java:1286</c>): an
    /// update past the seeded bounds fails alike on both mocks, with the core mock's message; an update to level 0
    /// succeeds alike on every key; and an empty map gives an empty result whose <c>All()</c> succeeds, on both.
    /// </summary>
    [Fact]
    public async Task UpdateFeatures_OnBothMocks_Agree()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        // One key, so the core mock's first-failure-aborts-the-batch is not ordered by its map.
        Dictionary<string, FeatureUpdate> rejected = new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
        {
            ["metadata.version"] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
        };
        UpdateFeaturesResult syncRejected = sync.UpdateFeatures(rejected);
        AsyncUpdateFeaturesResult asyncRejected = async.UpdateFeatures(rejected);
        AssertKafka(
            InvalidRequestCode,
            "Invalid update version 1 for feature metadata.version. Can't upgrade above 0",
            await Agree(
                FutureProbe.Of(syncRejected.Values["metadata.version"]),
                FutureProbe.Of(asyncRejected.Values["metadata.version"])));
        AssertKafka(
            InvalidRequestCode,
            "Invalid update version 1 for feature metadata.version. Can't upgrade above 0",
            await Agree(FutureProbe.Of(syncRejected.All()), FutureProbe.Of(asyncRejected.All())));

        string[] features = { "metadata.version", "group.version" };
        Dictionary<string, FeatureUpdate> accepted = features.ToDictionary(
            feature => feature, _ => new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade), StringComparer.Ordinal);
        UpdateFeaturesResult syncAccepted = sync.UpdateFeatures(accepted);
        AsyncUpdateFeaturesResult asyncAccepted = async.UpdateFeatures(accepted);
        Assert.Equal(features, syncAccepted.Values.Keys);
        Assert.Equal(features, asyncAccepted.Values.Keys);
        foreach (string feature in features)
        {
            Assert.Same(
                BlockingCall.Returned,
                await Agree(FutureProbe.Of(syncAccepted.Values[feature]), FutureProbe.Of(asyncAccepted.Values[feature])));
        }

        Assert.Same(BlockingCall.Returned, await Agree(FutureProbe.Of(syncAccepted.All()), FutureProbe.Of(asyncAccepted.All())));

        Dictionary<string, FeatureUpdate> empty = new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal);
        UpdateFeaturesResult syncEmpty = sync.UpdateFeatures(empty);
        AsyncUpdateFeaturesResult asyncEmpty = async.UpdateFeatures(empty);
        Assert.Empty(syncEmpty.Values);
        Assert.Empty(asyncEmpty.Values);
        Assert.Same(BlockingCall.Returned, await Agree(FutureProbe.Of(syncEmpty.All()), FutureProbe.Of(asyncEmpty.All())));
    }

    /// <summary>
    /// The synchronous argument guards of the SCRAM, delegation-token and feature members (M15/P14 S5e) throw alike on
    /// both mocks — the same type, parameter name and message — before any future exists: a null argument where a
    /// member takes a required reference, a null element where it takes a collection, and a request string the native
    /// client cannot take unchanged (an embedded NUL). <c>Description</c>'s guards are the Result's own, at the call
    /// on both flavours.
    /// </summary>
    [Fact]
    public void ScramTokensAndFeaturesGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] nulUser = { "diff\0ar" };
        UserScramCredentialAlteration?[] nullAlteration =
            { new UserScramCredentialDeletion("diff-ar", ScramMechanism.ScramSha256), null };
        UserScramCredentialAlteration[] nulAlteration =
            { new UserScramCredentialDeletion("diff\0ar", ScramMechanism.ScramSha256) };
        Dictionary<string, FeatureUpdate> nullUpdate =
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal) { ["metadata.version"] = null! };
        Dictionary<string, FeatureUpdate> nulFeature = new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
        {
            ["metadata\0version"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
        };

        AssertThrowAlike<ArgumentException>(
            () => sync.DescribeUserScramCredentials(nulUser), () => async.DescribeUserScramCredentials(nulUser));

        AssertThrowAlike(() => sync.AlterUserScramCredentials(null!), () => async.AlterUserScramCredentials(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterUserScramCredentials(nullAlteration!), () => async.AlterUserScramCredentials(nullAlteration!));
        AssertThrowAlike<ArgumentException>(
            () => sync.AlterUserScramCredentials(nulAlteration), () => async.AlterUserScramCredentials(nulAlteration));

        AssertThrowAlike(() => sync.RenewDelegationToken(null!), () => async.RenewDelegationToken(null!));
        AssertThrowAlike(() => sync.ExpireDelegationToken(null!), () => async.ExpireDelegationToken(null!));

        AssertThrowAlike(() => sync.UpdateFeatures(null!), () => async.UpdateFeatures(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.UpdateFeatures(nullUpdate), () => async.UpdateFeatures(nullUpdate));
        AssertThrowAlike<ArgumentException>(() => sync.UpdateFeatures(nulFeature), () => async.UpdateFeatures(nulFeature));

        DescribeUserScramCredentialsResult syncResult = sync.DescribeUserScramCredentials();
        AsyncDescribeUserScramCredentialsResult asyncResult = async.DescribeUserScramCredentials();
        AssertThrowAlike(() => syncResult.Description(null!), () => asyncResult.Description(null!));
        AssertThrowAlike<ArgumentException>(() => syncResult.Description("diff\0ar"), () => asyncResult.Description("diff\0ar"));

        // The rows' own pins: the null-element guards are the binding's, with its messages, and the request-string
        // guard names the member's own parameter.
        Assert.Equal("hmac", Assert.Throws<ArgumentNullException>(() => sync.RenewDelegationToken(null!)).ParamName);
        Assert.Equal("hmac", Assert.Throws<ArgumentNullException>(() => sync.ExpireDelegationToken(null!)).ParamName);
        Assert.Equal("alterations", Assert.Throws<ArgumentNullException>(() => sync.AlterUserScramCredentials(null!)).ParamName);
        Assert.Equal("featureUpdates", Assert.Throws<ArgumentNullException>(() => sync.UpdateFeatures(null!)).ParamName);
        Assert.Equal("userName", Assert.Throws<ArgumentNullException>(() => syncResult.Description(null!)).ParamName);
        AssertArgument(
            "The SCRAM credential alterations must not contain a null element.",
            "alterations",
            () => sync.AlterUserScramCredentials(nullAlteration!));
        AssertArgument(
            "The feature updates must not contain a null update.", "featureUpdates", () => sync.UpdateFeatures(nullUpdate));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "users", () => sync.DescribeUserScramCredentials(nulUser));
        AssertArgument(
            Internal.AdminStrings.InvalidStringMessage, "alterations", () => sync.AlterUserScramCredentials(nulAlteration));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "featureUpdates", () => sync.UpdateFeatures(nulFeature));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "userName", () => syncResult.Description("diff\0ar"));
    }

    /// <summary>
    /// <c>FenceProducers</c> (M15/P14 S5f), which Java's mock does not implement (<c>MockAdminClient.java:1396</c>
    /// throws from the call; the core mock fails every id instead): both mocks key the result by the requested ids, in
    /// request order and once per distinct id, and each id's erased future, its <c>ProducerId</c> / <c>EpochId</c>
    /// projections and <c>All()</c> fail alike; an empty request is an empty success on both.
    /// </summary>
    [Fact]
    public async Task FenceProducers_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] ids = { "diff-fp-b", "diff-fp-a" };
        string[] requested = { ids[0], ids[1], ids[0] };

        FenceProducersResult syncResult = sync.FenceProducers(requested);
        AsyncFenceProducersResult asyncResult = async.FenceProducers(requested);
        Assert.Equal(ids, syncResult.FencedProducers.Keys);
        Assert.Equal(ids, asyncResult.FencedProducers.Keys);

        foreach (string id in ids)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.FencedProducers[id]), FutureProbe.Of(asyncResult.FencedProducers[id])));
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.ProducerId(id)), FutureProbe.Of(asyncResult.ProducerId(id))));
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.EpochId(id)), FutureProbe.Of(asyncResult.EpochId(id))));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        FenceProducersResult syncEmpty = sync.FenceProducers(Array.Empty<string>());
        AsyncFenceProducersResult asyncEmpty = async.FenceProducers(Array.Empty<string>());
        Assert.Empty(syncEmpty.FencedProducers);
        Assert.Empty(asyncEmpty.FencedProducers);
        Assert.Same(BlockingCall.Returned, await Agree(FutureProbe.Of(syncEmpty.All()), FutureProbe.Of(asyncEmpty.All())));
    }

    /// <summary>
    /// <c>DescribeTransactions</c> (M15/P14 S5f), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1376</c> throws from the call; the core mock fails every id instead): each requested id's
    /// <c>Description</c> and <c>All()</c> fail alike; an empty request gathers an equal, empty map on both.
    /// </summary>
    [Fact]
    public async Task DescribeTransactions_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] ids = { "diff-dt-b", "diff-dt-a", "diff-dt-b" };

        DescribeTransactionsResult syncResult = sync.DescribeTransactions(ids);
        AsyncDescribeTransactionsResult asyncResult = async.DescribeTransactions(ids);
        foreach (string id in ids)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.Description(id)), FutureProbe.Of(asyncResult.Description(id))));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<string, TransactionDescription>>(
            await Agree(
                FutureProbe.Of(sync.DescribeTransactions(Array.Empty<string>()).All()),
                FutureProbe.Of(async.DescribeTransactions(Array.Empty<string>()).All()))));
    }

    /// <summary>
    /// <c>DescribeProducers</c> (M15/P14 S5f), which Java's mock does not implement (<c>MockAdminClient.java:1371</c>
    /// throws from the call; the core mock fails every partition instead): each requested partition's
    /// <c>PartitionResult</c> and <c>All()</c> fail alike, a repeated partition included; an empty request gathers an
    /// equal, empty map on both.
    /// </summary>
    [Fact]
    public async Task DescribeProducers_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        TopicPartition[] partitions =
        {
            new TopicPartition("diff-dp", 1), new TopicPartition("diff-dp", 0), new TopicPartition("diff-dp", 1),
        };

        DescribeProducersResult syncResult = sync.DescribeProducers(partitions);
        AsyncDescribeProducersResult asyncResult = async.DescribeProducers(partitions);
        foreach (TopicPartition partition in partitions)
        {
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(
                    FutureProbe.Of(syncResult.PartitionResult(partition)), FutureProbe.Of(asyncResult.PartitionResult(partition))));
        }

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>>(
            await Agree(
                FutureProbe.Of(sync.DescribeProducers(Array.Empty<TopicPartition>()).All()),
                FutureProbe.Of(async.DescribeProducers(Array.Empty<TopicPartition>()).All()))));
    }

    /// <summary>
    /// <c>ListTransactions</c> (M15/P14 S5f), which Java's mock does not implement (<c>MockAdminClient.java:1391</c>
    /// throws from the call; the core mock fails the broker discovery instead): <c>ByBrokerId()</c>,
    /// <c>AllByBrokerId()</c> and <c>All()</c> fail alike, with every filter set as with none.
    /// </summary>
    [Fact]
    public async Task ListTransactions_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        ListTransactionsOptions filtered = new ListTransactionsOptions
        {
            FilteredStates = new[] { TransactionState.Ongoing },
            FilteredProducerIds = new[] { 7L },
            FilteredDuration = 1_000L,
            FilteredTransactionalIdPattern = "diff-lt-.*",
        };

        foreach (ListTransactionsOptions? options in new[] { null, filtered })
        {
            ListTransactionsResult syncResult = sync.ListTransactions(options);
            AsyncListTransactionsResult asyncResult = async.ListTransactions(options);
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.ByBrokerId()), FutureProbe.Of(asyncResult.ByBrokerId())));
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.AllByBrokerId()), FutureProbe.Of(asyncResult.AllByBrokerId())));
            AssertKafka(
                UnsupportedVersionCode,
                NotImplementedYet,
                await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));
        }
    }

    /// <summary>
    /// <c>AbortTransaction</c> (M15/P14 S5f), which Java's mock does not implement (<c>MockAdminClient.java:1381</c>
    /// throws from the call; the core mock fails the one completion instead): <c>All()</c> fails alike.
    /// </summary>
    [Fact]
    public async Task AbortTransaction_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AbortTransactionSpec spec = new AbortTransactionSpec(new TopicPartition("diff-at", 3), 91_234L, 7, 42);

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(FutureProbe.Of(sync.AbortTransaction(spec).All()), FutureProbe.Of(async.AbortTransaction(spec).All())));
    }

    /// <summary>
    /// <c>ForceTerminateTransaction</c> (M15/P14 S5f), which Java's mock does not implement
    /// (<c>MockAdminClient.java:1386</c> throws from the call; the core mock fails the one completion instead):
    /// <c>Result()</c> fails alike.
    /// </summary>
    [Fact]
    public async Task ForceTerminateTransaction_OnBothMocks_FailsAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        AssertKafka(
            UnsupportedVersionCode,
            NotImplementedYet,
            await Agree(
                FutureProbe.Of(sync.ForceTerminateTransaction("diff-ft").Result()),
                FutureProbe.Of(async.ForceTerminateTransaction("diff-ft").Result())));
    }

    /// <summary>
    /// The synchronous argument guards of the producer and transaction members (M15/P14 S5f) throw alike on both mocks —
    /// the same type, parameter name and message — before any future exists: a null argument where a member takes a
    /// required reference, a null element (or a partition with a null topic) where it takes a collection, and a request
    /// string the native client cannot take unchanged (an embedded NUL). The Results' own lookup guards
    /// (<c>ProducerId</c>, <c>EpochId</c>, <c>Description</c>, <c>PartitionResult</c>) are at the call on both flavours.
    /// </summary>
    [Fact]
    public void ProducersAndTransactionsGuards_OnBothMocks_ThrowAlike()
    {
        MockAdminClient sync = new MockAdminClient(1);
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        string[] nullId = { "diff-pg", null! };
        string[] nulId = { "diff\0pg" };
        TopicPartition[] nullTopic = { new TopicPartition("diff-pg", 0), default };
        TopicPartition[] nulTopic = { new TopicPartition("diff\0pg", 0) };
        AbortTransactionSpec nullTopicSpec = new AbortTransactionSpec(default, 1L, 1, 1);
        AbortTransactionSpec nulTopicSpec = new AbortTransactionSpec(new TopicPartition("diff\0pg", 0), 1L, 1, 1);
        ListTransactionsOptions nulPattern = new ListTransactionsOptions { FilteredTransactionalIdPattern = "diff\0pg" };

        AssertThrowAlike(() => sync.FenceProducers(null!), () => async.FenceProducers(null!));
        AssertThrowAlike<ArgumentException>(() => sync.FenceProducers(nullId), () => async.FenceProducers(nullId));
        AssertThrowAlike<ArgumentException>(() => sync.FenceProducers(nulId), () => async.FenceProducers(nulId));

        AssertThrowAlike(() => sync.DescribeTransactions(null!), () => async.DescribeTransactions(null!));
        AssertThrowAlike<ArgumentException>(() => sync.DescribeTransactions(nullId), () => async.DescribeTransactions(nullId));
        AssertThrowAlike<ArgumentException>(() => sync.DescribeTransactions(nulId), () => async.DescribeTransactions(nulId));

        AssertThrowAlike(() => sync.DescribeProducers(null!), () => async.DescribeProducers(null!));
        AssertThrowAlike<ArgumentException>(() => sync.DescribeProducers(nullTopic), () => async.DescribeProducers(nullTopic));
        AssertThrowAlike<ArgumentException>(() => sync.DescribeProducers(nulTopic), () => async.DescribeProducers(nulTopic));

        AssertThrowAlike<ArgumentException>(() => sync.ListTransactions(nulPattern), () => async.ListTransactions(nulPattern));

        AssertThrowAlike(() => sync.AbortTransaction(null!), () => async.AbortTransaction(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.AbortTransaction(nullTopicSpec), () => async.AbortTransaction(nullTopicSpec));
        AssertThrowAlike<ArgumentException>(
            () => sync.AbortTransaction(nulTopicSpec), () => async.AbortTransaction(nulTopicSpec));

        AssertThrowAlike(() => sync.ForceTerminateTransaction(null!), () => async.ForceTerminateTransaction(null!));
        AssertThrowAlike<ArgumentException>(
            () => sync.ForceTerminateTransaction("diff\0pg"), () => async.ForceTerminateTransaction("diff\0pg"));

        FenceProducersResult syncFenced = sync.FenceProducers(new[] { "diff-pg" });
        AsyncFenceProducersResult asyncFenced = async.FenceProducers(new[] { "diff-pg" });
        AssertThrowAlike(() => syncFenced.ProducerId(null!), () => asyncFenced.ProducerId(null!));
        AssertThrowAlike(() => syncFenced.EpochId(null!), () => asyncFenced.EpochId(null!));
        AssertThrowAlike<ArgumentException>(() => syncFenced.ProducerId("diff-missing"), () => asyncFenced.ProducerId("diff-missing"));
        AssertThrowAlike<ArgumentException>(() => syncFenced.EpochId("diff-missing"), () => asyncFenced.EpochId("diff-missing"));

        DescribeTransactionsResult syncDescribed = sync.DescribeTransactions(new[] { "diff-pg" });
        AsyncDescribeTransactionsResult asyncDescribed = async.DescribeTransactions(new[] { "diff-pg" });
        AssertThrowAlike(() => syncDescribed.Description(null!), () => asyncDescribed.Description(null!));
        AssertThrowAlike<ArgumentException>(
            () => syncDescribed.Description("diff-missing"), () => asyncDescribed.Description("diff-missing"));

        DescribeProducersResult syncProducers = sync.DescribeProducers(new[] { new TopicPartition("diff-pg", 0) });
        AsyncDescribeProducersResult asyncProducers = async.DescribeProducers(new[] { new TopicPartition("diff-pg", 0) });
        TopicPartition missing = new TopicPartition("diff-missing", 0);
        AssertThrowAlike<ArgumentException>(
            () => syncProducers.PartitionResult(missing), () => asyncProducers.PartitionResult(missing));

        // The rows' own pins: each guard names the member's own parameter, with the binding's (or Java's) message.
        Assert.Equal("transactionalIds", Assert.Throws<ArgumentNullException>(() => sync.FenceProducers(null!)).ParamName);
        Assert.Equal("transactionalIds", Assert.Throws<ArgumentNullException>(() => sync.DescribeTransactions(null!)).ParamName);
        Assert.Equal("partitions", Assert.Throws<ArgumentNullException>(() => sync.DescribeProducers(null!)).ParamName);
        Assert.Equal("spec", Assert.Throws<ArgumentNullException>(() => sync.AbortTransaction(null!)).ParamName);
        Assert.Equal(
            "transactionalId", Assert.Throws<ArgumentNullException>(() => sync.ForceTerminateTransaction(null!)).ParamName);
        Assert.Equal("transactionalId", Assert.Throws<ArgumentNullException>(() => syncFenced.ProducerId(null!)).ParamName);
        Assert.Equal("transactionalId", Assert.Throws<ArgumentNullException>(() => syncDescribed.Description(null!)).ParamName);
        AssertArgument(
            "The transactional ids must not contain a null element.", "transactionalIds", () => sync.FenceProducers(nullId));
        AssertArgument(
            "The transactional ids must not contain a null element.", "transactionalIds", () => sync.DescribeTransactions(nullId));
        AssertArgument(
            "The partitions must not contain a topic partition with a null topic.",
            "partitions",
            () => sync.DescribeProducers(nullTopic));
        AssertArgument(
            "The abort transaction spec must not carry a topic partition with a null topic.",
            "spec",
            () => sync.AbortTransaction(nullTopicSpec));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "transactionalIds", () => sync.FenceProducers(nulId));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "transactionalIds", () => sync.DescribeTransactions(nulId));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "partitions", () => sync.DescribeProducers(nulTopic));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "options", () => sync.ListTransactions(nulPattern));
        AssertArgument(Internal.AdminStrings.InvalidStringMessage, "spec", () => sync.AbortTransaction(nulTopicSpec));
        AssertArgument(
            Internal.AdminStrings.InvalidStringMessage, "transactionalId", () => sync.ForceTerminateTransaction("diff\0pg"));
        AssertArgument(
            "TransactionalId `diff-missing` was not included in the request",
            "transactionalId",
            () => syncFenced.ProducerId("diff-missing"));
        AssertArgument(
            "TransactionalId `diff-missing` was not included in the request",
            "transactionalId",
            () => syncDescribed.Description("diff-missing"));
        AssertArgument(
            "Topic partition " + missing + " was not included in the request",
            "partition",
            () => syncProducers.PartitionResult(missing));
    }

    /// <summary>
    /// <paramref name="call"/> throws exactly <see cref="ArgumentException"/>, naming <paramref name="parameterName"/>
    /// with <paramref name="message"/>.
    /// </summary>
    private static void AssertArgument(string message, string parameterName, Action call)
    {
        ArgumentException failure = Assert.Throws<ArgumentException>(call);
        Assert.Equal(parameterName, failure.ParamName);
        Assert.Equal(new ArgumentException(message, parameterName).Message, failure.Message);
    }

    /// <summary>
    /// Runs both probes to their outcomes and asserts they agree: equal values, or exceptions of the same type with
    /// the same message (and code). Returns the sync outcome for the row's own pin.
    /// </summary>
    private static async Task<object?> Agree(FutureProbe sync, FutureProbe async)
    {
        object? syncOutcome = await sync.Outcome();
        object? asyncOutcome = await async.Outcome();
        AssertAgree(syncOutcome, asyncOutcome);
        return syncOutcome;
    }

    /// <summary>
    /// <see cref="Agree(FutureProbe, FutureProbe)"/> for a value comparable only through a projection — one carrying an
    /// id each mock mints, or an unordered collection. A failure must agree as there; a value must agree on
    /// <paramref name="project"/>. Returns the sync outcome for the row's own pin.
    /// </summary>
    private static async Task<object?> Agree(FutureProbe sync, FutureProbe async, Func<object, string> project)
    {
        object? syncOutcome = await sync.Outcome();
        object? asyncOutcome = await async.Outcome();
        if (syncOutcome is Exception || asyncOutcome is Exception || syncOutcome is null || asyncOutcome is null)
        {
            AssertAgree(syncOutcome, asyncOutcome);
        }
        else
        {
            Assert.Equal(project(syncOutcome), project(asyncOutcome));
        }

        return syncOutcome;
    }

    private static void AssertAgree(object? syncOutcome, object? asyncOutcome)
    {
        if (syncOutcome is Exception syncFailure)
        {
            Exception asyncFailure = Assert.IsAssignableFrom<Exception>(asyncOutcome);
            Assert.Equal(syncFailure.GetType(), asyncFailure.GetType());
            Assert.Equal(syncFailure.Message, asyncFailure.Message);
            if (syncFailure is KafkaException syncKafka)
            {
                KafkaException asyncKafka = (KafkaException)asyncFailure;
                Assert.Equal(syncKafka.Code, asyncKafka.Code);
                Assert.Equal(syncKafka.IsRetriable, asyncKafka.IsRetriable);
            }
        }
        else if (ReferenceEquals(syncOutcome, BlockingCall.Returned))
        {
            Assert.Same(BlockingCall.Returned, asyncOutcome);
        }
        else
        {
            Assert.Equal(syncOutcome, asyncOutcome);
        }
    }

    private static void AssertKafka(int code, string message, object? outcome)
    {
        KafkaException failure = Assert.IsType<KafkaException>(outcome);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
    }

    private static List<string> Entries(Config config) =>
        config.Entries.Select(static entry => entry.Name + "=" + entry.Value).OrderBy(static e => e, StringComparer.Ordinal).ToList();

    private static List<string> Sorted(IEnumerable<string> values) =>
        values.OrderBy(static value => value, StringComparer.Ordinal).ToList();

    private static string Listed(IEnumerable<TopicListing> listings) =>
        string.Join(",", Sorted(listings.Select(static listing => listing.Name + "/" + (listing.IsInternal ? "internal" : "user"))));

    private static string Resources(IEnumerable<ConfigResource> resources) =>
        string.Join(",", Sorted(resources.Select(static resource => resource.ToString())));

    /// <summary>A reassignment map, comparable across mocks: each partition, sorted, with its reassignment.</summary>
    private static string ReassignmentsOf(IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments) =>
        string.Join("|", Sorted(reassignments.Select(static entry => entry.Key + "=" + entry.Value)));

    /// <summary>
    /// The target replicas a reassignment moves to: its current replicas plus those being added, less those being
    /// removed, sorted.
    /// </summary>
    private static List<int> Target(PartitionReassignment reassignment) =>
        reassignment.Replicas
            .Concat(reassignment.AddingReplicas)
            .Except(reassignment.RemovingReplicas)
            .Distinct()
            .OrderBy(static broker => broker)
            .ToList();

    private static string Offsets(ListOffsetsResult.ListOffsetsResultInfo info) =>
        info.Offset + "/" + info.Timestamp + "/" + (info.LeaderEpoch?.ToString() ?? "none");

    /// <summary>Group listings, comparable across mocks: each group id, sorted, with its type, state and protocol.</summary>
    private static string Listings(IEnumerable<GroupListing> listings) =>
        string.Join(
            ",",
            Sorted(listings.Select(static listing =>
                listing.GroupId + "/" + listing.Type + "/" + listing.GroupState + "/" + listing.Protocol)));

    /// <summary>
    /// One group's committed offsets, comparable across mocks: each partition, sorted, with its offset, leader epoch
    /// and quoted metadata, or <c>null</c> for a partition with no committed offset.
    /// </summary>
    private static string GroupOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets) =>
        string.Join(
            ",",
            Sorted(offsets.Select(static entry =>
                entry.Key + "="
                + (entry.Value is null
                    ? "null"
                    : entry.Value.Offset + "/" + (entry.Value.LeaderEpoch?.ToString() ?? "none") + "/'" + entry.Value.Metadata + "'"))));

    /// <summary>Every group's committed offsets, comparable across mocks: each group id, sorted, with its offsets.</summary>
    private static string AllGroupOffsets(
        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> groups) =>
        string.Join(
            "|",
            groups.OrderBy(static entry => entry.Key, StringComparer.Ordinal)
                .Select(static entry => entry.Key + ":" + GroupOffsets(entry.Value)));

    /// <summary>
    /// Gives each of <paramref name="groupIds"/> a group config on both mocks, asserting both agree — a group config is
    /// what makes either mock list a group.
    /// </summary>
    private static async Task SeedGroups(MockAdminClient sync, AsyncMockAdminClient async, params string[] groupIds)
    {
        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> request = groupIds.ToDictionary(
            static groupId => new ConfigResource(ConfigResourceType.Group, groupId),
            static _ => (IReadOnlyCollection<AlterConfigOp>)new[]
            {
                new AlterConfigOp(new ConfigEntry("consumer.session.timeout.ms", "45000"), AlterConfigOpType.Set),
            });
        Assert.Same(
            BlockingCall.Returned,
            await Agree(
                FutureProbe.Of(sync.IncrementalAlterConfigs(request).All()),
                FutureProbe.Of(async.IncrementalAlterConfigs(request).All())));
    }

    /// <summary>
    /// Creates <paramref name="topic"/> on both mocks and reassigns <paramref name="partition"/> to
    /// <paramref name="targetReplicas"/> on both, asserting both agree.
    /// </summary>
    private static async Task Reassign(
        MockAdminClient sync,
        AsyncMockAdminClient async,
        NewTopic topic,
        TopicPartition partition,
        params int[] targetReplicas)
    {
        await Seed(sync, async, topic);
        Dictionary<TopicPartition, NewPartitionReassignment?> request = new Dictionary<TopicPartition, NewPartitionReassignment?>
        {
            [partition] = new NewPartitionReassignment(targetReplicas),
        };
        Assert.Same(
            BlockingCall.Returned,
            await Agree(
                FutureProbe.Of(sync.AlterPartitionReassignments(request).All()),
                FutureProbe.Of(async.AlterPartitionReassignments(request).All())));
    }

    /// <summary>
    /// One broker's log directories, comparable across mocks: each path, sorted, with its error (if any) and its
    /// replicas' partitions, sorted.
    /// </summary>
    private static string LogDirs(IReadOnlyDictionary<string, LogDirDescription> directories) =>
        string.Join(
            "|",
            directories.OrderBy(static entry => entry.Key, StringComparer.Ordinal).Select(static entry =>
                entry.Key
                + (entry.Value.Error is null ? string.Empty : "!" + entry.Value.Error.Message)
                + ":"
                + string.Join(";", Sorted(entry.Value.ReplicaInfos.Keys.Select(static partition => partition.ToString())))));

    /// <summary>
    /// Creates <paramref name="topics"/> on both mocks, asserting they agree, and returns each mock's own id for the
    /// first topic.
    /// </summary>
    private static async Task<(Uuid Sync, Uuid Async)> Seed(
        MockAdminClient sync, AsyncMockAdminClient async, params NewTopic[] topics)
    {
        CreateTopicsResult syncCreated = sync.CreateTopics(topics);
        AsyncCreateTopicsResult asyncCreated = async.CreateTopics(topics);
        Assert.Same(BlockingCall.Returned, await Agree(FutureProbe.Of(syncCreated.All()), FutureProbe.Of(asyncCreated.All())));

        Uuid syncId = Assert.IsType<Uuid>(await FutureProbe.Of(syncCreated.TopicId(topics[0].Name)).Outcome());
        Uuid asyncId = Assert.IsType<Uuid>(await FutureProbe.Of(asyncCreated.TopicId(topics[0].Name)).Outcome());
        Assert.NotEqual(Uuid.Zero, syncId);
        Assert.NotEqual(Uuid.Zero, asyncId);
        return (syncId, asyncId);
    }

    /// <summary>The topic names both mocks list, which must agree, sorted.</summary>
    private static async Task<List<string>> TopicNames(MockAdminClient sync, AsyncMockAdminClient async)
    {
        IReadOnlyCollection<string> names = Assert.IsAssignableFrom<IReadOnlyCollection<string>>(
            await Agree(
                FutureProbe.Of(sync.ListTopics().Names()),
                FutureProbe.Of(async.ListTopics().Names()),
                static outcome => string.Join(",", Sorted((IReadOnlyCollection<string>)outcome))));
        return Sorted(names);
    }

    private static AclBinding Acl(string topic, string principal) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, topic, PatternType.Literal),
            new AccessControlEntry(principal, "*", AclOperation.Write, AclPermissionType.Allow));

    private static AclBindingFilter AclFilter(string topic) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, topic, PatternType.Literal),
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Any, AclPermissionType.Any));

    /// <summary>A user entity; a <see langword="null"/> name is the default user.</summary>
    private static ClientQuotaEntity QuotaEntity(string? user) =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = user });

    /// <summary>A token request whose renewers are the users <paramref name="names"/>, for a day.</summary>
    private static CreateDelegationTokenOptions Renewers(params string[] names) =>
        new CreateDelegationTokenOptions
        {
            Renewers = names.Select(name => new KafkaPrincipal("User", name)).ToArray(),
            MaxLifetimeMs = 86_400_000L,
        };

    /// <summary>
    /// The token a mock minted, read through <paramref name="minted"/> within its deadline; a mint that failed fails the
    /// test here rather than in a later step.
    /// </summary>
    private static async Task<DelegationToken> Minted(FutureProbe minted) =>
        Assert.IsType<DelegationToken>(await minted.Outcome());

    /// <summary>A token's mock-independent fields: its owner, renewers, maximum lifetime and expiry.</summary>
    private static string TokenOf(DelegationToken token) =>
        $"owner={token.TokenInfo.OwnerAsString};renewers={string.Join(",", token.TokenInfo.RenewersAsString)};" +
        $"max={token.TokenInfo.MaxTimestamp};expiry={token.TokenInfo.ExpiryTimestamp}";

    /// <summary>Every token's <see cref="TokenOf"/>, in ordinal order.</summary>
    private static string TokensOf(IReadOnlyList<DelegationToken> tokens) =>
        string.Join("|", tokens.Select(TokenOf).OrderBy(token => token, StringComparer.Ordinal));

    /// <summary>A feature description's two tables, each in ordinal key order, and its epoch.</summary>
    private static string FeaturesOf(FeatureMetadata metadata) =>
        "finalized=" + string.Join(
            ",",
            metadata.FinalizedFeatures
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(entry => $"{entry.Key}:{entry.Value.MinVersionLevel}-{entry.Value.MaxVersionLevel}")) +
        $";epoch={metadata.FinalizedFeaturesEpoch};supported=" + string.Join(
            ",",
            metadata.SupportedFeatures
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(entry => $"{entry.Key}:{entry.Value.MinVersion}-{entry.Value.MaxVersion}"));

    /// <summary>One quota of <paramref name="entity"/> set to <paramref name="value"/>, or removed when it is null.</summary>
    private static ClientQuotaAlteration QuotaAlteration(ClientQuotaEntity entity, string quota, double? value) =>
        new ClientQuotaAlteration(entity, new[] { new ClientQuotaAlteration.Op(quota, value) });

    /// <summary>
    /// Both calls throw synchronously, alike: an <see cref="ArgumentNullException"/> with the same message and
    /// parameter name.
    /// </summary>
    private static void AssertThrowAlike(Action sync, Action async) => AssertThrowAlike<ArgumentNullException>(sync, async);

    /// <summary>
    /// Both calls throw synchronously, alike: exactly <typeparamref name="TException"/>, with the same message and
    /// parameter name.
    /// </summary>
    private static void AssertThrowAlike<TException>(Action sync, Action async)
        where TException : ArgumentException
    {
        TException syncFailure = Assert.Throws<TException>(sync);
        TException asyncFailure = Assert.Throws<TException>(async);
        Assert.Equal(syncFailure.ParamName, asyncFailure.ParamName);
        Assert.Equal(syncFailure.Message, asyncFailure.Message);
    }
}
