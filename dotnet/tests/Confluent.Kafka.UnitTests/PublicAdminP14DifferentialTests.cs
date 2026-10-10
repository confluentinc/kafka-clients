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
/// The differential rows of M15/P14 S4, S5a and S5b: each converted RPC, called with the same arguments on a
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

    // The core's default message for UNKNOWN_TOPIC_OR_PARTITION, which the core mock's reassignment of an unknown
    // partition carries (Java's mock throws a message-less UnknownTopicOrPartitionException).
    private const string UnknownTopicOrPartitionMessage = "This server does not host this topic-partition.";

    private static readonly Uuid s_missingId = new Uuid(0x7A7A7A7A7A7A7A7AL, 0x5B5B5B5B5B5B5B5BL);

    /// <summary>
    /// <c>CreateTopics</c>: a creatable topic and one whose replication factor exceeds the broker count, then the
    /// creatable one again (now existing). Values, <c>All()</c> and the four typed accessors agree on both mocks. The
    /// topic id is minted by each mock independently, so only its presence is compared.
    /// </summary>
    [Fact]
    public async Task CreateTopics_OnBothMocks_GivesEqualOutcomes()
    {
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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

        string notFound = "Topic id " + s_missingId + " not found.";
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(3);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(3);
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
            (object)"4A5xz_QZTB2CtL4wc0X0Jw",
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);

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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(3);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(3);
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
        using MockAdminClient sync = new MockAdminClient(3);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(3);
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
        using MockAdminClient sync = new MockAdminClient(3);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(3);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
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
