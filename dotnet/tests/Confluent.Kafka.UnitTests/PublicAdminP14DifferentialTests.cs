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
/// The differential rows of M15/P14 S4: each converted RPC, called with the same arguments on a
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

    private const string NotImplementedYet = "Not implemented yet";

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
    /// Both calls throw synchronously, alike: the same type, message and parameter name — an
    /// <see cref="ArgumentNullException"/> for every guard in this file.
    /// </summary>
    private static void AssertThrowAlike(Action sync, Action async)
    {
        ArgumentNullException syncFailure = Assert.Throws<ArgumentNullException>(sync);
        ArgumentNullException asyncFailure = Assert.Throws<ArgumentNullException>(async);
        Assert.Equal(syncFailure.ParamName, asyncFailure.ParamName);
        Assert.Equal(syncFailure.Message, asyncFailure.Message);
    }
}
