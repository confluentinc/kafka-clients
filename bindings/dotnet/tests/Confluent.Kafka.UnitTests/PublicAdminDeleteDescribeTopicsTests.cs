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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>deleteTopics</c> and <c>describeTopics</c> end to end against
/// <see cref="MockAdminClient"/> — both key forms, no broker.
/// </summary>
public sealed class PublicAdminDeleteDescribeTopicsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The base64 topic-id round trip in full: a <see cref="Uuid"/> goes out as Java's
    /// <c>Uuid.toString()</c> text, the ABI hands the same text back as the result key,
    /// and it parses to the <b>identical</b> <see cref="Uuid"/> — which is why the per-key
    /// bridge's key type had to become generic at all.
    /// </summary>
    /// <remarks>
    /// The id is not invented: it is the one the broker assigned, read back through
    /// <c>describeTopics</c> by name. So the assertion covers the ABI's own round trip
    /// (<c>topic_id</c> out of a description, then in as a request key, then out again as
    /// a result key), not just <see cref="Uuid.Parse"/> against
    /// <see cref="Uuid.ToString"/>.
    /// </remarks>
    [Fact]
    public async Task TopicId_RoundTripsThroughTheAbiAsBase64_AndComesBackAsTheSameUuid()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        const string Topic = "p2a-roundtrip";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 1) }).All(), s_deadline);

        DescribeTopicsResult byName = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        TopicDescription described = await TestTimeout.Run(() => byName.TopicNameValues![Topic], s_deadline);

        Uuid topicId = described.TopicId;
        Assert.NotEqual(Uuid.Zero, topicId);

        // Out as base64, back as the same Uuid — the result is keyed by the parsed id.
        DescribeTopicsResult byId = admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { topicId }));
        Assert.True(byId.TopicIdValues!.ContainsKey(topicId));

        TopicDescription again = await TestTimeout.Run(() => byId.TopicIdValues![topicId], s_deadline);
        Assert.Equal(Topic, again.Name);
        Assert.Equal(topicId, again.TopicId);

        // …and the same for deleteTopics, whose key reader is a different delegate.
        DeleteTopicsResult deleted = admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { topicId }));
        Assert.True(deleted.TopicIdValues!.ContainsKey(topicId));
        await TestTimeout.Run(() => deleted.TopicIdValues![topicId], s_deadline);
    }

    /// <summary>
    /// ⚠ <b>Decision D8 — a wrong-accessor read returns <see langword="null"/></b>, in
    /// both directions and on both result types. Java's javadoc says so
    /// (<c>DeleteTopicsResult.java:51-67</c>, <c>DescribeTopicsResult.java:54-72</c>), and
    /// every C# instinct says otherwise: throwing or returning an empty map both look
    /// tidier and both silently break a caller ported from Java, who wrote
    /// <c>if (result.topicIdValues() != null)</c>.
    /// </summary>
    [Fact]
    public async Task WrongAccessor_ReturnsNull_BothDirections_BothResultTypes()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        const string Alpha = "p2a-d8-alpha";
        const string Beta = "p2a-d8-beta";

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Alpha, 1, 1), new NewTopic(Beta, 1, 1) }).All(),
            s_deadline);

        DescribeTopicsResult describedByName =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Alpha }));
        Assert.NotNull(describedByName.TopicNameValues);
        Assert.Null(describedByName.TopicIdValues);
        Assert.NotNull(describedByName.AllTopicNames());
        Assert.Null(describedByName.AllTopicIds());

        Uuid alphaId = (await TestTimeout.Run(
            () => describedByName.TopicNameValues![Alpha], s_deadline)).TopicId;

        DescribeTopicsResult describedById = admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { alphaId }));
        Assert.NotNull(describedById.TopicIdValues);
        Assert.Null(describedById.TopicNameValues);
        Assert.NotNull(describedById.AllTopicIds());
        Assert.Null(describedById.AllTopicNames());

        DeleteTopicsResult deletedByName = admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Alpha }));
        Assert.NotNull(deletedByName.TopicNameValues);
        Assert.Null(deletedByName.TopicIdValues);
        await TestTimeout.Run(deletedByName.All, s_deadline);

        DescribeTopicsResult betaByName = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Beta }));
        Uuid betaId = (await TestTimeout.Run(() => betaByName.TopicNameValues![Beta], s_deadline)).TopicId;

        DeleteTopicsResult deletedById = admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { betaId }));
        Assert.NotNull(deletedById.TopicIdValues);
        Assert.Null(deletedById.TopicNameValues);
        await TestTimeout.Run(deletedById.All, s_deadline);
    }

    /// <summary>
    /// The discriminator against "fault everything as soon as any key fails": one topic
    /// succeeds and one fails <b>in the same batch</b>, each carrying its own outcome, and
    /// the aggregate faults.
    /// </summary>
    [Fact]
    public async Task MixedOutcome_EachKeyCarriesItsOwnResult()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        const string Present = "p2a-mixed-present";
        const string Absent = "p2a-mixed-absent";

        // A second live topic for the describe half: the delete half consumes `Present`.
        const string StillPresent = "p2a-mixed-still-present";

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Present, 1, 1), new NewTopic(StillPresent, 1, 1) }).All(),
            s_deadline);

        DeleteTopicsResult deleted =
            admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Present, Absent }));

        await TestTimeout.Run(() => deleted.TopicNameValues![Present], s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => deleted.TopicNameValues![Absent], s_deadline));
        Assert.Equal($"Topic {Absent} does not exist.", failure.Message);

        // all() aggregates the failure rather than hanging or reporting success.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(deleted.All, s_deadline));

        // …and the same shape on describeTopics, where success also carries a value.
        DescribeTopicsResult described =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Absent, StillPresent }));
        Assert.Equal(StillPresent, (await TestTimeout.Run(
            () => described.TopicNameValues![StillPresent], s_deadline)).Name);

        KafkaException describeFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => described.TopicNameValues![Absent], s_deadline));
        Assert.Equal($"Topic {Absent} not found.", describeFailure.Message);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => described.AllTopicNames()!, s_deadline));
    }

    /// <summary>
    /// The description tree is fully <b>copied out</b> of the native result before its
    /// root is destroyed, so every string, node and flag read here is owned managed state
    /// rather than a pointer into freed memory. The result root is destroyed by the
    /// completion trampoline's <c>finally</c>, which has already run by the time the
    /// awaited value is observable.
    /// </summary>
    [Fact]
    public async Task DescriptionTree_IsCopiedOut_AndOutlivesTheResultRoot()
    {
        using MockAdminClient admin = new MockAdminClient(3);
        const string Topic = "p2a-tree";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 3) }).All(), s_deadline);

        DescribeTopicsResult result = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        TopicDescription description = await TestTimeout.Run(() => result.TopicNameValues![Topic], s_deadline);

        Assert.Equal(Topic, description.Name);
        Assert.False(description.IsInternal);
        Assert.Equal(2, description.Partitions.Count);

        TopicPartitionInfo partition = description.Partitions[0];
        Assert.Equal(0, partition.Partition);
        Assert.NotNull(partition.Leader);
        Assert.Equal(3, partition.Replicas.Count);

        // The Node tree came through the shipped consumer-side NodeMarshal, so its
        // host/port are the mock's own broker records rather than defaults.
        Assert.NotEmpty(partition.Leader!.Host);
        Assert.True(partition.Leader.Port > 0);

        // ToString touches every branch, including the absent-set spelling, so a stale
        // pointer anywhere in the tree would surface here rather than silently.
        Assert.Contains("partition=0", partition.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠ <b>Absent and empty are different facts, and the marshaller must read the
    /// <c>has_*</c> discriminant rather than the count.</b> The mock reports
    /// <em>present-but-empty</em> sets for all three (Java's mock likewise passes
    /// <c>Collections.emptySet()</c>), so this pins the direction the mock <em>can</em>
    /// reach: a count of 0 with the discriminant true must yield a <b>non-null empty</b>
    /// collection, never <see langword="null"/>. An implementation that derived
    /// nullability from <c>count == 0</c> — the natural shortcut — goes red here.
    /// </summary>
    /// <remarks>
    /// The other direction (<c>has_* == false</c> ⇒ <see langword="null"/>) has <b>no
    /// broker-free vehicle</b>: the mock always reports the sets, and the ABI exposes no
    /// way to fabricate a <c>TopicDescription_t</c>. It is covered by the nullable
    /// annotations pinned in <see cref="PublicAdminP2aShapeParityTests"/> and by the
    /// marshaller reading the discriminant, not by a test — stated rather than papered
    /// over.
    /// </remarks>
    [Fact]
    public async Task ReportedButEmptySets_AreEmptyCollections_NotNull()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        const string Topic = "p2a-empty-sets";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        DescribeTopicsResult result = admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            new DescribeTopicsOptions { IncludeAuthorizedOperations = true });

        TopicDescription description = await TestTimeout.Run(() => result.TopicNameValues![Topic], s_deadline);

        Assert.NotNull(description.AuthorizedOperations);
        Assert.Empty(description.AuthorizedOperations!);

        TopicPartitionInfo partition = description.Partitions[0];
        Assert.NotNull(partition.Elr);
        Assert.Empty(partition.Elr!);
        Assert.NotNull(partition.LastKnownElr);
        Assert.Empty(partition.LastKnownElr!);
        Assert.NotNull(partition.InSyncReplicas);
    }

    /// <summary>
    /// A repeated key yields one entry, as Java's map-keyed result does — for both key
    /// forms, since the two take different de-duplication paths.
    /// </summary>
    [Fact]
    public async Task RepeatedKeys_YieldOneEntry()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        const string Topic = "p2a-dedup";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        DescribeTopicsResult byName =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic, Topic, Topic }));
        Assert.Single(byName.TopicNameValues!);

        Uuid topicId = (await TestTimeout.Run(() => byName.TopicNameValues![Topic], s_deadline)).TopicId;

        DescribeTopicsResult byId =
            admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { topicId, topicId }));
        Assert.Single(byId.TopicIdValues!);
    }

    /// <summary>
    /// Preconditions are validated <b>before</b> any native call (ffi §B5) and raise .NET
    /// exceptions, never <see cref="KafkaException"/>. The two negative-number guards are
    /// deliberately stricter than Java, because the ABI silently rereads a negative as
    /// "unset" — the caller would never learn the value they asked for was discarded.
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Throws<ArgumentNullException>(() => admin.DeleteTopics(null!));
        Assert.Throws<ArgumentNullException>(() => admin.DescribeTopics(null!));

        ArgumentException nullName = Assert.Throws<ArgumentException>(
            () => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "alpha", null! })));
        Assert.StartsWith(
            "The topic names must not contain a null element.", nullName.Message, StringComparison.Ordinal);
        Assert.Equal("topics", nullName.ParamName);

        ArgumentOutOfRangeException deleteTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DeleteTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DeleteTopicsOptions { TimeoutMs = -1 }));
        Assert.StartsWith(
            "DeleteTopicsOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            deleteTimeout.Message,
            StringComparison.Ordinal);
        Assert.Equal("options", deleteTimeout.ParamName);

        ArgumentOutOfRangeException describeTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DescribeTopicsOptions { TimeoutMs = -5 }));
        Assert.StartsWith(
            "DescribeTopicsOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            describeTimeout.Message,
            StringComparison.Ordinal);

        ArgumentOutOfRangeException limit = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DescribeTopicsOptions { PartitionSizeLimitPerResponse = -1 }));
        Assert.StartsWith(
            "DescribeTopicsOptions.PartitionSizeLimitPerResponse must not be negative.",
            limit.Message,
            StringComparison.Ordinal);

        // Zero is passed through untouched, as Java would — only a negative is rejected.
        admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { "alpha" }),
            new DescribeTopicsOptions { PartitionSizeLimitPerResponse = 0 });
    }

    /// <summary>
    /// Both RPCs reject use after close, like <c>CreateTopics</c>.
    /// </summary>
    [Fact]
    public void AfterClose_BothRpcsThrowObjectDisposed()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "alpha" })));
        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "alpha" })));
    }

    /// <summary>
    /// A non-ASCII topic name survives both directions of the hand-rolled UTF-8
    /// marshalling — out as a pinned NUL-terminated request key, back as the borrowed
    /// result key and as <see cref="TopicDescription.Name"/> (ffi §B3; the <c>LPStr</c>
    /// guard, which corrupts silently in an ASCII-only suite).
    /// </summary>
    [Fact]
    public async Task NonAsciiTopicName_RoundTripsThroughBothRpcs()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        const string Topic = "p2a-témas-日本語";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        DescribeTopicsResult described = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        Assert.Equal(Topic, (await TestTimeout.Run(
            () => described.TopicNameValues![Topic], s_deadline)).Name);

        DeleteTopicsResult deleted = admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        Assert.True(deleted.TopicNameValues!.ContainsKey(Topic));
        await TestTimeout.Run(deleted.All, s_deadline);
    }

    /// <summary>
    /// The whole surface works through the <see cref="IAdmin"/> interface, on the real
    /// client type as well as the mock — the two clients differ only in construction.
    /// </summary>
    [Fact]
    public void BothClientsImplementTheRpcs()
    {
        IAdmin mock = new MockAdminClient(1);
        try
        {
            Assert.NotNull(mock.DescribeTopics(TopicCollection.OfTopicNames(Array.Empty<string>())));
        }
        finally
        {
            mock.Dispose();
        }

        IAdmin real = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
        try
        {
            // No broker is contacted: an empty collection has no key to submit for.
            Assert.NotNull(real.DeleteTopics(TopicCollection.OfTopicNames(Array.Empty<string>())));
        }
        finally
        {
            real.Dispose();
        }
    }
}
