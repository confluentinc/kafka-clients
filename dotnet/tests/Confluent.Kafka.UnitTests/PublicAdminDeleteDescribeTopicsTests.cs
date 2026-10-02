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
using System.Reflection;
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
    /// exceptions, never <see cref="KafkaException"/>. The negative
    /// <c>PartitionSizeLimitPerResponse</c> guard is deliberately stricter than Java, because
    /// the ABI silently rereads a negative as "unset" — the caller would never learn the value
    /// they asked for was discarded. A negative <c>TimeoutMs</c> is not rejected (M15/P13.5 X2):
    /// it is sent as 0, Java's <c>calcDeadlineMs</c> clamp.
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        // The cast picks the TopicCollection overload: a lone literal null matches the
        // name-collection one too (CS0121) since M15/P13.5 G1-10.
        Assert.Throws<ArgumentNullException>(() => admin.DeleteTopics((TopicCollection)null!));
        Assert.Throws<ArgumentNullException>(() => admin.DescribeTopics((TopicCollection)null!));

        ArgumentException nullName = Assert.Throws<ArgumentException>(
            () => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "alpha", null! })));
        Assert.StartsWith(
            "The topic names must not contain a null element.", nullName.Message, StringComparison.Ordinal);
        Assert.Equal("topics", nullName.ParamName);

        Assert.Null(Record.Exception(
            () => admin.DeleteTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DeleteTopicsOptions { TimeoutMs = -1 })));
        Assert.Null(Record.Exception(
            () => admin.DescribeTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DescribeTopicsOptions { TimeoutMs = -5 })));

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

    /// <summary>
    /// M15/P13.5 G1-10: Java's name-collection overloads — <c>deleteTopics(Collection&lt;String&gt;)</c>
    /// / <c>(…, DeleteTopicsOptions)</c> and <c>describeTopics(Collection&lt;String&gt;)</c> /
    /// <c>(…, DescribeTopicsOptions)</c> (<c>Admin.java:212</c>, <c>:226</c>, <c>:295</c>,
    /// <c>:306</c>) — exist on <see cref="IAdmin"/> and on both clients, each as one member
    /// whose options default to <see langword="null"/>.
    /// </summary>
    /// <param name="type">The type the overloads must be declared on.</param>
    [Theory]
    [InlineData(typeof(IAdmin))]
    [InlineData(typeof(KafkaAdminClient))]
    [InlineData(typeof(MockAdminClient))]
    public void G1_10_TheNameCollectionOverloads_AreDeclared(Type type)
    {
        MethodInfo? delete = type.GetMethod(
            nameof(IAdmin.DeleteTopics),
            new[] { typeof(IReadOnlyCollection<string>), typeof(DeleteTopicsOptions) });
        MethodInfo? describe = type.GetMethod(
            nameof(IAdmin.DescribeTopics),
            new[] { typeof(IReadOnlyCollection<string>), typeof(DescribeTopicsOptions) });

        Assert.NotNull(delete);
        Assert.NotNull(describe);
        Assert.Equal(typeof(DeleteTopicsResult), delete!.ReturnType);
        Assert.Equal(typeof(DescribeTopicsResult), describe!.ReturnType);

        foreach (MethodInfo method in new[] { delete, describe })
        {
            ParameterInfo[] parameters = method.GetParameters();
            Assert.Equal("topicNames", parameters[0].Name);
            Assert.False(parameters[0].IsOptional);
            Assert.True(parameters[1].IsOptional);
            Assert.Null(parameters[1].DefaultValue);
        }

        // Each RPC now has exactly the two forms: TopicCollection and the name collection.
        Assert.Equal(2, type.GetMethods().Count(method => method.Name == nameof(IAdmin.DeleteTopics)));
        Assert.Equal(2, type.GetMethods().Count(method => method.Name == nameof(IAdmin.DescribeTopics)));
    }

    /// <summary>
    /// M15/P13.5 G1-10: the name-collection forms do exactly what the
    /// <see cref="TopicCollection"/> form does with <see cref="TopicCollection.OfTopicNames"/>
    /// — the same per-name keys, the same success values, the same failures — through the
    /// <see cref="IAdmin"/> interface.
    /// </summary>
    [Fact]
    public async Task G1_10_TheNameCollectionForms_BehaveAsTheTopicCollectionForm()
    {
        using MockAdminClient mock = new MockAdminClient(1);
        IAdmin admin = mock;
        const string Present = "p135-g110-present";
        const string Absent = "p135-g110-absent";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Present, 2, 1) }).All(), s_deadline);

        DescribeTopicsResult byName = admin.DescribeTopics(new[] { Present, Absent });
        DescribeTopicsResult byCollection =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Present, Absent }));
        Assert.Null(byName.TopicIdValues);
        Assert.Equal(
            byCollection.TopicNameValues!.Keys.OrderBy(name => name, StringComparer.Ordinal),
            byName.TopicNameValues!.Keys.OrderBy(name => name, StringComparer.Ordinal));

        TopicDescription named = await TestTimeout.Run(() => byName.TopicNameValues![Present], s_deadline);
        TopicDescription collected = await TestTimeout.Run(() => byCollection.TopicNameValues![Present], s_deadline);
        Assert.Equal(Present, named.Name);
        Assert.Equal(collected.Partitions.Count, named.Partitions.Count);
        Assert.Equal(collected.TopicId, named.TopicId);

        KafkaException describeFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => byName.TopicNameValues![Absent], s_deadline));
        Assert.Equal($"Topic {Absent} not found.", describeFailure.Message);

        DeleteTopicsResult deleted = admin.DeleteTopics(new[] { Present, Absent }, new DeleteTopicsOptions());
        Assert.Null(deleted.TopicIdValues);
        await TestTimeout.Run(() => deleted.TopicNameValues![Present], s_deadline);
        KafkaException deleteFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => deleted.TopicNameValues![Absent], s_deadline));
        Assert.Equal($"Topic {Absent} does not exist.", deleteFailure.Message);

        // The delete took effect: describing the topic by name now fails.
        DescribeTopicsResult afterDelete = admin.DescribeTopics(new[] { Present }, new DescribeTopicsOptions());
        KafkaException gone = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => afterDelete.TopicNameValues![Present], s_deadline));
        Assert.Equal($"Topic {Present} not found.", gone.Message);
    }

    /// <summary>
    /// M15/P13.5 G1-10: the name-collection forms hand their options through. Each call is
    /// compared with the <see cref="TopicCollection"/> form given the same options, so the
    /// assertion is about forwarding, not about what any one option does.
    /// </summary>
    /// <remarks>
    /// The <c>PartitionSizeLimitPerResponse</c> rejection is synchronous, so it is compared on
    /// both clients. A negative <c>TimeoutMs</c> no longer is (M15/P13.5 X2 sends it as 0), so it
    /// is compared through the outcome instead, on the real client only: with no broker, both
    /// forms fail through the result with the same timeout error, while a form that dropped its
    /// options would wait out the client default and fail the bound. The mock ignores timeouts,
    /// so there the timeout has no outcome to compare.
    /// </remarks>
    [Fact]
    public async Task G1_10_TheNameCollectionForms_ForwardTheirOptions()
    {
        IAdmin mock = new MockAdminClient(1);
        IAdmin real = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });
        try
        {
            string[] names = { "alpha" };
            foreach (IAdmin admin in new[] { mock, real })
            {
                DescribeTopicsOptions badLimit = new DescribeTopicsOptions { PartitionSizeLimitPerResponse = -1 };
                string? described = Rejection(() => admin.DescribeTopics(names, badLimit));
                Assert.Equal(
                    Rejection(() => admin.DescribeTopics(TopicCollection.OfTopicNames(names), badLimit)), described);
                Assert.NotNull(described);
            }

            DeleteTopicsOptions deleteTimeout = new DeleteTopicsOptions { TimeoutMs = -1 };
            AssertSameTimeout(
                await Failure(() => real.DeleteTopics(TopicCollection.OfTopicNames(names), deleteTimeout).All()),
                await Failure(() => real.DeleteTopics(names, deleteTimeout).All()));

            DescribeTopicsOptions describeTimeout = new DescribeTopicsOptions { TimeoutMs = -1 };
            AssertSameTimeout(
                await Failure(() => real.DescribeTopics(TopicCollection.OfTopicNames(names), describeTimeout).AllTopicNames()!),
                await Failure(() => real.DescribeTopics(names, describeTimeout).AllTopicNames()!));
        }
        finally
        {
            mock.Dispose();
            real.Dispose();
        }
    }

    /// <summary>The failure an awaited call faults with, bounded by the test deadline.</summary>
    private static Task<KafkaException> Failure(Func<Task> call) =>
        TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(call), s_deadline);

    /// <summary>
    /// Both failures are the core's timeout error (REQUEST_TIMED_OUT), and they are the same one.
    /// </summary>
    private static void AssertSameTimeout(KafkaException expected, KafkaException actual)
    {
        Assert.Equal(7, expected.Code);
        Assert.Equal(expected.Code, actual.Code);
        Assert.Equal(expected.Message, actual.Message);
    }

    /// <summary>
    /// M15/P13.5 G1-10: a null name collection is rejected before anything is forwarded, with
    /// the overload's own parameter name — on both clients, through <see cref="IAdmin"/>.
    /// </summary>
    [Fact]
    public void G1_10_ANullNameCollection_IsRejectedWithItsParameterName()
    {
        IAdmin mock = new MockAdminClient(1);
        IAdmin real = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
        try
        {
            foreach (IAdmin admin in new[] { mock, real })
            {
                ArgumentNullException delete = Assert.Throws<ArgumentNullException>(
                    () => admin.DeleteTopics((IReadOnlyCollection<string>)null!));
                Assert.Equal("topicNames", delete.ParamName);
                Assert.StartsWith("Value cannot be null.", delete.Message, StringComparison.Ordinal);

                ArgumentNullException describe = Assert.Throws<ArgumentNullException>(
                    () => admin.DescribeTopics((IReadOnlyCollection<string>)null!, new DescribeTopicsOptions()));
                Assert.Equal("topicNames", describe.ParamName);
                Assert.StartsWith("Value cannot be null.", describe.Message, StringComparison.Ordinal);
            }
        }
        finally
        {
            mock.Dispose();
            real.Dispose();
        }
    }

    /// <summary>
    /// What a call threw, as one comparable string, or <see langword="null"/> if it did not
    /// throw.
    /// </summary>
    private static string? Rejection(Action call)
    {
        try
        {
            call();
            return null;
        }
        catch (Exception thrown)
        {
            return $"{thrown.GetType().FullName}|{(thrown as ArgumentException)?.ParamName}|{thrown.Message}";
        }
    }
}
