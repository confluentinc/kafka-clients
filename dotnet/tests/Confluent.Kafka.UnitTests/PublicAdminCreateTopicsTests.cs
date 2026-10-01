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
/// The per-key bridge as a user sees it: <see cref="IAdmin.CreateTopics"/> returns
/// <b>immediately</b> with one awaitable per topic, and each awaitable carries that
/// topic's own outcome. Driven end to end against <see cref="MockAdminClient"/> — no
/// broker.
/// </summary>
public sealed class PublicAdminCreateTopicsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The C code Kafka assigns to <c>INVALID_REPLICATION_FACTOR</c>.</summary>
    private const int InvalidReplicationFactorCode = 38;

    /// <summary>The C code Kafka assigns to <c>TOPIC_ALREADY_EXISTS</c>.</summary>
    private const int TopicAlreadyExistsCode = 36;

    /// <summary>
    /// <b>The discriminating bridge test.</b> One call, one topic that succeeds and one
    /// that fails: each <see cref="Task"/> must carry <em>its own</em> outcome. An
    /// implementation that faults everything as soon as any key fails — the natural
    /// mistake when one aggregate callback resolves N awaiters — fails here and nowhere
    /// else.
    /// </summary>
    [Fact]
    public async Task MixedOutcome_EachTopicCarriesItsOwnResult()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        // Replication factor 5 against a 1-broker mock is rejected; the other is fine.
        CreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("mixed-ok", 2, 1),
            new NewTopic("mixed-bad", 1, 5),
        });

        // `Values` is Java's `values()` — Map<String, KafkaFuture<Void>>: it says whether
        // the topic was created, and the metadata comes from the typed accessors.
        await TestTimeout.Run(() => result.Values["mixed-ok"], s_deadline);

        int numPartitions = 0;
        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions("mixed-ok"), s_deadline);
        Assert.Equal(2, numPartitions);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["mixed-bad"], s_deadline));
        Assert.Equal(InvalidReplicationFactorCode, failure.Code);
        Assert.Equal("Replication factor: 5 is larger than brokers: 1", failure.Message);
    }

    /// <summary>
    /// Awaiting one topic works without awaiting the others — including when one of the
    /// others has failed. That is the whole point of per-key granularity, and it is what
    /// collapsing the result into a single aggregate awaitable would destroy.
    /// </summary>
    [Fact]
    public async Task OneTopicCanBeAwaited_WithoutTouchingTheOthers()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("solo-ok", 1, 1),
            new NewTopic("solo-bad", 1, 9),
        });

        await TestTimeout.Run(() => result.Values["solo-ok"], s_deadline);

        // The failed sibling is deliberately left unawaited above; observe it now so the
        // test leaves no unobserved fault behind. AWAIT it rather than read `.Exception`:
        // the per-key tasks are completed one callback at a time and the await above
        // resumes asynchronously (RunContinuationsAsynchronously), so the sibling can
        // still be pending when it does — reading `.Exception` there raced (it failed
        // about 1 run in 8 on net8.0, at 61097faa too).
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.Values["solo-bad"], s_deadline));
    }

    /// <summary>
    /// <c>All()</c> mirrors Java's <c>all()</c>: it succeeds only when every topic
    /// succeeded, and faults with the first failure otherwise.
    /// </summary>
    [Fact]
    public async Task All_SucceedsWhenEveryTopicSucceeds_AndFaultsOtherwise()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult ok = admin.CreateTopics(new[]
        {
            new NewTopic("all-a", 1, 1),
            new NewTopic("all-b", 1, 1),
        });
        await TestTimeout.Run(ok.All, s_deadline);

        CreateTopicsResult mixed = admin.CreateTopics(new[]
        {
            new NewTopic("all-c", 1, 1),
            new NewTopic("all-d", 1, 4),
        });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(mixed.All, s_deadline));
        Assert.Equal(InvalidReplicationFactorCode, failure.Code);

        // The successful sibling is still successful — All faulting does not fault it.
        await TestTimeout.Run(() => mixed.Values["all-c"], s_deadline);
    }

    /// <summary>
    /// The <c>thenApply</c>-style accessors — Java's <c>config</c> / <c>topicId</c> /
    /// <c>numPartitions</c> / <c>replicationFactor</c> — each derive from that topic's own
    /// awaitable, including the non-ASCII config value round-trip that guards against
    /// <c>LPStr</c> marshalling (ffi §B3).
    /// </summary>
    [Fact]
    public async Task DerivedAccessors_ProjectOneTopicsOwnResult()
    {
        const string Topic = "derived-topic";
        const string ConfigValue = "délété-🎈";

        using MockAdminClient admin = new MockAdminClient(3);

        CreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic(Topic, 4, 2)
            {
                Configs = new Dictionary<string, string>
                {
                    ["cleanup.policy"] = "compact",
                    ["custom.marker"] = ConfigValue,
                },
            },
        });

        int numPartitions = 0;
        int replicationFactor = 0;
        Uuid topicId = Uuid.Zero;
        Config config = default!;

        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions(Topic), s_deadline);
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor(Topic), s_deadline);
        await TestTimeout.Run(async () => topicId = await result.TopicId(Topic), s_deadline);
        await TestTimeout.Run(async () => config = await result.Config(Topic), s_deadline);

        Assert.Equal(4, numPartitions);
        Assert.Equal(2, replicationFactor);
        Assert.NotEqual(Uuid.Zero, topicId);

        // The id round-trips through the ABI's base64 text form.
        Assert.Equal(topicId, Uuid.Parse(topicId.ToString()));

        Assert.Equal("compact", config.Get("cleanup.policy")!.Value);
        Assert.Equal(ConfigValue, config.Get("custom.marker")!.Value);
        Assert.Null(config.Get("not.set.at.all"));
    }

    /// <summary>
    /// A derived accessor for a topic that was not part of the request reports the
    /// mistake as an argument error, naming the parameter — a precondition, not a Kafka
    /// outcome (ffi §B5). It is raised <b>synchronously</b>, at the call site: a usage
    /// error is not an operation outcome, so it must not be deferred into a
    /// <see cref="Task"/> the caller might await much later, or never.
    /// </summary>
    [Fact]
    public async Task DerivedAccessor_ForAnUnrequestedTopic_ThrowsArgumentExceptionSynchronously()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("known", 1, 1) });
        await TestTimeout.Run(result.All, s_deadline);

        // Assert.Throws (not ThrowsAsync): the exception must escape the call itself,
        // before any Task is handed back.
        ArgumentException failure =
            Assert.Throws<ArgumentException>(() => { _ = result.Config("unknown"); });
        Assert.Equal("topic", failure.ParamName);
        Assert.StartsWith(
            "Topic 'unknown' was not part of this createTopics request.",
            failure.Message,
            StringComparison.Ordinal);

        ArgumentNullException nullTopic =
            Assert.Throws<ArgumentNullException>(() => { _ = result.TopicId(null!); });
        Assert.Equal("topic", nullTopic.ParamName);
    }

    /// <summary>
    /// A topic that already exists fails with that topic's error, message and code — the
    /// contract that a per-topic failure is not a call failure.
    /// </summary>
    [Fact]
    public async Task ExistingTopic_FailsOnlyThatTopic()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult first = admin.CreateTopics(new[] { new NewTopic("dup", 1, 1) });
        await TestTimeout.Run(first.All, s_deadline);

        CreateTopicsResult second = admin.CreateTopics(new[]
        {
            new NewTopic("dup", 1, 1),
            new NewTopic("fresh", 1, 1),
        });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => second.Values["dup"], s_deadline));
        Assert.Equal(TopicAlreadyExistsCode, failure.Code);
        Assert.Equal("Topic dup exists already.", failure.Message);

        await TestTimeout.Run(() => second.Values["fresh"], s_deadline);
    }

    /// <summary>
    /// A repeated topic name yields <b>one</b> entry, as Java's map-keyed result does
    /// (<c>KafkaAdminClient.createTopics</c> populates its future map only for a name it
    /// has not seen).
    /// </summary>
    [Fact]
    public async Task RepeatedTopicName_YieldsOneEntry()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("repeated", 1, 1),
            new NewTopic("repeated", 7, 1),
        });

        Assert.Single(result.Values);
        await TestTimeout.Run(result.All, s_deadline);

        // The FIRST occurrence is the one that was sent, as in Java.
        int numPartitions = 0;
        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions("repeated"), s_deadline);
        Assert.Equal(1, numPartitions);
    }

    /// <summary>
    /// An empty request is legal: an empty result whose <c>All()</c> completes at once.
    /// </summary>
    [Fact]
    public async Task EmptyRequest_YieldsAnEmptyResultThatCompletes()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult result = admin.CreateTopics(Array.Empty<NewTopic>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// The replica-assignment form travels end to end. The two <see cref="NewTopic"/>
    /// forms are mutually exclusive <b>by construction</b> — this constructor leaves the
    /// partition count and replication factor unset, and there is no way to set them
    /// afterwards — which is what keeps the ABI's "assignments present ⇒ those two are
    /// not sent" switch from being something a caller can contradict.
    /// </summary>
    [Fact]
    public async Task ReplicaAssignmentForm_IsMutuallyExclusiveAndRoundTrips()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        NewTopic assigned = new NewTopic(
            "assigned-topic",
            new Dictionary<int, IReadOnlyList<int>> { [0] = new[] { 0 } });

        Assert.Null(assigned.NumPartitions);
        Assert.Null(assigned.ReplicationFactor);
        Assert.NotNull(assigned.ReplicasAssignments);

        NewTopic counted = new NewTopic("counted-topic", 1, 1);
        Assert.Null(counted.ReplicasAssignments);

        CreateTopicsResult result = admin.CreateTopics(new[] { assigned });
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// Java's option defaults, mirrored exactly — including
    /// <see cref="CreateTopicsOptions.RetryOnQuotaViolation"/>, the one whose default is
    /// <see langword="true"/> rather than the C# default for its type. A
    /// <see langword="null"/> options argument must behave identically to a fresh
    /// instance.
    /// </summary>
    [Fact]
    public void OptionDefaults_MirrorJava()
    {
        CreateTopicsOptions options = new CreateTopicsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.ValidateOnly);
        Assert.True(options.RetryOnQuotaViolation);
    }

    /// <summary>
    /// Both <c>bool</c> parameters travel across the ABI correctly for every combination.
    /// They sit next to each other and immediately before the callback pointer, so a
    /// missing <c>MarshalAs(UnmanagedType.I1)</c> — which would widen a one-byte C
    /// <c>bool</c> to a four-byte Win32 <c>BOOL</c> — is the classic way to corrupt the
    /// argument that follows.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task BothBoolOptions_CrossTheAbiForEveryCombination(bool validateOnly, bool retryOnQuotaViolation)
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic($"bools-{validateOnly}-{retryOnQuotaViolation}", 1, 1) },
            new CreateTopicsOptions
            {
                ValidateOnly = validateOnly,
                RetryOnQuotaViolation = retryOnQuotaViolation,
            });

        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// An explicit timeout crosses the ABI and the call still completes. (The exact
    /// millisecond value that reaches the P/Invoke — including a <see langword="null"/>
    /// mapping to <b>negative</b>, not zero — is pinned deterministically in
    /// <c>AdminSubmitArgumentTests</c>, which can read the argument itself.)
    /// </summary>
    [Fact]
    public async Task ExplicitTimeout_CrossesTheAbi()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic("timeout-topic", 1, 1) },
            new CreateTopicsOptions { TimeoutMs = 5_000 });

        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// M15/P13.3 D13 — a <c>createTopics</c> entry is now read through
    /// <c>kafka_admin_TopicMetadataAndConfig_config</c> with the <c>describeConfigs</c>
    /// reader, so it is a full <see cref="ConfigEntry"/>. Against the mock that entry is
    /// exactly Java's mock's: <c>MockAdminClient.config(NewTopic)</c> builds
    /// <c>new ConfigEntry(key, value)</c> (<c>MockAdminClient.java:426-434</c>), whose source,
    /// type, documentation, synonyms and flags are all the two-argument constructor's
    /// defaults — which the header states for a <c>createTopics</c> entry (no synonyms, a null
    /// type and documentation; the null type reads as <see cref="ConfigEntry.ConfigType.Unknown"/>).
    /// </summary>
    /// <remarks>
    /// Not discriminating between the old flat reader and this one: against the mock both
    /// produce <see cref="ConfigEntry.ConfigSource.Unknown"/>. The difference — the core's
    /// real source instead of one guessed from <c>is_default</c> — shows only against a
    /// broker. The removal itself is pinned structurally in
    /// <c>CommonNativeMethodsMarshallingTests</c>.
    /// </remarks>
    [Fact]
    public async Task ConfigEntries_AreJavasMockEntries_ReadThroughTheConfigGetter()
    {
        const string Topic = "d13-config-entries";

        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult created = admin.CreateTopics(new[]
        {
            new NewTopic(Topic, 1, 1)
            {
                Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            },
        });

        Config config = default!;
        await TestTimeout.Run(async () => config = await created.Config(Topic), s_deadline);

        ConfigEntry entry = Assert.Single(config.Entries);
        Assert.Equal(new ConfigEntry("cleanup.policy", "compact"), entry);
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, entry.Source);
        Assert.Equal(ConfigEntry.ConfigType.Unknown, entry.Type);
        Assert.Null(entry.Documentation);
        Assert.Empty(entry.Synonyms);
        Assert.False(entry.IsDefault);
        Assert.False(entry.IsSensitive);
        Assert.False(entry.IsReadOnly);
    }

    /// <summary>
    /// M15/P13.3 D16 — Java's <c>NewTopic.configs(Map)</c> accepts a null value and sends it,
    /// and the ABI keeps a NULL <c>value</c> as that null (header,
    /// <c>kafka_admin_NewTopic_put_config</c>). So the topic is created, and both the
    /// <c>createTopics</c> config result and <c>describeConfigs</c> report the entry with a
    /// <see langword="null"/> value — not the empty string, and not a missing entry.
    /// </summary>
    [Fact]
    public async Task NullConfigValue_IsSentAsJavasNull()
    {
        const string Topic = "d16-null-config";
        const string Name = "retention.ms";

        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult created = admin.CreateTopics(new[]
        {
            new NewTopic(Topic, 1, 1)
            {
                Configs = new Dictionary<string, string> { [Name] = null!, ["cleanup.policy"] = "compact" },
            },
        });

        await TestTimeout.Run(created.All, s_deadline);

        Config fromCreate = default!;
        await TestTimeout.Run(async () => fromCreate = await created.Config(Topic), s_deadline);
        ConfigEntry? createEntry = fromCreate.Get(Name);
        Assert.NotNull(createEntry);
        Assert.Null(createEntry!.Value);
        Assert.Equal("compact", fromCreate.Get("cleanup.policy")!.Value);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        DescribeConfigsResult described = admin.DescribeConfigs(new[] { resource });
        Config fromDescribe = default!;
        await TestTimeout.Run(async () => fromDescribe = await described.Values[resource], s_deadline);
        ConfigEntry? describeEntry = fromDescribe.Get(Name);
        Assert.NotNull(describeEntry);
        Assert.Null(describeEntry!.Value);
        Assert.Equal("compact", fromDescribe.Get("cleanup.policy")!.Value);
    }

    /// <summary>
    /// Every precondition fires <b>before</b> any native call, with the .NET exception
    /// the mistake deserves rather than a <see cref="KafkaException"/> (ffi §B5), and
    /// names its parameter.
    /// </summary>
    [Fact]
    public void Preconditions_FireBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        ArgumentNullException nullTopics =
            Assert.Throws<ArgumentNullException>(() => admin.CreateTopics(null!));
        Assert.Equal("newTopics", nullTopics.ParamName);

        ArgumentException nullElement =
            Assert.Throws<ArgumentException>(() => admin.CreateTopics(new NewTopic[] { null! }));
        Assert.Equal("newTopics", nullElement.ParamName);
        Assert.StartsWith(
            "The topics to create must not contain a null element.",
            nullElement.Message,
            StringComparison.Ordinal);

        // A null configuration VALUE is no longer a precondition failure: it is Java's null,
        // and it is sent (M15/P13.3 D16 — see NullConfigValue_IsSentAsJavasNull).

        // A negative timeout would be silently reinterpreted by the ABI as "unset", so it
        // is rejected rather than forwarded.
        ArgumentOutOfRangeException negativeTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.CreateTopics(
                new[] { new NewTopic("neg", 1, 1) },
                new CreateTopicsOptions { TimeoutMs = -1 }));
        Assert.Equal("options", negativeTimeout.ParamName);
        Assert.StartsWith(
            "CreateTopicsOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            negativeTimeout.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="NewTopic"/>'s own preconditions, likewise before anything native.
    /// <c>-1</c> is Java-legal for both counts — it is <c>NO_NUM_PARTITIONS</c> /
    /// <c>NO_REPLICATION_FACTOR</c> (<c>CreateTopicsRequest.java:82-83</c>), what Java's
    /// <c>NewTopic</c> sends for an empty <c>Optional</c> — so it is accepted by both
    /// constructors and <b>stored as <c>-1</c></b>, not folded to <see langword="null"/>
    /// (Java keeps <c>Optional.of(-1)</c> apart from <c>Optional.empty()</c>). Below
    /// <c>-1</c> stays rejected (M15/P13.2 D7): the ABI reads <em>every</em> negative as
    /// "unset", so <c>-2</c> would silently create with the broker defaults where Java's
    /// broker rejects it.
    /// </summary>
    [Fact]
    public void NewTopicPreconditions_AcceptMinusOne_AndRejectWhatTheAbiWouldReinterpret()
    {
        Assert.Equal("name", Assert.Throws<ArgumentNullException>(() => new NewTopic(null!, 1, 1)).ParamName);

        // -1 through both count constructors, and stored as -1.
        NewTopic viaNonNullable = new NewTopic("t", -1, (short)-1);
        Assert.Equal(-1, viaNonNullable.NumPartitions);
        Assert.Equal((short)-1, viaNonNullable.ReplicationFactor);

        NewTopic viaNullable = new NewTopic("t", (int?)-1, (short?)-1);
        Assert.Equal(-1, viaNullable.NumPartitions);
        Assert.Equal((short)-1, viaNullable.ReplicationFactor);

        // Each independently of the other.
        Assert.Equal(-1, new NewTopic("t", (int?)-1, (short?)3).NumPartitions);
        Assert.Equal((short)-1, new NewTopic("t", (int?)3, (short?)-1).ReplicationFactor);

        // And null stays null — the two spellings of "broker default" are distinct values.
        NewTopic unset = new NewTopic("t", (int?)null, (short?)null);
        Assert.Null(unset.NumPartitions);
        Assert.Null(unset.ReplicationFactor);

        foreach (Func<NewTopic> belowMinusOne in new Func<NewTopic>[]
        {
            () => new NewTopic("t", (int?)-2, (short?)1),
            () => new NewTopic("t", -2, (short)1),
            () => new NewTopic("t", (int?)int.MinValue, (short?)1),
        })
        {
            ArgumentOutOfRangeException partitions = Assert.Throws<ArgumentOutOfRangeException>(belowMinusOne);
            Assert.Equal("numPartitions", partitions.ParamName);
            Assert.StartsWith(
                "Number of partitions must be non-negative, or -1 (or null) for the broker default.",
                partitions.Message,
                StringComparison.Ordinal);
        }

        Assert.Equal(-2, Assert.Throws<ArgumentOutOfRangeException>(() => new NewTopic("t", (int?)-2, (short?)1)).ActualValue);

        foreach (Func<NewTopic> belowMinusOne in new Func<NewTopic>[]
        {
            () => new NewTopic("t", (int?)1, (short?)-2),
            () => new NewTopic("t", 1, (short)-2),
            () => new NewTopic("t", (int?)1, (short?)short.MinValue),
        })
        {
            ArgumentOutOfRangeException factor = Assert.Throws<ArgumentOutOfRangeException>(belowMinusOne);
            Assert.Equal("replicationFactor", factor.ParamName);
            Assert.StartsWith(
                "Replication factor must be non-negative, or -1 (or null) for the broker default.",
                factor.Message,
                StringComparison.Ordinal);
        }

        Assert.Equal(
            (short)-2,
            Assert.Throws<ArgumentOutOfRangeException>(() => new NewTopic("t", (int?)1, (short?)-2)).ActualValue);

        Assert.Equal(
            "replicasAssignments",
            Assert.Throws<ArgumentNullException>(
                () => new NewTopic("t", (IReadOnlyDictionary<int, IReadOnlyList<int>>)null!)).ParamName);

        Assert.Equal(
            "replicasAssignments",
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new NewTopic("t", new Dictionary<int, IReadOnlyList<int>> { [-1] = new[] { 0 } })).ParamName);

        Assert.Equal(
            "replicasAssignments",
            Assert.Throws<ArgumentNullException>(
                () => new NewTopic("t", new Dictionary<int, IReadOnlyList<int>> { [0] = null! })).ParamName);
    }

    /// <summary>
    /// <c>-1</c> reaches the core as Java's wire value and means "the broker default" —
    /// driven end to end: the mock resolves it to its own defaults, exactly as it does an
    /// unset count (<c>mock_admin_client.rs</c> <c>default_partitions</c> = 1,
    /// <c>default_replication_factor</c> = <c>min(brokers, 3)</c> = 2 here). The ABI has no
    /// <c>NewTopic</c> getter and <c>NewTopicMarshal</c> calls the P/Invoke directly, so
    /// the argument is observed through its effect rather than captured.
    /// </summary>
    [Fact]
    public async Task MinusOneCounts_CreateWithTheBrokerDefaults_LikeUnsetOnes()
    {
        using MockAdminClient admin = new MockAdminClient(2);

        CreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("minus-one", -1, (short)-1),
            new NewTopic("unset", (int?)null, (short?)null),
            new NewTopic("explicit", 3, 1),
        });

        int[] partitions = new int[3];
        int[] factors = new int[3];
        string[] topics = { "minus-one", "unset", "explicit" };
        for (int i = 0; i < topics.Length; i++)
        {
            int index = i;
            await TestTimeout.Run(async () => partitions[index] = await result.NumPartitions(topics[index]), s_deadline);
            await TestTimeout.Run(async () => factors[index] = await result.ReplicationFactor(topics[index]), s_deadline);
        }

        Assert.Equal(new[] { 1, 1, 3 }, partitions);
        Assert.Equal(new[] { 2, 2, 1 }, factors);
    }

    /// <summary>
    /// <c>MockAdminClient_new</c> returns <b>null</b> for fewer than one broker — Java's
    /// <c>MockAdminClient.Builder.build()</c> throw in the FFI's idiom. The binding must
    /// reject it before the call rather than dereference the null.
    /// </summary>
    [Fact]
    public void MockAdminClient_RejectsFewerThanOneBroker()
    {
        ArgumentOutOfRangeException failure =
            Assert.Throws<ArgumentOutOfRangeException>(() => new MockAdminClient(0));
        Assert.Equal("numBrokers", failure.ParamName);
        Assert.StartsWith(
            "A mock admin client requires at least one broker.",
            failure.Message,
            StringComparison.Ordinal);

        Assert.Throws<ArgumentOutOfRangeException>(() => new MockAdminClient(-5));
    }
}
