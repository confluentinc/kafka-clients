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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IAdmin.CreateTopics"/> end to end against the mocks: one future per topic carrying that topic's own
/// outcome, the derived accessors and their cancellation, the options, and the C options and topic the call builds.
/// </summary>
public sealed class PublicAdminCreateTopicsTests
{
    private const BindingFlags Private = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    private const int InvalidReplicationFactorCode = 38;

    private const int TopicAlreadyExistsCode = 36;

    private const string CreateTopicsThreadExited = "The AdminClient thread has exited. Call: createTopics";

    [Fact]
    public async Task MixedOutcome_EachTopicCarriesItsOwnResult()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        // Replication factor 5 against a 1-broker mock is rejected; the other is fine.
        AsyncCreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("mixed-ok", 2, 1),
            new NewTopic("mixed-bad", 1, 5),
        });

        await TestTimeout.Run(() => result.Values["mixed-ok"].Get(), s_deadline);

        int numPartitions = 0;
        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions("mixed-ok").Get(), s_deadline);
        Assert.Equal(2, numPartitions);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["mixed-bad"].Get(), s_deadline));
        Assert.Equal(InvalidReplicationFactorCode, failure.Code);
        Assert.Equal("Replication factor: 5 is larger than brokers: 1", failure.Message);
    }

    [Fact]
    public async Task OneTopicCanBeAwaited_WithoutTouchingTheOthers()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("solo-ok", 1, 1),
            new NewTopic("solo-bad", 1, 9),
        });

        await TestTimeout.Run(() => result.Values["solo-ok"].Get(), s_deadline);

        // Await the failed sibling rather than read its Exception: the futures complete one at a time, so it can still be
        // pending here.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.Values["solo-bad"].Get(), s_deadline));
    }

    [Fact]
    public async Task All_SucceedsWhenEveryTopicSucceeds_AndFaultsOtherwise()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult ok = admin.CreateTopics(new[]
        {
            new NewTopic("all-a", 1, 1),
            new NewTopic("all-b", 1, 1),
        });
        await TestTimeout.Run(() => ok.All().Get(), s_deadline);

        AsyncCreateTopicsResult mixed = admin.CreateTopics(new[]
        {
            new NewTopic("all-c", 1, 1),
            new NewTopic("all-d", 1, 4),
        });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.All().Get(), s_deadline));
        Assert.Equal(InvalidReplicationFactorCode, failure.Code);

        await TestTimeout.Run(() => mixed.Values["all-c"].Get(), s_deadline);
    }

    [Fact]
    public async Task DerivedAccessors_ProjectOneTopicsOwnResult()
    {
        const string Topic = "derived-topic";
        const string ConfigValue = "délété-🎈";

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[]
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

        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions(Topic).Get(), s_deadline);
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor(Topic).Get(), s_deadline);
        await TestTimeout.Run(async () => topicId = await result.TopicId(Topic).Get(), s_deadline);
        await TestTimeout.Run(async () => config = await result.Config(Topic).Get(), s_deadline);

        Assert.Equal(4, numPartitions);
        Assert.Equal(2, replicationFactor);
        Assert.NotEqual(Uuid.Zero, topicId);

        Assert.Equal(topicId, Uuid.Parse(topicId.ToString()));

        Assert.Equal("compact", config.Get("cleanup.policy")!.Value);
        Assert.Equal(ConfigValue, config.Get("custom.marker")!.Value);
        Assert.Null(config.Get("not.set.at.all"));
    }

    [Fact]
    public async Task DerivedAccessor_ForAnUnrequestedTopic_FailsItsFuture_WithTheCoresArgumentError()
    {
        const string Unrequested = "Topic unknown was not part of the createTopics request";

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("known", 1, 1) });
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        Func<Task>[] gets =
        {
            () => result.Config("unknown").Get(),
            () => result.TopicId("unknown").Get(),
            () => result.NumPartitions("unknown").Get(),
            () => result.ReplicationFactor("unknown").Get(),
        };
        foreach (Func<Task> get in gets)
        {
            ArgumentException failure =
                await Assert.ThrowsAsync<ArgumentException>(() => TestTimeout.Run(get, s_deadline));
            Assert.Equal(Unrequested, failure.Message);
        }

        MockAdminClient syncAdmin = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(syncAdmin);
        CreateTopicsResult syncResult = syncAdmin.CreateTopics(new[] { new NewTopic("known", 1, 1) });
        KafkaFuture<int> unknown = syncResult.NumPartitions("unknown");
        Assert.Equal(Unrequested, Assert.Throws<ArgumentException>(() => unknown.Get()).Message);
        Assert.True(unknown.IsCompletedExceptionally);

        ArgumentNullException nullTopic =
            Assert.Throws<ArgumentNullException>(() => { _ = result.TopicId(null!); });
        Assert.Equal("topic", nullTopic.ParamName);
    }

    [Fact]
    public void CancellingAValuesFuture_LeavesTheAccessorsAllAndAFreshValuesFuturePending()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
        try
        {
            CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("linked", 1, 1) });
            KafkaFuture<int> numPartitions = result.NumPartitions("linked");
            KafkaFuture<Config> config = result.Config("linked");
            KafkaFuture allBefore = result.All();
            KafkaFuture cancelled = result.Values["linked"];

            Assert.True(cancelled.Cancel(true));

            Assert.True(cancelled.IsCancelled);
            KafkaFuture allAfter = result.All();
            KafkaFuture fresh = result.Values["linked"];
            Assert.False(numPartitions.IsDone);
            Assert.False(config.IsDone);
            Assert.False(allBefore.IsDone);
            Assert.False(allAfter.IsDone);
            Assert.False(fresh.IsDone);

            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

            AssertCreateTopicsThreadExited(BlockingCall.RunGet(() => numPartitions.Get()));
            AssertCreateTopicsThreadExited(BlockingCall.RunGet(() => config.Get()));
            AssertCreateTopicsThreadExited(BlockingCall.RunWait(() => allBefore.Get()));
            AssertCreateTopicsThreadExited(BlockingCall.RunWait(() => allAfter.Get()));
            AssertCreateTopicsThreadExited(BlockingCall.RunWait(() => fresh.Get()));
            Assert.IsType<OperationCanceledException>(BlockingCall.RunWait(() => cancelled.Get()));
        }
        finally
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public async Task CancellingAnAsyncValuesFuture_LeavesTheAccessorsAllAndAFreshValuesFuturePending()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);
        try
        {
            AsyncCreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("linked", 1, 1) });
            AsyncKafkaFuture<int> numPartitions = result.NumPartitions("linked");
            AsyncKafkaFuture<Config> config = result.Config("linked");
            AsyncKafkaFuture allBefore = result.All();
            AsyncKafkaFuture cancelled = result.Values["linked"];

            Assert.True(cancelled.Cancel(true));

            Assert.True(cancelled.IsCancelled);
            AsyncKafkaFuture allAfter = result.All();
            AsyncKafkaFuture fresh = result.Values["linked"];
            Assert.False(numPartitions.IsDone);
            Assert.False(config.IsDone);
            Assert.False(allBefore.IsDone);
            Assert.False(allAfter.IsDone);
            Assert.False(fresh.IsDone);

            await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), BlockingCall.Deadline);

            AssertCreateTopicsThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => numPartitions.Get(), BlockingCall.Deadline)));
            AssertCreateTopicsThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => config.Get(), BlockingCall.Deadline)));
            AssertCreateTopicsThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => allBefore.Get(), BlockingCall.Deadline)));
            AssertCreateTopicsThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => allAfter.Get(), BlockingCall.Deadline)));
            AssertCreateTopicsThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => fresh.Get(), BlockingCall.Deadline)));
            Assert.IsAssignableFrom<OperationCanceledException>(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => cancelled.Get(), BlockingCall.Deadline)));
        }
        finally
        {
            await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), BlockingCall.Deadline);
            await TestTimeout.DisposeAsync(admin);
        }
    }

    [Fact]
    public async Task Values_IsAFreshDependantPerAccess_SoOneReadAfterACancelIsNotCancelled()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
        AsyncKafkaAdminClient asyncAdmin = new AsyncKafkaAdminClient(s_unreachable);
        try
        {
            CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("t", 1, 1) });
            AsyncCreateTopicsResult asyncResult = asyncAdmin.CreateTopics(new[] { new NewTopic("t", 1, 1) });
            KafkaFuture first = result.Values["t"];
            AsyncKafkaFuture asyncFirst = asyncResult.Values["t"];
            Assert.True(first.Cancel(true));
            Assert.True(asyncFirst.Cancel(true));

            KafkaFuture fresh = result.Values["t"];
            AsyncKafkaFuture asyncFresh = asyncResult.Values["t"];

            Assert.NotEqual(first, fresh);
            Assert.NotEqual(asyncFirst, asyncFresh);
            Assert.False(fresh.IsCancelled);
            Assert.False(asyncFresh.IsCancelled);
            Assert.False(fresh.IsDone);
            Assert.False(asyncFresh.IsDone);
        }
        finally
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));
            TestTimeout.Dispose(admin);
            await TestTimeout.Run(() => asyncAdmin.Close(TimeSpan.Zero), BlockingCall.Deadline);
            await TestTimeout.DisposeAsync(asyncAdmin);
        }
    }

    [Fact]
    public void Refinement_OfACancelledTopicFuture_IsCancelledAsADependant()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
        try
        {
            CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("linked", 1, 1) });
            KafkaFutureImpl<object?> key = new KafkaFutureImpl<object?>();
            CreateTopicsRefinements refinements = new CreateTopicsRefinements(
                result.Native!,
                new Dictionary<string, KafkaFutureImpl<object?>>(StringComparer.Ordinal) { ["linked"] = key });
            KafkaFutureImpl<int> numPartitions = refinements.NumPartitions("linked");
            KafkaFutureImpl<object?> dependant = key.ThenApply(static _ => (object?)null);
            Assert.False(numPartitions.IsDone);

            Assert.True(key.Cancel(true));

            Assert.True(dependant.IsCancelled);
            Assert.False(dependant.Cancel(true));
            Assert.True(numPartitions.IsCancelled);
            Assert.False(numPartitions.Cancel(true));
            Assert.Throws<OperationCanceledException>(() => numPartitions.Get());
        }
        finally
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public async Task ExistingTopic_FailsOnlyThatTopic()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult first = admin.CreateTopics(new[] { new NewTopic("dup", 1, 1) });
        await TestTimeout.Run(() => first.All().Get(), s_deadline);

        AsyncCreateTopicsResult second = admin.CreateTopics(new[]
        {
            new NewTopic("dup", 1, 1),
            new NewTopic("fresh", 1, 1),
        });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => second.Values["dup"].Get(), s_deadline));
        Assert.Equal(TopicAlreadyExistsCode, failure.Code);
        Assert.Equal("Topic dup exists already.", failure.Message);

        await TestTimeout.Run(() => second.Values["fresh"].Get(), s_deadline);
    }

    [Fact]
    public async Task NonAsciiTopicName_RoundTripsThroughTheKeyAndTheErrorMessage()
    {
        const string Topic = "témas-日本語-🎉";

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);
        AsyncCreateTopicsResult second = admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) });

        Assert.Equal(new[] { Topic }, second.Values.Keys);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => second.Values[Topic].Get(), s_deadline));
        Assert.Equal(TopicAlreadyExistsCode, failure.Code);
        Assert.Equal($"Topic {Topic} exists already.", failure.Message);
    }

    [Fact]
    public async Task RepeatedTopicName_YieldsOneEntry_ThatJavasMockFailsAsExisting()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[]
        {
            new NewTopic("repeated", 1, 1),
            new NewTopic("repeated", 7, 1),
        });

        Assert.Single(result.Values);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["repeated"].Get(), s_deadline));
        Assert.Equal(TopicAlreadyExistsCode, failure.Code);
        Assert.Equal("Topic repeated exists already.", failure.Message);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));

        IReadOnlyCollection<string> names = Array.Empty<string>();
        await TestTimeout.Run(async () => names = await admin.ListTopics().Names().Get(), s_deadline);
        Assert.Equal(new[] { "repeated" }, names);
    }

    [Fact]
    public async Task EmptyRequest_YieldsAnEmptyResultThatCompletes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(Array.Empty<NewTopic>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task ReplicaAssignmentForm_IsMutuallyExclusiveAndRoundTrips()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        NewTopic assigned = new NewTopic(
            "assigned-topic",
            new Dictionary<int, IReadOnlyList<int>> { [0] = new[] { 0 } });

        Assert.Null(assigned.NumPartitions);
        Assert.Null(assigned.ReplicationFactor);
        Assert.NotNull(assigned.ReplicasAssignments);

        NewTopic counted = new NewTopic("counted-topic", 1, 1);
        Assert.Null(counted.ReplicasAssignments);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[] { assigned });
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public void OptionDefaults_MirrorJava()
    {
        CreateTopicsOptions options = new CreateTopicsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.ValidateOnly);
        Assert.True(options.RetryOnQuotaViolation);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task BothBoolOptions_CrossTheAbiForEveryCombination(bool validateOnly, bool retryOnQuotaViolation)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic($"bools-{validateOnly}-{retryOnQuotaViolation}", 1, 1) },
            new CreateTopicsOptions
            {
                ValidateOnly = validateOnly,
                RetryOnQuotaViolation = retryOnQuotaViolation,
            });

        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task ExplicitTimeout_CrossesTheAbi()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic("timeout-topic", 1, 1) },
            new CreateTopicsOptions { TimeoutMs = 5_000 });

        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task ConfigEntries_AreJavasMockEntries_ReadThroughTheConfigGetter()
    {
        const string Topic = "d13-config-entries";

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult created = admin.CreateTopics(new[]
        {
            new NewTopic(Topic, 1, 1)
            {
                Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            },
        });

        Config config = default!;
        await TestTimeout.Run(async () => config = await created.Config(Topic).Get(), s_deadline);

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

    [Fact]
    public void NullConfigValue_IsSentAsJavasNull()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        NewTopic topic = new NewTopic("null-config", 1, 1)
        {
            Configs = new Dictionary<string, string> { ["retention.ms"] = null! },
        };

        Assert.EndsWith(", configs={retention.ms=null})", topic.ToString(), StringComparison.Ordinal);

        admin.CreateTopics(new[] { topic }).All().Get();

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, "null-config");
        Config config = admin.DescribeConfigs(new[] { resource }).All().Get()[resource];
        ConfigEntry? entry = config.Get("retention.ms");
        Assert.NotNull(entry);
        Assert.Null(entry!.Value);
    }

    [Fact]
    public void Preconditions_FireBeforeAnyNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

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

        // A negative timeout is not a precondition: it is sent as 0, as Java clamps the deadline.
        Assert.Null(Record.Exception(
            () => admin.CreateTopics(
                new[] { new NewTopic("neg", 1, 1) },
                new CreateTopicsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void NewTopicPreconditions_AcceptMinusOne_AndRejectWhatTheAbiWouldReinterpret()
    {
        Assert.Equal("name", Assert.Throws<ArgumentNullException>(() => new NewTopic(null!, 1, 1)).ParamName);

        NewTopic viaNonNullable = new NewTopic("t", -1, (short)-1);
        Assert.Equal(-1, viaNonNullable.NumPartitions);
        Assert.Equal((short)-1, viaNonNullable.ReplicationFactor);

        NewTopic viaNullable = new NewTopic("t", (int?)-1, (short?)-1);
        Assert.Equal(-1, viaNullable.NumPartitions);
        Assert.Equal((short)-1, viaNullable.ReplicationFactor);

        Assert.Equal(-1, new NewTopic("t", (int?)-1, (short?)3).NumPartitions);
        Assert.Equal((short)-1, new NewTopic("t", (int?)3, (short?)-1).ReplicationFactor);

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

    [Fact]
    public async Task MinusOneCounts_CreateWithTheBrokerDefaults_LikeUnsetOnes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(2).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[]
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
            await TestTimeout.Run(async () => partitions[index] = await result.NumPartitions(topics[index]).Get(), s_deadline);
            await TestTimeout.Run(async () => factors[index] = await result.ReplicationFactor(topics[index]).Get(), s_deadline);
        }

        Assert.Equal(new[] { 1, 1, 3 }, partitions);
        Assert.Equal(new[] { 2, 2, 1 }, factors);
    }

    [Fact]
    public void MockAdminClient_Builder_RejectsNoBrokerAtBuild_AndANegativeCountAtTheSetter()
    {
        MockAdminClient.Builder builder = MockAdminClient.Create().NumBrokers(0);
        ArgumentException noBroker = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Equal("Index 0 out of bounds for length 0", noBroker.Message);

        ArgumentException negative = Assert.Throws<ArgumentException>(() => MockAdminClient.Create().NumBrokers(-5));
        Assert.Equal("fromIndex(0) > toIndex(-5)", negative.Message);
    }

    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack(null));
        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack(new CreateTopicsOptions()));
        Assert.Empty(new CreateTopicsOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal((expected, (sbyte)0, (sbyte)1), ReadBack(new CreateTopicsOptions { TimeoutMs = timeoutMs }));
    }

    [Fact]
    public void EachSetter_ReachesTheCOptions_AndASetterRunTwiceSendsItsLastValue()
    {
        Assert.Equal((-1, (sbyte)1, (sbyte)1), ReadBack(new CreateTopicsOptions { ValidateOnly = true }));
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack(new CreateTopicsOptions { RetryOnQuotaViolation = false }));

        CreateTopicsOptions options = new CreateTopicsOptions { ValidateOnly = true, RetryOnQuotaViolation = false, TimeoutMs = 10 };
        options.ValidateOnly = false;
        options.RetryOnQuotaViolation = true;
        options.TimeoutMs = 20;
        Assert.Equal((20, (sbyte)0, (sbyte)1), ReadBack(options));
    }

    [Fact]
    public void OnlyTheSettersThatRan_ReachTheCOptions_InTheOrderTheyLastRan()
    {
        CreateTopicsOptions options = new CreateTopicsOptions();
        options.RetryOnQuotaViolation = false;
        options.TimeoutMs = 5;
        options.RetryOnQuotaViolation = true;
        Assert.Equal(
            new[] { nameof(CreateTopicsOptions.TimeoutMs), nameof(CreateTopicsOptions.RetryOnQuotaViolation) },
            options.Setters.InOrder);
        Assert.Equal(
            new[] { nameof(CreateTopicsOptions.TimeoutMs), nameof(CreateTopicsOptions.RetryOnQuotaViolation) },
            Built(options));
        Assert.Equal(new[] { nameof(CreateTopicsOptions.ValidateOnly) }, Built(new CreateTopicsOptions { ValidateOnly = false }));
        Assert.Empty(Built(new CreateTopicsOptions()));
    }

    [Fact]
    public void TheNativeTopic_CarriesTheCountsAndTheConfigs()
    {
        NewTopic counted = new NewTopic("counted-é", 3, (short)2)
        {
            Configs = new Dictionary<string, string> { ["segment.ms"] = "2", ["cleanup.policy"] = "compact" },
        };
        WithNativeTopic(counted, native =>
        {
            Assert.Equal("counted-é", Utf8Marshal.PtrToString(Read(native, Native.NewTopicName)));
            Assert.Equal(3, ReadInt(native, Native.NewTopicNumPartitions));
            Assert.Equal(2, ReadShort(native, Native.NewTopicReplicationFactor));
            Assert.Equal(IntPtr.Zero, Read(native, Native.NewTopicReplicasAssignments));
            Assert.Equal(
                new (string?, string?)[] { ("cleanup.policy", "compact"), ("segment.ms", "2") },
                ReadMap<string?, string?>(Read(native, Native.NewTopicConfigs), Utf8Marshal.PtrToString, Utf8Marshal.PtrToString));
        });

        WithNativeTopic(new NewTopic("unset", (int?)null, (short?)null), native =>
        {
            Assert.Equal(-1, ReadInt(native, Native.NewTopicNumPartitions));
            Assert.Equal(-1, ReadShort(native, Native.NewTopicReplicationFactor));
            Assert.Equal(IntPtr.Zero, Read(native, Native.NewTopicConfigs));
        });
    }

    [Fact]
    public void TheNativeTopic_CarriesTheReplicaAssignments()
    {
        NewTopic assigned = new NewTopic(
            "assigned",
            new Dictionary<int, IReadOnlyList<int>> { [2] = Array.Empty<int>(), [0] = new[] { 4 }, [1] = new[] { 5, 6 } });
        WithNativeTopic(assigned, native =>
        {
            Assert.Equal(-1, ReadInt(native, Native.NewTopicNumPartitions));
            Assert.Equal(-1, ReadShort(native, Native.NewTopicReplicationFactor));
            (int Partition, int[] Brokers)[] assignments = ReadMap(
                Read(native, Native.NewTopicReplicasAssignments),
                static key => Marshal.ReadInt32(key),
                static list => ReadInt32List(list));
            Assert.Equal(new[] { 0, 1, 2 }, assignments.Select(a => a.Partition));
            Assert.Equal(new[] { 4 }, assignments[0].Brokers);
            Assert.Equal(new[] { 5, 6 }, assignments[1].Brokers);
            Assert.Empty(assignments[2].Brokers);
        });
    }

    [Fact]
    public void AMapFuture_HoldsItsMap_UntilItsDelivery()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        SafeKafkaFutureHandle first;
        SafeKafkaFutureHandle second;
        SafeHandle map;
        using (new DrainGate(CompletionOf(admin)))
        {
            CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("one", 1, 1), new NewTopic("two", 1, 1) });
            first = HandleOf(KeyOf(result, "one"));
            second = HandleOf(KeyOf(result, "two"));
            map = OwnerOf(first);
            Assert.Same(map, OwnerOf(second));
            Assert.False(map.IsClosed);
            Assert.False(first.IsClosed);
        }

        FlushUntil(CompletionOf(admin), () => first.IsClosed && second.IsClosed && map.IsClosed);
        Assert.True(first.IsClosed);
        Assert.True(second.IsClosed);
        Assert.True(map.IsClosed);
    }

    private static void AssertCreateTopicsThreadExited(object? thrown) =>
        BlockingCall.AssertThrew(thrown, 7, CreateTopicsThreadExited);

    private static (int TimeoutMs, sbyte ValidateOnly, sbyte RetryOnQuotaViolation) ReadBack(CreateTopicsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewCreateTopicsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.CreateTopicsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte validateOnly = Native.CreateTopicsOptionsShouldValidateOnly(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte retry = Native.CreateTopicsOptionsShouldRetryOnQuotaViolation(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, validateOnly, retry);
        }
        finally
        {
            NativeAdminClient.FreeCreateTopicsOptions(native);
        }
    }

    private static List<string> Built(CreateTopicsOptions options)
    {
        List<string> ran = new List<string>();
        IntPtr native = AdminOptionsMarshal.Build(
            NativeMethods.CreateTopicsOptionsNew,
            options,
            options.Setters,
            (IntPtr n, CreateTopicsOptions o, string setter, ref IntPtr outErr) => ran.Add(setter),
            NativeMethods.CreateTopicsOptionsDestroy);
        NativeAdminClient.FreeCreateTopicsOptions(native);
        return ran;
    }

    private static void WithNativeTopic(NewTopic topic, Action<IntPtr> read)
    {
        IntPtr native = NativeAdminClient.NewNativeTopic(topic);
        try
        {
            read(native);
        }
        finally
        {
            NativeAdminClient.DestroyNewTopic(native);
        }
    }

    private static IntPtr Read(IntPtr native, Native.Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr value = getter(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static int ReadInt(IntPtr native, Native.Int32Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        int value = getter(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static short ReadShort(IntPtr native, Native.Int16Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        short value = getter(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static (TKey, TValue)[] ReadMap<TKey, TValue>(IntPtr map, Func<IntPtr, TKey> key, Func<IntPtr, TValue> value)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int count = NativeMethods.MapSize(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            (TKey, TValue)[] entries = new (TKey, TValue)[count];
            for (int i = 0; i < count; i++)
            {
                IntPtr k = NativeMethods.MapKey(map, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr v = NativeMethods.MapValue(map, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                entries[i] = (key(k), value(v));
            }

            return entries;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MapDestroy(map, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    private static int[] ReadInt32List(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int[] values = new int[count];
        for (int i = 0; i < count; i++)
        {
            IntPtr element = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            values[i] = Marshal.ReadInt32(element);
        }

        return values;
    }

    private static CompletionThread CompletionOf(object admin)
    {
        object native = admin.GetType().GetField("_native", Private)!.GetValue(admin)!;
        return (CompletionThread)typeof(NativeAdminClient).GetField("_completion", Private)!.GetValue(native)!;
    }

    private static KafkaFutureImpl<object?> KeyOf(CreateTopicsResult result, string topic) =>
        ((CreateTopicsRefinements)typeof(CreateTopicsResult).GetField("_native", Private)!.GetValue(result)!).Values[topic];

    private static SafeKafkaFutureHandle HandleOf(KafkaFutureImpl<object?> future)
    {
        object source = typeof(KafkaFutureImpl<object?>).GetField("_source", Private)!.GetValue(future)!;
        return (SafeKafkaFutureHandle)typeof(NativeFutureSource<object?>).GetField("_handle", Private)!.GetValue(source)!;
    }

    private static SafeHandle OwnerOf(SafeKafkaFutureHandle future) =>
        (SafeHandle)typeof(SafeKafkaFutureHandle).GetField("_owner", Private)!.GetValue(future)!;

    private static void Flush(CompletionThread completion)
    {
        using ManualResetEventSlim drained = new ManualResetEventSlim();
        completion.PostAfterDrain(drained.Set);
        Assert.True(drained.Wait(BlockingCall.Deadline));
    }

    // The core queues a future's delivery from a task of its own, so one drain can run before it is queued.
    private static void FlushUntil(CompletionThread completion, Func<bool> done)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        do
        {
            Flush(completion);
        }
        while (!done() && elapsed.Elapsed < BlockingCall.Deadline);
    }

    /// <summary>Holds a client's drain thread inside a posted action until disposed.</summary>
    private sealed class DrainGate : IDisposable
    {
        private readonly ManualResetEventSlim _release = new ManualResetEventSlim();

        internal DrainGate(CompletionThread completion)
        {
            using ManualResetEventSlim entered = new ManualResetEventSlim();
            completion.Post(() =>
            {
                entered.Set();
                _release.Wait(BlockingCall.Deadline);
            });
            Assert.True(entered.Wait(BlockingCall.Deadline));
        }

        public void Dispose() => _release.Set();
    }

    /// <summary>The C getters only these tests call, to read the C options and the C topic back.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal delegate IntPtr Getter(IntPtr self, ref IntPtr outErr);

        internal delegate int Int32Getter(IntPtr self, ref IntPtr outErr);

        internal delegate short Int16Getter(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int CreateTopicsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_should_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte CreateTopicsOptionsShouldValidateOnly(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_should_retry_on_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte CreateTopicsOptionsShouldRetryOnQuotaViolation(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_name_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr NewTopicName(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_num_partitions_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int NewTopicNumPartitions(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_replication_factor_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern short NewTopicReplicationFactor(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_replicas_assignments_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr NewTopicReplicasAssignments(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_configs_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr NewTopicConfigs(IntPtr self, ref IntPtr outErr);
    }
}
