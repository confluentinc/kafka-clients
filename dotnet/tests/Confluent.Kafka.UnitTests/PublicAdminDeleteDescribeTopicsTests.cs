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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DeleteTopics</c> and <c>DescribeTopics</c> end to end against the mocks, by topic id and by name, with the
/// name-collection overloads and the C options the calls build.
/// </summary>
public sealed class PublicAdminDeleteDescribeTopicsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task TopicId_RoundTripsThroughTheAbi_AndComesBackAsTheSameUuid()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Topic = "p2a-roundtrip";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 1) }).All().Get(), s_deadline);

        AsyncDescribeTopicsResult byName = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        TopicDescription described = await TestTimeout.Run(() => byName.TopicNameValues![Topic].Get(), s_deadline);

        Uuid topicId = described.TopicId;
        Assert.NotEqual(Uuid.Zero, topicId);

        AsyncDescribeTopicsResult byId = admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { topicId }));
        Assert.True(byId.TopicIdValues!.ContainsKey(topicId));

        TopicDescription again = await TestTimeout.Run(() => byId.TopicIdValues![topicId].Get(), s_deadline);
        Assert.Equal(Topic, again.Name);
        Assert.Equal(topicId, again.TopicId);

        AsyncDeleteTopicsResult deleted = admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { topicId }));
        Assert.True(deleted.TopicIdValues!.ContainsKey(topicId));
        await TestTimeout.Run(() => deleted.TopicIdValues![topicId].Get(), s_deadline);
    }

    [Fact]
    public async Task WrongAccessor_ReturnsNull_BothDirections_BothResultTypes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Alpha = "p2a-d8-alpha";
        const string Beta = "p2a-d8-beta";

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Alpha, 1, 1), new NewTopic(Beta, 1, 1) }).All().Get(),
            s_deadline);

        AsyncDescribeTopicsResult describedByName =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Alpha }));
        Assert.NotNull(describedByName.TopicNameValues);
        Assert.Null(describedByName.TopicIdValues);
        Assert.NotNull(describedByName.AllTopicNames());
        Assert.Null(describedByName.AllTopicIds());

        Uuid alphaId = (await TestTimeout.Run(
            () => describedByName.TopicNameValues![Alpha].Get(), s_deadline)).TopicId;

        AsyncDescribeTopicsResult describedById = admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { alphaId }));
        Assert.NotNull(describedById.TopicIdValues);
        Assert.Null(describedById.TopicNameValues);
        Assert.NotNull(describedById.AllTopicIds());
        Assert.Null(describedById.AllTopicNames());

        AsyncDeleteTopicsResult deletedByName = admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Alpha }));
        Assert.NotNull(deletedByName.TopicNameValues);
        Assert.Null(deletedByName.TopicIdValues);
        await TestTimeout.Run(() => deletedByName.All().Get(), s_deadline);

        AsyncDescribeTopicsResult betaByName = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Beta }));
        Uuid betaId = (await TestTimeout.Run(() => betaByName.TopicNameValues![Beta].Get(), s_deadline)).TopicId;

        AsyncDeleteTopicsResult deletedById = admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { betaId }));
        Assert.NotNull(deletedById.TopicIdValues);
        Assert.Null(deletedById.TopicNameValues);
        await TestTimeout.Run(() => deletedById.All().Get(), s_deadline);
    }

    [Fact]
    public async Task MixedOutcome_EachKeyCarriesItsOwnResult()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Present = "p2a-mixed-present";
        const string Absent = "p2a-mixed-absent";

        // A second live topic for the describe half: the delete half consumes `Present`.
        const string StillPresent = "p2a-mixed-still-present";

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Present, 1, 1), new NewTopic(StillPresent, 1, 1) }).All().Get(),
            s_deadline);

        AsyncDeleteTopicsResult deleted =
            admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Present, Absent }));

        await TestTimeout.Run(() => deleted.TopicNameValues![Present].Get(), s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => deleted.TopicNameValues![Absent].Get(), s_deadline));
        Assert.Equal($"Topic {Absent} does not exist.", failure.Message);

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => deleted.All().Get(), s_deadline));

        AsyncDescribeTopicsResult described =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Absent, StillPresent }));
        Assert.Equal(StillPresent, (await TestTimeout.Run(
            () => described.TopicNameValues![StillPresent].Get(), s_deadline)).Name);

        KafkaException describeFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => described.TopicNameValues![Absent].Get(), s_deadline));
        Assert.Equal($"Topic {Absent} not found.", describeFailure.Message);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => described.AllTopicNames()!.Value.Get(), s_deadline));
    }

    [Fact]
    public async Task DescriptionTree_IsCopiedOut_AndOutlivesTheResultRoot()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Topic = "p2a-tree";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 3) }).All().Get(), s_deadline);

        AsyncDescribeTopicsResult result = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        TopicDescription description = await TestTimeout.Run(() => result.TopicNameValues![Topic].Get(), s_deadline);

        Assert.Equal(Topic, description.Name);
        Assert.False(description.IsInternal);
        Assert.Equal(2, description.Partitions.Count);

        TopicPartitionInfo partition = description.Partitions[0];
        Assert.Equal(0, partition.Partition);
        Assert.NotNull(partition.Leader);
        Assert.Equal(3, partition.Replicas.Count);

        Assert.NotEmpty(partition.Leader!.Host);
        Assert.True(partition.Leader.Port > 0);

        Assert.Contains("partition=0", partition.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReportedButEmptySets_AreEmptyCollections_NotNull()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Topic = "p2a-empty-sets";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        AsyncDescribeTopicsResult result = admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            new DescribeTopicsOptions { IncludeAuthorizedOperations = true });

        TopicDescription description = await TestTimeout.Run(() => result.TopicNameValues![Topic].Get(), s_deadline);

        Assert.NotNull(description.AuthorizedOperations);
        Assert.Empty(description.AuthorizedOperations!);

        TopicPartitionInfo partition = description.Partitions[0];
        Assert.NotNull(partition.Elr);
        Assert.Empty(partition.Elr!);
        Assert.NotNull(partition.LastKnownElr);
        Assert.Empty(partition.LastKnownElr!);
        Assert.NotNull(partition.InSyncReplicas);
    }

    [Fact]
    public async Task RepeatedKeys_YieldOneEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Topic = "p2a-dedup";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        AsyncDescribeTopicsResult byName =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic, Topic, Topic }));
        Assert.Single(byName.TopicNameValues!);

        Uuid topicId = (await TestTimeout.Run(() => byName.TopicNameValues![Topic].Get(), s_deadline)).TopicId;

        AsyncDescribeTopicsResult byId =
            admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { topicId, topicId }));
        Assert.Single(byId.TopicIdValues!);
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeAnyNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        // The cast picks the TopicCollection overload: a lone literal null matches the name-collection one too (CS0121).
        ArgumentException nullDelete = Assert.Throws<ArgumentException>(() => admin.DeleteTopics((TopicCollection)null!));
        Assert.Equal(
            "The TopicCollection: null provided did not match any supported classes for deleteTopics.",
            nullDelete.Message);
        Assert.Null(nullDelete.ParamName);
        ArgumentException nullDescribe =
            Assert.Throws<ArgumentException>(() => admin.DescribeTopics((TopicCollection)null!));
        Assert.Equal(
            "The TopicCollection: null provided did not match any supported classes for describeTopics.",
            nullDescribe.Message);
        Assert.Null(nullDescribe.ParamName);

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

        Assert.Null(Record.Exception(
            () => admin.DescribeTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DescribeTopicsOptions { PartitionSizeLimitPerResponse = -1 })));
        Assert.Null(Record.Exception(
            () => admin.DescribeTopics(
                TopicCollection.OfTopicNames(new[] { "alpha" }),
                new DescribeTopicsOptions { PartitionSizeLimitPerResponse = 0 })));
    }

    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)1), ReadBack((DeleteTopicsOptions?)null));
        Assert.Equal((-1, (sbyte)1), ReadBack(new DeleteTopicsOptions()));
        Assert.Empty(new DeleteTopicsOptions().Setters.InOrder);

        Assert.Equal((-1, (sbyte)0, 2000), ReadBack((DescribeTopicsOptions?)null));
        Assert.Equal((-1, (sbyte)0, 2000), ReadBack(new DescribeTopicsOptions()));
        Assert.Empty(new DescribeTopicsOptions().Setters.InOrder);
    }

    [Fact]
    public void EachSetter_ReachesTheCOptions_AndASetterRunTwiceSendsItsLastValue()
    {
        Assert.Equal((0, (sbyte)1), ReadBack(new DeleteTopicsOptions { TimeoutMs = -5 }));
        Assert.Equal((-1, (sbyte)0), ReadBack(new DeleteTopicsOptions { RetryOnQuotaViolation = false }));

        DeleteTopicsOptions delete = new DeleteTopicsOptions { TimeoutMs = 10, RetryOnQuotaViolation = false };
        delete.TimeoutMs = 20;
        delete.RetryOnQuotaViolation = true;
        Assert.Equal((20, (sbyte)1), ReadBack(delete));

        Assert.Equal((-1, (sbyte)1, 2000), ReadBack(new DescribeTopicsOptions { IncludeAuthorizedOperations = true }));
        Assert.Equal((-1, (sbyte)0, -7), ReadBack(new DescribeTopicsOptions { PartitionSizeLimitPerResponse = -7 }));

        DescribeTopicsOptions describe = new DescribeTopicsOptions
        {
            TimeoutMs = 10,
            IncludeAuthorizedOperations = true,
            PartitionSizeLimitPerResponse = 5,
        };
        describe.TimeoutMs = null;
        describe.IncludeAuthorizedOperations = false;
        describe.PartitionSizeLimitPerResponse = 6;
        Assert.Equal((-1, (sbyte)0, 6), ReadBack(describe));
    }

    [Fact]
    public void AfterClose_BothRpcsThrowObjectDisposed()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "alpha" })));
        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "alpha" })));
    }

    [Fact]
    public async Task NonAsciiTopicName_RoundTripsThroughBothRpcs()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        const string Topic = "p2a-témas-日本語";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        AsyncDescribeTopicsResult described = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        Assert.Equal(Topic, (await TestTimeout.Run(
            () => described.TopicNameValues![Topic].Get(), s_deadline)).Name);

        AsyncDeleteTopicsResult deleted = admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Topic }));
        Assert.True(deleted.TopicNameValues!.ContainsKey(Topic));
        await TestTimeout.Run(() => deleted.All().Get(), s_deadline);
    }

    [Fact]
    public void BothClientsImplementTheRpcs()
    {
        IAdmin mock = MockAdminClient.Create().Build();
        try
        {
            Assert.NotNull(mock.DescribeTopics(TopicCollection.OfTopicNames(Array.Empty<string>())));
        }
        finally
        {
            TestTimeout.Dispose(mock);
        }

        IAdmin real = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
        try
        {
            Assert.NotNull(real.DeleteTopics(TopicCollection.OfTopicNames(Array.Empty<string>())));
        }
        finally
        {
            TestTimeout.Dispose(real);
        }
    }

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

        Assert.Equal(2, type.GetMethods().Count(method => method.Name == nameof(IAdmin.DeleteTopics)));
        Assert.Equal(2, type.GetMethods().Count(method => method.Name == nameof(IAdmin.DescribeTopics)));
    }

    [Fact]
    public async Task G1_10_TheNameCollectionForms_BehaveAsTheTopicCollectionForm()
    {
        AsyncMockAdminClient mock = AsyncMockAdminClient.Create().Build();
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IAsyncAdmin admin = mock;
        const string Present = "p135-g110-present";
        const string Absent = "p135-g110-absent";

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Present, 2, 1) }).All().Get(), s_deadline);

        AsyncDescribeTopicsResult byName = admin.DescribeTopics(new[] { Present, Absent });
        AsyncDescribeTopicsResult byCollection =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Present, Absent }));
        Assert.Null(byName.TopicIdValues);
        Assert.Equal(
            byCollection.TopicNameValues!.Keys.OrderBy(name => name, StringComparer.Ordinal),
            byName.TopicNameValues!.Keys.OrderBy(name => name, StringComparer.Ordinal));

        TopicDescription named = await TestTimeout.Run(() => byName.TopicNameValues![Present].Get(), s_deadline);
        TopicDescription collected = await TestTimeout.Run(() => byCollection.TopicNameValues![Present].Get(), s_deadline);
        Assert.Equal(Present, named.Name);
        Assert.Equal(collected.Partitions.Count, named.Partitions.Count);
        Assert.Equal(collected.TopicId, named.TopicId);

        KafkaException describeFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => byName.TopicNameValues![Absent].Get(), s_deadline));
        Assert.Equal($"Topic {Absent} not found.", describeFailure.Message);

        AsyncDeleteTopicsResult deleted = admin.DeleteTopics(new[] { Present, Absent }, new DeleteTopicsOptions());
        Assert.Null(deleted.TopicIdValues);
        await TestTimeout.Run(() => deleted.TopicNameValues![Present].Get(), s_deadline);
        KafkaException deleteFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => deleted.TopicNameValues![Absent].Get(), s_deadline));
        Assert.Equal($"Topic {Absent} does not exist.", deleteFailure.Message);

        AsyncDescribeTopicsResult afterDelete = admin.DescribeTopics(new[] { Present }, new DescribeTopicsOptions());
        KafkaException gone = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => afterDelete.TopicNameValues![Present].Get(), s_deadline));
        Assert.Equal($"Topic {Present} not found.", gone.Message);
    }

    [Fact]
    public async Task G1_10_TheNameCollectionForms_ForwardTheirOptions()
    {
        IAsyncAdmin real = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });
        try
        {
            string[] names = { "alpha" };
            DeleteTopicsOptions deleteTimeout = new DeleteTopicsOptions { TimeoutMs = -1 };
            AssertSameTimeout(
                await Failure(() => real.DeleteTopics(TopicCollection.OfTopicNames(names), deleteTimeout).All().Get()),
                await Failure(() => real.DeleteTopics(names, deleteTimeout).All().Get()));

            DescribeTopicsOptions describeTimeout = new DescribeTopicsOptions { TimeoutMs = -1 };
            AssertSameTimeout(
                await Failure(() => real.DescribeTopics(TopicCollection.OfTopicNames(names), describeTimeout).AllTopicNames()!.Value.Get()),
                await Failure(() => real.DescribeTopics(names, describeTimeout).AllTopicNames()!.Value.Get()));
        }
        finally
        {
            TestTimeout.Dispose(real);
        }
    }

    private static Task<KafkaException> Failure(Func<Task> call) =>
        TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(call), s_deadline);

    private static void AssertSameTimeout(KafkaException expected, KafkaException actual)
    {
        Assert.Equal(7, expected.Code);
        Assert.Equal(expected.Code, actual.Code);
        Assert.Equal(expected.Message, actual.Message);
    }

    [Fact]
    public void G1_10_ANullNameCollection_IsRejectedWithItsParameterName()
    {
        IAdmin mock = MockAdminClient.Create().Build();
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
            TestTimeout.Dispose(mock);
            TestTimeout.Dispose(real);
        }
    }

    private static (int TimeoutMs, sbyte RetryOnQuotaViolation) ReadBack(DeleteTopicsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDeleteTopicsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DeleteTopicsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte retry = Native.DeleteTopicsOptionsShouldRetryOnQuotaViolation(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, retry);
        }
        finally
        {
            NativeAdminClient.FreeDeleteTopicsOptions(native);
        }
    }

    private static (int TimeoutMs, sbyte IncludeAuthorizedOperations, int PartitionSizeLimitPerResponse) ReadBack(
        DescribeTopicsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeTopicsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeTopicsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte include = Native.DescribeTopicsOptionsIncludeAuthorizedOperations(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int limit = Native.DescribeTopicsOptionsPartitionSizeLimitPerResponse(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, include, limit);
        }
        finally
        {
            NativeAdminClient.FreeDescribeTopicsOptions(native);
        }
    }

    /// <summary>The options getters the binding never reads, declared for the read-back tests.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DeleteTopicsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsOptions_should_retry_on_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DeleteTopicsOptionsShouldRetryOnQuotaViolation(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeTopicsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeTopicsOptionsIncludeAuthorizedOperations(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_partition_size_limit_per_response_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeTopicsOptionsPartitionSizeLimitPerResponse(IntPtr self, ref IntPtr outErr);
    }
}
