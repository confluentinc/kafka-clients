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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A null options value: the real clients hand it to the core, which throws <see cref="ArgumentNullException"/>,
/// except where the RPC's arguments send no request; the mocks take it as new options, except in the five RPCs whose
/// mock reads it. A null <c>configResourceTypes</c> reaches the core too.
/// </summary>
public sealed class PublicAdminNullOptionsTests
{
    private const string OptionsMustNotBeNull = "options must not be null";

    private const string TypesMustNotBeNull = "config_resource_types must not be null";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly string[] s_readingCalls =
    {
        "CreateTopics",
        "DeleteTopics(names)",
        "DeleteTopics(ids)",
        "DeleteTopics(TopicCollection names)",
        "DescribeTopics(names)",
        "DescribeTopics(ids)",
        "DescribeTopics(TopicCollection names)",
        "DescribeAcls",
        "DescribeConfigs",
        "IncrementalAlterConfigs",
        "DescribeLogDirs",
        "AlterReplicaLogDirs",
        "DescribeReplicaLogDirs",
        "CreatePartitions",
        "ListConfigResources",
        "DescribeCluster",
        "ListPartitionReassignments(partitions, options)",
        "ListPartitionReassignments(options)",
    };

    private static readonly string[] s_unsentCalls =
    {
        "CreateTopics: no topic",
        "CreateTopics: an empty name",
        "DeleteTopics(names): an empty name",
        "DeleteTopics(TopicCollection): an empty name",
        "DeleteTopics(TopicCollection): the zero id",
        "DescribeTopics(names): an empty name",
        "DescribeTopics(TopicCollection): an empty name",
        "DescribeAcls: an unknown filter",
        "DescribeConfigs: no resource",
        "IncrementalAlterConfigs: no resource",
        "DescribeLogDirs: no broker",
        "AlterReplicaLogDirs: no replica",
        "DescribeReplicaLogDirs: no replica",
        "CreatePartitions: no topic",
        "ListPartitionReassignments: a negative partition",
        "ListPartitionReassignments: an empty topic",
    };

    private static readonly string[] s_mockReadingCalls =
    {
        "CreateDelegationToken",
        "RenewDelegationToken",
        "ExpireDelegationToken",
        "DescribeDelegationToken",
        "UpdateFeatures",
    };

    private static readonly string[] s_mockSwappingCalls =
    {
        "CreateTopics(IEnumerable<NewTopic>)",
        "DeleteTopics(TopicCollection)",
        "DeleteTopics(IReadOnlyCollection<String>)",
        "DescribeTopics(TopicCollection)",
        "DescribeTopics(IReadOnlyCollection<String>)",
        "ListTopics()",
        "CreatePartitions(IReadOnlyDictionary<String, NewPartitions>)",
        "DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete>)",
        "DescribeCluster()",
        "ListConfigResources(IReadOnlyCollection<ConfigResourceType>)",
        "DescribeConfigs(IReadOnlyCollection<ConfigResource>)",
        "IncrementalAlterConfigs(IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>)",
        "DescribeLogDirs(IReadOnlyCollection<Int32>)",
        "AlterReplicaLogDirs(IReadOnlyDictionary<TopicPartitionReplica, String>)",
        "DescribeReplicaLogDirs(IReadOnlyCollection<TopicPartitionReplica>)",
        "ElectLeaders(ElectionType, IReadOnlyCollection<TopicPartition>)",
        "AlterPartitionReassignments(IReadOnlyDictionary<TopicPartition, NewPartitionReassignment>)",
        "AlterConsumerGroupOffsets(String, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)",
        "DeleteConsumerGroupOffsets(String, IReadOnlyCollection<TopicPartition>)",
        "ListPartitionReassignments(IReadOnlyCollection<TopicPartition>)",
        "ListPartitionReassignments()",
        "ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec>)",
        "ListGroups()",
        "DescribeConsumerGroups(IReadOnlyCollection<String>)",
        "DescribeClassicGroups(IReadOnlyCollection<String>)",
        "ListConsumerGroupOffsets(String)",
        "ListConsumerGroupOffsets(IReadOnlyDictionary<String, ListConsumerGroupOffsetsSpec>)",
        "DeleteConsumerGroups(IReadOnlyCollection<String>)",
        "CreateAcls(IEnumerable<AclBinding>)",
        "DeleteAcls(IEnumerable<AclBindingFilter>)",
        "DescribeAcls(AclBindingFilter)",
        "DescribeClientQuotas(ClientQuotaFilter)",
        "AlterClientQuotas(IEnumerable<ClientQuotaAlteration>)",
        "DescribeUserScramCredentials(IReadOnlyCollection<String>)",
        "AlterUserScramCredentials(IEnumerable<UserScramCredentialAlteration>)",
        "DescribeFeatures()",
        "UnregisterController(Int32)",
        "FenceProducers(IReadOnlyCollection<String>)",
        "DescribeTransactions(IReadOnlyCollection<String>)",
        "DescribeProducers(IReadOnlyCollection<TopicPartition>)",
        "ListTransactions()",
        "AbortTransaction(AbortTransactionSpec)",
        "ForceTerminateTransaction(String)",
    };

    public static TheoryData<bool, string> ReadingCalls => Rows(s_readingCalls);

    public static TheoryData<bool, string> UnsentCalls => Rows(s_unsentCalls);

    public static TheoryData<bool, string> MockReadingCalls => Rows(s_mockReadingCalls);

    public static TheoryData<bool, string> MockSwappingCalls => Rows(s_mockSwappingCalls);

    public static TheoryData<bool, bool> EveryClient => new TheoryData<bool, bool>
    {
        { false, false },
        { false, true },
        { true, false },
        { true, true },
    };

    [Theory]
    [MemberData(nameof(ReadingCalls))]
    public void RealClient_NullOptions_ThrowsTheCoresArgumentNullException(bool async, string rpc)
    {
        IDisposable admin = async ? IAsyncAdmin.Create(Unreachable()) : IAdmin.Create(Unreachable());
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AssertNullFromTheCore(
            OptionsMustNotBeNull,
            Record.Exception(() => admin is IAsyncAdmin asyncAdmin ? Reading(asyncAdmin, rpc) : Reading((IAdmin)admin, rpc)));
    }

    [Theory]
    [MemberData(nameof(UnsentCalls))]
    public async Task RealClient_NullOptions_WhereTheArgumentsSendNoRequest_RunsTheRpc(bool async, string rpc)
    {
        IDisposable admin = async ? IAsyncAdmin.Create(Unreachable()) : IAdmin.Create(Unreachable());
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        FutureProbe probe = admin is IAsyncAdmin asyncAdmin ? Unsent(asyncAdmin, rpc) : Unsent((IAdmin)admin, rpc);

        AssertUnsentOutcome(rpc, await TestTimeout.Run(() => probe.Outcome(), s_deadline));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RealClient_AnotherNullArgument_IsReportedBeforeTheOptions(bool async)
    {
        IDisposable admin = async ? IAsyncAdmin.Create(Unreachable()) : IAdmin.Create(Unreachable());
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Exception? createTopics = Record.Exception(() => admin is IAsyncAdmin asyncAdmin
            ? asyncAdmin.CreateTopics(null!, null!)
            : ((IAdmin)admin).CreateTopics(null!, null!));
        Exception? describeAcls = Record.Exception(() => admin is IAsyncAdmin asyncAdmin
            ? asyncAdmin.DescribeAcls(null!, null!)
            : ((IAdmin)admin).DescribeAcls(null!, null!));

        Assert.Equal("newTopics", Assert.IsType<ArgumentNullException>(createTopics).ParamName);
        Assert.Equal("filter", Assert.IsType<ArgumentNullException>(describeAcls).ParamName);
    }

    [Theory]
    [MemberData(nameof(EveryClient))]
    public void NullConfigResourceTypes_ThrowsTheCoresArgumentNullException_BeforeANullOptions(bool async, bool mock)
    {
        IDisposable admin = (async, mock) switch
        {
            (false, false) => IAdmin.Create(Unreachable()),
            (true, false) => IAsyncAdmin.Create(Unreachable()),
            (false, true) => MockAdminClient.Create().Build(),
            _ => AsyncMockAdminClient.Create().Build(),
        };
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        foreach (ListConfigResourcesOptions? options in new[] { new ListConfigResourcesOptions(), null })
        {
            AssertNullFromTheCore(
                TypesMustNotBeNull,
                Record.Exception(() => admin is IAsyncAdmin asyncAdmin
                    ? asyncAdmin.ListConfigResources(null!, options!)
                    : ((IAdmin)admin).ListConfigResources(null!, options!)));
        }
    }

    [Theory]
    [MemberData(nameof(MockReadingCalls))]
    public void Mock_NullOptions_WhereTheMockReadsThem_ThrowsTheCoresArgumentNullException(bool async, string rpc)
    {
        IDisposable admin = async ? AsyncMockAdminClient.Create().Build() : MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AssertNullFromTheCore(
            OptionsMustNotBeNull,
            Record.Exception(() => admin is IAsyncAdmin asyncAdmin
                ? MockReading(asyncAdmin, rpc)
                : MockReading((IAdmin)admin, rpc)));
    }

    [Theory]
    [MemberData(nameof(MockSwappingCalls))]
    public async Task Mock_NullOptions_WhereTheMockDoesNotReadThem_RunsTheRpc(bool async, string rpc)
    {
        IDisposable admin = async ? AsyncMockAdminClient.Create().Build() : MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        FutureProbe? probe = null;
        Exception? thrown = Record.Exception(() => probe = admin is IAsyncAdmin asyncAdmin
            ? MockSwapping(asyncAdmin, rpc)
            : MockSwapping((IAdmin)admin, rpc));

        Assert.True(thrown is null, $"{rpc} threw {thrown}");
        await TestTimeout.Run(() => probe!.Outcome(), s_deadline);
        Assert.True(probe!.IsDone);
    }

    [Theory]
    [InlineData(typeof(MockAdminClient))]
    [InlineData(typeof(AsyncMockAdminClient))]
    public void MockSwappingCalls_AreEveryLongFormWhoseOptionsTheMockDoesNotRead(Type mock)
    {
        Assert.Equal(
            s_mockSwappingCalls.OrderBy(call => call, StringComparer.Ordinal),
            mock.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.GetParameters().LastOrDefault() is { } options
                    && IsAdminOptions(options.ParameterType)
                    && !s_mockReadingCalls.Contains(method.Name)
                    && method.Name != nameof(IAdmin.RemoveMembersFromConsumerGroup))
                .Select(Signature)
                .OrderBy(call => call, StringComparer.Ordinal));
    }

    [Fact]
    public void Mock_NullOptions_AreTheDefaultOptions()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        admin.CreateTopics(new[] { new NewTopic("created", 1, 1) }, null!).All().Get(s_deadline);

        Assert.Contains("created", admin.ListTopics(null!).Names().Get(s_deadline));
        Assert.Equal(
            admin.DescribeCluster().ClusterId().Get(s_deadline),
            admin.DescribeCluster(null!).ClusterId().Get(s_deadline));
        Assert.Equal(
            new ConfigResource(ConfigResourceType.TOPIC, "created"),
            Assert.Single(admin.ListConfigResources(new[] { ConfigResourceType.TOPIC }, null!).All().Get(s_deadline)));
        Assert.Empty(admin.ListPartitionReassignments((ListPartitionReassignmentsOptions)null!)
            .Reassignments().Get(s_deadline));
        admin.DeleteTopics(new[] { "created" }, null!).All().Get(s_deadline);
        Assert.DoesNotContain("created", admin.ListTopics().Names().Get(s_deadline));
    }

    [Fact]
    public async Task AsyncMock_NullOptions_AreTheDefaultOptions()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("created", 1, 1) }, null!).All().Get(), s_deadline);

        Assert.Contains("created", await TestTimeout.Run(() => admin.ListTopics(null!).Names().Get(), s_deadline));
        Assert.Equal(
            await TestTimeout.Run(() => admin.DescribeCluster().ClusterId().Get(), s_deadline),
            await TestTimeout.Run(() => admin.DescribeCluster(null!).ClusterId().Get(), s_deadline));
        Assert.Equal(
            new ConfigResource(ConfigResourceType.TOPIC, "created"),
            Assert.Single(await TestTimeout.Run(
                () => admin.ListConfigResources(new[] { ConfigResourceType.TOPIC }, null!).All().Get(), s_deadline)));
        Assert.Empty(await TestTimeout.Run(
            () => admin.ListPartitionReassignments((ListPartitionReassignmentsOptions)null!).Reassignments().Get(),
            s_deadline));
        await TestTimeout.Run(() => admin.DeleteTopics(new[] { "created" }, null!).All().Get(), s_deadline);
        Assert.DoesNotContain("created", await TestTimeout.Run(() => admin.ListTopics().Names().Get(), s_deadline));
    }

    [Fact]
    public void EveryOptionsMarshaller_BuildsNoCOptions_ForANullOptions()
    {
        List<MethodInfo> builders = typeof(NativeAdminClient)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => method.Name.StartsWith("New", StringComparison.Ordinal)
                && method.ReturnType == typeof(IntPtr)
                && method.GetParameters().Length == 1
                && IsAdminOptions(method.GetParameters()[0].ParameterType))
            .ToList();

        Assert.Equal(
            typeof(IAdmin).GetMethods()
                .Select(method => method.GetParameters().LastOrDefault()?.ParameterType)
                .Where(type => type is not null && IsAdminOptions(type))
                .Select(type => type!.Name)
                .Distinct()
                .OrderBy(name => name, StringComparer.Ordinal),
            builders.Select(builder => builder.GetParameters()[0].ParameterType.Name).OrderBy(name => name, StringComparer.Ordinal));
        // RemoveMembersFromConsumerGroup rejects a null options itself, before its options are built.
        foreach (MethodInfo builder in builders.Where(builder =>
            builder.GetParameters()[0].ParameterType != typeof(RemoveMembersFromConsumerGroupOptions)))
        {
            Assert.Equal(IntPtr.Zero, (IntPtr)builder.Invoke(null, new object?[] { null })!);
        }
    }

    private static TheoryData<bool, string> Rows(string[] rpcs)
    {
        TheoryData<bool, string> rows = new TheoryData<bool, string>();
        foreach (bool async in new[] { false, true })
        {
            foreach (string rpc in rpcs)
            {
                rows.Add(async, rpc);
            }
        }

        return rows;
    }

    private static object Reading(IAdmin admin, string rpc) => rpc switch
    {
        "CreateTopics" => admin.CreateTopics(new[] { new NewTopic("t", 1, 1) }, null!),
        "DeleteTopics(names)" => admin.DeleteTopics(new[] { "", "t" }, null!),
        "DeleteTopics(ids)" => admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { Uuid.ZERO_UUID, Uuid.RandomUuid() }), null!),
        "DeleteTopics(TopicCollection names)" => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "", "t" }), null!),
        "DescribeTopics(names)" => admin.DescribeTopics(new[] { "", "t" }, null!),
        "DescribeTopics(ids)" => admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { Uuid.ZERO_UUID }), null!),
        "DescribeTopics(TopicCollection names)" => admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "", "t" }), null!),
        "DescribeAcls" => admin.DescribeAcls(AclBindingFilter.ANY, null!),
        "DescribeConfigs" => admin.DescribeConfigs(new[] { TopicResource() }, null!),
        "IncrementalAlterConfigs" => admin.IncrementalAlterConfigs(ConfigChanges(), null!),
        "DescribeLogDirs" => admin.DescribeLogDirs(new[] { 0 }, null!),
        "AlterReplicaLogDirs" => admin.AlterReplicaLogDirs(ReplicaAssignment(), null!),
        "DescribeReplicaLogDirs" => admin.DescribeReplicaLogDirs(new[] { Replica() }, null!),
        "CreatePartitions" => admin.CreatePartitions(PartitionIncrease(), null!),
        "ListConfigResources" => admin.ListConfigResources(Array.Empty<ConfigResourceType>(), null!),
        "DescribeCluster" => admin.DescribeCluster(null!),
        "ListPartitionReassignments(partitions, options)" => admin.ListPartitionReassignments(new[] { Partition() }, null!),
        "ListPartitionReassignments(options)" => admin.ListPartitionReassignments((ListPartitionReassignmentsOptions)null!),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static object Reading(IAsyncAdmin admin, string rpc) => rpc switch
    {
        "CreateTopics" => admin.CreateTopics(new[] { new NewTopic("t", 1, 1) }, null!),
        "DeleteTopics(names)" => admin.DeleteTopics(new[] { "", "t" }, null!),
        "DeleteTopics(ids)" => admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { Uuid.ZERO_UUID, Uuid.RandomUuid() }), null!),
        "DeleteTopics(TopicCollection names)" => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "", "t" }), null!),
        "DescribeTopics(names)" => admin.DescribeTopics(new[] { "", "t" }, null!),
        "DescribeTopics(ids)" => admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { Uuid.ZERO_UUID }), null!),
        "DescribeTopics(TopicCollection names)" => admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "", "t" }), null!),
        "DescribeAcls" => admin.DescribeAcls(AclBindingFilter.ANY, null!),
        "DescribeConfigs" => admin.DescribeConfigs(new[] { TopicResource() }, null!),
        "IncrementalAlterConfigs" => admin.IncrementalAlterConfigs(ConfigChanges(), null!),
        "DescribeLogDirs" => admin.DescribeLogDirs(new[] { 0 }, null!),
        "AlterReplicaLogDirs" => admin.AlterReplicaLogDirs(ReplicaAssignment(), null!),
        "DescribeReplicaLogDirs" => admin.DescribeReplicaLogDirs(new[] { Replica() }, null!),
        "CreatePartitions" => admin.CreatePartitions(PartitionIncrease(), null!),
        "ListConfigResources" => admin.ListConfigResources(Array.Empty<ConfigResourceType>(), null!),
        "DescribeCluster" => admin.DescribeCluster(null!),
        "ListPartitionReassignments(partitions, options)" => admin.ListPartitionReassignments(new[] { Partition() }, null!),
        "ListPartitionReassignments(options)" => admin.ListPartitionReassignments((ListPartitionReassignmentsOptions)null!),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static FutureProbe MockSwapping(IAdmin admin, string rpc) => rpc switch
    {
        "CreateTopics(IEnumerable<NewTopic>)" =>
            FutureProbe.Of(admin.CreateTopics(new[] { new NewTopic("t", 1, 1) }, null!).All()),
        "DeleteTopics(TopicCollection)" =>
            FutureProbe.Of(admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "t" }), null!).All()),
        "DeleteTopics(IReadOnlyCollection<String>)" => FutureProbe.Of(admin.DeleteTopics(new[] { "t" }, null!).All()),
        "DescribeTopics(TopicCollection)" =>
            FutureProbe.Of(admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "t" }), null!).AllTopicNames()!.Value),
        "DescribeTopics(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeTopics(new[] { "t" }, null!).AllTopicNames()!.Value),
        "ListTopics()" => FutureProbe.Of(admin.ListTopics(null!).Names()),
        "CreatePartitions(IReadOnlyDictionary<String, NewPartitions>)" =>
            FutureProbe.Of(admin.CreatePartitions(PartitionIncrease(), null!).All()),
        "DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete>)" =>
            FutureProbe.Of(admin.DeleteRecords(RecordsToDeleteBefore(), null!).All()),
        "DescribeCluster()" => FutureProbe.Of(admin.DescribeCluster(null!).ClusterId()),
        "ListConfigResources(IReadOnlyCollection<ConfigResourceType>)" =>
            FutureProbe.Of(admin.ListConfigResources(new[] { ConfigResourceType.TOPIC }, null!).All()),
        "DescribeConfigs(IReadOnlyCollection<ConfigResource>)" =>
            FutureProbe.Of(admin.DescribeConfigs(new[] { TopicResource() }, null!).All()),
        "IncrementalAlterConfigs(IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>)" =>
            FutureProbe.Of(admin.IncrementalAlterConfigs(ConfigChanges(), null!).All()),
        "DescribeLogDirs(IReadOnlyCollection<Int32>)" =>
            FutureProbe.Of(admin.DescribeLogDirs(new[] { 0 }, null!).AllDescriptions()),
        "AlterReplicaLogDirs(IReadOnlyDictionary<TopicPartitionReplica, String>)" =>
            FutureProbe.Of(admin.AlterReplicaLogDirs(ReplicaAssignment(), null!).All()),
        "DescribeReplicaLogDirs(IReadOnlyCollection<TopicPartitionReplica>)" =>
            FutureProbe.Of(admin.DescribeReplicaLogDirs(new[] { Replica() }, null!).All()),
        "ElectLeaders(ElectionType, IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.ElectLeaders(ElectionType.PREFERRED, new[] { Partition() }, null!).All()),
        "AlterPartitionReassignments(IReadOnlyDictionary<TopicPartition, NewPartitionReassignment>)" =>
            FutureProbe.Of(admin.AlterPartitionReassignments(Reassignment(), null!).All()),
        "AlterConsumerGroupOffsets(String, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)" =>
            FutureProbe.Of(admin.AlterConsumerGroupOffsets("g", Offsets(), null!).All()),
        "DeleteConsumerGroupOffsets(String, IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.DeleteConsumerGroupOffsets("g", new[] { Partition() }, null!).All()),
        "ListPartitionReassignments(IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.ListPartitionReassignments(new[] { Partition() }, null!).Reassignments()),
        "ListPartitionReassignments()" =>
            FutureProbe.Of(admin.ListPartitionReassignments((ListPartitionReassignmentsOptions)null!).Reassignments()),
        "ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec>)" =>
            FutureProbe.Of(admin.ListOffsets(LatestOffsets(), null!).All()),
        "ListGroups()" => FutureProbe.Of(admin.ListGroups(null!).All()),
        "DescribeConsumerGroups(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeConsumerGroups(new[] { "g" }, null!).All()),
        "DescribeClassicGroups(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeClassicGroups(new[] { "g" }, null!).All()),
        "ListConsumerGroupOffsets(String)" => FutureProbe.Of(admin.ListConsumerGroupOffsets("g", null!).All()),
        "ListConsumerGroupOffsets(IReadOnlyDictionary<String, ListConsumerGroupOffsetsSpec>)" =>
            FutureProbe.Of(admin.ListConsumerGroupOffsets(GroupSpecs(), null!).All()),
        "DeleteConsumerGroups(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DeleteConsumerGroups(new[] { "g" }, null!).All()),
        "CreateAcls(IEnumerable<AclBinding>)" => FutureProbe.Of(admin.CreateAcls(new[] { Acl() }, null!).All()),
        "DeleteAcls(IEnumerable<AclBindingFilter>)" =>
            FutureProbe.Of(admin.DeleteAcls(new[] { AclBindingFilter.ANY }, null!).All()),
        "DescribeAcls(AclBindingFilter)" => FutureProbe.Of(admin.DescribeAcls(AclBindingFilter.ANY, null!).Values()),
        "DescribeClientQuotas(ClientQuotaFilter)" =>
            FutureProbe.Of(admin.DescribeClientQuotas(ClientQuotaFilter.All(), null!).Entities()),
        "AlterClientQuotas(IEnumerable<ClientQuotaAlteration>)" =>
            FutureProbe.Of(admin.AlterClientQuotas(new[] { QuotaAlteration() }, null!).All()),
        "DescribeUserScramCredentials(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeUserScramCredentials(new[] { "u" }, null!).All()),
        "AlterUserScramCredentials(IEnumerable<UserScramCredentialAlteration>)" =>
            FutureProbe.Of(admin.AlterUserScramCredentials(ScramDeletion(), null!).All()),
        "DescribeFeatures()" => FutureProbe.Of(admin.DescribeFeatures(null!).FeatureMetadata()),
        "UnregisterController(Int32)" => FutureProbe.Of(admin.UnregisterController(0, null!).All()),
        "FenceProducers(IReadOnlyCollection<String>)" => FutureProbe.Of(admin.FenceProducers(new[] { "x" }, null!).All()),
        "DescribeTransactions(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeTransactions(new[] { "x" }, null!).All()),
        "DescribeProducers(IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.DescribeProducers(new[] { Partition() }, null!).All()),
        "ListTransactions()" => FutureProbe.Of(admin.ListTransactions(null!).All()),
        "AbortTransaction(AbortTransactionSpec)" => FutureProbe.Of(admin.AbortTransaction(AbortSpec(), null!).All()),
        "ForceTerminateTransaction(String)" => FutureProbe.Of(admin.ForceTerminateTransaction("x", null!).Result()),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static FutureProbe MockSwapping(IAsyncAdmin admin, string rpc) => rpc switch
    {
        "CreateTopics(IEnumerable<NewTopic>)" =>
            FutureProbe.Of(admin.CreateTopics(new[] { new NewTopic("t", 1, 1) }, null!).All()),
        "DeleteTopics(TopicCollection)" =>
            FutureProbe.Of(admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "t" }), null!).All()),
        "DeleteTopics(IReadOnlyCollection<String>)" => FutureProbe.Of(admin.DeleteTopics(new[] { "t" }, null!).All()),
        "DescribeTopics(TopicCollection)" =>
            FutureProbe.Of(admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "t" }), null!).AllTopicNames()!.Value),
        "DescribeTopics(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeTopics(new[] { "t" }, null!).AllTopicNames()!.Value),
        "ListTopics()" => FutureProbe.Of(admin.ListTopics(null!).Names()),
        "CreatePartitions(IReadOnlyDictionary<String, NewPartitions>)" =>
            FutureProbe.Of(admin.CreatePartitions(PartitionIncrease(), null!).All()),
        "DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete>)" =>
            FutureProbe.Of(admin.DeleteRecords(RecordsToDeleteBefore(), null!).All()),
        "DescribeCluster()" => FutureProbe.Of(admin.DescribeCluster(null!).ClusterId()),
        "ListConfigResources(IReadOnlyCollection<ConfigResourceType>)" =>
            FutureProbe.Of(admin.ListConfigResources(new[] { ConfigResourceType.TOPIC }, null!).All()),
        "DescribeConfigs(IReadOnlyCollection<ConfigResource>)" =>
            FutureProbe.Of(admin.DescribeConfigs(new[] { TopicResource() }, null!).All()),
        "IncrementalAlterConfigs(IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>)" =>
            FutureProbe.Of(admin.IncrementalAlterConfigs(ConfigChanges(), null!).All()),
        "DescribeLogDirs(IReadOnlyCollection<Int32>)" =>
            FutureProbe.Of(admin.DescribeLogDirs(new[] { 0 }, null!).AllDescriptions()),
        "AlterReplicaLogDirs(IReadOnlyDictionary<TopicPartitionReplica, String>)" =>
            FutureProbe.Of(admin.AlterReplicaLogDirs(ReplicaAssignment(), null!).All()),
        "DescribeReplicaLogDirs(IReadOnlyCollection<TopicPartitionReplica>)" =>
            FutureProbe.Of(admin.DescribeReplicaLogDirs(new[] { Replica() }, null!).All()),
        "ElectLeaders(ElectionType, IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.ElectLeaders(ElectionType.PREFERRED, new[] { Partition() }, null!).All()),
        "AlterPartitionReassignments(IReadOnlyDictionary<TopicPartition, NewPartitionReassignment>)" =>
            FutureProbe.Of(admin.AlterPartitionReassignments(Reassignment(), null!).All()),
        "AlterConsumerGroupOffsets(String, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)" =>
            FutureProbe.Of(admin.AlterConsumerGroupOffsets("g", Offsets(), null!).All()),
        "DeleteConsumerGroupOffsets(String, IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.DeleteConsumerGroupOffsets("g", new[] { Partition() }, null!).All()),
        "ListPartitionReassignments(IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.ListPartitionReassignments(new[] { Partition() }, null!).Reassignments()),
        "ListPartitionReassignments()" =>
            FutureProbe.Of(admin.ListPartitionReassignments((ListPartitionReassignmentsOptions)null!).Reassignments()),
        "ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec>)" =>
            FutureProbe.Of(admin.ListOffsets(LatestOffsets(), null!).All()),
        "ListGroups()" => FutureProbe.Of(admin.ListGroups(null!).All()),
        "DescribeConsumerGroups(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeConsumerGroups(new[] { "g" }, null!).All()),
        "DescribeClassicGroups(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeClassicGroups(new[] { "g" }, null!).All()),
        "ListConsumerGroupOffsets(String)" => FutureProbe.Of(admin.ListConsumerGroupOffsets("g", null!).All()),
        "ListConsumerGroupOffsets(IReadOnlyDictionary<String, ListConsumerGroupOffsetsSpec>)" =>
            FutureProbe.Of(admin.ListConsumerGroupOffsets(GroupSpecs(), null!).All()),
        "DeleteConsumerGroups(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DeleteConsumerGroups(new[] { "g" }, null!).All()),
        "CreateAcls(IEnumerable<AclBinding>)" => FutureProbe.Of(admin.CreateAcls(new[] { Acl() }, null!).All()),
        "DeleteAcls(IEnumerable<AclBindingFilter>)" =>
            FutureProbe.Of(admin.DeleteAcls(new[] { AclBindingFilter.ANY }, null!).All()),
        "DescribeAcls(AclBindingFilter)" => FutureProbe.Of(admin.DescribeAcls(AclBindingFilter.ANY, null!).Values()),
        "DescribeClientQuotas(ClientQuotaFilter)" =>
            FutureProbe.Of(admin.DescribeClientQuotas(ClientQuotaFilter.All(), null!).Entities()),
        "AlterClientQuotas(IEnumerable<ClientQuotaAlteration>)" =>
            FutureProbe.Of(admin.AlterClientQuotas(new[] { QuotaAlteration() }, null!).All()),
        "DescribeUserScramCredentials(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeUserScramCredentials(new[] { "u" }, null!).All()),
        "AlterUserScramCredentials(IEnumerable<UserScramCredentialAlteration>)" =>
            FutureProbe.Of(admin.AlterUserScramCredentials(ScramDeletion(), null!).All()),
        "DescribeFeatures()" => FutureProbe.Of(admin.DescribeFeatures(null!).FeatureMetadata()),
        "UnregisterController(Int32)" => FutureProbe.Of(admin.UnregisterController(0, null!).All()),
        "FenceProducers(IReadOnlyCollection<String>)" => FutureProbe.Of(admin.FenceProducers(new[] { "x" }, null!).All()),
        "DescribeTransactions(IReadOnlyCollection<String>)" =>
            FutureProbe.Of(admin.DescribeTransactions(new[] { "x" }, null!).All()),
        "DescribeProducers(IReadOnlyCollection<TopicPartition>)" =>
            FutureProbe.Of(admin.DescribeProducers(new[] { Partition() }, null!).All()),
        "ListTransactions()" => FutureProbe.Of(admin.ListTransactions(null!).All()),
        "AbortTransaction(AbortTransactionSpec)" => FutureProbe.Of(admin.AbortTransaction(AbortSpec(), null!).All()),
        "ForceTerminateTransaction(String)" => FutureProbe.Of(admin.ForceTerminateTransaction("x", null!).Result()),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static object MockReading(IAdmin admin, string rpc) => rpc switch
    {
        "CreateDelegationToken" => admin.CreateDelegationToken(null!),
        "RenewDelegationToken" => admin.RenewDelegationToken(new byte[] { 1 }, null!),
        "ExpireDelegationToken" => admin.ExpireDelegationToken(new byte[] { 1 }, null!),
        "DescribeDelegationToken" => admin.DescribeDelegationToken(null!),
        "UpdateFeatures" => admin.UpdateFeatures(FeatureUpdates(), null!),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static object MockReading(IAsyncAdmin admin, string rpc) => rpc switch
    {
        "CreateDelegationToken" => admin.CreateDelegationToken(null!),
        "RenewDelegationToken" => admin.RenewDelegationToken(new byte[] { 1 }, null!),
        "ExpireDelegationToken" => admin.ExpireDelegationToken(new byte[] { 1 }, null!),
        "DescribeDelegationToken" => admin.DescribeDelegationToken(null!),
        "UpdateFeatures" => admin.UpdateFeatures(FeatureUpdates(), null!),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static FutureProbe Unsent(IAdmin admin, string rpc) => rpc switch
    {
        "CreateTopics: no topic" => FutureProbe.Of(admin.CreateTopics(Array.Empty<NewTopic>(), null!).All()),
        "CreateTopics: an empty name" =>
            FutureProbe.Of(admin.CreateTopics(new[] { new NewTopic("", 1, 1) }, null!).Values()[""]),
        "DeleteTopics(names): an empty name" =>
            FutureProbe.Of(admin.DeleteTopics(new[] { "" }, null!).TopicNameValues()![""]),
        "DeleteTopics(TopicCollection): an empty name" =>
            FutureProbe.Of(admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "" }), null!).TopicNameValues()![""]),
        "DeleteTopics(TopicCollection): the zero id" => FutureProbe.Of(
            admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { Uuid.ZERO_UUID }), null!).TopicIdValues()![Uuid.ZERO_UUID]),
        "DescribeTopics(names): an empty name" =>
            FutureProbe.Of(admin.DescribeTopics(new[] { "" }, null!).TopicNameValues()![""]),
        "DescribeTopics(TopicCollection): an empty name" => FutureProbe.Of(
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "" }), null!).TopicNameValues()![""]),
        "DescribeAcls: an unknown filter" => FutureProbe.Of(admin.DescribeAcls(UnknownFilter(), null!).Values()),
        "DescribeConfigs: no resource" =>
            FutureProbe.Of(admin.DescribeConfigs(Array.Empty<ConfigResource>(), null!).All()),
        "IncrementalAlterConfigs: no resource" => FutureProbe.Of(admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(), null!).All()),
        "DescribeLogDirs: no broker" =>
            FutureProbe.Of(admin.DescribeLogDirs(Array.Empty<int>(), null!).AllDescriptions()),
        "AlterReplicaLogDirs: no replica" => FutureProbe.Of(
            admin.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string>(), null!).All()),
        "DescribeReplicaLogDirs: no replica" =>
            FutureProbe.Of(admin.DescribeReplicaLogDirs(Array.Empty<TopicPartitionReplica>(), null!).All()),
        "CreatePartitions: no topic" =>
            FutureProbe.Of(admin.CreatePartitions(new Dictionary<string, NewPartitions>(), null!).All()),
        "ListPartitionReassignments: a negative partition" => FutureProbe.Of(admin.ListPartitionReassignments(
            new[] { new TopicPartition("t", 0), new TopicPartition("t", -1) }, null!).Reassignments()),
        "ListPartitionReassignments: an empty topic" => FutureProbe.Of(
            admin.ListPartitionReassignments(new[] { new TopicPartition("", 0) }, null!).Reassignments()),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static FutureProbe Unsent(IAsyncAdmin admin, string rpc) => rpc switch
    {
        "CreateTopics: no topic" => FutureProbe.Of(admin.CreateTopics(Array.Empty<NewTopic>(), null!).All()),
        "CreateTopics: an empty name" =>
            FutureProbe.Of(admin.CreateTopics(new[] { new NewTopic("", 1, 1) }, null!).Values()[""]),
        "DeleteTopics(names): an empty name" =>
            FutureProbe.Of(admin.DeleteTopics(new[] { "" }, null!).TopicNameValues()![""]),
        "DeleteTopics(TopicCollection): an empty name" =>
            FutureProbe.Of(admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "" }), null!).TopicNameValues()![""]),
        "DeleteTopics(TopicCollection): the zero id" => FutureProbe.Of(
            admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { Uuid.ZERO_UUID }), null!).TopicIdValues()![Uuid.ZERO_UUID]),
        "DescribeTopics(names): an empty name" =>
            FutureProbe.Of(admin.DescribeTopics(new[] { "" }, null!).TopicNameValues()![""]),
        "DescribeTopics(TopicCollection): an empty name" => FutureProbe.Of(
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "" }), null!).TopicNameValues()![""]),
        "DescribeAcls: an unknown filter" => FutureProbe.Of(admin.DescribeAcls(UnknownFilter(), null!).Values()),
        "DescribeConfigs: no resource" =>
            FutureProbe.Of(admin.DescribeConfigs(Array.Empty<ConfigResource>(), null!).All()),
        "IncrementalAlterConfigs: no resource" => FutureProbe.Of(admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(), null!).All()),
        "DescribeLogDirs: no broker" =>
            FutureProbe.Of(admin.DescribeLogDirs(Array.Empty<int>(), null!).AllDescriptions()),
        "AlterReplicaLogDirs: no replica" => FutureProbe.Of(
            admin.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string>(), null!).All()),
        "DescribeReplicaLogDirs: no replica" =>
            FutureProbe.Of(admin.DescribeReplicaLogDirs(Array.Empty<TopicPartitionReplica>(), null!).All()),
        "CreatePartitions: no topic" =>
            FutureProbe.Of(admin.CreatePartitions(new Dictionary<string, NewPartitions>(), null!).All()),
        "ListPartitionReassignments: a negative partition" => FutureProbe.Of(admin.ListPartitionReassignments(
            new[] { new TopicPartition("t", 0), new TopicPartition("t", -1) }, null!).Reassignments()),
        "ListPartitionReassignments: an empty topic" => FutureProbe.Of(
            admin.ListPartitionReassignments(new[] { new TopicPartition("", 0) }, null!).Reassignments()),
        _ => throw new ArgumentOutOfRangeException(nameof(rpc), rpc, null),
    };

    private static void AssertUnsentOutcome(string rpc, object? outcome)
    {
        switch (rpc)
        {
            case "CreateTopics: no topic":
            case "IncrementalAlterConfigs: no resource":
            case "AlterReplicaLogDirs: no replica":
            case "CreatePartitions: no topic":
                Assert.Same(BlockingCall.Returned, outcome);
                break;
            case "DescribeConfigs: no resource":
                Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<ConfigResource, Config>>(outcome));
                break;
            case "DescribeLogDirs: no broker":
                Assert.Empty(
                    Assert.IsAssignableFrom<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>(outcome));
                break;
            case "DescribeReplicaLogDirs: no replica":
                Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartitionReplica,
                    DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>(outcome));
                break;
            case "DescribeAcls: an unknown filter":
                Assert.Equal(
                    "The AclBindingFilter must not contain UNKNOWN elements.",
                    Assert.IsType<InvalidRequestException>(outcome).Message);
                break;
            case "DeleteTopics(TopicCollection): the zero id":
                Assert.Equal(
                    "The given topic ID 'AAAAAAAAAAAAAAAAAAAAAA' cannot be represented in a request.",
                    Assert.IsType<InvalidTopicException>(outcome).Message);
                break;
            case "ListPartitionReassignments: a negative partition":
                Assert.Equal(
                    "The given partition index -1 is not valid.", Assert.IsType<InvalidTopicException>(outcome).Message);
                break;
            default:
                Assert.Equal(
                    "The given topic name '' cannot be represented in a request.",
                    Assert.IsType<InvalidTopicException>(outcome).Message);
                break;
        }
    }

    private static void AssertNullFromTheCore(string message, Exception? thrown)
    {
        ArgumentNullException rejected = Assert.IsType<ArgumentNullException>(thrown);
        Assert.Equal(message, rejected.Message);
        Assert.Null(rejected.ParamName);
    }

    private static bool IsAdminOptions(Type type) =>
        type.Namespace == typeof(IAdmin).Namespace && type.Name.EndsWith("Options", StringComparison.Ordinal);

    private static string Signature(MethodInfo method)
    {
        ParameterInfo[] parameters = method.GetParameters();
        return method.Name + "("
            + string.Join(", ", parameters.Take(parameters.Length - 1).Select(parameter => TypeName(parameter.ParameterType)))
            + ")";
    }

    private static string TypeName(Type type) =>
        type.IsGenericType
            ? type.Name.Substring(0, type.Name.IndexOf('`')) + "<" + string.Join(", ", type.GetGenericArguments().Select(TypeName)) + ">"
            : type.Name;

    private static TopicPartition Partition() => new TopicPartition("t", 0);

    private static TopicPartitionReplica Replica() => new TopicPartitionReplica("t", 0, 0);

    private static ConfigResource TopicResource() => new ConfigResource(ConfigResourceType.TOPIC, "t");

    private static Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> ConfigChanges() =>
        new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
        {
            [TopicResource()] = new[] { new AlterConfigOp(new ConfigEntry("retention.ms", "1"), OpType.SET) },
        };

    private static Dictionary<TopicPartitionReplica, string> ReplicaAssignment() =>
        new Dictionary<TopicPartitionReplica, string> { [Replica()] = "/logs" };

    private static Dictionary<string, NewPartitions> PartitionIncrease() =>
        new Dictionary<string, NewPartitions> { ["t"] = NewPartitions.IncreaseTo(2) };

    private static Dictionary<TopicPartition, RecordsToDelete> RecordsToDeleteBefore() =>
        new Dictionary<TopicPartition, RecordsToDelete> { [Partition()] = RecordsToDelete.BeforeOffset(0) };

    private static Dictionary<TopicPartition, NewPartitionReassignment?> Reassignment() =>
        new Dictionary<TopicPartition, NewPartitionReassignment?> { [Partition()] = new NewPartitionReassignment(new[] { 0 }) };

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [Partition()] = new OffsetAndMetadata(0) };

    private static Dictionary<TopicPartition, OffsetSpec> LatestOffsets() =>
        new Dictionary<TopicPartition, OffsetSpec> { [Partition()] = OffsetSpec.Latest() };

    private static Dictionary<string, ListConsumerGroupOffsetsSpec> GroupSpecs() =>
        new Dictionary<string, ListConsumerGroupOffsetsSpec> { ["g"] = new ListConsumerGroupOffsetsSpec() };

    private static AclBinding Acl() =>
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "t", PatternType.LITERAL),
            new AccessControlEntry("User:u", "*", AclOperation.READ, AclPermissionType.ALLOW));

    private static ClientQuotaAlteration QuotaAlteration() =>
        new ClientQuotaAlteration(
            new ClientQuotaEntity(new Dictionary<string, string?> { [ClientQuotaEntity.USER] = "u" }),
            new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1.0) });

    private static UserScramCredentialAlteration[] ScramDeletion() =>
        new UserScramCredentialAlteration[] { new UserScramCredentialDeletion("u", ScramMechanism.SCRAM_SHA_256) };

    private static AbortTransactionSpec AbortSpec() => new AbortTransactionSpec(Partition(), 1L, 0, 0);

    private static AclBindingFilter UnknownFilter() =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.ANY), AccessControlEntryFilter.ANY);

    private static Dictionary<string, FeatureUpdate> FeatureUpdates() =>
        new Dictionary<string, FeatureUpdate> { ["feature"] = new FeatureUpdate(1, UpgradeType.UPGRADE) };

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = "127.0.0.1:1" };
}
