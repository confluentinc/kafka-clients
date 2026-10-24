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
/// <c>DescribeLogDirs</c>, <c>AlterReplicaLogDirs</c> and <c>DescribeReplicaLogDirs</c> end to end against the mocks,
/// with the renderings and <see cref="LogDirDescription"/>'s three public constructors.
/// </summary>
public sealed class PublicAdminLogDirsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "public-logdir-topic";

    private const string DefaultLogDir = "/tmp/kafka-logs";

    [Fact]
    public async Task DescribeLogDirs_ReportsABrokersDirectoriesAndReplicas()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 1) }).All().Get(), s_deadline);

        AsyncDescribeLogDirsResult result = admin.DescribeLogDirs(new[] { 0 });

        IReadOnlyDictionary<string, LogDirDescription> directories =
            await TestTimeout.Run(() => result.Descriptions[0].Get(), s_deadline);

        LogDirDescription description = Assert.Contains(DefaultLogDir, directories);
        Assert.Null(description.Error);
        Assert.Equal(2, description.ReplicaInfos.Count);
        Assert.Contains(new TopicPartition(Topic, 0), description.ReplicaInfos);
        Assert.Contains(new TopicPartition(Topic, 1), description.ReplicaInfos);

        IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>> all =
            await TestTimeout.Run(() => result.AllDescriptions().Get(), s_deadline);
        Assert.Equal(2, all[0][DefaultLogDir].ReplicaInfos.Count);
    }

    [Fact]
    public async Task DescribeLogDirs_CarriesTheCordonedFlag()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        AsyncDescribeLogDirsResult result = admin.DescribeLogDirs(new[] { 0 });

        IReadOnlyDictionary<string, LogDirDescription> directories =
            await TestTimeout.Run(() => result.Descriptions[0].Get(), s_deadline);

        LogDirDescription description = Assert.Contains(DefaultLogDir, directories);
        Assert.False(description.IsCordoned);
        Assert.Contains("isCordoned=false", description.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task DescribeLogDirs_ABrokerWithNoTopics_SucceedsWithAnEmptyMap()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeLogDirsResult result = admin.DescribeLogDirs(new[] { 0 });

        IReadOnlyDictionary<string, LogDirDescription> directories =
            await TestTimeout.Run(() => result.Descriptions[0].Get(), s_deadline);
        Assert.Empty(directories);
    }

    [Fact]
    public async Task AMoveIsAccepted_AndThenDescribedAsInProgress()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);

        AsyncAlterReplicaLogDirsResult moved = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = DefaultLogDir });

        await TestTimeout.Run(() => moved.Values[replica].Get(), s_deadline);
        await TestTimeout.Run(() => moved.All().Get(), s_deadline);

        AsyncDescribeReplicaLogDirsResult described = admin.DescribeReplicaLogDirs(new[] { replica });
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info =
            await TestTimeout.Run(() => described.Values[replica].Get(), s_deadline);

        Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());
        Assert.Equal(DefaultLogDir, info.GetFutureReplicaLogDir());
        Assert.Equal(0, info.GetCurrentReplicaOffsetLag());
        Assert.Equal(0, info.GetFutureReplicaOffsetLag());

        Assert.Equal(
            "(currentReplicaLogDir=/tmp/kafka-logs, futureReplicaLogDir=/tmp/kafka-logs, futureReplicaOffsetLag=0)",
            info.ToString());
    }

    [Fact]
    public async Task AReplicaAtRest_RendersWithTheAtRestShape()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);
        AsyncDescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(new[] { replica });

        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info =
            await TestTimeout.Run(() => result.Values[replica].Get(), s_deadline);

        Assert.Null(info.GetFutureReplicaLogDir());
        Assert.Equal("ReplicaLogDirInfo(currentReplicaLogDir=/tmp/kafka-logs)", info.ToString());

        IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> all =
            await TestTimeout.Run(() => result.All().Get(), s_deadline);
        Assert.Equal(DefaultLogDir, all[new TopicPartitionReplica(Topic, 0, 0)].GetCurrentReplicaLogDir());
    }

    [Fact]
    public async Task AMoveToAnUnknownDirectory_IsRejectedPerReplica()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);
        AsyncAlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = "/mnt/absent" });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[replica].Get()), s_deadline);
        Assert.Equal("Log directory /mnt/absent is offline", failure.Message);
    }

    [Fact]
    public async Task AMoveToAnUnknownBroker_IsRejectedNamingTheReplica()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 7);
        AsyncAlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = DefaultLogDir });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[replica].Get()), s_deadline);

        // The core renders the replica into this message, which is why ToString is topic-partition-brokerId.
        Assert.Equal($"Can't find {replica}", failure.Message);
        Assert.Equal($"Can't find {Topic}-0-7", failure.Message);
    }

    [Fact]
    public async Task AFreshlyBuiltKey_LooksUpTheSameEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        AsyncDescribeReplicaLogDirsResult result =
            admin.DescribeReplicaLogDirs(new[] { new TopicPartitionReplica(Topic, 0, 0) });

        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = await TestTimeout.Run(
            () => result.Values[new TopicPartitionReplica(Topic, 0, 0)].Get(), s_deadline);
        Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());

        AsyncAlterReplicaLogDirsResult moved = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string>
            {
                [new TopicPartitionReplica(Topic, 0, 0)] = DefaultLogDir,
            });
        await TestTimeout.Run(() => moved.Values[new TopicPartitionReplica(Topic, 0, 0)].Get(), s_deadline);
    }

    [Fact]
    public void TheRenderings_MatchJavas()
    {
        Assert.Equal("logdir-topic-4-9", new TopicPartitionReplica("logdir-topic", 4, 9).ToString());
        Assert.Equal("ReplicaInfo(size=12, offsetLag=3, isFuture=true)", new ReplicaInfo(12, 3, true).ToString());
        Assert.Equal("ReplicaInfo(size=12, offsetLag=3, isFuture=false)", new ReplicaInfo(12, 3, false).ToString());

        LogDirDescription empty = new LogDirDescription(
            error: null,
            new Dictionary<TopicPartition, ReplicaInfo>(),
            totalBytes: null,
            usableBytes: null,
            isCordoned: false);
        Assert.Equal(
            "LogDirDescription(replicaInfos={}, error=null, totalBytes=empty, usableBytes=empty, "
                + "isCordoned=false)",
            empty.ToString());

        LogDirDescription sized = new LogDirDescription(
            error: null,
            new Dictionary<TopicPartition, ReplicaInfo> { [new TopicPartition("t", 0)] = new ReplicaInfo(1, 2, false) },
            totalBytes: 100,
            usableBytes: 0,
            isCordoned: true);
        Assert.Equal(
            "LogDirDescription(replicaInfos={t-0=ReplicaInfo(size=1, offsetLag=2, isFuture=false)}, "
                + "error=null, totalBytes=100, usableBytes=0, isCordoned=true)",
            sized.ToString());
    }

    [Fact]
    public void EveryNewRpc_ThrowsAfterDispose()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);

        Assert.Throws<ObjectDisposedException>(() => admin.DescribeLogDirs(new[] { 0 }));
        Assert.Throws<ObjectDisposedException>(
            () => admin.AlterReplicaLogDirs(
                new Dictionary<TopicPartitionReplica, string> { [replica] = DefaultLogDir }));
        Assert.Throws<ObjectDisposedException>(() => admin.DescribeReplicaLogDirs(new[] { replica }));
    }

    [Fact]
    public void G3_8_LogDirDescription_HasJavasThreePublicConstructors()
    {
        Type[][] shapes = typeof(LogDirDescription).GetConstructors()
            .Select(ctor => ctor.GetParameters().Select(parameter => parameter.ParameterType).ToArray())
            .OrderBy(types => types.Length)
            .ToArray();

        Type replicaInfos = typeof(IReadOnlyDictionary<TopicPartition, ReplicaInfo>);
        Assert.Equal(3, shapes.Length);
        Assert.Equal(new[] { typeof(KafkaException), replicaInfos }, shapes[0]);
        Assert.Equal(new[] { typeof(KafkaException), replicaInfos, typeof(long?), typeof(long?) }, shapes[1]);
        Assert.Equal(
            new[] { typeof(KafkaException), replicaInfos, typeof(long?), typeof(long?), typeof(bool) },
            shapes[2]);
        Assert.Empty(typeof(LogDirDescription).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
    }

    [Fact]
    public void G3_8_LogDirDescription_ConstructorsStoreTheirArguments_AndMapJavasMinusOneToNull()
    {
        KafkaException error = new KafkaException("the log directory is offline");
        Dictionary<TopicPartition, ReplicaInfo> replicas = new Dictionary<TopicPartition, ReplicaInfo>
        {
            [new TopicPartition("t", 0)] = new ReplicaInfo(1, 2, false),
        };

        LogDirDescription two = new LogDirDescription(error, replicas);
        Assert.Same(error, two.Error);

        // A read-only view over the caller's map, not the map itself.
        Assert.Equal(replicas, two.ReplicaInfos);
        Assert.Null(two.TotalBytes);
        Assert.Null(two.UsableBytes);
        Assert.False(two.IsCordoned);

        LogDirDescription four = new LogDirDescription(null, replicas, 100, 0);
        Assert.Null(four.Error);
        Assert.Equal(100, four.TotalBytes);
        Assert.Equal(0, four.UsableBytes);
        Assert.False(four.IsCordoned);

        LogDirDescription javaUnknown = new LogDirDescription(null, replicas, -1, -1, true);
        Assert.Null(javaUnknown.TotalBytes);
        Assert.Null(javaUnknown.UsableBytes);
        Assert.True(javaUnknown.IsCordoned);

        LogDirDescription fourUnknown = new LogDirDescription(null, replicas, -1, 5);
        Assert.Null(fourUnknown.TotalBytes);
        Assert.Equal(5, fourUnknown.UsableBytes);

        LogDirDescription otherNegative = new LogDirDescription(null, replicas, -2, 7, false);
        Assert.Equal(-2, otherNegative.TotalBytes);
        Assert.Equal(7, otherNegative.UsableBytes);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(5)]
    public void G3_8_LogDirDescription_RejectsANullReplicaInfos(int arity)
    {
        Action construct = arity switch
        {
            2 => () => _ = new LogDirDescription(null, null!),
            4 => () => _ = new LogDirDescription(null, null!, 1, 1),
            _ => () => _ = new LogDirDescription(null, null!, 1, 1, false),
        };

        ArgumentNullException thrown = Assert.Throws<ArgumentNullException>(construct);
        Assert.Equal("replicaInfos", thrown.ParamName);
        Assert.StartsWith("Value cannot be null.", thrown.Message, StringComparison.Ordinal);
    }
}
