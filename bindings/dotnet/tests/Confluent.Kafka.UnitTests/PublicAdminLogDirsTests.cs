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
/// The public behaviour of M15/P3 Stage 3's three RPCs, driven end to end through
/// <see cref="MockAdminClient"/> — the real marshalling, the real bridge, the real
/// teardown, with no broker.
/// </summary>
public sealed class PublicAdminLogDirsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "public-logdir-topic";

    /// <summary>
    /// The path the mock seeds every broker with — Java's
    /// <c>MockAdminClient.DEFAULT_LOG_DIRS</c>.
    /// </summary>
    private const string DefaultLogDir = "/tmp/kafka-logs";

    /// <summary>
    /// <c>describeLogDirs</c> reports one broker's directories, keyed by <b>path</b>, with a
    /// <see cref="ReplicaInfo"/> per hosted partition.
    /// </summary>
    [Fact]
    public async Task DescribeLogDirs_ReportsABrokersDirectoriesAndReplicas()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 1) }).All(), s_deadline);

        DescribeLogDirsResult result = admin.DescribeLogDirs(new[] { 0 });

        IReadOnlyDictionary<string, LogDirDescription> directories =
            await TestTimeout.Run(() => result.Descriptions[0], s_deadline);

        LogDirDescription description = Assert.Contains(DefaultLogDir, directories);
        Assert.Null(description.Error);
        Assert.Equal(2, description.ReplicaInfos.Count);
        Assert.Contains(new TopicPartition(Topic, 0), description.ReplicaInfos);
        Assert.Contains(new TopicPartition(Topic, 1), description.ReplicaInfos);

        // …and AllDescriptions() gathers the same value under the same broker key.
        IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>> all =
            await TestTimeout.Run(result.AllDescriptions, s_deadline);
        Assert.Equal(2, all[0][DefaultLogDir].ReplicaInfos.Count);
    }

    /// <summary>
    /// ⚠ <b>A broker with nothing on it succeeds with an EMPTY map</b> — not a fault, and
    /// not a missing key. Java's result carries a future per requested broker either way.
    /// </summary>
    [Fact]
    public async Task DescribeLogDirs_ABrokerWithNoTopics_SucceedsWithAnEmptyMap()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeLogDirsResult result = admin.DescribeLogDirs(new[] { 0 });

        IReadOnlyDictionary<string, LogDirDescription> directories =
            await TestTimeout.Run(() => result.Descriptions[0], s_deadline);
        Assert.Empty(directories);
    }

    /// <summary>
    /// The full round trip: a move is accepted, and <c>describeReplicaLogDirs</c> then
    /// reports it as <b>in progress</b> — a current directory <em>and</em> a future one.
    /// </summary>
    /// <remarks>
    /// ⚠ Recording a move is what produces a non-null <c>GetFutureReplicaLogDir()</c> — the
    /// mock returns the stored <c>ReplicaLogDirInfo</c> only for a replica it has seen an
    /// alter for (<c>mock_admin_client.rs:1374-1379</c>) — and with it the <b>first</b> of
    /// Java's two <c>toString()</c> shapes. The at-rest shape is asserted separately, by
    /// <see cref="AReplicaAtRest_RendersWithTheAtRestShape"/>.
    /// </remarks>
    [Fact]
    public async Task AMoveIsAccepted_AndThenDescribedAsInProgress()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);

        AlterReplicaLogDirsResult moved = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = DefaultLogDir });

        // Result shape 2: success carries no value, so awaiting is the whole assertion.
        await TestTimeout.Run(() => moved.Values[replica], s_deadline);
        await TestTimeout.Run(moved.All, s_deadline);

        DescribeReplicaLogDirsResult described = admin.DescribeReplicaLogDirs(new[] { replica });
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info =
            await TestTimeout.Run(() => described.Values[replica], s_deadline);

        Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());
        Assert.Equal(DefaultLogDir, info.GetFutureReplicaLogDir());
        Assert.Equal(0, info.GetCurrentReplicaOffsetLag());
        Assert.Equal(0, info.GetFutureReplicaOffsetLag());

        // Java's move-in-progress rendering (DescribeReplicaLogDirsResult.java:117-121).
        Assert.Equal(
            "(currentReplicaLogDir=/tmp/kafka-logs, futureReplicaLogDir=/tmp/kafka-logs, futureReplicaOffsetLag=0)",
            info.ToString());
    }

    /// <summary>
    /// A replica at rest renders with Java's <b>other</b> <c>toString()</c> shape — the one
    /// that names the type and omits the future columns entirely.
    /// </summary>
    [Fact]
    public async Task AReplicaAtRest_RendersWithTheAtRestShape()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);
        DescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(new[] { replica });

        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info =
            await TestTimeout.Run(() => result.Values[replica], s_deadline);

        Assert.Null(info.GetFutureReplicaLogDir());
        Assert.Equal("ReplicaLogDirInfo(currentReplicaLogDir=/tmp/kafka-logs)", info.ToString());

        // …and All() gathers it under the same key, which only holds because the aggregate
        // is built with the bridge's own comparer over a reference type with value equality.
        IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> all =
            await TestTimeout.Run(result.All, s_deadline);
        Assert.Equal(DefaultLogDir, all[new TopicPartitionReplica(Topic, 0, 0)].GetCurrentReplicaLogDir());
    }

    /// <summary>
    /// A move to a directory the broker does not have is rejected <b>per replica</b>, with
    /// the broker's own message (<c>definition-of-done.md</c> §3).
    /// </summary>
    [Fact]
    public async Task AMoveToAnUnknownDirectory_IsRejectedPerReplica()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);
        AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = "/mnt/absent" });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[replica]), s_deadline);
        Assert.Equal("Log directory /mnt/absent is offline", failure.Message);
    }

    /// <summary>
    /// A move naming a broker that does not exist is rejected with a message carrying the
    /// replica's <see cref="TopicPartitionReplica.ToString"/> rendering — which is how the
    /// C# and Rust renderings are pinned to each other.
    /// </summary>
    [Fact]
    public async Task AMoveToAnUnknownBroker_IsRejectedNamingTheReplica()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 7);
        AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = DefaultLogDir });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[replica]), s_deadline);

        // ⚠ The core builds this message from its OWN Display for TopicPartitionReplica
        // (topic_partition_replica.rs:53), so an equal rendering here is evidence the two
        // agree — the reason the C# ToString is `topic-partition-brokerId` and not a
        // prettier shape.
        Assert.Equal($"Can't find {replica}", failure.Message);
        Assert.Equal($"Can't find {Topic}-0-7", failure.Message);
    }

    /// <summary>
    /// ⚠ <b><see cref="TopicPartitionReplica"/>'s value equality is what makes both result
    /// maps usable.</b> A caller looks a key up with a fresh instance, not the one it
    /// submitted.
    /// </summary>
    [Fact]
    public async Task AFreshlyBuiltKey_LooksUpTheSameEntry()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        DescribeReplicaLogDirsResult result =
            admin.DescribeReplicaLogDirs(new[] { new TopicPartitionReplica(Topic, 0, 0) });

        // A different instance with equal fields, exactly as a caller would write it.
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = await TestTimeout.Run(
            () => result.Values[new TopicPartitionReplica(Topic, 0, 0)], s_deadline);
        Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());

        AlterReplicaLogDirsResult moved = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string>
            {
                [new TopicPartitionReplica(Topic, 0, 0)] = DefaultLogDir,
            });
        await TestTimeout.Run(() => moved.Values[new TopicPartitionReplica(Topic, 0, 0)], s_deadline);
    }

    /// <summary>
    /// The three diagnostic renderings match Java's, including
    /// <see cref="LogDirDescription"/>'s empty <c>OptionalLong</c>s.
    /// </summary>
    /// <remarks>
    /// ⚠ Java's <c>LogDirDescription.toString()</c> ends with <c>, isCordoned=…)</c>
    /// (<c>LogDirDescription.java:105</c>). It is omitted here for the same reason the
    /// accessor is (decision D15, §15 Gap 1): rendering a value the binding cannot know
    /// would be a fabrication in diagnostic output, which is exactly where a reader would
    /// trust it.
    /// </remarks>
    [Fact]
    public void TheRenderings_MatchJavas()
    {
        Assert.Equal("logdir-topic-4-9", new TopicPartitionReplica("logdir-topic", 4, 9).ToString());
        Assert.Equal("ReplicaInfo(size=12, offsetLag=3, isFuture=True)", new ReplicaInfo(12, 3, true).ToString());

        LogDirDescription empty = new LogDirDescription(
            error: null,
            new Dictionary<TopicPartition, ReplicaInfo>(),
            totalBytes: null,
            usableBytes: null);
        Assert.Equal(
            "LogDirDescription(replicaInfos={}, error=null, totalBytes=empty, usableBytes=empty)",
            empty.ToString());

        LogDirDescription sized = new LogDirDescription(
            error: null,
            new Dictionary<TopicPartition, ReplicaInfo> { [new TopicPartition("t", 0)] = new ReplicaInfo(1, 2, false) },
            totalBytes: 100,
            usableBytes: 0);
        Assert.Equal(
            "LogDirDescription(replicaInfos={t-0=ReplicaInfo(size=1, offsetLag=2, isFuture=False)}, "
                + "error=null, totalBytes=100, usableBytes=0)",
            sized.ToString());
    }

    /// <summary>
    /// Every new RPC rejects use after <see cref="MockAdminClient.Dispose"/> with
    /// <see cref="ObjectDisposedException"/>, before any native call (ffi §B5).
    /// </summary>
    [Fact]
    public void EveryNewRpc_ThrowsAfterDispose()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);

        Assert.Throws<ObjectDisposedException>(() => admin.DescribeLogDirs(new[] { 0 }));
        Assert.Throws<ObjectDisposedException>(
            () => admin.AlterReplicaLogDirs(
                new Dictionary<TopicPartitionReplica, string> { [replica] = DefaultLogDir }));
        Assert.Throws<ObjectDisposedException>(() => admin.DescribeReplicaLogDirs(new[] { replica }));
    }
}
