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
/// M15/P13.3 (c), decision D9 — every string that makes up a per-key admin key is rejected,
/// synchronously and with <see cref="ArgumentException"/>, when it contains a NUL or an
/// unpaired surrogate: the C ABI would receive it truncated (<c>"a\0b"</c> → <c>"a"</c>) or
/// altered (a lone surrogate → U+FFFD), so two caller keys could reach the core as one, and
/// since round 70 the core answers one callback per <em>distinct</em> key.
/// </summary>
/// <remarks>
/// One row per guarded string, over the public surface, each asserting the one shared message
/// and the parameter it blames. The rows are the commit's guarded-string list; a string added
/// to a per-key key without a row here is the gap this table is meant to make visible.
/// </remarks>
public sealed class PublicAdminKeyStringTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The message every rejection carries (asserted verbatim, DoD §3).</summary>
    private const string InvalidKeyMessage =
        "An admin request key must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    /// <summary>
    /// The two kinds of string the ABI cannot carry unchanged: an embedded NUL, which ends the
    /// C string, and a lone high surrogate, which UTF-8 encodes as U+FFFD.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> s_badStrings =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["nul"] = "key\0tail",
            ["lone-surrogate"] = "key\uD800tail",
        };

    /// <summary>
    /// Every guarded string: the RPC, the string's role in the key, the parameter the
    /// rejection must blame, and the call that puts <c>bad</c> in that role.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, (string Parameter, Action<MockAdminClient, string> Call)> s_sites =
        new Dictionary<string, (string, Action<MockAdminClient, string>)>(StringComparer.Ordinal)
        {
            ["createTopics:topic"] = ("newTopics", (admin, bad) =>
                admin.CreateTopics(new[] { new NewTopic(bad, 1, 1) })),
            ["deleteTopics:topic"] = ("topics", (admin, bad) =>
                admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { bad }))),
            ["describeTopics:topic"] = ("topics", (admin, bad) =>
                admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { bad }))),
            ["createPartitions:topic"] = ("newPartitions", (admin, bad) =>
                admin.CreatePartitions(new Dictionary<string, NewPartitions> { [bad] = NewPartitions.IncreaseTo(2) })),
            ["deleteRecords:topic"] = ("recordsToDelete", (admin, bad) =>
                admin.DeleteRecords(new Dictionary<TopicPartition, RecordsToDelete>
                {
                    [new TopicPartition(bad, 0)] = RecordsToDelete.BeforeOffset(1),
                })),
            ["describeConfigs:resourceName"] = ("resources", (admin, bad) =>
                admin.DescribeConfigs(new[] { new ConfigResource(ConfigResourceType.Topic, bad) })),
            ["incrementalAlterConfigs:resourceName"] = ("configs", (admin, bad) =>
                admin.IncrementalAlterConfigs(new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [new ConfigResource(ConfigResourceType.Topic, bad)] =
                        new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
                })),
            ["alterReplicaLogDirs:topic"] = ("replicaAssignment", (admin, bad) =>
                admin.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string>
                {
                    [new TopicPartitionReplica(bad, 0, 0)] = "/tmp/dir",
                })),
            ["describeReplicaLogDirs:topic"] = ("replicas", (admin, bad) =>
                admin.DescribeReplicaLogDirs(new[] { new TopicPartitionReplica(bad, 0, 0) })),
            ["alterPartitionReassignments:topic"] = ("reassignments", (admin, bad) =>
                admin.AlterPartitionReassignments(new Dictionary<TopicPartition, NewPartitionReassignment?>
                {
                    [new TopicPartition(bad, 0)] = null,
                })),
            ["listOffsets:topic"] = ("topicPartitionOffsets", (admin, bad) =>
                admin.ListOffsets(new Dictionary<TopicPartition, OffsetSpec>
                {
                    [new TopicPartition(bad, 0)] = OffsetSpec.Latest(),
                })),
            ["describeConsumerGroups:groupId"] = ("groupIds", (admin, bad) =>
                admin.DescribeConsumerGroups(new[] { bad })),
            ["describeClassicGroups:groupId"] = ("groupIds", (admin, bad) =>
                admin.DescribeClassicGroups(new[] { bad })),
            ["listConsumerGroupOffsets(string):groupId"] = ("groupId", (admin, bad) =>
                admin.ListConsumerGroupOffsets(bad)),
            ["listConsumerGroupOffsets(map):groupId"] = ("groupSpecs", (admin, bad) =>
                admin.ListConsumerGroupOffsets(new Dictionary<string, ListConsumerGroupOffsetsSpec>
                {
                    [bad] = new ListConsumerGroupOffsetsSpec(),
                })),
            ["deleteConsumerGroups:groupId"] = ("groupIds", (admin, bad) =>
                admin.DeleteConsumerGroups(new[] { bad })),
            ["createAcls:resourceName"] = ("acls", (admin, bad) =>
                admin.CreateAcls(new[] { Binding(bad, "User:alice", "*") })),
            ["createAcls:principal"] = ("acls", (admin, bad) =>
                admin.CreateAcls(new[] { Binding("topic", bad, "*") })),
            ["createAcls:host"] = ("acls", (admin, bad) =>
                admin.CreateAcls(new[] { Binding("topic", "User:alice", bad) })),
            ["deleteAcls:resourceName"] = ("filters", (admin, bad) =>
                admin.DeleteAcls(new[]
                {
                    new AclBindingFilter(
                        new ResourcePatternFilter(ResourceType.Topic, bad, PatternType.Literal),
                        AccessControlEntryFilter.Any),
                })),
            ["deleteAcls:principal"] = ("filters", (admin, bad) =>
                admin.DeleteAcls(new[]
                {
                    new AclBindingFilter(
                        ResourcePatternFilter.Any,
                        new AccessControlEntryFilter(bad, null, AclOperation.Any, AclPermissionType.Any)),
                })),
            ["deleteAcls:host"] = ("filters", (admin, bad) =>
                admin.DeleteAcls(new[]
                {
                    new AclBindingFilter(
                        ResourcePatternFilter.Any,
                        new AccessControlEntryFilter(null, bad, AclOperation.Any, AclPermissionType.Any)),
                })),
            ["alterClientQuotas:entityType"] = ("entries", (admin, bad) =>
                admin.AlterClientQuotas(new[] { Quota(bad, "alice") })),
            ["alterClientQuotas:entityName"] = ("entries", (admin, bad) =>
                admin.AlterClientQuotas(new[] { Quota(ClientQuotaEntity.User, bad) })),
            ["alterUserScramCredentials:user"] = ("alterations", (admin, bad) =>
                admin.AlterUserScramCredentials(new[] { new UserScramCredentialDeletion(bad, ScramMechanism.ScramSha256) })),
            ["updateFeatures:featureName"] = ("featureUpdates", (admin, bad) =>
                admin.UpdateFeatures(new Dictionary<string, FeatureUpdate>
                {
                    [bad] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                })),
            ["fenceProducers:transactionalId"] = ("transactionalIds", (admin, bad) =>
                admin.FenceProducers(new[] { bad })),
            ["describeTransactions:transactionalId"] = ("transactionalIds", (admin, bad) =>
                admin.DescribeTransactions(new[] { bad })),
            ["describeProducers:topic"] = ("partitions", (admin, bad) =>
                admin.DescribeProducers(new[] { new TopicPartition(bad, 0) })),
        };

    /// <summary>Every (site, bad string) pair — the cross product of the two tables above.</summary>
    public static IEnumerable<object[]> Cases() =>
        from site in s_sites.Keys
        from kind in s_badStrings.Keys
        select new object[] { site, kind };

    /// <summary>
    /// The row count, pinned: 29 guarded strings over the 23 per-key RPCs that carry a string
    /// key (<c>describeLogDirs</c>, keyed by broker id, has none). A row deleted from the
    /// table would otherwise shrink the proof silently.
    /// </summary>
    [Fact]
    public void EveryGuardedStringHasARow()
    {
        Assert.Equal(29, s_sites.Count);
        Assert.Equal(
            23,
            s_sites.Keys.Select(site => site.Substring(0, site.IndexOf(':')))
                .Select(rpc => rpc.StartsWith("listConsumerGroupOffsets", StringComparison.Ordinal)
                    ? "listConsumerGroupOffsets"
                    : rpc)
                .Distinct(StringComparer.Ordinal)
                .Count());
    }

    /// <summary>
    /// Each guarded string, holding each bad kind, is rejected synchronously with the shared
    /// message and the caller's parameter name — before any result exists to await.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public void AKeyStringTheAbiWouldChange_IsRejectedSynchronously(string site, string kind)
    {
        using MockAdminClient admin = new MockAdminClient(1);
        (string parameter, Action<MockAdminClient, string> call) = s_sites[site];

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => call(admin, s_badStrings[kind]));

        Assert.Equal(parameter, rejected.ParamName);
        Assert.StartsWith(InvalidKeyMessage, rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The one guard outside the shared native path: the real client's single-group
    /// <c>listConsumerGroupOffsets</c> overload blames its own <c>groupId</c> parameter, not
    /// the batched form's <c>groupSpecs</c> it delegates to. Nothing is sent, so no broker is
    /// needed.
    /// </summary>
    [Theory]
    [InlineData("nul")]
    [InlineData("lone-surrogate")]
    public async Task RealClient_SingleGroupListConsumerGroupOffsets_BlamesGroupId(string kind)
    {
        await using KafkaAdminClient admin =
            new KafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        ArgumentException rejected =
            Assert.Throws<ArgumentException>(() => admin.ListConsumerGroupOffsets(s_badStrings[kind]));

        Assert.Equal("groupId", rejected.ParamName);
        Assert.StartsWith(InvalidKeyMessage, rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The scenario the guard exists for: two different caller keys that the ABI would deliver
    /// as one (<c>"a\0b"</c> and <c>"a\0c"</c> both reach it as <c>"a"</c>). Rejected, rather
    /// than answered once for two awaitables. The client stays usable afterwards: nothing was
    /// left half-submitted.
    /// </summary>
    [Fact]
    public async Task TwoKeysThatWouldCollapseAtTheAbi_AreRejected_AndTheClientStaysUsable()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => admin.CreateTopics(new[] { new NewTopic("a\0b", 1, 1), new NewTopic("a\0c", 1, 1) }));
        Assert.Equal("newTopics", rejected.ParamName);
        Assert.StartsWith(InvalidKeyMessage, rejected.Message, StringComparison.Ordinal);

        // Nothing named "a" was created by the rejected call.
        DescribeTopicsResult described = admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "a" }));
        KafkaException absent = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => described.TopicNameValues!["a"], s_deadline));
        Assert.Equal("Topic a not found.", absent.Message);

        CreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic("after-rejection", 1, 1) });
        await TestTimeout.Run(created.All, s_deadline);
    }

    /// <summary>
    /// The guard is not over-broad: non-ASCII text and a <b>well-formed</b> surrogate pair
    /// cross the ABI unchanged, so they are accepted and round-trip as the same key.
    /// </summary>
    [Theory]
    [InlineData("délété")]
    [InlineData("topic-🎈")]
    [InlineData("🎈")]
    [InlineData("日本語")]
    public async Task AKeyStringThatCrossesUnchanged_IsAccepted(string name)
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic(name, 1, 1) });
        Assert.Equal(new[] { name }, created.Values.Keys);
        await TestTimeout.Run(created.All, s_deadline);

        ListTopicsResult listed = admin.ListTopics();
        IReadOnlyCollection<string> names = Array.Empty<string>();
        await TestTimeout.Run(async () => names = await listed.Names(), s_deadline);
        Assert.Contains(name, names);
    }

    /// <summary>
    /// A null ACL filter component means "any" and is not a key string the ABI would change,
    /// so the guard lets it through — and a quota entity's null name (the default entity)
    /// likewise.
    /// </summary>
    [Fact]
    public void NullWildcards_AreNotRejected()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        _ = admin.DeleteAcls(new[] { new AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any) });
        _ = admin.AlterClientQuotas(new[] { Quota(ClientQuotaEntity.User, null) });
    }

    private static AclBinding Binding(string resourceName, string principal, string host) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, resourceName, PatternType.Literal),
            new AccessControlEntry(principal, host, AclOperation.Read, AclPermissionType.Allow));

    private static ClientQuotaAlteration Quota(string entityType, string? entityName) =>
        new ClientQuotaAlteration(
            new ClientQuotaEntity(new Dictionary<string, string?> { [entityType] = entityName }),
            new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1024) });
}
