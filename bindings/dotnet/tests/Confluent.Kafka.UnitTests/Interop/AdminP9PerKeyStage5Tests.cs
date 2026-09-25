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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P9 CP6 — the ten <b>shape 4b</b> (void) RPCs, where a null per-key error <em>is</em>
/// that key's success and there is no result root at all. Covers the three key sources the
/// batch introduces beyond a plain string: a base64 <see cref="Uuid"/>, two composite scalar
/// tuples, and two <b>owned handle</b> keys the callback must destroy.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Every assertion is on the mock's OWN code and message.</b> A key the trampoline
/// reconstructs wrongly is not left pending — the core faults it with a synthesized
/// "not present in the response" error instead — so <c>ThrowsAsync&lt;KafkaException&gt;</c>
/// alone proves nothing here. Pairing distinct keys whose expected messages <em>differ</em>
/// is what rules out a crossed or collapsed key.
/// </para>
/// <para>
/// ⚠ <c>alterClientQuotas</c>' expected message is <c>"Not implement yet"</c>, not
/// <c>"Not implemented yet"</c> — a typo in the core's mock
/// (<c>mock_admin_client.rs:1766</c>), asserted verbatim because the message is the
/// contract, not the intent.
/// </para>
/// </remarks>
public sealed class AdminP9PerKeyStage5Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code the mock reports for every unimplemented RPC.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>The code Kafka assigns to <c>UNKNOWN_TOPIC_OR_PARTITION</c>.</summary>
    private const int UnknownTopicOrPartitionCode = 3;

    private const int KafkaStorageErrorCode = 56;

    private const string NotImplemented = "Not implemented yet";

    // ------------------------------------------------------------------------------------
    // Owned-handle keys — createAcls and alterClientQuotas.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠ <b>The owned <c>kafka_common_AclBinding_t</c> key.</b> Three bindings differing only
    /// in resource name each fault under their own key, which is only possible if the
    /// trampoline copied the handle out into an equal <see cref="AclBinding"/> before
    /// destroying it.
    /// </summary>
    [Fact]
    public async Task CreateAcls_EveryBindingFaultsUnderItsOwnOwnedKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AclBinding[] bindings = { Binding("cp6-alpha"), Binding("cp6-beta"), Binding("cp6-gamma") };

        CreateAclsResult result = admin.CreateAcls(bindings, options: null);

        Assert.Equal(bindings.Length, result.Values.Count);
        foreach (AclBinding binding in bindings)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[binding], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// ⚠ <b>The owned <c>kafka_common_ClientQuotaEntity_t</c> key</b>, whose reader has to
    /// rebuild a whole entity map — and whose expected message carries the core's typo.
    /// </summary>
    [Fact]
    public async Task AlterClientQuotas_EveryEntityFaultsUnderItsOwnOwnedKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        ClientQuotaEntity alpha = Entity("cp6-quota-alpha");
        ClientQuotaEntity beta = Entity("cp6-quota-beta");

        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration(alpha), Alteration(beta) }, options: null);

        Assert.Equal(2, result.Values.Count);
        foreach (ClientQuotaEntity entity in new[] { alpha, beta })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[entity], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);

            // ⚠ The core's own wording, typo included (mock_admin_client.rs:1766).
            Assert.Equal("Not implement yet", failure.Message);
        }
    }

    /// <summary>
    /// Many owned keys, over many operations of <b>both</b> owned-key RPCs, leave the
    /// reference count <b>balanced</b> — an over-release would throw out of the
    /// <see cref="System.Runtime.InteropServices.SafeHandle"/>, an under-release leaves
    /// <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public async Task OwnedKeyRpcs_OverManyOperations_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int round = 0; round < 3; round++)
        {
            AclBinding[] bindings = new AclBinding[8];
            ClientQuotaAlteration[] quotas = new ClientQuotaAlteration[8];
            for (int i = 0; i < bindings.Length; i++)
            {
                bindings[i] = Binding($"cp6-balance-{round}-{i}");
                quotas[i] = Alteration(Entity($"cp6-quota-balance-{round}-{i}"));
            }

            CreateAclsResult acls = admin.CreateAcls(bindings, options: null);
            AlterClientQuotasResult altered = admin.AlterClientQuotas(quotas, options: null);

            Assert.Equal(bindings.Length, acls.Values.Count);
            Assert.Equal(quotas.Length, altered.Values.Count);

            foreach (Task awaitable in acls.Values.Values)
            {
                await TestTimeout.Run(() => Settle(awaitable), s_deadline);
            }

            foreach (Task awaitable in altered.Values.Values)
            {
                await TestTimeout.Run(() => Settle(awaitable), s_deadline);
            }

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "48 owned-key callbacks must leave the count balanced");
    }

    // ------------------------------------------------------------------------------------
    // Uuid and composite scalar keys.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠ <b>The base64 <see cref="Uuid"/> key.</b> The ABI hands the callback the id's
    /// <em>text</em>, which the trampoline parses back — so two ids fault with messages
    /// naming their <em>own</em> id, which a crossed or truncated parse could not produce.
    /// </summary>
    [Fact]
    public async Task DeleteTopicsByIds_EachIdFaultsUnderItsOwnParsedKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Uuid first = new Uuid(11L, 22L);
        Uuid second = new Uuid(33L, 44L);
        Assert.NotEqual(first, second);

        DeleteTopicsResult result = admin.DeleteTopics(
            TopicCollection.OfTopicIds(new[] { first, second }), options: null);

        IReadOnlyDictionary<Uuid, Task> values = result.TopicIdValues!;
        Assert.Equal(2, values.Count);

        foreach (Uuid id in new[] { first, second })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => values[id], s_deadline));
            Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
            Assert.Equal($"Topic {id} does not exist.", failure.Message);
        }
    }

    /// <summary>
    /// ⚠ <b>The three-scalar <c>(topic, partition, brokerId)</c> key.</b> Two replicas of the
    /// same topic differing only in partition take different log directories, so their
    /// messages differ — which a key built from the topic alone could not distinguish.
    /// </summary>
    [Fact]
    public async Task AlterReplicaLogDirs_EachReplicaFaultsUnderItsOwnThreeScalarKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp6-logdirs", 2, 1) }, options: null).All(),
            s_deadline);

        TopicPartitionReplica zero = new TopicPartitionReplica("cp6-logdirs", 0, 0);
        TopicPartitionReplica one = new TopicPartitionReplica("cp6-logdirs", 1, 0);

        AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string>
            {
                [zero] = "/cp6-offline-zero",
                [one] = "/cp6-offline-one",
            },
            options: null);

        Assert.Equal(2, result.Values.Count);

        KafkaException zeroFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[zero], s_deadline));
        Assert.Equal(KafkaStorageErrorCode, zeroFailure.Code);
        Assert.Equal("Log directory /cp6-offline-zero is offline", zeroFailure.Message);

        KafkaException oneFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[one], s_deadline));
        Assert.Equal("Log directory /cp6-offline-one is offline", oneFailure.Message);
    }

    /// <summary>
    /// ⚠ <b>The two-scalar <c>(topic, partition)</c> key, with a MIXED outcome.</b> One
    /// partition succeeds and the other faults on the same call — the shape 4b property that
    /// a null error is a success, asserted in both directions at once.
    /// </summary>
    [Fact]
    public async Task AlterPartitionReassignments_OnePartitionSucceeds_WhileAnotherFaults()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp6-reassign", 1, 1) }, options: null).All(),
            s_deadline);

        TopicPartition inRange = new TopicPartition("cp6-reassign", 0);
        TopicPartition outOfRange = new TopicPartition("cp6-reassign", 7);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [inRange] = new NewPartitionReassignment(new[] { 0 }),
                [outOfRange] = new NewPartitionReassignment(new[] { 0 }),
            },
            options: null);

        await TestTimeout.Run(() => result.Values[inRange], s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[outOfRange], s_deadline));
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
    }

    // ------------------------------------------------------------------------------------
    // String keys, plus the two key counts that are NOT the row count.
    // ------------------------------------------------------------------------------------

    /// <summary>Each topic and each group faults under its own string key.</summary>
    [Fact]
    public async Task CreatePartitionsAndDeleteConsumerGroups_EachStringKeyFaultsIndependently()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CreatePartitionsResult partitions = admin.CreatePartitions(
            new Dictionary<string, NewPartitions>
            {
                ["cp6-parts-alpha"] = NewPartitions.IncreaseTo(3),
                ["cp6-parts-beta"] = NewPartitions.IncreaseTo(4),
            },
            options: null);

        Assert.Equal(2, partitions.Values.Count);
        foreach (string topic in new[] { "cp6-parts-alpha", "cp6-parts-beta" })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => partitions.Values[topic], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }

        DeleteConsumerGroupsResult groups =
            admin.DeleteConsumerGroups(new[] { "cp6-group-alpha", "cp6-group-beta" }, options: null);

        Assert.Equal(2, groups.DeletedGroups.Count);
        foreach (string group in new[] { "cp6-group-alpha", "cp6-group-beta" })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => groups.DeletedGroups[group], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// ⚠⚠ <b>The one key count that is neither the row count nor the map size.</b>
    /// <c>alterUserScramCredentials</c> sends one row per alteration but the ABI fires once
    /// per <b>distinct user</b> (<c>h:8875-8886</c>), so three rows over two users must
    /// settle both keys — no more, no fewer.
    /// </summary>
    /// <remarks>
    /// An <c>n</c> taken from the row count would leave the countdown one short: both keys
    /// would still fault (the callbacks arrive), but the operation would never reach
    /// countdown zero, so its <c>GCHandle</c> and span-the-op reference would never be
    /// released — which is why <c>Dispose</c> is asserted here rather than only the faults.
    /// </remarks>
    [Fact]
    public async Task AlterUserScramCredentials_ThreeRowsOverTwoUsers_SettleTwoKeys_AndRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        AlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(
            new UserScramCredentialAlteration[]
            {
                new UserScramCredentialDeletion("cp6-user-a", ScramMechanism.ScramSha256),
                new UserScramCredentialDeletion("cp6-user-a", ScramMechanism.ScramSha512),
                new UserScramCredentialDeletion("cp6-user-b", ScramMechanism.ScramSha256),
            },
            options: null);

        Assert.Equal(2, result.Values.Count);
        foreach (string user in new[] { "cp6-user-a", "cp6-user-b" })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[user], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "n must be the DISTINCT-user count: a row-count n never reaches countdown zero");
    }

    /// <summary>
    /// The <c>n == 0</c> rule across shape 4b: an empty input fires <b>no</b> callback, so
    /// only the submit's own countdown slot can release the operation — and the release must
    /// happen for every one of these RPCs, not just the first.
    /// </summary>
    [Fact]
    public void EmptyInputs_ResolveAtTheSubmitBoundary_AndReleaseTheClient()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Empty(admin.CreateAcls(Array.Empty<AclBinding>(), options: null).Values);
        Assert.Empty(admin.AlterClientQuotas(Array.Empty<ClientQuotaAlteration>(), options: null).Values);
        Assert.Empty(
            admin.CreatePartitions(new Dictionary<string, NewPartitions>(), options: null).Values);
        Assert.Empty(admin.DeleteConsumerGroups(Array.Empty<string>(), options: null).DeletedGroups);
        Assert.Empty(
            admin.AlterUserScramCredentials(Array.Empty<UserScramCredentialAlteration>(), options: null)
                .Values);
        Assert.Empty(
            admin.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string>(), options: null)
                .Values);
        Assert.Empty(
            admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>(), options: null).Values);
        Assert.Empty(
            admin.DeleteTopics(TopicCollection.OfTopicIds(Array.Empty<Uuid>()), options: null)
                .TopicIdValues!);

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(
            handle.IsClosed,
            "zero callbacks must still release: the submit token is the only thing that can");
    }

    // ------------------------------------------------------------------------------------
    // Harness.
    // ------------------------------------------------------------------------------------

    /// <summary>Awaits one key's outcome, treating a fault as a settlement.</summary>
    private static async Task Settle(Task awaitable)
    {
        try
        {
            await awaitable.ConfigureAwait(false);
        }
        catch (KafkaException)
        {
        }
    }

    /// <summary>A binding differing only in resource name.</summary>
    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    /// <summary>A client-id entity differing only in name.</summary>
    private static ClientQuotaEntity Entity(string name) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.ClientId] = name });

    private static ClientQuotaAlteration Alteration(ClientQuotaEntity entity) =>
        new ClientQuotaAlteration(
            entity,
            new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0) });
}
