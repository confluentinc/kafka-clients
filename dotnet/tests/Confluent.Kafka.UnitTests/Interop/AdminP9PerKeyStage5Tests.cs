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
/// The void keyed RPCs over the core mock, where a key's future completing is that key's success: each key settles
/// under its own key, for the keys beyond a plain string (a <see cref="Uuid"/>, a topic partition, a replica, an ACL
/// binding and a client-quota entity).
/// </summary>
/// <remarks>
/// Every assertion is on the core mock's own code and message, and distinct keys expect distinct messages, so a key
/// read back wrongly fails the test.
/// </remarks>
public sealed class AdminP9PerKeyStage5Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnsupportedVersionCode = 35;

    private const int UnknownTopicOrPartitionCode = 3;

    private const int KafkaStorageErrorCode = 56;

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task CreateAcls_EveryBindingFaultsUnderItsOwnOwnedKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBinding[] bindings = { Binding("cp6-alpha"), Binding("cp6-beta"), Binding("cp6-gamma") };

        AsyncCreateAclsResult result = AsyncCreateAclsResult.OfClient(admin.CreateAcls(bindings, options: null));

        Assert.Equal(bindings.Length, result.Values.Count);
        foreach (AclBinding binding in bindings)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[binding].Get(), s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    [Fact]
    public async Task AlterClientQuotas_EveryEntityFaultsUnderItsOwnOwnedKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ClientQuotaEntity alpha = Entity("cp6-quota-alpha");
        ClientQuotaEntity beta = Entity("cp6-quota-beta");

        AsyncAlterClientQuotasResult result = AsyncAlterClientQuotasResult.OfClient(admin.AlterClientQuotas(
            new[] { Alteration(alpha), Alteration(beta) }, options: null));

        Assert.Equal(2, result.Values.Count);
        foreach (ClientQuotaEntity entity in new[] { alpha, beta })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[entity].Get(), s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);

            // The core mock's own wording, typo included.
            Assert.Equal("Not implement yet", failure.Message);
        }
    }

    [Fact]
    public async Task DeleteTopicsByIds_EachIdFaultsUnderItsOwnParsedKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Uuid first = new Uuid(11L, 22L);
        Uuid second = new Uuid(33L, 44L);
        Assert.NotEqual(first, second);

        AsyncDeleteTopicsResult result = AsyncDeleteTopicsResult.OfClient(admin.DeleteTopics(
            TopicCollection.OfTopicIds(new[] { first, second }), options: null));

        IReadOnlyDictionary<Uuid, AsyncKafkaFuture> values = result.TopicIdValues!;
        Assert.Equal(2, values.Count);

        foreach (Uuid id in new[] { first, second })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => values[id].Get(), s_deadline));
            Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
            Assert.Equal($"Topic {id} does not exist.", failure.Message);
        }
    }

    [Fact]
    public async Task AlterReplicaLogDirs_EachReplicaFaultsUnderItsOwnThreeScalarKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("cp6-logdirs", 2, 1) }, null)).All().Get(),
            s_deadline);

        TopicPartitionReplica zero = new TopicPartitionReplica("cp6-logdirs", 0, 0);
        TopicPartitionReplica one = new TopicPartitionReplica("cp6-logdirs", 1, 0);

        AsyncAlterReplicaLogDirsResult result = AsyncAlterReplicaLogDirsResult.OfClient(admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string>
            {
                [zero] = "/cp6-offline-zero",
                [one] = "/cp6-offline-one",
            },
            options: null));

        Assert.Equal(2, result.Values.Count);

        KafkaException zeroFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[zero].Get(), s_deadline));
        Assert.Equal(KafkaStorageErrorCode, zeroFailure.Code);
        Assert.Equal("Log directory /cp6-offline-zero is offline", zeroFailure.Message);

        KafkaException oneFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[one].Get(), s_deadline));
        Assert.Equal("Log directory /cp6-offline-one is offline", oneFailure.Message);
    }

    [Fact]
    public async Task AlterPartitionReassignments_OnePartitionSucceeds_WhileAnotherFaults()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("cp6-reassign", 1, 1) }, null)).All().Get(),
            s_deadline);

        TopicPartition inRange = new TopicPartition("cp6-reassign", 0);
        TopicPartition outOfRange = new TopicPartition("cp6-reassign", 7);

        AsyncAlterPartitionReassignmentsResult result = AsyncAlterPartitionReassignmentsResult.OfClient(admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [inRange] = new NewPartitionReassignment(new[] { 0 }),
                [outOfRange] = new NewPartitionReassignment(new[] { 0 }),
            },
            options: null));

        await TestTimeout.Run(() => result.Values[inRange].Get(), s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[outOfRange].Get(), s_deadline));
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
    }

    [Fact]
    public async Task DeleteConsumerGroups_EachStringKeyFaultsIndependently()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDeleteConsumerGroupsResult groups =
            AsyncDeleteConsumerGroupsResult.OfClient(admin.DeleteConsumerGroups(new[] { "cp6-group-alpha", "cp6-group-beta" }, options: null));

        Assert.Equal(2, groups.DeletedGroups.Count);
        foreach (string group in new[] { "cp6-group-alpha", "cp6-group-beta" })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => groups.DeletedGroups[group].Get(), s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    [Fact]
    public async Task AlterUserScramCredentials_ThreeRowsOverTwoUsers_SettleTwoKeys()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterUserScramCredentialsResult result = AsyncAlterUserScramCredentialsResult.OfClient(admin.AlterUserScramCredentials(
            new UserScramCredentialAlteration[]
            {
                new UserScramCredentialDeletion("cp6-user-a", ScramMechanism.ScramSha256),
                new UserScramCredentialDeletion("cp6-user-a", ScramMechanism.ScramSha512),
                new UserScramCredentialDeletion("cp6-user-b", ScramMechanism.ScramSha256),
            },
            options: null));

        Assert.Equal(2, result.Values.Count);
        foreach (string user in new[] { "cp6-user-a", "cp6-user-b" })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[user].Get(), s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    private static ClientQuotaEntity Entity(string name) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.ClientId] = name });

    private static ClientQuotaAlteration Alteration(ClientQuotaEntity entity) =>
        new ClientQuotaAlteration(
            entity,
            new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0) });
}
