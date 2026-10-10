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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A broker-less, in-memory admin client for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.MockAdminClient</c>, and the only unit-test vehicle
/// for this binding.
/// </summary>
/// <remarks>
/// <para>
/// The ABI returns the <em>same</em> client handle type as the real constructor, so the
/// entire RPC surface works against the mock unchanged — a test exercises the real
/// marshalling, the real bridge and the real teardown, differing only in what the core
/// does behind them.
/// </para>
/// <para>
/// Brokers are <c>localhost:1000+id</c>, the controller is broker 0, the default
/// partition count is 1 and the default replication factor is
/// <c>min(numBrokers, 3)</c>. <b>Clipped to today's ABI:</b> Java's mock also accepts an
/// explicit broker list and controller; the ABI exposes only the broker count, so that
/// is what this constructor takes. Java's <c>addTopic</c>, <c>markTopicForDeletion</c>
/// and the broker log-dir seeder are likewise clipped — they are not at the C ABI.
/// </para>
/// </remarks>
public sealed class MockAdminClient : IAdmin
{
    private readonly NativeAdminClient _native;

    /// <summary>
    /// Creates a mock admin client simulating <paramref name="numBrokers"/> brokers —
    /// Java's <c>MockAdminClient.create().numBrokers(n).build()</c>.
    /// </summary>
    /// <param name="numBrokers">
    /// The number of brokers to simulate; at least 1. Java rejects fewer by throwing,
    /// because the mock places every partition leader and the controller on broker 0.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="numBrokers"/> is less than 1.</exception>
    /// <exception cref="KafkaException">The core could not create the mock.</exception>
    public MockAdminClient(int numBrokers = 1)
    {
        _native = NativeAdminClient.CreateMock(numBrokers);
    }

    /// <inheritdoc/>
    public CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options = null) =>
        CreateTopicsResult.From(_native.CreateTopics(newTopics, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null) =>
        DeleteTopicsResult.From(_native.DeleteTopics(topics, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topicNames, DeleteTopicsOptions? options = null) =>
        DeleteTopicsResult.From(_native.DeleteTopics(
            TopicCollection.OfTopicNames(topicNames ?? throw new ArgumentNullException(nameof(topicNames))),
            options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null) =>
        DescribeTopicsResult.From(_native.DescribeTopics(topics, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options = null) =>
        DescribeTopicsResult.From(_native.DescribeTopics(
            TopicCollection.OfTopicNames(topicNames ?? throw new ArgumentNullException(nameof(topicNames))),
            options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListTopicsResult ListTopics(ListTopicsOptions? options = null) =>
        ListTopicsResult.From(_native.ListTopics(options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options = null) =>
        CreatePartitionsResult.From(_native.CreatePartitions(newPartitions, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options = null) =>
        DeleteRecordsResult.From(_native.DeleteRecords(recordsToDelete, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeClusterResult DescribeCluster(DescribeClusterOptions? options = null) =>
        DescribeClusterResult.From(_native.DescribeCluster(options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes = null,
        ListConfigResourcesOptions? options = null) =>
        ListConfigResourcesResult.From(
            _native.ListConfigResources(configResourceTypes, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options = null) =>
        DescribeConfigsResult.From(_native.DescribeConfigs(resources, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options = null) =>
        AlterConfigsResult.From(_native.IncrementalAlterConfigs(configs, options, AdminFlavour.Sync));


    /// <inheritdoc/>
    public DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options = null) =>
        DescribeLogDirsResult.From(_native.DescribeLogDirs(brokers, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options = null) =>
        AlterReplicaLogDirsResult.From(
            _native.AlterReplicaLogDirs(replicaAssignment, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options = null) =>
        DescribeReplicaLogDirsResult.From(_native.DescribeReplicaLogDirs(replicas, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options = null) =>
        ElectLeadersResult.From(_native.ElectLeaders(electionType, partitions, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options = null) =>
        AlterPartitionReassignmentsResult.From(
            _native.AlterPartitionReassignments(reassignments, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options = null) =>
        AlterConsumerGroupOffsetsResult.From(
            _native.AlterConsumerGroupOffsets(groupId, offsets, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options = null) =>
        DeleteConsumerGroupOffsetsResult.From(
            _native.DeleteConsumerGroupOffsets(groupId, partitions, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions = null,
        ListPartitionReassignmentsOptions? options = null) =>
        ListPartitionReassignmentsResult.From(
            _native.ListPartitionReassignments(partitions, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options) =>
        ListPartitionReassignmentsResult.From(
            _native.ListPartitionReassignments(null, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions? options = null) =>
        ListOffsetsResult.From(_native.ListOffsets(topicPartitionOffsets, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListGroupsResult ListGroups(ListGroupsOptions? options = null) =>
        ListGroupsResult.From(_native.ListGroups(options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options = null) =>
        DescribeConsumerGroupsResult.From(_native.DescribeConsumerGroups(groupIds, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options = null) =>
        DescribeClassicGroupsResult.From(_native.DescribeClassicGroups(groupIds, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions? options = null)
    {
        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        // Here, so that a rejected id blames this overload's own parameter rather than the
        // batched form's `groupSpecs` (M15/P13.3 (c)).
        AdminStrings.Validate(groupId, nameof(groupId));

        // Java's default overload (Admin.java:912-918): delegate to the batched form
        // with a fresh spec, whose null TopicPartitions means "every committed
        // partition", and let the batched form ignore any partitions on the options.
        return ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                [groupId] = new ListConsumerGroupOffsetsSpec(),
            },
            options);
    }

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options = null) =>
        ListConsumerGroupOffsetsResult.From(
            _native.ListConsumerGroupOffsets(groupSpecs, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.deleteConsumerGroups</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:773-775</c>); the Rust mock surfaces that as a faulted
    /// future per group id (<c>admin-client.md</c> §9), which this binding does not
    /// need to special-case — it delegates to the same native mock every other RPC
    /// here does.
    /// </remarks>
    public DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null) =>
        DeleteConsumerGroupsResult.From(_native.DeleteConsumerGroups(groupIds, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.removeMembersFromConsumerGroup</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:801-803</c>); the Rust mock surfaces that as a single
    /// faulted future (<c>admin-client.md</c> §9), which this binding does not need to
    /// special-case — it delegates to the same native mock every other RPC here does.
    /// </remarks>
    public RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        RemoveMembersFromConsumerGroupResult.From(
            _native.RemoveMembersFromConsumerGroup(groupId, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.createAcls</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:806-808</c>); the Rust mock mirrors that as one exceptional
    /// future <em>per binding</em> rather than a panic (<c>admin-client.md</c> §9), so every
    /// binding's future fails with that message instead of succeeding.
    /// </remarks>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null) =>
        CreateAclsResult.From(_native.CreateAcls(acls, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.deleteAcls</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:816-818</c>); the Rust mock mirrors that as one exceptional
    /// future <em>per filter</em> rather than a panic (<c>admin-client.md</c> §9), so every
    /// filter's future fails with that message instead of succeeding.
    /// </remarks>
    public DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null) =>
        DeleteAclsResult.From(_native.DeleteAcls(filters, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.describeAcls</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:811-813</c>); the Rust mock mirrors that as one exceptional
    /// future for the whole call (<c>admin-client.md</c> §9), so
    /// <see cref="DescribeAclsResult.Values"/> fails with that message.
    /// </remarks>
    public DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null) =>
        DescribeAclsResult.From(_native.DescribeAcls(filter, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.describeClientQuotas</c> throws
    /// <c>UnsupportedOperationException("Not implement yet")</c> — Java's own typo, mirrored
    /// (<c>MockAdminClient.java:1244-1246</c>); the Rust mock surfaces that as one exceptional
    /// future for the whole call (<c>admin-client.md</c> §9), so
    /// <see cref="DescribeClientQuotasResult.Entities"/> fails with that message.
    /// </remarks>
    public DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null) =>
        DescribeClientQuotasResult.From(_native.DescribeClientQuotas(filter, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.alterClientQuotas</c> throws
    /// <c>UnsupportedOperationException("Not implement yet")</c> — Java's own typo, mirrored
    /// (<c>MockAdminClient.java:1249-1251</c>); the Rust mock surfaces that as one exceptional
    /// future <em>per entity</em> (<c>admin-client.md</c> §9), so every entity's future
    /// fails with that message instead of succeeding.
    /// </remarks>
    public AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null) =>
        AlterClientQuotasResult.From(_native.AlterClientQuotas(entries, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.describeUserScramCredentials</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:1254</c>); the Rust mock mirrors that as one exceptional future
    /// for the whole call (<c>admin-client.md</c> §9), so every accessor faults with that
    /// message. This is faithful, not a gap.
    /// </remarks>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null) =>
        DescribeUserScramCredentialsResult.From(
            _native.DescribeUserScramCredentials(users, options, AdminFlavour.Async));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's <c>MockAdminClient.alterUserScramCredentials</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:1259</c>); the Rust mock mirrors that <em>per user</em>, so
    /// every user's awaitable faults with that message.
    /// </remarks>
    public AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions? options = null) =>
        AlterUserScramCredentialsResult.From(
            _native.AlterUserScramCredentials(alterations, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public CreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions? options = null) =>
        CreateDelegationTokenResult.From(_native.CreateDelegationToken(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public RenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options = null) =>
        RenewDelegationTokenResult.From(_native.RenewDelegationToken(hmac, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public ExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options = null) =>
        ExpireDelegationTokenResult.From(_native.ExpireDelegationToken(hmac, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options = null) =>
        DescribeDelegationTokenResult.From(_native.DescribeDelegationToken(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions? options = null) =>
        DescribeFeaturesResult.From(_native.DescribeFeatures(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public UpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions? options = null) =>
        UpdateFeaturesResult.From(_native.UpdateFeatures(featureUpdates, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public FenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options = null) =>
        FenceProducersResult.From(_native.FenceProducers(transactionalIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options = null) =>
        DescribeTransactionsResult.From(
            _native.DescribeTransactions(transactionalIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options = null) =>
        DescribeProducersResult.From(_native.DescribeProducers(partitions, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public ListTransactionsResult ListTransactions(ListTransactionsOptions? options = null) =>
        ListTransactionsResult.From(_native.ListTransactionsOperation(options));

    /// <inheritdoc/>
    public AbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options = null) =>
        AbortTransactionResult.From(_native.AbortTransaction(spec, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public TerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions? options = null) =>
        TerminateTransactionResult.From(
            _native.ForceTerminateTransaction(transactionalId, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout) => _native.Close(timeout);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();

    // ---- Seeding — inherent, never on IAdmin (admin-client.md §9) ----

    /// <summary>
    /// Makes the next <paramref name="numberOfRequests"/> operations fail with a timeout —
    /// Java's <c>MockAdminClient.timeoutNextRequest(int)</c>.
    /// </summary>
    /// <param name="numberOfRequests">How many subsequent operations to time out.</param>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    public void TimeoutNextRequest(int numberOfRequests) =>
        _native.TimeoutNextRequest(numberOfRequests);

    /// <summary>
    /// Seeds the offsets <see cref="ListOffsets"/> reports for <c>OffsetSpec.Earliest</c> —
    /// Java's <c>MockAdminClient.updateBeginningOffsets(Map)</c>. <b>Merges</b> into what
    /// was seeded before.
    /// </summary>
    /// <param name="offsets">The beginning offset per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    public void UpdateBeginningOffsets(IReadOnlyDictionary<TopicPartition, long> offsets) =>
        _native.UpdateBeginningOffsets(offsets);

    /// <summary>
    /// Seeds the offsets <see cref="ListOffsets"/> reports for every <c>OffsetSpec</c> other
    /// than earliest and for-timestamp — Java's
    /// <c>MockAdminClient.updateEndOffsets(Map)</c>. <b>Merges</b>.
    /// </summary>
    /// <param name="offsets">The end offset per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    public void UpdateEndOffsets(IReadOnlyDictionary<TopicPartition, long> offsets) =>
        _native.UpdateEndOffsets(offsets);

    /// <summary>
    /// Seeds the committed offsets <c>ListConsumerGroupOffsets</c> reports — Java's
    /// <c>MockAdminClient.updateConsumerGroupOffsets(Map)</c>. <b>Merges</b>.
    /// </summary>
    /// <param name="offsets">The committed offset per topic partition.</param>
    /// <remarks>
    /// The mock keys these by partition only and ignores the group id, so there is no group
    /// parameter; the real client has no such restriction.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    public void UpdateConsumerGroupOffsets(IReadOnlyDictionary<TopicPartition, long> offsets) =>
        _native.UpdateConsumerGroupOffsets(offsets);

    /// <summary>
    /// Seeds the feature levels <see cref="DescribeFeatures"/> reports and
    /// <see cref="UpdateFeatures"/> validates against. <b>Replaces</b> what was seeded
    /// before, unlike the offset setters.
    /// </summary>
    /// <param name="featureLevels">
    /// The current, minimum-supported and maximum-supported level per feature.
    /// </param>
    /// <remarks>
    /// Java takes these as three separate <c>MockAdminClient.Builder</c> setters
    /// (<c>featureLevels</c> / <c>minSupportedFeatureLevels</c> /
    /// <c>maxSupportedFeatureLevels</c>); the ABI exposes one setter over a shared key set,
    /// so the three levels travel together per feature — the shape Python binds too.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="featureLevels"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key is null, or contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    public void SetFeatureLevels(
        IReadOnlyDictionary<string, (short Level, short MinSupported, short MaxSupported)> featureLevels) =>
        _native.SetFeatureLevels(featureLevels);
}
