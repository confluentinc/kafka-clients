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
/// The real asynchronous Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.KafkaAdminClient</c> whose per-key futures are awaited, created from a
/// configuration map (Java's <c>Admin.create(Properties)</c>).
/// </summary>
/// <remarks>
/// Every RPC returns immediately with one <see cref="AsyncKafkaFuture{T}"/> per key; only
/// <see cref="Close(TimeSpan)"/> is awaited for the operation itself. See <see cref="IAsyncAdmin"/>.
/// </remarks>
/// <seealso cref="KafkaAdminClient"/>
public sealed class AsyncKafkaAdminClient : IAsyncAdmin
{
    private readonly NativeAdminClient _native;

    /// <summary>
    /// Creates an admin client from a configuration map — Java's
    /// <c>Admin.create(Properties)</c>. Keys are the Java dotted names;
    /// <c>bootstrap.servers</c> is required.
    /// </summary>
    /// <param name="config">Configuration keyed by Java dotted names.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A configuration value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    public AsyncKafkaAdminClient(IReadOnlyDictionary<string, string> config)
    {
        _native = NativeAdminClient.Create(config);
    }

    /// <inheritdoc/>
    public AsyncCreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options = null) =>
        AsyncCreateTopicsResult.From(_native.CreateTopics(newTopics, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null) =>
        AsyncDeleteTopicsResult.From(_native.DeleteTopics(topics, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topicNames, DeleteTopicsOptions? options = null) =>
        AsyncDeleteTopicsResult.From(_native.DeleteTopics(
            TopicCollection.OfTopicNames(topicNames ?? throw new ArgumentNullException(nameof(topicNames))),
            options,
            AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null) =>
        AsyncDescribeTopicsResult.From(_native.DescribeTopics(topics, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options = null) =>
        AsyncDescribeTopicsResult.From(_native.DescribeTopics(
            TopicCollection.OfTopicNames(topicNames ?? throw new ArgumentNullException(nameof(topicNames))),
            options,
            AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListTopicsResult ListTopics(ListTopicsOptions? options = null) =>
        AsyncListTopicsResult.From(_native.ListTopics(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncCreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options = null) =>
        AsyncCreatePartitionsResult.From(_native.CreatePartitions(newPartitions, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options = null) =>
        AsyncDeleteRecordsResult.From(_native.DeleteRecords(recordsToDelete, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeClusterResult DescribeCluster(DescribeClusterOptions? options = null) =>
        AsyncDescribeClusterResult.From(_native.DescribeCluster(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes = null,
        ListConfigResourcesOptions? options = null) =>
        AsyncListConfigResourcesResult.From(
            _native.ListConfigResources(configResourceTypes, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options = null) =>
        AsyncDescribeConfigsResult.From(_native.DescribeConfigs(resources, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncAlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options = null) =>
        AsyncAlterConfigsResult.From(_native.IncrementalAlterConfigs(configs, options, AdminFlavour.Async));


    /// <inheritdoc/>
    public AsyncDescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options = null) =>
        AsyncDescribeLogDirsResult.From(_native.DescribeLogDirs(brokers, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncAlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options = null) =>
        AsyncAlterReplicaLogDirsResult.From(
            _native.AlterReplicaLogDirs(replicaAssignment, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options = null) =>
        AsyncDescribeReplicaLogDirsResult.From(_native.DescribeReplicaLogDirs(replicas, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options = null) =>
        AsyncElectLeadersResult.From(_native.ElectLeaders(electionType, partitions, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncAlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options = null) =>
        AsyncAlterPartitionReassignmentsResult.From(
            _native.AlterPartitionReassignments(reassignments, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncAlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options = null) =>
        AsyncAlterConsumerGroupOffsetsResult.From(
            _native.AlterConsumerGroupOffsets(groupId, offsets, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options = null) =>
        AsyncDeleteConsumerGroupOffsetsResult.From(
            _native.DeleteConsumerGroupOffsets(groupId, partitions, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions = null,
        ListPartitionReassignmentsOptions? options = null) =>
        AsyncListPartitionReassignmentsResult.From(
            _native.ListPartitionReassignments(partitions, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options) =>
        AsyncListPartitionReassignmentsResult.From(
            _native.ListPartitionReassignments(null, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions? options = null) =>
        AsyncListOffsetsResult.From(_native.ListOffsets(topicPartitionOffsets, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListGroupsResult ListGroups(ListGroupsOptions? options = null) =>
        AsyncListGroupsResult.From(_native.ListGroups(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options = null) =>
        AsyncDescribeConsumerGroupsResult.From(_native.DescribeConsumerGroups(groupIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options = null) =>
        AsyncDescribeClassicGroupsResult.From(_native.DescribeClassicGroups(groupIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
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
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options = null) =>
        AsyncListConsumerGroupOffsetsResult.From(
            _native.ListConsumerGroupOffsets(groupSpecs, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null) =>
        AsyncDeleteConsumerGroupsResult.From(_native.DeleteConsumerGroups(groupIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncRemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        AsyncRemoveMembersFromConsumerGroupResult.From(
            _native.RemoveMembersFromConsumerGroup(groupId, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncCreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null) =>
        AsyncCreateAclsResult.From(_native.CreateAcls(acls, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null) =>
        AsyncDeleteAclsResult.From(_native.DeleteAcls(filters, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null) =>
        AsyncDescribeAclsResult.From(_native.DescribeAcls(filter, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null) =>
        AsyncDescribeClientQuotasResult.From(_native.DescribeClientQuotas(filter, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncAlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null) =>
        AsyncAlterClientQuotasResult.From(_native.AlterClientQuotas(entries, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null) =>
        AsyncDescribeUserScramCredentialsResult.From(
            _native.DescribeUserScramCredentials(users, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncAlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions? options = null) =>
        AsyncAlterUserScramCredentialsResult.From(
            _native.AlterUserScramCredentials(alterations, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncCreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions? options = null) =>
        AsyncCreateDelegationTokenResult.From(_native.CreateDelegationToken(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncRenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options = null) =>
        AsyncRenewDelegationTokenResult.From(_native.RenewDelegationToken(hmac, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options = null) =>
        AsyncExpireDelegationTokenResult.From(_native.ExpireDelegationToken(hmac, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options = null) =>
        AsyncDescribeDelegationTokenResult.From(_native.DescribeDelegationToken(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions? options = null) =>
        AsyncDescribeFeaturesResult.From(_native.DescribeFeatures(options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncUpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions? options = null) =>
        AsyncUpdateFeaturesResult.From(_native.UpdateFeatures(featureUpdates, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncFenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options = null) =>
        AsyncFenceProducersResult.From(_native.FenceProducers(transactionalIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options = null) =>
        AsyncDescribeTransactionsResult.From(
            _native.DescribeTransactions(transactionalIds, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncDescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options = null) =>
        AsyncDescribeProducersResult.From(_native.DescribeProducers(partitions, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncListTransactionsResult ListTransactions(ListTransactionsOptions? options = null) =>
        AsyncListTransactionsResult.From(_native.ListTransactionsOperation(options));

    /// <inheritdoc/>
    public AsyncAbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options = null) =>
        AsyncAbortTransactionResult.From(_native.AbortTransaction(spec, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AsyncTerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions? options = null) =>
        AsyncTerminateTransactionResult.From(
            _native.ForceTerminateTransaction(transactionalId, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout) => _native.Close(timeout);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
