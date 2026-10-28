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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The real asynchronous Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.KafkaAdminClient</c> whose per-key futures are awaited, created from a
/// configuration map (Java's <c>Admin.create(Properties)</c>).
/// </summary>
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
        _native = NativeAdminClient.Create(config, AdminFlavour.Async);
    }

    /// <inheritdoc/>
    public AsyncCreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options = null) =>
        AsyncCreateTopicsResult.OfClient(_native.CreateTopics(newTopics, options));

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null) =>
        AsyncDeleteTopicsResult.OfClient(_native.DeleteTopics(topics, options));

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topicNames, DeleteTopicsOptions? options = null) =>
        AsyncDeleteTopicsResult.OfClient(_native.DeleteTopics(
            topicNames ?? throw new ArgumentNullException(nameof(topicNames)),
            options));

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null) =>
        AsyncDescribeTopicsResult.OfClient(_native.DescribeTopics(topics, options));

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options = null) =>
        AsyncDescribeTopicsResult.OfClient(_native.DescribeTopics(
            topicNames ?? throw new ArgumentNullException(nameof(topicNames)),
            options));

    /// <inheritdoc/>
    public AsyncListTopicsResult ListTopics(ListTopicsOptions? options = null)
    {
        var (namesToListings, result) = _native.ListTopics(options);
        return new AsyncListTopicsResult(new AsyncKafkaFuture<IReadOnlyDictionary<string, TopicListing>>(namesToListings), result);
    }

    /// <inheritdoc/>
    public AsyncCreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options = null) =>
        AsyncCreatePartitionsResult.OfClient(_native.CreatePartitions(newPartitions, options));

    /// <inheritdoc/>
    public AsyncDeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options = null) =>
        AsyncDeleteRecordsResult.OfClient(_native.DeleteRecords(recordsToDelete, options));

    /// <inheritdoc/>
    public AsyncDescribeClusterResult DescribeCluster(DescribeClusterOptions? options = null) =>
        AsyncDescribeClusterResult.OfClient(_native.DescribeCluster(options));

    /// <inheritdoc/>
    public AsyncListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes = null,
        ListConfigResourcesOptions? options = null) =>
        AsyncListConfigResourcesResult.OfClient(_native.ListConfigResources(configResourceTypes, options));

    /// <inheritdoc/>
    public AsyncDescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options = null) =>
        AsyncDescribeConfigsResult.OfClient(_native.DescribeConfigs(resources, options));

    /// <inheritdoc/>
    public AsyncAlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options = null) =>
        AsyncAlterConfigsResult.OfClient(_native.IncrementalAlterConfigs(configs, options));


    /// <inheritdoc/>
    public AsyncDescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options = null) =>
        AsyncDescribeLogDirsResult.OfClient(_native.DescribeLogDirs(brokers, options));

    /// <inheritdoc/>
    public AsyncAlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options = null) =>
        AsyncAlterReplicaLogDirsResult.OfClient(
            _native.AlterReplicaLogDirs(replicaAssignment, options));

    /// <inheritdoc/>
    public AsyncDescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options = null) =>
        AsyncDescribeReplicaLogDirsResult.OfClient(_native.DescribeReplicaLogDirs(replicas, options));

    /// <inheritdoc/>
    public AsyncElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options = null) =>
        AsyncElectLeadersResult.OfClient(_native.ElectLeaders(electionType, partitions, options));

    /// <inheritdoc/>
    public AsyncAlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options = null) =>
        AsyncAlterPartitionReassignmentsResult.OfClient(
            _native.AlterPartitionReassignments(reassignments, options));

    /// <inheritdoc/>
    public AsyncAlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options = null) =>
        AsyncAlterConsumerGroupOffsetsResult.OfClient(
            _native.AlterConsumerGroupOffsets(groupId, offsets, options));

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options = null) =>
        AsyncDeleteConsumerGroupOffsetsResult.OfClient(
            _native.DeleteConsumerGroupOffsets(groupId, partitions, options));

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions = null,
        ListPartitionReassignmentsOptions? options = null) =>
        AsyncListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(partitions, options));

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options) =>
        AsyncListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(null, options));

    /// <inheritdoc/>
    public AsyncListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions? options = null) =>
        AsyncListOffsetsResult.OfClient(_native.ListOffsets(topicPartitionOffsets, options));

    /// <inheritdoc/>
    public AsyncListGroupsResult ListGroups(ListGroupsOptions? options = null) =>
        AsyncListGroupsResult.OfClient(_native.ListGroups(options));

    /// <inheritdoc/>
    public AsyncDescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options = null) =>
        AsyncDescribeConsumerGroupsResult.OfClient(_native.DescribeConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    public AsyncDescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options = null) =>
        AsyncDescribeClassicGroupsResult.OfClient(_native.DescribeClassicGroups(groupIds, options));

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions? options = null) =>
        AsyncListConsumerGroupOffsetsResult.OfClient(_native.ListConsumerGroupOffsets(groupId, options));

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options = null) =>
        AsyncListConsumerGroupOffsetsResult.OfClient(
            _native.ListConsumerGroupOffsets(groupSpecs, options));

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null) =>
        AsyncDeleteConsumerGroupsResult.OfClient(
            _native.DeleteConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    public AsyncRemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        AsyncRemoveMembersFromConsumerGroupResult.OfClient(
            _native.RemoveMembersFromConsumerGroup(groupId, options));

    /// <inheritdoc/>
    public AsyncCreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null) =>
        AsyncCreateAclsResult.OfClient(_native.CreateAcls(acls, options));

    /// <inheritdoc/>
    public AsyncDeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null) =>
        AsyncDeleteAclsResult.OfClient(_native.DeleteAcls(filters, options));

    /// <inheritdoc/>
    public AsyncDescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null) =>
        AsyncDescribeAclsResult.OfClient(_native.DescribeAcls(filter, options));

    /// <inheritdoc/>
    public AsyncDescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null) =>
        AsyncDescribeClientQuotasResult.OfClient(_native.DescribeClientQuotas(filter, options));

    /// <inheritdoc/>
    public AsyncAlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null) =>
        AsyncAlterClientQuotasResult.OfClient(_native.AlterClientQuotas(entries, options));

    /// <inheritdoc/>
    public AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null) =>
        AsyncDescribeUserScramCredentialsResult.OfClient(
            _native.DescribeUserScramCredentials(users, options));

    /// <inheritdoc/>
    public AsyncAlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions? options = null) =>
        AsyncAlterUserScramCredentialsResult.OfClient(
            _native.AlterUserScramCredentials(alterations, options));

    /// <inheritdoc/>
    public AsyncCreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions? options = null) =>
        AsyncCreateDelegationTokenResult.OfClient(_native.CreateDelegationToken(options));

    /// <inheritdoc/>
    public AsyncRenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options = null) =>
        AsyncRenewDelegationTokenResult.OfClient(_native.RenewDelegationToken(hmac, options));

    /// <inheritdoc/>
    public AsyncExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options = null) =>
        AsyncExpireDelegationTokenResult.OfClient(_native.ExpireDelegationToken(hmac, options));

    /// <inheritdoc/>
    public AsyncDescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options = null) =>
        AsyncDescribeDelegationTokenResult.OfClient(_native.DescribeDelegationToken(options));

    /// <inheritdoc/>
    public AsyncDescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions? options = null) =>
        AsyncDescribeFeaturesResult.OfClient(_native.DescribeFeatures(options));

    /// <inheritdoc/>
    public AsyncUpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions? options = null) =>
        AsyncUpdateFeaturesResult.OfClient(_native.UpdateFeatures(featureUpdates, options));

    /// <inheritdoc/>
    public AsyncUnregisterControllerResult UnregisterController(
        int controllerId, UnregisterControllerOptions? options = null) =>
        AsyncUnregisterControllerResult.OfClient(_native.UnregisterController(controllerId, options));

    /// <inheritdoc/>
    public AsyncFenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options = null) =>
        AsyncFenceProducersResult.OfClient(_native.FenceProducers(transactionalIds, options));

    /// <inheritdoc/>
    public AsyncDescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options = null) =>
        AsyncDescribeTransactionsResult.OfClient(
            _native.DescribeTransactions(transactionalIds, options));

    /// <inheritdoc/>
    public AsyncDescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options = null) =>
        AsyncDescribeProducersResult.OfClient(_native.DescribeProducers(partitions, options));

    /// <inheritdoc/>
    public AsyncListTransactionsResult ListTransactions(ListTransactionsOptions? options = null) =>
        AsyncListTransactionsResult.OfClient(_native.ListTransactions(options));

    /// <inheritdoc/>
    public AsyncAbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options = null) =>
        AsyncAbortTransactionResult.OfClient(_native.AbortTransaction(spec, options));

    /// <inheritdoc/>
    public AsyncTerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions? options = null) =>
        AsyncTerminateTransactionResult.OfClient(
            _native.ForceTerminateTransaction(transactionalId, options));

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) => _native.Close(cancellationToken);

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.Close(timeout, cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
