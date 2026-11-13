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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The real asynchronous Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.KafkaAdminClient</c> whose per-key futures are awaited, created from a
/// configuration map (Java's <c>Admin.create(Map)</c>).
/// </summary>
/// <seealso cref="KafkaAdminClient"/>
public sealed class AsyncKafkaAdminClient : IAsyncAdmin
{
    private readonly NativeAdminClient _native;

    /// <summary>Creates the client over the given configuration.</summary>
    internal AsyncKafkaAdminClient(IReadOnlyDictionary<string, object?> configs)
    {
        _native = NativeAdminClient.Create(configs, AdminFlavour.Async);
    }

    /// <inheritdoc/>
    public AsyncCreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics) =>
        CreateTopics(newTopics, new CreateTopicsOptions());

    /// <inheritdoc/>
    public AsyncCreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions options)
    {
        CreateTopicsOptions? sent = AdminEarlyReturns.CreateTopics(ref newTopics, options);
        return AsyncCreateTopicsResult.OfClient(_native.CreateTopics(newTopics, sent));
    }

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(TopicCollection topics) =>
        DeleteTopics(topics, new DeleteTopicsOptions());

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions options) =>
        AsyncDeleteTopicsResult.OfClient(_native.DeleteTopics(topics, AdminEarlyReturns.DeleteTopics(topics, options)));

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(IReadOnlyCollection<string> topics) =>
        DeleteTopics(topics, new DeleteTopicsOptions());

    /// <inheritdoc/>
    public AsyncDeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topics, DeleteTopicsOptions options) =>
        AsyncDeleteTopicsResult.OfClient(_native.DeleteTopics(
            topics ?? throw new ArgumentNullException(nameof(topics)),
            AdminEarlyReturns.DeleteTopics(topics, options)));

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(TopicCollection topics) =>
        DescribeTopics(topics, new DescribeTopicsOptions());

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions options) =>
        AsyncDescribeTopicsResult.OfClient(
            _native.DescribeTopics(topics, AdminEarlyReturns.DescribeTopics(topics, options)));

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(IReadOnlyCollection<string> topicNames) =>
        DescribeTopics(topicNames, new DescribeTopicsOptions());

    /// <inheritdoc/>
    public AsyncDescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions options) =>
        AsyncDescribeTopicsResult.OfClient(_native.DescribeTopics(
            topicNames ?? throw new ArgumentNullException(nameof(topicNames)),
            AdminEarlyReturns.DescribeTopics(topicNames, options)));

    /// <inheritdoc/>
    public AsyncListTopicsResult ListTopics() =>
        ListTopics(new ListTopicsOptions());

    /// <inheritdoc/>
    public AsyncListTopicsResult ListTopics(ListTopicsOptions options)
    {
        var (namesToListings, result) = _native.ListTopics(options);
        return new AsyncListTopicsResult(new AsyncKafkaFuture<IReadOnlyDictionary<string, TopicListing>>(namesToListings), result);
    }

    /// <inheritdoc/>
    public AsyncCreatePartitionsResult CreatePartitions(IReadOnlyDictionary<string, NewPartitions> newPartitions) =>
        CreatePartitions(newPartitions, new CreatePartitionsOptions());

    /// <inheritdoc/>
    public AsyncCreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions options) =>
        AsyncCreatePartitionsResult.OfClient(
            _native.CreatePartitions(newPartitions, AdminEarlyReturns.CreatePartitions(newPartitions, options)));

    /// <inheritdoc/>
    public AsyncDeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete) =>
        DeleteRecords(recordsToDelete, new DeleteRecordsOptions());

    /// <inheritdoc/>
    public AsyncDeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions options) =>
        AsyncDeleteRecordsResult.OfClient(_native.DeleteRecords(recordsToDelete, options));

    /// <inheritdoc/>
    public AsyncDescribeClusterResult DescribeCluster() =>
        DescribeCluster(new DescribeClusterOptions());

    /// <inheritdoc/>
    public AsyncDescribeClusterResult DescribeCluster(DescribeClusterOptions options) =>
        AsyncDescribeClusterResult.OfClient(_native.DescribeCluster(options));

    /// <inheritdoc/>
    public AsyncListConfigResourcesResult ListConfigResources() =>
        ListConfigResources(Array.Empty<ConfigResourceType>(), new ListConfigResourcesOptions());

    /// <inheritdoc/>
    public AsyncListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType> configResourceTypes,
        ListConfigResourcesOptions options) =>
        AsyncListConfigResourcesResult.OfClient(_native.ListConfigResources(configResourceTypes, options));

    /// <inheritdoc/>
    public AsyncDescribeConfigsResult DescribeConfigs(IReadOnlyCollection<ConfigResource> resources) =>
        DescribeConfigs(resources, new DescribeConfigsOptions());

    /// <inheritdoc/>
    public AsyncDescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions options) =>
        AsyncDescribeConfigsResult.OfClient(
            _native.DescribeConfigs(resources, AdminEarlyReturns.DescribeConfigs(resources, options)));

    /// <inheritdoc/>
    public AsyncAlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs) =>
        IncrementalAlterConfigs(configs, new AlterConfigsOptions());

    /// <inheritdoc/>
    public AsyncAlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions options) =>
        AsyncAlterConfigsResult.OfClient(
            _native.IncrementalAlterConfigs(configs, AdminEarlyReturns.IncrementalAlterConfigs(configs, options)));


    /// <inheritdoc/>
    public AsyncDescribeLogDirsResult DescribeLogDirs(IReadOnlyCollection<int> brokers) =>
        DescribeLogDirs(brokers, new DescribeLogDirsOptions());

    /// <inheritdoc/>
    public AsyncDescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions options) =>
        AsyncDescribeLogDirsResult.OfClient(
            _native.DescribeLogDirs(brokers, AdminEarlyReturns.DescribeLogDirs(brokers, options)));

    /// <inheritdoc/>
    public AsyncAlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment) =>
        AlterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());

    /// <inheritdoc/>
    public AsyncAlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions options) =>
        AsyncAlterReplicaLogDirsResult.OfClient(
            _native.AlterReplicaLogDirs(
                replicaAssignment, AdminEarlyReturns.AlterReplicaLogDirs(replicaAssignment, options)));

    /// <inheritdoc/>
    public AsyncDescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas) =>
        DescribeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());

    /// <inheritdoc/>
    public AsyncDescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions options) =>
        AsyncDescribeReplicaLogDirsResult.OfClient(
            _native.DescribeReplicaLogDirs(replicas, AdminEarlyReturns.DescribeReplicaLogDirs(replicas, options)));

    /// <inheritdoc/>
    public AsyncElectLeadersResult ElectLeaders(
        ElectionType electionType, IReadOnlyCollection<TopicPartition>? partitions) =>
        ElectLeaders(electionType, partitions, new ElectLeadersOptions());

    /// <inheritdoc/>
    public AsyncElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions options) =>
        AsyncElectLeadersResult.OfClient(_native.ElectLeaders(electionType, partitions, options));

    /// <inheritdoc/>
    public AsyncAlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments) =>
        AlterPartitionReassignments(reassignments, new AlterPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public AsyncAlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions options) =>
        AsyncAlterPartitionReassignmentsResult.OfClient(
            _native.AlterPartitionReassignments(reassignments, options));

    /// <inheritdoc/>
    public AsyncAlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        AlterConsumerGroupOffsets(groupId, offsets, new AlterConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public AsyncAlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions options) =>
        AsyncAlterConsumerGroupOffsetsResult.OfClient(
            _native.AlterConsumerGroupOffsets(groupId, offsets, options));

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId, IReadOnlyCollection<TopicPartition> partitions) =>
        DeleteConsumerGroupOffsets(groupId, partitions, new DeleteConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions options) =>
        AsyncDeleteConsumerGroupOffsetsResult.OfClient(
            _native.DeleteConsumerGroupOffsets(groupId, partitions, options));

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments() =>
        ListPartitionReassignments(new ListPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions) =>
        ListPartitionReassignments(partitions, new ListPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions,
        ListPartitionReassignmentsOptions options) =>
        AsyncListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(
                partitions, AdminEarlyReturns.ListPartitionReassignments(partitions, options)));

    /// <inheritdoc/>
    public AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options) =>
        AsyncListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(null, options));

    /// <inheritdoc/>
    public AsyncListOffsetsResult ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets) =>
        ListOffsets(topicPartitionOffsets, new ListOffsetsOptions());

    /// <inheritdoc/>
    public AsyncListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions options) =>
        AsyncListOffsetsResult.OfClient(_native.ListOffsets(topicPartitionOffsets, options));

    /// <inheritdoc/>
    public AsyncListGroupsResult ListGroups() =>
        ListGroups(new ListGroupsOptions());

    /// <inheritdoc/>
    public AsyncListGroupsResult ListGroups(ListGroupsOptions options) =>
        AsyncListGroupsResult.OfClient(_native.ListGroups(options));

    /// <inheritdoc/>
    public AsyncDescribeConsumerGroupsResult DescribeConsumerGroups(IReadOnlyCollection<string> groupIds) =>
        DescribeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());

    /// <inheritdoc/>
    public AsyncDescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions options) =>
        AsyncDescribeConsumerGroupsResult.OfClient(_native.DescribeConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    public AsyncDescribeClassicGroupsResult DescribeClassicGroups(IReadOnlyCollection<string> groupIds) =>
        DescribeClassicGroups(groupIds, new DescribeClassicGroupsOptions());

    /// <inheritdoc/>
    public AsyncDescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions options) =>
        AsyncDescribeClassicGroupsResult.OfClient(_native.DescribeClassicGroups(groupIds, options));

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(string groupId) =>
        ListConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions options) =>
        AsyncListConsumerGroupOffsetsResult.OfClient(_native.ListConsumerGroupOffsets(groupId, options));

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs) =>
        ListConsumerGroupOffsets(groupSpecs, new ListConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions options) =>
        AsyncListConsumerGroupOffsetsResult.OfClient(
            _native.ListConsumerGroupOffsets(groupSpecs, options));

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupsResult DeleteConsumerGroups(IReadOnlyCollection<string> groupIds) =>
        DeleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());

    /// <inheritdoc/>
    public AsyncDeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions options) =>
        AsyncDeleteConsumerGroupsResult.OfClient(
            _native.DeleteConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    public AsyncRemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        AsyncRemoveMembersFromConsumerGroupResult.OfClient(
            _native.RemoveMembersFromConsumerGroup(groupId, options), options);

    /// <inheritdoc/>
    public AsyncCreateAclsResult CreateAcls(IEnumerable<AclBinding> acls) =>
        CreateAcls(acls, new CreateAclsOptions());

    /// <inheritdoc/>
    public AsyncCreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions options) =>
        AsyncCreateAclsResult.OfClient(_native.CreateAcls(acls, options));

    /// <inheritdoc/>
    public AsyncDeleteAclsResult DeleteAcls(IEnumerable<AclBindingFilter> filters) =>
        DeleteAcls(filters, new DeleteAclsOptions());

    /// <inheritdoc/>
    public AsyncDeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions options) =>
        AsyncDeleteAclsResult.OfClient(_native.DeleteAcls(filters, options));

    /// <inheritdoc/>
    public AsyncDescribeAclsResult DescribeAcls(AclBindingFilter filter) =>
        DescribeAcls(filter, new DescribeAclsOptions());

    /// <inheritdoc/>
    public AsyncDescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions options) =>
        AsyncDescribeAclsResult.OfClient(_native.DescribeAcls(filter, AdminEarlyReturns.DescribeAcls(filter, options)));

    /// <inheritdoc/>
    public AsyncDescribeClientQuotasResult DescribeClientQuotas(ClientQuotaFilter filter) =>
        DescribeClientQuotas(filter, new DescribeClientQuotasOptions());

    /// <inheritdoc/>
    public AsyncDescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions options) =>
        AsyncDescribeClientQuotasResult.OfClient(_native.DescribeClientQuotas(filter, options));

    /// <inheritdoc/>
    public AsyncAlterClientQuotasResult AlterClientQuotas(IEnumerable<ClientQuotaAlteration> entries) =>
        AlterClientQuotas(entries, new AlterClientQuotasOptions());

    /// <inheritdoc/>
    public AsyncAlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) =>
        AsyncAlterClientQuotasResult.OfClient(_native.AlterClientQuotas(entries, options));

    /// <inheritdoc/>
    public AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials() =>
        DescribeUserScramCredentials(null, new DescribeUserScramCredentialsOptions());

    /// <inheritdoc/>
    public AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials(IReadOnlyCollection<string>? users) =>
        DescribeUserScramCredentials(users, new DescribeUserScramCredentialsOptions());

    /// <inheritdoc/>
    public AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users,
        DescribeUserScramCredentialsOptions options) =>
        AsyncDescribeUserScramCredentialsResult.OfClient(
            _native.DescribeUserScramCredentials(users, options));

    /// <inheritdoc/>
    public AsyncAlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations) =>
        AlterUserScramCredentials(alterations, new AlterUserScramCredentialsOptions());

    /// <inheritdoc/>
    public AsyncAlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions options) =>
        AsyncAlterUserScramCredentialsResult.OfClient(
            _native.AlterUserScramCredentials(alterations, options));

    /// <inheritdoc/>
    public AsyncCreateDelegationTokenResult CreateDelegationToken() =>
        CreateDelegationToken(new CreateDelegationTokenOptions());

    /// <inheritdoc/>
    public AsyncCreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions options) =>
        AsyncCreateDelegationTokenResult.OfClient(_native.CreateDelegationToken(options));

    /// <inheritdoc/>
    public AsyncRenewDelegationTokenResult RenewDelegationToken(byte[] hmac) =>
        RenewDelegationToken(hmac, new RenewDelegationTokenOptions());

    /// <inheritdoc/>
    public AsyncRenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions options) =>
        AsyncRenewDelegationTokenResult.OfClient(_native.RenewDelegationToken(hmac, options));

    /// <inheritdoc/>
    public AsyncExpireDelegationTokenResult ExpireDelegationToken(byte[] hmac) =>
        ExpireDelegationToken(hmac, new ExpireDelegationTokenOptions());

    /// <inheritdoc/>
    public AsyncExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions options) =>
        AsyncExpireDelegationTokenResult.OfClient(_native.ExpireDelegationToken(hmac, options));

    /// <inheritdoc/>
    public AsyncDescribeDelegationTokenResult DescribeDelegationToken() =>
        DescribeDelegationToken(new DescribeDelegationTokenOptions());

    /// <inheritdoc/>
    public AsyncDescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions options) =>
        AsyncDescribeDelegationTokenResult.OfClient(_native.DescribeDelegationToken(options));

    /// <inheritdoc/>
    public AsyncDescribeFeaturesResult DescribeFeatures() =>
        DescribeFeatures(new DescribeFeaturesOptions());

    /// <inheritdoc/>
    public AsyncDescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions options) =>
        AsyncDescribeFeaturesResult.OfClient(_native.DescribeFeatures(options));

    /// <inheritdoc/>
    public AsyncUpdateFeaturesResult UpdateFeatures(IReadOnlyDictionary<string, FeatureUpdate> featureUpdates) =>
        UpdateFeatures(featureUpdates, new UpdateFeaturesOptions());

    /// <inheritdoc/>
    public AsyncUpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions options) =>
        AsyncUpdateFeaturesResult.OfClient(_native.UpdateFeatures(featureUpdates, options));

    /// <inheritdoc/>
    public AsyncUnregisterControllerResult UnregisterController(int controllerId) =>
        UnregisterController(controllerId, new UnregisterControllerOptions());

    /// <inheritdoc/>
    public AsyncUnregisterControllerResult UnregisterController(
        int controllerId, UnregisterControllerOptions options) =>
        AsyncUnregisterControllerResult.OfClient(_native.UnregisterController(controllerId, options));

    /// <inheritdoc/>
    public AsyncFenceProducersResult FenceProducers(IReadOnlyCollection<string> transactionalIds) =>
        FenceProducers(transactionalIds, new FenceProducersOptions());

    /// <inheritdoc/>
    public AsyncFenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions options) =>
        AsyncFenceProducersResult.OfClient(_native.FenceProducers(transactionalIds, options));

    /// <inheritdoc/>
    public AsyncDescribeTransactionsResult DescribeTransactions(IReadOnlyCollection<string> transactionalIds) =>
        DescribeTransactions(transactionalIds, new DescribeTransactionsOptions());

    /// <inheritdoc/>
    public AsyncDescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions options) =>
        AsyncDescribeTransactionsResult.OfClient(
            _native.DescribeTransactions(transactionalIds, options));

    /// <inheritdoc/>
    public AsyncDescribeProducersResult DescribeProducers(IReadOnlyCollection<TopicPartition> partitions) =>
        DescribeProducers(partitions, new DescribeProducersOptions());

    /// <inheritdoc/>
    public AsyncDescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions options) =>
        AsyncDescribeProducersResult.OfClient(_native.DescribeProducers(partitions, options));

    /// <inheritdoc/>
    public AsyncListTransactionsResult ListTransactions() =>
        ListTransactions(new ListTransactionsOptions());

    /// <inheritdoc/>
    public AsyncListTransactionsResult ListTransactions(ListTransactionsOptions options) =>
        AsyncListTransactionsResult.OfClient(_native.ListTransactions(options));

    /// <inheritdoc/>
    public AsyncAbortTransactionResult AbortTransaction(AbortTransactionSpec spec) =>
        AbortTransaction(spec, new AbortTransactionOptions());

    /// <inheritdoc/>
    public AsyncAbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions options) =>
        AsyncAbortTransactionResult.OfClient(_native.AbortTransaction(spec, options));

    /// <inheritdoc/>
    public AsyncTerminateTransactionResult ForceTerminateTransaction(string transactionalId) =>
        ForceTerminateTransaction(transactionalId, new TerminateTransactionOptions());

    /// <inheritdoc/>
    public AsyncTerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions options) =>
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
