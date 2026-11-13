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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The real Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.KafkaAdminClient</c>, created from a
/// configuration map (Java's <c>Admin.create(Map)</c>).
/// </summary>
public sealed class KafkaAdminClient : IAdmin
{
    private readonly NativeAdminClient _native;

    /// <summary>Creates the client over the given configuration.</summary>
    internal KafkaAdminClient(IReadOnlyDictionary<string, object?> configs)
    {
        _native = NativeAdminClient.Create(configs, AdminFlavour.Sync);
    }

    /// <inheritdoc/>
    public CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics) =>
        CreateTopics(newTopics, new CreateTopicsOptions());

    /// <inheritdoc/>
    public CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions options)
    {
        CreateTopicsOptions? sent = AdminEarlyReturns.CreateTopics(ref newTopics, options);
        return CreateTopicsResult.OfClient(_native.CreateTopics(newTopics, sent));
    }

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics) =>
        DeleteTopics(topics, new DeleteTopicsOptions());

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions options) =>
        DeleteTopicsResult.OfClient(_native.DeleteTopics(topics, AdminEarlyReturns.DeleteTopics(topics, options)));

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(IReadOnlyCollection<string> topics) =>
        DeleteTopics(topics, new DeleteTopicsOptions());

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topics, DeleteTopicsOptions options) =>
        DeleteTopicsResult.OfClient(_native.DeleteTopics(
            topics ?? throw new ArgumentNullException(nameof(topics)),
            AdminEarlyReturns.DeleteTopics(topics, options)));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics) =>
        DescribeTopics(topics, new DescribeTopicsOptions());

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions options) =>
        DescribeTopicsResult.OfClient(
            _native.DescribeTopics(topics, AdminEarlyReturns.DescribeTopics(topics, options)));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(IReadOnlyCollection<string> topicNames) =>
        DescribeTopics(topicNames, new DescribeTopicsOptions());

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions options) =>
        DescribeTopicsResult.OfClient(_native.DescribeTopics(
            topicNames ?? throw new ArgumentNullException(nameof(topicNames)),
            AdminEarlyReturns.DescribeTopics(topicNames, options)));

    /// <inheritdoc/>
    public ListTopicsResult ListTopics() =>
        ListTopics(new ListTopicsOptions());

    /// <inheritdoc/>
    public ListTopicsResult ListTopics(ListTopicsOptions options)
    {
        var (namesToListings, result) = _native.ListTopics(options);
        return new ListTopicsResult(new KafkaFuture<IReadOnlyDictionary<string, TopicListing>>(namesToListings), result);
    }

    /// <inheritdoc/>
    public CreatePartitionsResult CreatePartitions(IReadOnlyDictionary<string, NewPartitions> newPartitions) =>
        CreatePartitions(newPartitions, new CreatePartitionsOptions());

    /// <inheritdoc/>
    public CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions options) =>
        CreatePartitionsResult.OfClient(
            _native.CreatePartitions(newPartitions, AdminEarlyReturns.CreatePartitions(newPartitions, options)));

    /// <inheritdoc/>
    public DeleteRecordsResult DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete) =>
        DeleteRecords(recordsToDelete, new DeleteRecordsOptions());

    /// <inheritdoc/>
    public DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions options) =>
        DeleteRecordsResult.OfClient(_native.DeleteRecords(recordsToDelete, options));

    /// <inheritdoc/>
    public DescribeClusterResult DescribeCluster() =>
        DescribeCluster(new DescribeClusterOptions());

    /// <inheritdoc/>
    public DescribeClusterResult DescribeCluster(DescribeClusterOptions options) =>
        DescribeClusterResult.OfClient(_native.DescribeCluster(options));

    /// <inheritdoc/>
    public ListConfigResourcesResult ListConfigResources() =>
        ListConfigResources(Array.Empty<ConfigResourceType>(), new ListConfigResourcesOptions());

    /// <inheritdoc/>
    public ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType> configResourceTypes,
        ListConfigResourcesOptions options) =>
        ListConfigResourcesResult.OfClient(_native.ListConfigResources(configResourceTypes, options));

    /// <inheritdoc/>
    public DescribeConfigsResult DescribeConfigs(IReadOnlyCollection<ConfigResource> resources) =>
        DescribeConfigs(resources, new DescribeConfigsOptions());

    /// <inheritdoc/>
    public DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions options) =>
        DescribeConfigsResult.OfClient(
            _native.DescribeConfigs(resources, AdminEarlyReturns.DescribeConfigs(resources, options)));

    /// <inheritdoc/>
    public AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs) =>
        IncrementalAlterConfigs(configs, new AlterConfigsOptions());

    /// <inheritdoc/>
    public AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions options) =>
        AlterConfigsResult.OfClient(
            _native.IncrementalAlterConfigs(configs, AdminEarlyReturns.IncrementalAlterConfigs(configs, options)));


    /// <inheritdoc/>
    public DescribeLogDirsResult DescribeLogDirs(IReadOnlyCollection<int> brokers) =>
        DescribeLogDirs(brokers, new DescribeLogDirsOptions());

    /// <inheritdoc/>
    public DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions options) =>
        DescribeLogDirsResult.OfClient(
            _native.DescribeLogDirs(brokers, AdminEarlyReturns.DescribeLogDirs(brokers, options)));

    /// <inheritdoc/>
    public AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment) =>
        AlterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());

    /// <inheritdoc/>
    public AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions options) =>
        AlterReplicaLogDirsResult.OfClient(
            _native.AlterReplicaLogDirs(
                replicaAssignment, AdminEarlyReturns.AlterReplicaLogDirs(replicaAssignment, options)));

    /// <inheritdoc/>
    public DescribeReplicaLogDirsResult DescribeReplicaLogDirs(IReadOnlyCollection<TopicPartitionReplica> replicas) =>
        DescribeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());

    /// <inheritdoc/>
    public DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions options) =>
        DescribeReplicaLogDirsResult.OfClient(
            _native.DescribeReplicaLogDirs(replicas, AdminEarlyReturns.DescribeReplicaLogDirs(replicas, options)));

    /// <inheritdoc/>
    public ElectLeadersResult ElectLeaders(ElectionType electionType, IReadOnlyCollection<TopicPartition>? partitions) =>
        ElectLeaders(electionType, partitions, new ElectLeadersOptions());

    /// <inheritdoc/>
    public ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions options) =>
        ElectLeadersResult.OfClient(_native.ElectLeaders(electionType, partitions, options));

    /// <inheritdoc/>
    public AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments) =>
        AlterPartitionReassignments(reassignments, new AlterPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions options) =>
        AlterPartitionReassignmentsResult.OfClient(
            _native.AlterPartitionReassignments(reassignments, options));

    /// <inheritdoc/>
    public AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        AlterConsumerGroupOffsets(groupId, offsets, new AlterConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions options) =>
        AlterConsumerGroupOffsetsResult.OfClient(
            _native.AlterConsumerGroupOffsets(groupId, offsets, options));

    /// <inheritdoc/>
    public DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId, IReadOnlyCollection<TopicPartition> partitions) =>
        DeleteConsumerGroupOffsets(groupId, partitions, new DeleteConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions options) =>
        DeleteConsumerGroupOffsetsResult.OfClient(
            _native.DeleteConsumerGroupOffsets(groupId, partitions, options));

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments() =>
        ListPartitionReassignments(new ListPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(IReadOnlyCollection<TopicPartition>? partitions) =>
        ListPartitionReassignments(partitions, new ListPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions,
        ListPartitionReassignmentsOptions options) =>
        ListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(
                partitions, AdminEarlyReturns.ListPartitionReassignments(partitions, options)));

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options) =>
        ListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(null, options));

    /// <inheritdoc/>
    public ListOffsetsResult ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets) =>
        ListOffsets(topicPartitionOffsets, new ListOffsetsOptions());

    /// <inheritdoc/>
    public ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions options) =>
        ListOffsetsResult.OfClient(_native.ListOffsets(topicPartitionOffsets, options));

    /// <inheritdoc/>
    public ListGroupsResult ListGroups() =>
        ListGroups(new ListGroupsOptions());

    /// <inheritdoc/>
    public ListGroupsResult ListGroups(ListGroupsOptions options) =>
        ListGroupsResult.OfClient(_native.ListGroups(options));

    /// <inheritdoc/>
    public DescribeConsumerGroupsResult DescribeConsumerGroups(IReadOnlyCollection<string> groupIds) =>
        DescribeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());

    /// <inheritdoc/>
    public DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions options) =>
        DescribeConsumerGroupsResult.OfClient(_native.DescribeConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    public DescribeClassicGroupsResult DescribeClassicGroups(IReadOnlyCollection<string> groupIds) =>
        DescribeClassicGroups(groupIds, new DescribeClassicGroupsOptions());

    /// <inheritdoc/>
    public DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions options) =>
        DescribeClassicGroupsResult.OfClient(_native.DescribeClassicGroups(groupIds, options));

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(string groupId) =>
        ListConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions options) =>
        ListConsumerGroupOffsetsResult.OfClient(_native.ListConsumerGroupOffsets(groupId, options));

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs) =>
        ListConsumerGroupOffsets(groupSpecs, new ListConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions options) =>
        ListConsumerGroupOffsetsResult.OfClient(
            _native.ListConsumerGroupOffsets(groupSpecs, options));

    /// <inheritdoc/>
    public DeleteConsumerGroupsResult DeleteConsumerGroups(IReadOnlyCollection<string> groupIds) =>
        DeleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());

    /// <inheritdoc/>
    public DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions options) =>
        DeleteConsumerGroupsResult.OfClient(
            _native.DeleteConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    public RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        RemoveMembersFromConsumerGroupResult.OfClient(
            _native.RemoveMembersFromConsumerGroup(groupId, options), options);

    /// <inheritdoc/>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls) =>
        CreateAcls(acls, new CreateAclsOptions());

    /// <inheritdoc/>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions options) =>
        CreateAclsResult.OfClient(_native.CreateAcls(acls, options));

    /// <inheritdoc/>
    public DeleteAclsResult DeleteAcls(IEnumerable<AclBindingFilter> filters) =>
        DeleteAcls(filters, new DeleteAclsOptions());

    /// <inheritdoc/>
    public DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions options) =>
        DeleteAclsResult.OfClient(_native.DeleteAcls(filters, options));

    /// <inheritdoc/>
    public DescribeAclsResult DescribeAcls(AclBindingFilter filter) =>
        DescribeAcls(filter, new DescribeAclsOptions());

    /// <inheritdoc/>
    public DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions options) =>
        DescribeAclsResult.OfClient(_native.DescribeAcls(filter, AdminEarlyReturns.DescribeAcls(filter, options)));

    /// <inheritdoc/>
    public DescribeClientQuotasResult DescribeClientQuotas(ClientQuotaFilter filter) =>
        DescribeClientQuotas(filter, new DescribeClientQuotasOptions());

    /// <inheritdoc/>
    public DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions options) =>
        DescribeClientQuotasResult.OfClient(_native.DescribeClientQuotas(filter, options));

    /// <inheritdoc/>
    public AlterClientQuotasResult AlterClientQuotas(IEnumerable<ClientQuotaAlteration> entries) =>
        AlterClientQuotas(entries, new AlterClientQuotasOptions());

    /// <inheritdoc/>
    public AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) =>
        AlterClientQuotasResult.OfClient(_native.AlterClientQuotas(entries, options));

    /// <inheritdoc/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials() =>
        DescribeUserScramCredentials(null, new DescribeUserScramCredentialsOptions());

    /// <inheritdoc/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(IReadOnlyCollection<string>? users) =>
        DescribeUserScramCredentials(users, new DescribeUserScramCredentialsOptions());

    /// <inheritdoc/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users,
        DescribeUserScramCredentialsOptions options) =>
        DescribeUserScramCredentialsResult.OfClient(
            _native.DescribeUserScramCredentials(users, options));

    /// <inheritdoc/>
    public AlterUserScramCredentialsResult AlterUserScramCredentials(IEnumerable<UserScramCredentialAlteration> alterations) =>
        AlterUserScramCredentials(alterations, new AlterUserScramCredentialsOptions());

    /// <inheritdoc/>
    public AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions options) =>
        AlterUserScramCredentialsResult.OfClient(
            _native.AlterUserScramCredentials(alterations, options));

    /// <inheritdoc/>
    public CreateDelegationTokenResult CreateDelegationToken() =>
        CreateDelegationToken(new CreateDelegationTokenOptions());

    /// <inheritdoc/>
    public CreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions options) =>
        CreateDelegationTokenResult.OfClient(_native.CreateDelegationToken(options));

    /// <inheritdoc/>
    public RenewDelegationTokenResult RenewDelegationToken(byte[] hmac) =>
        RenewDelegationToken(hmac, new RenewDelegationTokenOptions());

    /// <inheritdoc/>
    public RenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions options) =>
        RenewDelegationTokenResult.OfClient(_native.RenewDelegationToken(hmac, options));

    /// <inheritdoc/>
    public ExpireDelegationTokenResult ExpireDelegationToken(byte[] hmac) =>
        ExpireDelegationToken(hmac, new ExpireDelegationTokenOptions());

    /// <inheritdoc/>
    public ExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions options) =>
        ExpireDelegationTokenResult.OfClient(_native.ExpireDelegationToken(hmac, options));

    /// <inheritdoc/>
    public DescribeDelegationTokenResult DescribeDelegationToken() =>
        DescribeDelegationToken(new DescribeDelegationTokenOptions());

    /// <inheritdoc/>
    public DescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions options) =>
        DescribeDelegationTokenResult.OfClient(_native.DescribeDelegationToken(options));

    /// <inheritdoc/>
    public DescribeFeaturesResult DescribeFeatures() =>
        DescribeFeatures(new DescribeFeaturesOptions());

    /// <inheritdoc/>
    public DescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions options) =>
        DescribeFeaturesResult.OfClient(_native.DescribeFeatures(options));

    /// <inheritdoc/>
    public UpdateFeaturesResult UpdateFeatures(IReadOnlyDictionary<string, FeatureUpdate> featureUpdates) =>
        UpdateFeatures(featureUpdates, new UpdateFeaturesOptions());

    /// <inheritdoc/>
    public UpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions options) =>
        UpdateFeaturesResult.OfClient(_native.UpdateFeatures(featureUpdates, options));

    /// <inheritdoc/>
    public UnregisterControllerResult UnregisterController(int controllerId) =>
        UnregisterController(controllerId, new UnregisterControllerOptions());

    /// <inheritdoc/>
    public UnregisterControllerResult UnregisterController(int controllerId, UnregisterControllerOptions options) =>
        UnregisterControllerResult.OfClient(_native.UnregisterController(controllerId, options));

    /// <inheritdoc/>
    public FenceProducersResult FenceProducers(IReadOnlyCollection<string> transactionalIds) =>
        FenceProducers(transactionalIds, new FenceProducersOptions());

    /// <inheritdoc/>
    public FenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions options) =>
        FenceProducersResult.OfClient(_native.FenceProducers(transactionalIds, options));

    /// <inheritdoc/>
    public DescribeTransactionsResult DescribeTransactions(IReadOnlyCollection<string> transactionalIds) =>
        DescribeTransactions(transactionalIds, new DescribeTransactionsOptions());

    /// <inheritdoc/>
    public DescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions options) =>
        DescribeTransactionsResult.OfClient(
            _native.DescribeTransactions(transactionalIds, options));

    /// <inheritdoc/>
    public DescribeProducersResult DescribeProducers(IReadOnlyCollection<TopicPartition> partitions) =>
        DescribeProducers(partitions, new DescribeProducersOptions());

    /// <inheritdoc/>
    public DescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions options) =>
        DescribeProducersResult.OfClient(_native.DescribeProducers(partitions, options));

    /// <inheritdoc/>
    public ListTransactionsResult ListTransactions() =>
        ListTransactions(new ListTransactionsOptions());

    /// <inheritdoc/>
    public ListTransactionsResult ListTransactions(ListTransactionsOptions options) =>
        ListTransactionsResult.OfClient(_native.ListTransactions(options));

    /// <inheritdoc/>
    public AbortTransactionResult AbortTransaction(AbortTransactionSpec spec) =>
        AbortTransaction(spec, new AbortTransactionOptions());

    /// <inheritdoc/>
    public AbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions options) =>
        AbortTransactionResult.OfClient(_native.AbortTransaction(spec, options));

    /// <inheritdoc/>
    public TerminateTransactionResult ForceTerminateTransaction(string transactionalId) =>
        ForceTerminateTransaction(transactionalId, new TerminateTransactionOptions());

    /// <inheritdoc/>
    public TerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions options) =>
        TerminateTransactionResult.OfClient(
            _native.ForceTerminateTransaction(transactionalId, options));

    /// <inheritdoc/>
    public void Close() => _native.Close();

    /// <inheritdoc/>
    public void Close(TimeSpan timeout) => _native.CloseSync(timeout);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
