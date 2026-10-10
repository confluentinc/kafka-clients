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
/// The real Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.KafkaAdminClient</c>, created from a
/// configuration map (Java's <c>Admin.create(Properties)</c>).
/// </summary>
/// <remarks>
/// Every RPC returns immediately with one awaitable per key; only
/// <see cref="Close(TimeSpan)"/> is awaited for the operation itself. See
/// <see cref="IAdmin"/> for why.
/// </remarks>
public sealed class KafkaAdminClient : IAdmin
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
    public KafkaAdminClient(IReadOnlyDictionary<string, string> config)
    {
        _native = NativeAdminClient.Create(config);
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
            options,
            AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null) =>
        DescribeTopicsResult.From(_native.DescribeTopics(topics, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options = null) =>
        DescribeTopicsResult.From(_native.DescribeTopics(
            TopicCollection.OfTopicNames(topicNames ?? throw new ArgumentNullException(nameof(topicNames))),
            options,
            AdminFlavour.Sync));

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
    public DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null) =>
        DeleteConsumerGroupsResult.From(_native.DeleteConsumerGroups(groupIds, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        RemoveMembersFromConsumerGroupResult.From(
            _native.RemoveMembersFromConsumerGroup(groupId, options, AdminFlavour.Sync));

    /// <inheritdoc/>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null) =>
        CreateAclsResult.From(_native.CreateAcls(acls, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null) =>
        DeleteAclsResult.From(_native.DeleteAcls(filters, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null) =>
        DescribeAclsResult.From(_native.DescribeAcls(filter, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null) =>
        DescribeClientQuotasResult.From(_native.DescribeClientQuotas(filter, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null) =>
        AlterClientQuotasResult.From(_native.AlterClientQuotas(entries, options, AdminFlavour.Async));

    /// <inheritdoc/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null) =>
        DescribeUserScramCredentialsResult.From(
            _native.DescribeUserScramCredentials(users, options, AdminFlavour.Async));

    /// <inheritdoc/>
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
}
