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
        _native.CreateTopics(newTopics, options);

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null) =>
        _native.DeleteTopics(topics, options);

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null) =>
        _native.DescribeTopics(topics, options);

    /// <inheritdoc/>
    public ListTopicsResult ListTopics(ListTopicsOptions? options = null) =>
        _native.ListTopics(options);

    /// <inheritdoc/>
    public CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options = null) =>
        _native.CreatePartitions(newPartitions, options);

    /// <inheritdoc/>
    public DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options = null) =>
        _native.DeleteRecords(recordsToDelete, options);

    /// <inheritdoc/>
    public DescribeClusterResult DescribeCluster(DescribeClusterOptions? options = null) =>
        _native.DescribeCluster(options);

    /// <inheritdoc/>
    public ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes = null,
        ListConfigResourcesOptions? options = null) =>
        _native.ListConfigResources(configResourceTypes, options);

    /// <inheritdoc/>
    [Obsolete(
        "Deprecated in Kafka since 4.1. Use ListConfigResources filtered to "
        + "ConfigResourceType.ClientMetrics instead.")]
    public ListClientMetricsResourcesResult ListClientMetricsResources(
        ListClientMetricsResourcesOptions? options = null) =>
        _native.ListClientMetricsResources(options);


    /// <inheritdoc/>
    public DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options = null) =>
        _native.DescribeConfigs(resources, options);

    /// <inheritdoc/>
    public AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options = null) =>
        _native.IncrementalAlterConfigs(configs, options);


    /// <inheritdoc/>
    public DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options = null) =>
        _native.DescribeLogDirs(brokers, options);

    /// <inheritdoc/>
    public AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options = null) =>
        _native.AlterReplicaLogDirs(replicaAssignment, options);

    /// <inheritdoc/>
    public DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options = null) =>
        _native.DescribeReplicaLogDirs(replicas, options);

    /// <inheritdoc/>
    public ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options = null) =>
        _native.ElectLeaders(electionType, partitions, options);

    /// <inheritdoc/>
    public AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options = null) =>
        _native.AlterPartitionReassignments(reassignments, options);

    /// <inheritdoc/>
    public AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options = null) =>
        _native.AlterConsumerGroupOffsets(groupId, offsets, options);

    /// <inheritdoc/>
    public DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options = null) =>
        _native.DeleteConsumerGroupOffsets(groupId, partitions, options);

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions,
        ListPartitionReassignmentsOptions? options = null) =>
        _native.ListPartitionReassignments(partitions, options);

    /// <inheritdoc/>
    public ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions? options = null) =>
        _native.ListOffsets(topicPartitionOffsets, options);

    /// <inheritdoc/>
    public ListGroupsResult ListGroups(ListGroupsOptions? options = null) =>
        _native.ListGroups(options);

    /// <inheritdoc/>
    [Obsolete(
        "Deprecated in Kafka since 4.1. Use ListGroups instead.")]
    public ListConsumerGroupsResult ListConsumerGroups(
        ListConsumerGroupsOptions? options = null) =>
        _native.ListConsumerGroups(options);

    /// <inheritdoc/>
    public DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options = null) =>
        _native.DescribeConsumerGroups(groupIds, options);

    /// <inheritdoc/>
    public DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options = null) =>
        _native.DescribeClassicGroups(groupIds, options);

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions? options = null)
    {
        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

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
        _native.ListConsumerGroupOffsets(groupSpecs, options);

    /// <inheritdoc/>
    public DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null) =>
        _native.DeleteConsumerGroups(groupIds, options);

    /// <inheritdoc/>
    public RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        _native.RemoveMembersFromConsumerGroup(groupId, options);

    /// <inheritdoc/>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null) =>
        _native.CreateAcls(acls, options);

    /// <inheritdoc/>
    public DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null) =>
        _native.DeleteAcls(filters, options);

    /// <inheritdoc/>
    public DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null) =>
        _native.DescribeAcls(filter, options);

    /// <inheritdoc/>
    public DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null) =>
        _native.DescribeClientQuotas(filter, options);

    /// <inheritdoc/>
    public AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null) =>
        _native.AlterClientQuotas(entries, options);

    /// <inheritdoc/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null) =>
        _native.DescribeUserScramCredentials(users, options);

    /// <inheritdoc/>
    public AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions? options = null) =>
        _native.AlterUserScramCredentials(alterations, options);

    /// <inheritdoc/>
    public CreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions? options = null) =>
        _native.CreateDelegationToken(options);

    /// <inheritdoc/>
    public RenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options = null) =>
        _native.RenewDelegationToken(hmac, options);

    /// <inheritdoc/>
    public ExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options = null) =>
        _native.ExpireDelegationToken(hmac, options);

    /// <inheritdoc/>
    public DescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options = null) =>
        _native.DescribeDelegationToken(options);

    /// <inheritdoc/>
    public DescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions? options = null) =>
        _native.DescribeFeatures(options);

    /// <inheritdoc/>
    public UpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions? options = null) =>
        _native.UpdateFeatures(featureUpdates, options);

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout) => _native.Close(timeout);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
