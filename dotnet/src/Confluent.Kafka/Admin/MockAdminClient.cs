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
using System.Collections.ObjectModel;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A broker-less, in-memory admin client for tests, Java's <c>org.apache.kafka.clients.admin.MockAdminClient</c>.
/// </summary>
/// <remarks>
/// <para>
/// Every RPC runs through the same marshalling and teardown as the real client. Build one with <see cref="Create"/>
/// and its <see cref="Builder"/>, or with Java's two constructors. <c>Close</c> does nothing, as Java's mock does;
/// <c>Dispose</c> frees the mock.
/// </para>
/// <para>
/// A null options is taken as the default options, except in
/// <see cref="CreateDelegationToken(CreateDelegationTokenOptions)"/>,
/// <see cref="RenewDelegationToken(byte[], RenewDelegationTokenOptions)"/>,
/// <see cref="ExpireDelegationToken(byte[], ExpireDelegationTokenOptions)"/>,
/// <see cref="DescribeDelegationToken(DescribeDelegationTokenOptions)"/>,
/// <see cref="UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate}, UpdateFeaturesOptions)"/> and
/// <see cref="RemoveMembersFromConsumerGroup(string, RemoveMembersFromConsumerGroupOptions)"/>, which throw
/// <see cref="ArgumentNullException"/>.
/// </para>
/// </remarks>
public sealed class MockAdminClient : IAdmin
{
    /// <summary>
    /// The cluster id of a mock whose <see cref="Builder"/> sets none, Java's <c>MockAdminClient.DEFAULT_CLUSTER_ID</c>.
    /// </summary>
    public const string DEFAULT_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";

    /// <summary>
    /// The log directories of each broker of a mock whose <see cref="Builder"/> sets none, Java's
    /// <c>MockAdminClient.DEFAULT_LOG_DIRS</c>.
    /// </summary>
    public static readonly IReadOnlyList<string> DEFAULT_LOG_DIRS = new ReadOnlyCollection<string>(new[] { "/tmp/kafka-logs" });

    private readonly NativeAdminClient _native;

    private MockAdminClient(NativeAdminClient native)
    {
        _native = native;
    }

    /// <summary>
    /// Creates a mock with one broker, Java's <c>Node.noNode()</c>, which is also the controller: Java's
    /// <c>new MockAdminClient()</c>.
    /// </summary>
    /// <exception cref="KafkaException">The core could not create the mock.</exception>
    public MockAdminClient()
        : this(NativeAdminClient.StartMock(MockAdminClientBuilderCore.NewMock(), AdminFlavour.Sync))
    {
    }

    /// <summary>
    /// Creates a mock over <paramref name="brokers"/> whose controller is <paramref name="controller"/>, Java's
    /// <c>new MockAdminClient(List&lt;Node&gt;, Node)</c>.
    /// </summary>
    /// <param name="brokers">The brokers.</param>
    /// <param name="controller">The controller, one of <paramref name="brokers"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="brokers"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="brokers"/> contains a null element, <paramref name="controller"/> is null or not one of them
    /// (Java's message for both), or a host or rack cannot cross the C ABI unchanged.
    /// </exception>
    public MockAdminClient(IReadOnlyList<Node> brokers, Node controller)
        : this(NativeAdminClient.StartMock(MockAdminClientBuilderCore.NewMock(brokers, controller), AdminFlavour.Sync))
    {
    }

    /// <summary>Returns a new <see cref="Builder"/> with one broker, Java's <c>MockAdminClient.create()</c>.</summary>
    /// <returns>The builder.</returns>
    /// <exception cref="KafkaException">The core could not create the builder.</exception>
    public static Builder Create() => new Builder(MockAdminClientBuilderCore.Create());

    /// <inheritdoc/>
    public CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics) =>
        CreateTopics(newTopics, new CreateTopicsOptions());

    /// <inheritdoc/>
    public CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions options) =>
        CreateTopicsResult.OfClient(_native.CreateTopics(newTopics, options ?? new CreateTopicsOptions()));

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics) =>
        DeleteTopics(topics, new DeleteTopicsOptions());

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions options) =>
        DeleteTopicsResult.OfClient(_native.DeleteTopics(topics, options ?? new DeleteTopicsOptions()));

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(IReadOnlyCollection<string> topics) =>
        DeleteTopics(topics, new DeleteTopicsOptions());

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topics, DeleteTopicsOptions options) =>
        DeleteTopicsResult.OfClient(_native.DeleteTopics(
            topics ?? throw new ArgumentNullException(nameof(topics)),
            options ?? new DeleteTopicsOptions()));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics) =>
        DescribeTopics(topics, new DescribeTopicsOptions());

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions options) =>
        DescribeTopicsResult.OfClient(_native.DescribeTopics(topics, options ?? new DescribeTopicsOptions()));

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(IReadOnlyCollection<string> topicNames) =>
        DescribeTopics(topicNames, new DescribeTopicsOptions());

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions options) =>
        DescribeTopicsResult.OfClient(_native.DescribeTopics(
            topicNames ?? throw new ArgumentNullException(nameof(topicNames)),
            options ?? new DescribeTopicsOptions()));

    /// <inheritdoc/>
    public ListTopicsResult ListTopics() =>
        ListTopics(new ListTopicsOptions());

    /// <inheritdoc/>
    public ListTopicsResult ListTopics(ListTopicsOptions options)
    {
        var (namesToListings, result) = _native.ListTopics(options ?? new ListTopicsOptions());
        return new ListTopicsResult(new KafkaFuture<IReadOnlyDictionary<string, TopicListing>>(namesToListings), result);
    }

    /// <inheritdoc/>
    public CreatePartitionsResult CreatePartitions(IReadOnlyDictionary<string, NewPartitions> newPartitions) =>
        CreatePartitions(newPartitions, new CreatePartitionsOptions());

    /// <inheritdoc/>
    public CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions options) =>
        CreatePartitionsResult.OfClient(_native.CreatePartitions(newPartitions, options ?? new CreatePartitionsOptions()));

    /// <inheritdoc/>
    public DeleteRecordsResult DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete) =>
        DeleteRecords(recordsToDelete, new DeleteRecordsOptions());

    /// <inheritdoc/>
    public DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions options) =>
        DeleteRecordsResult.OfClient(_native.DeleteRecords(recordsToDelete, options ?? new DeleteRecordsOptions()));

    /// <inheritdoc/>
    public DescribeClusterResult DescribeCluster() =>
        DescribeCluster(new DescribeClusterOptions());

    /// <inheritdoc/>
    public DescribeClusterResult DescribeCluster(DescribeClusterOptions options) =>
        DescribeClusterResult.OfClient(_native.DescribeCluster(options ?? new DescribeClusterOptions()));

    /// <inheritdoc/>
    public ListConfigResourcesResult ListConfigResources() =>
        ListConfigResources(Array.Empty<ConfigResourceType>(), new ListConfigResourcesOptions());

    /// <inheritdoc/>
    public ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType> configResourceTypes,
        ListConfigResourcesOptions options) =>
        ListConfigResourcesResult.OfClient(_native.ListConfigResources(configResourceTypes, options ?? new ListConfigResourcesOptions()));

    /// <inheritdoc/>
    public DescribeConfigsResult DescribeConfigs(IReadOnlyCollection<ConfigResource> resources) =>
        DescribeConfigs(resources, new DescribeConfigsOptions());

    /// <inheritdoc/>
    public DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions options) =>
        DescribeConfigsResult.OfClient(_native.DescribeConfigs(resources, options ?? new DescribeConfigsOptions()));

    /// <inheritdoc/>
    public AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs) =>
        IncrementalAlterConfigs(configs, new AlterConfigsOptions());

    /// <inheritdoc/>
    public AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions options) =>
        AlterConfigsResult.OfClient(_native.IncrementalAlterConfigs(configs, options ?? new AlterConfigsOptions()));


    /// <inheritdoc/>
    public DescribeLogDirsResult DescribeLogDirs(IReadOnlyCollection<int> brokers) =>
        DescribeLogDirs(brokers, new DescribeLogDirsOptions());

    /// <inheritdoc/>
    public DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions options) =>
        DescribeLogDirsResult.OfClient(_native.DescribeLogDirs(brokers, options ?? new DescribeLogDirsOptions()));

    /// <inheritdoc/>
    public AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment) =>
        AlterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());

    /// <inheritdoc/>
    public AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions options) =>
        AlterReplicaLogDirsResult.OfClient(
            _native.AlterReplicaLogDirs(replicaAssignment, options ?? new AlterReplicaLogDirsOptions()));

    /// <inheritdoc/>
    public DescribeReplicaLogDirsResult DescribeReplicaLogDirs(IReadOnlyCollection<TopicPartitionReplica> replicas) =>
        DescribeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());

    /// <inheritdoc/>
    public DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions options) =>
        DescribeReplicaLogDirsResult.OfClient(_native.DescribeReplicaLogDirs(replicas, options ?? new DescribeReplicaLogDirsOptions()));

    /// <inheritdoc/>
    public ElectLeadersResult ElectLeaders(ElectionType electionType, IReadOnlyCollection<TopicPartition>? partitions) =>
        ElectLeaders(electionType, partitions, new ElectLeadersOptions());

    /// <inheritdoc/>
    public ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions options) =>
        ElectLeadersResult.OfClient(_native.ElectLeaders(electionType, partitions, options ?? new ElectLeadersOptions()));

    /// <inheritdoc/>
    public AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments) =>
        AlterPartitionReassignments(reassignments, new AlterPartitionReassignmentsOptions());

    /// <inheritdoc/>
    public AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions options) =>
        AlterPartitionReassignmentsResult.OfClient(
            _native.AlterPartitionReassignments(reassignments, options ?? new AlterPartitionReassignmentsOptions()));

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
            _native.AlterConsumerGroupOffsets(groupId, offsets, options ?? new AlterConsumerGroupOffsetsOptions()));

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
            _native.DeleteConsumerGroupOffsets(groupId, partitions, options ?? new DeleteConsumerGroupOffsetsOptions()));

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
            _native.ListPartitionReassignments(partitions, options ?? new ListPartitionReassignmentsOptions()));

    /// <inheritdoc/>
    public ListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options) =>
        ListPartitionReassignmentsResult.OfClient(
            _native.ListPartitionReassignments(null, options ?? new ListPartitionReassignmentsOptions()));

    /// <inheritdoc/>
    public ListOffsetsResult ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets) =>
        ListOffsets(topicPartitionOffsets, new ListOffsetsOptions());

    /// <inheritdoc/>
    public ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions options) =>
        ListOffsetsResult.OfClient(_native.ListOffsets(topicPartitionOffsets, options ?? new ListOffsetsOptions()));

    /// <inheritdoc/>
    public ListGroupsResult ListGroups() =>
        ListGroups(new ListGroupsOptions());

    /// <inheritdoc/>
    public ListGroupsResult ListGroups(ListGroupsOptions options) =>
        ListGroupsResult.OfClient(_native.ListGroups(options ?? new ListGroupsOptions()));

    /// <inheritdoc/>
    public DescribeConsumerGroupsResult DescribeConsumerGroups(IReadOnlyCollection<string> groupIds) =>
        DescribeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());

    /// <inheritdoc/>
    public DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions options) =>
        DescribeConsumerGroupsResult.OfClient(_native.DescribeConsumerGroups(groupIds, options ?? new DescribeConsumerGroupsOptions()));

    /// <inheritdoc/>
    public DescribeClassicGroupsResult DescribeClassicGroups(IReadOnlyCollection<string> groupIds) =>
        DescribeClassicGroups(groupIds, new DescribeClassicGroupsOptions());

    /// <inheritdoc/>
    public DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions options) =>
        DescribeClassicGroupsResult.OfClient(_native.DescribeClassicGroups(groupIds, options ?? new DescribeClassicGroupsOptions()));

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(string groupId) =>
        ListConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions options) =>
        ListConsumerGroupOffsetsResult.OfClient(_native.ListConsumerGroupOffsets(groupId, options ?? new ListConsumerGroupOffsetsOptions()));

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs) =>
        ListConsumerGroupOffsets(groupSpecs, new ListConsumerGroupOffsetsOptions());

    /// <inheritdoc/>
    public ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions options) =>
        ListConsumerGroupOffsetsResult.OfClient(
            _native.ListConsumerGroupOffsets(groupSpecs, options ?? new ListConsumerGroupOffsetsOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="DeleteConsumerGroups(IReadOnlyCollection{string}, DeleteConsumerGroupsOptions)" path="/remarks"/>
    public DeleteConsumerGroupsResult DeleteConsumerGroups(IReadOnlyCollection<string> groupIds) =>
        DeleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every group id's future fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions options) =>
        DeleteConsumerGroupsResult.OfClient(
            _native.DeleteConsumerGroups(groupIds, options ?? new DeleteConsumerGroupsOptions()));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so the call's one future fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        RemoveMembersFromConsumerGroupResult.OfClient(
            _native.RemoveMembersFromConsumerGroup(groupId, options), options);

    /// <inheritdoc/>
    /// <inheritdoc cref="CreateAcls(IEnumerable{AclBinding}, CreateAclsOptions)" path="/remarks"/>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls) =>
        CreateAcls(acls, new CreateAclsOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every binding's future fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions options) =>
        CreateAclsResult.OfClient(_native.CreateAcls(acls, options ?? new CreateAclsOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="DeleteAcls(IEnumerable{AclBindingFilter}, DeleteAclsOptions)" path="/remarks"/>
    public DeleteAclsResult DeleteAcls(IEnumerable<AclBindingFilter> filters) =>
        DeleteAcls(filters, new DeleteAclsOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every filter's future fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions options) =>
        DeleteAclsResult.OfClient(_native.DeleteAcls(filters, options ?? new DeleteAclsOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="DescribeAcls(AclBindingFilter, DescribeAclsOptions)" path="/remarks"/>
    public DescribeAclsResult DescribeAcls(AclBindingFilter filter) =>
        DescribeAcls(filter, new DescribeAclsOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so <see cref="DescribeAclsResult.Values"/> fails with an <see cref="UnsupportedOperationException"/>
    /// carrying that message.
    /// </remarks>
    public DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions options) =>
        DescribeAclsResult.OfClient(_native.DescribeAcls(filter, options ?? new DescribeAclsOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="DescribeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)" path="/remarks"/>
    public DescribeClientQuotasResult DescribeClientQuotas(ClientQuotaFilter filter) =>
        DescribeClientQuotas(filter, new DescribeClientQuotasOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implement yet")</c> here, Java's own typo kept,
    /// so <see cref="DescribeClientQuotasResult.Entities"/> fails with an <see cref="UnsupportedOperationException"/>
    /// carrying that message.
    /// </remarks>
    public DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions options) =>
        DescribeClientQuotasResult.OfClient(_native.DescribeClientQuotas(filter, options ?? new DescribeClientQuotasOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="AlterClientQuotas(IEnumerable{ClientQuotaAlteration}, AlterClientQuotasOptions)" path="/remarks"/>
    public AlterClientQuotasResult AlterClientQuotas(IEnumerable<ClientQuotaAlteration> entries) =>
        AlterClientQuotas(entries, new AlterClientQuotasOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implement yet")</c> here, Java's own typo kept,
    /// so every entity's future fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) =>
        AlterClientQuotasResult.OfClient(_native.AlterClientQuotas(entries, options ?? new AlterClientQuotasOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)" path="/remarks"/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials() =>
        DescribeUserScramCredentials(null, new DescribeUserScramCredentialsOptions());

    /// <inheritdoc/>
    /// <inheritdoc cref="DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)" path="/remarks"/>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(IReadOnlyCollection<string>? users) =>
        DescribeUserScramCredentials(users, new DescribeUserScramCredentialsOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every accessor fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users,
        DescribeUserScramCredentialsOptions options) =>
        DescribeUserScramCredentialsResult.OfClient(
            _native.DescribeUserScramCredentials(users, options ?? new DescribeUserScramCredentialsOptions()));

    /// <inheritdoc/>
    /// <inheritdoc cref="AlterUserScramCredentials(IEnumerable{UserScramCredentialAlteration}, AlterUserScramCredentialsOptions)" path="/remarks"/>
    public AlterUserScramCredentialsResult AlterUserScramCredentials(IEnumerable<UserScramCredentialAlteration> alterations) =>
        AlterUserScramCredentials(alterations, new AlterUserScramCredentialsOptions());

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every user's future fails with an <see cref="UnsupportedOperationException"/> carrying that message.
    /// </remarks>
    public AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions options) =>
        AlterUserScramCredentialsResult.OfClient(
            _native.AlterUserScramCredentials(alterations, options ?? new AlterUserScramCredentialsOptions()));

    /// <inheritdoc/>
    /// <exception cref="IndexOutOfBoundsException">
    /// Always: the default options name no renewer, and the mock makes the first renewer the token's owner.
    /// </exception>
    public CreateDelegationTokenResult CreateDelegationToken() =>
        CreateDelegationToken(new CreateDelegationTokenOptions());

    /// <inheritdoc/>
    /// <exception cref="IndexOutOfBoundsException">
    /// No renewer is given: Java's mock makes the first renewer the token's owner.
    /// </exception>
    public CreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions options)
    {
        // Java's mock reads renewers().get(0) as the token's owner, out of bounds for its default LinkedList.
        if (options?.Renewers() is { Count: 0 })
        {
            throw new IndexOutOfBoundsException("Index: 0, Size: 0");
        }

        return CreateDelegationTokenResult.OfClient(_native.CreateDelegationToken(options));
    }

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
        DescribeFeaturesResult.OfClient(_native.DescribeFeatures(options ?? new DescribeFeaturesOptions()));

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
        UnregisterControllerResult.OfClient(_native.UnregisterController(controllerId, options ?? new UnregisterControllerOptions()));

    /// <inheritdoc/>
    public FenceProducersResult FenceProducers(IReadOnlyCollection<string> transactionalIds) =>
        FenceProducers(transactionalIds, new FenceProducersOptions());

    /// <inheritdoc/>
    public FenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions options) =>
        FenceProducersResult.OfClient(_native.FenceProducers(transactionalIds, options ?? new FenceProducersOptions()));

    /// <inheritdoc/>
    public DescribeTransactionsResult DescribeTransactions(IReadOnlyCollection<string> transactionalIds) =>
        DescribeTransactions(transactionalIds, new DescribeTransactionsOptions());

    /// <inheritdoc/>
    public DescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions options) =>
        DescribeTransactionsResult.OfClient(
            _native.DescribeTransactions(transactionalIds, options ?? new DescribeTransactionsOptions()));

    /// <inheritdoc/>
    public DescribeProducersResult DescribeProducers(IReadOnlyCollection<TopicPartition> partitions) =>
        DescribeProducers(partitions, new DescribeProducersOptions());

    /// <inheritdoc/>
    public DescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions options) =>
        DescribeProducersResult.OfClient(_native.DescribeProducers(partitions, options ?? new DescribeProducersOptions()));

    /// <inheritdoc/>
    public ListTransactionsResult ListTransactions() =>
        ListTransactions(new ListTransactionsOptions());

    /// <inheritdoc/>
    public ListTransactionsResult ListTransactions(ListTransactionsOptions options) =>
        ListTransactionsResult.OfClient(_native.ListTransactions(options ?? new ListTransactionsOptions()));

    /// <inheritdoc/>
    public AbortTransactionResult AbortTransaction(AbortTransactionSpec spec) =>
        AbortTransaction(spec, new AbortTransactionOptions());

    /// <inheritdoc/>
    public AbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions options) =>
        AbortTransactionResult.OfClient(_native.AbortTransaction(spec, options ?? new AbortTransactionOptions()));

    /// <inheritdoc/>
    public TerminateTransactionResult ForceTerminateTransaction(string transactionalId) =>
        ForceTerminateTransaction(transactionalId, new TerminateTransactionOptions());

    /// <inheritdoc/>
    public TerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions options) =>
        TerminateTransactionResult.OfClient(
            _native.ForceTerminateTransaction(transactionalId, options ?? new TerminateTransactionOptions()));

    /// <summary>Does nothing, as Java's mock; <see cref="Dispose"/> frees the mock.</summary>
    public void Close()
    {
    }

    /// <summary>Does nothing and reads no timeout, as Java's mock; <see cref="Dispose"/> frees the mock.</summary>
    public void Close(TimeSpan timeout)
    {
    }

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <summary>
    /// Makes <paramref name="controller"/>, which must be one of the mock's brokers, the controller that
    /// <c>DescribeCluster</c> reports, as Java's <c>MockAdminClient.controller(Node)</c>; otherwise throws
    /// <see cref="ArgumentException"/> and leaves the controller unchanged.
    /// </summary>
    public void Controller(Node controller) => _native.Controller(controller);

    /// <summary>
    /// Adds a topic with a random topic id, as Java's <c>MockAdminClient.addTopic(boolean, String, List, Map)</c>;
    /// throws <see cref="ArgumentException"/> with the core's message when the topic exists or a partition names a
    /// node that is not one of the mock's brokers. A <see langword="null"/> <paramref name="configs"/> is Java's
    /// <c>null</c>.
    /// </summary>
    public void AddTopic(
        bool @internal,
        string name,
        IReadOnlyList<TopicPartitionInfo> partitions,
        IReadOnlyDictionary<string, string>? configs) =>
        _native.AddTopic(@internal, name, partitions, configs);

    /// <summary>
    /// Marks a topic as being deleted, so that <c>DescribeTopics</c> and <c>DescribeConfigs</c> no longer find it, as
    /// Java's <c>MockAdminClient.markTopicForDeletion(String)</c>; throws <see cref="ArgumentException"/> for an
    /// unknown topic.
    /// </summary>
    public void MarkTopicForDeletion(string name) => _native.MarkTopicForDeletion(name);

    /// <summary>
    /// Makes the next <paramref name="numberOfRequest"/> operations fail with a timeout —
    /// Java's <c>MockAdminClient.timeoutNextRequest(int)</c>.
    /// </summary>
    /// <param name="numberOfRequest">How many subsequent operations to time out.</param>
    /// <exception cref="IllegalStateException">The client has been disposed.</exception>
    public void TimeoutNextRequest(int numberOfRequest) =>
        _native.TimeoutNextRequest(numberOfRequest);

    /// <summary>
    /// Seeds the offsets <see cref="ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec}, ListOffsetsOptions)"/> reports for <c>OffsetSpec.Earliest</c> —
    /// Java's <c>MockAdminClient.updateBeginningOffsets(Map)</c>. <b>Merges</b> into what
    /// was seeded before.
    /// </summary>
    /// <param name="newOffsets">The beginning offset per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="newOffsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been disposed.</exception>
    public void UpdateBeginningOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        _native.UpdateBeginningOffsets(newOffsets);

    /// <summary>
    /// Seeds the offsets <see cref="ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec}, ListOffsetsOptions)"/> reports for every <c>OffsetSpec</c> other
    /// than earliest and for-timestamp — Java's
    /// <c>MockAdminClient.updateEndOffsets(Map)</c>. <b>Merges</b>.
    /// </summary>
    /// <param name="newOffsets">The end offset per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="newOffsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been disposed.</exception>
    public void UpdateEndOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        _native.UpdateEndOffsets(newOffsets);

    /// <summary>
    /// Seeds the committed offsets <c>ListConsumerGroupOffsets</c> reports — Java's
    /// <c>MockAdminClient.updateConsumerGroupOffsets(Map)</c>. <b>Merges</b>.
    /// </summary>
    /// <param name="newOffsets">The committed offset per topic partition.</param>
    /// <remarks>
    /// The mock keys these by partition only and ignores the group id, so there is no group
    /// parameter; the real client has no such restriction.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="newOffsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been disposed.</exception>
    public void UpdateConsumerGroupOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        _native.UpdateConsumerGroupOffsets(newOffsets);

    /// <summary>
    /// Hides a topic from the next <paramref name="fetchesRemainingUntilVisible"/> listings and descriptions, as Java's
    /// <c>MockAdminClient.setFetchesRemainingUntilVisible(String, int)</c>; throws <see cref="ArgumentException"/>
    /// with the core's message for an unknown topic.
    /// </summary>
    public void SetFetchesRemainingUntilVisible(string topicName, int fetchesRemainingUntilVisible) =>
        _native.SetFetchesRemainingUntilVisible(topicName, fetchesRemainingUntilVisible);

    /// <summary>
    /// Builds a <see cref="MockAdminClient"/>, Java's <c>MockAdminClient.Builder</c>. Each setter returns this builder; a
    /// setter the core rejects throws <see cref="ArgumentException"/> with the core's message and leaves the
    /// builder unchanged. <see cref="Build"/> may be called again.
    /// </summary>
    public sealed class Builder
    {
        private readonly MockAdminClientBuilderCore _core;

        /// <summary>Creates a builder with one broker, Java's <c>new MockAdminClient.Builder()</c>.</summary>
        /// <exception cref="KafkaException">The core could not create the builder.</exception>
        public Builder()
            : this(MockAdminClientBuilderCore.New())
        {
        }

        internal Builder(MockAdminClientBuilderCore core)
        {
            _core = core;
        }

        /// <summary>Sets the cluster id, Java's <c>clusterId(String)</c>.</summary>
        /// <param name="clusterId">The cluster id.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="clusterId"/> is null.</exception>
        public Builder ClusterId(string clusterId)
        {
            _core.ClusterId(clusterId);
            return this;
        }

        /// <summary>Sets the brokers, Java's <c>brokers(List&lt;Node&gt;)</c>.</summary>
        /// <param name="brokers">The brokers.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="brokers"/> is null.</exception>
        public Builder Brokers(IReadOnlyList<Node> brokers)
        {
            _core.Brokers(brokers);
            return this;
        }

        /// <summary>Sets the number of brokers, Java's <c>numBrokers(int)</c>.</summary>
        /// <param name="numBrokers">The number of brokers.</param>
        /// <returns>This builder.</returns>
        public Builder NumBrokers(int numBrokers)
        {
            _core.NumBrokers(numBrokers);
            return this;
        }

        /// <summary>Makes the broker at <paramref name="index"/> the controller, Java's <c>controller(int)</c>.</summary>
        /// <param name="index">The index of the broker.</param>
        /// <returns>This builder.</returns>
        public Builder Controller(int index)
        {
            _core.Controller(index);
            return this;
        }

        /// <summary>Sets each broker's log directories, Java's <c>brokerLogDirs(List&lt;List&lt;String&gt;&gt;)</c>.</summary>
        /// <param name="brokerLogDirs">The log directories, one list per broker id.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="brokerLogDirs"/> is null.</exception>
        public Builder BrokerLogDirs(IReadOnlyList<IReadOnlyList<string>> brokerLogDirs)
        {
            _core.BrokerLogDirs(brokerLogDirs);
            return this;
        }

        /// <summary>Sets the default replication factor, Java's <c>defaultReplicationFactor(int)</c>.</summary>
        /// <param name="defaultReplicationFactor">The replication factor of a topic created without one.</param>
        /// <returns>This builder.</returns>
        public Builder DefaultReplicationFactor(int defaultReplicationFactor)
        {
            _core.DefaultReplicationFactor(defaultReplicationFactor);
            return this;
        }

        /// <summary>Sets whether the mock uses a KRaft controller, Java's <c>usingRaftController(boolean)</c>.</summary>
        /// <param name="usingRaftController">Whether the mock uses a KRaft controller.</param>
        /// <returns>This builder.</returns>
        public Builder UsingRaftController(bool usingRaftController)
        {
            _core.UsingRaftController(usingRaftController);
            return this;
        }

        /// <summary>Sets the default partition count, Java's <c>defaultPartitions(short)</c>.</summary>
        /// <param name="numPartitions">The partition count of a topic created without one.</param>
        /// <returns>This builder.</returns>
        public Builder DefaultPartitions(short numPartitions)
        {
            _core.DefaultPartitions(numPartitions);
            return this;
        }

        /// <summary>Sets the finalized feature levels, Java's <c>featureLevels(Map&lt;String, Short&gt;)</c>.</summary>
        /// <param name="featureLevels">The level per feature.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="featureLevels"/> is null.</exception>
        public Builder FeatureLevels(IReadOnlyDictionary<string, short> featureLevels)
        {
            _core.FeatureLevels(featureLevels);
            return this;
        }

        /// <summary>
        /// Sets the minimum supported feature levels, Java's <c>minSupportedFeatureLevels(Map&lt;String, Short&gt;)</c>.
        /// </summary>
        /// <param name="minSupportedFeatureLevels">The minimum supported level per feature.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="minSupportedFeatureLevels"/> is null.</exception>
        public Builder MinSupportedFeatureLevels(IReadOnlyDictionary<string, short> minSupportedFeatureLevels)
        {
            _core.MinSupportedFeatureLevels(minSupportedFeatureLevels);
            return this;
        }

        /// <summary>
        /// Sets the maximum supported feature levels, Java's <c>maxSupportedFeatureLevels(Map&lt;String, Short&gt;)</c>.
        /// </summary>
        /// <param name="maxSupportedFeatureLevels">The maximum supported level per feature.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="maxSupportedFeatureLevels"/> is null.</exception>
        public Builder MaxSupportedFeatureLevels(IReadOnlyDictionary<string, short> maxSupportedFeatureLevels)
        {
            _core.MaxSupportedFeatureLevels(maxSupportedFeatureLevels);
            return this;
        }

        /// <summary>
        /// Sets the configs overlaid onto every group's, Java's <c>defaultGroupConfigs(Map&lt;String, String&gt;)</c>.
        /// </summary>
        /// <param name="defaultGroupConfigs">The default group configs.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="defaultGroupConfigs"/> is null.</exception>
        public Builder DefaultGroupConfigs(IReadOnlyDictionary<string, string> defaultGroupConfigs)
        {
            _core.DefaultGroupConfigs(defaultGroupConfigs);
            return this;
        }

        /// <summary>Builds a mock from the current settings, Java's <c>build()</c>.</summary>
        /// <returns>The mock.</returns>
        /// <exception cref="ArgumentException">The settings name no broker or a controller that is not one of them.</exception>
        public MockAdminClient Build() => new MockAdminClient(NativeAdminClient.StartMock(_core.Build(), AdminFlavour.Sync));
    }
}
