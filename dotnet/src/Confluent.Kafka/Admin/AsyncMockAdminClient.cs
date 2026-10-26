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
/// A broker-less, in-memory asynchronous admin client for tests, Java's <c>org.apache.kafka.clients.admin.MockAdminClient</c>.
/// </summary>
/// <remarks>
/// Every RPC runs through the same marshalling and teardown as the real client. Build one with <see cref="Create"/>
/// and its <see cref="Builder"/>, or with Java's two constructors. <c>Close</c> does nothing, as Java's mock does;
/// <c>Dispose</c> frees the mock.
/// </remarks>
/// <seealso cref="MockAdminClient"/>
public sealed class AsyncMockAdminClient : IAsyncAdmin
{
    /// <summary>
    /// The cluster id of a mock whose <see cref="Builder"/> sets none, Java's <c>MockAdminClient.DEFAULT_CLUSTER_ID</c>.
    /// </summary>
    public const string DefaultClusterId = MockAdminClient.DefaultClusterId;

    /// <summary>
    /// The log directories of each broker of a mock whose <see cref="Builder"/> sets none, Java's
    /// <c>MockAdminClient.DEFAULT_LOG_DIRS</c>.
    /// </summary>
    public static IReadOnlyList<string> DefaultLogDirs { get; } = MockAdminClient.DefaultLogDirs;

    private readonly NativeAdminClient _native;

    private AsyncMockAdminClient(NativeAdminClient native)
    {
        _native = native;
    }

    /// <summary>
    /// Creates a mock with one broker, Java's <c>Node.noNode()</c>, which is also the controller: Java's
    /// <c>new MockAdminClient()</c>.
    /// </summary>
    /// <exception cref="KafkaException">The core could not create the mock.</exception>
    public AsyncMockAdminClient()
        : this(NativeAdminClient.StartMock(MockAdminClientBuilderCore.NewMock(), AdminFlavour.Async))
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
    public AsyncMockAdminClient(IReadOnlyList<Node> brokers, Node controller)
        : this(NativeAdminClient.StartMock(MockAdminClientBuilderCore.NewMock(brokers, controller), AdminFlavour.Async))
    {
    }

    /// <summary>Returns a new <see cref="Builder"/> with one broker, Java's <c>MockAdminClient.create()</c>.</summary>
    /// <returns>The builder.</returns>
    /// <exception cref="KafkaException">The core could not create the builder.</exception>
    public static Builder Create() => new Builder(MockAdminClientBuilderCore.Create());

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
            TopicCollection.OfTopicNames(topicNames ?? throw new ArgumentNullException(nameof(topicNames))),
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
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every group id's future fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
    public AsyncDeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null) =>
        AsyncDeleteConsumerGroupsResult.OfClient(
            _native.DeleteConsumerGroups(groupIds, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so the call's one future fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
    public AsyncRemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        AsyncRemoveMembersFromConsumerGroupResult.OfClient(
            _native.RemoveMembersFromConsumerGroup(groupId, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every binding's future fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
    public AsyncCreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null) =>
        AsyncCreateAclsResult.OfClient(_native.CreateAcls(acls, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every filter's future fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
    public AsyncDeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null) =>
        AsyncDeleteAclsResult.OfClient(_native.DeleteAcls(filters, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so <see cref="AsyncDescribeAclsResult.Values"/> fails with a <see cref="NotSupportedException"/>
    /// carrying that message.
    /// </remarks>
    public AsyncDescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null) =>
        AsyncDescribeAclsResult.OfClient(_native.DescribeAcls(filter, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implement yet")</c> here, Java's own typo kept,
    /// so <see cref="AsyncDescribeClientQuotasResult.Entities"/> fails with a <see cref="NotSupportedException"/>
    /// carrying that message.
    /// </remarks>
    public AsyncDescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null) =>
        AsyncDescribeClientQuotasResult.OfClient(_native.DescribeClientQuotas(filter, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implement yet")</c> here, Java's own typo kept,
    /// so every entity's future fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
    public AsyncAlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null) =>
        AsyncAlterClientQuotasResult.OfClient(_native.AlterClientQuotas(entries, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every accessor fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
    public AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null) =>
        AsyncDescribeUserScramCredentialsResult.OfClient(
            _native.DescribeUserScramCredentials(users, options));

    /// <inheritdoc/>
    /// <remarks>
    /// Java's mock throws <c>UnsupportedOperationException("Not implemented yet")</c> here,
    /// so every user's future fails with a <see cref="NotSupportedException"/> carrying that message.
    /// </remarks>
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

    /// <summary>Does nothing, as Java's mock; <see cref="DisposeAsync"/> frees the mock.</summary>
    public Task Close(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>Does nothing and reads no timeout, as Java's mock; <see cref="DisposeAsync"/> frees the mock.</summary>
    public Task Close(TimeSpan timeout, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();

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
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
    public void TimeoutNextRequest(int numberOfRequest) =>
        _native.TimeoutNextRequest(numberOfRequest);

    /// <summary>
    /// Seeds the offsets <see cref="ListOffsets"/> reports for <c>OffsetSpec.Earliest</c> —
    /// Java's <c>MockAdminClient.updateBeginningOffsets(Map)</c>. <b>Merges</b> into what
    /// was seeded before.
    /// </summary>
    /// <param name="newOffsets">The beginning offset per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="newOffsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
    public void UpdateBeginningOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        _native.UpdateBeginningOffsets(newOffsets);

    /// <summary>
    /// Seeds the offsets <see cref="ListOffsets"/> reports for every <c>OffsetSpec</c> other
    /// than earliest and for-timestamp — Java's
    /// <c>MockAdminClient.updateEndOffsets(Map)</c>. <b>Merges</b>.
    /// </summary>
    /// <param name="newOffsets">The end offset per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="newOffsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key has a null topic, or a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
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
    /// A key has a null topic, or a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
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
    /// Builds a <see cref="AsyncMockAdminClient"/>, Java's <c>MockAdminClient.Builder</c>. Each setter returns this builder; a
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
        public AsyncMockAdminClient Build() => new AsyncMockAdminClient(NativeAdminClient.StartMock(_core.Build(), AdminFlavour.Async));
    }
}
