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
/// is what this constructor takes.
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
    public Task Close(TimeSpan timeout) => _native.Close(timeout);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
