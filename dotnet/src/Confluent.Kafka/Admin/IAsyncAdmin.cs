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
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The asynchronous Kafka admin client, Java's <c>org.apache.kafka.clients.admin.Admin</c> with awaitable futures,
/// implemented by <see cref="AsyncKafkaAdminClient"/> and the broker-free <see cref="AsyncMockAdminClient"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every RPC returns its <c>Async*Result</c> at once, as Java's do, holding one <see cref="AsyncKafkaFuture{T}"/>
/// per key; <c>await future.Get()</c> is Java's <c>future.get()</c>. <c>Close</c> is the one method that blocks in
/// Java, so both its overloads return a <see cref="Task"/>.
/// </para>
/// <para>
/// The RPCs are <see cref="IAdmin"/>'s, with the same names, parameters, preconditions and messages; its remarks on
/// thread safety, disposal, strings and null keys apply here unchanged.
/// </para>
/// </remarks>
/// <seealso cref="IAdmin"/>
public interface IAsyncAdmin : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Creates topics — Java's <c>createTopics(Collection&lt;NewTopic&gt;,
    /// CreateTopicsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one future per topic.
    /// </summary>
    /// <param name="newTopics">
    /// The topics to create. A repeated topic name yields one entry, as Java's
    /// map-keyed result does.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per topic. A topic that fails faults only <em>its own</em>
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="newTopics"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="newTopics"/> contains a null element, or a topic name, a configuration
    /// name or a configuration value containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks), or a null configuration value, which the native
    /// client cannot send (see <see cref="NewTopic.Configs"/>). A null topic name is rejected
    /// by the <see cref="NewTopic"/> constructor.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncCreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options = null);

    /// <summary>
    /// Deletes topics — Java's <c>deleteTopics(TopicCollection, DeleteTopicsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// future per topic.
    /// </summary>
    /// <param name="topics">
    /// The topics to delete, identified <b>either</b> by name <b>or</b> by id. Build one
    /// with <see cref="TopicCollection.OfTopicNames"/> or
    /// <see cref="TopicCollection.OfTopicIds"/>; the choice selects which of the two ABI
    /// entry points runs, and which of
    /// <see cref="AsyncDeleteTopicsResult.TopicNameValues"/> /
    /// <see cref="AsyncDeleteTopicsResult.TopicIdValues"/> is non-null. A repeated topic yields
    /// one entry, as Java's map-keyed result does.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per topic. A topic that fails faults only <em>its own</em>
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> is a name collection containing a null element, or a
    /// name containing a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null);

    /// <summary>
    /// Deletes topics by name — Java's <c>deleteTopics(Collection&lt;String&gt;)</c> and
    /// <c>deleteTopics(Collection&lt;String&gt;, DeleteTopicsOptions)</c>. Like Java's, it wraps the names in
    /// <see cref="TopicCollection.OfTopicNames"/> and is otherwise
    /// <see cref="DeleteTopics(TopicCollection, DeleteTopicsOptions?)"/>.
    /// </summary>
    /// <param name="topicNames">The names of the topics to delete.</param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per topic, in <see cref="AsyncDeleteTopicsResult.TopicNameValues"/>.
    /// </returns>
    /// <remarks>
    /// A lone literal <see langword="null"/> argument matches both <c>DeleteTopics</c>
    /// overloads and does not compile (CS0121); cast it to the parameter type meant.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topicNames"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicNames"/> contains a null element, or a name containing a NUL
    /// character or an unpaired surrogate.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topicNames, DeleteTopicsOptions? options = null);

    /// <summary>
    /// Describes topics — Java's
    /// <c>describeTopics(TopicCollection, DescribeTopicsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// future per topic.
    /// </summary>
    /// <param name="topics">
    /// The topics to describe, identified <b>either</b> by name <b>or</b> by id — see
    /// <see cref="DeleteTopics(TopicCollection, DeleteTopicsOptions?)"/>. The choice also
    /// selects which of
    /// <see cref="AsyncDescribeTopicsResult.AllTopicNames"/> /
    /// <see cref="AsyncDescribeTopicsResult.AllTopicIds"/> returns a future rather than
    /// <see langword="null"/>.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per topic, each carrying that topic's own
    /// <see cref="TopicDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> is a name collection containing a null element, or a
    /// name containing a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null);

    /// <summary>
    /// Describes topics by name — Java's <c>describeTopics(Collection&lt;String&gt;)</c> and
    /// <c>describeTopics(Collection&lt;String&gt;, DescribeTopicsOptions)</c>. Like Java's, it wraps the names in
    /// <see cref="TopicCollection.OfTopicNames"/> and is otherwise
    /// <see cref="DescribeTopics(TopicCollection, DescribeTopicsOptions?)"/>.
    /// </summary>
    /// <param name="topicNames">The names of the topics to describe.</param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per topic, in <see cref="AsyncDescribeTopicsResult.TopicNameValues"/>.
    /// </returns>
    /// <remarks>
    /// A lone literal <see langword="null"/> argument matches both <c>DescribeTopics</c>
    /// overloads and does not compile (CS0121); cast it to the parameter type meant.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topicNames"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicNames"/> contains a null element, or a name containing a NUL
    /// character or an unpaired surrogate.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options = null);

    /// <summary>
    /// Lists the cluster's topics — Java's <c>listTopics(ListTopicsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults — notably
    /// <see cref="ListTopicsOptions.ListInternal"/> <see langword="false"/>, so internal
    /// topics such as <c>__consumer_offsets</c> are excluded.
    /// </param>
    /// <returns>
    /// <b>One</b> future over the whole listing: either the call fails, or the whole map succeeds.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListTopicsResult ListTopics(ListTopicsOptions? options = null);

    /// <summary>
    /// Increases the partition count of the given topics — Java's
    /// <c>createPartitions(Map&lt;String, NewPartitions&gt;, CreatePartitionsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// future per topic.
    /// </summary>
    /// <param name="newPartitions">
    /// Topic name → the new partition count for it (and, optionally, an explicit replica
    /// assignment). ⚠ <see cref="NewPartitions.IncreaseTo(int)"/> and
    /// <see cref="NewPartitions.IncreaseTo(int, IReadOnlyList{IReadOnlyList{int}})"/> with
    /// an <b>empty</b> list are <em>different requests</em>, and the broker treats them
    /// differently — see the null-versus-empty note on <see cref="NewPartitions"/>.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per topic. A topic that fails faults only <em>its own</em>
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="newPartitions"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="newPartitions"/> contains a null topic name or a null entry, or a
    /// topic name containing a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncCreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options = null);

    /// <summary>
    /// Deletes the records before a given offset in each partition — Java's
    /// <c>deleteRecords(Map&lt;TopicPartition, RecordsToDelete&gt;, DeleteRecordsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// future per partition.
    /// </summary>
    /// <param name="recordsToDelete">
    /// Topic partition → the offset before which its records are deleted. An offset of
    /// <c>-1</c> truncates that partition to its high watermark, as Java documents.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// <see cref="DeleteRecordsOptions"/> carries only a timeout, as Java's does.
    /// </param>
    /// <returns>
    /// One future per partition, each carrying that partition's own
    /// <see cref="DeletedRecords"/> or its own failure. ⚠ A resulting low watermark of
    /// <c>-1</c> is a <b>success</b> carrying <c>-1</c>, not a failure — see the note on
    /// <see cref="AsyncDeleteRecordsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="recordsToDelete"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="recordsToDelete"/> contains a topic partition with a null topic, or
    /// a null entry, or a topic containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options = null);

    /// <summary>
    /// Describes the cluster — Java's <c>describeCluster(DescribeClusterOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults — notably
    /// <see cref="DescribeClusterOptions.IncludeAuthorizedOperations"/>
    /// <see langword="false"/>, so <see cref="AsyncDescribeClusterResult.AuthorizedOperations"/>
    /// yields <see langword="null"/>.
    /// </param>
    /// <returns>
    /// ⚠ <b>Four</b> futures — one per cluster attribute — rather than one per key. Two
    /// of them are nullable — <see cref="AsyncDescribeClusterResult.Controller"/> and
    /// <see cref="AsyncDescribeClusterResult.AuthorizedOperations"/>: see
    /// <see cref="AsyncDescribeClusterResult"/>.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeClusterResult DescribeCluster(DescribeClusterOptions? options = null);

    /// <summary>
    /// Lists the cluster's configuration resources — Java's
    /// <c>listConfigResources(Set&lt;ConfigResource.Type&gt;, ListConfigResourcesOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="configResourceTypes">
    /// The resource types to list. ⚠ <see langword="null"/> or an <b>empty</b> collection
    /// means <b>every supported type</b>, as Java's no-argument <c>listConfigResources()</c>
    /// delegates with <c>Set.of()</c>, so neither is rejected. Java's parameter is a
    /// <c>Set</c>, so a repeated type is one entry.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole listing, and a <em>collection</em> rather than
    /// a map: there is no per-resource outcome to report, so either the call fails and the
    /// future faults, or the whole listing succeeds.
    /// </returns>
    /// <remarks>
    /// Java's no-argument <c>listConfigResources()</c> is this method with both parameters left out.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes = null,
        ListConfigResourcesOptions? options = null);

    /// <summary>
    /// Describes the configuration of the given resources — Java's
    /// <c>describeConfigs(Collection&lt;ConfigResource&gt;, DescribeConfigsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// future per resource.
    /// </summary>
    /// <param name="resources">
    /// The resources to describe. A repeated resource yields one entry, as Java's
    /// map-keyed result does.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults — notably
    /// <see cref="DescribeConfigsOptions.IncludeSynonyms"/> and
    /// <see cref="DescribeConfigsOptions.IncludeDocumentation"/> both
    /// <see langword="false"/>, so <see cref="ConfigEntry.Synonyms"/> comes back empty and
    /// <see cref="ConfigEntry.Documentation"/> <see langword="null"/>.
    /// </param>
    /// <returns>
    /// One future per resource, each carrying that resource's own <see cref="Config"/>
    /// or its own failure. A resource that fails faults only <em>its own</em> future;
    /// a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="resources"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="resources"/> contains a null element, or a resource name containing a NUL
    /// character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options = null);

    /// <summary>
    /// Incrementally alters the configuration of the given resources — Java's
    /// <c>incrementalAlterConfigs(Map&lt;ConfigResource, Collection&lt;AlterConfigOp&gt;&gt;,
    /// AlterConfigsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one future per resource.
    /// </summary>
    /// <param name="configs">
    /// Resource → the operations to apply to it, in order. ⚠ An
    /// <see cref="AlterConfigOpType.Delete"/> whose
    /// <see cref="ConfigEntry.Value"/> is <see langword="null"/> is a <b>real request</b>
    /// and the null reaches the broker as a null; it is not the same as an empty string.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per resource, each reporting only whether <em>that</em> resource was
    /// altered — Java's per-resource future is <c>KafkaFuture&lt;Void&gt;</c>.
    /// </returns>
    /// <remarks>
    /// The result type is Java's return type, <see cref="AlterConfigsResult"/>; there is no
    /// <c>IncrementalAlterConfigsResult</c> in Java.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configs"/> contains a null resource, a null operation collection, or
    /// a null operation, or a resource name, a config name or a config value containing a NUL
    /// character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks). A null
    /// config value (<see cref="AlterConfigOpType.Delete"/>'s) is <em>not</em> rejected.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options = null);

    /// <summary>
    /// Queries the log directories of the given brokers — Java's
    /// <c>describeLogDirs(Collection&lt;Integer&gt;, DescribeLogDirsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one future
    /// per broker.
    /// </summary>
    /// <param name="brokers">
    /// The broker ids to query. A repeated broker yields one entry, as Java's map-keyed
    /// result does.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future per broker, each yielding that broker's log directories keyed by path.
    /// ⚠ A <b>log directory</b> that is offline does not fault anything — the future
    /// succeeds and that directory's <see cref="LogDirDescription.Error"/> is non-null.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="brokers"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options = null);

    /// <summary>
    /// Moves the given replicas to new log directories — Java's
    /// <c>alterReplicaLogDirs(Map&lt;TopicPartitionReplica, String&gt;,
    /// AlterReplicaLogDirsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one future per replica.
    /// </summary>
    /// <param name="replicaAssignment">Replica → the log directory to move it to.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future per replica, each reporting only whether <em>that</em> move was
    /// accepted — Java's per-replica future is <c>KafkaFuture&lt;Void&gt;</c>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="replicaAssignment"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="replicaAssignment"/> contains a null replica or a null log directory.
    /// Also thrown for a replica whose topic, or a
    /// log directory, contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options = null);

    /// <summary>
    /// Queries the log directories of the given replicas — Java's
    /// <c>describeReplicaLogDirs(Collection&lt;TopicPartitionReplica&gt;,
    /// DescribeReplicaLogDirsOptions)</c>. Returns <b>immediately</b>, without waiting for
    /// the broker; the result carries one future per replica.
    /// </summary>
    /// <param name="replicas">
    /// The replicas to query. A repeated replica yields one entry, as Java's map-keyed
    /// result does.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>One future per replica, each carrying where that replica's log lives.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="replicas"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="replicas"/> contains a null element, or a replica whose topic
    /// contains a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options = null);

    /// <summary>
    /// Elects a leader for the given partitions — Java's
    /// <c>electLeaders(ElectionType, Set&lt;TopicPartition&gt;, ElectLeadersOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="electionType">The kind of election to conduct.</param>
    /// <param name="partitions">
    /// The partitions to elect leaders for, or <see langword="null"/> for <b>every
    /// partition in the cluster</b> — Java's null <c>Set</c>.
    /// ⚠ <see langword="null"/> and an <b>empty</b>
    /// collection are <em>different requests</em>: empty asks for an election over no
    /// partitions. Java's parameter is a <c>Set</c>, so a repeated partition is one entry.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole election, and a per-partition failure is a
    /// <b>value in its map</b> rather than a faulted future — Java's
    /// <c>Optional&lt;Throwable&gt;</c>. See <see cref="AsyncElectLeadersResult"/>; the future
    /// itself faults only when the election could not be run at all.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="electionType"/> is not one of <see cref="ElectionType.Preferred"/>
    /// / <see cref="ElectionType.Unclean"/> — a value Java's enum parameter cannot
    /// express, but a C# cast can.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic, or with a
    /// topic containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options = null);

    /// <summary>
    /// Changes or reverts the reassignment of one or more partitions — Java's
    /// <c>alterPartitionReassignments(Map&lt;TopicPartition, Optional&lt;NewPartitionReassignment&gt;&gt;,
    /// AlterPartitionReassignmentsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result carries one future per partition.
    /// </summary>
    /// <param name="reassignments">
    /// Topic partition → its new target replicas, or <see langword="null"/> to
    /// <b>revert</b> that partition's reassignment — Java's <c>Optional.empty()</c>.
    /// ⚠ Reverting is <em>not</em> the same as an empty
    /// replica list: <see cref="NewPartitionReassignment"/> rejects an empty list, as Java
    /// does, and the two travel on separate wires to the broker.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults — notably
    /// <see cref="AlterPartitionReassignmentsOptions.AllowReplicationFactorChange"/>
    /// <see langword="true"/>.
    /// </param>
    /// <returns>
    /// One future per partition, each reporting only whether <em>that</em>
    /// reassignment was initiated — Java's per-partition future is
    /// <c>KafkaFuture&lt;Void&gt;</c>. A partition that fails faults only its own
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="reassignments"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="reassignments"/> contains a topic partition with a null topic, or
    /// with a topic containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options = null);

    /// <summary>
    /// Alters the committed offsets for a consumer group — Java's
    /// <c>alterConsumerGroupOffsets(String, Map&lt;TopicPartition, OffsetAndMetadata&gt;,
    /// AlterConsumerGroupOffsetsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker.
    /// </summary>
    /// <param name="groupId">The consumer group id.</param>
    /// <param name="offsets">Topic partition → the offset (and metadata) to commit it to.</param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole request, and a per-partition failure is a
    /// <b>value in its map</b> rather than a faulted future — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c>. See <see cref="AsyncAlterConsumerGroupOffsetsResult"/>.
    /// The future itself faults only when the whole request failed or could not be submitted
    /// at all — never for one partition's own outcome — and it resolves only when the core
    /// reports, an empty <paramref name="offsets"/> included.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="offsets"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="offsets"/> contains a topic partition with a null topic, or a null
    /// <see cref="OffsetAndMetadata"/> value; or <paramref name="groupId"/>, a topic or an
    /// offset's metadata contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks). A null metadata is <em>not</em> rejected.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options = null);

    /// <summary>
    /// Deletes the committed offsets for a set of partitions in a consumer group — Java's
    /// <c>deleteConsumerGroupOffsets(String, Set&lt;TopicPartition&gt;,
    /// DeleteConsumerGroupOffsetsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker.
    /// </summary>
    /// <param name="groupId">The consumer group id.</param>
    /// <param name="partitions">The partitions whose committed offsets should be deleted.</param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole request, and a per-partition failure is a
    /// <b>value in its map</b> rather than a faulted future — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c>. See <see cref="AsyncDeleteConsumerGroupOffsetsResult"/>.
    /// The future itself faults only when the whole request failed or could not be submitted
    /// at all — never for one partition's own outcome — and it resolves only when the core
    /// reports, an empty <paramref name="partitions"/> included.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="partitions"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic; or
    /// <paramref name="groupId"/>, or a topic, contains a NUL character or an unpaired
    /// surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options = null);

    /// <summary>
    /// Lists the cluster's ongoing partition reassignments — Java's
    /// <c>listPartitionReassignments(Optional&lt;Set&lt;TopicPartition&gt;&gt;,
    /// ListPartitionReassignmentsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker.
    /// </summary>
    /// <param name="partitions">
    /// The partitions to ask about, or <see langword="null"/> for <b>every ongoing
    /// reassignment in the cluster</b> — Java's <c>Optional.empty()</c>.
    /// ⚠ <see langword="null"/> and an <b>empty</b>
    /// collection are <em>different requests</em>. Java's parameter is a <c>Set</c>, so a
    /// repeated partition is one entry.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// <b>One</b> future over the whole listing, as Java's. A requested partition with no
    /// ongoing reassignment is absent from the map; that is not an error.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic, or with a
    /// topic containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <remarks>
    /// <c>ListPartitionReassignments()</c> is Java's zero-argument
    /// <c>listPartitionReassignments()</c>. A lone literal <see langword="null"/> argument binds
    /// to <see cref="ListPartitionReassignments(ListPartitionReassignmentsOptions)"/>, not to
    /// <paramref name="partitions"/>: it still lists every reassignment, with a CS8625 warning.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions = null,
        ListPartitionReassignmentsOptions? options = null);

    /// <summary>
    /// Lists <b>every</b> ongoing partition reassignment in the cluster with the given
    /// options — Java's <c>listPartitionReassignments(ListPartitionReassignmentsOptions)</c>,
    /// which passes <c>Optional.empty()</c> for the partitions.
    /// Exactly
    /// <see cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}?, ListPartitionReassignmentsOptions?)"/>
    /// with a <see langword="null"/> partition set.
    /// </summary>
    /// <param name="options">
    /// Request options. A <see langword="null"/> one is forwarded unchanged and means
    /// Java's defaults.
    /// </param>
    /// <returns>
    /// One future over the whole listing — see
    /// <see cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}?, ListPartitionReassignmentsOptions?)"/>.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options);

    /// <summary>
    /// Lists offsets for the given partitions — Java's
    /// <c>listOffsets(Map&lt;TopicPartition, OffsetSpec&gt;, ListOffsetsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// future per partition.
    /// </summary>
    /// <param name="topicPartitionOffsets">
    /// Topic partition → which offset to retrieve for it. ⚠ Against a cluster,
    /// <see cref="OffsetSpec.ForTimestamp"/> with a value that happens to equal a wire
    /// sentinel sends the <b>same</b> request as the no-argument kind sharing that number —
    /// <c>ForTimestamp(-2)</c> and <see cref="OffsetSpec.Earliest"/> both become <c>-2</c>,
    /// as Java's <c>getOffsetFromSpec</c> collapses them before building the request.
    /// The two differ only up to that point, and
    /// on a <see cref="AsyncMockAdminClient"/>, which tells them apart.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults — notably
    /// <see cref="ListOffsetsOptions.IsolationLevel"/>
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.ReadUncommitted"/>.
    /// </param>
    /// <returns>
    /// One future per partition, each carrying that partition's own
    /// <see cref="ListOffsetsResult.ListOffsetsResultInfo"/> or its own failure. A
    /// partition that fails faults only <em>its own</em> future.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topicPartitionOffsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicPartitionOffsets"/> contains a topic partition with a null
    /// topic, or a topic containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks). A null spec is not rejected: it reads as
    /// <see cref="OffsetSpec.Latest"/>, as in Java.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.IsolationLevel</c> is not one of
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.ReadUncommitted"/> /
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.ReadCommitted"/> — a value Java's enum
    /// parameter cannot express, but a C# cast can.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions? options = null);

    /// <summary>
    /// Lists the groups available in the cluster — Java's
    /// <c>listGroups(ListGroupsOptions)</c>. Returns <b>immediately</b>, without waiting for
    /// the broker.
    /// </summary>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults. ⚠ Its three filters
    /// are <b>independent axes</b>, and each one left empty — the default — means "do not
    /// filter on that axis", never "match nothing"; so the no-options call lists groups of
    /// every state, protocol type and type.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> future carrying <em>two independent</em> collections: the listings
    /// that were returned, and the failures of the brokers that could not be queried. They
    /// are not parallel — one listing beside three errors is a legitimate outcome — so
    /// <see cref="AsyncListGroupsResult.Valid"/> and <see cref="AsyncListGroupsResult.Errors"/> each
    /// have their own length, and only <see cref="AsyncListGroupsResult.All"/> turns the first
    /// error into a fault.
    /// </returns>
    /// <remarks>
    /// Java's no-argument <c>listGroups()</c> is this method with the options left out.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// A filter holds a <see cref="GroupState"/> / <see cref="GroupType"/> value no member
    /// defines, which Java's enum-typed <c>Set</c> cannot express but a C# cast can.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <c>options.ProtocolTypes</c> holds a protocol type containing a NUL character or an
    /// unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListGroupsResult ListGroups(ListGroupsOptions? options = null);

    /// <summary>
    /// Describes some consumer groups in the cluster — Java's
    /// <c>describeConsumerGroups(Collection&lt;String&gt;, DescribeConsumerGroupsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries
    /// one future per group id.
    /// </summary>
    /// <remarks>
    /// Java's <c>describeConsumerGroups(Collection&lt;String&gt;)</c> is this method with the options left out.
    /// </remarks>
    /// <param name="groupIds">
    /// The IDs of the groups to describe. Duplicates are collapsed ordinally, so the
    /// result holds one future per distinct id.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults. Note
    /// <see cref="DescribeConsumerGroupsOptions.IncludeAuthorizedOperations"/> defaults to
    /// <see langword="false"/>, which leaves
    /// <see cref="ConsumerGroupDescription.AuthorizedOperations"/> <see langword="null"/>.
    /// </param>
    /// <returns>
    /// One future per group id, each carrying that group's own
    /// <see cref="ConsumerGroupDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupIds"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupIds"/> contains a null element, or a group id containing a NUL
    /// character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options = null);

    /// <summary>
    /// Describes some classic (non-KIP-848) groups in the cluster — Java's
    /// <c>describeClassicGroups(Collection&lt;String&gt;, DescribeClassicGroupsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries
    /// one future per group id.
    /// </summary>
    /// <remarks>
    /// Java's <c>describeClassicGroups(Collection&lt;String&gt;)</c> is this method with the options left out.
    /// </remarks>
    /// <param name="groupIds">
    /// The IDs of the groups to describe. Duplicates are collapsed ordinally, so the
    /// result holds one future per distinct id.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults. Note
    /// <see cref="DescribeClassicGroupsOptions.IncludeAuthorizedOperations"/> defaults to
    /// <see langword="false"/>, which leaves
    /// <see cref="ClassicGroupDescription.AuthorizedOperations"/> <see langword="null"/>.
    /// </param>
    /// <returns>
    /// One future per group id, each carrying that group's own
    /// <see cref="ClassicGroupDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupIds"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupIds"/> contains a null element, or a group id containing a NUL
    /// character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options = null);

    /// <summary>
    /// Lists the committed offsets of a single consumer group — Java's
    /// <c>listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries
    /// one future, keyed by <paramref name="groupId"/>.
    /// </summary>
    /// <remarks>
    /// Java's <c>listConsumerGroupOffsets(String)</c> is this method with the options left out. Like Java's, it
    /// delegates to the batched form with a fresh <see cref="ListConsumerGroupOffsetsSpec"/>, so every committed
    /// partition of the group is returned.
    /// </remarks>
    /// <param name="groupId">The ID of the group whose committed offsets to list.</param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future, keyed by <paramref name="groupId"/>, carrying that group's
    /// committed offsets or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupId"/> contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions? options = null);

    /// <summary>
    /// Lists the committed offsets of several consumer groups — Java's
    /// <c>listConsumerGroupOffsets(Map&lt;String, ListConsumerGroupOffsetsSpec&gt;,
    /// ListConsumerGroupOffsetsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result carries one future per group id.
    /// </summary>
    /// <remarks>
    /// Java's <c>listConsumerGroupOffsets(Map&lt;String, ListConsumerGroupOffsetsSpec&gt;)</c> is this method with
    /// the options left out.
    /// </remarks>
    /// <param name="groupSpecs">
    /// The groups to query, each with the partitions to restrict its result to. A spec
    /// whose <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions"/> is
    /// <see langword="null"/> returns every committed partition of that group; an
    /// <b>empty</b> collection returns nothing for it. The two are distinct.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per group id, each carrying that group's committed offsets or its
    /// own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupSpecs"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupSpecs"/> contains a null group id, a null spec, a null
    /// selected topic, or a repeated group id, or a group id or selected topic containing a
    /// NUL character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options = null);

    /// <summary>
    /// Deletes consumer groups from the cluster — Java's
    /// <c>deleteConsumerGroups(Collection&lt;String&gt;, DeleteConsumerGroupsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="groupIds">The consumer group ids to delete. Duplicates collapse.</param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per group id — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>.
    /// See <see cref="AsyncDeleteConsumerGroupsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupIds"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupIds"/> contains a null element, or a group id containing a NUL
    /// character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options = null);

    /// <summary>
    /// Removes members from a consumer group by their static member identity — Java's
    /// <c>removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="groupId">The id of the group to remove members from.</param>
    /// <param name="options">
    /// The members to remove, or a no-argument
    /// <see cref="RemoveMembersFromConsumerGroupOptions"/> to remove every member of the
    /// group. Unlike every other RPC on this interface, Java has <b>no</b> options-free
    /// overload for this one — <paramref name="options"/> is
    /// therefore required here too, not optional.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole request, and a per-member failure is a
    /// <b>value in its map</b> rather than a faulted future — Java's
    /// <c>Map&lt;MemberIdentity, Errors&gt;</c>. See
    /// <see cref="AsyncRemoveMembersFromConsumerGroupResult"/>. The future itself faults only when
    /// the whole request failed or could not be submitted at all — never for one member's
    /// own outcome, which in <c>removeAll</c> mode only
    /// <see cref="AsyncRemoveMembersFromConsumerGroupResult.All"/> reports.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="options"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupId"/>, <c>options.Reason</c> or a member's group instance id
    /// contains a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/>
    /// remarks). A null reason is <em>not</em> rejected.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncRemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options);

    /// <summary>
    /// Creates ACL bindings on the cluster — Java's
    /// <c>createAcls(Collection&lt;AclBinding&gt;, CreateAclsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="acls">
    /// The ACL bindings to create. Duplicates collapse, because Java keys its result on a
    /// map. <see cref="ResourcePattern"/> and <see cref="AccessControlEntry"/> reject the ANY
    /// and MATCH values at construction. Those constructors also store an enum value that is
    /// not a defined member as <c>Unknown</c>, as Java's <c>fromCode</c> does, so two bindings
    /// that differ only in such values are duplicates. A binding with an
    /// <c>Unknown</c> field still constructs, and fails its own future, as in Java.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per binding — Java's
    /// <c>Map&lt;AclBinding, KafkaFuture&lt;Void&gt;&gt;</c>. See
    /// <see cref="AsyncCreateAclsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="acls"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="acls"/> contains a null element, or a binding whose resource name,
    /// principal or host contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncCreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options = null);

    /// <summary>
    /// Deletes every ACL matching any of the given filters — Java's
    /// <c>deleteAcls(Collection&lt;AclBindingFilter&gt;, DeleteAclsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="filters">
    /// The filters to apply. Duplicates collapse, because Java keys its result on a map. A
    /// <see langword="null"/> name, principal or host matches <em>any</em> value, and is
    /// distinct from <c>""</c>, which filters on the literal empty string. The filter
    /// constructors store an enum value that is not a defined member as <c>Unknown</c>, as
    /// Java's <c>fromCode</c> does, so two filters that differ only in such values are
    /// duplicates.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per filter — Java's
    /// <c>Map&lt;AclBindingFilter, KafkaFuture&lt;FilterResults&gt;&gt;</c>. See
    /// <see cref="AsyncDeleteAclsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="filters"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="filters"/> contains a null element, or a filter whose resource name,
    /// principal or host contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks). A null name, principal or host means "any" and is
    /// accepted.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options = null);

    /// <summary>
    /// Lists every ACL matching the given filter — Java's
    /// <c>describeAcls(AclBindingFilter, DescribeAclsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="filter">
    /// The filter to apply — a <b>single</b> filter, as Java takes. A
    /// <see langword="null"/> name, principal or host matches <em>any</em> value, and is
    /// distinct from <c>""</c>, which filters on the literal empty string.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future over the whole matching collection — Java's
    /// <c>KafkaFuture&lt;Collection&lt;AclBinding&gt;&gt;</c>. See
    /// <see cref="AsyncDescribeAclsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="filter"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// The filter's resource name, principal or host contains a NUL character or an unpaired
    /// surrogate (see the <see cref="IAdmin"/> remarks). A null component — "any" — is
    /// <em>not</em> rejected.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options = null);

    /// <summary>
    /// Describes the quotas of every entity matching the given filter — Java's
    /// <c>describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="filter">
    /// The filter to apply. An empty, non-strict filter is
    /// <see cref="ClientQuotaFilter.All"/> and matches everything.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future over the whole map — Java's
    /// <c>KafkaFuture&lt;Map&lt;ClientQuotaEntity, Map&lt;String, Double&gt;&gt;&gt;</c>. See
    /// <see cref="AsyncDescribeClientQuotasResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="filter"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A component matches exactly but carries no match name, or a component's entity type
    /// or match name contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options = null);

    /// <summary>
    /// Alters the quotas of the given entities — Java's
    /// <c>alterClientQuotas(Collection&lt;ClientQuotaAlteration&gt;, AlterClientQuotasOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="entries">
    /// The alterations to apply. An <see cref="ClientQuotaAlteration.Op"/> with a null
    /// <see cref="ClientQuotaAlteration.Op.Value"/> <em>removes</em> that quota, while
    /// <c>0.0</c> sets it to zero. A <b>repeated entity</b> is accepted, as Java accepts it:
    /// every alteration is sent, in order, and the
    /// entity has one future in the result, as Java's per-entity future map collapses it.
    /// An alteration whose entity has <b>no</b> entity types is sent as given, as Java does,
    /// and is answered for that entity alone.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One future per entity — Java's <c>Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt;</c>.
    /// See <see cref="AsyncAlterClientQuotasResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="entries"/> contains a null element, or an alteration whose entity has
    /// a type or name, or one of whose ops has a quota name, containing a NUL character or an
    /// unpaired surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options = null);

    /// <summary>
    /// Describes users' SASL/SCRAM credentials — Java's
    /// <c>describeUserScramCredentials(List&lt;String&gt;, DescribeUserScramCredentialsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="users">
    /// The users to describe, or <see langword="null"/> (or empty) to describe <b>every</b>
    /// user — Java's no-argument overload. A null element is skipped, as Java skips it,
    /// so a list of only nulls describes every user
    /// too.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// The three derived accessors Java publishes. See
    /// <see cref="AsyncDescribeUserScramCredentialsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="users"/> contains a user name containing a NUL character or an unpaired
    /// surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users = null,
        DescribeUserScramCredentialsOptions? options = null);

    /// <summary>
    /// Upserts and deletes users' SASL/SCRAM credentials — Java's
    /// <c>alterUserScramCredentials(List&lt;UserScramCredentialAlteration&gt;, AlterUserScramCredentialsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="alterations">
    /// The alterations to apply. ⚠ A repeated user is <b>passed through</b>, as Java does: both
    /// rows reach the broker and collapse to one outcome keyed by that user. An <b>empty
    /// password</b> is likewise not rejected here — Java reports it per-user.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future per user — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>. See
    /// <see cref="AsyncAlterUserScramCredentialsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="alterations"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="alterations"/> contains a null element, or an alteration whose user
    /// name contains a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions? options = null);

    /// <summary>
    /// Creates a delegation token — Java's
    /// <c>createDelegationToken(CreateDelegationTokenOptions)</c>. Returns <b>immediately</b>,
    /// without waiting for the broker.
    /// </summary>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future over the issued token. See <see cref="AsyncCreateDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <c>options.Renewers</c> is null or contains a null element, or a renewer's or
    /// <c>options.Owner</c>'s principal type or name contains a NUL character or an unpaired
    /// surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncCreateDelegationTokenResult CreateDelegationToken(CreateDelegationTokenOptions? options = null);

    /// <summary>
    /// Renews a delegation token — Java's
    /// <c>renewDelegationToken(byte[], RenewDelegationTokenOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="hmac">
    /// The token's MAC, passed back <b>verbatim</b> from
    /// <see cref="Confluent.Kafka.Common.Security.Token.Delegation.DelegationToken.Hmac"/>. ⚠ It may contain interior zero
    /// bytes, so it must never be treated as a NUL-terminated string.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future over the new expiry timestamp. See <see cref="AsyncRenewDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="hmac"/> is null, on a real client; a mock fails the future with the core's
    /// <c>DelegationTokenNotFound</c> error instead, as Java's mock does.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncRenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options = null);

    /// <summary>
    /// Expires a delegation token — Java's
    /// <c>expireDelegationToken(byte[], ExpireDelegationTokenOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="hmac">
    /// The token's MAC, passed back verbatim — see <see cref="RenewDelegationToken"/>.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future over the expiry timestamp. See <see cref="AsyncExpireDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="hmac"/> is null, on a real client; a mock fails the future with the core's
    /// <c>DelegationTokenNotFound</c> error instead, as Java's mock does.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options = null);

    /// <summary>
    /// Describes delegation tokens — Java's
    /// <c>describeDelegationToken(DescribeDelegationTokenOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults. ⚠ A null
    /// <c>Owners</c> describes every token the caller may see, while an <b>empty</b> one
    /// filters by nothing.
    /// </param>
    /// <returns>
    /// One future over the token list. See <see cref="AsyncDescribeDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <c>options.Owners</c> contains a null element, or an owner whose principal type or
    /// name contains a NUL character or an unpaired surrogate (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options = null);

    /// <summary>
    /// Describes the cluster's finalized and supported features — Java's
    /// <c>describeFeatures(DescribeFeaturesOptions)</c>. Returns <b>immediately</b>, without
    /// waiting for the broker.
    /// </summary>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// The <b>single</b> future Java publishes, over the whole composite. See
    /// <see cref="AsyncDescribeFeaturesResult"/>.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions? options = null);

    /// <summary>
    /// Applies feature updates — Java's
    /// <c>updateFeatures(Map&lt;String, FeatureUpdate&gt;, UpdateFeaturesOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="featureUpdates">The update to apply to each feature, keyed by feature name.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One future per feature — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>. See
    /// <see cref="AsyncUpdateFeaturesResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="featureUpdates"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="featureUpdates"/> contains a null name, a null update, a name containing a
    /// NUL character or an unpaired surrogate (see the <see cref="IAdmin"/> remarks), or a name its
    /// own comparer lets appear twice; or, on a <see cref="AsyncKafkaAdminClient"/>, it is empty or contains a
    /// blank name, the two Java rejects before enqueuing anything. A <see cref="AsyncMockAdminClient"/> accepts
    /// an empty map, and returns an empty result, and a blank name, as Java's mock checks no names.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncUpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions? options = null);

    /// <summary>
    /// Unregisters a controller — Java's
    /// <c>unregisterController(int, UnregisterControllerOptions)</c>, which Java marks unstable.
    /// Returns <b>immediately</b>, without waiting for the controller.
    /// </summary>
    /// <param name="controllerId">The id of the controller to unregister.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>The <b>single</b> future Java publishes. See <see cref="AsyncUnregisterControllerResult"/>.</returns>
    /// <remarks>
    /// The future fails with a <see cref="KafkaException"/> carrying Java's code when the request
    /// times out, the broker is too old, the id is not a registered controller, the request does
    /// not reach the active controller, or it names the active controller. A
    /// <see cref="AsyncMockAdminClient"/> fails it with the UnsupportedVersion code (35) and an
    /// empty message unless it uses a raft controller, as Java's mock does.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncUnregisterControllerResult UnregisterController(int controllerId, UnregisterControllerOptions? options = null);

    /// <summary>
    /// Fences out every active producer using the given transactional ids — Java's
    /// <c>fenceProducers(Collection, FenceProducersOptions)</c>. Returns <b>immediately</b>,
    /// without waiting for the broker.
    /// </summary>
    /// <param name="transactionalIds">
    /// The transactional ids to fence. De-duplicated, because Java keys its result on a map.
    /// An empty collection is a valid request that succeeds with an empty result, as Java's is.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>One future per id, plus the three derived views. See <see cref="AsyncFenceProducersResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalIds"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalIds"/> contains a null element, which the ABI would skip
    /// silently, or an id containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncFenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options = null);

    /// <summary>
    /// Describes the ongoing transactions of the given transactional ids — Java's
    /// <c>describeTransactions(Collection, DescribeTransactionsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="transactionalIds">
    /// The transactional ids to describe. De-duplicated, because Java keys its result on a map.
    /// An empty collection is a valid request that succeeds with an empty result, as Java's is.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>One future per id. See <see cref="AsyncDescribeTransactionsResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalIds"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalIds"/> contains a null element, which the ABI would skip
    /// silently, or an id containing a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options = null);

    /// <summary>
    /// Describes the active producers of the given partitions — Java's
    /// <c>describeProducers(Collection, DescribeProducersOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="partitions">
    /// The partitions to describe. De-duplicated, because Java keys its result on a map. An
    /// empty collection is a valid request that succeeds with an empty result, as Java's is.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults. <c>BrokerId</c> is
    /// Java's <c>OptionalInt</c>: unset sends the request to each partition's leader.
    /// </param>
    /// <returns>One future per partition. See <see cref="AsyncDescribeProducersResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic, which the
    /// ABI would skip silently, or with a topic containing a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncDescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options = null);

    /// <summary>
    /// Lists the cluster's transactions, fanning out to every broker — Java's
    /// <c>listTransactions(ListTransactionsOptions)</c>. Returns <b>immediately</b>, without
    /// waiting for the brokers.
    /// </summary>
    /// <param name="options">
    /// Request filters, or <see langword="null"/> for Java's defaults (every state, every
    /// producer, no duration filter, no transactional-id pattern). ⚠ A negative
    /// <c>options.FilteredDuration</c> is <b>not</b> rejected: it is Java's own "no duration
    /// filter" default.
    /// </param>
    /// <returns>
    /// The three views over one broker-keyed future. ⚠ A broker that fails is not a call
    /// failure — see <see cref="AsyncListTransactionsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <c>options.FilteredTransactionalIdPattern</c> contains a NUL character or an unpaired
    /// surrogate (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncListTransactionsResult ListTransactions(ListTransactionsOptions? options = null);

    /// <summary>
    /// Aborts one hanging transaction on a partition — Java's
    /// <c>abortTransaction(AbortTransactionSpec, AbortTransactionOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="spec">Which transaction to abort.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>The single future Java publishes. See <see cref="AsyncAbortTransactionResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="spec"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="spec"/> carries a topic partition whose topic is null (constructible, as
    /// in Java, or a <c>default(TopicPartition)</c>) — a null topic cannot cross the C ABI —
    /// or whose topic contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncAbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options = null);

    /// <summary>
    /// Forcefully terminates the ongoing transaction of one transactional id — Java's
    /// <c>forceTerminateTransaction(String, TerminateTransactionOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="transactionalId">Whose transaction to terminate.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// The single future Java publishes — ⚠ as <c>Result()</c>, not <c>All()</c>. See
    /// <see cref="AsyncTerminateTransactionResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> contains a NUL character or an unpaired surrogate
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AsyncTerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions? options = null);

    /// <summary>
    /// Closes the client, waiting up to <paramref name="timeout"/> for the background
    /// task to finish — Java's <c>close(Duration)</c>. Idempotent: closing an
    /// already-closed client completes without error.
    /// </summary>
    /// <param name="timeout">
    /// How long to wait. <see cref="TimeSpan.Zero"/> is valid (do not wait); <see cref="Close(CancellationToken)"/>
    /// waits without a bound.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends only the caller's wait, with <see cref="OperationCanceledException"/>: the close goes on, and the client is
    /// freed when it ends.
    /// </param>
    /// <returns>A task that completes when the client is closed.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    Task Close(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the client, waiting without a bound until the operations in flight have completed — Java's
    /// <c>close()</c>. Idempotent. On <see cref="AsyncMockAdminClient"/> it does nothing, as Java's mock;
    /// <see cref="IAsyncDisposable.DisposeAsync"/> frees the mock.
    /// </summary>
    /// <param name="cancellationToken">
    /// Ends only the caller's wait, with <see cref="OperationCanceledException"/>: the close goes on, and the client is
    /// freed when it ends.
    /// </param>
    /// <returns>A task that completes when the client is closed.</returns>
    Task Close(CancellationToken cancellationToken = default);
}
