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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.Admin</c>. Implemented by
/// <see cref="KafkaAdminClient"/> and, for broker-less tests, by
/// <see cref="MockAdminClient"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every RPC method is synchronous and returns a <c>*Result</c> holding one
/// awaitable per key.</b> Java's <c>Admin</c> methods do not block: <c>createTopics</c>
/// hands work to a background thread and returns instantly with a result object whose
/// <c>KafkaFuture</c>s the caller may await individually. Mapping such a method to
/// <c>Task&lt;CreateTopicsResult&gt;</c> would invent blocking Java does not have and
/// would collapse N per-key futures into one, so the <see cref="Task"/> mapping belongs
/// on the futures <em>inside</em> the result — not on the method.
/// </para>
/// <para>
/// That is also why there is a single interface here, where the producer and consumer
/// each ship a sync/async pair. Those pairs exist because their Java methods block,
/// leaving two defensible mappings; admin's do not, so a second interface would be a
/// synonym rather than a choice.
/// </para>
/// <para>
/// <see cref="Close(TimeSpan)"/> is the one exception: Java's <c>close(Duration)</c>
/// joins the background thread, so it blocks, so it maps to a <see cref="Task"/>.
/// </para>
/// <para>
/// <b>Thread safety.</b> Concurrent operations on one client are permitted — the admin
/// ABI has no single-owner access guard, unlike the consumer. Disposing while an
/// operation is in flight is safe and simply defers the native release until that
/// operation completes.
/// </para>
/// </remarks>
public interface IAdmin : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Creates topics — Java's <c>createTopics(Collection&lt;NewTopic&gt;,
    /// CreateTopicsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one awaitable per topic.
    /// </summary>
    /// <param name="newTopics">
    /// The topics to create. A repeated topic name yields one entry, as Java's
    /// map-keyed result does.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One awaitable per topic. A topic that fails faults only <em>its own</em>
    /// awaitable; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="newTopics"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="newTopics"/> contains a null element, or a topic carries a null
    /// configuration value.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative. Leave it <see langword="null"/> to use the
    /// client default — the ABI reads a negative timeout as "unset", so a negative value
    /// would be silently reinterpreted rather than honoured.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options = null);

    /// <summary>
    /// Deletes topics — Java's <c>deleteTopics(TopicCollection, DeleteTopicsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// awaitable per topic.
    /// </summary>
    /// <param name="topics">
    /// The topics to delete, identified <b>either</b> by name <b>or</b> by id. Build one
    /// with <see cref="TopicCollection.OfTopicNames"/> or
    /// <see cref="TopicCollection.OfTopicIds"/>; the choice selects which of the two ABI
    /// entry points runs, and which of
    /// <see cref="DeleteTopicsResult.TopicNameValues"/> /
    /// <see cref="DeleteTopicsResult.TopicIdValues"/> is non-null. A repeated topic yields
    /// one entry, as Java's map-keyed result does.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One awaitable per topic. A topic that fails faults only <em>its own</em>
    /// awaitable; a partially failed batch is not a failed call.
    /// </returns>
    /// <remarks>
    /// Java also declares <c>deleteTopics(Collection&lt;String&gt;)</c> convenience
    /// overloads, but they are <c>default</c> interface methods that simply call
    /// <c>TopicCollection.ofTopicNames(...)</c>. C# default interface methods need
    /// .NET Standard 2.1 and this binding's floor is netstandard2.0 — the same constraint
    /// that put <c>onPartitionsLost</c>'s default on
    /// <see cref="ConsumerRebalanceListenerBase"/> — so the one-line call is left to the
    /// caller rather than duplicated into both client classes.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> is a name collection containing a null element.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative. Leave it <see langword="null"/> to use the
    /// client default — the ABI reads a negative timeout as "unset", so a negative value
    /// would be silently reinterpreted rather than honoured.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null);

    /// <summary>
    /// Describes topics — Java's
    /// <c>describeTopics(TopicCollection, DescribeTopicsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// awaitable per topic.
    /// </summary>
    /// <param name="topics">
    /// The topics to describe, identified <b>either</b> by name <b>or</b> by id — see
    /// <see cref="DeleteTopics"/>. The choice also selects which of
    /// <see cref="DescribeTopicsResult.AllTopicNames"/> /
    /// <see cref="DescribeTopicsResult.AllTopicIds"/> returns a task rather than
    /// <see langword="null"/>.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// One awaitable per topic, each carrying that topic's own
    /// <see cref="TopicDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> is a name collection containing a null element.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> or <c>options.PartitionSizeLimitPerResponse</c> is
    /// negative — the ABI reads a negative of either as "unset" and would silently
    /// substitute a default.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null);

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
    /// ⚠ <b>One</b> awaitable over the whole listing, unlike every other RPC here. There
    /// is no per-topic outcome to report — the request carries no topic list, and the
    /// ABI's result type has no per-key error channel — so either the call fails and the
    /// task faults, or the whole map succeeds.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative. Leave it <see langword="null"/> to use the
    /// client default — the ABI reads a negative timeout as "unset", so a negative value
    /// would be silently reinterpreted rather than honoured.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    ListTopicsResult ListTopics(ListTopicsOptions? options = null);

    /// <summary>
    /// Increases the partition count of the given topics — Java's
    /// <c>createPartitions(Map&lt;String, NewPartitions&gt;, CreatePartitionsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// awaitable per topic.
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
    /// One awaitable per topic. A topic that fails faults only <em>its own</em>
    /// awaitable; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="newPartitions"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="newPartitions"/> contains a null topic name or a null entry.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options = null);

    /// <summary>
    /// Deletes the records before a given offset in each partition — Java's
    /// <c>deleteRecords(Map&lt;TopicPartition, RecordsToDelete&gt;, DeleteRecordsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// awaitable per partition.
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
    /// One awaitable per partition, each carrying that partition's own
    /// <see cref="DeletedRecords"/> or its own failure. ⚠ A resulting low watermark of
    /// <c>-1</c> is a <b>success</b> carrying <c>-1</c>, not a failure — see the note on
    /// <see cref="DeleteRecordsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="recordsToDelete"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="recordsToDelete"/> contains a topic partition with a null topic, or
    /// a null entry.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options = null);

    /// <summary>
    /// Describes the cluster — Java's <c>describeCluster(DescribeClusterOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults — notably
    /// <see cref="DescribeClusterOptions.IncludeAuthorizedOperations"/>
    /// <see langword="false"/>, so <see cref="DescribeClusterResult.AuthorizedOperations"/>
    /// yields <see langword="null"/>.
    /// </param>
    /// <returns>
    /// ⚠ <b>Four</b> awaitables — one per cluster attribute — rather than one per key. Two
    /// of them are genuinely nullable: see <see cref="DescribeClusterResult"/>.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative. Leave it <see langword="null"/> to use the
    /// client default — the ABI reads a negative timeout as "unset", so a negative value
    /// would be silently reinterpreted rather than honoured.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DescribeClusterResult DescribeCluster(DescribeClusterOptions? options = null);

    /// <summary>
    /// Lists the cluster's configuration resources — Java's
    /// <c>listConfigResources(Set&lt;ConfigResource.Type&gt;, ListConfigResourcesOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="configResourceTypes">
    /// The resource types to list. ⚠ <see langword="null"/> or an <b>empty</b> collection
    /// means <b>every supported type</b> — that is Java's own default, whose no-argument
    /// <c>listConfigResources()</c> delegates with <c>Set.of()</c>
    /// (<c>Admin.java:1812</c>) — so neither is rejected. Java's parameter is a
    /// <c>Set</c>, so a repeated type is one entry.
    /// </param>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> awaitable over the whole listing, and a <em>collection</em> rather than
    /// a map: there is no per-resource outcome to report, so either the call fails and the
    /// task faults, or the whole listing succeeds.
    /// </returns>
    /// <remarks>
    /// Java also declares a no-argument <c>listConfigResources()</c> convenience overload,
    /// collapsed here into the two optional parameters — the same treatment the other RPCs
    /// give Java's <c>default</c> overloads, and for the same reason (C# default interface
    /// methods need .NET Standard 2.1, above this binding's floor).
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes = null,
        ListConfigResourcesOptions? options = null);

    /// <summary>
    /// Lists the cluster's client-metrics resources — Java's
    /// <c>listClientMetricsResources(ListClientMetricsResourcesOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker.
    /// </summary>
    /// <param name="options">
    /// Request options, or <see langword="null"/> for Java's defaults.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> awaitable over the whole listing — see
    /// <see cref="ListConfigResources"/>.
    /// </returns>
    /// <remarks>
    /// ⚠ <b>Deprecated in Kafka since 4.1, and the deprecation is carried through rather
    /// than dropped</b> (<c>Admin.java:1821-1824</c>:
    /// <c>@Deprecated(since = "4.1", forRemoval = true)</c>). Java deprecates the result,
    /// options and listing types on the same grounds, so all four carry
    /// <see cref="ObsoleteAttribute"/> here. Prefer
    /// <see cref="ListConfigResources"/> filtered to
    /// <see cref="ConfigResourceType.ClientMetrics"/>.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    [Obsolete(
        "Deprecated in Kafka since 4.1. Use ListConfigResources filtered to "
        + "ConfigResourceType.ClientMetrics instead.")]
    ListClientMetricsResourcesResult ListClientMetricsResources(
        ListClientMetricsResourcesOptions? options = null);

    /// <summary>
    /// Describes the configuration of the given resources — Java's
    /// <c>describeConfigs(Collection&lt;ConfigResource&gt;, DescribeConfigsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// awaitable per resource.
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
    /// One awaitable per resource, each carrying that resource's own <see cref="Config"/>
    /// or its own failure. A resource that fails faults only <em>its own</em> awaitable;
    /// a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="resources"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="resources"/> contains a null element.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options = null);

    /// <summary>
    /// Incrementally alters the configuration of the given resources — Java's
    /// <c>incrementalAlterConfigs(Map&lt;ConfigResource, Collection&lt;AlterConfigOp&gt;&gt;,
    /// AlterConfigsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one awaitable per resource.
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
    /// One awaitable per resource, each reporting only whether <em>that</em> resource was
    /// altered — Java's per-resource future is <c>KafkaFuture&lt;Void&gt;</c>.
    /// </returns>
    /// <remarks>
    /// ⚠ <b>The result type is named for Java's return type</b>,
    /// <see cref="AlterConfigsResult"/> (<c>Admin.java:501, :530</c>) — there is no
    /// <c>IncrementalAlterConfigsResult</c> in Java or in the ABI.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configs"/> contains a null resource, a null operation collection, or
    /// a null operation.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options = null);

    /// <summary>
    /// Queries the log directories of the given brokers — Java's
    /// <c>describeLogDirs(Collection&lt;Integer&gt;, DescribeLogDirsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one awaitable
    /// per broker.
    /// </summary>
    /// <param name="brokers">
    /// The broker ids to query. A repeated broker yields one entry, as Java's map-keyed
    /// result does.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One awaitable per broker, each yielding that broker's log directories keyed by path.
    /// ⚠ A <b>log directory</b> that is offline does not fault anything — the awaitable
    /// succeeds and that directory's <see cref="LogDirDescription.Error"/> is non-null.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="brokers"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options = null);

    /// <summary>
    /// Moves the given replicas to new log directories — Java's
    /// <c>alterReplicaLogDirs(Map&lt;TopicPartitionReplica, String&gt;,
    /// AlterReplicaLogDirsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one awaitable per replica.
    /// </summary>
    /// <param name="replicaAssignment">Replica → the log directory to move it to.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>
    /// One awaitable per replica, each reporting only whether <em>that</em> move was
    /// accepted — Java's per-replica future is <c>KafkaFuture&lt;Void&gt;</c>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="replicaAssignment"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="replicaAssignment"/> contains a null replica or a null log directory —
    /// the ABI would silently skip such a row, leaving the caller holding an awaitable for a
    /// replica the broker was never asked about.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options = null);

    /// <summary>
    /// Queries the log directories of the given replicas — Java's
    /// <c>describeReplicaLogDirs(Collection&lt;TopicPartitionReplica&gt;,
    /// DescribeReplicaLogDirsOptions)</c>. Returns <b>immediately</b>, without waiting for
    /// the broker; the result carries one awaitable per replica.
    /// </summary>
    /// <param name="replicas">
    /// The replicas to query. A repeated replica yields one entry, as Java's map-keyed
    /// result does.
    /// </param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    /// <returns>One awaitable per replica, each carrying where that replica's log lives.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="replicas"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="replicas"/> contains a null element.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.TimeoutMs</c> is negative — see <see cref="ListTopics"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been closed.</exception>
    DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options = null);

    /// <summary>
    /// Closes the client, waiting up to <paramref name="timeout"/> for the background
    /// task to finish — Java's <c>close(Duration)</c>. Idempotent: closing an
    /// already-closed client completes without error.
    /// </summary>
    /// <param name="timeout">
    /// How long to wait. <see cref="TimeSpan.Zero"/> is valid (do not wait); for Java's
    /// no-argument <c>close()</c> — wait indefinitely — use
    /// <see cref="IDisposable.Dispose"/> or <see cref="IAsyncDisposable.DisposeAsync"/>.
    /// </param>
    /// <returns>A task that completes when the client is closed.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    Task Close(TimeSpan timeout);
}
