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
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The synchronous Kafka admin client, Java's <c>org.apache.kafka.clients.admin.Admin</c>, implemented by
/// <see cref="KafkaAdminClient"/> and the broker-free <see cref="MockAdminClient"/>. <see cref="IAsyncAdmin"/> is its
/// asynchronous sibling over the same native client.
/// </summary>
/// <remarks>
/// <para>
/// Every RPC returns its <c>*Result</c> at once, as Java's do, holding one future per key; a future's <c>Get()</c>
/// blocks the calling thread, as Java's <c>KafkaFuture.get()</c> does. <see cref="Close(TimeSpan)"/>,
/// <see cref="Close()"/> and <see cref="IDisposable.Dispose"/> block too, as Java's <c>close</c> joins the background
/// thread; async callers use <see cref="IAsyncAdmin"/>.
/// </para>
/// <para>
/// Concurrent calls on one client are allowed. Disposing while a call is in flight defers the native release until
/// the call completes.
/// </para>
/// <para>
/// A string that cannot cross the C ABI unchanged is rejected: where a method's <see cref="ArgumentException"/>
/// entry says so, a string containing a NUL character throws before anything is sent. The native client receives
/// NUL-terminated UTF-8, so <c>"a\0b"</c> would name <c>"a"</c>. Java has no such limit. An unpaired UTF-16
/// surrogate is sent as <c>'?'</c>, as Java's <c>String.getBytes(UTF_8)</c> writes it.
/// </para>
/// <para>
/// A null key, element or argument is rejected for the whole call, before anything is sent, so the valid keys in the
/// same call are not sent either. This is stricter than Java, which fails only that key's future, because the C ABI
/// cannot carry a null key. To get Java's outcome for the other keys, leave the null ones out of the call.
/// </para>
/// </remarks>
/// <seealso cref="IAsyncAdmin"/>
public interface IAdmin : IDisposable
{
    /// <summary>Creates topics with the default options.</summary>
    /// <inheritdoc cref="CreateTopics(IEnumerable{NewTopic}, CreateTopicsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="CreateTopics(IEnumerable{NewTopic}, CreateTopicsOptions)" path="/returns"/>
    /// <inheritdoc cref="CreateTopics(IEnumerable{NewTopic}, CreateTopicsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.CreateTopics(IEnumerable{NewTopic})"/>
    CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics);

    /// <summary>
    /// Creates topics — Java's <c>createTopics(Collection&lt;NewTopic&gt;,
    /// CreateTopicsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one <see cref="KafkaFuture{T}"/> per topic, whose
    /// <c>Get()</c> blocks the calling thread until that topic completes.
    /// </summary>
    /// <param name="newTopics">
    /// The topics to create. A repeated topic name yields one entry, as Java's
    /// map-keyed result does.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per topic. A topic that fails fails only <em>its own</em>
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="newTopics"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="newTopics"/> holds no topic with a
    /// non-empty name, since nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="newTopics"/> contains a null element, or a topic name, a configuration
    /// name or a configuration value containing a NUL character
    /// (see the <see cref="IAdmin"/> remarks), or a null configuration value, which the native
    /// client cannot send (see <see cref="NewTopic.Configs()"/>). A null topic name is rejected
    /// by the <see cref="NewTopic"/> constructor.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.CreateTopics(IEnumerable{NewTopic}, CreateTopicsOptions)"/>
    CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions options);

    /// <summary>Deletes topics with the default options.</summary>
    /// <inheritdoc cref="DeleteTopics(TopicCollection, DeleteTopicsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DeleteTopics(TopicCollection, DeleteTopicsOptions)" path="/returns"/>
    /// <inheritdoc cref="DeleteTopics(TopicCollection, DeleteTopicsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DeleteTopics(TopicCollection)"/>
    DeleteTopicsResult DeleteTopics(TopicCollection topics);

    /// <summary>
    /// Deletes topics — Java's <c>deleteTopics(TopicCollection, DeleteTopicsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per topic, whose <c>Get()</c> blocks the calling thread until
    /// that topic completes.
    /// </summary>
    /// <param name="topics">
    /// The topics to delete, identified <b>either</b> by name <b>or</b> by id. Build one
    /// with <see cref="TopicCollection.OfTopicNames"/> or
    /// <see cref="TopicCollection.OfTopicIds"/>; the choice selects which of the two ABI
    /// entry points runs, and which of
    /// <see cref="DeleteTopicsResult.TopicNameValues()"/> /
    /// <see cref="DeleteTopicsResult.TopicIdValues()"/> is non-null. A repeated topic yields
    /// one entry, as Java's map-keyed result does.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per topic. A topic that fails fails only <em>its own</em>
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="topics"/> holds no non-empty name, or no
    /// id other than <see cref="Uuid.ZERO_UUID"/>, since nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> is a name collection containing a null element, or a
    /// name containing a NUL character (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DeleteTopics(TopicCollection, DeleteTopicsOptions)"/>
    DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions options);

    /// <summary>
    /// Deletes topics by name with the default options: the same as
    /// <see cref="DeleteTopics(TopicCollection)"/> with the names wrapped in <see cref="TopicCollection.OfTopicNames"/>.
    /// </summary>
    /// <inheritdoc cref="DeleteTopics(IReadOnlyCollection{string}, DeleteTopicsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DeleteTopics(IReadOnlyCollection{string}, DeleteTopicsOptions)" path="/returns"/>
    /// <inheritdoc cref="DeleteTopics(IReadOnlyCollection{string}, DeleteTopicsOptions)" path="/remarks"/>
    /// <inheritdoc cref="DeleteTopics(IReadOnlyCollection{string}, DeleteTopicsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DeleteTopics(IReadOnlyCollection{string})"/>
    DeleteTopicsResult DeleteTopics(IReadOnlyCollection<string> topics);

    /// <summary>
    /// Deletes topics by name with the given options: the same as
    /// <see cref="DeleteTopics(TopicCollection, DeleteTopicsOptions)"/> with the names wrapped in
    /// <see cref="TopicCollection.OfTopicNames"/>.
    /// </summary>
    /// <param name="topics">The names of the topics to delete.</param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per topic, in <see cref="DeleteTopicsResult.TopicNameValues()"/>.
    /// </returns>
    /// <remarks>
    /// A literal <see langword="null"/> for <paramref name="topics"/> matches two <c>DeleteTopics</c> overloads and
    /// does not compile (CS0121); cast it to the parameter type meant.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="topics"/> holds no non-empty name, since
    /// nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> contains a null element, or a name containing a NUL
    /// character.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DeleteTopics(IReadOnlyCollection{string}, DeleteTopicsOptions)"/>
    DeleteTopicsResult DeleteTopics(
        IReadOnlyCollection<string> topics, DeleteTopicsOptions options);

    /// <summary>Describes topics with the default options.</summary>
    /// <inheritdoc cref="DescribeTopics(TopicCollection, DescribeTopicsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeTopics(TopicCollection, DescribeTopicsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeTopics(TopicCollection, DescribeTopicsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeTopics(TopicCollection)"/>
    DescribeTopicsResult DescribeTopics(TopicCollection topics);

    /// <summary>
    /// Describes topics — Java's
    /// <c>describeTopics(TopicCollection, DescribeTopicsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per topic, whose <c>Get()</c> blocks the calling thread
    /// until that topic completes.
    /// </summary>
    /// <param name="topics">
    /// The topics to describe, identified <b>either</b> by name <b>or</b> by id — see
    /// <see cref="DeleteTopics(TopicCollection, DeleteTopicsOptions)"/>. The choice also
    /// selects which of
    /// <see cref="DescribeTopicsResult.AllTopicNames"/> /
    /// <see cref="DescribeTopicsResult.AllTopicIds"/> returns a future rather than
    /// <see langword="null"/>.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per topic, each carrying that topic's own
    /// <see cref="TopicDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="topics"/> is a collection of names with no
    /// non-empty name, since nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topics"/> is a name collection containing a null element, or a
    /// name containing a NUL character (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeTopics(TopicCollection, DescribeTopicsOptions)"/>
    DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions options);

    /// <summary>
    /// Describes topics by name with the default options: the same as
    /// <see cref="DescribeTopics(TopicCollection)"/> with the names wrapped in <see cref="TopicCollection.OfTopicNames"/>.
    /// </summary>
    /// <inheritdoc cref="DescribeTopics(IReadOnlyCollection{string}, DescribeTopicsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeTopics(IReadOnlyCollection{string}, DescribeTopicsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeTopics(IReadOnlyCollection{string}, DescribeTopicsOptions)" path="/remarks"/>
    /// <inheritdoc cref="DescribeTopics(IReadOnlyCollection{string}, DescribeTopicsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeTopics(IReadOnlyCollection{string})"/>
    DescribeTopicsResult DescribeTopics(IReadOnlyCollection<string> topicNames);

    /// <summary>
    /// Describes topics by name with the given options: the same as
    /// <see cref="DescribeTopics(TopicCollection, DescribeTopicsOptions)"/> with the names wrapped in
    /// <see cref="TopicCollection.OfTopicNames"/>.
    /// </summary>
    /// <param name="topicNames">The names of the topics to describe.</param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per topic, in <see cref="DescribeTopicsResult.TopicNameValues()"/>.
    /// </returns>
    /// <remarks>
    /// A literal <see langword="null"/> for <paramref name="topicNames"/> matches two <c>DescribeTopics</c> overloads
    /// and does not compile (CS0121); cast it to the parameter type meant.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topicNames"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="topicNames"/> holds no non-empty name,
    /// since nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicNames"/> contains a null element, or a name containing a NUL
    /// character.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeTopics(IReadOnlyCollection{string}, DescribeTopicsOptions)"/>
    DescribeTopicsResult DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions options);

    /// <summary>
    /// Lists the cluster's topics with the default options, which leave out internal topics such as
    /// <c>__consumer_offsets</c>.
    /// </summary>
    /// <inheritdoc cref="ListTopics(ListTopicsOptions)" path="/returns"/>
    /// <inheritdoc cref="ListTopics(ListTopicsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListTopics()"/>
    ListTopicsResult ListTopics();

    /// <summary>
    /// Lists the cluster's topics — Java's <c>listTopics(ListTopicsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// listing completes.
    /// </summary>
    /// <param name="options">
    /// The options for the request. By default <see cref="ListTopicsOptions.ShouldListInternal()"/> is
    /// <see langword="false"/>, so internal topics such as <c>__consumer_offsets</c> are excluded.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// <b>One</b> future over the whole listing: either the call fails, or the whole map succeeds.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListTopics(ListTopicsOptions)"/>
    ListTopicsResult ListTopics(ListTopicsOptions options);

    /// <summary>Increases the partition count of the given topics with the default options.</summary>
    /// <inheritdoc cref="CreatePartitions(IReadOnlyDictionary{string, NewPartitions}, CreatePartitionsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="CreatePartitions(IReadOnlyDictionary{string, NewPartitions}, CreatePartitionsOptions)" path="/returns"/>
    /// <inheritdoc cref="CreatePartitions(IReadOnlyDictionary{string, NewPartitions}, CreatePartitionsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.CreatePartitions(IReadOnlyDictionary{string, NewPartitions})"/>
    CreatePartitionsResult CreatePartitions(IReadOnlyDictionary<string, NewPartitions> newPartitions);

    /// <summary>
    /// Increases the partition count of the given topics — Java's
    /// <c>createPartitions(Map&lt;String, NewPartitions&gt;, CreatePartitionsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per topic, whose <c>Get()</c> blocks the calling thread until
    /// that topic completes.
    /// </summary>
    /// <param name="newPartitions">
    /// Topic name → the new partition count for it (and, optionally, an explicit replica
    /// assignment). ⚠ <see cref="NewPartitions.IncreaseTo(int)"/> and
    /// <see cref="NewPartitions.IncreaseTo(int, IReadOnlyList{IReadOnlyList{int}})"/> with
    /// an <b>empty</b> list are <em>different requests</em>, and the broker treats them
    /// differently — see the null-versus-empty note on <see cref="NewPartitions"/>.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per topic. A topic that fails fails only <em>its own</em>
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="newPartitions"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="newPartitions"/> is empty, since nothing
    /// is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="newPartitions"/> contains a null topic name or a null entry, or a
    /// topic name containing a NUL character (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.CreatePartitions(IReadOnlyDictionary{string, NewPartitions}, CreatePartitionsOptions)"/>
    CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions options);

    /// <summary>Deletes the records before a given offset in each partition, with the default options.</summary>
    /// <inheritdoc cref="DeleteRecords(IReadOnlyDictionary{TopicPartition, RecordsToDelete}, DeleteRecordsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DeleteRecords(IReadOnlyDictionary{TopicPartition, RecordsToDelete}, DeleteRecordsOptions)" path="/returns"/>
    /// <inheritdoc cref="DeleteRecords(IReadOnlyDictionary{TopicPartition, RecordsToDelete}, DeleteRecordsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DeleteRecords(IReadOnlyDictionary{TopicPartition, RecordsToDelete})"/>
    DeleteRecordsResult DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete);

    /// <summary>
    /// Deletes the records before a given offset in each partition — Java's
    /// <c>deleteRecords(Map&lt;TopicPartition, RecordsToDelete&gt;, DeleteRecordsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per partition, whose <c>Get()</c> blocks the calling thread
    /// until that partition completes.
    /// </summary>
    /// <param name="recordsToDelete">
    /// Topic partition → the offset before which its records are deleted. An offset of
    /// <c>-1</c> truncates that partition to its high watermark, as Java documents.
    /// </param>
    /// <param name="options">
    /// The options for the request. <see cref="DeleteRecordsOptions"/> carries only a timeout.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// One future per partition, each carrying that partition's own
    /// <see cref="DeletedRecords"/> or its own failure. ⚠ A resulting low watermark of
    /// <c>-1</c> is a <b>success</b> carrying <c>-1</c>, not a failure — see the note on
    /// <see cref="DeleteRecordsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="recordsToDelete"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="recordsToDelete"/> contains a topic partition with a null topic, or
    /// a null entry, or a topic containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DeleteRecords(IReadOnlyDictionary{TopicPartition, RecordsToDelete}, DeleteRecordsOptions)"/>
    DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions options);

    /// <summary>
    /// Describes the cluster with the default options, which leave out the authorized operations, so
    /// <see cref="DescribeClusterResult.AuthorizedOperations"/> yields <see langword="null"/>.
    /// </summary>
    /// <inheritdoc cref="DescribeCluster(DescribeClusterOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeCluster(DescribeClusterOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeCluster()"/>
    DescribeClusterResult DescribeCluster();

    /// <summary>
    /// Describes the cluster — Java's <c>describeCluster(DescribeClusterOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries four
    /// <see cref="KafkaFuture{T}"/>s, whose <c>Get()</c> blocks the calling thread until the
    /// description completes.
    /// </summary>
    /// <param name="options">
    /// The options for the request. By default <see cref="DescribeClusterOptions.IncludeAuthorizedOperations()"/>
    /// is <see langword="false"/>, so <see cref="DescribeClusterResult.AuthorizedOperations"/> yields
    /// <see langword="null"/>.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// ⚠ <b>Four</b> futures — one per cluster attribute — rather than one per key. Two
    /// of them are nullable — <see cref="DescribeClusterResult.Controller"/> and
    /// <see cref="DescribeClusterResult.AuthorizedOperations"/>: see
    /// <see cref="DescribeClusterResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeCluster(DescribeClusterOptions)"/>
    DescribeClusterResult DescribeCluster(DescribeClusterOptions options);

    /// <summary>Lists the cluster's configuration resources of every supported type, with the default options.</summary>
    /// <inheritdoc cref="ListConfigResources(IReadOnlyCollection{ConfigResourceType}, ListConfigResourcesOptions)" path="/returns"/>
    /// <inheritdoc cref="ListConfigResources(IReadOnlyCollection{ConfigResourceType}, ListConfigResourcesOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListConfigResources()"/>
    ListConfigResourcesResult ListConfigResources();

    /// <summary>
    /// Lists the cluster's configuration resources — Java's
    /// <c>listConfigResources(Set&lt;ConfigResource.Type&gt;, ListConfigResourcesOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// listing completes.
    /// </summary>
    /// <param name="configResourceTypes">
    /// The resource types to list, a repeated type counting once. ⚠ An <b>empty</b> collection lists
    /// <b>every supported type</b>.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole listing, and a <em>collection</em> rather than
    /// a map: there is no per-resource outcome to report, so either the call fails and the
    /// future fails, or the whole listing succeeds.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="configResourceTypes"/> or <paramref name="options"/> is null.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListConfigResources(IReadOnlyCollection{ConfigResourceType}, ListConfigResourcesOptions)"/>
    ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType> configResourceTypes,
        ListConfigResourcesOptions options);

    /// <summary>
    /// Describes the configuration of the given resources with the default options, which leave out synonyms and
    /// documentation, so <see cref="ConfigEntry.Synonyms()"/> comes back empty and
    /// <see cref="ConfigEntry.Documentation()"/> <see langword="null"/>.
    /// </summary>
    /// <inheritdoc cref="DescribeConfigs(IReadOnlyCollection{ConfigResource}, DescribeConfigsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeConfigs(IReadOnlyCollection{ConfigResource}, DescribeConfigsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeConfigs(IReadOnlyCollection{ConfigResource}, DescribeConfigsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeConfigs(IReadOnlyCollection{ConfigResource})"/>
    DescribeConfigsResult DescribeConfigs(IReadOnlyCollection<ConfigResource> resources);

    /// <summary>
    /// Describes the configuration of the given resources — Java's
    /// <c>describeConfigs(Collection&lt;ConfigResource&gt;, DescribeConfigsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per resource, whose <c>Get()</c> blocks the calling thread
    /// until that resource completes.
    /// </summary>
    /// <param name="resources">
    /// The resources to describe. A repeated resource yields one entry, as Java's
    /// map-keyed result does.
    /// </param>
    /// <param name="options">
    /// The options for the request. By default <see cref="DescribeConfigsOptions.IncludeSynonyms()"/> and
    /// <see cref="DescribeConfigsOptions.IncludeDocumentation()"/> are both <see langword="false"/>, so
    /// <see cref="ConfigEntry.Synonyms()"/> comes back empty and <see cref="ConfigEntry.Documentation()"/>
    /// <see langword="null"/>.
    /// A null is rejected unless nothing is sent.
    /// </param>
    /// <returns>
    /// One future per resource, each carrying that resource's own <see cref="Config"/>
    /// or its own failure. A resource that fails fails only <em>its own</em> future;
    /// a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="resources"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="resources"/> is empty, since nothing is
    /// sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="resources"/> contains a null element, or a resource name containing a NUL
    /// character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeConfigs(IReadOnlyCollection{ConfigResource}, DescribeConfigsOptions)"/>
    DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions options);

    /// <summary>Incrementally alters the configuration of the given resources with the default options.</summary>
    /// <inheritdoc cref="IncrementalAlterConfigs(IReadOnlyDictionary{ConfigResource, IReadOnlyCollection{AlterConfigOp}}, AlterConfigsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="IncrementalAlterConfigs(IReadOnlyDictionary{ConfigResource, IReadOnlyCollection{AlterConfigOp}}, AlterConfigsOptions)" path="/returns"/>
    /// <inheritdoc cref="IncrementalAlterConfigs(IReadOnlyDictionary{ConfigResource, IReadOnlyCollection{AlterConfigOp}}, AlterConfigsOptions)" path="/remarks"/>
    /// <inheritdoc cref="IncrementalAlterConfigs(IReadOnlyDictionary{ConfigResource, IReadOnlyCollection{AlterConfigOp}}, AlterConfigsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.IncrementalAlterConfigs(IReadOnlyDictionary{ConfigResource, IReadOnlyCollection{AlterConfigOp}})"/>
    AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs);

    /// <summary>
    /// Incrementally alters the configuration of the given resources — Java's
    /// <c>incrementalAlterConfigs(Map&lt;ConfigResource, Collection&lt;AlterConfigOp&gt;&gt;,
    /// AlterConfigsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one <see cref="KafkaFuture"/> per resource, whose <c>Get()</c>
    /// blocks the calling thread until that resource completes.
    /// </summary>
    /// <param name="configs">
    /// Resource → the operations to apply to it, in order. ⚠ An
    /// <see cref="OpType.DELETE"/> whose
    /// <see cref="ConfigEntry.Value()"/> is <see langword="null"/> is a <b>real request</b>
    /// and the null reaches the broker as a null; it is not the same as an empty string.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per resource, each reporting only whether <em>that</em> resource was
    /// altered — Java's per-resource future is a <see cref="KafkaFuture"/>, which carries no value.
    /// </returns>
    /// <remarks>
    /// The result type is Java's return type, <see cref="AlterConfigsResult"/>; there is no
    /// <c>IncrementalAlterConfigsResult</c> in Java.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="configs"/> is empty, since nothing is sent
    /// then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configs"/> contains a null resource, a null operation collection, or
    /// a null operation, or a resource name, a config name or a config value containing a NUL
    /// character (see the <see cref="IAdmin"/> remarks). A null
    /// config value (<see cref="OpType.DELETE"/>'s) is <em>not</em> rejected.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.IncrementalAlterConfigs(IReadOnlyDictionary{ConfigResource, IReadOnlyCollection{AlterConfigOp}}, AlterConfigsOptions)"/>
    AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions options);

    /// <summary>Queries the log directories of the given brokers with the default options.</summary>
    /// <inheritdoc cref="DescribeLogDirs(IReadOnlyCollection{int}, DescribeLogDirsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeLogDirs(IReadOnlyCollection{int}, DescribeLogDirsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeLogDirs(IReadOnlyCollection{int}, DescribeLogDirsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeLogDirs(IReadOnlyCollection{int})"/>
    DescribeLogDirsResult DescribeLogDirs(IReadOnlyCollection<int> brokers);

    /// <summary>
    /// Queries the log directories of the given brokers — Java's
    /// <c>describeLogDirs(Collection&lt;Integer&gt;, DescribeLogDirsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per broker, whose <c>Get()</c> blocks the calling thread
    /// until that broker completes.
    /// </summary>
    /// <param name="brokers">
    /// The broker ids to query. A repeated broker yields one entry, as Java's map-keyed
    /// result does.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per broker, each yielding that broker's log directories keyed by path.
    /// ⚠ A <b>log directory</b> that is offline does not fail anything — the future
    /// succeeds and that directory's <see cref="LogDirDescription.Error()"/> is non-null.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="brokers"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="brokers"/> is empty, since nothing is sent
    /// then.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeLogDirs(IReadOnlyCollection{int}, DescribeLogDirsOptions)"/>
    DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions options);

    /// <summary>Moves the given replicas to new log directories with the default options.</summary>
    /// <inheritdoc cref="AlterReplicaLogDirs(IReadOnlyDictionary{TopicPartitionReplica, string}, AlterReplicaLogDirsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="AlterReplicaLogDirs(IReadOnlyDictionary{TopicPartitionReplica, string}, AlterReplicaLogDirsOptions)" path="/returns"/>
    /// <inheritdoc cref="AlterReplicaLogDirs(IReadOnlyDictionary{TopicPartitionReplica, string}, AlterReplicaLogDirsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.AlterReplicaLogDirs(IReadOnlyDictionary{TopicPartitionReplica, string})"/>
    AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment);

    /// <summary>
    /// Moves the given replicas to new log directories — Java's
    /// <c>alterReplicaLogDirs(Map&lt;TopicPartitionReplica, String&gt;,
    /// AlterReplicaLogDirsOptions)</c>. Returns <b>immediately</b>, without waiting for the
    /// broker; the result carries one <see cref="KafkaFuture"/> per replica, whose <c>Get()</c>
    /// blocks the calling thread until that replica completes.
    /// </summary>
    /// <param name="replicaAssignment">Replica → the log directory to move it to.</param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future per replica, each reporting only whether <em>that</em> move was
    /// accepted — Java's per-replica future is a <see cref="KafkaFuture"/>, which carries no value.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="replicaAssignment"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="replicaAssignment"/> is empty, since
    /// nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="replicaAssignment"/> contains a null replica or a null log directory.
    /// Also thrown for a replica whose topic, or a
    /// log directory, contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.AlterReplicaLogDirs(IReadOnlyDictionary{TopicPartitionReplica, string}, AlterReplicaLogDirsOptions)"/>
    AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions options);

    /// <summary>Queries the log directories of the given replicas with the default options.</summary>
    /// <inheritdoc cref="DescribeReplicaLogDirs(IReadOnlyCollection{TopicPartitionReplica}, DescribeReplicaLogDirsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeReplicaLogDirs(IReadOnlyCollection{TopicPartitionReplica}, DescribeReplicaLogDirsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeReplicaLogDirs(IReadOnlyCollection{TopicPartitionReplica}, DescribeReplicaLogDirsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeReplicaLogDirs(IReadOnlyCollection{TopicPartitionReplica})"/>
    DescribeReplicaLogDirsResult DescribeReplicaLogDirs(IReadOnlyCollection<TopicPartitionReplica> replicas);

    /// <summary>
    /// Queries the log directories of the given replicas — Java's
    /// <c>describeReplicaLogDirs(Collection&lt;TopicPartitionReplica&gt;,
    /// DescribeReplicaLogDirsOptions)</c>. Returns <b>immediately</b>, without waiting for
    /// the broker; the result carries one <see cref="KafkaFuture{T}"/> per replica, whose
    /// <c>Get()</c> blocks the calling thread until that replica completes.
    /// </summary>
    /// <param name="replicas">
    /// The replicas to query. A repeated replica yields one entry, as Java's map-keyed
    /// result does.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>One future per replica, each carrying where that replica's log lives.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="replicas"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="replicas"/> is empty, since nothing is
    /// sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="replicas"/> contains a null element, or a replica whose topic
    /// contains a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeReplicaLogDirs(IReadOnlyCollection{TopicPartitionReplica}, DescribeReplicaLogDirsOptions)"/>
    DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions options);

    /// <summary>Elects a leader for the given partitions with the default options.</summary>
    /// <inheritdoc cref="ElectLeaders(ElectionType, IReadOnlyCollection{TopicPartition}, ElectLeadersOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ElectLeaders(ElectionType, IReadOnlyCollection{TopicPartition}, ElectLeadersOptions)" path="/returns"/>
    /// <inheritdoc cref="ElectLeaders(ElectionType, IReadOnlyCollection{TopicPartition}, ElectLeadersOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ElectLeaders(ElectionType, IReadOnlyCollection{TopicPartition})"/>
    ElectLeadersResult ElectLeaders(ElectionType electionType, IReadOnlyCollection<TopicPartition>? partitions);

    /// <summary>
    /// Elects a leader for the given partitions — Java's
    /// <c>electLeaders(ElectionType, Set&lt;TopicPartition&gt;, ElectLeadersOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// election completes.
    /// </summary>
    /// <param name="electionType">The kind of election to conduct.</param>
    /// <param name="partitions">
    /// The partitions to elect leaders for, or <see langword="null"/> for <b>every
    /// partition in the cluster</b> — Java's null <c>Set</c>.
    /// ⚠ <see langword="null"/> and an <b>empty</b>
    /// collection are <em>different requests</em>: empty asks for an election over no
    /// partitions. Java's parameter is a <c>Set</c>, so a repeated partition is one entry.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// ⚠ <b>One</b> future over the whole election, and a per-partition failure is a
    /// <b>value in its map</b> rather than a failed future — Java's
    /// <c>Optional&lt;Throwable&gt;</c>. See <see cref="ElectLeadersResult"/>; the future
    /// itself fails only when the election could not be run at all.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="electionType"/> is not one of <see cref="ElectionType.PREFERRED"/>
    /// / <see cref="ElectionType.UNCLEAN"/> — a value Java's enum parameter cannot
    /// express, but a C# cast can — read as <see cref="ElectionTypeMethods.ValueOf"/> reads it:
    /// <c>Value &lt;value&gt; must be one of [PREFERRED, UNCLEAN]</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic, or with a
    /// topic containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ElectLeaders(ElectionType, IReadOnlyCollection{TopicPartition}, ElectLeadersOptions)"/>
    ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions options);

    /// <summary>
    /// Changes or reverts the reassignment of one or more partitions with the default options, which allow a
    /// change of replication factor.
    /// </summary>
    /// <inheritdoc cref="AlterPartitionReassignments(IReadOnlyDictionary{TopicPartition, NewPartitionReassignment}, AlterPartitionReassignmentsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="AlterPartitionReassignments(IReadOnlyDictionary{TopicPartition, NewPartitionReassignment}, AlterPartitionReassignmentsOptions)" path="/returns"/>
    /// <inheritdoc cref="AlterPartitionReassignments(IReadOnlyDictionary{TopicPartition, NewPartitionReassignment}, AlterPartitionReassignmentsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.AlterPartitionReassignments(IReadOnlyDictionary{TopicPartition, NewPartitionReassignment})"/>
    AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments);

    /// <summary>
    /// Changes or reverts the reassignment of one or more partitions — Java's
    /// <c>alterPartitionReassignments(Map&lt;TopicPartition, Optional&lt;NewPartitionReassignment&gt;&gt;,
    /// AlterPartitionReassignmentsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result carries one <see cref="KafkaFuture"/> per partition, whose
    /// <c>Get()</c> blocks the calling thread until that partition completes.
    /// </summary>
    /// <param name="reassignments">
    /// Topic partition → its new target replicas, or <see langword="null"/> to
    /// <b>revert</b> that partition's reassignment — Java's <c>Optional.empty()</c>.
    /// ⚠ Reverting is <em>not</em> the same as an empty
    /// replica list: <see cref="NewPartitionReassignment"/> rejects an empty list, as Java
    /// does, and the two travel on separate wires to the broker.
    /// </param>
    /// <param name="options">
    /// The options for the request. By default
    /// <see cref="AlterPartitionReassignmentsOptions.AllowReplicationFactorChange()"/> is <see langword="true"/>.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// One future per partition, each reporting only whether <em>that</em>
    /// reassignment was initiated — Java's per-partition future is
    /// a <see cref="KafkaFuture"/>, which carries no value. A partition that fails fails only its own
    /// future; a partially failed batch is not a failed call.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="reassignments"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="reassignments"/> contains a topic partition with a null topic, or
    /// with a topic containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.AlterPartitionReassignments(IReadOnlyDictionary{TopicPartition, NewPartitionReassignment}, AlterPartitionReassignmentsOptions)"/>
    AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions options);

    /// <summary>Alters the committed offsets for a consumer group with the default options.</summary>
    /// <inheritdoc cref="AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, AlterConsumerGroupOffsetsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, AlterConsumerGroupOffsetsOptions)" path="/returns"/>
    /// <inheritdoc cref="AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, AlterConsumerGroupOffsetsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata})"/>
    AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets);

    /// <summary>
    /// Alters the committed offsets for a consumer group — Java's
    /// <c>alterConsumerGroupOffsets(String, Map&lt;TopicPartition, OffsetAndMetadata&gt;,
    /// AlterConsumerGroupOffsetsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result's futures share one native completion, and each
    /// <c>Get()</c> blocks the calling thread until the request completes.
    /// </summary>
    /// <param name="groupId">The consumer group id.</param>
    /// <param name="offsets">Topic partition → the offset (and metadata) to commit it to.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// ⚠ <b>One</b> completion over the whole request, and a per-partition failure is a
    /// <b>value in its map</b> rather than a failed completion — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c>. See <see cref="AlterConsumerGroupOffsetsResult"/>.
    /// The completion itself fails only when the whole request failed or could not be submitted
    /// at all — never for one partition's own outcome — and it resolves only when the core
    /// reports, an empty <paramref name="offsets"/> included.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="offsets"/> is null.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="offsets"/> contains a topic partition with a null topic, or a null
    /// <see cref="OffsetAndMetadata"/> value; or <paramref name="groupId"/>, a topic or an
    /// offset's metadata contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks). A null metadata is <em>not</em> rejected.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, AlterConsumerGroupOffsetsOptions)"/>
    AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions options);

    /// <summary>
    /// Deletes the committed offsets for a set of partitions in a consumer group, with the default options.
    /// </summary>
    /// <inheritdoc cref="DeleteConsumerGroupOffsets(string, IReadOnlyCollection{TopicPartition}, DeleteConsumerGroupOffsetsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DeleteConsumerGroupOffsets(string, IReadOnlyCollection{TopicPartition}, DeleteConsumerGroupOffsetsOptions)" path="/returns"/>
    /// <inheritdoc cref="DeleteConsumerGroupOffsets(string, IReadOnlyCollection{TopicPartition}, DeleteConsumerGroupOffsetsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DeleteConsumerGroupOffsets(string, IReadOnlyCollection{TopicPartition})"/>
    DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId, IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Deletes the committed offsets for a set of partitions in a consumer group — Java's
    /// <c>deleteConsumerGroupOffsets(String, Set&lt;TopicPartition&gt;,
    /// DeleteConsumerGroupOffsetsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result's futures share one native completion, and each
    /// <c>Get()</c> blocks the calling thread until the request completes.
    /// </summary>
    /// <param name="groupId">The consumer group id.</param>
    /// <param name="partitions">The partitions whose committed offsets should be deleted.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// ⚠ <b>One</b> completion over the whole request, and a per-partition failure is a
    /// <b>value in its map</b> rather than a failed completion — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c>. See <see cref="DeleteConsumerGroupOffsetsResult"/>.
    /// The completion itself fails only when the whole request failed or could not be submitted
    /// at all — never for one partition's own outcome — and it resolves only when the core
    /// reports, an empty <paramref name="partitions"/> included.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="partitions"/> is null.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic; or
    /// <paramref name="groupId"/>, or a topic, contains a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DeleteConsumerGroupOffsets(string, IReadOnlyCollection{TopicPartition}, DeleteConsumerGroupOffsetsOptions)"/>
    DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions options);

    /// <summary>Lists <b>every</b> ongoing partition reassignment in the cluster with the default options.</summary>
    /// <inheritdoc cref="ListPartitionReassignments(ListPartitionReassignmentsOptions)" path="/returns"/>
    /// <inheritdoc cref="ListPartitionReassignments(ListPartitionReassignmentsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListPartitionReassignments()"/>
    ListPartitionReassignmentsResult ListPartitionReassignments();

    /// <summary>Lists the ongoing reassignments of the given partitions with the default options.</summary>
    /// <inheritdoc cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}, ListPartitionReassignmentsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}, ListPartitionReassignmentsOptions)" path="/returns"/>
    /// <inheritdoc cref="ListPartitionReassignments(ListPartitionReassignmentsOptions)" path="/remarks"/>
    /// <inheritdoc cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}, ListPartitionReassignmentsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListPartitionReassignments(IReadOnlyCollection{TopicPartition})"/>
    ListPartitionReassignmentsResult ListPartitionReassignments(IReadOnlyCollection<TopicPartition>? partitions);

    /// <summary>
    /// Lists the cluster's ongoing partition reassignments — Java's
    /// <c>listPartitionReassignments(Optional&lt;Set&lt;TopicPartition&gt;&gt;,
    /// ListPartitionReassignmentsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result carries one <see cref="KafkaFuture{T}"/>, whose
    /// <c>Get()</c> blocks the calling thread until the listing completes.
    /// </summary>
    /// <param name="partitions">
    /// The partitions to ask about, or <see langword="null"/> for <b>every ongoing
    /// reassignment in the cluster</b> — Java's <c>Optional.empty()</c>.
    /// ⚠ <see langword="null"/> and an <b>empty</b>
    /// collection are <em>different requests</em>. Java's parameter is a <c>Set</c>, so a
    /// repeated partition is one entry.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// <b>One</b> future over the whole listing, as Java's. A requested partition with no
    /// ongoing reassignment is absent from the map; that is not an error.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="partitions"/> holds a partition with an
    /// empty topic or a negative partition number, since nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic, or with a
    /// topic containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListPartitionReassignments(IReadOnlyCollection{TopicPartition}, ListPartitionReassignmentsOptions)"/>
    ListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions,
        ListPartitionReassignmentsOptions options);

    /// <summary>
    /// Lists <b>every</b> ongoing partition reassignment in the cluster with the given
    /// options — Java's <c>listPartitionReassignments(ListPartitionReassignmentsOptions)</c>,
    /// which passes <c>Optional.empty()</c> for the partitions.
    /// Exactly
    /// <see cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}, ListPartitionReassignmentsOptions)"/>
    /// with a <see langword="null"/> partition set: the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// listing completes.
    /// </summary>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future over the whole listing — see
    /// <see cref="ListPartitionReassignments(IReadOnlyCollection{TopicPartition}, ListPartitionReassignmentsOptions)"/>.
    /// </returns>
    /// <remarks>
    /// A lone literal <see langword="null"/> argument matches both one-argument <c>ListPartitionReassignments</c>
    /// overloads and does not compile (CS0121); name the parameter (<c>partitions: null</c>) or cast it to the
    /// parameter type meant.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListPartitionReassignments(ListPartitionReassignmentsOptions)"/>
    ListPartitionReassignmentsResult ListPartitionReassignments(
        ListPartitionReassignmentsOptions options);

    /// <summary>
    /// Lists offsets for the given partitions with the default options, which read at
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.READ_UNCOMMITTED"/>.
    /// </summary>
    /// <inheritdoc cref="ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec}, ListOffsetsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec}, ListOffsetsOptions)" path="/returns"/>
    /// <inheritdoc cref="ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec}, ListOffsetsOptions)" path="/exception[not(contains(., 'options.')) and not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec})"/>
    ListOffsetsResult ListOffsets(IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets);

    /// <summary>
    /// Lists offsets for the given partitions — Java's
    /// <c>listOffsets(Map&lt;TopicPartition, OffsetSpec&gt;, ListOffsetsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per partition, whose <c>Get()</c> blocks the calling thread
    /// until that partition completes.
    /// </summary>
    /// <param name="topicPartitionOffsets">
    /// Topic partition → which offset to retrieve for it. ⚠ Against a cluster,
    /// <see cref="OffsetSpec.ForTimestamp"/> with a value that happens to equal a wire
    /// sentinel sends the <b>same</b> request as the no-argument kind sharing that number —
    /// <c>ForTimestamp(-2)</c> and <see cref="OffsetSpec.Earliest"/> both become <c>-2</c>,
    /// as Java's <c>getOffsetFromSpec</c> collapses them before building the request.
    /// The two differ only up to that point, and
    /// on a <see cref="MockAdminClient"/>, which tells them apart.
    /// </param>
    /// <param name="options">
    /// The options for the request. By default <see cref="ListOffsetsOptions.IsolationLevel()"/> is
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.READ_UNCOMMITTED"/>.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// One future per partition, each carrying that partition's own
    /// <see cref="ListOffsetsResult.ListOffsetsResultInfo"/> or its own failure. A
    /// partition that fails fails only <em>its own</em> future.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topicPartitionOffsets"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicPartitionOffsets"/> contains a topic partition with a null
    /// topic, or a topic containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks). A null spec is not rejected: it reads as
    /// <see cref="OffsetSpec.Latest"/>, as in Java.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// <c>options.IsolationLevel</c> is not one of
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.READ_UNCOMMITTED"/> /
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.READ_COMMITTED"/> — a value Java's enum
    /// parameter cannot express, but a C# cast can — read as
    /// <see cref="Confluent.Kafka.Common.IsolationLevelMethods.ForId"/> reads it:
    /// <c>Unknown isolation level &lt;id&gt;</c>.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListOffsets(IReadOnlyDictionary{TopicPartition, OffsetSpec}, ListOffsetsOptions)"/>
    ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions options);

    /// <summary>
    /// Lists the groups available in the cluster with the default options, which filter on nothing: groups of every
    /// state, protocol type and type.
    /// </summary>
    /// <inheritdoc cref="ListGroups(ListGroupsOptions)" path="/returns"/>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListGroups()"/>
    ListGroupsResult ListGroups();

    /// <summary>
    /// Lists the groups available in the cluster — Java's
    /// <c>listGroups(ListGroupsOptions)</c>. Returns <b>immediately</b>, without waiting for
    /// the broker; the result's three futures share one native completion, and each
    /// <c>Get()</c> blocks the calling thread until the listing completes.
    /// </summary>
    /// <param name="options">
    /// The options for the request. ⚠ Its three filters are <b>independent axes</b>, and each one left empty — the
    /// default — means "do not filter on that axis", never "match nothing"; so the default options list groups of
    /// every state, protocol type and type.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// ⚠ <b>One</b> completion carrying <em>two independent</em> collections: the listings
    /// that were returned, and the failures of the brokers that could not be queried. They
    /// are not parallel — one listing beside three errors is a legitimate outcome — so
    /// <see cref="ListGroupsResult.Valid"/> and <see cref="ListGroupsResult.Errors"/> each
    /// have their own length, and only <see cref="ListGroupsResult.All"/> turns the first
    /// error into a failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// A filter holds a <see cref="GroupState"/> / <see cref="GroupType"/> value no member
    /// defines, which Java's enum-typed <c>Set</c> cannot express but a C# cast can.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <c>options.ProtocolTypes</c> holds a protocol type containing a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListGroups(ListGroupsOptions)"/>
    ListGroupsResult ListGroups(ListGroupsOptions options);

    /// <summary>
    /// Describes some consumer groups in the cluster with the default options, which leave
    /// <see cref="ConsumerGroupDescription.AuthorizedOperations()"/> <see langword="null"/>.
    /// </summary>
    /// <inheritdoc cref="DescribeConsumerGroups(IReadOnlyCollection{string}, DescribeConsumerGroupsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeConsumerGroups(IReadOnlyCollection{string}, DescribeConsumerGroupsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeConsumerGroups(IReadOnlyCollection{string}, DescribeConsumerGroupsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeConsumerGroups(IReadOnlyCollection{string})"/>
    DescribeConsumerGroupsResult DescribeConsumerGroups(IReadOnlyCollection<string> groupIds);

    /// <summary>
    /// Describes some consumer groups in the cluster — Java's
    /// <c>describeConsumerGroups(Collection&lt;String&gt;, DescribeConsumerGroupsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries
    /// one <see cref="KafkaFuture{T}"/> per group id, whose <c>Get()</c> blocks the calling
    /// thread until that group completes.
    /// </summary>
    /// <param name="groupIds">
    /// The IDs of the groups to describe. Duplicates are collapsed ordinally, so the
    /// result holds one future per distinct id.
    /// </param>
    /// <param name="options">
    /// The options for the request. Note
    /// <see cref="DescribeConsumerGroupsOptions.IncludeAuthorizedOperations()"/> defaults to
    /// <see langword="false"/>, which leaves
    /// <see cref="ConsumerGroupDescription.AuthorizedOperations()"/> <see langword="null"/>.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// One future per group id, each carrying that group's own
    /// <see cref="ConsumerGroupDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupIds"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupIds"/> contains a null element, or a group id containing a NUL
    /// character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeConsumerGroups(IReadOnlyCollection{string}, DescribeConsumerGroupsOptions)"/>
    DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions options);

    /// <summary>
    /// Describes some classic (non-KIP-848) groups in the cluster with the default options, which leave
    /// <see cref="ClassicGroupDescription.AuthorizedOperations()"/> <see langword="null"/>.
    /// </summary>
    /// <inheritdoc cref="DescribeClassicGroups(IReadOnlyCollection{string}, DescribeClassicGroupsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeClassicGroups(IReadOnlyCollection{string}, DescribeClassicGroupsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeClassicGroups(IReadOnlyCollection{string}, DescribeClassicGroupsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeClassicGroups(IReadOnlyCollection{string})"/>
    DescribeClassicGroupsResult DescribeClassicGroups(IReadOnlyCollection<string> groupIds);

    /// <summary>
    /// Describes some classic (non-KIP-848) groups in the cluster — Java's
    /// <c>describeClassicGroups(Collection&lt;String&gt;, DescribeClassicGroupsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries
    /// one <see cref="KafkaFuture{T}"/> per group id, whose <c>Get()</c> blocks the calling
    /// thread until that group completes.
    /// </summary>
    /// <param name="groupIds">
    /// The IDs of the groups to describe. Duplicates are collapsed ordinally, so the
    /// result holds one future per distinct id.
    /// </param>
    /// <param name="options">
    /// The options for the request. Note
    /// <see cref="DescribeClassicGroupsOptions.IncludeAuthorizedOperations()"/> defaults to
    /// <see langword="false"/>, which leaves
    /// <see cref="ClassicGroupDescription.AuthorizedOperations()"/> <see langword="null"/>.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// One future per group id, each carrying that group's own
    /// <see cref="ClassicGroupDescription"/> or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupIds"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupIds"/> contains a null element, or a group id containing a NUL
    /// character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeClassicGroups(IReadOnlyCollection{string}, DescribeClassicGroupsOptions)"/>
    DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions options);

    /// <summary>Lists the committed offsets of a single consumer group with the default options.</summary>
    /// <inheritdoc cref="ListConsumerGroupOffsets(string, ListConsumerGroupOffsetsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ListConsumerGroupOffsets(string, ListConsumerGroupOffsetsOptions)" path="/returns"/>
    /// <inheritdoc cref="ListConsumerGroupOffsets(string, ListConsumerGroupOffsetsOptions)" path="/remarks"/>
    /// <inheritdoc cref="ListConsumerGroupOffsets(string, ListConsumerGroupOffsetsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListConsumerGroupOffsets(string)"/>
    ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(string groupId);

    /// <summary>
    /// Lists the committed offsets of a single consumer group — Java's
    /// <c>listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries
    /// one <see cref="KafkaFuture{T}"/>, keyed by <paramref name="groupId"/>, whose
    /// <c>Get()</c> blocks the calling thread until the group completes.
    /// </summary>
    /// <remarks>
    /// It runs the batched
    /// <see cref="ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec}, ListConsumerGroupOffsetsOptions)"/>
    /// with <paramref name="groupId"/> mapped to a new <see cref="ListConsumerGroupOffsetsSpec"/>, so every committed
    /// partition of the group is returned.
    /// </remarks>
    /// <param name="groupId">The ID of the group whose committed offsets to list.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future, keyed by <paramref name="groupId"/>, carrying that group's
    /// committed offsets or its own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupId"/> contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListConsumerGroupOffsets(string, ListConsumerGroupOffsetsOptions)"/>
    ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        string groupId, ListConsumerGroupOffsetsOptions options);

    /// <summary>Lists the committed offsets of several consumer groups with the default options.</summary>
    /// <inheritdoc cref="ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec}, ListConsumerGroupOffsetsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec}, ListConsumerGroupOffsetsOptions)" path="/returns"/>
    /// <inheritdoc cref="ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec}, ListConsumerGroupOffsetsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec})"/>
    ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs);

    /// <summary>
    /// Lists the committed offsets of several consumer groups — Java's
    /// <c>listConsumerGroupOffsets(Map&lt;String, ListConsumerGroupOffsetsSpec&gt;,
    /// ListConsumerGroupOffsetsOptions)</c>. Returns <b>immediately</b>, without waiting
    /// for the broker; the result carries one <see cref="KafkaFuture{T}"/> per group id, whose
    /// <c>Get()</c> blocks the calling thread until that group completes.
    /// </summary>
    /// <param name="groupSpecs">
    /// The groups to query, each with the partitions to restrict its result to. A spec
    /// whose <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions()"/> is
    /// <see langword="null"/> returns every committed partition of that group; an
    /// <b>empty</b> collection returns nothing for it. The two are distinct.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per group id, each carrying that group's committed offsets or its
    /// own failure.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupSpecs"/> is null.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupSpecs"/> contains a null group id, a null spec, a null
    /// selected topic, or a repeated group id, or a group id or selected topic containing a
    /// NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec}, ListConsumerGroupOffsetsOptions)"/>
    ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions options);

    /// <summary>Deletes consumer groups from the cluster with the default options.</summary>
    /// <inheritdoc cref="DeleteConsumerGroups(IReadOnlyCollection{string}, DeleteConsumerGroupsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DeleteConsumerGroups(IReadOnlyCollection{string}, DeleteConsumerGroupsOptions)" path="/returns"/>
    /// <inheritdoc cref="DeleteConsumerGroups(IReadOnlyCollection{string}, DeleteConsumerGroupsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DeleteConsumerGroups(IReadOnlyCollection{string})"/>
    DeleteConsumerGroupsResult DeleteConsumerGroups(IReadOnlyCollection<string> groupIds);

    /// <summary>
    /// Deletes consumer groups from the cluster — Java's
    /// <c>deleteConsumerGroups(Collection&lt;String&gt;, DeleteConsumerGroupsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per group id, whose <c>Get()</c> blocks the calling thread
    /// until that group completes.
    /// </summary>
    /// <param name="groupIds">The consumer group ids to delete. Duplicates collapse.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per group id — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>.
    /// See <see cref="DeleteConsumerGroupsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupIds"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupIds"/> contains a null element, or a group id containing a NUL
    /// character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DeleteConsumerGroups(IReadOnlyCollection{string}, DeleteConsumerGroupsOptions)"/>
    DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions options);

    /// <summary>
    /// Removes members from a consumer group by their static member identity — Java's
    /// <c>removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result's futures share
    /// one native completion, and each <c>Get()</c> blocks the calling thread until the request
    /// completes.
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
    /// ⚠ <b>One</b> completion over the whole request, and a per-member failure is a
    /// <b>value in its map</b> rather than a failed completion — Java's
    /// <c>Map&lt;MemberIdentity, Errors&gt;</c>. See
    /// <see cref="RemoveMembersFromConsumerGroupResult"/>. The completion itself fails only when
    /// the whole request failed or could not be submitted at all — never for one member's
    /// own outcome, which in <c>removeAll</c> mode only
    /// <see cref="RemoveMembersFromConsumerGroupResult.All"/> reports.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="options"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupId"/>, <c>options.Reason</c> or a member's group instance id
    /// contains a NUL character (see the <see cref="IAdmin"/>
    /// remarks). A null reason is <em>not</em> rejected.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.RemoveMembersFromConsumerGroup"/>
    RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options);

    /// <summary>Creates ACL bindings on the cluster with the default options.</summary>
    /// <inheritdoc cref="CreateAcls(IEnumerable{AclBinding}, CreateAclsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="CreateAcls(IEnumerable{AclBinding}, CreateAclsOptions)" path="/returns"/>
    /// <inheritdoc cref="CreateAcls(IEnumerable{AclBinding}, CreateAclsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.CreateAcls(IEnumerable{AclBinding})"/>
    CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls);

    /// <summary>
    /// Creates ACL bindings on the cluster — Java's
    /// <c>createAcls(Collection&lt;AclBinding&gt;, CreateAclsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per binding, whose <c>Get()</c> blocks the calling thread until
    /// that binding completes.
    /// </summary>
    /// <param name="acls">
    /// The ACL bindings to create. Duplicates collapse, because Java keys its result on a
    /// map. <see cref="ResourcePattern"/> and <see cref="AccessControlEntry"/> reject the ANY
    /// and MATCH values at construction. Those constructors also store an enum value that is
    /// not a defined member as <c>UNKNOWN</c>, as Java's <c>fromCode</c> does, so two bindings
    /// that differ only in such values are duplicates. A binding with an
    /// <c>UNKNOWN</c> field still constructs, and fails its own future, as in Java.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per binding — Java's
    /// <c>Map&lt;AclBinding, KafkaFuture&lt;Void&gt;&gt;</c>. See
    /// <see cref="CreateAclsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="acls"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="acls"/> contains a null element, or a binding whose resource name,
    /// principal or host contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.CreateAcls(IEnumerable{AclBinding}, CreateAclsOptions)"/>
    CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions options);

    /// <summary>Deletes every ACL matching any of the given filters, with the default options.</summary>
    /// <inheritdoc cref="DeleteAcls(IEnumerable{AclBindingFilter}, DeleteAclsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DeleteAcls(IEnumerable{AclBindingFilter}, DeleteAclsOptions)" path="/returns"/>
    /// <inheritdoc cref="DeleteAcls(IEnumerable{AclBindingFilter}, DeleteAclsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DeleteAcls(IEnumerable{AclBindingFilter})"/>
    DeleteAclsResult DeleteAcls(IEnumerable<AclBindingFilter> filters);

    /// <summary>
    /// Deletes every ACL matching any of the given filters — Java's
    /// <c>deleteAcls(Collection&lt;AclBindingFilter&gt;, DeleteAclsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per filter, whose <c>Get()</c> blocks the calling thread until
    /// that filter completes.
    /// </summary>
    /// <param name="filters">
    /// The filters to apply. Duplicates collapse, because Java keys its result on a map. A
    /// <see langword="null"/> name, principal or host matches <em>any</em> value, and is
    /// distinct from <c>""</c>, which filters on the literal empty string. The filter
    /// constructors store an enum value that is not a defined member as <c>UNKNOWN</c>, as
    /// Java's <c>fromCode</c> does, so two filters that differ only in such values are
    /// duplicates.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per filter — Java's
    /// <c>Map&lt;AclBindingFilter, KafkaFuture&lt;FilterResults&gt;&gt;</c>. See
    /// <see cref="DeleteAclsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="filters"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="filters"/> contains a null element, or a filter whose resource name,
    /// principal or host contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks). A null name, principal or host means "any" and is
    /// accepted.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DeleteAcls(IEnumerable{AclBindingFilter}, DeleteAclsOptions)"/>
    DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions options);

    /// <summary>Lists every ACL matching the given filter, with the default options.</summary>
    /// <inheritdoc cref="DescribeAcls(AclBindingFilter, DescribeAclsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeAcls(AclBindingFilter, DescribeAclsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeAcls(AclBindingFilter, DescribeAclsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeAcls(AclBindingFilter)"/>
    DescribeAclsResult DescribeAcls(AclBindingFilter filter);

    /// <summary>
    /// Lists every ACL matching the given filter — Java's
    /// <c>describeAcls(AclBindingFilter, DescribeAclsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// description completes.
    /// </summary>
    /// <param name="filter">
    /// The filter to apply — a <b>single</b> filter, as Java takes. A
    /// <see langword="null"/> name, principal or host matches <em>any</em> value, and is
    /// distinct from <c>""</c>, which filters on the literal empty string.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected unless nothing is sent.</param>
    /// <returns>
    /// One future over the whole matching collection — Java's
    /// <c>KafkaFuture&lt;Collection&lt;AclBinding&gt;&gt;</c>. See
    /// <see cref="DescribeAclsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="filter"/> is null.</exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> is null. Not thrown when <paramref name="filter"/> is unknown
    /// (<see cref="AclBindingFilter.IsUnknown()"/>), since nothing is sent then.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The filter's resource name, principal or host contains a NUL character (see the <see cref="IAdmin"/> remarks). A null component — "any" — is
    /// <em>not</em> rejected.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeAcls(AclBindingFilter, DescribeAclsOptions)"/>
    DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions options);

    /// <summary>Describes the quotas of every entity matching the given filter, with the default options.</summary>
    /// <inheritdoc cref="DescribeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeClientQuotas(ClientQuotaFilter)"/>
    DescribeClientQuotasResult DescribeClientQuotas(ClientQuotaFilter filter);

    /// <summary>
    /// Describes the quotas of every entity matching the given filter — Java's
    /// <c>describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// description completes.
    /// </summary>
    /// <param name="filter">
    /// The filter to apply. An empty, non-strict filter is
    /// <see cref="ClientQuotaFilter.All"/> and matches everything.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future over the whole map — Java's
    /// <c>KafkaFuture&lt;Map&lt;ClientQuotaEntity, Map&lt;String, Double&gt;&gt;&gt;</c>. See
    /// <see cref="DescribeClientQuotasResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="filter"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A component matches exactly but carries no match name, or a component's entity type
    /// or match name contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)"/>
    DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions options);

    /// <summary>Alters the quotas of the given entities with the default options.</summary>
    /// <inheritdoc cref="AlterClientQuotas(IEnumerable{ClientQuotaAlteration}, AlterClientQuotasOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="AlterClientQuotas(IEnumerable{ClientQuotaAlteration}, AlterClientQuotasOptions)" path="/returns"/>
    /// <inheritdoc cref="AlterClientQuotas(IEnumerable{ClientQuotaAlteration}, AlterClientQuotasOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.AlterClientQuotas(IEnumerable{ClientQuotaAlteration})"/>
    AlterClientQuotasResult AlterClientQuotas(IEnumerable<ClientQuotaAlteration> entries);

    /// <summary>
    /// Alters the quotas of the given entities — Java's
    /// <c>alterClientQuotas(Collection&lt;ClientQuotaAlteration&gt;, AlterClientQuotasOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per entity, whose <c>Get()</c> blocks the calling thread until
    /// that entity completes.
    /// </summary>
    /// <param name="entries">
    /// The alterations to apply. An <see cref="ClientQuotaAlteration.Op"/> with a null
    /// <see cref="ClientQuotaAlteration.Op.Value()"/> <em>removes</em> that quota, while
    /// <c>0.0</c> sets it to zero. A <b>repeated entity</b> is accepted, as Java accepts it:
    /// every alteration is sent, in order, and the
    /// entity has one future in the result, as Java's per-entity future map collapses it.
    /// An alteration whose entity has <b>no</b> entity types is sent as given, as Java does,
    /// and is answered for that entity alone.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per entity — Java's <c>Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt;</c>.
    /// See <see cref="AlterClientQuotasResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="entries"/> contains a null element, or an alteration whose entity has
    /// a type or name, or one of whose ops has a quota name, containing a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.AlterClientQuotas(IEnumerable{ClientQuotaAlteration}, AlterClientQuotasOptions)"/>
    AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions options);

    /// <summary>Describes <b>every</b> user's SASL/SCRAM credentials with the default options.</summary>
    /// <inheritdoc cref="DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)" path="/returns"/>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeUserScramCredentials()"/>
    DescribeUserScramCredentialsResult DescribeUserScramCredentials();

    /// <summary>Describes the given users' SASL/SCRAM credentials with the default options.</summary>
    /// <inheritdoc cref="DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeUserScramCredentials(IReadOnlyCollection{string})"/>
    DescribeUserScramCredentialsResult DescribeUserScramCredentials(IReadOnlyCollection<string>? users);

    /// <summary>
    /// Describes users' SASL/SCRAM credentials — Java's
    /// <c>describeUserScramCredentials(List&lt;String&gt;, DescribeUserScramCredentialsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries three
    /// <see cref="KafkaFuture{T}"/> accessors, whose <c>Get()</c> blocks the calling thread until
    /// the description completes.
    /// </summary>
    /// <param name="users">
    /// The users to describe, or <see langword="null"/> (or empty) to describe <b>every</b> user. A null element is
    /// skipped, so a list of only nulls describes every user too.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// The three derived accessors Java publishes. See
    /// <see cref="DescribeUserScramCredentialsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="users"/> contains a user name containing a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeUserScramCredentials(IReadOnlyCollection{string}, DescribeUserScramCredentialsOptions)"/>
    DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users,
        DescribeUserScramCredentialsOptions options);

    /// <summary>Upserts and deletes users' SASL/SCRAM credentials with the default options.</summary>
    /// <inheritdoc cref="AlterUserScramCredentials(IEnumerable{UserScramCredentialAlteration}, AlterUserScramCredentialsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="AlterUserScramCredentials(IEnumerable{UserScramCredentialAlteration}, AlterUserScramCredentialsOptions)" path="/returns"/>
    /// <inheritdoc cref="AlterUserScramCredentials(IEnumerable{UserScramCredentialAlteration}, AlterUserScramCredentialsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.AlterUserScramCredentials(IEnumerable{UserScramCredentialAlteration})"/>
    AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations);

    /// <summary>
    /// Upserts and deletes users' SASL/SCRAM credentials — Java's
    /// <c>alterUserScramCredentials(List&lt;UserScramCredentialAlteration&gt;, AlterUserScramCredentialsOptions)</c>.
    /// Returns <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per user, whose <c>Get()</c> blocks the calling thread until that
    /// user completes.
    /// </summary>
    /// <param name="alterations">
    /// The alterations to apply. ⚠ A repeated user is <b>passed through</b>, as Java does: both
    /// rows reach the broker and collapse to one outcome keyed by that user. An <b>empty
    /// password</b> is likewise not rejected here — Java reports it per-user.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per user — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>. See
    /// <see cref="AlterUserScramCredentialsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="alterations"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="alterations"/> contains a null element, or an alteration whose user
    /// name contains a NUL character (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.AlterUserScramCredentials(IEnumerable{UserScramCredentialAlteration}, AlterUserScramCredentialsOptions)"/>
    AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions options);

    /// <summary>Creates a delegation token with the default options.</summary>
    /// <inheritdoc cref="CreateDelegationToken(CreateDelegationTokenOptions)" path="/returns"/>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.CreateDelegationToken()"/>
    CreateDelegationTokenResult CreateDelegationToken();

    /// <summary>
    /// Creates a delegation token — Java's
    /// <c>createDelegationToken(CreateDelegationTokenOptions)</c>. Returns <b>immediately</b>,
    /// without waiting for the broker; the result carries one <see cref="KafkaFuture{T}"/>, whose
    /// <c>Get()</c> blocks the calling thread until the token is issued.
    /// </summary>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future over the issued token. See <see cref="CreateDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <c>options.Renewers</c> is null or contains a null element, or a renewer's or
    /// <c>options.Owner</c>'s principal type or name contains a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.CreateDelegationToken(CreateDelegationTokenOptions)"/>
    CreateDelegationTokenResult CreateDelegationToken(CreateDelegationTokenOptions options);

    /// <summary>Renews a delegation token with the default options.</summary>
    /// <inheritdoc cref="RenewDelegationToken(byte[], RenewDelegationTokenOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="RenewDelegationToken(byte[], RenewDelegationTokenOptions)" path="/returns"/>
    /// <inheritdoc cref="RenewDelegationToken(byte[], RenewDelegationTokenOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.RenewDelegationToken(byte[])"/>
    RenewDelegationTokenResult RenewDelegationToken(byte[] hmac);

    /// <summary>
    /// Renews a delegation token — Java's
    /// <c>renewDelegationToken(byte[], RenewDelegationTokenOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// renewal completes.
    /// </summary>
    /// <param name="hmac">
    /// The token's MAC, passed back <b>verbatim</b> from
    /// <see cref="Confluent.Kafka.Common.Security.Token.Delegation.DelegationToken.Hmac()"/>. ⚠ It may contain interior zero
    /// bytes, so it must never be treated as a NUL-terminated string.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future over the new expiry timestamp. See <see cref="RenewDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="hmac"/> is null, on a real client; a mock fails the future with the core's
    /// <c>DelegationTokenNotFound</c> error instead, as Java's mock does.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.RenewDelegationToken(byte[], RenewDelegationTokenOptions)"/>
    RenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions options);

    /// <summary>Expires a delegation token with the default options.</summary>
    /// <inheritdoc cref="ExpireDelegationToken(byte[], ExpireDelegationTokenOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ExpireDelegationToken(byte[], ExpireDelegationTokenOptions)" path="/returns"/>
    /// <inheritdoc cref="ExpireDelegationToken(byte[], ExpireDelegationTokenOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ExpireDelegationToken(byte[])"/>
    ExpireDelegationTokenResult ExpireDelegationToken(byte[] hmac);

    /// <summary>
    /// Expires a delegation token — Java's
    /// <c>expireDelegationToken(byte[], ExpireDelegationTokenOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// expiry completes.
    /// </summary>
    /// <param name="hmac">
    /// The token's MAC, passed back verbatim — see <see cref="RenewDelegationToken(byte[], RenewDelegationTokenOptions)"/>.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future over the expiry timestamp. See <see cref="ExpireDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="hmac"/> is null, on a real client; a mock fails the future with the core's
    /// <c>DelegationTokenNotFound</c> error instead, as Java's mock does.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ExpireDelegationToken(byte[], ExpireDelegationTokenOptions)"/>
    ExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions options);

    /// <summary>Describes every delegation token the caller may see, with the default options.</summary>
    /// <inheritdoc cref="DescribeDelegationToken(DescribeDelegationTokenOptions)" path="/returns"/>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeDelegationToken()"/>
    DescribeDelegationTokenResult DescribeDelegationToken();

    /// <summary>
    /// Describes delegation tokens — Java's
    /// <c>describeDelegationToken(DescribeDelegationTokenOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/>, whose <c>Get()</c> blocks the calling thread until the
    /// description completes.
    /// </summary>
    /// <param name="options">
    /// The options for the request. ⚠ A null <c>Owners</c>, the default, describes every token the caller may see,
    /// while an <b>empty</b> one filters by nothing.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// One future over the token list. See <see cref="DescribeDelegationTokenResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <c>options.Owners</c> contains a null element, or an owner whose principal type or
    /// name contains a NUL character (see the <see cref="IAdmin"/>
    /// remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeDelegationToken(DescribeDelegationTokenOptions)"/>
    DescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions options);

    /// <summary>Describes the cluster's finalized and supported features with the default options.</summary>
    /// <inheritdoc cref="DescribeFeatures(DescribeFeaturesOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeFeatures(DescribeFeaturesOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeFeatures()"/>
    DescribeFeaturesResult DescribeFeatures();

    /// <summary>
    /// Describes the cluster's finalized and supported features — Java's
    /// <c>describeFeatures(DescribeFeaturesOptions)</c>. Returns <b>immediately</b>, without
    /// waiting for the broker; the result carries one <see cref="KafkaFuture{T}"/>, whose
    /// <c>Get()</c> blocks the calling thread until the description completes.
    /// </summary>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// The <b>single</b> future Java publishes, over the whole composite. See
    /// <see cref="DescribeFeaturesResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeFeatures(DescribeFeaturesOptions)"/>
    DescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions options);

    /// <summary>Applies feature updates with the default options.</summary>
    /// <inheritdoc cref="UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate}, UpdateFeaturesOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate}, UpdateFeaturesOptions)" path="/returns"/>
    /// <inheritdoc cref="UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate}, UpdateFeaturesOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate})"/>
    UpdateFeaturesResult UpdateFeatures(IReadOnlyDictionary<string, FeatureUpdate> featureUpdates);

    /// <summary>
    /// Applies feature updates — Java's
    /// <c>updateFeatures(Map&lt;String, FeatureUpdate&gt;, UpdateFeaturesOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/> per feature, whose <c>Get()</c> blocks the calling thread until
    /// that feature completes.
    /// </summary>
    /// <param name="featureUpdates">The update to apply to each feature, keyed by feature name.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// One future per feature — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>. See
    /// <see cref="UpdateFeaturesResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="featureUpdates"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="featureUpdates"/> contains a null name, a null update, a name containing a
    /// NUL character (see the <see cref="IAdmin"/> remarks), or a name its
    /// own comparer lets appear twice; or, on a <see cref="KafkaAdminClient"/>, it is empty or contains a
    /// blank name, the two Java rejects before enqueuing anything. A <see cref="MockAdminClient"/> accepts
    /// an empty map, and returns an empty result, and a blank name, as Java's mock checks no names.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate}, UpdateFeaturesOptions)"/>
    UpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions options);

    /// <summary>Unregisters a controller with the default options.</summary>
    /// <inheritdoc cref="UnregisterController(int, UnregisterControllerOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="UnregisterController(int, UnregisterControllerOptions)" path="/returns"/>
    /// <inheritdoc cref="UnregisterController(int, UnregisterControllerOptions)" path="/remarks"/>
    /// <inheritdoc cref="UnregisterController(int, UnregisterControllerOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.UnregisterController(int)"/>
    UnregisterControllerResult UnregisterController(int controllerId);

    /// <summary>
    /// Unregisters a controller — Java's
    /// <c>unregisterController(int, UnregisterControllerOptions)</c>, which Java marks unstable.
    /// Returns <b>immediately</b>, without waiting for the controller; the result carries one
    /// <see cref="KafkaFuture"/>, whose <c>Get()</c> blocks the calling thread until the call
    /// completes.
    /// </summary>
    /// <param name="controllerId">The id of the controller to unregister.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>The <b>single</b> future Java publishes. See <see cref="UnregisterControllerResult"/>.</returns>
    /// <remarks>
    /// The future fails with a <see cref="KafkaException"/> carrying Java's code when the request
    /// times out, the broker is too old, the id is not a registered controller, the request does
    /// not reach the active controller, or it names the active controller. A
    /// <see cref="MockAdminClient"/> fails it with the UnsupportedVersion code (35) and an empty
    /// message unless it uses a raft controller, as Java's mock does.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.UnregisterController(int, UnregisterControllerOptions)"/>
    UnregisterControllerResult UnregisterController(int controllerId, UnregisterControllerOptions options);

    /// <summary>Fences out every active producer using the given transactional ids, with the default options.</summary>
    /// <inheritdoc cref="FenceProducers(IReadOnlyCollection{string}, FenceProducersOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="FenceProducers(IReadOnlyCollection{string}, FenceProducersOptions)" path="/returns"/>
    /// <inheritdoc cref="FenceProducers(IReadOnlyCollection{string}, FenceProducersOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.FenceProducers(IReadOnlyCollection{string})"/>
    FenceProducersResult FenceProducers(IReadOnlyCollection<string> transactionalIds);

    /// <summary>
    /// Fences out every active producer using the given transactional ids — Java's
    /// <c>fenceProducers(Collection, FenceProducersOptions)</c>. Returns <b>immediately</b>,
    /// without waiting for the broker; the result carries one <see cref="KafkaFuture"/> per id,
    /// whose <c>Get()</c> blocks the calling thread until that id completes.
    /// </summary>
    /// <param name="transactionalIds">
    /// The transactional ids to fence. De-duplicated, because Java keys its result on a map.
    /// An empty collection is a valid request that succeeds with an empty result, as Java's is.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>One future per id, plus the three derived views. See <see cref="FenceProducersResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalIds"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalIds"/> contains a null element, which the ABI would skip
    /// silently, or an id containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.FenceProducers(IReadOnlyCollection{string}, FenceProducersOptions)"/>
    FenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions options);

    /// <summary>Describes the ongoing transactions of the given transactional ids, with the default options.</summary>
    /// <inheritdoc cref="DescribeTransactions(IReadOnlyCollection{string}, DescribeTransactionsOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeTransactions(IReadOnlyCollection{string}, DescribeTransactionsOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeTransactions(IReadOnlyCollection{string}, DescribeTransactionsOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeTransactions(IReadOnlyCollection{string})"/>
    DescribeTransactionsResult DescribeTransactions(IReadOnlyCollection<string> transactionalIds);

    /// <summary>
    /// Describes the ongoing transactions of the given transactional ids — Java's
    /// <c>describeTransactions(Collection, DescribeTransactionsOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per id, whose <c>Get()</c> blocks the calling thread until
    /// that id completes.
    /// </summary>
    /// <param name="transactionalIds">
    /// The transactional ids to describe. De-duplicated, because Java keys its result on a map.
    /// An empty collection is a valid request that succeeds with an empty result, as Java's is.
    /// </param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>One future per id. See <see cref="DescribeTransactionsResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalIds"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalIds"/> contains a null element, which the ABI would skip
    /// silently, or an id containing a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeTransactions(IReadOnlyCollection{string}, DescribeTransactionsOptions)"/>
    DescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions options);

    /// <summary>
    /// Describes the active producers of the given partitions with the default options, which send the request to
    /// each partition's leader.
    /// </summary>
    /// <inheritdoc cref="DescribeProducers(IReadOnlyCollection{TopicPartition}, DescribeProducersOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="DescribeProducers(IReadOnlyCollection{TopicPartition}, DescribeProducersOptions)" path="/returns"/>
    /// <inheritdoc cref="DescribeProducers(IReadOnlyCollection{TopicPartition}, DescribeProducersOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.DescribeProducers(IReadOnlyCollection{TopicPartition})"/>
    DescribeProducersResult DescribeProducers(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Describes the active producers of the given partitions — Java's
    /// <c>describeProducers(Collection, DescribeProducersOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture{T}"/> per partition, whose <c>Get()</c> blocks the calling thread
    /// until that partition completes.
    /// </summary>
    /// <param name="partitions">
    /// The partitions to describe. De-duplicated, because Java keys its result on a map. An
    /// empty collection is a valid request that succeeds with an empty result, as Java's is.
    /// </param>
    /// <param name="options">
    /// The options for the request. An unset <c>BrokerId</c>, the default, sends the request to each partition's
    /// leader.
    /// A null is rejected.
    /// </param>
    /// <returns>One future per partition. See <see cref="DescribeProducersResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="partitions"/> contains a topic partition with a null topic, which the
    /// ABI would skip silently, or with a topic containing a NUL character
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.DescribeProducers(IReadOnlyCollection{TopicPartition}, DescribeProducersOptions)"/>
    DescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions options);

    /// <summary>
    /// Lists the cluster's transactions with the default options, which filter on nothing: every state, every
    /// producer, any duration and any transactional id.
    /// </summary>
    /// <inheritdoc cref="ListTransactions(ListTransactionsOptions)" path="/returns"/>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListTransactions()"/>
    ListTransactionsResult ListTransactions();

    /// <summary>
    /// Lists the cluster's transactions, fanning out to every broker — Java's
    /// <c>listTransactions(ListTransactionsOptions)</c>. Returns <b>immediately</b>, without
    /// waiting for the brokers; the result's views are <see cref="KafkaFuture{T}"/>s, whose
    /// <c>Get()</c> blocks the calling thread until that view completes.
    /// </summary>
    /// <param name="options">
    /// The request's filters; by default every state, every producer, no duration filter and no transactional-id
    /// pattern. ⚠ A negative <c>options.FilteredDuration</c> is <b>not</b> rejected: it is the default, meaning no
    /// duration filter.
    /// A null is rejected.
    /// </param>
    /// <returns>
    /// The three views over one broker-keyed future. ⚠ A broker that fails is not a call
    /// failure — see <see cref="ListTransactionsResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <c>options.FilteredTransactionalIdPattern</c> contains a NUL character (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <c>options.FilteredStates</c> holds a <see cref="TransactionState"/> value no member defines, which Java's
    /// enum-typed <c>Set</c> cannot express but a C# cast can.
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ListTransactions(ListTransactionsOptions)"/>
    ListTransactionsResult ListTransactions(ListTransactionsOptions options);

    /// <summary>Aborts one hanging transaction on a partition, with the default options.</summary>
    /// <inheritdoc cref="AbortTransaction(AbortTransactionSpec, AbortTransactionOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="AbortTransaction(AbortTransactionSpec, AbortTransactionOptions)" path="/returns"/>
    /// <inheritdoc cref="AbortTransaction(AbortTransactionSpec, AbortTransactionOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.AbortTransaction(AbortTransactionSpec)"/>
    AbortTransactionResult AbortTransaction(AbortTransactionSpec spec);

    /// <summary>
    /// Aborts one hanging transaction on a partition — Java's
    /// <c>abortTransaction(AbortTransactionSpec, AbortTransactionOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/>, whose <c>Get()</c> blocks the calling thread until the abort
    /// completes.
    /// </summary>
    /// <param name="spec">Which transaction to abort.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>The single future Java publishes. See <see cref="AbortTransactionResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="spec"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="spec"/> carries a topic partition whose topic is null (constructible, as
    /// in Java, or a <c>default(TopicPartition)</c>) — a null topic cannot cross the C ABI —
    /// or whose topic contains a NUL character (see the
    /// <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.AbortTransaction(AbortTransactionSpec, AbortTransactionOptions)"/>
    AbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions options);

    /// <summary>Forcefully terminates the ongoing transaction of one transactional id, with the default options.</summary>
    /// <inheritdoc cref="ForceTerminateTransaction(string, TerminateTransactionOptions)" path="/param[@name!='options']"/>
    /// <inheritdoc cref="ForceTerminateTransaction(string, TerminateTransactionOptions)" path="/returns"/>
    /// <inheritdoc cref="ForceTerminateTransaction(string, TerminateTransactionOptions)" path="/exception[not(paramref[@name='options'])]"/>
    /// <seealso cref="IAsyncAdmin.ForceTerminateTransaction(string)"/>
    TerminateTransactionResult ForceTerminateTransaction(string transactionalId);

    /// <summary>
    /// Forcefully terminates the ongoing transaction of one transactional id — Java's
    /// <c>forceTerminateTransaction(String, TerminateTransactionOptions)</c>. Returns
    /// <b>immediately</b>, without waiting for the broker; the result carries one
    /// <see cref="KafkaFuture"/>, whose <c>Get()</c> blocks the calling thread until the
    /// termination completes.
    /// </summary>
    /// <param name="transactionalId">Whose transaction to terminate.</param>
    /// <param name="options">The options for the request. A null is rejected.</param>
    /// <returns>
    /// The single future Java publishes — ⚠ as <c>Result()</c>, not <c>All()</c>. See
    /// <see cref="TerminateTransactionResult"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> contains a NUL character
    /// (see the <see cref="IAdmin"/> remarks).
    /// </exception>
    /// <exception cref="IllegalStateException">The client has been closed.</exception>
    /// <seealso cref="IAsyncAdmin.ForceTerminateTransaction(string, TerminateTransactionOptions)"/>
    TerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions options);

    /// <summary>
    /// Closes the client, <b>blocking the calling thread</b> for up to
    /// <paramref name="timeout"/> while the background task finishes — Java's
    /// <c>close(Duration)</c>. Idempotent: closing an already-closed client returns at once,
    /// without error.
    /// </summary>
    /// <remarks>
    /// Operations still pending when the timeout passes are failed by the core, as Java's
    /// <c>close(Duration)</c> fails them; their futures complete with that error, so a thread
    /// blocked in a future's <c>Get()</c> wakes and throws it.
    /// </remarks>
    /// <param name="timeout">
    /// How long to wait. <see cref="TimeSpan.Zero"/> is valid (do not wait); <see cref="Close()"/> waits without a
    /// bound.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <seealso cref="IAsyncAdmin.Close(TimeSpan, System.Threading.CancellationToken)"/>
    void Close(TimeSpan timeout);

    /// <summary>
    /// Closes the client, <b>blocking the calling thread</b> without a bound until the operations in flight have
    /// completed — Java's <c>close()</c>. Idempotent. On <see cref="MockAdminClient"/> it does nothing, as Java's mock;
    /// <see cref="IDisposable.Dispose"/> frees the mock.
    /// </summary>
    /// <remarks>
    /// Called from a future's continuation on the client's thread it does not wait: the client is torn down once that
    /// continuation has returned and the close has completed.
    /// </remarks>
    void Close();
}

/// <summary>
/// The static method Java declares on the <see cref="IAdmin"/> interface, which an interface cannot carry on
/// netstandard2.0. From C# 14 it is called on the interface (<c>IAdmin.Create(…)</c>); older C# calls it on this class.
/// </summary>
public static class Admin
{
    extension(IAdmin)
    {
        /// <summary>Create a new Admin with the given configuration.</summary>
        /// <remarks>
        /// C# looks a simple name up in each enclosing namespace before the <c>using</c> directives at the top of the
        /// file, so an enclosing namespace with a member named <c>Admin</c> hides this class and <c>Admin.Create</c>
        /// fails with CS0234: a namespace <c>MyApp.Admin</c> for code in <c>MyApp.Services</c>, and
        /// <c>Confluent.Kafka.Admin</c> for code in the other <c>Confluent.Kafka.*</c> namespaces. There, write
        /// <c>IAdmin.Create</c> (C# 14), the full name <c>global::Confluent.Kafka.Admin.Admin.Create</c>, or put the
        /// <c>using</c> inside the namespace block.
        /// </remarks>
        /// <param name="conf">
        /// The admin client configuration, keyed by Java's dotted names (<c>bootstrap.servers</c> or
        /// <c>bootstrap.controllers</c> is required): each value of the type Java's <c>ConfigDef</c> accepts for its
        /// key, or the string Java would parse. A <c>null</c> value leaves its key unset, unless the key's validator
        /// refuses <c>null</c>.
        /// </param>
        /// <returns>The new KafkaAdminClient.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="conf"/> is null.</exception>
        /// <exception cref="ConfigException">
        /// A value is not of a type its key accepts, a <c>null</c> value is one its key's validator refuses, a key is
        /// <c>null</c>, or the client refuses a value.
        /// </exception>
        /// <exception cref="KafkaException">The core rejected the configuration.</exception>
        public static IAdmin Create(IReadOnlyDictionary<string, object?> conf) =>
            new KafkaAdminClient(conf ?? throw new ArgumentNullException(nameof(conf)));
    }
}
