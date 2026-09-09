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
