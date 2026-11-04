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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The consumer members that do not block in Java, shared by <see cref="IConsumer{TKey, TValue}"/> and
/// <see cref="IAsyncConsumer{TKey, TValue}"/>, so both flavors carry them with the same signatures.
/// </summary>
/// <remarks>
/// The state reads are methods, not properties, as in Java: each crosses into the core, can throw, and returns a
/// new copy. A Java <c>Set</c> is returned as an <see cref="IReadOnlyCollection{T}"/>, since
/// <c>IReadOnlySet</c> is not on netstandard2.0.
/// </remarks>
public interface IConsumerCommon
{
    /// <summary>
    /// Interrupts a blocked operation of this consumer (Java <c>wakeup()</c>): the operation in progress, or the
    /// next one Java's <c>wakeup()</c> interrupts, throws a <see cref="KafkaException"/> with the Wakeup code, once.
    /// It may be called from any thread, and does nothing once the consumer is closed.
    /// </summary>
    void Wakeup();

    /// <summary>
    /// Returns the current consumer group metadata (Java <c>groupMetadata()</c>).
    /// </summary>
    /// <returns>A snapshot of the group membership.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    /// <exception cref="KafkaException">
    /// A <see cref="KafkaConsumer{TKey, TValue}"/> or <see cref="AsyncKafkaConsumer{TKey, TValue}"/> has no
    /// <c>group.id</c>: code 24 (<c>INVALID_GROUP_ID</c>) with Java's message, after the closed and concurrency checks.
    /// </exception>
    ConsumerGroupMetadata GroupMetadata();

    /// <summary>
    /// Returns the current partition assignment (Java <c>assignment()</c>), as a new copy.
    /// </summary>
    /// <returns>The assigned topic-partitions (a copy; empty when nothing is assigned).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<TopicPartition> Assignment();

    /// <summary>
    /// Returns the current topic subscription (Java <c>subscription()</c>), as a new copy.
    /// </summary>
    /// <returns>The subscribed topics (a copy; empty when not subscribed).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<string> Subscription();

    /// <summary>
    /// Returns the currently paused partitions (Java <c>paused()</c>), as a new copy.
    /// </summary>
    /// <returns>The paused topic-partitions (a copy; empty when none are paused).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<TopicPartition> Paused();

    /// <summary>
    /// Triggers a rebalance (Java <c>enforceRebalance()</c> and <c>enforceRebalance(String reason)</c>, one method
    /// with an optional <paramref name="reason"/>).
    /// </summary>
    /// <remarks>
    /// Under the KIP-848 group protocol it only logs and returns, as Java's <c>AsyncKafkaConsumer</c> does; the
    /// <see cref="KafkaException"/> below cannot occur under that protocol.
    /// </remarks>
    /// <param name="reason">An optional human-readable reason, or <see langword="null"/>.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a rebalance failure (not reachable under the current KIP-848
    /// no-op).
    /// </exception>
    void EnforceRebalance(string? reason = null);

    /// <summary>
    /// Returns a snapshot of the consumer's metrics (Java <c>Map&lt;MetricName, ? extends Metric&gt;
    /// metrics()</c>), keyed by <see cref="MetricName"/>.
    /// </summary>
    /// <remarks>
    /// Each <see cref="IMetric.Value"/> was measured once, by this call; the entries do not measure again. Keys
    /// compare by name, group and tags, not description, so per-partition metrics that share a name are distinct
    /// keys.
    /// </remarks>
    /// <returns>An owned, immutable snapshot (empty when no metrics are registered — e.g. a mock).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();

    /// <summary>
    /// Returns a new <see cref="ConsumerHandle"/>, the way into this consumer from inside one of its own callbacks,
    /// as a Java listener calls the <c>consumer</c> it captured. Inside a callback the consumer routes its own calls
    /// to a handle of its own, which also has the forms <see cref="ConsumerHandle"/> lacks, so a callback can also
    /// call the consumer directly.
    /// </summary>
    /// <remarks>
    /// It may be called from any thread, a callback included. The handle holds the consumer weakly: it does not
    /// keep the consumer alive. Once the consumer is destroyed, the handle's <see cref="ConsumerHandle.Wakeup"/> does
    /// nothing, its <see cref="ConsumerHandle.Assignment"/> and <see cref="ConsumerHandle.Paused"/> still return, and
    /// its other operations throw <see cref="InvalidOperationException"/>. See <see cref="ConsumerHandle"/>.
    /// </remarks>
    /// <returns>A new, caller-owned handle (never <see langword="null"/>).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    ConsumerHandle Handle();
}
