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
using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The consumer members that do not block in Java, shared by <see cref="IConsumer{TKey, TValue}"/> and
/// <see cref="IAsyncConsumer{TKey, TValue}"/>, so both flavors carry them with the same signatures.
/// </summary>
/// <remarks>
/// The state reads are methods, as in Java: each crosses into the core, can throw, and returns a
/// new copy. A Java <c>Set</c> is returned as an <see cref="IReadOnlyCollection{T}"/>, since
/// <c>IReadOnlySet</c> is not on netstandard2.0.
/// </remarks>
public interface IConsumerCommon
{
    /// <summary>
    /// Interrupts a blocked operation of this consumer (Java <c>wakeup()</c>): the operation in progress, or the
    /// next one Java's <c>wakeup()</c> interrupts, throws a <see cref="WakeupException"/>, once.
    /// It may be called from any thread, and does nothing once the consumer is closed.
    /// </summary>
    void Wakeup();

    /// <summary>
    /// Returns the current consumer group metadata (Java <c>groupMetadata()</c>).
    /// </summary>
    /// <returns>A snapshot of the group membership.</returns>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    /// <exception cref="InvalidGroupIdException">
    /// A <see cref="KafkaConsumer{TKey, TValue}"/> or <see cref="AsyncKafkaConsumer{TKey, TValue}"/> has no
    /// <c>group.id</c>, with Java's message, after the closed and concurrency checks.
    /// </exception>
    ConsumerGroupMetadata GroupMetadata();

    /// <summary>
    /// Returns the current partition assignment (Java <c>assignment()</c>), as a new copy.
    /// </summary>
    /// <returns>The assigned topic-partitions (a copy; empty when nothing is assigned).</returns>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<TopicPartition> Assignment();

    /// <summary>
    /// Returns the current topic subscription (Java <c>subscription()</c>), as a new copy.
    /// </summary>
    /// <returns>The subscribed topics (a copy; empty when not subscribed).</returns>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<string> Subscription();

    /// <summary>
    /// Returns the currently paused partitions (Java <c>paused()</c>), as a new copy.
    /// </summary>
    /// <returns>The paused topic-partitions (a copy; empty when none are paused).</returns>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<TopicPartition> Paused();

    /// <summary>
    /// Triggers a rebalance (Java <c>enforceRebalance()</c>).
    /// </summary>
    /// <remarks>
    /// Under the KIP-848 group protocol it only logs and returns, as Java's <c>AsyncKafkaConsumer</c> does. As in
    /// Java, it makes no closed check, so on a closed consumer it returns, and no owner check, so another thread's
    /// call does not refuse it; the core still refuses it while another operation is in flight. The
    /// <see cref="KafkaException"/> below cannot occur under that protocol.
    /// </remarks>
    /// <exception cref="ConcurrentModificationException">
    /// The core refused it while another operation was in flight (the core's own check).
    /// </exception>
    /// <exception cref="KafkaException">
    /// The core reported a rebalance failure (not reachable under the current KIP-848
    /// no-op).
    /// </exception>
    void EnforceRebalance();

    /// <summary>
    /// Triggers a rebalance, giving a <paramref name="reason"/> (Java <c>enforceRebalance(String reason)</c>).
    /// </summary>
    /// <remarks>
    /// It behaves as <see cref="EnforceRebalance()"/>: under the KIP-848 group protocol it only logs and returns.
    /// </remarks>
    /// <param name="reason">A human-readable reason, or <see langword="null"/>.</param>
    /// <exception cref="ConcurrentModificationException">
    /// The core refused it while another operation was in flight (the core's own check).
    /// </exception>
    /// <exception cref="KafkaException">
    /// The core reported a rebalance failure (not reachable under the current KIP-848
    /// no-op).
    /// </exception>
    void EnforceRebalance(string? reason);

    /// <summary>
    /// Get the metrics kept by the consumer (Java <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c>).
    /// </summary>
    /// <remarks>
    /// The returned map is an unmodifiable live view of the metrics. Changes to the underlying metrics will be
    /// reflected in the returned map. Each value is a <see cref="Common.Metrics.KafkaMetric"/>, which measures the
    /// metric again on each <see cref="IMetric.MetricValue()"/>, and a lookup returns a new one each time. Keys compare
    /// by name, group and tags, not description. On <see cref="KafkaConsumer{TKey, TValue}"/> and
    /// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> another thread's call does not refuse it, as Java's
    /// <c>KafkaConsumer.metrics()</c> takes no <c>acquire()</c>, and the map answers after the consumer is closed. The
    /// mocks wait for it and their map holds no metric, as Java's synchronized <c>MockConsumer.metrics()</c>.
    /// On <see cref="KafkaConsumer{TKey, TValue}"/> and <see cref="AsyncKafkaConsumer{TKey, TValue}"/> the map lacks
    /// Java's network and app-info metrics, which the native core does not register yet: the connection, request and
    /// I/O metrics Java's <c>Selector</c> adds to <c>consumer-metrics</c> (<c>connection-count</c>,
    /// <c>request-total</c>, ...), the per-node <c>consumer-node-metrics</c> group, and the <c>app-info</c> group
    /// (<c>version</c>, <c>commit-id</c>, <c>start-time-ms</c>).
    /// </remarks>
    /// <returns>An unmodifiable live view of the map of metrics currently maintained by the consumer.</returns>
    /// <exception cref="IllegalStateException">
    /// A <see cref="MockConsumer{TKey, TValue}"/> or <see cref="AsyncMockConsumer{TKey, TValue}"/> is closed, as Java's
    /// mock throws.
    /// </exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();
}
