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

using System.Collections.Generic;

namespace Confluent.Kafka;

/// <summary>
/// The callbacks run when the partitions assigned to the consumer change, Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerRebalanceListener</c>. Pass an implementation to
/// <see cref="IConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IConsumerRebalanceListener)"/>
/// or <see cref="IAsyncConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IConsumerRebalanceListener, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// <para>
/// The rebalance, and the operation that triggered it, wait until each method returns. On
/// <see cref="IConsumer{TKey, TValue}"/> the methods run on the calling thread, inside the blocking call that
/// triggered them; on <see cref="IAsyncConsumer{TKey, TValue}"/> they run on the consumer's drain thread,
/// <c>confluent-kafka-consumer-pump</c>, one at a time. <see cref="IAsyncConsumerRebalanceListener"/> is the
/// asynchronous form, for <see cref="IAsyncConsumer{TKey, TValue}"/> only.
/// </para>
/// <para>
/// A listener can call back into its own consumer, as a Java listener calls the consumer it
/// captured. A call the <see cref="ConsumerHandle"/> has goes to the consumer's own handle and blocks;
/// a waiting call it lacks (<c>Poll</c>, <c>Subscribe</c>, <c>Unsubscribe</c>, <c>Assign</c>,
/// <c>PartitionsFor</c>, <c>ListTopics</c>, and the timed <c>Commit</c>, <c>Committed</c>,
/// <c>BeginningOffsets</c>, <c>EndOffsets</c> and <c>OffsetsForTimes</c>) throws
/// <see cref="System.InvalidOperationException"/> at once, and so does <c>Close</c>, which leaves the consumer
/// open. On a mock consumer the handle has only the commits, <c>CurrentLag</c>, <c>Assignment</c> and
/// <c>Paused</c>, and another routed call throws <see cref="KafkaException"/>. A thread the listener starts is
/// not inside the listener.
/// </para>
/// <para>
/// An exception a method throws never unwinds into native code: it propagates out of the consumer call that ran
/// the listener, as a Java listener's does.
/// </para>
/// <para>
/// A listener is bound to a subscription and fires many times. A replacing subscribe, a listener-less one
/// included, releases it, as does disposing the consumer; <c>Unsubscribe()</c> and <c>Close()</c> keep it, as
/// Java's <c>SubscriptionState.unsubscribe()</c> does.
/// </para>
/// <para>
/// Java's <c>onPartitionsLost</c> defaults to <c>onPartitionsRevoked</c>. A C# default interface method needs
/// .NET Standard 2.1, above this binding's netstandard2.0 floor, so the default lives on
/// <see cref="ConsumerRebalanceListenerBase"/>.
/// </para>
/// </remarks>
public interface IConsumerRebalanceListener
{
    /// <summary>
    /// Invoked before the consumer gives up ownership of <paramref name="partitions"/>
    /// (Java <c>onPartitionsRevoked(Collection&lt;TopicPartition&gt;)</c>). Called with the
    /// partitions being <b>taken away</b>, and only when something is actually being
    /// removed. The rebalance does not proceed until this returns.
    /// </summary>
    /// <param name="partitions">The partitions being revoked (never null; may be empty).</param>
    void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Invoked after the consumer has been assigned <paramref name="partitions"/> and
    /// before it starts fetching them (Java
    /// <c>onPartitionsAssigned(Collection&lt;TopicPartition&gt;)</c>). Called with the
    /// <b>newly added</b> partitions — <em>not</em> the full assignment, matching Java —
    /// and it fires even when the added set is empty. The rebalance does not proceed until
    /// this returns.
    /// </summary>
    /// <param name="partitions">The newly added partitions (never null; may be empty).</param>
    void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Invoked when the consumer detects that it has lost <paramref name="partitions"/>
    /// without a clean revocation (Java
    /// <c>onPartitionsLost(Collection&lt;TopicPartition&gt;)</c>) — for example after being
    /// fenced by the group coordinator. Java's default delegates to
    /// <see cref="OnPartitionsRevoked"/>; on this binding's netstandard2.0 floor that
    /// default lives on <see cref="ConsumerRebalanceListenerBase"/> instead.
    /// </summary>
    /// <param name="partitions">The lost partitions (never null; may be empty).</param>
    void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}
