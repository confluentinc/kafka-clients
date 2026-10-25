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
/// The rebalance, and the operation that triggered it, wait until each method returns. The methods run only
/// inside <c>Poll</c>, <c>Unsubscribe</c> and <c>Close</c>: on <see cref="IConsumer{TKey, TValue}"/> on the
/// calling thread; on <see cref="IAsyncConsumer{TKey, TValue}"/> on the consumer's drain thread,
/// <c>confluent-kafka-consumer-pump</c>, one at a time. <see cref="IAsyncConsumerRebalanceListener"/> is the
/// asynchronous form, for <see cref="IAsyncConsumer{TKey, TValue}"/> only.
/// </para>
/// <para>
/// A listener can call back into its own consumer, as a Java listener calls the consumer it captured. <c>Poll</c>,
/// <c>Subscribe</c>, <c>Unsubscribe</c> and <c>Assign</c> throw <see cref="System.InvalidOperationException"/> at
/// once, and so does <c>Close</c>, which leaves the consumer open. <c>Seek</c>, <c>SeekToBeginning</c>,
/// <c>SeekToEnd</c>, <c>Pause</c>, <c>Resume</c>, <c>Position</c>, <c>CurrentLag</c>, <c>Commit</c>,
/// <c>CommitAsync</c>, <c>Committed</c>, <c>BeginningOffsets</c>, <c>EndOffsets</c>, <c>OffsetsForTimes</c>,
/// <c>PartitionsFor</c>, <c>ListTopics</c>, <c>Assignment</c>, <c>Paused</c> and <c>GroupMetadata</c>, timed or not,
/// go to the consumer's own handle and block, also where <see cref="ConsumerHandle"/> lacks the form. On a mock
/// consumer only the commits, timed or not, <c>CurrentLag</c>, <c>Assignment</c>, <c>Paused</c> and
/// <c>GroupMetadata</c> work there; every other routed call throws <see cref="System.NotSupportedException"/>
/// ("ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer
/// directly."). A thread the listener starts is not inside the listener.
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
