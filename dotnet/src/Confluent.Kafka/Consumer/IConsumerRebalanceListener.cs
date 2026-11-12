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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The callbacks run when the partitions assigned to the consumer change, Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerRebalanceListener</c>. Pass an implementation to
/// <see cref="IConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IConsumerRebalanceListener)"/>
/// or <see cref="IAsyncConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IConsumerRebalanceListener, System.Threading.CancellationToken)"/>,
/// or to their pattern forms
/// <see cref="IConsumer{TKey, TValue}.Subscribe(SubscriptionPattern, IConsumerRebalanceListener)"/> and
/// <see cref="IAsyncConsumer{TKey, TValue}.Subscribe(SubscriptionPattern, IConsumerRebalanceListener, System.Threading.CancellationToken)"/>.
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
/// <c>Subscribe</c>, <c>Unsubscribe</c> and <c>Assign</c> throw <see cref="ConcurrentModificationException"/> at
/// once, and so does <c>Close</c>, which leaves the consumer open. <c>Seek</c>, <c>SeekToBeginning</c>,
/// <c>SeekToEnd</c>, <c>Pause</c>, <c>Resume</c>, <c>Position</c>, <c>CurrentLag</c>, <c>Commit</c>,
/// <c>CommitAsync</c>, <c>Committed</c>, <c>BeginningOffsets</c>, <c>EndOffsets</c>, <c>OffsetsForTimes</c>,
/// <c>PartitionsFor</c>, <c>ListTopics</c>, <c>Assignment</c>, <c>Paused</c> and <c>GroupMetadata</c>, timed or not,
/// go to the consumer's own handle and block, also where <see cref="ConsumerHandle"/> lacks the form. On a mock
/// consumer only the commits, timed or not, <c>CurrentLag</c>, <c>Assignment</c>, <c>Paused</c> and
/// <c>GroupMetadata</c> work there; every other routed call throws <see cref="UnsupportedOperationException"/>
/// ("ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer
/// directly."). A thread the listener starts is not inside the listener.
/// </para>
/// <para>
/// An exception a method throws never unwinds into native code: it propagates out of the consumer call that ran
/// the listener, as a Java listener's does.
/// </para>
/// <para>
/// A listener is bound to a subscription and fires many times. A subscribe that succeeds, a listener-less one
/// included, releases it, as does closing or disposing the consumer (closing once its callbacks have run);
/// <c>Unsubscribe()</c> keeps it, as Java's <c>SubscriptionState.unsubscribe()</c> does, and so does a subscribe to no
/// topics on a <c>KafkaConsumer</c> or <c>AsyncKafkaConsumer</c>, which is <c>Unsubscribe()</c>.
/// Java's <c>MockConsumer</c> keeps firing the listener after <c>close()</c>; a closed
/// <see cref="MockConsumer{TKey, TValue}"/> or <see cref="AsyncMockConsumer{TKey, TValue}"/> has released it, and its
/// <c>Rebalance</c> throws <see cref="IllegalStateException"/>.
/// </para>
/// <para>
/// Java's default <c>onPartitionsLost</c>, which calls <c>onPartitionsRevoked</c>, is
/// <see cref="ConsumerRebalanceListenerExtensions.OnPartitionsLost(IConsumerRebalanceListener, IReadOnlyCollection{TopicPartition})"/>,
/// which a listener replaces by implementing <see cref="IConsumerRebalanceListenerWithPartitionsLost"/>.
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
}

/// <summary>
/// A rebalance listener with its own handling of lost partitions: it replaces Java's default
/// <c>ConsumerRebalanceListener.onPartitionsLost</c>, which calls
/// <see cref="IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/>. On lost
/// partitions a consumer calls this method for a listener that implements this interface, and
/// <see cref="IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/> for any other.
/// </summary>
/// <remarks>
/// A consumer's <c>Subscribe</c> refuses a listener that declares a public
/// <c>void OnPartitionsLost(IReadOnlyCollection&lt;TopicPartition&gt;)</c> but does not implement this interface, since
/// it would never call that method. A public <c>Task</c> one is not refused: it is the method of
/// <see cref="IAsyncConsumerRebalanceListenerWithPartitionsLost"/>. The check reads the type's metadata, so under
/// trimming or Native AOT, which can remove a method nothing calls, it is best effort.
/// </remarks>
public interface IConsumerRebalanceListenerWithPartitionsLost : IConsumerRebalanceListener
{
    /// <summary>
    /// Cleans up resources for partitions that have already been reassigned to other consumers. It is not called during
    /// normal execution, as the owned partitions are first revoked through
    /// <see cref="IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/> before they are
    /// reassigned to other consumers during a rebalance. In exceptional scenarios, when the consumer finds that it no
    /// longer owns a partition without a normal rebalance having revoked it, this method is called instead: for
    /// example, when the consumer's session timeout has expired, or when a fatal error has been received indicating the
    /// consumer is no longer part of the group. The rebalance does not proceed until this returns.
    /// </summary>
    /// <param name="partitions">The partitions that were assigned to the consumer and have now been reassigned to other
    /// consumers: always all the partitions previously assigned to it (never null; may be empty).</param>
    void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}

/// <summary>Java's default method of <c>ConsumerRebalanceListener</c>, for every <see cref="IConsumerRebalanceListener"/>.</summary>
public static class ConsumerRebalanceListenerExtensions
{
    /// <summary>
    /// Handles the loss of <paramref name="partitions"/>, Java's <c>ConsumerRebalanceListener.onPartitionsLost</c>:
    /// <see cref="IConsumerRebalanceListenerWithPartitionsLost.OnPartitionsLost(IReadOnlyCollection{TopicPartition})"/>
    /// when <paramref name="listener"/> implements it, and otherwise
    /// <see cref="IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/>, as Java's
    /// default.
    /// </summary>
    /// <param name="listener">The listener.</param>
    /// <param name="partitions">The lost partitions (never null; may be empty).</param>
    /// <exception cref="ArgumentNullException"><paramref name="listener"/> is null.</exception>
    public static void OnPartitionsLost(this IConsumerRebalanceListener listener, IReadOnlyCollection<TopicPartition> partitions)
    {
        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        if (listener is IConsumerRebalanceListenerWithPartitionsLost withPartitionsLost)
        {
            withPartitionsLost.OnPartitionsLost(partitions);
        }
        else
        {
            listener.OnPartitionsRevoked(partitions);
        }
    }
}
