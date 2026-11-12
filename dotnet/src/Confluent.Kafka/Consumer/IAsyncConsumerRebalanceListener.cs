// Copyright 2026 Confluent Inc.
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
/// The asynchronous form of <see cref="IConsumerRebalanceListener"/>, accepted by
/// <see cref="IAsyncConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IAsyncConsumerRebalanceListener, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// Each method is called on the consumer's drain thread, which does not wait for the returned
/// <see cref="Task"/>; the rebalance, and the call that triggered it, do not advance until the
/// <see cref="Task"/> ends. A faulted or cancelled <see cref="Task"/>, or an exception thrown before one
/// is returned, propagates out of that call as the same instance. Java's default <c>onPartitionsLost</c>, which calls
/// <c>onPartitionsRevoked</c>, is
/// <see cref="AsyncConsumerRebalanceListenerExtensions.OnPartitionsLost(IAsyncConsumerRebalanceListener, IReadOnlyCollection{TopicPartition})"/>,
/// which a listener replaces by implementing <see cref="IAsyncConsumerRebalanceListenerWithPartitionsLost"/>.
/// </remarks>
public interface IAsyncConsumerRebalanceListener
{
    /// <summary>
    /// Called before the consumer gives up ownership of <paramref name="partitions"/> (Java
    /// <c>onPartitionsRevoked</c>).
    /// </summary>
    /// <param name="partitions">The partitions being revoked (never null; may be empty).</param>
    /// <returns>A <see cref="Task"/> whose end lets the rebalance advance.</returns>
    Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Called after the consumer has been assigned <paramref name="partitions"/> and before it fetches
    /// them (Java <c>onPartitionsAssigned</c>).
    /// </summary>
    /// <param name="partitions">The newly added partitions (never null; may be empty).</param>
    /// <returns>A <see cref="Task"/> whose end lets the rebalance advance.</returns>
    Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);
}

/// <summary>
/// The asynchronous form of <see cref="IConsumerRebalanceListenerWithPartitionsLost"/>: an
/// <see cref="IAsyncConsumerRebalanceListener"/> with its own handling of lost partitions, replacing Java's default
/// <c>ConsumerRebalanceListener.onPartitionsLost</c>, which calls
/// <see cref="IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/>. On lost
/// partitions a consumer calls this method for a listener that implements this interface, and
/// <see cref="IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/> for any other.
/// </summary>
/// <remarks>
/// A consumer's <c>Subscribe</c> refuses a listener that declares a public
/// <c>OnPartitionsLost(IReadOnlyCollection&lt;TopicPartition&gt;)</c> but does not implement this interface, since it
/// would never call that method. The check reads the type's metadata, so under trimming or Native AOT, which can remove
/// a method nothing calls, it is best effort.
/// </remarks>
public interface IAsyncConsumerRebalanceListenerWithPartitionsLost : IAsyncConsumerRebalanceListener
{
    /// <summary>
    /// Cleans up resources for partitions that have already been reassigned to other consumers. It is not called during
    /// normal execution, where the owned partitions are first revoked through
    /// <see cref="IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/>; it is
    /// called when the consumer finds that it no longer owns partitions that no rebalance revoked: for example, when its
    /// session timeout has expired, or when a fatal error has been received indicating it is no longer part of the group.
    /// </summary>
    /// <param name="partitions">The partitions that were assigned to the consumer and have now been reassigned to other
    /// consumers: always all the partitions previously assigned to it (never null; may be empty).</param>
    /// <returns>A <see cref="Task"/> whose end lets the rebalance advance.</returns>
    Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}

/// <summary>
/// Java's default method of <c>ConsumerRebalanceListener</c>, for every <see cref="IAsyncConsumerRebalanceListener"/>.
/// </summary>
public static class AsyncConsumerRebalanceListenerExtensions
{
    /// <summary>
    /// Handles the loss of <paramref name="partitions"/>, Java's <c>ConsumerRebalanceListener.onPartitionsLost</c>:
    /// <see cref="IAsyncConsumerRebalanceListenerWithPartitionsLost.OnPartitionsLost(IReadOnlyCollection{TopicPartition})"/>
    /// when <paramref name="listener"/> implements it, and otherwise
    /// <see cref="IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection{TopicPartition})"/>, as Java's
    /// default.
    /// </summary>
    /// <param name="listener">The listener.</param>
    /// <param name="partitions">The lost partitions (never null; may be empty).</param>
    /// <returns>The <see cref="Task"/> of the method called, whose end lets the rebalance advance.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="listener"/> is null.</exception>
    public static Task OnPartitionsLost(this IAsyncConsumerRebalanceListener listener, IReadOnlyCollection<TopicPartition> partitions)
    {
        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        return listener is IAsyncConsumerRebalanceListenerWithPartitionsLost withPartitionsLost
            ? withPartitionsLost.OnPartitionsLost(partitions)
            : listener.OnPartitionsRevoked(partitions);
    }
}
