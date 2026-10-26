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

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confluent.Kafka;

/// <summary>
/// The asynchronous form of <see cref="IConsumerRebalanceListener"/>, accepted by
/// <see cref="IAsyncConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IAsyncConsumerRebalanceListener, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// Each method is called on the consumer's drain thread, which does not wait for the returned
/// <see cref="Task"/>; the rebalance, and the call that triggered it, do not advance until the
/// <see cref="Task"/> ends. A faulted or cancelled <see cref="Task"/>, or an exception thrown before one
/// is returned, propagates out of that call as the same instance. Derive from
/// <see cref="AsyncConsumerRebalanceListenerBase"/> for Java's default <c>onPartitionsLost</c>.
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

    /// <summary>
    /// Called when the consumer has lost <paramref name="partitions"/> without a clean revocation (Java
    /// <c>onPartitionsLost</c>).
    /// </summary>
    /// <param name="partitions">The lost partitions (never null; may be empty).</param>
    /// <returns>A <see cref="Task"/> whose end lets the rebalance advance.</returns>
    Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}
