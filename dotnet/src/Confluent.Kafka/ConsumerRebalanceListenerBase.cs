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
/// Carries Java's default <c>onPartitionsLost</c>, which delegates to <c>onPartitionsRevoked</c>, for an
/// <see cref="IConsumerRebalanceListener"/>.
/// </summary>
/// <remarks>
/// Java puts the default on the interface; this binding's floor, netstandard2.0, has no default interface
/// methods, so the default lives on this base. Implementing <see cref="IConsumerRebalanceListener"/> directly
/// stays possible.
/// </remarks>
public abstract class ConsumerRebalanceListenerBase : IConsumerRebalanceListener
{
    /// <inheritdoc/>
    public abstract void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);

    /// <inheritdoc/>
    public abstract void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);

    /// <inheritdoc/>
    public virtual void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        OnPartitionsRevoked(partitions);
}
