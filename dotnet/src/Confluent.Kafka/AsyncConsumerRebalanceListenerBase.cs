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
/// Carries Java's default <c>onPartitionsLost</c>, which delegates to <c>onPartitionsRevoked</c>, for an
/// <see cref="IAsyncConsumerRebalanceListener"/>.
/// </summary>
public abstract class AsyncConsumerRebalanceListenerBase : IAsyncConsumerRebalanceListener
{
    /// <inheritdoc/>
    public abstract Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);

    /// <inheritdoc/>
    public abstract Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);

    /// <inheritdoc/>
    public virtual Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        OnPartitionsRevoked(partitions);
}
