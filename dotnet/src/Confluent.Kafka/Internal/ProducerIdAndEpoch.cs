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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The per-id value <c>fenceProducers</c> carries — Java's
/// <c>org.apache.kafka.common.utils.ProducerIdAndEpoch</c> (<c>:21</c>), whose two fields are
/// public and accessor-less, which is why Java writes <c>p -&gt; p.producerId</c>.
/// </summary>
/// <remarks>
/// ⚠ <b>Deliberately internal</b> (M15/P8 D50). No Java accessor publishes this type —
/// <c>FenceProducersResult</c> erases or projects it at every one of its four views — so a
/// public C# type would be surface Java's own API does not have
/// (<c>definition-of-done.md</c> §7). It exists only to carry the pair from the result walk
/// to those views.
/// </remarks>
internal readonly struct ProducerIdAndEpoch
{
    internal ProducerIdAndEpoch(long producerId, short epoch)
    {
        ProducerId = producerId;
        Epoch = epoch;
    }

    /// <summary>Java's public <c>producerId</c> field (<c>:24</c>).</summary>
    internal long ProducerId { get; }

    /// <summary>Java's public <c>epoch</c> field (<c>:25</c>), a <see langword="short"/>.</summary>
    internal short Epoch { get; }
}
