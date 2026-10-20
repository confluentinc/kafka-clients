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
/// The producer id and epoch that <c>fenceProducers</c> returns for one transactional id, Java's
/// <c>ProducerIdAndEpoch</c>.
/// </summary>
internal readonly struct ProducerIdAndEpoch
{
    internal ProducerIdAndEpoch(long producerId, short epoch)
    {
        ProducerId = producerId;
        Epoch = epoch;
    }

    internal long ProducerId { get; }

    internal short Epoch { get; }
}
