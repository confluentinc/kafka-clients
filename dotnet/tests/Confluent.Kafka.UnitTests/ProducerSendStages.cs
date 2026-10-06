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

using System.Threading.Tasks;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Adapts the two-stage async <c>Send</c> (M11/P3.5: accepted, then delivered) to the delivery
/// <see cref="Task{TResult}"/> the producer tests were written against. A send whose first stage is
/// already complete yields its delivery task as-is — no wrapper, no state machine — so the
/// allocation-budget tests keep measuring the send itself; a still-pending first stage is unwrapped
/// into one task that completes with the delivery.
/// </summary>
internal static class ProducerSendStages
{
    /// <summary>The record's delivery: the accepted stage, then the delivery task it yields.</summary>
    internal static Task<RecordMetadata> Delivery(this ValueTask<Task<RecordMetadata>> send) =>
        send.IsCompletedSuccessfully ? send.Result : send.AsTask().Unwrap();
}
