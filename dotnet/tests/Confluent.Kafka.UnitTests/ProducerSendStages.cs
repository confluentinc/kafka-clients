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
using Confluent.Kafka.Common;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Adapts the two-stage async <c>Send</c> to its delivery <see cref="Task{TResult}"/>: a completed first stage
/// yields the future's <see cref="AsyncKafkaFuture{T}.Get"/> task as-is, so the allocation tests measure the
/// send itself; a pending one is unwrapped into one task.
/// </summary>
internal static class ProducerSendStages
{
    internal static Task<RecordMetadata> Delivery(this ValueTask<AsyncKafkaFuture<RecordMetadata>> send) =>
        send.IsCompletedSuccessfully ? send.Result.Get() : Unwrap(send);

    private static async Task<RecordMetadata> Unwrap(ValueTask<AsyncKafkaFuture<RecordMetadata>> send) =>
        await (await send.ConfigureAwait(false)).Get().ConfigureAwait(false);
}
