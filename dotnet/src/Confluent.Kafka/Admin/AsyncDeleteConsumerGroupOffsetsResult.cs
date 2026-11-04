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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DeleteConsumerGroupOffsets"/> — Java's
/// <c>org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult</c>: the outcome per partition and for the whole request.
/// </summary>
/// <remarks>
/// Each call of an accessor returns a new future, as Java's do.
/// </remarks>
/// <seealso cref="DeleteConsumerGroupOffsetsResult"/>
public sealed class AsyncDeleteConsumerGroupOffsetsResult
{
    private readonly SafeAdminResultHandle _native;

    private AsyncDeleteConsumerGroupOffsetsResult(SafeAdminResultHandle native)
    {
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The outcome of deleting the offset for one partition — Java's <c>partitionResult(TopicPartition)</c>.
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A new future that completes when that partition's offset was deleted, and fails with the partition's error, or
    /// with the call's error when the whole request failed.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> was not part of the request, as Java throws at the call; or it has a null topic,
    /// or a topic with a NUL character or an unpaired surrogate.
    /// </exception>
    public AsyncKafkaFuture PartitionResult(TopicPartition partition) =>
        new AsyncKafkaFuture(NativeAdminClient.DeleteConsumerGroupOffsetsPartitionResult(_native, partition));

    /// <summary>
    /// Completes when every requested partition's offset was deleted, and fails otherwise — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A new future over the whole request; its failure is the first failed partition's error.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(NativeAdminClient.DeleteConsumerGroupOffsetsAll(_native));

    internal static AsyncDeleteConsumerGroupOffsetsResult OfClient(SafeAdminResultHandle native) => new AsyncDeleteConsumerGroupOffsetsResult(native);
}
