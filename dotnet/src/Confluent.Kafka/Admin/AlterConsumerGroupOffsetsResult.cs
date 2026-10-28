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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AlterConsumerGroupOffsets"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult</c>: the outcome per partition and for the whole request.
/// </summary>
/// <remarks>
/// Each call of an accessor returns a new future, as Java's do.
/// </remarks>
/// <seealso cref="AsyncAlterConsumerGroupOffsetsResult"/>
public sealed class AlterConsumerGroupOffsetsResult
{
    private readonly SafeAdminResultHandle _native;

    private AlterConsumerGroupOffsetsResult(SafeAdminResultHandle native)
    {
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The outcome of altering the offset for one partition — Java's <c>partitionResult(TopicPartition)</c>.
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A new future that completes when that partition's offset was altered, and fails with the partition's error, or
    /// with the call's error when the whole request failed. For a partition that was not in the request it fails with
    /// an <see cref="ArgumentException"/>, as Java's does.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> has a null topic, or a topic with a NUL character or an unpaired surrogate.
    /// </exception>
    public KafkaFuture PartitionResult(TopicPartition partition) =>
        new KafkaFuture(NativeAdminClient.AlterConsumerGroupOffsetsPartitionResult(_native, partition));

    /// <summary>
    /// Completes when every partition's offset was altered, and fails otherwise — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A new future over the whole request; its failure is the core's, naming every failed partition.</returns>
    public KafkaFuture All() => new KafkaFuture(NativeAdminClient.AlterConsumerGroupOffsetsAll(_native));

    internal static AlterConsumerGroupOffsetsResult OfClient(SafeAdminResultHandle native) => new AlterConsumerGroupOffsetsResult(native);
}
