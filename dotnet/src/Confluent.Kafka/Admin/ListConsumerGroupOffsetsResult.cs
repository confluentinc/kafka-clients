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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.ListConsumerGroupOffsets(string, ListConsumerGroupOffsetsOptions?)"/> — Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult</c>: one future per group id, whose value maps each
/// partition to its committed offset, or to <see langword="null"/> when the partition has none.
/// </summary>
/// <seealso cref="AsyncListConsumerGroupOffsetsResult"/>
public sealed class ListConsumerGroupOffsetsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> _futures;
    private readonly SafeAdminResultHandle? _native;

    internal ListConsumerGroupOffsetsResult(IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        _futures = futures;
    }

    private ListConsumerGroupOffsetsResult(IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// The offsets of the one group requested — Java's <c>partitionsToOffsetAndMetadata()</c>.
    /// </summary>
    /// <returns>That group's future.</returns>
    /// <exception cref="InvalidOperationException">The request did not name exactly one group; Java's message.</exception>
    public KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> PartitionsToOffsetAndMetadata()
    {
        if (_futures.Count != 1)
        {
            throw new InvalidOperationException(
                "Offsets from multiple consumer groups were requested. " +
                "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.");
        }

        return _futures.Values.First();
    }

    /// <summary>
    /// The offsets of one requested group — Java's <c>partitionsToOffsetAndMetadata(String)</c>.
    /// </summary>
    /// <param name="groupId">A group id from the request.</param>
    /// <returns>That group's future, the same one on every call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="groupId"/> was not part of the request.</exception>
    public KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> PartitionsToOffsetAndMetadata(string groupId)
    {
        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        if (!_futures.TryGetValue(groupId, out KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future))
        {
            throw new ArgumentException(
                string.Format(CultureInfo.InvariantCulture, "Offsets for consumer group '{0}' were not requested.", groupId),
                nameof(groupId));
        }

        return future;
    }

    /// <summary>
    /// Every requested group's offsets, keyed by group id, once all of them succeeded — Java's <c>all()</c>. It fails
    /// with the first failure to complete. Each call builds a new future.
    /// </summary>
    /// <returns>A future over every group's offsets.</returns>
    public KafkaFuture<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> All()
    {
        IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures = _futures;
        KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>[] values = new KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>[futures.Count];
        int next = 0;
        foreach (KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future in futures.Values)
        {
            values[next++] = future;
        }

        return KafkaFuture.AllOf(values).ThenApply<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(() =>
        {
            Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> offsets = new Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> entry in futures)
            {
                offsets.Add(entry.Key, entry.Value.GetNow(null!));
            }

            return offsets;
        });
    }

    internal static ListConsumerGroupOffsetsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> Futures, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures = new Dictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(native.Futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> entry in native.Futures)
        {
            futures.Add(entry.Key, new KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(entry.Value));
        }

        return new ListConsumerGroupOffsetsResult(futures, native.Result);
    }
}
