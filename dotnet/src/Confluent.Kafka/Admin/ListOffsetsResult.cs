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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.ListOffsets"/> — Java's
/// <c>org.apache.kafka.clients.admin.ListOffsetsResult</c>: one future per partition.
/// </summary>
/// <remarks>
/// A partition that fails fails only its own future. <see cref="PartitionResult"/> throws for a
/// partition that was not requested, as Java's does.
/// </remarks>
/// <seealso cref="AsyncListOffsetsResult"/>
public sealed class ListOffsetsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> _futures;
    private readonly IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's public constructor: a recorded deviation. The dictionary is held by
    // reference, as Java holds its map.
    internal ListOffsetsResult(IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    private ListOffsetsResult(
        IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures,
        IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>> nativeFutures,
        SafeAdminResultHandle native)
    {
        _futures = futures;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// The future for one requested partition — Java's <c>partitionResult(TopicPartition)</c>.
    /// </summary>
    /// <param name="partition">A partition that was part of the request.</param>
    /// <returns>That partition's future, carrying its own value or its own failure.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> was not requested, with Java's message.
    /// </exception>
    public KafkaFuture<ListOffsetsResultInfo> PartitionResult(TopicPartition partition)
    {
        if (!_futures.TryGetValue(partition, out KafkaFuture<ListOffsetsResultInfo> future))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "List Offsets for partition \"{0}\" was not attempted",
                    partition),
                nameof(partition));
        }

        return future;
    }

    /// <summary>
    /// Completes when every requested partition has resolved, yielding the whole map — Java's
    /// <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding every partition's offset information.</returns>
    public KafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResultInfo>> All() =>
        _nativeFutures is not null
            ? new KafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResultInfo>>(
                KafkaFutureImpl.GatherAll(_nativeFutures, EqualityComparer<TopicPartition>.Default))
            : SyncAdminFutures.GatherAll(_futures, EqualityComparer<TopicPartition>.Default);

    internal static ListOffsetsResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>> Futures, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures =
            new Dictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>(native.Futures.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>> entry in native.Futures)
        {
            futures.Add(entry.Key, new KafkaFuture<ListOffsetsResultInfo>(entry.Value));
        }

        return new ListOffsetsResult(futures, native.Futures, native.Result);
    }

    /// <summary>
    /// One partition's offset information — Java's <c>ListOffsetsResult.ListOffsetsResultInfo</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="LeaderEpoch"/> is <see langword="null"/> for Java's <c>Optional.empty()</c>, which the
    /// core reports as a leader epoch of <c>-1</c>.
    /// </remarks>
    public sealed class ListOffsetsResultInfo
    {
        /// <summary>
        /// Creates offset information — Java's
        /// <c>ListOffsetsResultInfo(long, long, Optional&lt;Integer&gt;)</c>.
        /// </summary>
        /// <param name="offset">The offset.</param>
        /// <param name="timestamp">The timestamp associated with it, or <c>-1</c> when the broker reported none.</param>
        /// <param name="leaderEpoch">
        /// The leader epoch, or <see langword="null"/> for Java's <c>Optional.empty()</c>. A negative
        /// value is present, not absent.
        /// </param>
        public ListOffsetsResultInfo(long offset, long timestamp, int? leaderEpoch)
        {
            Offset = offset;
            Timestamp = timestamp;
            LeaderEpoch = leaderEpoch;
        }

        /// <summary>The offset — Java's <c>offset()</c>.</summary>
        public long Offset { get; }

        /// <summary>
        /// The timestamp associated with the offset, or <c>-1</c> when the broker reported
        /// none — Java's <c>timestamp()</c>.
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// The leader epoch, or <see langword="null"/> when Java's <c>leaderEpoch()</c> is
        /// <c>Optional.empty()</c>.
        /// </summary>
        public int? LeaderEpoch { get; }

        /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
        /// <returns>The rendering.</returns>
        /// <remarks>
        /// Java interpolates an <c>Optional</c>, whose <c>toString</c> is <c>Optional[5]</c> or
        /// <c>Optional.empty</c>; that spelling is reproduced so the two clients' diagnostics read alike.
        /// </remarks>
        public override string ToString() =>
            string.Format(
                CultureInfo.InvariantCulture,
                "ListOffsetsResultInfo(offset={0}, timestamp={1}, leaderEpoch={2})",
                Offset,
                Timestamp,
                LeaderEpoch is null
                    ? "Optional.empty"
                    : string.Format(CultureInfo.InvariantCulture, "Optional[{0}]", LeaderEpoch.Value));
    }
}
