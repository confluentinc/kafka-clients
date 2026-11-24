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

using System;
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A validated copy of a <c>TopicPartition</c> to <c>OffsetAndMetadata</c> map, as the parallel arrays
/// <c>NativeCallArguments.OffsetMap</c> builds the native map from: the consumer's commits and the producer's
/// <c>SendOffsetsToTransaction</c> share it.
/// </summary>
internal readonly struct OffsetsSnapshot
{
    private OffsetsSnapshot(
        string[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, string[] metadata, int count,
        string? nulMessage)
    {
        Topics = topics;
        Partitions = partitions;
        Offsets = offsets;
        LeaderEpochs = leaderEpochs;
        Metadata = metadata;
        Count = count;
        NulMessage = nulMessage;
    }

    internal string[] Topics { get; }

    internal int[] Partitions { get; }

    internal long[] Offsets { get; }

    internal int[] LeaderEpochs { get; }

    internal string[] Metadata { get; }

    internal int Count { get; }

    // The message for the first topic or metadata holding a NUL, which the C ABI would cut: the caller answers it
    // where it answers a null key, and the snapshot never crosses the C ABI.
    internal string? NulMessage { get; }

    // Throws for a null key, then for a NUL, as Java reaches the null key wherever it is in the map.
    internal static OffsetsSnapshot Of(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        OffsetsSnapshot snapshot = Snapshot(offsets, nullKeyGivesNone: false).GetValueOrDefault();
        return snapshot.NulMessage is null ? snapshot : throw NulString(snapshot.NulMessage);
    }

    // The snapshot, or null when a key is null, for the caller to answer it where Java does; the entries after it are
    // not read.
    internal static OffsetsSnapshot? TryOf(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        Snapshot(offsets, nullKeyGivesNone: true);

    internal static ArgumentNullException NullKey() => new ArgumentNullException("offsets", "offsets must not contain null");

    internal static ArgumentException NulString(string message) => new ArgumentException(message, "offsets");

    private static OffsetsSnapshot? Snapshot(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, bool nullKeyGivesNone)
    {
        if (offsets is null)
        {
            throw new ArgumentNullException(nameof(offsets));
        }

        int count = offsets.Count;
        string[] topics = new string[count];
        int[] partitions = new int[count];
        long[] offsetValues = new long[count];
        int[] leaderEpochs = new int[count];
        string[] metadata = new string[count];
        string? nulMessage = null;
        int index = 0;
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
            TopicPartition? tp = entry.Key;
            if (tp is null)
            {
                return nullKeyGivesNone ? null : throw NullKey();
            }

            if (tp.Topic() is null)
            {
                throw new ArgumentNullException(nameof(offsets), "offsets must not contain a TopicPartition with a null topic");
            }

            OffsetAndMetadata value = entry.Value;
            if (value is null)
            {
                throw new ArgumentNullException(nameof(offsets), "offsets must not contain null");
            }

            topics[index] = tp.Topic();
            partitions[index] = tp.Partition();
            offsetValues[index] = value.Offset();
            leaderEpochs[index] = value.LeaderEpoch() ?? -1;
            metadata[index] = value.Metadata() ?? string.Empty;
            nulMessage ??= NativeStrings.ContainsNul(topics[index]) ? NativeStrings.InvalidTopicMessage
                : NativeStrings.ContainsNul(metadata[index]) ? NativeStrings.InvalidOffsetMetadataMessage
                : null;
            index++;
        }

        return new OffsetsSnapshot(topics, partitions, offsetValues, leaderEpochs, metadata, count, nulMessage);
    }
}
