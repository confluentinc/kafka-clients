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

namespace Confluent.Kafka.Internal;

/// <summary>
/// A validated copy of a <c>TopicPartition</c> to <c>OffsetAndMetadata</c> map, as the parallel arrays
/// <c>NativeCallArguments.OffsetMap</c> builds the native map from: the consumer's commits and the producer's
/// <c>SendOffsetsToTransaction</c> share it.
/// </summary>
internal readonly struct OffsetsSnapshot
{
    private OffsetsSnapshot(
        string[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, string[] metadata, int count)
    {
        Topics = topics;
        Partitions = partitions;
        Offsets = offsets;
        LeaderEpochs = leaderEpochs;
        Metadata = metadata;
        Count = count;
    }

    internal string[] Topics { get; }

    internal int[] Partitions { get; }

    internal long[] Offsets { get; }

    internal int[] LeaderEpochs { get; }

    internal string[] Metadata { get; }

    internal int Count { get; }

    internal static OffsetsSnapshot Of(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
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
        int index = 0;
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
            TopicPartition tp = entry.Key;
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
            index++;
        }

        return new OffsetsSnapshot(topics, partitions, offsetValues, leaderEpochs, metadata, count);
    }
}
