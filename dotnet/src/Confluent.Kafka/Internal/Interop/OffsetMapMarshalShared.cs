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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// What the three offset-map marshallers share: copying a borrowed <c>TopicPartition_t</c> key, reading the leader
/// epoch's presence flag, and giving every requested partition an entry, <see langword="null"/> where the map has
/// none.
/// </summary>
internal static class OffsetMapMarshalShared
{
    internal static IReadOnlyDictionary<TopicPartition, TValue?> EveryRequested<TValue>(
        IReadOnlyDictionary<TopicPartition, TValue> found, (string Topic, int Partition)[]? requested)
        where TValue : class
    {
        Dictionary<TopicPartition, TValue?> result = Copy(found, requested?.Length ?? 0);
        if (requested is not null)
        {
            foreach ((string topic, int partition) in requested)
            {
                AddMissing(result, topic, partition);
            }
        }

        return result;
    }

    internal static IReadOnlyDictionary<TopicPartition, TValue?> EveryRequested<TValue>(
        IReadOnlyDictionary<TopicPartition, TValue> found, string[] topics, int[] partitions, int count)
        where TValue : class
    {
        Dictionary<TopicPartition, TValue?> result = Copy(found, count);
        for (int i = 0; i < count; i++)
        {
            AddMissing(result, topics[i], partitions[i]);
        }

        return result;
    }

    private static Dictionary<TopicPartition, TValue?> Copy<TValue>(
        IReadOnlyDictionary<TopicPartition, TValue> found, int requested)
        where TValue : class
    {
        Dictionary<TopicPartition, TValue?> result =
            new Dictionary<TopicPartition, TValue?>(Math.Max(found.Count, requested));
        foreach (KeyValuePair<TopicPartition, TValue> entry in found)
        {
            result[entry.Key] = entry.Value;
        }

        return result;
    }

    private static void AddMissing<TValue>(Dictionary<TopicPartition, TValue?> result, string topic, int partition)
        where TValue : class
    {
        TopicPartition key = new TopicPartition(topic, partition);
        if (!result.ContainsKey(key))
        {
            result[key] = null;
        }
    }

    internal static TopicPartition CopyKey(IntPtr keyPtr) => TopicPartitionListMarshal.CopyOutElement(keyPtr);

    internal static int? ReadLeaderEpoch(bool present, int epoch) => present ? epoch : (int?)null;
}
