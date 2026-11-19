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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the mock producer's group offsets out of the owned containers the core returns: a <c>kafka_Map_t</c> of a
/// <c>char *</c> group id to a <c>kafka_Map_t *</c> of <c>TopicPartition_t</c> to <c>OffsetAndMetadata_t</c>, alone or
/// in a <c>kafka_List_t</c>. Only the root is destroyed, which frees the borrowed elements read on the way.
/// </summary>
internal static class GroupOffsetsMarshal
{
    internal static IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>
        CopyOutListAndDestroy(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return Array.Empty<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>();
        }

        try
        {
            IntPtr outErr = IntPtr.Zero;
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>> result =
                new List<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>(
                    Math.Max(count, 0));
            for (int i = 0; i < count; i++)
            {
                IntPtr groups = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                if (groups != IntPtr.Zero)
                {
                    result.Add(CopyOut(groups));
                }
            }

            return result;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    internal static IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>
        CopyOutMapAndDestroy(IntPtr map)
    {
        if (map == IntPtr.Zero)
        {
            return new Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>();
        }

        try
        {
            return CopyOut(map);
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MapDestroy(map, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    // A borrowed map: its keys, inner maps and their entries belong to the root the caller destroys.
    private static Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> CopyOut(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> result =
            new Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>(Math.Max(count, 0));
        for (int i = 0; i < count; i++)
        {
            IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string? groupId = Utf8Marshal.PtrToString(key);
            if (groupId is null || value == IntPtr.Zero)
            {
                continue;
            }

            result[groupId] = OffsetMapMarshal.CopyOut(value);
        }

        return result;
    }
}
