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

/// <summary>Copies a <c>kafka_Map_t</c> of topic to a <c>kafka_List_t</c> of <c>PartitionInfo_t</c> into a managed dictionary.</summary>
internal static class TopicPartitionInfoMapMarshal
{
    internal static IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> CopyOutAndDestroy(IntPtr map)
    {
        try
        {
            return CopyOut(map);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapDestroy(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    internal static IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> CopyOut(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>.Instance;
        }

        Dictionary<string, IReadOnlyList<PartitionInfo>> result =
            new Dictionary<string, IReadOnlyList<PartitionInfo>>(count, StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string? topic = Utf8Marshal.PtrToString(key);
            if (topic is null)
            {
                continue;
            }

            result[topic] = value == IntPtr.Zero ? Array.Empty<PartitionInfo>() : PartitionInfoListMarshal.CopyOut(value);
        }

        return result;
    }
}
