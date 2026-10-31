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
using System.Runtime.InteropServices;
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>Copies a <c>kafka_Map_t</c> of <c>TopicPartition_t</c> to <c>int64_t</c> into a managed dictionary.</summary>
internal static class LongOffsetMapMarshal
{
    internal static IReadOnlyDictionary<TopicPartition, long> CopyOutAndDestroy(IntPtr map)
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

    internal static IReadOnlyDictionary<TopicPartition, long> CopyOut(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, long>.Instance;
        }

        Dictionary<TopicPartition, long> result = new Dictionary<TopicPartition, long>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (key == IntPtr.Zero || value == IntPtr.Zero)
            {
                continue;
            }

            result[OffsetMapMarshalShared.CopyKey(key)] = Marshal.ReadInt64(value);
        }

        return result;
    }
}
