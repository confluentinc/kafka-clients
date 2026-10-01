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
/// The receive-path <b>copy-out</b> marshaller for <c>beginningOffsets</c> /
/// <c>endOffsets</c> (ffi-marshalling.md §B2/§B3/§B4, §6.4). It turns a borrowed native
/// <c>LongOffsetMap_t</c> (a Category-3 borrow-root) into an owned managed
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> of <see cref="TopicPartition"/> →
/// <see cref="long"/>. The simplest of the three offset-map marshallers: the value is a
/// bare <c>int64_t</c> returned <b>by value</b> (no handle, nothing borrowed to copy out
/// beyond the scalar), so only the <c>TopicPartition_t</c> key needs a borrow-safe copy.
/// </summary>
/// <remarks>
/// <b>Borrow discipline (§B2 Category 4).</b> The <c>TopicPartition_t</c> keys are
/// borrowed map elements — read only during the copy, never freed here; only the map
/// root is destroyed (by the trampoline, after this returns). Shared by both
/// <c>BeginningOffsets</c> and <c>EndOffsets</c> (they share <c>long_offsets_callback_t</c>
/// and this <c>LongOffsetMap_t</c> result).
/// </remarks>
internal static class LongOffsetMapMarshal
{
    /// <summary>
    /// Copies the borrowed native <paramref name="map"/> into an owned dictionary. The
    /// caller retains ownership of <paramref name="map"/> and must destroy it
    /// <b>after</b> this returns. An empty map (count ≤ 0) returns an empty, non-null
    /// dictionary.
    /// </summary>
    internal static IReadOnlyDictionary<TopicPartition, long> CopyOut(IntPtr map)
    {
        int count = NativeMethods.LongOffsetMapCount(map);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, long>.Instance;
        }

        Dictionary<TopicPartition, long> result = new Dictionary<TopicPartition, long>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr keyPtr = NativeMethods.LongOffsetMapGetKey(map, i);
            if (keyPtr == IntPtr.Zero)
            {
                // Defensive: get_key returns null only out of range, guarded by count.
                continue;
            }

            TopicPartition key = OffsetMapMarshalShared.CopyKey(keyPtr);
            long offset = NativeMethods.LongOffsetMapGetValue(map, i);
            result[key] = offset;
        }

        return result;
    }
}
