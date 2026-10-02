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
/// The receive-path <b>copy-out</b> marshaller for <c>offsetsForTimes</c>
/// (ffi-marshalling.md §B2/§B3/§B4, §6.4). It turns a borrowed native
/// <c>OffsetAndTimestampMap_t</c> (a Category-3 borrow-root) into an owned managed
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> of <see cref="TopicPartition"/> →
/// <see cref="OffsetAndTimestamp"/>, copying every key/value so <b>nothing native-backed
/// escapes</b>. Identical shape to <see cref="OffsetMapMarshal"/>; the value reads
/// <c>offset</c> / <c>timestamp</c> / <c>leader_epoch</c> instead of
/// <c>offset</c> / <c>metadata</c> / <c>leader_epoch</c>.
/// </summary>
/// <remarks>
/// <b>Borrow discipline (§B2 Category 4).</b> The <c>TopicPartition_t</c> keys and
/// <c>OffsetAndTimestamp_t</c> values are borrowed map elements — read only during the
/// copy, never freed here; only the map root is destroyed (by the trampoline, after this
/// returns). The leader-epoch presence flag is honored (§0.1 / PLAN §1).
/// </remarks>
internal static class OffsetAndTimestampMapMarshal
{
    /// <summary>
    /// Copies the borrowed native <paramref name="map"/> into an owned dictionary. The
    /// caller retains ownership of <paramref name="map"/> and must destroy it
    /// <b>after</b> this returns. An empty map (count ≤ 0) returns an empty, non-null
    /// dictionary.
    /// </summary>
    internal static IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> CopyOut(IntPtr map)
    {
        int count = NativeMethods.OffsetAndTimestampMapCount(map);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>.Instance;
        }

        Dictionary<TopicPartition, OffsetAndTimestamp> result =
            new Dictionary<TopicPartition, OffsetAndTimestamp>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr keyPtr = NativeMethods.OffsetAndTimestampMapGetKey(map, i);
            IntPtr valuePtr = NativeMethods.OffsetAndTimestampMapGetValue(map, i);
            if (keyPtr == IntPtr.Zero || valuePtr == IntPtr.Zero)
            {
                // Defensive: get_* returns null only out of range, guarded by count.
                continue;
            }

            TopicPartition key = OffsetMapMarshalShared.CopyKey(keyPtr);

            long offset = NativeMethods.OffsetAndTimestampOffset(valuePtr);
            long timestamp = NativeMethods.OffsetAndTimestampTimestamp(valuePtr);
            int? leaderEpoch = OffsetMapMarshalShared.ReadLeaderEpoch(
                NativeMethods.OffsetAndTimestampLeaderEpoch(valuePtr, out int epoch), epoch);

            result[key] = new OffsetAndTimestamp(offset, timestamp, leaderEpoch);
        }

        return result;
    }
}
