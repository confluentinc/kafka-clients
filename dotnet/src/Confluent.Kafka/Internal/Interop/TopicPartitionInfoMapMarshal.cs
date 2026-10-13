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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The receive-path <b>copy-out</b> marshaller for <c>listTopics</c> — the outermost layer
/// of the M5/P5 partition-metadata tree (ffi-marshalling.md §B2/§B3/§B4,
/// consumer-threading.md §27). Turns an owned native <c>TopicPartitionInfoMap_t</c> (a
/// Category-3 borrow-root) into an owned managed
/// <see cref="IReadOnlyDictionary{String, IReadOnlyList}"/> of topic →
/// <see cref="PartitionInfo"/> list, reusing <see cref="PartitionInfoListMarshal"/> per
/// entry (the nested borrowed list is copied out too) so <b>nothing native-backed
/// escapes</b>. The caller (the completion trampoline) invokes <see cref="CopyOut"/> on the
/// core's dispatcher thread and then destroys the map root — safely, because after
/// <see cref="CopyOut"/> returns no borrowed pointer is retained.
/// </summary>
/// <remarks>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> Each topic string and each nested
/// <c>PartitionInfoList_t</c> value (from <c>get_partitions</c>, a <c>const *</c> return)
/// is a <b>borrowed</b> view — copied out here, <b>never</b> <c>PartitionInfoList_destroy</c>'d
/// as a map value (that would free a borrowed nested list). Only the map root is destroyed
/// (by the trampoline, after this returns). The whole 2-to-3-level tree (map → list → info →
/// node lists → node strings) is copied out before the root destroy.
/// </para>
/// <para>
/// <b>Two string forms coexist (§B3).</b> The topic keys here are <b>NUL-terminated</b>
/// (handle-owned) → <see cref="Utf8Marshal.PtrToString(IntPtr)"/> (NUL-scan); the
/// <c>Node</c> host/rack deeper in the tree are <b>length-delimited</b> (via
/// <see cref="NodeMarshal"/>). Use the matching form per accessor.
/// </para>
/// <para>
/// <b>Empty result.</b> <c>count ≤ 0</c> returns the shared
/// <see cref="EmptyReadOnlyDictionary{String, IReadOnlyList}"/> singleton (the E1 empty-map
/// precedent). Not null-safe on the root (the E1 finding): <c>_count</c> / <c>_get_*</c>
/// require a valid handle, so <see cref="CopyOut"/> is only called on the success branch.
/// </para>
/// </remarks>
internal static class TopicPartitionInfoMapMarshal
{
    /// <summary>
    /// Copies the owned native <paramref name="map"/> into an owned dictionary of topic →
    /// <see cref="PartitionInfo"/> list. The caller retains ownership of
    /// <paramref name="map"/> and must destroy it <b>after</b> this returns (it is never
    /// null on the success path the trampoline uses). An empty map (count ≤ 0) returns an
    /// empty, non-null dictionary.
    /// </summary>
    internal static IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> CopyOut(IntPtr map)
    {
        int count = NativeMethods.TopicPartitionInfoMapCount(map);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>.Instance;
        }

        Dictionary<string, IReadOnlyList<PartitionInfo>> result =
            new Dictionary<string, IReadOnlyList<PartitionInfo>>(count);
        for (int i = 0; i < count; i++)
        {
            // Topic key: NUL-terminated, handle-owned (§B3 NUL-scan form). Guarded by count;
            // a defensive null is skipped rather than keying on empty.
            IntPtr topicPtr = NativeMethods.TopicPartitionInfoMapGetTopic(map, i);
            if (topicPtr == IntPtr.Zero)
            {
                continue;
            }

            string topic = Utf8Marshal.PtrToString(topicPtr) ?? string.Empty;

            // Nested borrowed PartitionInfoList value → copy it out too (before the root
            // destroy); NEVER PartitionInfoList_destroy it here (borrowed map value). The
            // nested list is non-null (guarded by count), so CopyOut reads it directly.
            IntPtr listPtr = NativeMethods.TopicPartitionInfoMapGetPartitions(map, i);
            IReadOnlyList<PartitionInfo> partitions = listPtr == IntPtr.Zero
                ? Array.Empty<PartitionInfo>()
                : PartitionInfoListMarshal.CopyOut(listPtr);

            result[topic] = partitions;
        }

        return result;
    }
}
