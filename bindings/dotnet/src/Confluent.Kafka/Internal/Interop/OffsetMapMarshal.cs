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
/// The receive-path <b>copy-out</b> marshaller for <c>committed</c>
/// (ffi-marshalling.md §B2/§B3/§B4, §6.4). It turns a borrowed native
/// <c>OffsetMap_t</c> (a Category-3 borrow-root) into an owned managed
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> of <see cref="TopicPartition"/> →
/// <see cref="OffsetAndMetadata"/>, copying every key/value so <b>nothing native-backed
/// escapes</b>. The caller (the completion trampoline) invokes <see cref="CopyOut"/> on
/// the core's dispatcher thread and then destroys the map root — safely, because after
/// <see cref="CopyOut"/> returns no borrowed pointer is retained.
/// </summary>
/// <remarks>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> Each <c>TopicPartition_t</c> key and each
/// <c>OffsetAndMetadata_t</c> value is a <b>borrowed</b> element of the map — read only
/// during the copy, <b>never freed</b> here. Only the map root is destroyed (by the
/// trampoline, after this returns).
/// </para>
/// <para>
/// <b>Strings are NUL-terminated (§B3 NUL-scan form).</b> The key's topic and the
/// value's metadata are handle-owned NUL-terminated <c>const char*</c> — marshalled via
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> (NUL-scan), NOT the length-delimited
/// receive-path form used for fetch-batch slices. Copied out before the root destroy.
/// </para>
/// <para>
/// <b>Leader epoch presence flag.</b> The value's <c>leader_epoch</c> accessor is the
/// presence-flag pattern (a 1-byte <c>bool</c> return + <c>out</c> epoch): <c>false</c> ⇒
/// <see langword="null"/>, <c>true</c> ⇒ the epoch. The <c>bool</c> return is honored —
/// the epoch is not hardcoded (PLAN §1 Critic check).
/// </para>
/// </remarks>
internal static class OffsetMapMarshal
{
    /// <summary>
    /// Copies the <b>owned</b> native <paramref name="map"/> into an owned dictionary and
    /// <b>destroys the root exactly once</b>, in a <c>finally</c> — so the handle is released
    /// even if the copy-out throws. The <see cref="TopicPartitionListMarshal.CopyOutAndDestroy"/>
    /// twin, for the <b>callback-owned</b> shape: use this wherever the ABI hands a map
    /// <em>to</em> a callback ("callbacks own the handles delivered to them",
    /// <c>confluent_kafka.h:243-248</c>), which is ffi-marshalling.md §B2's Category 3.
    /// </summary>
    /// <remarks>
    /// <b>Why this exists (M9/P7 review).</b> Before it, <see cref="CopyOut"/> was the only
    /// entry point here while the list marshaller had both — an asymmetry that made "copy the
    /// listener trampoline's shape" a **leak** on the commit path, and one that only a prose
    /// warning stood between a future phase and repeating. Having the twin removes the trap
    /// instead of guarding it: the destroy now lives in one place, shared with the paths that
    /// already got it right. <see cref="CopyOut"/> remains for the <em>query</em> paths, whose
    /// caller owns the root and destroys it itself.
    /// </remarks>
    internal static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> CopyOutAndDestroy(IntPtr map)
    {
        try
        {
            return CopyOut(map);
        }
        finally
        {
            // Null-safe. Exactly once, on every path — including a throwing copy-out. The
            // borrowed key/value ELEMENTS are never destroyed (§B2 Category 4); only the root.
            NativeMethods.OffsetMapDestroy(map);
        }
    }

    /// <summary>
    /// Copies the borrowed native <paramref name="map"/> into an owned dictionary. The
    /// caller retains ownership of <paramref name="map"/> and must destroy it
    /// <b>after</b> this returns (it is never null on the success path the trampoline
    /// uses). An empty map (count ≤ 0) returns an empty, non-null dictionary.
    /// </summary>
    /// <remarks>
    /// For a map the callee <b>owns</b> — one delivered <em>to</em> a callback — use
    /// <see cref="CopyOutAndDestroy"/> instead, which releases the root itself.
    /// </remarks>
    internal static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> CopyOut(IntPtr map)
    {
        int count = NativeMethods.OffsetMapCount(map);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance;
        }

        Dictionary<TopicPartition, OffsetAndMetadata> result = new Dictionary<TopicPartition, OffsetAndMetadata>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr keyPtr = NativeMethods.OffsetMapGetKey(map, i);
            IntPtr valuePtr = NativeMethods.OffsetMapGetValue(map, i);
            if (keyPtr == IntPtr.Zero || valuePtr == IntPtr.Zero)
            {
                // Defensive: get_* returns null only out of range, which count guards
                // against — skip rather than deref a null borrowed element.
                continue;
            }

            TopicPartition key = OffsetMapMarshalShared.CopyKey(keyPtr);

            long offset = NativeMethods.OffsetAndMetadataOffset(valuePtr);
            // NUL-terminated, handle-owned metadata string, copied out before the root
            // destroy (§B3). Java's OffsetAndMetadata.metadata() is never null → normalize.
            string metadata = Utf8Marshal.PtrToString(NativeMethods.OffsetAndMetadataMetadata(valuePtr)) ?? string.Empty;
            int? leaderEpoch = OffsetMapMarshalShared.ReadLeaderEpoch(
                NativeMethods.OffsetAndMetadataLeaderEpoch(valuePtr, out int epoch), epoch);

            result[key] = new OffsetAndMetadata(offset, metadata, leaderEpoch);
        }

        return result;
    }
}
