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
/// The receive-path <b>copy-out</b> marshaller for <c>partitionsFor</c> — turns an owned
/// native <c>PartitionInfoList_t</c> (a Category-3 borrow-root) into an owned managed
/// <see cref="IReadOnlyList{PartitionInfo}"/>, copying every element via
/// <see cref="PartitionInfoMarshal"/> so <b>nothing native-backed escapes</b>
/// (ffi-marshalling.md §B2/§B3/§B4, consumer-threading.md §27). Also the per-topic value
/// copy-out reused by <see cref="TopicPartitionInfoMapMarshal"/>. The caller (the
/// completion trampoline) invokes <see cref="CopyOut"/> on the core's dispatcher thread
/// and then destroys the list root — safely, because after <see cref="CopyOut"/> returns
/// no borrowed pointer is retained.
/// </summary>
/// <remarks>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> Each <c>PartitionInfo_t</c> element (from
/// <c>PartitionInfoList_get</c>, a <c>const *</c> return) is a <b>borrowed</b> view — read
/// only during the copy, <b>never</b> <c>PartitionInfo_destroy</c>'d here. Only the list
/// root is destroyed (by the trampoline, after this returns). The whole nested tree
/// (info → leader / replicas → node strings) is copied out before the root destroy.
/// </para>
/// <para>
/// <b>Empty result.</b> <c>count ≤ 0</c> returns the shared <see cref="Array.Empty{PartitionInfo}"/>
/// — no allocation, no new <c>EmptyReadOnlyList</c> surface (§8). The mock returns an empty
/// list broker-free for an unregistered topic (via <c>partitions_for</c>).
/// </para>
/// <para>
/// <b>Not null-safe on the root (the E1 finding).</b> <c>PartitionInfoList_count</c> /
/// <c>_get</c> require a valid handle (only <c>_destroy</c> is null-safe), so
/// <see cref="CopyOut"/> is only ever called on the success branch (non-null root); the
/// failure branch has a null root and calls only the null-safe
/// <see cref="NativeMethods.PartitionInfoListDestroy"/>.
/// </para>
/// </remarks>
internal static class PartitionInfoListMarshal
{
    /// <summary>
    /// Copies the owned native <paramref name="list"/> into an owned list of
    /// <see cref="PartitionInfo"/>. The caller retains ownership of <paramref name="list"/>
    /// and must destroy it <b>after</b> this returns (it is never null on the success path
    /// the trampoline uses; when reached as a borrowed map value it is likewise non-null,
    /// guarded by the map count).
    /// </summary>
    internal static IReadOnlyList<PartitionInfo> CopyOut(IntPtr list)
    {
        int count = NativeMethods.PartitionInfoListCount(list);
        if (count <= 0)
        {
            return Array.Empty<PartitionInfo>();
        }

        List<PartitionInfo> result = new List<PartitionInfo>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr info = NativeMethods.PartitionInfoListGet(list, i);
            if (info == IntPtr.Zero)
            {
                // Defensive: get() returns null only out of range, which count guards
                // against — skip rather than deref a null borrowed view.
                continue;
            }

            result.Add(PartitionInfoMarshal.CopyOut(info));
        }

        return result;
    }
}
