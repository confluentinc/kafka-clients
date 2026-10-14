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
/// The receive-path <b>copy-out</b> marshaller for a borrowed <c>PartitionInfo_t</c> — the
/// middle layer of the M5/P5 partition-metadata tree (ffi-marshalling.md §B2/§B3,
/// consumer-threading.md §27). Turns a borrowed (Category-4) <c>PartitionInfo_t</c> view
/// into an owned managed <see cref="PartitionInfo"/>, reusing <see cref="NodeMarshal"/>
/// for the leader and the three replica lists so <b>nothing native-backed escapes</b>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> A <c>PartitionInfo_t</c> obtained from a
/// <c>PartitionInfoList_get</c> / <c>TopicPartitionInfoMap_get_partitions</c> element is a
/// <b>borrowed</b> view (a <c>const *</c> return) — it is <b>never</b>
/// <c>PartitionInfo_destroy</c>'d here (that destroy is only for a standalone-owned info;
/// freeing a list/map element is a double-free/UAF). Its leader / replica <c>Node_t</c>
/// views are likewise borrowed (no <c>Node_destroy</c>). Only the root container is
/// destroyed, by the trampoline, after this returns. This runs on the dispatcher thread
/// before the root <c>_destroy</c> and retains no native pointer.
/// </para>
/// <para>
/// <b>Two string forms coexist in one tree (§B3).</b> The topic here is
/// <b>NUL-terminated</b> (handle-owned) → <see cref="Utf8Marshal.PtrToString(IntPtr)"/>
/// (NUL-scan); the <c>Node</c> host/rack that <see cref="NodeMarshal"/> reads are
/// <b>length-delimited</b> (§B3). The marshaller must not use one form uniformly.
/// </para>
/// <para>
/// <b>Leader may be null.</b> <c>PartitionInfo_leader</c> returns null when the partition
/// has no leader; <see cref="NodeMarshal.CopyOut"/> maps a null pointer to a null
/// <see cref="Node"/>, so <see cref="PartitionInfo.Leader"/> is nullable (Java parity).
/// </para>
/// </remarks>
internal static class PartitionInfoMarshal
{
    /// <summary>
    /// Copies a borrowed <c>PartitionInfo_t</c> at <paramref name="info"/> into an owned
    /// <see cref="PartitionInfo"/>. The caller retains ownership of the owning root and
    /// must destroy it <b>after</b> this returns; this frees nothing.
    /// </summary>
    internal static PartitionInfo CopyOut(IntPtr info)
    {
        // Topic: NUL-terminated, handle-owned (§B3 NUL-scan form) — NOT the length form.
        // A partition-info always has a topic; normalize a defensive null to empty.
        string topic = Utf8Marshal.PtrToString(NativeMethods.PartitionInfoTopic(info)) ?? string.Empty;
        int partition = NativeMethods.PartitionInfoPartition(info);

        // Leader: borrowed Node view, may be null → nullable (NodeMarshal maps Zero → null).
        Node? leader = NodeMarshal.CopyOut(NativeMethods.PartitionInfoLeader(info));

        IReadOnlyList<Node> replicas = CopyNodes(
            NativeMethods.PartitionInfoReplicaCount(info),
            NativeMethods.PartitionInfoReplica,
            info);
        IReadOnlyList<Node> inSyncReplicas = CopyNodes(
            NativeMethods.PartitionInfoInSyncReplicaCount(info),
            NativeMethods.PartitionInfoInSyncReplica,
            info);
        IReadOnlyList<Node> offlineReplicas = CopyNodes(
            NativeMethods.PartitionInfoOfflineReplicaCount(info),
            NativeMethods.PartitionInfoOfflineReplica,
            info);

        return new PartitionInfo(topic, partition, leader, replicas, inSyncReplicas, offlineReplicas);
    }

    /// <summary>
    /// Copies a borrowed <c>Node</c> list (<paramref name="count"/> elements read via
    /// <paramref name="elementAt"/>) into an owned <see cref="IReadOnlyList{Node}"/>, each
    /// element via <see cref="NodeMarshal.CopyOut"/>. An empty list (count ≤ 0) returns the
    /// shared <see cref="Array.Empty{Node}"/> — no allocation (§8: no new
    /// <c>EmptyReadOnlyList</c> surface). Never frees a borrowed <c>Node</c> element.
    /// </summary>
    private static IReadOnlyList<Node> CopyNodes(int count, Func<IntPtr, int, IntPtr> elementAt, IntPtr info)
    {
        if (count <= 0)
        {
            return Array.Empty<Node>();
        }

        List<Node> nodes = new List<Node>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr nodePtr = elementAt(info, i);
            if (nodePtr == IntPtr.Zero)
            {
                // Defensive: the replica accessors return null only out of range, which
                // count guards against — skip rather than add a null element (the replica
                // lists are never expected to contain an absent node).
                continue;
            }

            // A replica Node is always present (non-null here), so CopyOut returns non-null;
            // the null-forgiving `!` reflects that the Zero case is already filtered above.
            nodes.Add(NodeMarshal.CopyOut(nodePtr)!);
        }

        return nodes;
    }
}
