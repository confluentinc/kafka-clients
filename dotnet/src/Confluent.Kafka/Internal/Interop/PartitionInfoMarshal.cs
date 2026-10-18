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
/// Copies a borrowed <c>kafka_common_PartitionInfo_t</c> view into an owned <see cref="PartitionInfo"/>, its leader
/// and replicas through <see cref="NodeMarshal"/>; it frees nothing.
/// </summary>
internal static class PartitionInfoMarshal
{
    internal static PartitionInfo CopyOut(IntPtr info)
    {
        string topic = Utf8Marshal.PtrToString(NativeMethods.PartitionInfoTopic(info)) ?? string.Empty;
        int partition = NativeMethods.PartitionInfoPartition(info);

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
                continue;
            }

            nodes.Add(NodeMarshal.CopyOut(nodePtr)!);
        }

        return nodes;
    }
}
