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
/// Copies a <c>kafka_common_PartitionInfo_t</c> into an owned <see cref="PartitionInfo"/>, its nodes through
/// <see cref="NodeMarshal"/>; it frees each replica list the accessors hand out, never the partition info.
/// </summary>
internal static class PartitionInfoMarshal
{
    private delegate IntPtr NodeListAccessor(IntPtr info, ref IntPtr outErr);

    internal static PartitionInfo CopyOut(IntPtr info)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr topic = NativeMethods.PartitionInfoTopic(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int partition = NativeMethods.PartitionInfoPartition(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr leader = NativeMethods.PartitionInfoLeader(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        return new PartitionInfo(
            Utf8Marshal.PtrToString(topic) ?? string.Empty,
            partition,
            NodeMarshal.CopyOut(leader),
            CopyOutNodes(info, NativeMethods.PartitionInfoReplicas),
            CopyOutNodes(info, NativeMethods.PartitionInfoInSyncReplicas),
            CopyOutNodes(info, NativeMethods.PartitionInfoOfflineReplicas));
    }

    private static IReadOnlyList<Node> CopyOutNodes(IntPtr info, NodeListAccessor accessor)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = accessor(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<Node> nodes = new List<Node>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr node = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                if (node != IntPtr.Zero)
                {
                    nodes.Add(NodeMarshal.CopyOut(node)!);
                }
            }

            return nodes;
        }
        finally
        {
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }
}
