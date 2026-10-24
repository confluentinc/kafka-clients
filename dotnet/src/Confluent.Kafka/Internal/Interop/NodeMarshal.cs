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
/// Copies a <c>kafka_common_Node_t</c> into an owned <see cref="Node"/>; it frees nothing.
/// </summary>
internal static class NodeMarshal
{
    internal static Node? CopyOut(IntPtr node)
    {
        if (node == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.NodeId(node, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr host = NativeMethods.NodeHost(node, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int port = NativeMethods.NodePort(node, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr rack = NativeMethods.NodeRack(node, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        bool isFenced = NativeMethods.NodeIsFenced(node, ref outErr) != 0;
        KafkaException.ThrowIfFfiFailed(ref outErr);

        return new Node(id, Utf8Marshal.PtrToString(host) ?? string.Empty, port, Utf8Marshal.PtrToString(rack), isFenced);
    }

    internal static List<Node>? CopyOutList(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<Node> nodes = new List<Node>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr node = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Node? copy = CopyOut(node);
            if (copy is not null)
            {
                nodes.Add(copy);
            }
        }

        return nodes;
    }
}
