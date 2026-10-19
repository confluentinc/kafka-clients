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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a borrowed <c>kafka_common_Node_t</c> view into an owned <see cref="Node"/>, reading its host and rack by
/// their length; it frees nothing.
/// </summary>
internal static class NodeMarshal
{
    internal static Node? CopyOut(IntPtr node)
    {
        if (node == IntPtr.Zero)
        {
            return null;
        }

        int id = NativeMethods.NodeId(node);

        IntPtr hostPtr = NativeMethods.NodeHost(node, out int hostLen);
        string host = Utf8Marshal.PtrToString(hostPtr, hostLen) ?? string.Empty;

        int port = NativeMethods.NodePort(node);

        IntPtr rackPtr = NativeMethods.NodeRack(node, out int rackLen);
        string? rack = Utf8Marshal.PtrToString(rackPtr, rackLen);

        bool isFenced = NativeMethods.NodeIsFenced(node);

        return new Node(id, host, port, rack, isFenced);
    }
}
