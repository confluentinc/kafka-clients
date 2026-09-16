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
/// The receive-path <b>copy-out</b> marshaller for a borrowed <c>Node_t</c>
/// (<c>kafka_common_Node</c>) — the innermost layer of the M5/P5 partition-metadata tree
/// (ffi-marshalling.md §B2/§B3, consumer-threading.md §27). Turns a borrowed
/// (Category-4) <c>Node_t</c> view into an owned managed <see cref="Node"/>, copying the
/// id / host / port / rack so <b>nothing native-backed escapes</b>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> A <c>Node_t</c> is a borrowed view (a
/// leader / replica element of a <c>PartitionInfo</c>); it has <b>no <c>_destroy</c></b>
/// and is <b>never freed</b> here — it dies with its owning <c>PartitionInfo</c>, which
/// dies with the root container. This runs entirely on the dispatcher thread before the
/// root <c>_destroy</c>, and retains no native pointer after returning.
/// </para>
/// <para>
/// <b>The length-delimited string form — the one shape difference from E1 (§B3).</b>
/// <c>Node_host</c> and <c>Node_rack</c> are <b>LENGTH-DELIMITED</b>
/// <c>(const char*, out int32_t len)</c>, NOT NUL-terminated — so they use
/// <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> with the ABI length, <b>never</b> a
/// NUL-scan (the slice borrows into the container with no terminator, so a scan
/// over-reads into the next field — the classic §B3 over-read AV). Contrast the
/// NUL-terminated topic strings elsewhere in the same tree
/// (<see cref="PartitionInfoMarshal"/> / <see cref="TopicPartitionInfoMapMarshal"/>).
/// </para>
/// <para>
/// <b>Rack absence.</b> <c>Node_rack</c> returns <c>(null, -1)</c> when absent;
/// <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> maps a null pointer (and a negative
/// length) to <see langword="null"/>, so <see cref="Node.Rack"/> is null when the rack is
/// absent (Java's <c>rack()</c> is nullable). Host is always present.
/// </para>
/// </remarks>
internal static class NodeMarshal
{
    /// <summary>
    /// Copies a borrowed <c>Node_t</c> at <paramref name="node"/> into an owned
    /// <see cref="Node"/>, or returns <see langword="null"/> when <paramref name="node"/>
    /// is <see cref="IntPtr.Zero"/> (an absent leader). The caller retains ownership of the
    /// owning root and must destroy it <b>after</b> this returns; this frees nothing.
    /// </summary>
    internal static Node? CopyOut(IntPtr node)
    {
        if (node == IntPtr.Zero)
        {
            // An absent node (e.g. a partition with no leader) → null Node.
            return null;
        }

        int id = NativeMethods.NodeId(node);

        // Host: length-delimited slice → owned string (§B3, NEVER NUL-scan). Host is always
        // present; normalize a defensive null to empty to keep Node.Host non-null.
        IntPtr hostPtr = NativeMethods.NodeHost(node, out int hostLen);
        string host = Utf8Marshal.PtrToString(hostPtr, hostLen) ?? string.Empty;

        int port = NativeMethods.NodePort(node);

        // Rack: length-delimited slice → owned string, or (null, -1) → null (§B3). Java's
        // rack() is nullable, so a null rack stays null (do NOT normalize to empty).
        IntPtr rackPtr = NativeMethods.NodeRack(node, out int rackLen);
        string? rack = Utf8Marshal.PtrToString(rackPtr, rackLen);

        return new Node(id, host, port, rack);
    }
}
