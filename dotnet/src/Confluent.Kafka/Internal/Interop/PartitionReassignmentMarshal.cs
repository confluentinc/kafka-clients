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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a <b>borrowed</b> <c>kafka_admin_PartitionReassignment_t</c> out into an owned
/// managed <see cref="PartitionReassignment"/>, before the result root that owns it dies.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The three lists are read through six FLATTENED accessors — there is no child
/// handle per list.</b> <c>replica_count</c>/<c>replica(i)</c> and the adding/removing
/// pairs, the same pattern M15/P3's replica-info tree uses. So nothing here is a handle
/// that could outlive the root, and what remains is the ordinary copy-out rule: every read
/// must happen before <c>ListPartitionReassignmentsResult_destroy</c> (ffi §B4).
/// </para>
/// <para>
/// A null pointer yields <see langword="null"/>, mirroring the ABI's documented
/// out-of-range return. The walker never passes one — <c>CompleteAggregate</c> reads
/// strictly inside <c>count</c> — so it is defence, not a live branch.
/// </para>
/// </remarks>
internal static class PartitionReassignmentMarshal
{
    /// <summary>Copies out one reassignment.</summary>
    /// <param name="reassignment">The borrowed pointer, or <see cref="IntPtr.Zero"/>.</param>
    /// <returns>An owned value, or <see langword="null"/> for a null pointer.</returns>
    internal static PartitionReassignment? CopyOut(IntPtr reassignment)
    {
        if (reassignment == IntPtr.Zero)
        {
            return null;
        }

        return new PartitionReassignment(
            ReadList(
                reassignment,
                NativeMethods.PartitionReassignmentReplicaCount,
                NativeMethods.PartitionReassignmentReplica),
            ReadList(
                reassignment,
                NativeMethods.PartitionReassignmentAddingReplicaCount,
                NativeMethods.PartitionReassignmentAddingReplica),
            ReadList(
                reassignment,
                NativeMethods.PartitionReassignmentRemovingReplicaCount,
                NativeMethods.PartitionReassignmentRemovingReplica));
    }

    /// <summary>
    /// Reads one count/element accessor pair into an owned list.
    /// </summary>
    /// <remarks>
    /// The accessors are taken as parameters so the three identical walks are one body —
    /// the same reason M15/P4 Stage 1's reader factories exist, and it is what lets a test
    /// drive this body once and cover all three lists.
    /// </remarks>
    private static List<int> ReadList(
        IntPtr reassignment, Func<IntPtr, int> count, Func<IntPtr, int, int> element)
    {
        int total = count(reassignment);
        List<int> brokers = new List<int>(Math.Max(total, 0));
        for (int index = 0; index < total; index++)
        {
            brokers.Add(element(reassignment, index));
        }

        return brokers;
    }
}
