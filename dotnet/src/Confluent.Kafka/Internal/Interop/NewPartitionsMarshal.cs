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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the owned <c>kafka_admin_NewPartitions_t</c> for one <see cref="NewPartitions"/>, through
/// <c>increase_to</c> or <c>increase_to_with_new_assignments</c>; the caller destroys it once the submit, which
/// copies it, has returned.
/// </summary>
internal static class NewPartitionsMarshal
{
    // Presence of the list, never its count: the broker rejects Java's increaseTo(n, emptyList()) with
    // INVALID_REPLICA_ASSIGNMENT, where it accepts increaseTo(n).
    internal static bool HasAssignments(NewPartitions partitions) => partitions.Assignments is not null;

    internal static IntPtr Build(NewPartitions partitions)
    {
        IntPtr outErr = IntPtr.Zero;
        if (!HasAssignments(partitions))
        {
            IntPtr increased = NativeMethods.NewPartitionsIncreaseTo(partitions.TotalCount, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return increased;
        }

        IReadOnlyList<IReadOnlyList<int>> assignments = partitions.Assignments!;
        int count = 0;
        foreach (IReadOnlyList<int> assignment in assignments)
        {
            count += assignment.Count;
        }

        int[] ids = new int[count];
        GCHandle pinned = GCHandle.Alloc(ids, GCHandleType.Pinned);
        List<IntPtr> lists = new List<IntPtr>(assignments.Count);
        IntPtr outer = IntPtr.Zero;
        try
        {
            IntPtr first = pinned.AddrOfPinnedObject();
            outer = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int next = 0;
            foreach (IReadOnlyList<int> assignment in assignments)
            {
                IntPtr brokers = NativeMethods.ListNew(ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                lists.Add(brokers);
                foreach (int broker in assignment)
                {
                    ids[next] = broker;
                    NativeMethods.ListAdd(brokers, IntPtr.Add(first, next * sizeof(int)), ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    next++;
                }

                NativeMethods.ListAdd(outer, brokers, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            IntPtr native = NativeMethods.NewPartitionsIncreaseToWithNewAssignments(partitions.TotalCount, outer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            DestroyList(outer);
            foreach (IntPtr brokers in lists)
            {
                DestroyList(brokers);
            }

            pinned.Free();
        }
    }

    internal static void Destroy(IntPtr native)
    {
        if (native == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.NewPartitionsDestroy(native, ref outErr);
        ReleaseError(outErr);
    }

    private static void DestroyList(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        ReleaseError(outErr);
    }

    private static void ReleaseError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }
}
