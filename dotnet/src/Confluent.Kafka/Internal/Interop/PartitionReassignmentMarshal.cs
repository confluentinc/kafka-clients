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
/// Copies the borrowed partition reassignments a <c>listPartitionReassignments</c> future delivers into owned
/// managed values.
/// </summary>
internal static class PartitionReassignmentMarshal
{
    private delegate IntPtr ReplicaList(IntPtr reassignment, ref IntPtr outErr);

    internal static IReadOnlyDictionary<TopicPartition, PartitionReassignment> CopyOutReassignments(IntPtr reassignments)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(reassignments, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<TopicPartition, PartitionReassignment> copied = new Dictionary<TopicPartition, PartitionReassignment>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr partition = NativeMethods.MapKey(reassignments, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr reassignment = NativeMethods.MapValue(reassignments, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[TopicPartitionListMarshal.CopyOutElement(partition)] = CopyOut(reassignment);
        }

        return copied;
    }

    internal static PartitionReassignment CopyOut(IntPtr reassignment) =>
        new PartitionReassignment(
            ReadList(reassignment, NativeMethods.PartitionReassignmentReplicas),
            ReadList(reassignment, NativeMethods.PartitionReassignmentAddingReplicas),
            ReadList(reassignment, NativeMethods.PartitionReassignmentRemovingReplicas));

    private static List<int> ReadList(IntPtr reassignment, ReplicaList accessor)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = accessor(reassignment, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<int> brokers = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr broker = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                brokers.Add(Marshal.ReadInt32(broker));
            }

            return brokers;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref destroyErr);
            if (destroyErr != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(destroyErr);
            }
        }
    }
}
