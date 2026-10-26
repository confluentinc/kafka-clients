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
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Copies the election outcomes and the partition reassignments the header delivers out of maps and handles built
/// here, and checks the copies outlive them.
/// </summary>
public sealed class AdminP4ResultMarshalTests
{
    private const int UnknownServerErrorCode = -1;

    private static readonly TopicPartition s_elected = new TopicPartition("élection", 0);
    private static readonly TopicPartition s_failed = new TopicPartition("élection", 1);

    [Fact]
    public void ElectLeaders_EveryPartitionIsCopiedOut_AndSurvivesTheDestroy()
    {
        IntPtr error = KafkaError("Election for élection-1 failed.");

        IReadOnlyDictionary<TopicPartition, KafkaException?> copied = CopyOutElection(
            new[] { (s_elected, IntPtr.Zero), (s_failed, error) });

        Assert.Equal(2, copied.Count);
        Assert.Null(copied[s_elected]);
        KafkaException failure = Assert.IsType<KafkaException>(copied[s_failed]);
        Assert.Equal(UnknownServerErrorCode, failure.Code);
        Assert.Equal("Election for élection-1 failed.", failure.Message);
    }

    [Fact]
    public void ElectLeaders_AnErrorThatIsNotAKafkaError_IsThrown()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("not a Kafka error"))
        {
            error = NativeMethods.ErrorLocalIllegalState(message.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);

        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(
            () => CopyOutElection(new[] { (s_failed, error) }));
        Assert.Equal("not a Kafka error", thrown.Message);
    }

    [Fact]
    public void ElectLeaders_AnEmptyMap_CopiesOutEmpty()
    {
        Assert.Empty(CopyOutElection(Array.Empty<(TopicPartition, IntPtr)>()));
    }

    [Fact]
    public void PartitionReassignment_TheThreeListsAreCopiedOutInOrder_AndSurviveTheDestroy()
    {
        PartitionReassignment copied = WithReassignment(
            new[] { 3, 1, 2 }, new[] { 2 }, new[] { 4, 0 }, PartitionReassignmentMarshal.CopyOut);

        Assert.Equal(new[] { 3, 1, 2 }, copied.Replicas);
        Assert.Equal(new[] { 2 }, copied.AddingReplicas);
        Assert.Equal(new[] { 4, 0 }, copied.RemovingReplicas);
    }

    [Fact]
    public void PartitionReassignment_NullListsAreCopiedOutEmpty()
    {
        PartitionReassignment copied = WithReassignment(null, null, null, PartitionReassignmentMarshal.CopyOut);

        Assert.Empty(copied.Replicas);
        Assert.Empty(copied.AddingReplicas);
        Assert.Empty(copied.RemovingReplicas);
    }

    [Fact]
    public void PartitionReassignments_TheMapIsCopiedOut_AndSurvivesTheDestroy()
    {
        IReadOnlyDictionary<TopicPartition, PartitionReassignment> copied = WithReassignment(
            new[] { 5 },
            new[] { 6 },
            null,
            reassignment =>
            {
                IntPtr outErr = IntPtr.Zero;
                IntPtr map = NativeMethods.MapNew(ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = IntPtr.Zero;
                try
                {
                    key = NewTopicPartition(s_elected);
                    NativeMethods.MapPut(map, key, reassignment, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    return PartitionReassignmentMarshal.CopyOutReassignments(map);
                }
                finally
                {
                    NativeMethods.MapDestroy(map, ref outErr);
                    NativeMethods.TopicPartitionDestroy(key, ref outErr);
                }
            });

        KeyValuePair<TopicPartition, PartitionReassignment> entry = Assert.Single(copied);
        Assert.Equal(s_elected, entry.Key);
        PartitionReassignment reassignment = entry.Value;
        Assert.Equal(new[] { 5 }, reassignment.Replicas);
        Assert.Equal(new[] { 6 }, reassignment.AddingReplicas);
        Assert.Empty(reassignment.RemovingReplicas);
    }

    private static IReadOnlyDictionary<TopicPartition, KafkaException?> CopyOutElection(
        IReadOnlyList<(TopicPartition Partition, IntPtr Error)> entries)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = NativeMethods.MapNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<IntPtr> keys = new List<IntPtr>();
        try
        {
            foreach ((TopicPartition partition, IntPtr error) in entries)
            {
                keys.Add(NewTopicPartition(partition));
                NativeMethods.MapPut(map, keys[keys.Count - 1], error, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return ElectLeadersMarshal.CopyOutPartitions(map);
        }
        finally
        {
            NativeMethods.MapDestroy(map, ref outErr);
            foreach (IntPtr key in keys)
            {
                NativeMethods.TopicPartitionDestroy(key, ref outErr);
            }

            foreach ((TopicPartition Partition, IntPtr Error) entry in entries)
            {
                NativeMethods.ErrorDestroy(entry.Error, ref outErr);
            }
        }
    }

    private static T WithReassignment<T>(
        int[]? replicas, int[]? addingReplicas, int[]? removingReplicas, Func<IntPtr, T> read)
    {
        List<GCHandle> pins = new List<GCHandle>();
        List<IntPtr> lists = new List<IntPtr>();
        IntPtr reassignment = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        try
        {
            IntPtr replicaList = NewIntList(replicas, pins, lists);
            IntPtr addingList = NewIntList(addingReplicas, pins, lists);
            IntPtr removingList = NewIntList(removingReplicas, pins, lists);
            reassignment = Native.PartitionReassignmentNew(replicaList, addingList, removingList, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            foreach (IntPtr list in lists)
            {
                NativeMethods.ListDestroy(list, ref outErr);
            }

            foreach (GCHandle pin in pins)
            {
                pin.Free();
            }
        }

        try
        {
            return read(reassignment);
        }
        finally
        {
            Native.PartitionReassignmentDestroy(reassignment, ref outErr);
        }
    }

    private static IntPtr NewIntList(int[]? values, List<GCHandle> pins, List<IntPtr> lists)
    {
        if (values is null)
        {
            return IntPtr.Zero;
        }

        GCHandle pin = GCHandle.Alloc(values, GCHandleType.Pinned);
        pins.Add(pin);
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ListNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        lists.Add(list);
        for (int i = 0; i < values.Length; i++)
        {
            NativeMethods.ListAdd(list, Marshal.UnsafeAddrOfPinnedArrayElement(values, i), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return list;
    }

    private static IntPtr NewTopicPartition(TopicPartition partition)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partition.Topic))
        {
            native = NativeMethods.TopicPartitionNew(topic.Pointer, partition.Partition, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr KafkaError(string message)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message))
        {
            error = Native.ErrorKafkaMessage(pinned.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return error;
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr PartitionReassignmentNew(
            IntPtr replicas, IntPtr addingReplicas, IntPtr removingReplicas, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void PartitionReassignmentDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_kafka_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorKafkaMessage(IntPtr message, ref IntPtr outErr);
    }
}
