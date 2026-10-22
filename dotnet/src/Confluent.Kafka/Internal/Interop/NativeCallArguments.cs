// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>The native arguments one call borrows: built before the call and freed together after it, containers before their elements.</summary>
internal sealed class NativeCallArguments : IDisposable
{
    private readonly List<(IntPtr Handle, Kind Kind)> _handles = new List<(IntPtr Handle, Kind Kind)>();
    private List<Utf8Marshal.PinnedUtf8String>? _pins;
    private List<IntPtr>? _blocks;

    private enum Kind
    {
        List,
        Map,
        TopicPartition,
        OffsetAndMetadata,
        Node,
        PartitionInfo,
    }

    internal IntPtr Utf8(string value)
    {
        Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(value);
        (_pins ??= new List<Utf8Marshal.PinnedUtf8String>()).Add(pin);
        return pin.Pointer;
    }

    internal IntPtr NullableUtf8(string? value) => value is null ? IntPtr.Zero : Utf8(value);

    internal IntPtr TopicPartition(string topic, int partition)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.TopicPartitionNew(Utf8(topic), partition, ref outErr);
        return Track(handle, Kind.TopicPartition, ref outErr);
    }

    internal IntPtr TopicPartitionList(IReadOnlyList<(string Topic, int Partition)> partitions)
    {
        IntPtr list = List();
        for (int i = 0; i < partitions.Count; i++)
        {
            Add(list, TopicPartition(partitions[i].Topic, partitions[i].Partition));
        }

        return list;
    }

    internal IntPtr StringList(IReadOnlyList<string> values)
    {
        IntPtr list = List();
        for (int i = 0; i < values.Count; i++)
        {
            Add(list, Utf8(values[i]));
        }

        return list;
    }

    internal IntPtr OffsetAndMetadata(long offset, int leaderEpoch, string metadata)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.OffsetAndMetadataWithLeaderEpochMetadata(offset, leaderEpoch, Utf8(metadata), ref outErr);
        return Track(handle, Kind.OffsetAndMetadata, ref outErr);
    }

    internal IntPtr OffsetMap(NativeConsumer.CommitOffsetsSnapshot snapshot)
    {
        IntPtr map = Map();
        for (int i = 0; i < snapshot.Count; i++)
        {
            Put(
                map,
                TopicPartition(snapshot.Topics[i], snapshot.Partitions[i]),
                OffsetAndMetadata(snapshot.Offsets[i], snapshot.LeaderEpochs[i], snapshot.Metadata[i]));
        }

        return map;
    }

    internal IntPtr LongMap(string[] topics, int[] partitions, long[] values, int count)
    {
        IntPtr map = Map();
        IntPtr block = Values(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr value = block + (i * sizeof(long));
            Marshal.WriteInt64(value, values[i]);
            Put(map, TopicPartition(topics[i], partitions[i]), value);
        }

        return map;
    }

    internal IntPtr Node(int id, string host, int port, string? rack)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.NodeWithRackIsFenced(id, Utf8(host), port, NullableUtf8(rack), 0, ref outErr);
        return Track(handle, Kind.Node, ref outErr);
    }

    internal IntPtr PartitionInfo(string topic, int partition, IntPtr leader, IntPtr replicas, IntPtr inSyncReplicas)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.PartitionInfoWithOfflineReplicas(
            Utf8(topic), partition, leader, replicas, inSyncReplicas, IntPtr.Zero, ref outErr);
        return Track(handle, Kind.PartitionInfo, ref outErr);
    }

    internal IntPtr List()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.ListNew(ref outErr);
        return Track(handle, Kind.List, ref outErr);
    }

    internal void Add(IntPtr list, IntPtr element)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListAdd(list, element, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    public void Dispose()
    {
        for (int i = _handles.Count - 1; i >= 0; i--)
        {
            if (_handles[i].Kind is Kind.List or Kind.Map)
            {
                Destroy(_handles[i]);
            }
        }

        foreach ((IntPtr Handle, Kind Kind) handle in _handles)
        {
            if (handle.Kind is not (Kind.List or Kind.Map))
            {
                Destroy(handle);
            }
        }

        _handles.Clear();
        if (_pins is not null)
        {
            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }

            _pins.Clear();
        }

        if (_blocks is not null)
        {
            foreach (IntPtr block in _blocks)
            {
                Marshal.FreeHGlobal(block);
            }

            _blocks.Clear();
        }
    }

    private static void Destroy((IntPtr Handle, Kind Kind) entry)
    {
        IntPtr outErr = IntPtr.Zero;
        switch (entry.Kind)
        {
            case Kind.List:
                NativeMethods.ListDestroy(entry.Handle, ref outErr);
                break;
            case Kind.Map:
                NativeMethods.MapDestroy(entry.Handle, ref outErr);
                break;
            case Kind.TopicPartition:
                NativeMethods.TopicPartitionDestroy(entry.Handle, ref outErr);
                break;
            case Kind.OffsetAndMetadata:
                NativeMethods.OffsetAndMetadataDestroy(entry.Handle, ref outErr);
                break;
            case Kind.Node:
                NativeMethods.NodeDestroy(entry.Handle, ref outErr);
                break;
            case Kind.PartitionInfo:
                NativeMethods.PartitionInfoDestroy(entry.Handle, ref outErr);
                break;
        }

        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }

    private IntPtr Map()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.MapNew(ref outErr);
        return Track(handle, Kind.Map, ref outErr);
    }

    private void Put(IntPtr map, IntPtr key, IntPtr value)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MapPut(map, key, value, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private IntPtr Values(int count)
    {
        List<IntPtr> blocks = _blocks ??= new List<IntPtr>();
        blocks.Capacity = blocks.Count + 1;
        IntPtr block = Marshal.AllocHGlobal(Math.Max(count, 1) * sizeof(long));
        blocks.Add(block);
        return block;
    }

    private IntPtr Track(IntPtr handle, Kind kind, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        _handles.Add((handle, kind));
        return handle;
    }
}
