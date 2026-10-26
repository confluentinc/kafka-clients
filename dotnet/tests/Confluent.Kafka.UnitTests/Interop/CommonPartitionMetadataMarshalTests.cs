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
using System.Linq;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests <see cref="NodeMarshal"/>, <see cref="PartitionInfoMarshal"/> and <see cref="PartitionInfoListMarshal"/>
/// against nodes, partition infos and lists the C ABI builds, with no broker.
/// </summary>
public sealed class CommonPartitionMetadataMarshalTests : IDisposable
{
    private const string Topic = "tópico-α";

    private readonly List<IntPtr> _nodes = new List<IntPtr>();

    private readonly List<IntPtr> _partitionInfos = new List<IntPtr>();

    private readonly List<IntPtr> _lists = new List<IntPtr>();

    public void Dispose()
    {
        IntPtr outErr = IntPtr.Zero;
        foreach (IntPtr list in _lists)
        {
            NativeMethods.ListDestroy(list, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }

        foreach (IntPtr info in _partitionInfos)
        {
            NativeMethods.PartitionInfoDestroy(info, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }

        foreach (IntPtr node in _nodes)
        {
            NativeMethods.NodeDestroy(node, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }

    [Fact]
    public void NodeCopyOut_ReadsEveryField()
    {
        Node node = NodeMarshal.CopyOut(NewNode(7, "brøker-7.example", 9093, "rack-ø", isFenced: true))!;

        Assert.Equal(7, node.Id);
        Assert.Equal("brøker-7.example", node.Host);
        Assert.Equal(9093, node.Port);
        Assert.Equal("rack-ø", node.Rack);
        Assert.True(node.IsFenced);
    }

    [Fact]
    public void NodeCopyOut_NodeWithoutARack_HasANullRack()
    {
        Node node = NodeMarshal.CopyOut(NewNode(3, "host-3", 9092, rack: null, isFenced: false))!;

        Assert.Equal(3, node.Id);
        Assert.Equal("host-3", node.Host);
        Assert.Equal(9092, node.Port);
        Assert.Null(node.Rack);
        Assert.False(node.IsFenced);
    }

    [Fact]
    public void NodeCopyOut_Zero_IsNull()
    {
        Assert.Null(NodeMarshal.CopyOut(IntPtr.Zero));
    }

    [Fact]
    public void PartitionInfoCopyOut_ReadsTheTopicPartitionLeaderAndEachReplicaList()
    {
        IntPtr n1 = NewNode(1, "host-1", 9091, "rack-1", isFenced: false);
        IntPtr n2 = NewNode(2, "host-2", 9092, rack: null, isFenced: false);
        IntPtr n3 = NewNode(3, "host-3", 9093, "rack-3", isFenced: true);

        PartitionInfo info = PartitionInfoMarshal.CopyOut(
            NewPartitionInfo(Topic, 4, n1, NewList(n1, n2, n3), NewList(n2, n1), NewList(n3)));

        Assert.Equal(Topic, info.Topic);
        Assert.Equal(4, info.Partition);
        Assert.Equal(1, info.Leader!.Id);
        Assert.Equal(new[] { 1, 2, 3 }, info.Replicas.Select(node => node.Id));
        Assert.Equal(new[] { 2, 1 }, info.InSyncReplicas.Select(node => node.Id));
        Assert.Equal(new[] { 3 }, info.OfflineReplicas.Select(node => node.Id));

        Node offline = info.OfflineReplicas[0];
        Assert.Equal("host-3", offline.Host);
        Assert.Equal(9093, offline.Port);
        Assert.Equal("rack-3", offline.Rack);
        Assert.True(offline.IsFenced);
        Assert.Null(info.InSyncReplicas[0].Rack);
    }

    [Fact]
    public void PartitionInfoCopyOut_WithoutALeaderOrReplicas_HasANullLeaderAndEmptyLists()
    {
        PartitionInfo info = PartitionInfoMarshal.CopyOut(
            NewPartitionInfo(Topic, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero));

        Assert.Equal(Topic, info.Topic);
        Assert.Equal(0, info.Partition);
        Assert.Null(info.Leader);
        Assert.Empty(info.Replicas);
        Assert.Empty(info.InSyncReplicas);
        Assert.Empty(info.OfflineReplicas);
    }

    [Fact]
    public void PartitionInfoListCopyOut_ReadsABorrowedListInOrder_AndFreesNothing()
    {
        IntPtr leader = NewNode(1, "host-1", 9091, rack: null, isFenced: false);
        IntPtr first = NewPartitionInfo(Topic, 1, leader, NewList(leader), NewList(leader), IntPtr.Zero);
        IntPtr second = NewPartitionInfo("other", 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
        IntPtr list = NewList(first, second);

        IReadOnlyList<PartitionInfo> infos = PartitionInfoListMarshal.CopyOut(list);

        Assert.Equal(new[] { (Topic, 1), ("other", 0) }, infos.Select(info => (info.Topic, info.Partition)));
        Assert.Equal(1, infos[0].Leader!.Id);
        Assert.Null(infos[1].Leader);

        IntPtr outErr = IntPtr.Zero;
        Assert.Equal(2, NativeMethods.ListSize(list, ref outErr));
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.Equal(Topic, PartitionInfoMarshal.CopyOut(first).Topic);
    }

    [Fact]
    public void PartitionInfoListCopyOutAndDestroy_ReadsAnOwnedListOfOwnedCopies()
    {
        IntPtr n1 = NewNode(1, "host-1", 9091, "rack-1", isFenced: false);
        IntPtr n2 = NewNode(2, "host-2", 9092, "rack-2", isFenced: false);
        IntPtr partitions = NewList(
            NewPartitionInfo(Topic, 0, n1, NewList(n1, n2), NewList(n1), IntPtr.Zero),
            NewPartitionInfo(Topic, 1, n2, NewList(n2, n1), NewList(n2, n1), NewList(n1)),
            NewPartitionInfo("other", 0, n1, NewList(n1), NewList(n1), IntPtr.Zero));

        IntPtr outErr = IntPtr.Zero;
        IntPtr cluster = NativeMethods.ClusterNew(IntPtr.Zero, NewList(n1, n2), partitions, IntPtr.Zero, IntPtr.Zero, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        try
        {
            IReadOnlyList<PartitionInfo> infos = PartitionInfoListMarshal.CopyOutAndDestroy(PartitionsForTopic(cluster, Topic));

            PartitionInfo[] byPartition = infos.OrderBy(info => info.Partition).ToArray();
            Assert.Equal(new[] { 0, 1 }, byPartition.Select(info => info.Partition));
            Assert.All(byPartition, info => Assert.Equal(Topic, info.Topic));
            Assert.Equal(new[] { 1, 2 }, byPartition.Select(info => info.Leader!.Id));
            Assert.Equal(new[] { 2, 1 }, byPartition[1].Replicas.Select(node => node.Id));
            Assert.Equal(new[] { 2, 1 }, byPartition[1].InSyncReplicas.Select(node => node.Id));
            Assert.Equal(new[] { 1 }, byPartition[1].OfflineReplicas.Select(node => node.Id));
            Assert.Equal("rack-2", byPartition[1].Leader!.Rack);

            Assert.Empty(PartitionInfoListMarshal.CopyOutAndDestroy(PartitionsForTopic(cluster, "absent")));
        }
        finally
        {
            NativeMethods.ClusterDestroy(cluster, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }

    private static IntPtr PartitionsForTopic(IntPtr cluster, string topic)
    {
        IntPtr outErr = IntPtr.Zero;
        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
        IntPtr list = NativeMethods.ClusterPartitionsForTopic(cluster, topicPin.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, list);
        return list;
    }

    private IntPtr NewNode(int id, string host, int port, string? rack, bool isFenced)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr node;
        using (Utf8Marshal.PinnedUtf8String hostPin = Utf8Marshal.Pin(host))
        {
            if (rack is null)
            {
                node = NativeMethods.NodeWithRackIsFenced(
                    id, hostPin.Pointer, port, IntPtr.Zero, isFenced ? (sbyte)1 : (sbyte)0, ref outErr);
            }
            else
            {
                using Utf8Marshal.PinnedUtf8String rackPin = Utf8Marshal.Pin(rack);
                node = NativeMethods.NodeWithRackIsFenced(
                    id, hostPin.Pointer, port, rackPin.Pointer, isFenced ? (sbyte)1 : (sbyte)0, ref outErr);
            }
        }

        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, node);
        _nodes.Add(node);
        return node;
    }

    private IntPtr NewPartitionInfo(
        string topic, int partition, IntPtr leader, IntPtr replicas, IntPtr inSyncReplicas, IntPtr offlineReplicas)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr info;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        {
            info = NativeMethods.PartitionInfoWithOfflineReplicas(
                topicPin.Pointer, partition, leader, replicas, inSyncReplicas, offlineReplicas, ref outErr);
        }

        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, info);
        _partitionInfos.Add(info);
        return info;
    }

    private IntPtr NewList(params IntPtr[] elements)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ListNew(ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        _lists.Add(list);
        foreach (IntPtr element in elements)
        {
            NativeMethods.ListAdd(list, element, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }

        return list;
    }
}
