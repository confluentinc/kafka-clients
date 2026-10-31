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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The group listing and description copy-out over real C handles built through the header's
/// constructors, and the consumer-group offsets copy-out over a hand-built C map.
/// </summary>
public sealed class AdminP5ResultMarshalTests
{
    public static TheoryData<GroupState> GroupStates => Values<GroupState>();

    public static TheoryData<GroupType> GroupTypes => Values<GroupType>();

    public static TheoryData<AclOperation> Operations => Values<AclOperation>();

    [Fact]
    public void Listings_CopyOutEveryListingInOrder_AnAbsentTypeOrStateReadingAsNull()
    {
        using CHandles c = new CHandles();
        IntPtr list = c.List(
            c.Listing("g-ø", GroupType.Consumer, "consumer", GroupState.Stable),
            c.Listing("classic", GroupType.Classic, string.Empty, GroupState.Empty),
            c.Listing("old", null, "proto", null));

        GroupListing[] listings = GroupDescriptionMarshal.CopyOutListings(list).ToArray();

        Assert.Equal(new[] { "g-ø", "classic", "old" }, listings.Select(l => l.GroupId));
        Assert.Equal(new GroupType?[] { GroupType.Consumer, GroupType.Classic, null }, listings.Select(l => l.Type));
        Assert.Equal(new[] { "consumer", string.Empty, "proto" }, listings.Select(l => l.Protocol));
        Assert.Equal(
            new GroupState?[] { GroupState.Stable, GroupState.Empty, null },
            listings.Select(l => l.GroupState));
        Assert.True(listings[1].IsSimpleConsumerGroup);
        Assert.False(listings[0].IsSimpleConsumerGroup);
    }

    [Fact]
    public void Listings_AnEmptyList_CopiesOutEmpty()
    {
        using CHandles c = new CHandles();

        Assert.Empty(GroupDescriptionMarshal.CopyOutListings(c.List()));
        Assert.Empty(GroupDescriptionMarshal.CopyOutErrors(c.List()));
    }

    [Fact]
    public void Errors_CopyOutEachBorrowedError_InOrder_AndLeaveItToItsOwner()
    {
        using CHandles c = new CHandles();
        IntPtr list = c.List(
            c.Error(NewTimeoutError("listing timed out")),
            c.Error(NewGroupAuthorizationError("grüppe", "grüppe denied")));

        KafkaException[] errors = GroupDescriptionMarshal.CopyOutErrors(list).ToArray();

        Assert.Equal(new[] { 7, 30 }, errors.Select(e => e.Code));
        Assert.Equal(new[] { "listing timed out", "grüppe denied" }, errors.Select(e => e.Message));
        Assert.True(errors[0].IsRetriable);
        Assert.False(errors[1].IsRetriable);
    }

    [Theory]
    [MemberData(nameof(GroupStates))]
    public void EveryGroupState_CrossesAsItself(GroupState state)
    {
        using CHandles c = new CHandles();
        IntPtr list = c.List(c.Listing("g", GroupType.Consumer, "consumer", state));

        Assert.Equal(state, GroupMarshal.StateFrom(GroupMarshal.StateSingleton(state)));
        Assert.Equal(state, Assert.Single(GroupDescriptionMarshal.CopyOutListings(list)).GroupState);
    }

    [Theory]
    [MemberData(nameof(GroupTypes))]
    public void EveryGroupType_CrossesAsItself(GroupType type)
    {
        using CHandles c = new CHandles();
        IntPtr list = c.List(c.Listing("g", type, "consumer", GroupState.Stable));

        Assert.Equal(type, GroupMarshal.TypeFrom(GroupMarshal.TypeSingleton(type)));
        Assert.Equal(type, Assert.Single(GroupDescriptionMarshal.CopyOutListings(list)).Type);
    }

    [Fact]
    public void AnUndefinedStateOrType_HasNoSingleton()
    {
        ArgumentOutOfRangeException state =
            Assert.Throws<ArgumentOutOfRangeException>(() => GroupMarshal.StateSingleton((GroupState)99));
        ArgumentOutOfRangeException type =
            Assert.Throws<ArgumentOutOfRangeException>(() => GroupMarshal.TypeSingleton((GroupType)99));

        Assert.Equal("state", state.ParamName);
        Assert.StartsWith("Not a defined GroupState member.", state.Message, StringComparison.Ordinal);
        Assert.Equal("type", type.ParamName);
        Assert.StartsWith("Not a defined GroupType member.", type.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANullTypeOrState_DecodesToAbsence()
    {
        Assert.Null(GroupMarshal.TypeFrom(IntPtr.Zero));
        Assert.Null(GroupMarshal.StateFrom(IntPtr.Zero));
    }

    [Fact]
    public void ConsumerDescription_EveryFieldCopiesOut()
    {
        using CHandles c = new CHandles();
        IntPtr full = c.Member(
            "m-1", "instance-ø", "rack-a", "client-1", "/10.0.0.1",
            c.Assignment(("t", 0), ("t", 1)), c.Assignment(("u", 2)), memberEpoch: 4, upgraded: 1);
        IntPtr bare = c.Member("m-2", null, null, "client-2", "/10.0.0.2", c.Assignment(), IntPtr.Zero, -1, -1);
        IntPtr description = c.ConsumerDescription(
            "grüppe", isSimple: 0, c.List(full, bare), "uniform", GroupType.Consumer, GroupState.Stable,
            c.Node(3, "brøker", 9092, "r1"), c.Operations(AclOperation.Read, AclOperation.Describe), 7, 9);

        ConsumerGroupDescription copied = GroupDescriptionMarshal.CopyOutConsumerGroupDescription(description);

        Assert.Equal("grüppe", copied.GroupId);
        Assert.False(copied.IsSimpleConsumerGroup);
        Assert.Equal("uniform", copied.PartitionAssignor);
        Assert.Equal(GroupType.Consumer, copied.Type);
        Assert.Equal(GroupState.Stable, copied.GroupState);
        Assert.Equal(7, copied.GroupEpoch);
        Assert.Equal(9, copied.TargetAssignmentEpoch);
        Node coordinator = Assert.IsType<Node>(copied.Coordinator);
        Assert.Equal((3, "brøker", 9092, "r1"), (coordinator.Id, coordinator.Host, coordinator.Port, coordinator.Rack));
        Assert.Equal(
            new[] { AclOperation.Describe, AclOperation.Read },
            Assert.IsAssignableFrom<IReadOnlyCollection<AclOperation>>(copied.AuthorizedOperations).OrderByDescending(o => o));
        Assert.Equal(
            new[]
            {
                new MemberDescription(
                    "m-1", "instance-ø", "rack-a", "client-1", "/10.0.0.1",
                    new MemberAssignment(new[] { new TopicPartition("t", 0), new TopicPartition("t", 1) }),
                    new MemberAssignment(new[] { new TopicPartition("u", 2) }),
                    4,
                    true),
                new MemberDescription(
                    "m-2", null, null, "client-2", "/10.0.0.2", new MemberAssignment(null), null, null, null),
            },
            copied.Members);
    }

    [Fact]
    public void ConsumerDescription_AbsentOptionals_ReadAsAbsence_NotAsSentinels()
    {
        using CHandles c = new CHandles();
        IntPtr description = c.ConsumerDescription(
            "g", isSimple: 1, IntPtr.Zero, null, GroupType.Classic, GroupState.Empty, IntPtr.Zero, IntPtr.Zero, -1, -1);

        ConsumerGroupDescription copied = GroupDescriptionMarshal.CopyOutConsumerGroupDescription(description);

        Assert.True(copied.IsSimpleConsumerGroup);
        Assert.Empty(copied.Members);
        Assert.Equal(string.Empty, copied.PartitionAssignor);
        Assert.Null(copied.Coordinator);
        Assert.Null(copied.AuthorizedOperations);
        Assert.Null(copied.GroupEpoch);
        Assert.Null(copied.TargetAssignmentEpoch);
    }

    [Fact]
    public void ConsumerDescription_FalsyPresentValues_StayPresent()
    {
        using CHandles c = new CHandles();
        IntPtr member = c.Member("m", string.Empty, string.Empty, string.Empty, string.Empty, c.Assignment(), c.Assignment(), 0, 0);
        IntPtr description = c.ConsumerDescription(
            string.Empty, isSimple: 0, c.List(member), string.Empty, GroupType.Consumer, GroupState.Stable,
            IntPtr.Zero, c.Operations(), 0, 0);

        ConsumerGroupDescription copied = GroupDescriptionMarshal.CopyOutConsumerGroupDescription(description);

        Assert.Equal(0, copied.GroupEpoch);
        Assert.Equal(0, copied.TargetAssignmentEpoch);
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyCollection<AclOperation>>(copied.AuthorizedOperations));
        MemberDescription only = Assert.Single(copied.Members);
        Assert.Equal(string.Empty, only.GroupInstanceId);
        Assert.Equal(string.Empty, only.RackId);
        Assert.Equal(0, only.MemberEpoch);
        Assert.False(only.Upgraded);
        Assert.NotNull(only.TargetAssignment);
        Assert.Empty(only.TargetAssignment!.TopicPartitions);
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public void ConsumerDescription_EveryAuthorizedOperation_CrossesAsItself(AclOperation operation)
    {
        using CHandles c = new CHandles();
        IntPtr description = c.ConsumerDescription(
            "g", isSimple: 0, IntPtr.Zero, "range", GroupType.Consumer, GroupState.Stable,
            IntPtr.Zero, c.Operations(operation), -1, -1);

        ConsumerGroupDescription copied = GroupDescriptionMarshal.CopyOutConsumerGroupDescription(description);

        Assert.Equal(operation, Assert.Single(copied.AuthorizedOperations!));
    }

    [Fact]
    public void ClassicDescription_EveryFieldCopiesOut()
    {
        using CHandles c = new CHandles();
        IntPtr member = c.Member("m-1", "i-1", "rack-a", "client-1", "/10.0.0.1", c.Assignment(("t", 3)), IntPtr.Zero, -1, -1);
        IntPtr description = c.ClassicDescription(
            "klassisch-ø", "range", "consumer", c.List(member), Native.ClassicPreparingRebalance,
            c.Node(1, "host", 9093, null), c.Operations(AclOperation.Delete));

        ClassicGroupDescription copied = GroupDescriptionMarshal.CopyOutClassicGroupDescription(description);

        Assert.Equal("klassisch-ø", copied.GroupId);
        Assert.Equal("range", copied.Protocol);
        Assert.Equal("consumer", copied.ProtocolData);
        Assert.False(copied.IsSimpleConsumerGroup);
        Assert.Equal(ClassicGroupState.PreparingRebalance, copied.State);
        Node coordinator = Assert.IsType<Node>(copied.Coordinator);
        Assert.Equal((1, "host", 9093, (string?)null), (coordinator.Id, coordinator.Host, coordinator.Port, coordinator.Rack));
        Assert.Equal(AclOperation.Delete, Assert.Single(copied.AuthorizedOperations!));
        Assert.Equal(
            new MemberDescription(
                "m-1", "i-1", "rack-a", "client-1", "/10.0.0.1",
                new MemberAssignment(new[] { new TopicPartition("t", 3) }), null, null, null),
            Assert.Single(copied.Members));
    }

    [Fact]
    public void ClassicDescription_AnEmptyProtocol_IsASimpleGroup_AndAbsentOperationsAreNotEmpty()
    {
        using CHandles c = new CHandles();
        IntPtr absent = c.ClassicDescription(
            "g", string.Empty, string.Empty, IntPtr.Zero, Native.ClassicEmpty, IntPtr.Zero, IntPtr.Zero);
        IntPtr empty = c.ClassicDescription(
            "g", string.Empty, string.Empty, c.List(), Native.ClassicEmpty, IntPtr.Zero, c.Operations());

        ClassicGroupDescription absentCopy = GroupDescriptionMarshal.CopyOutClassicGroupDescription(absent);
        ClassicGroupDescription emptyCopy = GroupDescriptionMarshal.CopyOutClassicGroupDescription(empty);

        Assert.True(absentCopy.IsSimpleConsumerGroup);
        Assert.Equal(ClassicGroupState.Empty, absentCopy.State);
        Assert.Empty(absentCopy.Members);
        Assert.Null(absentCopy.Coordinator);
        Assert.Null(absentCopy.AuthorizedOperations);
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyCollection<AclOperation>>(emptyCopy.AuthorizedOperations));
    }

    private static TheoryData<T> Values<T>()
        where T : struct, Enum
    {
        TheoryData<T> data = new TheoryData<T>();
        foreach (T value in (T[])Enum.GetValues(typeof(T)))
        {
            data.Add(value);
        }

        return data;
    }

    private static IntPtr NewTimeoutError(string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorTimeout(pinned.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private static IntPtr NewGroupAuthorizationError(string groupId, string message)
    {
        using Utf8Marshal.PinnedUtf8String group = Utf8Marshal.Pin(groupId);
        using Utf8Marshal.PinnedUtf8String text = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = Native.GroupAuthorizationWithMessage(group.Pointer, text.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    /// <summary>Builds C handles for one test and frees each one, the lists first.</summary>
    private sealed class CHandles : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _pins = new List<Utf8Marshal.PinnedUtf8String>();

        private readonly List<IntPtr> _lists = new List<IntPtr>();

        private readonly List<IntPtr> _maps = new List<IntPtr>();

        private readonly List<(IntPtr Handle, Destroy Destroy)> _owned = new List<(IntPtr, Destroy)>();

        internal delegate void Destroy(IntPtr self, ref IntPtr outErr);

        public void Dispose()
        {
            IntPtr outErr = IntPtr.Zero;
            foreach (IntPtr map in _maps)
            {
                NativeMethods.MapDestroy(map, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr list in _lists)
            {
                NativeMethods.ListDestroy(list, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            for (int i = _owned.Count - 1; i >= 0; i--)
            {
                _owned[i].Destroy(_owned[i].Handle, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }
        }

        internal IntPtr List(params IntPtr[] elements)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _lists.Add(list);
            foreach (IntPtr element in elements)
            {
                NativeMethods.ListAdd(list, element, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return list;
        }

        internal IntPtr Error(IntPtr error) => Own(error, NativeMethods.ErrorDestroy);

        internal IntPtr Map(params (IntPtr Key, IntPtr Value)[] entries)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _maps.Add(map);
            foreach ((IntPtr key, IntPtr value) in entries)
            {
                NativeMethods.MapPut(map, key, value, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return map;
        }

        internal IntPtr TopicPartition(string topic, int partition)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr handle = NativeMethods.TopicPartitionNew(Pin(topic), partition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(handle, NativeMethods.TopicPartitionDestroy);
        }

        internal IntPtr OffsetAndMetadata(long offset, int leaderEpoch, string? metadata)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr handle = NativeMethods.OffsetAndMetadataWithLeaderEpochMetadata(offset, leaderEpoch, Pin(metadata), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(handle, NativeMethods.OffsetAndMetadataDestroy);
        }

        internal IntPtr Listing(string groupId, GroupType? type, string protocol, GroupState? state)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr listing = Native.GroupListingNew(
                Pin(groupId),
                type is null ? IntPtr.Zero : GroupMarshal.TypeSingleton(type.Value),
                Pin(protocol),
                state is null ? IntPtr.Zero : GroupMarshal.StateSingleton(state.Value),
                ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(listing, Native.GroupListingDestroy);
        }

        internal IntPtr Node(int id, string host, int port, string? rack)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr node = Native.NodeWithRack(id, Pin(host), port, Pin(rack), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(node, NativeMethods.NodeDestroy);
        }

        internal IntPtr Operations(params AclOperation[] operations)
        {
            IntPtr[] singletons = new IntPtr[operations.Length];
            for (int i = 0; i < operations.Length; i++)
            {
                IntPtr outErr = IntPtr.Zero;
                singletons[i] = NativeMethods.AclOperationFromCode((sbyte)operations[i], ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return List(singletons);
        }

        internal IntPtr Assignment(params (string Topic, int Partition)[] partitions)
        {
            IntPtr[] natives = new IntPtr[partitions.Length];
            for (int i = 0; i < partitions.Length; i++)
            {
                IntPtr tpErr = IntPtr.Zero;
                IntPtr partition = NativeMethods.TopicPartitionNew(Pin(partitions[i].Topic), partitions[i].Partition, ref tpErr);
                KafkaException.ThrowIfFfiFailed(ref tpErr);
                natives[i] = Own(partition, NativeMethods.TopicPartitionDestroy);
            }

            IntPtr outErr = IntPtr.Zero;
            IntPtr assignment = Native.MemberAssignmentNew(List(natives), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(assignment, Native.MemberAssignmentDestroy);
        }

        internal IntPtr Member(
            string memberId,
            string? groupInstanceId,
            string? rackId,
            string clientId,
            string host,
            IntPtr assignment,
            IntPtr targetAssignment,
            int memberEpoch,
            sbyte upgraded)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr member = Native.MemberDescriptionNew(
                Pin(memberId), Pin(groupInstanceId), Pin(rackId), Pin(clientId), Pin(host),
                assignment, targetAssignment, memberEpoch, upgraded, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(member, Native.MemberDescriptionDestroy);
        }

        internal IntPtr ConsumerDescription(
            string groupId,
            sbyte isSimple,
            IntPtr members,
            string? assignor,
            GroupType type,
            GroupState state,
            IntPtr coordinator,
            IntPtr operations,
            int groupEpoch,
            int targetAssignmentEpoch)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr description = Native.ConsumerGroupDescriptionNew(
                Pin(groupId), isSimple, members, Pin(assignor), GroupMarshal.TypeSingleton(type),
                GroupMarshal.StateSingleton(state), coordinator, operations, groupEpoch, targetAssignmentEpoch,
                ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(description, Native.ConsumerGroupDescriptionDestroy);
        }

        internal IntPtr ClassicDescription(
            string groupId,
            string protocol,
            string protocolData,
            IntPtr members,
            Native.Singleton state,
            IntPtr coordinator,
            IntPtr operations)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr singleton = state(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr description = Native.ClassicGroupDescriptionWithAuthorizedOperations(
                Pin(groupId), Pin(protocol), Pin(protocolData), members, singleton, coordinator, operations,
                ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Own(description, Native.ClassicGroupDescriptionDestroy);
        }

        private IntPtr Own(IntPtr handle, Destroy destroy)
        {
            Assert.NotEqual(IntPtr.Zero, handle);
            _owned.Add((handle, destroy));
            return handle;
        }

        private IntPtr Pin(string? value)
        {
            if (value is null)
            {
                return IntPtr.Zero;
            }

            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(value);
            _pins.Add(pinned);
            return pinned.Pointer;
        }
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal delegate IntPtr Singleton(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_group_authorization_with_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr GroupAuthorizationWithMessage(IntPtr groupId, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_GroupListing_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr GroupListingNew(IntPtr groupId, IntPtr type, IntPtr protocol, IntPtr state, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_GroupListing_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void GroupListingDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Node_with_rack_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr NodeWithRack(int id, IntPtr host, int port, IntPtr rack, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_MemberAssignment_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MemberAssignmentNew(IntPtr topicPartitions, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_MemberAssignment_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MemberAssignmentDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MemberDescriptionNew(
            IntPtr memberId,
            IntPtr groupInstanceId,
            IntPtr rackId,
            IntPtr clientId,
            IntPtr host,
            IntPtr assignment,
            IntPtr targetAssignment,
            int memberEpoch,
            sbyte upgraded,
            ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MemberDescriptionDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerGroupDescriptionNew(
            IntPtr groupId,
            sbyte isSimpleConsumerGroup,
            IntPtr members,
            IntPtr partitionAssignor,
            IntPtr groupType,
            IntPtr groupState,
            IntPtr coordinator,
            IntPtr authorizedOperations,
            int groupEpoch,
            int targetAssignmentEpoch,
            ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerGroupDescriptionDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_with_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClassicGroupDescriptionWithAuthorizedOperations(
            IntPtr groupId,
            IntPtr protocol,
            IntPtr protocolData,
            IntPtr members,
            IntPtr state,
            IntPtr coordinator,
            IntPtr authorizedOperations,
            ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ClassicGroupDescriptionDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_preparing_rebalance_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClassicPreparingRebalance(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_empty_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClassicEmpty(ref IntPtr outErr);
    }

    [Fact]
    public void ListOffsets_AnUncommittedPartition_IsListedWithANullValue()
    {
        using CHandles c = new CHandles();
        IntPtr map = c.Map(
            (c.TopicPartition("t", 0), c.OffsetAndMetadata(42, 9, "m")),
            (c.TopicPartition("t", 1), IntPtr.Zero),
            (c.TopicPartition("t", 2), c.OffsetAndMetadata(0, -1, null)));

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets = OffsetMapMarshal.CopyOutNullable(map);

        Assert.Equal(3, offsets.Count);

        OffsetAndMetadata committed = offsets[new TopicPartition("t", 0)]!;
        Assert.Equal(42, committed.Offset);
        Assert.Equal("m", committed.Metadata);
        Assert.Equal(9, committed.LeaderEpoch);

        Assert.True(offsets.ContainsKey(new TopicPartition("t", 1)));
        Assert.Null(offsets[new TopicPartition("t", 1)]);

        OffsetAndMetadata zero = offsets[new TopicPartition("t", 2)]!;
        Assert.Equal(0, zero.Offset);
        Assert.Equal(string.Empty, zero.Metadata);
        Assert.Null(zero.LeaderEpoch);
    }

    [Fact]
    public void ListOffsets_TheLeaderEpoch_IsNullOnlyWhenAbsent()
    {
        using CHandles c = new CHandles();
        IntPtr map = c.Map(
            (c.TopicPartition("t", 0), c.OffsetAndMetadata(5, -1, "a")),
            (c.TopicPartition("t", 1), c.OffsetAndMetadata(6, 0, "b")));

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets = OffsetMapMarshal.CopyOutNullable(map);

        Assert.Null(offsets[new TopicPartition("t", 0)]!.LeaderEpoch);
        Assert.Equal(0, offsets[new TopicPartition("t", 1)]!.LeaderEpoch);
    }

    [Fact]
    public void ListOffsets_AnEmptyMap_IsAnEmptyDictionary()
    {
        using CHandles c = new CHandles();

        Assert.Empty(OffsetMapMarshal.CopyOutNullable(c.Map()));
    }
}
