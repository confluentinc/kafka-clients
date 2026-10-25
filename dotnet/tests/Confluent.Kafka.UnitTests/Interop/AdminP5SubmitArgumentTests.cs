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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What the group options become in C, read back through the header's getters, and the checks the
/// group RPCs make before they call the core.
/// </summary>
public sealed class AdminP5SubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    public static TheoryData<GroupState> GroupStates => Values<GroupState>();

    public static TheoryData<GroupType> GroupTypes => Values<GroupType>();

    [Fact]
    public void ListGroupsOptions_Defaults_SendNoFilterAndAnUnsetTimeout()
    {
        Assert.Empty(new ListGroupsOptions().Setters.InOrder);

        foreach (ListGroupsOptions? options in new[] { null, new ListGroupsOptions() })
        {
            ListGroupsReadBack sent = ReadBack(options);
            Assert.Equal(-1, sent.TimeoutMs);
            Assert.Empty(sent.GroupStates);
            Assert.Empty(sent.ProtocolTypes);
            Assert.Empty(sent.Types);
        }
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void ListGroupsOptions_Timeout_IsSentAsTheCoreReadsIt(int? timeoutMs, int expected)
    {
        ListGroupsOptions options = new ListGroupsOptions { TimeoutMs = timeoutMs };

        Assert.Equal(new[] { nameof(ListGroupsOptions.TimeoutMs) }, options.Setters.InOrder);
        Assert.Equal(expected, ReadBack(options).TimeoutMs);
    }

    [Fact]
    public void ListGroupsOptions_EachSetterRunTwice_SendsItsLastValue()
    {
        ListGroupsOptions options = new ListGroupsOptions
        {
            TimeoutMs = 5,
            GroupStates = new[] { GroupState.Stable },
            ProtocolTypes = new[] { "consumer" },
            Types = new[] { GroupType.Share },
        };
        options.TimeoutMs = 7;
        options.GroupStates = new[] { GroupState.Dead, GroupState.Empty };
        options.ProtocolTypes = Array.Empty<string>();
        options.Types = null!;

        Assert.Equal(
            new[]
            {
                nameof(ListGroupsOptions.TimeoutMs),
                nameof(ListGroupsOptions.GroupStates),
                nameof(ListGroupsOptions.ProtocolTypes),
                nameof(ListGroupsOptions.Types),
            },
            options.Setters.InOrder);

        ListGroupsReadBack sent = ReadBack(options);
        Assert.Equal(7, sent.TimeoutMs);
        Assert.Equal(new[] { GroupState.Dead, GroupState.Empty }, sent.GroupStates.OrderBy(state => state));
        Assert.Empty(sent.ProtocolTypes);
        Assert.Empty(sent.Types);
    }

    [Theory]
    [MemberData(nameof(GroupStates))]
    public void ListGroupsOptions_EveryGroupState_CrossesAsItself(GroupState state) =>
        Assert.Equal(new[] { state }, ReadBack(new ListGroupsOptions { GroupStates = new[] { state } }).GroupStates);

    [Theory]
    [MemberData(nameof(GroupTypes))]
    public void ListGroupsOptions_EveryGroupType_CrossesAsItself(GroupType type) =>
        Assert.Equal(new[] { type }, ReadBack(new ListGroupsOptions { Types = new[] { type } }).Types);

    [Fact]
    public void ListGroupsOptions_TheThreeFilters_EachCrossWithItsOwnValues()
    {
        ListGroupsReadBack sent = ReadBack(new ListGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable, GroupState.Empty, GroupState.Stable, GroupState.Reconciling },
            ProtocolTypes = new[] { "", "consumer", "prötocol-日本" },
            Types = new[] { GroupType.Consumer },
        });

        Assert.Equal(
            new[] { GroupState.Stable, GroupState.Empty, GroupState.Reconciling },
            sent.GroupStates.OrderBy(state => state));
        Assert.Equal(
            new[] { "", "consumer", "prötocol-日本" },
            sent.ProtocolTypes.OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal(new[] { GroupType.Consumer }, sent.Types);
    }

    [Fact]
    public void ListGroupsOptions_JavasFactories_SendTheFiltersTheyDocument()
    {
        ListGroupsOptions consumer = ListGroupsOptions.ForConsumerGroups();
        Assert.Equal(
            new[] { nameof(ListGroupsOptions.Types), nameof(ListGroupsOptions.ProtocolTypes) },
            consumer.Setters.InOrder);
        ListGroupsReadBack forConsumer = ReadBack(consumer);
        Assert.Equal(new[] { GroupType.Consumer, GroupType.Classic }, forConsumer.Types.OrderBy(type => type));
        Assert.Equal(new[] { "", "consumer" }, forConsumer.ProtocolTypes.OrderBy(name => name, StringComparer.Ordinal));
        Assert.Empty(forConsumer.GroupStates);
        Assert.Equal(-1, forConsumer.TimeoutMs);

        ListGroupsReadBack forShare = ReadBack(ListGroupsOptions.ForShareGroups());
        Assert.Equal(new[] { GroupType.Share }, forShare.Types);
        Assert.Empty(forShare.ProtocolTypes);

        ListGroupsReadBack forStreams = ReadBack(ListGroupsOptions.ForStreamsGroups());
        Assert.Equal(new[] { GroupType.Streams }, forStreams.Types);
        Assert.Empty(forStreams.ProtocolTypes);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ListGroups_AnUndefinedStateOrType_IsRejectedBeforeTheCall(bool onTheStateAxis)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ListGroupsOptions options = onTheStateAxis
            ? new ListGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }
            : new ListGroupsOptions { Types = new[] { (GroupType)99 } };

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(() => admin.ListGroups(options));

        Assert.Equal("options", failure.ParamName);
        Assert.StartsWith(
            onTheStateAxis
                ? "ListGroupsOptions.GroupStates must contain only defined GroupState members."
                : "ListGroupsOptions.Types must contain only defined GroupType members.",
            failure.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ListGroups_AProtocolTypeThatCannotCross_IsRejectedBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentException failure = Assert.Throws<ArgumentException>(
            () => admin.ListGroups(new ListGroupsOptions { ProtocolTypes = new[] { "con\0sumer" } }));

        Assert.Equal("options", failure.ParamName);
        Assert.StartsWith(AdminStrings.InvalidStringMessage, failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ListGroupsOptions_ANullProtocolType_IsRejectedBySetter()
    {
        ArgumentException failure = Assert.Throws<ArgumentException>(
            () => new ListGroupsOptions { ProtocolTypes = new[] { "consumer", null! } });

        Assert.Equal("value", failure.ParamName);
        Assert.StartsWith("A protocol type must not be null", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ListGroups_OnAnEmptyMock_CompletesEveryFutureEmpty()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        var listed = admin.ListGroups(ListGroupsOptions.ForConsumerGroups());

        Assert.Empty(listed.All.Get(s_deadline));
        Assert.Empty(listed.Valid.Get(s_deadline));
        Assert.Empty(listed.Errors.Get(s_deadline));
        listed.Result.Dispose();
    }

    [Fact]
    public void DescribeGroupsOptions_Defaults_SendNoFlagAndAnUnsetTimeout()
    {
        Assert.Empty(new DescribeConsumerGroupsOptions().Setters.InOrder);
        Assert.Empty(new DescribeClassicGroupsOptions().Setters.InOrder);

        foreach (DescribeConsumerGroupsOptions? options in new[] { null, new DescribeConsumerGroupsOptions() })
        {
            Assert.Equal((-1, false), ReadBack(options));
        }

        foreach (DescribeClassicGroupsOptions? options in new[] { null, new DescribeClassicGroupsOptions() })
        {
            Assert.Equal((-1, false), ReadBack(options));
        }
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void DescribeGroupsOptions_Timeout_IsSentAsTheCoreReadsIt(int? timeoutMs, int expected)
    {
        DescribeConsumerGroupsOptions consumer = new DescribeConsumerGroupsOptions { TimeoutMs = timeoutMs };
        DescribeClassicGroupsOptions classic = new DescribeClassicGroupsOptions { TimeoutMs = timeoutMs };

        Assert.Equal(new[] { nameof(DescribeConsumerGroupsOptions.TimeoutMs) }, consumer.Setters.InOrder);
        Assert.Equal(new[] { nameof(DescribeClassicGroupsOptions.TimeoutMs) }, classic.Setters.InOrder);
        Assert.Equal((expected, false), ReadBack(consumer));
        Assert.Equal((expected, false), ReadBack(classic));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DescribeGroupsOptions_IncludeAuthorizedOperations_CrossesInBothStates(bool include)
    {
        DescribeConsumerGroupsOptions consumer = new DescribeConsumerGroupsOptions { IncludeAuthorizedOperations = include };
        DescribeClassicGroupsOptions classic = new DescribeClassicGroupsOptions { IncludeAuthorizedOperations = include };

        Assert.Equal(
            new[] { nameof(DescribeConsumerGroupsOptions.IncludeAuthorizedOperations) }, consumer.Setters.InOrder);
        Assert.Equal((-1, include), ReadBack(consumer));
        Assert.Equal((-1, include), ReadBack(classic));
    }

    [Fact]
    public void DescribeGroupsOptions_EachSetterRunTwice_SendsItsLastValue()
    {
        DescribeConsumerGroupsOptions consumer = new DescribeConsumerGroupsOptions
        {
            IncludeAuthorizedOperations = true,
            TimeoutMs = 5,
        };
        consumer.IncludeAuthorizedOperations = false;
        consumer.TimeoutMs = 7;
        DescribeClassicGroupsOptions classic = new DescribeClassicGroupsOptions
        {
            TimeoutMs = 5,
            IncludeAuthorizedOperations = false,
        };
        classic.TimeoutMs = 9;
        classic.IncludeAuthorizedOperations = true;

        Assert.Equal(
            new[]
            {
                nameof(DescribeConsumerGroupsOptions.IncludeAuthorizedOperations),
                nameof(DescribeConsumerGroupsOptions.TimeoutMs),
            },
            consumer.Setters.InOrder);
        Assert.Equal((7, false), ReadBack(consumer));
        Assert.Equal((9, true), ReadBack(classic));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DescribeGroups_BadGroupIds_AreRejectedBeforeTheCall(bool classic)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Action<IReadOnlyCollection<string>> describe = classic
            ? groupIds => admin.DescribeClassicGroups(groupIds, null)
            : groupIds => admin.DescribeConsumerGroups(groupIds, null);

        Assert.Equal("groupIds", Assert.Throws<ArgumentNullException>(() => describe(null!)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(() => describe(new[] { "g", null! }));
        Assert.Equal("groupIds", nullElement.ParamName);
        Assert.StartsWith("The group ids must not contain a null element.", nullElement.Message, StringComparison.Ordinal);

        ArgumentException nul = Assert.Throws<ArgumentException>(() => describe(new[] { "g\0" }));
        Assert.Equal("groupIds", nul.ParamName);
        Assert.StartsWith(AdminStrings.InvalidStringMessage, nul.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeConsumerGroups_DuplicateIds_CollapseOrdinally_AndEachKeyFailsAsTheMockDoes()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        var described = admin.DescribeConsumerGroups(
            new[] { "group", "GROUP", "group", "grüppe" }, new DescribeConsumerGroupsOptions { TimeoutMs = 100 });

        Assert.Equal(
            new[] { "GROUP", "group", "grüppe" },
            described.DescribedGroups.Keys.OrderBy(key => key, StringComparer.Ordinal));
        foreach (KafkaFutureImpl<ConsumerGroupDescription> future in described.DescribedGroups.Values)
        {
            AssertNotImplemented(Record.Exception(() => future.Get(s_deadline)));
        }

        described.Result.Dispose();
    }

    [Fact]
    public void DescribeClassicGroups_DuplicateIds_CollapseOrdinally_AndEachKeyFailsAsTheMockDoes()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        var described = admin.DescribeClassicGroups(
            new[] { "group", "GROUP", "group" }, new DescribeClassicGroupsOptions { IncludeAuthorizedOperations = true });

        Assert.Equal(
            new[] { "GROUP", "group" }, described.DescribedGroups.Keys.OrderBy(key => key, StringComparer.Ordinal));
        foreach (KafkaFutureImpl<ClassicGroupDescription> future in described.DescribedGroups.Values)
        {
            AssertNotImplemented(Record.Exception(() => future.Get(s_deadline)));
        }

        described.Result.Dispose();
    }

    [Fact]
    public void DescribeGroups_NoGroupIds_YieldAnEmptyResult()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        var consumer = admin.DescribeConsumerGroups(Array.Empty<string>(), null);
        Assert.Empty(consumer.DescribedGroups);
        consumer.Result.Dispose();

        var classic = admin.DescribeClassicGroups(Array.Empty<string>(), null);
        Assert.Empty(classic.DescribedGroups);
        classic.Result.Dispose();
    }

    private static TheoryData<T> Values<T>()
        where T : struct, Enum
    {
        TheoryData<T> data = new TheoryData<T>();
        foreach (T value in Enum.GetValues(typeof(T)).Cast<T>())
        {
            data.Add(value);
        }

        return data;
    }

    private static void AssertNotImplemented(Exception? outcome)
    {
        NotSupportedException failure = Assert.IsType<NotSupportedException>(outcome);
        Assert.Equal("Not implemented yet", failure.Message);
    }

    private static ListGroupsReadBack ReadBack(ListGroupsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListGroupsOptions(options);
        try
        {
            return new ListGroupsReadBack(
                ReadInt32(native, Native.ListGroupsOptionsTimeoutMs),
                ReadList(native, Native.ListGroupsOptionsGroupStates, element => GroupMarshal.StateFrom(element)!.Value),
                ReadList(native, Native.ListGroupsOptionsProtocolTypes, element => Utf8Marshal.PtrToString(element)!),
                ReadList(native, Native.ListGroupsOptionsTypes, element => GroupMarshal.TypeFrom(element)!.Value));
        }
        finally
        {
            NativeAdminClient.FreeListGroupsOptions(native);
        }
    }

    private static (int TimeoutMs, bool IncludeAuthorizedOperations) ReadBack(DescribeConsumerGroupsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeConsumerGroupsOptions(options);
        try
        {
            return (
                ReadInt32(native, Native.DescribeConsumerGroupsOptionsTimeoutMs),
                ReadFlag(native, Native.DescribeConsumerGroupsOptionsIncludeAuthorizedOperations));
        }
        finally
        {
            NativeAdminClient.FreeDescribeConsumerGroupsOptions(native);
        }
    }

    private static (int TimeoutMs, bool IncludeAuthorizedOperations) ReadBack(DescribeClassicGroupsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeClassicGroupsOptions(options);
        try
        {
            return (
                ReadInt32(native, Native.DescribeClassicGroupsOptionsTimeoutMs),
                ReadFlag(native, Native.DescribeClassicGroupsOptionsIncludeAuthorizedOperations));
        }
        finally
        {
            NativeAdminClient.FreeDescribeClassicGroupsOptions(native);
        }
    }

    private static int ReadInt32(IntPtr self, Native.Int32Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        int value = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static bool ReadFlag(IntPtr self, Native.FlagGetter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        sbyte value = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value != 0;
    }

    private static IReadOnlyList<T> ReadList<T>(IntPtr self, Native.PointerGetter getter, Func<IntPtr, T> element) =>
        ReadOptionalList(self, getter, element) ?? throw new InvalidOperationException("The getter returned no list.");

    private static IReadOnlyList<T>? ReadOptionalList<T>(IntPtr self, Native.PointerGetter getter, Func<IntPtr, T> element)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (list == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            int size = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            T[] values = new T[size];
            for (int i = 0; i < size; i++)
            {
                IntPtr item = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                values[i] = element(item);
            }

            return values;
        }
        finally
        {
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    /// <summary>The four fields of a C <c>ListGroupsOptions_t</c>, read back through its getters.</summary>
    private sealed class ListGroupsReadBack
    {
        internal ListGroupsReadBack(
            int timeoutMs,
            IReadOnlyList<GroupState> groupStates,
            IReadOnlyList<string> protocolTypes,
            IReadOnlyList<GroupType> types)
        {
            TimeoutMs = timeoutMs;
            GroupStates = groupStates;
            ProtocolTypes = protocolTypes;
            Types = types;
        }

        internal int TimeoutMs { get; }

        internal IReadOnlyList<GroupState> GroupStates { get; }

        internal IReadOnlyList<string> ProtocolTypes { get; }

        internal IReadOnlyList<GroupType> Types { get; }
    }

    /// <summary>The header's group Options and request getters, which the binding itself never calls.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal delegate IntPtr PointerGetter(IntPtr self, ref IntPtr outErr);

        internal delegate int Int32Getter(IntPtr self, ref IntPtr outErr);

        internal delegate sbyte FlagGetter(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListGroupsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_group_states_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListGroupsOptionsGroupStates(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_protocol_types_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListGroupsOptionsProtocolTypes(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_types_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListGroupsOptionsTypes(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeConsumerGroupsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsOptions_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeConsumerGroupsOptionsIncludeAuthorizedOperations(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeClassicGroupsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsOptions_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeClassicGroupsOptionsIncludeAuthorizedOperations(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListConsumerGroupOffsetsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsOptions_require_stable_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte ListConsumerGroupOffsetsOptionsRequireStable(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListConsumerGroupOffsetsSpecTopicPartitions(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterConsumerGroupOffsetsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DeleteConsumerGroupOffsetsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DeleteConsumerGroupsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_members_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr RemoveMembersOptionsMembers(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_reason_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr RemoveMembersOptionsReason(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_remove_all_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte RemoveMembersOptionsRemoveAll(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int RemoveMembersOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_MemberToRemove_group_instance_id_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MemberToRemoveGroupInstanceId(IntPtr self, ref IntPtr outErr);
    }

    [Fact]
    public void GroupOffsetsOptions_Defaults_SendAnUnsetTimeout()
    {
        Assert.Empty(new ListConsumerGroupOffsetsOptions().Setters.InOrder);
        Assert.Empty(new AlterConsumerGroupOffsetsOptions().Setters.InOrder);
        Assert.Empty(new DeleteConsumerGroupOffsetsOptions().Setters.InOrder);
        Assert.Empty(new DeleteConsumerGroupsOptions().Setters.InOrder);

        foreach (ListConsumerGroupOffsetsOptions? options in new[] { null, new ListConsumerGroupOffsetsOptions() })
        {
            Assert.Equal((-1, false), ReadBack(options));
        }

        foreach (AlterConsumerGroupOffsetsOptions? options in new[] { null, new AlterConsumerGroupOffsetsOptions() })
        {
            Assert.Equal(-1, ReadBack(options));
        }

        foreach (DeleteConsumerGroupOffsetsOptions? options in new[] { null, new DeleteConsumerGroupOffsetsOptions() })
        {
            Assert.Equal(-1, ReadBack(options));
        }

        foreach (DeleteConsumerGroupsOptions? options in new[] { null, new DeleteConsumerGroupsOptions() })
        {
            Assert.Equal(-1, ReadBack(options));
        }
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void GroupOffsetsOptions_Timeout_IsSentAsTheCoreReadsIt(int? timeoutMs, int expected)
    {
        ListConsumerGroupOffsetsOptions list = new ListConsumerGroupOffsetsOptions { TimeoutMs = timeoutMs };
        AlterConsumerGroupOffsetsOptions alter = new AlterConsumerGroupOffsetsOptions { TimeoutMs = timeoutMs };
        DeleteConsumerGroupOffsetsOptions deleteOffsets = new DeleteConsumerGroupOffsetsOptions { TimeoutMs = timeoutMs };
        DeleteConsumerGroupsOptions deleteGroups = new DeleteConsumerGroupsOptions { TimeoutMs = timeoutMs };
        RemoveMembersFromConsumerGroupOptions remove = new RemoveMembersFromConsumerGroupOptions { TimeoutMs = timeoutMs };

        Assert.Equal(new[] { nameof(ListConsumerGroupOffsetsOptions.TimeoutMs) }, list.Setters.InOrder);
        Assert.Equal(new[] { nameof(RemoveMembersFromConsumerGroupOptions.TimeoutMs) }, remove.Setters.InOrder);
        Assert.Equal((expected, false), ReadBack(list));
        Assert.Equal(expected, ReadBack(alter));
        Assert.Equal(expected, ReadBack(deleteOffsets));
        Assert.Equal(expected, ReadBack(deleteGroups));
        Assert.Equal(expected, ReadBack(remove).TimeoutMs);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ListConsumerGroupOffsetsOptions_RequireStable_CrossesInBothStates(bool requireStable)
    {
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions { RequireStable = requireStable };

        Assert.Equal(new[] { nameof(ListConsumerGroupOffsetsOptions.RequireStable) }, options.Setters.InOrder);
        Assert.Equal((-1, requireStable), ReadBack(options));
    }

    [Fact]
    public void GroupOffsetsOptions_EachSetterRunTwice_SendsItsLastValue()
    {
        ListConsumerGroupOffsetsOptions list = new ListConsumerGroupOffsetsOptions { TimeoutMs = 5, RequireStable = true };
        list.TimeoutMs = 7;
        list.RequireStable = false;
        Assert.Equal(
            new[] { nameof(ListConsumerGroupOffsetsOptions.TimeoutMs), nameof(ListConsumerGroupOffsetsOptions.RequireStable) },
            list.Setters.InOrder);
        Assert.Equal((7, false), ReadBack(list));

        AlterConsumerGroupOffsetsOptions alter = new AlterConsumerGroupOffsetsOptions { TimeoutMs = 5 };
        alter.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(alter));

        DeleteConsumerGroupOffsetsOptions deleteOffsets = new DeleteConsumerGroupOffsetsOptions { TimeoutMs = 5 };
        deleteOffsets.TimeoutMs = 8;
        Assert.Equal(8, ReadBack(deleteOffsets));

        DeleteConsumerGroupsOptions deleteGroups = new DeleteConsumerGroupsOptions { TimeoutMs = 5 };
        deleteGroups.TimeoutMs = 9;
        Assert.Equal(9, ReadBack(deleteGroups));

        RemoveMembersFromConsumerGroupOptions remove = new RemoveMembersFromConsumerGroupOptions { Reason = "first", TimeoutMs = 5 };
        remove.Reason = null;
        remove.TimeoutMs = 6;
        Assert.Equal(
            new[] { nameof(RemoveMembersFromConsumerGroupOptions.Reason), nameof(RemoveMembersFromConsumerGroupOptions.TimeoutMs) },
            remove.Setters.InOrder);
        RemoveMembersReadBack sent = ReadBack(remove);
        Assert.Equal(string.Empty, sent.Reason);
        Assert.Equal(6, sent.TimeoutMs);
    }

    [Fact]
    public void RemoveMembersOptions_TheNoArgumentConstructor_SendsRemoveAll()
    {
        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();
        Assert.Empty(options.Setters.InOrder);

        RemoveMembersReadBack sent = ReadBack(options);
        Assert.True(sent.RemoveAll);
        Assert.Empty(sent.Members);
        Assert.Null(sent.Reason);
        Assert.Equal(-1, sent.TimeoutMs);
    }

    [Fact]
    public void RemoveMembersOptions_TheMembers_CrossAtCreation_ARepeatCollapsed()
    {
        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(
            new[] { new MemberToRemove("instance-1"), new MemberToRemove("instance-é"), new MemberToRemove("instance-1") })
        {
            Reason = "rebalance",
        };

        RemoveMembersReadBack sent = ReadBack(options);
        Assert.False(sent.RemoveAll);
        Assert.Equal(new[] { "instance-1", "instance-é" }, sent.Members.OrderBy(id => id, StringComparer.Ordinal));
        Assert.Equal("rebalance", sent.Reason);
        Assert.Equal(-1, sent.TimeoutMs);
    }

    [Fact]
    public void ListConsumerGroupOffsetsSpec_AllPartitionsAndAnEmptySelection_CrossDifferently()
    {
        Assert.Null(ReadBack(new ListConsumerGroupOffsetsSpec()));
        Assert.Empty(ReadBack(new ListConsumerGroupOffsetsSpec { TopicPartitions = Array.Empty<TopicPartition>() })!);

        TopicPartition[] selected =
        {
            new TopicPartition("alpha", 0),
            new TopicPartition("beta-é", 11),
            new TopicPartition("alpha", 2),
        };
        Assert.Equal(selected, ReadBack(new ListConsumerGroupOffsetsSpec { TopicPartitions = selected }));
    }

    [Fact]
    public void ListConsumerGroupOffsets_BadArguments_AreRejectedBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException nullGroupId = Assert.Throws<ArgumentNullException>(
            () => admin.ListConsumerGroupOffsets((string)null!, null));
        Assert.Equal("groupId", nullGroupId.ParamName);

        ArgumentNullException missing = Assert.Throws<ArgumentNullException>(
            () => admin.ListConsumerGroupOffsets((IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec>)null!, null));
        Assert.Equal("groupSpecs", missing.ParamName);

        AssertRejected(
            admin,
            new GroupSpecEntries(new KeyValuePair<string, ListConsumerGroupOffsetsSpec>(null!, new ListConsumerGroupOffsetsSpec())),
            "The group specs must not contain a null group id.");
        AssertRejected(
            admin,
            new GroupSpecEntries(
                new KeyValuePair<string, ListConsumerGroupOffsetsSpec>("g1", new ListConsumerGroupOffsetsSpec()),
                new KeyValuePair<string, ListConsumerGroupOffsetsSpec>("g1", new ListConsumerGroupOffsetsSpec())),
            "The group specs must not contain the group id 'g1' more than once.");
        AssertRejected(
            admin,
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal) { ["g1"] = null! },
            "The spec for group id 'g1' must not be null.");
        AssertRejected(
            admin,
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["g1"] = new ListConsumerGroupOffsetsSpec { TopicPartitions = new[] { default(TopicPartition) } },
            },
            "The spec for group id 'g1' must not select a topic partition with a null topic.");
    }

    private static void AssertRejected(
        NativeAdminClient admin, IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs, string message)
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.ListConsumerGroupOffsets(groupSpecs, null));
        Assert.Equal("groupSpecs", rejected.ParamName);
        Assert.StartsWith(message, rejected.Message, StringComparison.Ordinal);
    }

    private static (int TimeoutMs, bool RequireStable) ReadBack(ListConsumerGroupOffsetsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListConsumerGroupOffsetsOptions(options);
        try
        {
            return (
                ReadInt32(native, Native.ListConsumerGroupOffsetsOptionsTimeoutMs),
                ReadFlag(native, Native.ListConsumerGroupOffsetsOptionsRequireStable));
        }
        finally
        {
            NativeAdminClient.FreeListConsumerGroupOffsetsOptions(native);
        }
    }

    private static int ReadBack(AlterConsumerGroupOffsetsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterConsumerGroupOffsetsOptions(options);
        try
        {
            return ReadInt32(native, Native.AlterConsumerGroupOffsetsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeAlterConsumerGroupOffsetsOptions(native);
        }
    }

    private static int ReadBack(DeleteConsumerGroupOffsetsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDeleteConsumerGroupOffsetsOptions(options);
        try
        {
            return ReadInt32(native, Native.DeleteConsumerGroupOffsetsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeDeleteConsumerGroupOffsetsOptions(native);
        }
    }

    private static int ReadBack(DeleteConsumerGroupsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDeleteConsumerGroupsOptions(options);
        try
        {
            return ReadInt32(native, Native.DeleteConsumerGroupsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeDeleteConsumerGroupsOptions(native);
        }
    }

    private static RemoveMembersReadBack ReadBack(RemoveMembersFromConsumerGroupOptions options)
    {
        IntPtr native = NativeAdminClient.NewRemoveMembersFromConsumerGroupOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr reason = Native.RemoveMembersOptionsReason(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return new RemoveMembersReadBack(
                ReadList(native, Native.RemoveMembersOptionsMembers, ReadGroupInstanceId),
                Utf8Marshal.PtrToString(reason),
                ReadFlag(native, Native.RemoveMembersOptionsRemoveAll),
                ReadInt32(native, Native.RemoveMembersOptionsTimeoutMs));
        }
        finally
        {
            NativeAdminClient.FreeRemoveMembersFromConsumerGroupOptions(native);
        }
    }

    private static IReadOnlyList<TopicPartition>? ReadBack(ListConsumerGroupOffsetsSpec spec)
    {
        IntPtr native = NativeAdminClient.NewNativeGroupSpec(spec);
        try
        {
            return ReadOptionalList(native, Native.ListConsumerGroupOffsetsSpecTopicPartitions, OffsetMapMarshalShared.CopyKey);
        }
        finally
        {
            NativeAdminClient.DestroyGroupSpec(native);
        }
    }

    private static string ReadGroupInstanceId(IntPtr member)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr id = Native.MemberToRemoveGroupInstanceId(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return Utf8Marshal.PtrToString(id)!;
    }

    /// <summary>The four fields of a C <c>RemoveMembersFromConsumerGroupOptions_t</c>, read back through its getters.</summary>
    private sealed class RemoveMembersReadBack
    {
        internal RemoveMembersReadBack(IReadOnlyList<string> members, string? reason, bool removeAll, int timeoutMs)
        {
            Members = members;
            Reason = reason;
            RemoveAll = removeAll;
            TimeoutMs = timeoutMs;
        }

        internal IReadOnlyList<string> Members { get; }

        internal string? Reason { get; }

        internal bool RemoveAll { get; }

        internal int TimeoutMs { get; }
    }

    /// <summary>
    /// A <see cref="IReadOnlyDictionary{TKey, TValue}"/> that can yield what a real
    /// dictionary cannot — a null key, or the same key twice — so the preconditions guarding
    /// against a hostile implementation of the parameter type are reachable from a test.
    /// </summary>
    private sealed class GroupSpecEntries : IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec>
    {
        private readonly KeyValuePair<string, ListConsumerGroupOffsetsSpec>[] _entries;

        internal GroupSpecEntries(params KeyValuePair<string, ListConsumerGroupOffsetsSpec>[] entries)
        {
            _entries = entries;
        }

        public int Count => _entries.Length;

        public IEnumerable<string> Keys => _entries.Select(entry => entry.Key);

        public IEnumerable<ListConsumerGroupOffsetsSpec> Values =>
            _entries.Select(entry => entry.Value);

        public ListConsumerGroupOffsetsSpec this[string key] => throw new NotSupportedException();

        public bool ContainsKey(string key) => throw new NotSupportedException();

        public bool TryGetValue(string key, out ListConsumerGroupOffsetsSpec value) =>
            throw new NotSupportedException();

        public IEnumerator<KeyValuePair<string, ListConsumerGroupOffsetsSpec>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, ListConsumerGroupOffsetsSpec>>)_entries).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _entries.GetEnumerator();
    }
}
