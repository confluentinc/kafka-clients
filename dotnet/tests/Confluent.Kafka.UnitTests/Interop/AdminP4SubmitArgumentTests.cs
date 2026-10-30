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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Reads the C options and arguments ElectLeaders and the two partition-reassignment RPCs build back through
/// the header's getters, and checks the preconditions their inputs meet before any native call.
/// </summary>
public sealed class AdminP4SubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefault()
    {
        Assert.Equal(-1, ReadBack((ElectLeadersOptions?)null));
        Assert.Equal(-1, ReadBack(new ElectLeadersOptions()));
        Assert.Empty(new ElectLeadersOptions().Setters.InOrder);

        Assert.Equal((-1, true), ReadBack((AlterPartitionReassignmentsOptions?)null));
        Assert.Equal((-1, true), ReadBack(new AlterPartitionReassignmentsOptions()));
        Assert.Empty(new AlterPartitionReassignmentsOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((ListPartitionReassignmentsOptions?)null));
        Assert.Equal(-1, ReadBack(new ListPartitionReassignmentsOptions()));
        Assert.Empty(new ListPartitionReassignmentsOptions().Setters.InOrder);
    }

    [Fact]
    public void TheTimeoutSetter_ReachesTheCOptions_AndItsLastValueWins()
    {
        Assert.Equal(0, ReadBack(new ElectLeadersOptions { TimeoutMs = -5 }));
        ElectLeadersOptions elect = new ElectLeadersOptions { TimeoutMs = 10 };
        elect.TimeoutMs = 20;
        Assert.Equal(20, ReadBack(elect));
        elect.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(elect));

        Assert.Equal((0, true), ReadBack(new AlterPartitionReassignmentsOptions { TimeoutMs = -5 }));
        AlterPartitionReassignmentsOptions alter = new AlterPartitionReassignmentsOptions { TimeoutMs = 10 };
        alter.TimeoutMs = 30;
        Assert.Equal((30, true), ReadBack(alter));
        alter.TimeoutMs = null;
        Assert.Equal((-1, true), ReadBack(alter));

        Assert.Equal(0, ReadBack(new ListPartitionReassignmentsOptions { TimeoutMs = -5 }));
        ListPartitionReassignmentsOptions list = new ListPartitionReassignmentsOptions { TimeoutMs = 0 };
        list.TimeoutMs = 40;
        Assert.Equal(40, ReadBack(list));
        list.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(list));
    }

    [Fact]
    public void AllowReplicationFactorChange_ReachesTheCOptions_BothWays()
    {
        Assert.Equal((-1, false), ReadBack(new AlterPartitionReassignmentsOptions { AllowReplicationFactorChange = false }));

        AlterPartitionReassignmentsOptions options = new AlterPartitionReassignmentsOptions
        {
            AllowReplicationFactorChange = false,
        };
        options.AllowReplicationFactorChange = true;
        Assert.Equal((-1, true), ReadBack(options));

        options.TimeoutMs = 50;
        options.AllowReplicationFactorChange = false;
        Assert.Equal((50, false), ReadBack(options));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(2)]
    [InlineData(int.MaxValue)]
    public void ElectLeaders_AnUndefinedElectionType_IsRejected(int value)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentOutOfRangeException rejected = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ElectLeaders((ElectionType)value, Array.Empty<TopicPartition>(), options: null));

        Assert.Equal("electionType", rejected.ParamName);
    }

    [Theory]
    [InlineData(ElectionType.Preferred, 0)]
    [InlineData(ElectionType.Unclean, 1)]
    public void ElectLeaders_TheElectionType_ReachesTheCore_AsItsJavaValue(ElectionType electionType, int expectedValue)
    {
        IntPtr native = NativeAdminClient.NativeElectionType(electionType);
        IntPtr outErr = IntPtr.Zero;
        sbyte value = Native.ElectionTypeValue(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        Assert.Equal(expectedValue, value);
    }

    [Fact]
    public void ElectLeaders_ANullSelectionIsAllPartitions_AndAnEmptyOneIsNot()
    {
        Assert.Null(SelectionSize(null));
        Assert.Equal(0, SelectionSize(Array.Empty<TopicPartition>()));
        Assert.Equal(
            2,
            SelectionSize(new[] { new TopicPartition("t", 0), new TopicPartition("t", 1), new TopicPartition("t", 0) }));
    }

    [Fact]
    public void NullTopics_AreRejectedWithTheirParameterNames()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "partitions",
            Assert.Throws<ArgumentException>(
                () => admin.ElectLeaders(ElectionType.Preferred, new[] { default(TopicPartition) }, options: null))
                .ParamName);
        Assert.Equal(
            "reassignments",
            Assert.Throws<ArgumentException>(
                () => admin.AlterPartitionReassignments(
                    new Dictionary<TopicPartition, NewPartitionReassignment?> { [default] = null }, options: null))
                .ParamName);
        Assert.Equal(
            "partitions",
            Assert.Throws<ArgumentException>(
                () => admin.ListPartitionReassignments(new[] { default(TopicPartition) }, options: null))
                .ParamName);
    }

    [Fact]
    public void AlterPartitionReassignments_ANullMap_IsRejected()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "reassignments",
            Assert.Throws<ArgumentNullException>(() => admin.AlterPartitionReassignments(null!, options: null)).ParamName);
    }

    [Fact]
    public async Task AlterPartitionReassignments_TwoEqualPartitionsUnderTheCallersComparer_SendTheFirst()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(3);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await SeedTopic(admin, "first-wins-reassignments", partitions: 1);

        TopicPartition first = new TopicPartition("first-wins-reassignments", 0);
        TopicPartition second = new TopicPartition("first-wins-reassignments", 0);
        Dictionary<TopicPartition, NewPartitionReassignment?> request =
            new Dictionary<TopicPartition, NewPartitionReassignment?>(new NeverEqualComparer())
            {
                [first] = new NewPartitionReassignment(new[] { 0, 1 }),
                [second] = new NewPartitionReassignment(new[] { 0, 2 }),
            };
        Assert.Equal(2, request.Count);

        AlterPartitionReassignmentsResult altered =
            AlterPartitionReassignmentsResult.OfClient(admin.AlterPartitionReassignments(request, options: null));
        Assert.Single(altered.Values);
        altered.All().Get(s_deadline);

        PartitionReassignment reassignment = Assert.Single(List(admin, new[] { first })).Value;
        Assert.Equal(new[] { 0 }, reassignment.Replicas);
        Assert.Equal(new[] { 1 }, reassignment.AddingReplicas);
        Assert.Empty(reassignment.RemovingReplicas);
    }

    [Fact]
    public async Task AlterPartitionReassignments_ANullTarget_CancelsThePendingReassignment()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(2);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await SeedTopic(admin, "cancel-reassignments", partitions: 2);
        TopicPartition kept = new TopicPartition("cancel-reassignments", 0);
        TopicPartition cancelled = new TopicPartition("cancel-reassignments", 1);

        AlterPartitionReassignmentsResult.OfClient(admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [kept] = new NewPartitionReassignment(new[] { 1 }),
                [cancelled] = new NewPartitionReassignment(new[] { 1 }),
            },
            options: null)).All().Get(s_deadline);
        AlterPartitionReassignmentsResult.OfClient(admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?> { [cancelled] = null },
            options: null)).All().Get(s_deadline);

        Assert.Equal(new[] { kept }, List(admin, null).Keys);
    }

    [Fact]
    public async Task ListPartitionReassignments_ANullSelectionListsEveryPartition_AndAnEmptyOneNone()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(2);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await SeedTopic(admin, "list-reassignments", partitions: 2);
        TopicPartition zero = new TopicPartition("list-reassignments", 0);
        TopicPartition one = new TopicPartition("list-reassignments", 1);
        AlterPartitionReassignmentsResult.OfClient(admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [zero] = new NewPartitionReassignment(new[] { 1 }),
                [one] = new NewPartitionReassignment(new[] { 1 }),
            },
            options: null)).All().Get(s_deadline);

        Assert.Equal(2, List(admin, null).Count);
        Assert.Empty(List(admin, Array.Empty<TopicPartition>()));
        Assert.Equal(new[] { one }, List(admin, new[] { one, one }).Keys);
    }

    [Fact]
    public void EmptyInputs_YieldAnEmptyResult()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        var altered = admin.AlterPartitionReassignments(new Dictionary<TopicPartition, NewPartitionReassignment?>(), null);
        Assert.Empty(altered.Values);
        altered.Result.Dispose();

        Assert.Empty(List(admin, Array.Empty<TopicPartition>()));
    }

    private static IReadOnlyDictionary<TopicPartition, PartitionReassignment> List(
        NativeAdminClient admin, IReadOnlyCollection<TopicPartition>? partitions)
    {
        var listed = admin.ListPartitionReassignments(partitions, options: null);
        try
        {
            return new KafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(listed.Reassignments)
                .Get(s_deadline);
        }
        finally
        {
            listed.Result.Dispose();
        }
    }

    private static Task SeedTopic(NativeAdminClient admin, string topic, int partitions) =>
        TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(
                admin.CreateTopics(new[] { new NewTopic(topic, partitions, 1) }, null)).All().Get(),
            s_deadline);

    private static int? SelectionSize(IReadOnlyCollection<TopicPartition>? partitions)
    {
        IntPtr list = NativeAdminClient.NewElectionPartitions(partitions, out IntPtr[] natives);
        try
        {
            if (list == IntPtr.Zero)
            {
                return null;
            }

            IntPtr outErr = IntPtr.Zero;
            int size = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return size;
        }
        finally
        {
            NativeAdminClient.FreeElectionPartitions(list, natives);
        }
    }

    private static int ReadBack(ElectLeadersOptions? options)
    {
        IntPtr native = NativeAdminClient.NewElectLeadersOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.ElectLeadersOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeElectLeadersOptions(native);
        }
    }

    private static (int TimeoutMs, bool AllowReplicationFactorChange) ReadBack(
        AlterPartitionReassignmentsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterPartitionReassignmentsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.AlterPartitionReassignmentsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte allow = Native.AlterPartitionReassignmentsOptionsAllowReplicationFactorChange(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, allow != 0);
        }
        finally
        {
            NativeAdminClient.FreeAlterPartitionReassignmentsOptions(native);
        }
    }

    private static int ReadBack(ListPartitionReassignmentsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListPartitionReassignmentsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.ListPartitionReassignmentsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeListPartitionReassignmentsOptions(native);
        }
    }

    /// <summary>Treats no two partitions as equal, so two equal partitions stay two entries.</summary>
    private sealed class NeverEqualComparer : IEqualityComparer<TopicPartition>
    {
        public bool Equals(TopicPartition x, TopicPartition y) => false;

        public int GetHashCode(TopicPartition obj) => 0;
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_common_ElectionType_value_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte ElectionTypeValue(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ElectLeadersOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterPartitionReassignmentsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsOptions_allow_replication_factor_change_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte AlterPartitionReassignmentsOptionsAllowReplicationFactorChange(
            IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListPartitionReassignmentsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);
    }
}
