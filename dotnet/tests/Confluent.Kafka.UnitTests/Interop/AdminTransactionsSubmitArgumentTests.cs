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
using System.Linq;
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Reads the C options and the abort spec that DescribeProducers, DescribeTransactions, AbortTransaction,
/// FenceProducers, ForceTerminateTransaction and ListTransactions build back through the header's getters.
/// </summary>
public sealed class AdminTransactionsSubmitArgumentTests
{
    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, -1), ReadBack((DescribeProducersOptions?)null));
        Assert.Equal((-1, -1), ReadBack(new DescribeProducersOptions()));
        Assert.Empty(new DescribeProducersOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((DescribeTransactionsOptions?)null));
        Assert.Equal(-1, ReadBack(new DescribeTransactionsOptions()));
        Assert.Empty(new DescribeTransactionsOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((AbortTransactionOptions?)null));
        Assert.Equal(-1, ReadBack(new AbortTransactionOptions()));
        Assert.Empty(new AbortTransactionOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((FenceProducersOptions?)null));
        Assert.Equal(-1, ReadBack(new FenceProducersOptions()));
        Assert.Empty(new FenceProducersOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((TerminateTransactionOptions?)null));
        Assert.Equal(-1, ReadBack(new TerminateTransactionOptions()));
        Assert.Empty(new TerminateTransactionOptions().Setters.InOrder);
    }

    [Fact]
    public void TheTimeoutSetter_ReachesTheCOptions_AndItsLastValueWins()
    {
        Assert.Equal((4321, -1), ReadBack(new DescribeProducersOptions { TimeoutMs = 4321 }));
        Assert.Equal((0, -1), ReadBack(new DescribeProducersOptions { TimeoutMs = 0 }));
        Assert.Equal((0, -1), ReadBack(new DescribeProducersOptions { TimeoutMs = -5 }));
        DescribeProducersOptions producers = new DescribeProducersOptions { TimeoutMs = 10 };
        producers.TimeoutMs = 20;
        Assert.Equal((20, -1), ReadBack(producers));
        producers.TimeoutMs = null;
        Assert.Equal((-1, -1), ReadBack(producers));

        Assert.Equal(4321, ReadBack(new DescribeTransactionsOptions { TimeoutMs = 4321 }));
        Assert.Equal(0, ReadBack(new DescribeTransactionsOptions { TimeoutMs = 0 }));
        Assert.Equal(0, ReadBack(new DescribeTransactionsOptions { TimeoutMs = -5 }));
        DescribeTransactionsOptions transactions = new DescribeTransactionsOptions { TimeoutMs = 10 };
        transactions.TimeoutMs = 30;
        Assert.Equal(30, ReadBack(transactions));
        transactions.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(transactions));

        Assert.Equal(4321, ReadBack(new AbortTransactionOptions { TimeoutMs = 4321 }));
        Assert.Equal(0, ReadBack(new AbortTransactionOptions { TimeoutMs = 0 }));
        Assert.Equal(0, ReadBack(new AbortTransactionOptions { TimeoutMs = -5 }));
        AbortTransactionOptions abort = new AbortTransactionOptions { TimeoutMs = 10 };
        abort.TimeoutMs = 40;
        Assert.Equal(40, ReadBack(abort));
        abort.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(abort));

        Assert.Equal(4321, ReadBack(new FenceProducersOptions { TimeoutMs = 4321 }));
        Assert.Equal(0, ReadBack(new FenceProducersOptions { TimeoutMs = 0 }));
        Assert.Equal(0, ReadBack(new FenceProducersOptions { TimeoutMs = -5 }));
        FenceProducersOptions fence = new FenceProducersOptions { TimeoutMs = 10 };
        fence.TimeoutMs = 50;
        Assert.Equal(50, ReadBack(fence));
        Assert.Equal(new[] { nameof(FenceProducersOptions.TimeoutMs) }, fence.Setters.InOrder);
        fence.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(fence));

        Assert.Equal(4321, ReadBack(new TerminateTransactionOptions { TimeoutMs = 4321 }));
        Assert.Equal(0, ReadBack(new TerminateTransactionOptions { TimeoutMs = 0 }));
        Assert.Equal(0, ReadBack(new TerminateTransactionOptions { TimeoutMs = -5 }));
        TerminateTransactionOptions terminate = new TerminateTransactionOptions { TimeoutMs = 10 };
        terminate.TimeoutMs = 60;
        Assert.Equal(60, ReadBack(terminate));
        Assert.Equal(new[] { nameof(TerminateTransactionOptions.TimeoutMs) }, terminate.Setters.InOrder);
        terminate.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(terminate));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    [InlineData(int.MaxValue)]
    public void TheBrokerIdSetter_ReachesTheCOptions(int brokerId)
    {
        DescribeProducersOptions options = new DescribeProducersOptions { BrokerId = brokerId };

        Assert.Equal((-1, brokerId), ReadBack(options));
        Assert.Equal(new[] { nameof(DescribeProducersOptions.BrokerId) }, options.Setters.InOrder);
    }

    [Fact]
    public void ANullBrokerId_Throws_AndLeavesTheFieldUnset()
    {
        DescribeProducersOptions unset = new DescribeProducersOptions();

        Assert.Equal("value", Assert.Throws<ArgumentNullException>(() => unset.BrokerId = null).ParamName);
        Assert.Null(unset.BrokerId);
        Assert.Empty(unset.Setters.InOrder);
        Assert.Equal((-1, -1), ReadBack(unset));

        DescribeProducersOptions set = new DescribeProducersOptions { BrokerId = 3 };
        Assert.Throws<ArgumentNullException>(() => set.BrokerId = null);
        Assert.Equal(3, set.BrokerId);
        Assert.Equal(new[] { nameof(DescribeProducersOptions.BrokerId) }, set.Setters.InOrder);
        Assert.Equal((-1, 3), ReadBack(set));
    }

    [Fact]
    public void TheSetters_AreReplayedInTheOrderTheyLastRan()
    {
        DescribeProducersOptions options = new DescribeProducersOptions { TimeoutMs = 10, BrokerId = 1 };
        options.TimeoutMs = 25;

        Assert.Equal(
            new[] { nameof(DescribeProducersOptions.BrokerId), nameof(DescribeProducersOptions.TimeoutMs) },
            options.Setters.InOrder);
        Assert.Equal((25, 1), ReadBack(options));
    }

    [Theory]
    [InlineData("txn-topic", 3, 91_234L, (short)7, 42)]
    [InlineData("txn-été", 0, 0L, (short)0, 0)]
    [InlineData("t", int.MaxValue, long.MaxValue, short.MaxValue, int.MaxValue)]
    [InlineData("t", 0, -1L, (short)-1, -1)]
    public void TheAbortSpec_CarriesEveryField(
        string topic, int partition, long producerId, short producerEpoch, int coordinatorEpoch)
    {
        IntPtr native = NativeAdminClient.NewAbortTransactionSpec(
            new AbortTransactionSpec(new TopicPartition(topic, partition), producerId, producerEpoch, coordinatorEpoch));
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr nativePartition = Native.AbortTransactionSpecTopicPartition(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string? readTopic = Utf8Marshal.PtrToString(NativeMethods.TopicPartitionTopic(nativePartition, ref outErr));
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int readPartition = NativeMethods.TopicPartitionPartition(nativePartition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            long readProducerId = Native.AbortTransactionSpecProducerId(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            short readProducerEpoch = Native.AbortTransactionSpecProducerEpoch(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int readCoordinatorEpoch = Native.AbortTransactionSpecCoordinatorEpoch(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            Assert.Equal(topic, readTopic);
            Assert.Equal(partition, readPartition);
            Assert.Equal(producerId, readProducerId);
            Assert.Equal(producerEpoch, readProducerEpoch);
            Assert.Equal(coordinatorEpoch, readCoordinatorEpoch);
        }
        finally
        {
            NativeAdminClient.DestroyAbortTransactionSpec(native);
        }
    }

    [Fact]
    public void ListTransactions_NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        foreach (ListTransactionsOptions? options in new[] { null, new ListTransactionsOptions() })
        {
            ListTransactionsReadBack read = ReadBack(options);

            Assert.Equal(-1, read.TimeoutMs);
            Assert.Empty(read.States);
            Assert.Empty(read.ProducerIds);
            Assert.Equal(-1L, read.DurationMs);
            Assert.Null(read.Pattern);
        }

        Assert.Empty(new ListTransactionsOptions().Setters.InOrder);
    }

    [Fact]
    public void ListTransactions_EveryFilterReachesTheCOptions()
    {
        ListTransactionsReadBack read = ReadBack(
            new ListTransactionsOptions
            {
                FilteredStates = new[] { TransactionState.PrepareAbort, TransactionState.Ongoing },
                FilteredProducerIds = new[] { 33L, 11L, 22L },
                FilteredDuration = 60_000L,
                FilteredTransactionalIdPattern = "txn-.*",
                TimeoutMs = 400,
            });

        Assert.Equal(new[] { TransactionState.Ongoing, TransactionState.PrepareAbort }, read.States.OrderBy(state => state));
        Assert.Equal(new[] { 11L, 22L, 33L }, read.ProducerIds.OrderBy(id => id));
        Assert.Equal(60_000L, read.DurationMs);
        Assert.Equal("txn-.*", read.Pattern);
        Assert.Equal(400, read.TimeoutMs);
    }

    [Fact]
    public void ListTransactions_EveryStateReachesTheCOptions()
    {
        TransactionState[] states = Enum.GetValues(typeof(TransactionState)).Cast<TransactionState>().ToArray();

        Assert.Equal(states, ReadBack(new ListTransactionsOptions { FilteredStates = states }).States.OrderBy(state => state));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void ListTransactions_NegativeTimeout_IsSentAsZero(int timeoutMs) =>
        Assert.Equal(0, ReadBack(new ListTransactionsOptions { TimeoutMs = timeoutMs }).TimeoutMs);

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(-5L)]
    public void ListTransactions_DurationCrossesUnchanged(long duration) =>
        Assert.Equal(duration, ReadBack(new ListTransactionsOptions { FilteredDuration = duration }).DurationMs);

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("txn-ünïcode-🎉.*")]
    public void ListTransactions_ThePatternCrossesUnchanged(string? pattern)
    {
        ListTransactionsOptions options = new ListTransactionsOptions { FilteredTransactionalIdPattern = pattern };

        Assert.Equal(pattern, ReadBack(options).Pattern);
        Assert.Equal(new[] { nameof(ListTransactionsOptions.FilteredTransactionalIdPattern) }, options.Setters.InOrder);
    }

    [Fact]
    public void ListTransactions_TheLastValueOfEachSetterWins()
    {
        ListTransactionsOptions options = new ListTransactionsOptions
        {
            FilteredStates = new[] { TransactionState.Ongoing },
            FilteredProducerIds = new[] { 1L },
            TimeoutMs = 10,
        };
        options.FilteredStates = new[] { TransactionState.Empty };
        options.FilteredProducerIds = Array.Empty<long>();

        ListTransactionsReadBack read = ReadBack(options);

        Assert.Equal(new[] { TransactionState.Empty }, read.States);
        Assert.Empty(read.ProducerIds);
        Assert.Equal(10, read.TimeoutMs);
        Assert.Equal(
            new[]
            {
                nameof(ListTransactionsOptions.TimeoutMs),
                nameof(ListTransactionsOptions.FilteredStates),
                nameof(ListTransactionsOptions.FilteredProducerIds),
            },
            options.Setters.InOrder);
    }

    private static (int TimeoutMs, int BrokerId) ReadBack(DescribeProducersOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeProducersOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeProducersOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int brokerId = Native.DescribeProducersOptionsBrokerId(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, brokerId);
        }
        finally
        {
            NativeAdminClient.FreeDescribeProducersOptions(native);
        }
    }

    private static int ReadBack(DescribeTransactionsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeTransactionsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeTransactionsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeDescribeTransactionsOptions(native);
        }
    }

    private static int ReadBack(AbortTransactionOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAbortTransactionOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.AbortTransactionOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeAbortTransactionOptions(native);
        }
    }

    private static int ReadBack(FenceProducersOptions? options)
    {
        IntPtr native = NativeAdminClient.NewFenceProducersOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.FenceProducersOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeFenceProducersOptions(native);
        }
    }

    private static int ReadBack(TerminateTransactionOptions? options)
    {
        IntPtr native = NativeAdminClient.NewTerminateTransactionOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.TerminateTransactionOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeTerminateTransactionOptions(native);
        }
    }

    private static ListTransactionsReadBack ReadBack(ListTransactionsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListTransactionsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            ListTransactionsReadBack read = new ListTransactionsReadBack
            {
                TimeoutMs = Native.ListTransactionsOptionsTimeoutMs(native, ref outErr),
            };
            KafkaException.ThrowIfFfiFailed(ref outErr);
            read.States = ReadList(
                Native.ListTransactionsOptionsFilteredStates(native, ref outErr),
                TransactionDescriptionMarshal.StateFrom);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            read.ProducerIds = ReadList(
                Native.ListTransactionsOptionsFilteredProducerIds(native, ref outErr),
                element => Marshal.ReadInt64(element));
            KafkaException.ThrowIfFfiFailed(ref outErr);
            read.DurationMs = Native.ListTransactionsOptionsFilteredDuration(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            read.Pattern = Utf8Marshal.PtrToString(
                Native.ListTransactionsOptionsFilteredTransactionalIdPattern(native, ref outErr));
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return read;
        }
        finally
        {
            NativeAdminClient.FreeListTransactionsOptions(native);
        }
    }

    private static T[] ReadList<T>(IntPtr list, Func<IntPtr, T> read)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            T[] values = new T[count];
            for (int i = 0; i < count; i++)
            {
                values[i] = read(NativeMethods.ListGet(list, i, ref outErr));
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return values;
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    /// <summary>The ListTransactions C options, read back through the header's getters.</summary>
    private sealed class ListTransactionsReadBack
    {
        internal int TimeoutMs { get; set; }

        internal TransactionState[] States { get; set; } = Array.Empty<TransactionState>();

        internal long[] ProducerIds { get; set; } = Array.Empty<long>();

        internal long DurationMs { get; set; }

        internal string? Pattern { get; set; }
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeProducersOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersOptions_broker_id_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeProducersOptionsBrokerId(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTransactionsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeTransactionsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AbortTransactionOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int FenceProducersOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TerminateTransactionOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int TerminateTransactionOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionSpec_topic_partition_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AbortTransactionSpecTopicPartition(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionSpec_producer_id_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern long AbortTransactionSpecProducerId(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionSpec_producer_epoch_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern short AbortTransactionSpecProducerEpoch(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionSpec_coordinator_epoch_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AbortTransactionSpecCoordinatorEpoch(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListTransactionsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filtered_states_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListTransactionsOptionsFilteredStates(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filtered_producer_ids_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListTransactionsOptionsFilteredProducerIds(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filtered_duration_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern long ListTransactionsOptionsFilteredDuration(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filtered_transactional_id_pattern_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListTransactionsOptionsFilteredTransactionalIdPattern(IntPtr self, ref IntPtr outErr);
    }
}
