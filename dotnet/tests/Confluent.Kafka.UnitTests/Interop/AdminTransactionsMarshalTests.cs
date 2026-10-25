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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Reads native producer states and transaction descriptions, built through the header's constructors, with
/// production's <see cref="PartitionProducerStateMarshal"/> and <see cref="TransactionDescriptionMarshal"/>, and
/// checks that what they copy out survives the native value.
/// </summary>
public sealed class AdminTransactionsMarshalTests
{
    private static readonly Dictionary<string, (Func<IntPtr> Singleton, TransactionState Expected)> s_states =
        new Dictionary<string, (Func<IntPtr>, TransactionState)>(StringComparer.Ordinal)
        {
            ["ongoing"] = (() => Singleton(Native.TransactionStateOngoing), TransactionState.Ongoing),
            ["prepare_abort"] = (() => Singleton(Native.TransactionStatePrepareAbort), TransactionState.PrepareAbort),
            ["prepare_commit"] = (() => Singleton(Native.TransactionStatePrepareCommit), TransactionState.PrepareCommit),
            ["complete_abort"] = (() => Singleton(Native.TransactionStateCompleteAbort), TransactionState.CompleteAbort),
            ["complete_commit"] = (() => Singleton(Native.TransactionStateCompleteCommit), TransactionState.CompleteCommit),
            ["empty"] = (() => Singleton(Native.TransactionStateEmpty), TransactionState.Empty),
            ["prepare_epoch_fence"] =
                (() => Singleton(Native.TransactionStatePrepareEpochFence), TransactionState.PrepareEpochFence),
            ["unknown"] = (() => Singleton(Native.TransactionStateUnknown), TransactionState.Unknown),
        };

    private delegate IntPtr SingletonGetter(ref IntPtr outErr);

    public static TheoryData<string> StateLabels()
    {
        TheoryData<string> labels = new TheoryData<string>();
        foreach (string label in s_states.Keys)
        {
            labels.Add(label);
        }

        return labels;
    }

    [Fact]
    public void CopyOutPartitionProducerState_ReadsEveryProducerInOrder_AndSurvivesTheDestroy()
    {
        ProducerState first = new ProducerState(1_234_567_890_123L, 5, 17, 1_700_000_000_000L, 3, 42L);
        ProducerState second = new ProducerState(long.MaxValue, 0, -1, 0L, null, null);
        ProducerState third = new ProducerState(-7L, int.MaxValue, int.MaxValue, long.MinValue, 0, 0L);

        DescribeProducersResult.PartitionProducerState copied =
            CopyOutBuiltProducers(new[] { first, second, third }, buildList: true);

        Assert.Equal(new[] { first, second, third }, copied.ActiveProducers);
        Assert.Null(copied.ActiveProducers[1].CoordinatorEpoch);
        Assert.Null(copied.ActiveProducers[1].CurrentTransactionStartOffset);
        Assert.Equal(0, copied.ActiveProducers[2].CoordinatorEpoch);
        Assert.Equal(0L, copied.ActiveProducers[2].CurrentTransactionStartOffset);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CopyOutPartitionProducerState_AnEmptyOrNullList_HasNoProducers(bool buildList)
    {
        DescribeProducersResult.PartitionProducerState copied =
            CopyOutBuiltProducers(Array.Empty<ProducerState>(), buildList);

        Assert.Empty(copied.ActiveProducers);
    }

    [Fact]
    public void CopyOut_ANullPointer_ThrowsWithTheMarshalsMessage()
    {
        Assert.Equal(
            "The admin result produced no partition producer state.",
            Assert.Throws<KafkaException>(() => PartitionProducerStateMarshal.CopyOut(IntPtr.Zero)).Message);
        Assert.Equal(
            "The admin result produced no producer state.",
            Assert.Throws<KafkaException>(() => PartitionProducerStateMarshal.CopyOutProducer(IntPtr.Zero)).Message);
        Assert.Equal(
            "The admin result produced no transaction description.",
            Assert.Throws<KafkaException>(() => TransactionDescriptionMarshal.CopyOut(IntPtr.Zero)).Message);
        Assert.Equal(
            "The admin result produced no transaction state.",
            Assert.Throws<KafkaException>(() => TransactionDescriptionMarshal.StateFrom(IntPtr.Zero)).Message);
    }

    [Fact]
    public void CopyOutTransactionDescription_ReadsEveryField_AndSurvivesTheDestroy()
    {
        TransactionDescription copied = CopyOutBuiltDescription(
            coordinatorId: 7,
            state: s_states["ongoing"].Singleton(),
            producerId: 1_234_567_890_123L,
            producerEpoch: 5,
            transactionTimeoutMs: 60_000L,
            transactionStartTimeMs: 1_700_000_000_000L,
            partitions: new[] { new TopicPartition("txn-été", 1), new TopicPartition("alpha", 2), new TopicPartition("alpha", 0) });

        Assert.Equal(7, copied.CoordinatorId);
        Assert.Equal(TransactionState.Ongoing, copied.State);
        Assert.Equal(1_234_567_890_123L, copied.ProducerId);
        Assert.Equal(5, copied.ProducerEpoch);
        Assert.Equal(60_000L, copied.TransactionTimeoutMs);
        Assert.Equal(1_700_000_000_000L, copied.TransactionStartTimeMs);
        Assert.Equal(
            new[] { new TopicPartition("alpha", 0), new TopicPartition("alpha", 2), new TopicPartition("txn-été", 1) },
            copied.TopicPartitions.ToArray());
    }

    [Theory]
    [InlineData(-1L, null)]
    [InlineData(0L, 0L)]
    [InlineData(long.MaxValue, long.MaxValue)]
    public void CopyOutTransactionDescription_OnlyANegativeStartTimeIsEmpty(long nativeStartTimeMs, long? expected)
    {
        TransactionDescription copied = CopyOutBuiltDescription(
            0, s_states["empty"].Singleton(), -1L, -1, 0L, nativeStartTimeMs, partitions: null);

        Assert.Equal(expected, copied.TransactionStartTimeMs);
        Assert.Empty(copied.TopicPartitions);
    }

    [Fact]
    public void CopyOutTransactionDescription_AnEmptyPartitionList_IsEmpty()
    {
        TransactionDescription copied = CopyOutBuiltDescription(
            1, s_states["complete_commit"].Singleton(), 2L, 3, 4L, -1L, Array.Empty<TopicPartition>());

        Assert.Empty(copied.TopicPartitions);
        Assert.Null(copied.TransactionStartTimeMs);
    }

    [Theory]
    [MemberData(nameof(StateLabels))]
    public void EveryStateSingleton_MapsToItsState_DirectlyAndThroughADescription(string label)
    {
        (Func<IntPtr> singleton, TransactionState expected) = s_states[label];

        Assert.Equal(expected, TransactionDescriptionMarshal.StateFrom(singleton()));
        Assert.Equal(
            expected,
            CopyOutBuiltDescription(0, singleton(), 0L, 0, 0L, -1L, partitions: null).State);
    }

    [Fact]
    public void EveryStateSingleton_HasADistinctEnumValue()
    {
        Assert.Equal(
            Enum.GetValues(typeof(TransactionState)).Cast<TransactionState>().OrderBy(state => state),
            s_states.Values.Select(entry => TransactionDescriptionMarshal.StateFrom(entry.Singleton())).OrderBy(state => state));
    }

    private static IntPtr Singleton(SingletonGetter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr state = getter(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, state);
        return state;
    }

    private static DescribeProducersResult.PartitionProducerState CopyOutBuiltProducers(
        ProducerState[] producers, bool buildList)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = IntPtr.Zero;
        List<IntPtr> elements = new List<IntPtr>();
        IntPtr state = IntPtr.Zero;
        try
        {
            if (buildList)
            {
                list = NativeMethods.ListNew(ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                foreach (ProducerState producer in producers)
                {
                    elements.Add(Native.ProducerStateNew(
                        producer.ProducerId,
                        producer.ProducerEpoch,
                        producer.LastSequence,
                        producer.LastTimestamp,
                        producer.CoordinatorEpoch ?? -1,
                        producer.CurrentTransactionStartOffset ?? -1L,
                        ref outErr));
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    NativeMethods.ListAdd(list, elements[elements.Count - 1], ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                }
            }

            state = Native.PartitionProducerStateNew(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return PartitionProducerStateMarshal.CopyOut(state);
        }
        finally
        {
            Native.PartitionProducerStateDestroy(state, ref outErr);
            NativeMethods.ListDestroy(list, ref outErr);
            foreach (IntPtr element in elements)
            {
                Native.ProducerStateDestroy(element, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    private static TransactionDescription CopyOutBuiltDescription(
        int coordinatorId,
        IntPtr state,
        long producerId,
        int producerEpoch,
        long transactionTimeoutMs,
        long transactionStartTimeMs,
        TopicPartition[]? partitions)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = IntPtr.Zero;
        List<IntPtr> elements = new List<IntPtr>();
        IntPtr description = IntPtr.Zero;
        try
        {
            if (partitions != null)
            {
                list = NativeMethods.ListNew(ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                foreach (TopicPartition partition in partitions)
                {
                    using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partition.Topic))
                    {
                        elements.Add(NativeMethods.TopicPartitionNew(topic.Pointer, partition.Partition, ref outErr));
                    }

                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    NativeMethods.ListAdd(list, elements[elements.Count - 1], ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                }
            }

            description = Native.TransactionDescriptionNew(
                coordinatorId,
                state,
                producerId,
                producerEpoch,
                transactionTimeoutMs,
                transactionStartTimeMs,
                list,
                ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return TransactionDescriptionMarshal.CopyOut(description);
        }
        finally
        {
            Native.TransactionDescriptionDestroy(description, ref outErr);
            NativeMethods.ListDestroy(list, ref outErr);
            foreach (IntPtr element in elements)
            {
                NativeMethods.TopicPartitionDestroy(element, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ProducerStateNew(
            long producerId,
            int producerEpoch,
            int lastSequence,
            long lastTimestamp,
            int coordinatorEpoch,
            long currentTransactionStartOffset,
            ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ProducerStateDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersResult_PartitionProducerState_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr PartitionProducerStateNew(IntPtr activeProducers, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersResult_PartitionProducerState_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void PartitionProducerStateDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionDescriptionNew(
            int coordinatorId,
            IntPtr state,
            long producerId,
            int producerEpoch,
            long transactionTimeoutMs,
            long transactionStartTimeMs,
            IntPtr topicPartitions,
            ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void TransactionDescriptionDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_ongoing_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStateOngoing(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_prepare_abort_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStatePrepareAbort(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_prepare_commit_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStatePrepareCommit(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_complete_abort_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStateCompleteAbort(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_complete_commit_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStateCompleteCommit(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_empty_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStateEmpty(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_prepare_epoch_fence_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStatePrepareEpochFence(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_unknown_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionStateUnknown(ref IntPtr outErr);
    }
}
