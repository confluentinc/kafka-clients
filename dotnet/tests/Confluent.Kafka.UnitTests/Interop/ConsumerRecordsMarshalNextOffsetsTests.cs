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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="ConsumerRecordsMarshal"/>'s next offsets over native batches built in a fixed partition order: a batch
/// with next offsets and no records, and the next offsets of a batch cut short by a failing deserializer.
/// </summary>
public sealed class ConsumerRecordsMarshalNextOffsetsTests
{
    private const int CoreEpoch = 7;
    private const string CoreMetadata = "core";

    private static readonly TopicPartition s_a0 = new TopicPartition("a", 0);
    private static readonly TopicPartition s_a1 = new TopicPartition("a", 1);
    private static readonly TopicPartition s_b0 = new TopicPartition("b", 0);
    private static readonly TopicPartition s_c0 = new TopicPartition("c", 0);

    [Fact]
    public void ABatchWithNextOffsetsAndNoRecords_GivesThemWithNoRecords_NotEMPTY()
    {
        using NativeBatch batch = new NativeBatch();
        batch.NextOffset(s_a0, 42, CoreEpoch, CoreMetadata);

        ConsumerRecords<string, string> records = batch.CopyOut(
            new FailingDeserializer("bad"), out RecordDeserializationException? error, out _);

        Assert.Null(error);
        Assert.NotSame(ConsumerRecords<string, string>.EMPTY, records);
        Assert.Equal(0, records.Count());
        Assert.True(records.IsEmpty());
        Assert.Empty(records.Partitions());
        Assert.Empty(records);
        OffsetAndMetadata next = Assert.Single(records.NextOffsets()).Value;
        Assert.Equal(s_a0, records.NextOffsets().Keys.Single());
        Assert.Equal(new OffsetAndMetadata(42, CoreEpoch, CoreMetadata), next);
        Assert.Equal(CoreEpoch, next.LeaderEpoch());
        Assert.Equal(CoreMetadata, next.Metadata());
    }

    [Fact]
    public void ABatchWithNeitherRecordsNorNextOffsets_GivesEMPTY()
    {
        using NativeBatch batch = new NativeBatch();

        ConsumerRecords<string, string> records = batch.CopyOut(
            new FailingDeserializer("bad"), out RecordDeserializationException? error, out _);

        Assert.Null(error);
        Assert.Same(ConsumerRecords<string, string>.EMPTY, records);
    }

    [Fact]
    public void ABatchThatDeserializes_KeepsTheCoreNextOffsets()
    {
        using NativeBatch batch = new NativeBatch();
        batch.Partition(s_a0, (0, 3, "a"), (1, 3, "b"));
        batch.Partition(s_b0, (5, 4, "c"));
        batch.NextOffset(s_a0, 100, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_b0, 300, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_c0, 400, CoreEpoch, CoreMetadata);

        ConsumerRecords<string, string> records = batch.CopyOut(
            new FailingDeserializer("bad"), out RecordDeserializationException? error, out _);

        Assert.Null(error);
        Assert.Equal(3, records.Count());
        Assert.Equal(3, records.NextOffsets().Count);
        Assert.Equal(new OffsetAndMetadata(100, CoreEpoch, CoreMetadata), records.NextOffsets()[s_a0]);
        Assert.Equal(new OffsetAndMetadata(300, CoreEpoch, CoreMetadata), records.NextOffsets()[s_b0]);
        Assert.Equal(new OffsetAndMetadata(400, CoreEpoch, CoreMetadata), records.NextOffsets()[s_c0]);
    }

    [Fact]
    public void AFailingDeserializer_GivesItsPartitionTheLastReturnedOffsetPlusOne_AndLaterPartitionsNone()
    {
        using NativeBatch batch = new NativeBatch();
        batch.Partition(s_a0, (0, 2, "a"), (1, 2, "b"));
        batch.Partition(s_a1, (4, 3, "c"), (5, 3, "bad"), (6, 3, "d"));
        batch.Partition(s_b0, (0, 5, "e"));
        batch.NextOffset(s_a0, 100, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_a1, 200, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_b0, 300, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_c0, 400, CoreEpoch, CoreMetadata);

        ConsumerRecords<string, string> records = batch.CopyOut(
            new FailingDeserializer("bad"),
            out RecordDeserializationException? error,
            out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind);

        Assert.NotNull(error);
        Assert.Equal(s_a1, error!.TopicPartition());
        Assert.Equal(5, error.Offset());
        Assert.Equal(
            new[] { new KeyValuePair<TopicPartition, long>(s_a1, 5), new KeyValuePair<TopicPartition, long>(s_b0, 0) },
            rewind);
        Assert.Equal(new[] { s_a0, s_a1 }, records.Partitions());
        Assert.Equal(new[] { "a", "b", "c" }, records.Select(r => r.Value()));

        Assert.Equal(3, records.NextOffsets().Count);
        Assert.Equal(new OffsetAndMetadata(100, CoreEpoch, CoreMetadata), records.NextOffsets()[s_a0]);
        OffsetAndMetadata cut = records.NextOffsets()[s_a1];
        Assert.Equal(5, cut.Offset());
        Assert.Equal(3, cut.LeaderEpoch());
        Assert.Equal("", cut.Metadata());
        Assert.False(records.NextOffsets().ContainsKey(s_b0));
        Assert.Equal(new OffsetAndMetadata(400, CoreEpoch, CoreMetadata), records.NextOffsets()[s_c0]);
    }

    [Fact]
    public void AFailingDeserializer_GivesNoNextOffset_ToItsPartitionWhenNoneOfItsRecordsWasReturned()
    {
        using NativeBatch batch = new NativeBatch();
        batch.Partition(s_a0, (0, 2, "a"), (1, 2, "b"));
        batch.Partition(s_a1, (4, 3, "bad"), (5, 3, "c"));
        batch.Partition(s_b0, (0, 5, "e"));
        batch.NextOffset(s_a0, 100, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_a1, 200, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_b0, 300, CoreEpoch, CoreMetadata);
        batch.NextOffset(s_c0, 400, CoreEpoch, CoreMetadata);

        ConsumerRecords<string, string> records = batch.CopyOut(
            new FailingDeserializer("bad"),
            out RecordDeserializationException? error,
            out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind);

        Assert.Equal(s_a1, error!.TopicPartition());
        Assert.Equal(
            new[] { new KeyValuePair<TopicPartition, long>(s_a1, 4), new KeyValuePair<TopicPartition, long>(s_b0, 0) },
            rewind);
        Assert.Equal(new[] { s_a0 }, records.Partitions());
        Assert.Equal(2, records.Count());

        Assert.Equal(2, records.NextOffsets().Count);
        Assert.Equal(new OffsetAndMetadata(100, CoreEpoch, CoreMetadata), records.NextOffsets()[s_a0]);
        Assert.False(records.NextOffsets().ContainsKey(s_a1));
        Assert.False(records.NextOffsets().ContainsKey(s_b0));
        Assert.Equal(new OffsetAndMetadata(400, CoreEpoch, CoreMetadata), records.NextOffsets()[s_c0]);
    }

    /// <summary>
    /// The inputs of one native batch, built through <c>kafka_consumer_ConsumerRecords_with_next_offsets</c> in the
    /// order they are added; it frees them after the batch, whose records borrow the key and value bytes.
    /// </summary>
    private sealed class NativeBatch : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _strings = new List<Utf8Marshal.PinnedUtf8String>();
        private readonly List<IntPtr> _bytes = new List<IntPtr>();
        private readonly List<IntPtr> _records = new List<IntPtr>();
        private readonly List<IntPtr> _lists = new List<IntPtr>();
        private readonly List<IntPtr> _topicPartitions = new List<IntPtr>();
        private readonly List<IntPtr> _offsets = new List<IntPtr>();
        private readonly IntPtr _recordsMap = Call(NativeMethods.MapNew);
        private readonly IntPtr _nextOffsetsMap = Call(NativeMethods.MapNew);

        internal void Partition(TopicPartition partition, params (long Offset, int LeaderEpoch, string Value)[] records)
        {
            IntPtr list = Call(NativeMethods.ListNew);
            _lists.Add(list);
            foreach ((long offset, int leaderEpoch, string value) in records)
            {
                IntPtr record = Record(partition, offset, leaderEpoch, value);
                IntPtr outErr = IntPtr.Zero;
                NativeMethods.ListAdd(list, record, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            Put(_recordsMap, TopicPartition(partition), list);
        }

        internal void NextOffset(TopicPartition partition, long offset, int leaderEpoch, string metadata)
        {
            IntPtr metadataPointer = String(metadata);
            IntPtr next = Call((ref IntPtr outErr) =>
                NativeMethods.OffsetAndMetadataWithLeaderEpochMetadata(offset, leaderEpoch, metadataPointer, ref outErr));
            _offsets.Add(next);
            Put(_nextOffsetsMap, TopicPartition(partition), next);
        }

        internal ConsumerRecords<string, string> CopyOut(
            IDeserializer<string> valueDeserializer,
            out RecordDeserializationException? error,
            out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind)
        {
            IntPtr records = Call((ref IntPtr outErr) =>
                Native.ConsumerRecordsWithNextOffsets(_recordsMap, _nextOffsetsMap, ref outErr));
            try
            {
                return ConsumerRecordsMarshal.CopyOut(
                    records,
                    Serdes.String().Deserializer(),
                    null,
                    valueDeserializer,
                    null,
                    out error,
                    out rewind);
            }
            finally
            {
                ConsumerRecordsMarshal.Destroy(records);
            }
        }

        public void Dispose()
        {
            foreach (IntPtr record in _records)
            {
                Free(NativeMethods.ConsumerRecordDestroy, record);
            }

            foreach (IntPtr list in _lists)
            {
                Free(NativeMethods.ListDestroy, list);
            }

            foreach (IntPtr next in _offsets)
            {
                Free(NativeMethods.OffsetAndMetadataDestroy, next);
            }

            foreach (IntPtr topicPartition in _topicPartitions)
            {
                Free(NativeMethods.TopicPartitionDestroy, topicPartition);
            }

            Free(NativeMethods.MapDestroy, _recordsMap);
            Free(NativeMethods.MapDestroy, _nextOffsetsMap);
            foreach (IntPtr block in _bytes)
            {
                Marshal.FreeHGlobal(block);
            }

            foreach (Utf8Marshal.PinnedUtf8String pinned in _strings)
            {
                pinned.Dispose();
            }
        }

        private IntPtr Record(TopicPartition partition, long offset, int leaderEpoch, string value)
        {
            IntPtr builder = Call(Native.ConsumerRecordOptionsBuilderNew);
            IntPtr options = IntPtr.Zero;
            try
            {
                IntPtr outErr = IntPtr.Zero;
                Native.ConsumerRecordOptionsBuilderSetTopic(builder, String(partition.Topic()), ref outErr);
                Native.ConsumerRecordOptionsBuilderSetPartition(builder, partition.Partition(), ref outErr);
                Native.ConsumerRecordOptionsBuilderSetOffset(builder, offset, ref outErr);
                Native.ConsumerRecordOptionsBuilderSetKey(builder, IntPtr.Zero, ref outErr);
                Native.ConsumerRecordOptionsBuilderSetValue(builder, Bytes(value), ref outErr);
                Native.ConsumerRecordOptionsBuilderSetLeaderEpoch(builder, leaderEpoch, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
                options = Call((ref IntPtr err) => Native.ConsumerRecordOptionsBuilderBuild(builder, ref err));
                IntPtr builtOptions = options;
                IntPtr record = Call((ref IntPtr err) => Native.ConsumerRecordWithOptions(builtOptions, ref err));
                _records.Add(record);
                return record;
            }
            finally
            {
                Free(Native.ConsumerRecordOptionsDestroy, options);
                Free(Native.ConsumerRecordOptionsBuilderDestroy, builder);
            }
        }

        private IntPtr Bytes(string text)
        {
            byte[] data = Encoding.UTF8.GetBytes(text);
            int header = Marshal.SizeOf<KafkaBytes>();
            IntPtr block = Marshal.AllocHGlobal(header + Math.Max(data.Length, 1));
            _bytes.Add(block);
            Marshal.Copy(data, 0, block + header, data.Length);
            Marshal.StructureToPtr(new KafkaBytes(block + header, data.Length), block, false);
            return block;
        }

        private IntPtr TopicPartition(TopicPartition partition)
        {
            IntPtr topic = String(partition.Topic());
            IntPtr handle = Call((ref IntPtr outErr) =>
                NativeMethods.TopicPartitionNew(topic, partition.Partition(), ref outErr));
            _topicPartitions.Add(handle);
            return handle;
        }

        private IntPtr String(string value)
        {
            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(value);
            _strings.Add(pinned);
            return pinned.Pointer;
        }

        private static void Put(IntPtr map, IntPtr key, IntPtr value)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapPut(map, key, value, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }

        private static IntPtr Call(NativeCall call)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr result = call(ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
            Assert.NotEqual(IntPtr.Zero, result);
            return result;
        }

        private static void Free(NativeFree free, IntPtr handle)
        {
            IntPtr outErr = IntPtr.Zero;
            free(handle, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }

    private delegate IntPtr NativeCall(ref IntPtr outErr);

    private delegate void NativeFree(IntPtr handle, ref IntPtr outErr);

    /// <summary>A UTF-8 deserializer that throws for the one value it is told to fail on.</summary>
    private sealed class FailingDeserializer : IDeserializer<string>
    {
        private readonly string _failOn;

        internal FailingDeserializer(string failOn)
        {
            _failOn = failOn;
        }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            string text = Encoding.UTF8.GetString(data.ToArray());
            return text == _failOn ? throw new FormatException("bad " + text) : text;
        }
    }

    /// <summary>The C functions that build a batch, which only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_with_next_offsets_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerRecordsWithNextOffsets(IntPtr records, IntPtr nextOffsets, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerRecordOptionsBuilderNew(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_topic_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderSetTopic(IntPtr self, IntPtr topic, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_partition_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderSetPartition(IntPtr self, int partition, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_offset_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderSetOffset(IntPtr self, long offset, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_key_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderSetKey(IntPtr self, IntPtr key, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_value_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderSetValue(IntPtr self, IntPtr value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_leader_epoch_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderSetLeaderEpoch(IntPtr self, int leaderEpoch, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_build", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerRecordOptionsBuilderBuild(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsBuilderDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConsumerRecordOptionsDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_with_options_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerRecordWithOptions(IntPtr options, ref IntPtr outErr);
    }
}
