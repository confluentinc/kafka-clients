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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The typed mock's record path over <see cref="NativeConsumer"/>: a record's key and value cross the C mock as ids
/// in the C record's slots, held by <see cref="MockRecordSlots{TKey, TValue}"/>, and come back as the same objects at
/// a poll; null slots stay null, every other field round-trips through the C record, and a batch whose slots hold ids
/// is released and destroyed without the core or the binding reading them.
/// </summary>
public sealed class MockRecordSlotsTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "slots-topic";
    private const int Partition = 0;

    private static NativeConsumer SyncMock()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock(withPump: false);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    private static NativeConsumer AsyncMock()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    private static ConsumerRecords<TKey, TValue> PollUntil<TKey, TValue>(
        NativeConsumer consumer, MockRecordSlots<TKey, TValue> slots, int count)
    {
        DateTime end = DateTime.UtcNow + s_deadline;
        while (true)
        {
            ConsumerRecords<TKey, TValue> records = consumer.PollResolved(s_pollTimeout, slots);
            if (records.Count() >= count || DateTime.UtcNow > end)
            {
                return records;
            }
        }
    }

    [Fact]
    public void PollResolved_ReturnsTheSameKeyAndValueObjects()
    {
        NativeConsumer consumer = SyncMock();
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<string, object> slots = new MockRecordSlots<string, object>();
        string key = new string('k', 3);
        object value = new object();

        consumer.AddRecord(new ConsumerRecord<string, object>(Topic, Partition, 5, key, value), slots);
        Assert.Equal(1, slots.KeyCount);
        Assert.Equal(1, slots.ValueCount);

        ConsumerRecord<string, object> record = Assert.Single(PollUntil(consumer, slots, 1));
        Assert.Same(key, record.Key());
        Assert.Same(value, record.Value());
        Assert.Equal(5, record.Offset());
        Assert.Equal(0, slots.KeyCount);
        Assert.Equal(0, slots.ValueCount);
    }

    [Fact]
    public void PollResolved_ValueTypeKeyAndValue_RoundTrip()
    {
        NativeConsumer consumer = SyncMock();
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<int, long> slots = new MockRecordSlots<int, long>();

        consumer.AddRecord(new ConsumerRecord<int, long>(Topic, Partition, 0, 0, long.MaxValue), slots);
        consumer.AddRecord(new ConsumerRecord<int, long>(Topic, Partition, 1, -7, 0), slots);

        ConsumerRecord<int, long>[] records = PollUntil(consumer, slots, 2).ToArray();
        Assert.Equal(2, records.Length);
        Assert.Equal(0, records[0].Key());
        Assert.Equal(long.MaxValue, records[0].Value());
        Assert.Equal(-7, records[1].Key());
        Assert.Equal(0L, records[1].Value());
    }

    [Fact]
    public void PollResolved_NullKeyAndValue_StayNull()
    {
        NativeConsumer consumer = SyncMock();
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();

        consumer.AddRecord(new ConsumerRecord<string, string>(Topic, Partition, 0, null, null), slots);
        consumer.AddRecord(new ConsumerRecord<string, string>(Topic, Partition, 1, "k", null), slots);
        consumer.AddRecord(new ConsumerRecord<string, string>(Topic, Partition, 2, null, "v"), slots);
        Assert.Equal(1, slots.KeyCount);
        Assert.Equal(1, slots.ValueCount);

        ConsumerRecord<string, string>[] records = PollUntil(consumer, slots, 3).ToArray();
        Assert.Equal(3, records.Length);
        Assert.Null(records[0].Key());
        Assert.Null(records[0].Value());
        Assert.Equal("k", records[1].Key());
        Assert.Null(records[1].Value());
        Assert.Null(records[2].Key());
        Assert.Equal("v", records[2].Value());
        Assert.Equal(0, slots.KeyCount);
        Assert.Equal(0, slots.ValueCount);
    }

    [Theory]
    [InlineData(TimestampType.CREATE_TIME)]
    [InlineData(TimestampType.LOG_APPEND_TIME)]
    [InlineData(TimestampType.NO_TIMESTAMP_TYPE)]
    public void PollResolved_EveryFieldRoundTrips(TimestampType timestampType)
    {
        NativeConsumer consumer = SyncMock();
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();
        IHeaders headers = new RecordHeaders()
            .Add("h1", Encoding.UTF8.GetBytes("v1"))
            .Add("h2", null)
            .Add("h1", Array.Empty<byte>());

        consumer.AddRecord(
            new ConsumerRecord<string, string>(
                Topic, Partition, 9, 1234567890123L, timestampType, 11, 22, "key", "value", headers, 3, (short)4),
            slots);

        ConsumerRecord<string, string> record = Assert.Single(PollUntil(consumer, slots, 1));
        Assert.Equal(Topic, record.Topic());
        Assert.Equal(Partition, record.Partition());
        Assert.Equal(9, record.Offset());
        Assert.Equal(1234567890123L, record.Timestamp());
        Assert.Equal(timestampType, record.TimestampType());
        Assert.Equal(11, record.SerializedKeySize());
        Assert.Equal(22, record.SerializedValueSize());
        Assert.Equal("key", record.Key());
        Assert.Equal("value", record.Value());
        Assert.Equal(3, record.LeaderEpoch());
        Assert.Equal((short)4, record.DeliveryCount());
        IHeader[] polled = record.Headers().ToArray();
        Assert.Equal(3, polled.Length);
        Assert.Equal("h1", polled[0].Key());
        Assert.Equal(Encoding.UTF8.GetBytes("v1"), polled[0].Value());
        Assert.Equal("h2", polled[1].Key());
        Assert.Null(polled[1].Value());
        Assert.Equal("h1", polled[2].Key());
        Assert.Equal(Array.Empty<byte>(), polled[2].Value());
    }

    [Fact]
    public void PollResolved_AbsentLeaderEpochAndDeliveryCount_StayAbsent()
    {
        NativeConsumer consumer = SyncMock();
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();

        consumer.AddRecord(
            new ConsumerRecord<string, string>(
                Topic, Partition, 0, ConsumerRecord<string, string>.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord<string, string>.NULL_SIZE, ConsumerRecord<string, string>.NULL_SIZE, "k", "v",
                new RecordHeaders(), null, null),
            slots);

        ConsumerRecord<string, string> record = Assert.Single(PollUntil(consumer, slots, 1));
        Assert.Null(record.LeaderEpoch());
        Assert.Null(record.DeliveryCount());
        Assert.Equal(ConsumerRecord<string, string>.NO_TIMESTAMP, record.Timestamp());
        Assert.Equal(ConsumerRecord<string, string>.NULL_SIZE, record.SerializedKeySize());
        Assert.Empty(record.Headers());
    }

    [Fact]
    public async Task PollWithCallback_Slots_ReturnsTheSameObjects()
    {
        NativeConsumer consumer = AsyncMock();
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<string, object> slots = new MockRecordSlots<string, object>();
        object value = new object();
        consumer.AddRecord(new ConsumerRecord<string, object>(Topic, Partition, 0, "k", value), slots);

        ConsumerRecords<string, object> records = ConsumerRecords<string, object>.EMPTY;
        DateTime end = DateTime.UtcNow + s_deadline;
        while (records.Count() == 0 && DateTime.UtcNow < end)
        {
            records = await consumer.PollWithCallback(s_pollTimeout, slots, CancellationToken.None);
        }

        ConsumerRecord<string, object> record = Assert.Single(records);
        Assert.Equal("k", record.Key());
        Assert.Same(value, record.Value());
        Assert.Equal(0, slots.KeyCount);
        Assert.Equal(0, slots.ValueCount);
    }

    // The batches hold ids in their slots: releasing them to the collector and destroying the consumer must not read
    // them as bytes.
    [Fact]
    public void BatchesWithIdSlots_AreReleasedAndDestroyedWithoutAFault()
    {
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();
        NativeConsumer consumer = SyncMock();
        List<ConsumerRecord<string, string>> kept = new List<ConsumerRecord<string, string>>();
        using (TestTimeout.Disposal(consumer))
        {
            for (int round = 0; round < 20; round++)
            {
                for (int i = 0; i < 10; i++)
                {
                    long offset = (round * 10) + i;
                    consumer.AddRecord(
                        new ConsumerRecord<string, string>(Topic, Partition, offset, "k" + offset, "v" + offset), slots);
                }

                ConsumerRecords<string, string> records = PollUntil(consumer, slots, 10);
                Assert.Equal(10, records.Count());
                kept.Add(records.First());
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            consumer.AddRecord(new ConsumerRecord<string, string>(Topic, Partition, 1000, "left", "behind"), slots);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        for (int round = 0; round < kept.Count; round++)
        {
            Assert.Equal("k" + (round * 10), kept[round].Key());
            Assert.Equal(round * 10, kept[round].Offset());
        }
    }

    [Fact]
    public void AddRecord_RefusedByTheCore_LeavesNoEntry()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock(withPump: false);
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();

        IllegalStateException error = Assert.Throws<IllegalStateException>(() => consumer.AddRecord(
            new ConsumerRecord<string, string>("not-assigned", Partition, 0, "k", "v"), slots));
        Assert.Equal("Cannot add records for a partition that is not assigned to the consumer", error.Message);
        Assert.Equal(0, slots.KeyCount);
        Assert.Equal(0, slots.ValueCount);
    }

    [Fact]
    public void Take_MissingEntry_IsAnIllegalState()
    {
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();
        slots.Add("k", "v", out nint keySlot, out nint valueSlot);
        Assert.Equal((nint)1, keySlot);
        Assert.Equal((nint)2, valueSlot);

        Assert.Throws<IllegalStateException>(() => slots.Take(keySlot + 10, 0, Topic, Partition, out _, out _));
        Assert.Throws<IllegalStateException>(() => slots.Take(keySlot, valueSlot + 10, Topic, Partition, out _, out _));
        Assert.Equal(1, slots.KeyCount);
        Assert.Equal(1, slots.ValueCount);

        slots.Clear();
        Assert.Equal(0, slots.KeyCount);
        Assert.Equal(0, slots.ValueCount);
        slots.Add("k2", null, out nint nextKey, out nint nullValue);
        Assert.Equal((nint)3, nextKey);
        Assert.Equal((nint)0, nullValue);
    }

    [Fact]
    public void Discard_RemovesTheSnapshotsEntries_AndKeepsTheOnesMarkedAfterIt()
    {
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();
        slots.Add("k0", "v0", out nint key0, out nint value0);
        slots.MarkInCore(key0, value0, Topic, 0);
        slots.Add(null, "v1", out nint key1, out nint value1);
        slots.MarkInCore(key1, value1, Topic, 1);
        // Added but never marked: the core did not take it, so no discard removes it.
        slots.Add("k2", "v2", out _, out _);
        MockRecordSlots<string, string>.InCore[] snapshot = slots.SnapshotInCore();
        Assert.Equal(2, snapshot.Length);
        // Marked after the snapshot, as a record added while the call runs would be.
        slots.Add("k3", "v3", out nint key3, out nint value3);
        slots.MarkInCore(key3, value3, Topic, 0);

        slots.Discard(snapshot);

        Assert.Equal(2, slots.KeyCount);
        Assert.Equal(2, slots.ValueCount);
        slots.Take(key3, value3, Topic, 0, out string? key, out string? value);
        Assert.Equal("k3", key);
        Assert.Equal("v3", value);
        Assert.Empty(slots.SnapshotInCore());
    }

    [Fact]
    public void Discard_WithPartitions_RemovesOnlyThoseEntries_AndToleratesTakenOnes()
    {
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();
        slots.Add("k0", "v0", out nint key0, out nint value0);
        slots.MarkInCore(key0, value0, Topic, 0);
        slots.Add("k1", "v1", out nint key1, out nint value1);
        slots.MarkInCore(key1, value1, Topic, 1);
        slots.Add("k2", "v2", out nint key2, out nint value2);
        slots.MarkInCore(key2, value2, Topic, 0);
        MockRecordSlots<string, string>.InCore[] snapshot = slots.SnapshotInCore();
        // A poll on another thread took this one after the snapshot.
        slots.Take(key2, value2, Topic, 0, out _, out _);

        slots.Discard(snapshot, new HashSet<(string Topic, int Partition)> { (Topic, 0) });

        Assert.Equal(1, slots.KeyCount);
        Assert.Equal(1, slots.ValueCount);
        slots.Take(key1, value1, Topic, 1, out string? key, out _);
        Assert.Equal("k1", key);
    }

    [Fact]
    public void MarkInCore_AfterTheEntriesAreGone_MarksNothing()
    {
        MockRecordSlots<string, string> slots = new MockRecordSlots<string, string>();
        slots.Add("k", "v", out nint keySlot, out nint valueSlot);
        slots.Clear();

        slots.MarkInCore(keySlot, valueSlot, Topic, 0);
        slots.MarkInCore(0, 0, Topic, 0);

        Assert.Empty(slots.SnapshotInCore());
    }
}
