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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0 and later only.
#if NET8_0_OR_GREATER

/// <summary>
/// The receive path decodes a topic once per batch, not once per record: varying only the topic's length at a fixed
/// record count moves the per-record cost by far less than one topic string, and records of one topic share one
/// string instance.
/// </summary>
/// <remarks>
/// Tests run in parallel, so the counter is per thread and the measured region does not await: a continuation could
/// resume on another thread.
/// </remarks>
public sealed class PublicConsumerTopicAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const int Partition = 0;
    private const int RecordCount = 256;

    private const int ShortTopicLength = 8;
    private const int LongTopicLength = 1008;

    private const int KeySize = 16;
    private const int ValueSize = 64;

    // About 4x the cost measured with one decode per topic, and far below one topic string per record.
    private const long PerRecordTopicBudgetBytes = 32;

    [Fact]
    public void Poll_LongerTopicName_AddsNoPerRecordTopicAllocation()
    {
        string shortTopic = new string('s', ShortTopicLength);
        string longTopic = new string('l', LongTopicLength);
        byte[] key = MakeBytes(KeySize, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);

        for (int i = 0; i < 5; i++)
        {
            MeasurePoll(shortTopic, RecordCount, key, value);
        }

        long shortBytes = MeasurePoll(shortTopic, RecordCount, key, value);
        long longBytes = MeasurePoll(longTopic, RecordCount, key, value);

        long perRecord = (longBytes - shortBytes) / RecordCount;

        Assert.True(
            perRecord <= PerRecordTopicBudgetBytes,
            $"Poll added {perRecord} B/record going from a {ShortTopicLength}-char topic to a " +
            $"{LongTopicLength}-char topic (budget {PerRecordTopicBudgetBytes} B; short={shortBytes} B, " +
            $"long={longBytes} B over {RecordCount} records). A per-record managed topic string would " +
            $"show ~{2 * (LongTopicLength - ShortTopicLength)} B/record here — the topic decode is NOT " +
            "shared across the batch (ffi §B4, consumer-threading §27).");
    }

    private static long MeasurePoll(string topic, int recordCount, byte[] key, byte[] value)
    {
        MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(topic, Partition) });
        consumer.Seek(new TopicPartition(topic, Partition), offset: 0);
        for (int i = 0; i < recordCount; i++)
        {
            consumer.AddRecord(topic, Partition, offset: i, key, value);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        ConsumerRecords<byte[], byte[]> records = consumer.Poll(s_pollTimeout);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(recordCount, records.Count);
        return after - before;
    }

    [Fact]
    public void Poll_SharedTopicString_IsReferenceEqualAcrossRecordsOfOneBatch()
    {
        // The mock gives every record its own copy of the topic, so a shared instance shows the decode is shared.
        string topic = new string('r', ShortTopicLength);
        byte[] value = MakeBytes(ValueSize, 0xEE);

        MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(topic, Partition) });
        consumer.Seek(new TopicPartition(topic, Partition), offset: 0);
        for (int i = 0; i < 8; i++)
        {
            consumer.AddRecord(topic, Partition, offset: i, null, value);
        }

        ConsumerRecords<byte[], byte[]> records = consumer.Poll(s_pollTimeout);
        Assert.Equal(8, records.Count);

        string? first = null;
        foreach (ConsumerRecord<byte[], byte[]> record in records)
        {
            Assert.Equal(topic, record.Topic);
            if (first is null)
            {
                first = record.Topic;
            }
            else
            {
                // ReferenceEquals: a per-record decode gives equal but distinct strings.
                Assert.True(
                    ReferenceEquals(first, record.Topic),
                    "Each record decoded its own topic string — the per-batch topic memo is not being hit.");
            }
        }
    }

    private static byte[] MakeBytes(int size, byte fill)
    {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++)
        {
            bytes[i] = fill;
        }

        return bytes;
    }
}

#endif
