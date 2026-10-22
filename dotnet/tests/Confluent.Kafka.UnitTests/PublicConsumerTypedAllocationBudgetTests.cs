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

using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0 and later only.
#if NET8_0_OR_GREATER

/// <summary>
/// The typed poll's per-record receive allocation on <see cref="MockConsumer{TKey, TValue}"/>: a value deserialized
/// to its span length is never copied, so a 64 KiB value costs no more per record than a 16-byte one.
/// </summary>
/// <remarks>
/// The copy-out runs on the calling thread, so the per-thread counter sees only this poll. The topic cancels in the
/// subtraction, so <see cref="PublicConsumerTopicAllocationBudgetTests"/> gates it.
/// </remarks>
public sealed class PublicConsumerTypedAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "typed-alloc-topic";
    private const int Partition = 0;

    private const int RecordCount = 64;
    private const int SmallValueSize = 16;
    private const int LargeValueSize = 64 * 1024;

    // Far below one value-sized copy, so an intermediate byte[] on the value path fails it.
    private const long MarginalPerRecordBudgetBytes = 1024;

    [Fact]
    public void TypedPoll_LargeValue_AddsNoValueSizedIntermediateAllocation()
    {
        for (int i = 0; i < 5; i++)
        {
            MeasurePoll(RecordCount, SmallValueSize);
        }

        long smallBytes = MeasurePoll(RecordCount, SmallValueSize);
        long largeBytes = MeasurePoll(RecordCount, LargeValueSize);

        long marginalPerRecord = (largeBytes - smallBytes) / RecordCount;

        Assert.True(
            marginalPerRecord <= MarginalPerRecordBudgetBytes,
            $"Typed poll added {marginalPerRecord} B/record going from a {SmallValueSize} B value to a " +
            $"{LargeValueSize} B value (budget {MarginalPerRecordBudgetBytes} B; small={smallBytes} B, " +
            $"large={largeBytes} B over {RecordCount} records). A per-record intermediate value-sized " +
            "byte[] on the key/value path would show ~the value-size delta here — the deserialize is NOT zero-copy.");
    }

    private static long MeasurePoll(int recordCount, int valueSize)
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        byte[] key = new byte[8];
        byte[] value = new byte[valueSize];

        MockConsumer<byte[], int> consumer =
            new MockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        for (int i = 0; i < recordCount; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, key, value);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        ConsumerRecords<byte[], int> records = consumer.Poll(s_pollTimeout);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(recordCount, records.Count);
        foreach (ConsumerRecord<byte[], int> record in records)
        {
            Assert.Equal(valueSize, record.Value);
        }

        return after - before;
    }
}

#endif
