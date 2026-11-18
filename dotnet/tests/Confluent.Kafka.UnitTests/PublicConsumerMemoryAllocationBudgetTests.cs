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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0 and later only.
#if NET8_0_OR_GREATER

/// <summary>
/// The memory form's per-record receive allocation on <see cref="MockConsumer{TKey, TValue}"/>: a key or value kept
/// as memory is never copied, and costs one small manager when it has bytes and nothing when it is empty.
/// </summary>
/// <remarks>
/// The copy-out runs on the calling thread, so the per-thread counter sees only this poll. Everything but the
/// managers cancels in the subtraction of two polls of the same record count.
/// </remarks>
public sealed class PublicConsumerMemoryAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "memory-alloc-topic";
    private const int Partition = 0;

    private const int RecordCount = 64;
    private const int SmallSize = 16;
    private const int LargeSize = 64 * 1024;

    // A manager holds a reference, a pointer, a length and a flag: 40 B on a 64-bit runtime.
    private const long ManagerBudgetBytes = 64;

    [Fact]
    public void MemoryPoll_LargeKeyAndValue_AddNoAllocationOverSmallOnes()
    {
        WarmUp();

        long smallBytes = MeasurePoll(RecordCount, SmallSize);
        long largeBytes = MeasurePoll(RecordCount, LargeSize);

        long marginalPerRecord = (largeBytes - smallBytes) / RecordCount;

        Assert.True(
            marginalPerRecord <= 0,
            $"The memory form added {marginalPerRecord} B/record going from {SmallSize} B to {LargeSize} B keys " +
            $"and values (small={smallBytes} B, large={largeBytes} B over {RecordCount} records): the bytes are copied.");
    }

    [Fact]
    public void MemoryPoll_AllocatesOneManagerPerKeyOrValueWithBytes_AndNoneForAnEmptyOne()
    {
        WarmUp();

        long emptyBytes = MeasurePoll(RecordCount, 0);
        long filledBytes = MeasurePoll(RecordCount, SmallSize);

        long managersPerRecord = (filledBytes - emptyBytes) / RecordCount;

        Assert.True(
            managersPerRecord > 0 && managersPerRecord <= 2 * ManagerBudgetBytes,
            $"A record with a key and a value cost {managersPerRecord} B more than one with both empty (budget " +
            $"{2 * ManagerBudgetBytes} B for the two managers; empty={emptyBytes} B, filled={filledBytes} B over " +
            $"{RecordCount} records).");
    }

    private static void WarmUp()
    {
        for (int i = 0; i < 5; i++)
        {
            MeasurePoll(RecordCount, SmallSize);
            MeasurePoll(RecordCount, 0);
        }
    }

    private static long MeasurePoll(int recordCount, int size)
    {
        byte[] key = new byte[size];
        byte[] value = new byte[size];

        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                Serdes.ReadOnlyMemory().Deserializer(), Serdes.ReadOnlyMemory().Deserializer());
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
        ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> records = consumer.Poll(s_pollTimeout);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(recordCount, records.Count());
        foreach (ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record in records)
        {
            Assert.Equal(size, record.Key()!.Value.Length);
            Assert.Equal(size, record.Value()!.Value.Length);
        }

        return after - before;
    }
}

#endif
