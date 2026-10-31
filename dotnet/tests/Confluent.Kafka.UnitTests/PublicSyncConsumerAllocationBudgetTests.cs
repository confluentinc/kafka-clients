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
/// The per-record receive allocation of the sync <see cref="IConsumer{TKey, TValue}.Poll"/> on the
/// <see cref="MockConsumer{TKey, TValue}"/>: the cost of a large poll minus a small one stays within the owned copies.
/// </summary>
/// <remarks>
/// The copy-out runs on the calling thread, so the per-thread counter sees only this poll, whatever other tests run.
/// </remarks>
public sealed class PublicSyncConsumerAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "sync-alloc-topic";
    private const int Partition = 0;

    private const int KeySize = 16;
    private const int ValueSize = 64;

    // The owned copies per record (key, value, the ConsumerRecord and its list slot), at about twice their measured
    // size, so a new per-record copy fails the budget. The topic has its own budget test.
    private const long PerRecordBudgetBytes = 448;

    [Fact]
    public void Poll_PerRecordAllocation_WithinCopyOutBudget()
    {
        byte[] key = MakeBytes(KeySize, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);

        for (int i = 0; i < 5; i++)
        {
            MeasurePoll(recordCount: 8, key, value);
        }

        const int smallCount = 16;
        const int largeCount = 256;

        long smallBytes = MeasurePoll(smallCount, key, value);
        long largeBytes = MeasurePoll(largeCount, key, value);

        long perRecord = (largeBytes - smallBytes) / (largeCount - smallCount);

        Assert.True(
            perRecord <= PerRecordBudgetBytes,
            $"Per-record receive-path allocation {perRecord} B exceeded the copy-out budget " +
            $"{PerRecordBudgetBytes} B (small={smallBytes} B/{smallCount} rec, " +
            $"large={largeBytes} B/{largeCount} rec) — a native-backed view or an extra copy would show here.");
    }

    private static long MeasurePoll(int recordCount, byte[] key, byte[] value)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
        ConsumerRecords<byte[], byte[]> records = consumer.Poll(s_pollTimeout);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(recordCount, records.Count);
        return after - before;
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
