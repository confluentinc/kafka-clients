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

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, and this is a runtime-behavior
// test (net462 runs are Windows/CI-only) — so the whole class is net8.0+ only, keeping the net462
// TFM smoke leg compiling.
#if NET8_0_OR_GREATER

/// <summary>
/// M5/P8a — per-record receive-path allocation budget through the <b>public synchronous</b>
/// <see cref="MockConsumer{TKey, TValue}"/> surface (PLAN §8; ffi-marshalling.md §B4,
/// consumer-threading §27, DoD §10). The sync <see cref="IConsumer{TKey, TValue}.Poll"/> copy-out
/// runs the shared receive-path marshaller (<c>ConsumerRecordsMarshal.CopyOut&lt;K,V&gt;</c>) on
/// the caller's thread and produces the same owned <c>byte[]</c> key/value + topic string + record
/// objects the async path does — so measuring it on-thread fully covers the async per-record
/// budget too (M7/P1). Asserts the marginal per-record cost (large − small) is within a budget
/// derived from the owned copy sizes; a native-backed view or an extra per-record copy would push
/// it over.
/// </summary>
/// <remarks>
/// Uses the <b>per-thread</b> <see cref="GC.GetAllocatedBytesForCurrentThread"/> counter (M7/P1),
/// not the process-wide <see cref="GC.GetTotalAllocatedBytes(bool)"/>: the sync copy-out runs on
/// the caller's (this) thread, so a per-thread count captures exactly this poll's allocation and is
/// <b>immune to allocations by concurrently-running tests on other threads</b> — robust under
/// parallel execution, with no assembly-wide serialization needed. The marginal (large − small)
/// subtraction still cancels the fixed per-poll cost, and the poll path is warmed up first.
/// </remarks>
public sealed class PublicSyncConsumerAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "sync-alloc-topic";
    private const int Partition = 0;

    private const int KeySize = 16;
    private const int ValueSize = 64;

    // Unavoidable per-record owned copies: key[16] + value[64] + topic string copy + the
    // ConsumerRecord object + its list slot. A generous ceiling that still catches an extra
    // native-backed view / a second key-or-value-sized copy. A budget, not an exact count.
    private const long PerRecordBudgetBytes = 1024;

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
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
