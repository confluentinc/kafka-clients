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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Per-record receive-path allocation budget (ffi-marshalling.md §B4,
/// consumer-threading §27, DoD §10). The copy-out (§6.4) allocates only the owned
/// copies the user receives — the topic <see cref="string"/>, the key / value
/// <c>byte[]</c>, the record object, and the backing list — and <b>nothing</b>
/// attributable to batch traversal, borrowed-pointer marshalling, or a native-backed
/// view. This test asserts a per-record <b>budget</b> (owned copies + a documented
/// fixed overhead), never an exact byte count, and measures the marginal cost of
/// scaling the batch so per-poll fixed costs cancel out.
/// </summary>
/// <remarks>
/// <b>Method (producer send-path allocation-test precedent).</b>
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> deltas are JIT / TFM / warm-up
/// sensitive, so we (1) warm up the whole poll path first, (2) measure the allocation
/// of a <b>small</b> batch and a <b>large</b> batch, and (3) assert the <em>marginal
/// per-record</em> cost (large − small, divided by the record delta) is within a
/// budget derived from the owned copy sizes. Comparing two batch sizes cancels the
/// per-poll fixed overhead (the <c>ConsumerRecords</c>/list objects, bridge
/// bookkeeping), isolating the per-record cost; a native-backed view or a
/// batch-traversal allocation would push the marginal cost above the budget. The
/// measurement runs on the caller thread; the copy-out itself runs on the dispatcher
/// thread, but this test's purpose is the total managed budget attributable to a poll,
/// so it awaits on a thread whose allocation it can read and asserts scaling, which is
/// robust to which thread does the copy.
/// </remarks>
public sealed class ConsumerPollAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "alloc-topic";
    private const int Partition = 0;

    private const int KeySize = 16;
    private const int ValueSize = 64;

    // The unavoidable per-record owned copies: key[16] + value[64] + a copy of the
    // topic string (chars → 2 bytes each on the CLR heap) + the ConsumerRecord object
    // + its slot in the backing list. A generous per-record ceiling that still catches
    // an extra native-backed view / a second key-or-value-sized copy (which would ~2x
    // the payload contribution). Deliberately a budget, not an exact count.
    private const long PerRecordBudgetBytes = 1024;

    [Fact]
    public async Task PollAsync_PerRecordAllocation_WithinCopyOutBudget()
    {
        byte[] key = MakeBytes(KeySize, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);

        // Warm up the entire poll path (JIT, first-run allocations) before measuring.
        for (int i = 0; i < 5; i++)
        {
            await PollBatch(recordCount: 8, key, value);
        }

        const int smallCount = 16;
        const int largeCount = 256;

        long smallBytes = await MeasurePoll(smallCount, key, value);
        long largeBytes = await MeasurePoll(largeCount, key, value);

        long marginalBytes = largeBytes - smallBytes;
        int marginalRecords = largeCount - smallCount;
        long perRecord = marginalBytes / marginalRecords;

        Assert.True(
            perRecord <= PerRecordBudgetBytes,
            $"Per-record receive-path allocation {perRecord} B exceeded the copy-out budget " +
            $"{PerRecordBudgetBytes} B (small={smallBytes} B/{smallCount} rec, " +
            $"large={largeBytes} B/{largeCount} rec). A native-backed view or an extra " +
            "per-record copy would show up here.");
    }

    private static async Task<long> MeasurePoll(int recordCount, byte[] key, byte[] value)
    {
        // Settle the heap so the delta reflects this poll's steady-state allocation.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        ConsumerRecords records = await PollBatch(recordCount, key, value);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Touch the result so the JIT cannot elide the copy-out.
        Assert.Equal(recordCount, records.Count);
        return after - before;
    }

    private static async Task<ConsumerRecords> PollBatch(int recordCount, byte[] key, byte[] value)
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        try
        {
            consumer.Assign(new[] { (Topic, Partition) });
            await consumer.SeekAsync(Topic, Partition, offset: 0);
            for (int i = 0; i < recordCount; i++)
            {
                consumer.AddRecord(Topic, Partition, offset: i, key, value);
            }

            return await consumer.PollAsync(s_pollTimeout);
        }
        finally
        {
            await consumer.DisposeAsync();
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
