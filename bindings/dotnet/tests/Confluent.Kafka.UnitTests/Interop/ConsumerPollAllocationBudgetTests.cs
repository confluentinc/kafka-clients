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

// GC.GetTotalAllocatedBytes has no net462 equivalent, and this is a runtime-behavior
// test (net462 runs are Windows/CI-only anyway) — so the whole class is net8.0+ only,
// keeping the net462 TFM smoke leg (PLAN §5.7) compiling.
#if NET8_0_OR_GREATER

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
/// <b>Method (producer send-path allocation-test precedent; Critic N=7 Finding 2).</b>
/// The copy-out runs on the core's foreign <b>dispatcher thread</b>, and the awaiter's
/// continuation resumes on a <b>different</b> pool thread — so a per-thread counter
/// (<c>GC.GetAllocatedBytesForCurrentThread</c>) bracketing the <c>await</c> measures
/// neither thread's real allocation (the delta can even go negative). We therefore use
/// the <b>process-wide</b> <see cref="GC.GetTotalAllocatedBytes(bool)"/> with
/// <c>precise: true</c>, which captures the dispatcher-thread copy-out regardless of
/// which thread ran it. To stay robust to JIT / TFM / warm-up noise and to any
/// unrelated ambient allocation the process-wide counter also sees, we (1) warm up the
/// whole poll path, then (2) measure a <b>small</b> and a <b>large</b> steady-state
/// batch and assert the <em>marginal per-record</em> cost (large − small, over the
/// record delta) is within a budget derived from the owned copy sizes. The marginal
/// subtraction cancels the per-poll fixed overhead (the <c>ConsumerRecords</c>/list
/// objects, bridge bookkeeping) <em>and</em> any per-poll ambient noise, isolating the
/// per-record copy-out cost; a native-backed view or a second key/value-sized copy
/// would push the marginal cost above the budget.
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
    public async Task PollWithCallback_PerRecordAllocation_WithinCopyOutBudget()
    {
        byte[] key = MakeBytes(KeySize, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);

        // Warm up the entire poll path (JIT, first-run allocations) before measuring.
        for (int i = 0; i < 5; i++)
        {
            await MeasurePoll(recordCount: 8, key, value);
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

    /// <summary>
    /// Sets up a consumer + records OUTSIDE the measured window, then brackets <b>only</b>
    /// the <c>PollWithCallback</c> call — where the copy-out happens — with the process-wide
    /// precise allocation counter. Consumer create / assign / seek / the per-record
    /// <c>AddRecord</c> marshal loop / dispose are all excluded (none is copy-out;
    /// Finding 2). The owned copy-out <see cref="ConsumerRecords"/> holds only managed
    /// values (no native handle, no <c>IDisposable</c>); it stays rooted until after the
    /// delta is read, then is discarded when the method returns.
    /// </summary>
    private static async Task<long> MeasurePoll(int recordCount, byte[] key, byte[] value)
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        try
        {
            consumer.Assign(new[] { (Topic, Partition) });
            consumer.Seek(Topic, Partition, offset: 0); // sync (M5/P7)
            for (int i = 0; i < recordCount; i++)
            {
                consumer.AddRecord(Topic, Partition, offset: i, key, value);
            }

            // Settle the heap so the delta reflects this poll's steady-state allocation.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            // Process-wide (precise) — the copy-out runs on the foreign dispatcher thread,
            // so a per-thread counter would miss it (Finding 2). The marginal (large −
            // small) subtraction in the caller cancels the fixed per-poll overhead and any
            // ambient allocation this process-wide counter also sees, leaving the
            // per-record copy-out cost.
            long before = GC.GetTotalAllocatedBytes(precise: true);
            ConsumerRecords records = await consumer.PollWithCallback(s_pollTimeout);
            long after = GC.GetTotalAllocatedBytes(precise: true);

            // Touch the result so the JIT cannot elide the copy-out.
            Assert.Equal(recordCount, records.Count);
            return after - before;
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

#endif
