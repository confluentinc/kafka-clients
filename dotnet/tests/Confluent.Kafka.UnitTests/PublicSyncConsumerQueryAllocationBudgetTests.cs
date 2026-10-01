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
/// M5/P8b — per-op allocation budget for the <b>synchronous</b> query family through the public
/// <see cref="MockConsumer{TKey, TValue}"/> surface (PLAN §8; DoD §10). These are per-RPC top-level
/// surfaces, not a hot path (CLAUDE.md §11), so a per-call owned result dictionary/list + entries is
/// amortized and fine. A LIGHT sanity bound (not zero-alloc): the marginal per-op cost does NOT
/// scale beyond the fixed one-entry result — an unbounded or per-something copy (e.g. a stored
/// native-backed view or a second copy pass) would show here. The sync query copy-out runs the
/// shared query marshallers (<c>OffsetMapMarshal.CopyOut</c> / <c>PartitionInfoListMarshal.CopyOut</c>)
/// on the caller's thread, so measuring them on-thread fully covers the async per-op query budget
/// too (M7/P1).
/// </summary>
/// <remarks>
/// Uses the <b>per-thread</b> <see cref="GC.GetAllocatedBytesForCurrentThread"/> counter (M7/P1),
/// not the process-wide <see cref="GC.GetTotalAllocatedBytes(bool)"/>: the sync copy-out runs on
/// the caller's (this) thread, so a per-thread count captures exactly these ops' allocation and is
/// <b>immune to allocations by concurrently-running tests on other threads</b> — robust under
/// parallel execution, with no assembly-wide serialization needed. The marginal (large − small)
/// subtraction still cancels the fixed per-call cost, and the op is warmed up first.
/// </remarks>
public sealed class PublicSyncConsumerQueryAllocationBudgetTests
{
    private const string Topic = "sync-query-alloc-topic";

    // A per-RPC (NOT per-record) sanity ceiling — a Task-free sync path allocates only the owned
    // result map + one entry + a couple of delegate closures per call. Generous enough not to be
    // brittle, tight enough to catch an unbounded / per-something copy.
    private const long PerOpBudgetBytes = 4096;

    [Fact]
    public void BeginningOffsets_PerOpAllocation_IsBounded()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateBeginningOffset(Topic, 0, 1);
        TopicPartition[] request = { new TopicPartition(Topic, 0) };

        for (int i = 0; i < 10; i++)
        {
            _ = consumer.BeginningOffsets(request);
        }

        long small = Measure(() => consumer.BeginningOffsets(request), count: 50);
        long large = Measure(() => consumer.BeginningOffsets(request), count: 500);

        long perOp = (large - small) / (500 - 50);

        Assert.True(
            perOp <= PerOpBudgetBytes,
            $"Per-op BeginningOffsets allocation {perOp} B exceeded the per-RPC sanity budget " +
            $"{PerOpBudgetBytes} B (small={small} B/50 op, large={large} B/500 op) — " +
            "an unbounded / per-something allocation would show here.");
    }

    [Fact]
    public void PartitionsFor_PerOpAllocation_IsBounded()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdatePartitions(Topic, partitionCount: 1, leaderId: 7, "broker-1", leaderPort: 9092);

        for (int i = 0; i < 10; i++)
        {
            _ = consumer.PartitionsFor(Topic);
        }

        long small = Measure(() => consumer.PartitionsFor(Topic), count: 50);
        long large = Measure(() => consumer.PartitionsFor(Topic), count: 500);

        long perOp = (large - small) / (500 - 50);

        Assert.True(
            perOp <= PerOpBudgetBytes,
            $"Per-op PartitionsFor allocation {perOp} B exceeded the per-RPC sanity budget " +
            $"{PerOpBudgetBytes} B (small={small} B/50 op, large={large} B/500 op) — " +
            "an unbounded / per-something allocation would show here.");
    }

    private static long Measure(Action op, int count)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < count; i++)
        {
            op();
        }

        long after = GC.GetAllocatedBytesForCurrentThread();
        return after - before;
    }
}

#endif
