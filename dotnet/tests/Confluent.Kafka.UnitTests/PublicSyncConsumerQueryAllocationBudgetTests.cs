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
/// The per-call allocation of the sync BeginningOffsets and PartitionsFor on <see cref="MockConsumer{TKey, TValue}"/>:
/// the marginal cost of a call stays under a ceiling for a one-entry result.
/// </summary>
/// <remarks>
/// The copy-out runs on the calling thread, so the per-thread counter sees only these calls, whatever other tests run.
/// </remarks>
public sealed class PublicSyncConsumerQueryAllocationBudgetTests
{
    private const string Topic = "sync-query-alloc-topic";

    // A sanity ceiling, not a zero-allocation bound: a query is per call, not per record.
    private const long PerOpBudgetBytes = 4096;

    [Fact]
    public void BeginningOffsets_PerOpAllocation_IsBounded()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
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
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
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
