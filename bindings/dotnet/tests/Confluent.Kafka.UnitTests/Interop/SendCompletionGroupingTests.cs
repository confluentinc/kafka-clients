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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

using Harness = Confluent.Kafka.UnitTests.Interop.SendAccumulatorTests.Harness;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.2 slice S3 — <b>per-<c>send_batch</c> completion grouping</b> (§3B, user decision
/// <b>D2</b>): the completion pump's unit is one <c>send_batch</c> call's records, which is the
/// anchor's own unit (<c>_confluentkafka.c:487-495</c> completes exactly one <c>BatchNode</c> per
/// <c>get_all</c>, and <c>:593</c> issues exactly one <c>send_batch</c> per node).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>"The records all arrived" cannot see any property in this file.</b> An ungrouped pump —
/// the pre-S3 flat per-record queue — delivers every record just as reliably; what it does
/// differently is bundle records from unrelated sends into one <c>get_all</c>, which returns only
/// once <em>every</em> future in it resolves. So every assertion here is on a <b>counter</b>, a
/// <b>pass size</b>, or a <b>relative completion order</b>, never on arrival.
/// </para>
/// <para>
/// <b>Settings are passed explicitly, not through the environment.</b> §3B.3's hazard is described
/// in terms of <c>CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD</c>, but reaching the same effective
/// settings through <see cref="SendAccumulatorSettings"/>'s constructor is the same input to the
/// same code with none of the process-wide mutation a concurrently-running test class could observe
/// — the convention the rest of this fixture already follows.
/// </para>
/// <para>
/// The fixture is <c>SendAccumulatorTests.Harness</c>, which wires producer + pump + topic cache +
/// accumulator exactly as <c>NativeProducer.EnsureAccumulator</c> does and appends through
/// production's own routing entry points (DoD §12).
/// </para>
/// </remarks>
public sealed class SendCompletionGroupingTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Pump_CompletesOneGroupPerSendBatchCall_WithEachGroupsOwnSize()
    {
        // ⚠ THE GROUPING PROPERTY (M11/P3.2 §6 test 10). 50 records at chunk 8 is ceil(50/8) == 7
        // send_batch calls of 8, 8, 8, 8, 8, 8, 2 — the same shape
        // SendAccumulatorTests.LoweredChunk_SplitsANodeIntoCeilCountOverChunkCalls asserts on the
        // SEND side. This is the COMPLETION side of the same drain, and the two counters must agree:
        //
        //   ProcessedBatchCount == SendBatchCallCount   — one get_all per send_batch call.
        //   LargestProcessedBatch == the largest GROUP  — 8, never the 50-record total.
        //
        // SendBatchCallCount is an INDEPENDENT witness: it is written by the batch thread inside the
        // chunk loop, and the pump never reads it.
        //
        // Mutation that must fail this: revert the pump to a flat per-record queue (or make one pass
        // coalesce consecutive queued groups up to DrainCap). One pass then carries the union, so
        // ProcessedBatchCount collapses toward 1 and LargestProcessedBatch becomes the total.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 8));

        Task<RecordMetadata>[] sends = harness.Append(50);
        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        Assert.Equal(7, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(8, harness.Accumulator.LargestSendBatchCount);

        Assert.Equal(
            harness.Accumulator.SendBatchCallCount,
            harness.ProcessedBatchCount);
        Assert.Equal(8, harness.LargestProcessedBatch);
    }

    [Fact]
    public async Task Pump_ASlowRecordInOneGroupDoesNotDelayAnotherGroupsCompletions()
    {
        // ⚠ THE HEAD-OF-LINE PROPERTY (M11/P3.2 §6 test 11) — the entire latency argument for D2,
        // and the only thing this phase asserts about it. It is unassertable from record arrival:
        // both shapes deliver everything eventually.
        //
        // 1000 records at chunk 600 is two send_batch calls, so two groups: 600 then 400. On a
        // MANUAL mock nothing resolves until CompleteNext() says so. Completing exactly the FIRST
        // group's 600 records must release exactly the first group's 600 awaiters — while the second
        // group's 400 stay pending, because its own get_all is still waiting on records nobody has
        // completed.
        //
        // Determinism rests on two verified facts, not on timing:
        //   * the mock's pending queue is FIFO — src/producer/mock_producer.rs holds a VecDeque,
        //     send pushes the back and complete_next pops the front — so CompleteNext() resolves the
        //     OLDEST record; and
        //   * group 1's send_batch precedes group 2's, because SendChain walks a node's chunks in
        //     index order.
        // So the 600 CompleteNext() calls resolve exactly group 1's records, in order.
        //
        // Mutation that must fail this: revert to the flat per-record queue. One get_all then spans
        // both groups' records, so it cannot return until group 2's records are completed too —
        // group 1's awaiters stay pending and the WhenAll below hits its deadline.
        const int Total = 1000;
        const int FirstGroup = 600;

        Harness harness = new Harness(
            () => new SendAccumulatorSettings(
                slotThreshold: 2000, maxAccumulatedRecords: 100_000, batchWindowMs: 60_000, batchChunk: FirstGroup),
            autoComplete: false);
        try
        {
            Task<RecordMetadata>[] sends = harness.Append(Total);
            harness.DrainNow();

            Assert.Equal(2, harness.Accumulator.SendBatchCallCount);

            for (int i = 0; i < FirstGroup; i++)
            {
                Assert.True(harness.CompleteNext(), $"the mock had no pending record left at {i}");
            }

            Task<RecordMetadata>[] first = new Task<RecordMetadata>[FirstGroup];
            Array.Copy(sends, first, FirstGroup);
            await TestTimeout.Run(() => Task.WhenAll(first), s_deadline);

            // The second group is still waiting on its own records — nothing in it may have been
            // released by the first group's completions.
            for (int i = FirstGroup; i < Total; i++)
            {
                Assert.False(
                    sends[i].IsCompleted,
                    $"send {i} is in the SECOND group but resolved while its own records are still " +
                    "pending — a pass carried records from both groups");
            }

            // Release the rest so teardown has nothing to fault, and so every task is observed.
            for (int i = FirstGroup; i < Total; i++)
            {
                Assert.True(harness.CompleteNext(), $"the mock had no pending record left at {i}");
            }

            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
            Assert.Equal(2, harness.ProcessedBatchCount);
        }
        finally
        {
            harness.Dispose();
        }
    }

    [Fact]
    public async Task Pump_AGroupLargerThanDrainCap_IsSplitAndStillCompletes()
    {
        // ⚠ THE §3B.3 HAZARD (M11/P3.2 §6 test 12) — the most likely place this slice breaks
        // something that worked before it. The pump's three marshalling arrays are DrainCap long, a
        // compile-time constant derived from the DEFAULT settings; a group is as large as the
        // producer's RUNTIME node capacity. Raising the threshold to 5000 gives a node capacity of
        // 5100 against arrays of 1100, so one send_batch call can hand the pump far more records
        // than the arrays hold.
        //
        // ProcessGroup splits it into ceil(count / DrainCap) bounded sub-passes. 2000 records in one
        // call is one group and two passes (1100 + 900).
        //
        // ⚠ ASSERT ON SUCCESS, never on the absence of a fault message: a faulted teardown send and
        // an accepted-residual send share an identical ObjectDisposedException message containing
        // "closed" (STATUS.md:20), so no message can separate outcomes here. The witnesses are
        // RanToCompletion, the pass count, the pass size, HistoryCount and DrainedSendCount.
        //
        // Mutation that must fail this: remove the split and hand the whole group to one pass.
        // Array.Clear(metadata, 0, 2000) over a 1100-long array throws, RunLoop's catch faults the
        // whole group, and every one of the 2000 sends fails.
        const int Total = 2000;
        const int ExpectedPasses = 2;   // ceil(2000 / 1100)

        Assert.Equal(1100, SendCompletionPump.DrainCap);
        Assert.Equal(ExpectedPasses, (Total + SendCompletionPump.DrainCap - 1) / SendCompletionPump.DrainCap);

        // ⚠ maxAdmittedRecords MUST be raised explicitly, like every other bound this test lifts out
        // of the way. Until M11/P3.3 S2 it was omitted and the DEFAULT happened to be 5000, which is
        // > Total — so the test passed on an ACCIDENT of that default rather than on anything it
        // states. When S2 set the measured default to 1000, admission throttled the 2000 records into
        // TWO send_batch calls and the "ONE group" assertion below failed (Expected 1, Actual 2) —
        // the test reporting a pump-splitting defect that did not exist. The subject here is the
        // PUMP's group splitting, so every accumulator-side bound is raised past Total deliberately.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 5000, maxAccumulatedRecords: 100_000, batchWindowMs: 60_000, batchChunk: 5100,
            maxAdmittedRecords: 100_000));

        Task<RecordMetadata>[] sends = harness.Append(Total);
        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.Equal(TaskStatus.RanToCompletion, send.Status);
        }

        Assert.Equal(Total, harness.HistoryCount);
        Assert.Equal(Total, harness.DrainedSendCount);

        // ONE send_batch call — so one group — split into exactly the arrays' worth per pass.
        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(ExpectedPasses, harness.ProcessedBatchCount);
        Assert.Equal(SendCompletionPump.DrainCap, harness.LargestProcessedBatch);
    }

    [Fact]
    public async Task DrainedSendCount_StillCountsRECORDS_NotGroups()
    {
        // ⚠ THE SILENT-BREAK GUARD (M11/P3.2 §6 test 15, §3B.4). DrainedSendCount is the witness for
        // the M11/P3.1 §3.8 teardown ordering, and four teardown tests read it as a RECORD count
        // (Expected: 24). Counting it once per group instead of once per record would leave every
        // one of them passing while silently measuring something else — the exact defect class this
        // phase exists to remove.
        //
        // 50 records across 7 groups separates the two readings by construction: 50 vs 7.
        //
        // Mutation that must fail this: increment by 1 per dequeued group instead of by the group's
        // record count. The counter drops to 7.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 8));

        Task<RecordMetadata>[] sends = harness.Append(50);
        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        Assert.Equal(7, harness.ProcessedBatchCount);
        Assert.Equal(50, harness.DrainedSendCount);
    }
}
