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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

using Harness = Confluent.Kafka.UnitTests.Interop.SendAccumulatorTests.Harness;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.2 slice S4 — <b>the bounded pre-stop pump drain</b> (§F4, user decision <b>D3</b>): after
/// the teardown flush and <b>before</b> <c>_stopping</c> is set, teardown waits — bounded — for the
/// completion pump's queue to reach empty while the loop is still running, so a send the core
/// already accepted is <em>completed</em> rather than faulted.
/// </summary>
/// <remarks>
/// <para>
/// <b>What was wrong.</b> <c>RunLoop</c> reads <c>_stopping</c> at its loop top, so a group enqueued
/// while the loop is parked in its wait is processed only if the pump thread is <em>scheduled</em>
/// before <c>Stop</c> sets that flag; otherwise the loop breaks and <c>DrainAndFaultRemaining</c>
/// faults every awaiter. The accumulator made that routine rather than rare: <c>StopPump</c> runs
/// <c>StopAccumulator()</c> first, so every close-with-buffered-records now dumps a burst into the
/// pump queue immediately before <c>_stopping</c>. The anchor has no fault-the-remainder path at
/// all — its poll thread keeps draining the pending chain to empty once <c>send_completed</c> is set
/// (<c>_confluentkafka.c:484</c>, <c>:504-507</c>, <c>:651</c>).
/// </para>
/// <para>
/// ⚠ <b>The exception-message trap applies directly here</b> (<c>STATUS.md:20</c>): a faulted
/// teardown send and an accepted-residual send carry an <em>identical</em>
/// <see cref="ObjectDisposedException"/> message containing "closed", so no assertion on a message
/// can separate the two outcomes. Every assertion below is therefore on <b>success</b> and on the
/// pump's <b>counters</b> — <c>DrainedSendCount</c> (records taken off the queue) and
/// <c>ProcessedBatchCount</c> (<c>get_all</c> passes actually run) — never on the absence of a
/// fault message.
/// </para>
/// </remarks>
public sealed class SendCompletionPumpPreStopDrainTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Teardown_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem()
    {
        // ⚠ M11/P3.2 §6 test 17 — THE S4 PROPERTY. Records are appended and left in the accumulator;
        // teardown's own StopAccumulator is what drains them, so the group reaches the pump's queue
        // in exactly the F4 window — microseconds before _stopping — and the teardown flush has
        // already resolved everything the pump's get_all will read. With the bounded wait in place
        // the pump ALWAYS gets to take the group, so every send completes and the pass runs.
        //
        // ⚠ A K-BURST WITH A FRESH HARNESS PER ROUND, deliberately, because the defect is a
        // SCHEDULING race rather than a deterministic ordering: without the wait, whether the group
        // is completed or faulted depends on whether the pump thread wakes inside a window made of
        // one CloseGate plus one Producer_flush. A single round would be a coin flip and therefore
        // no guard at all (the same lesson SendCompletionPumpDrainCapTests records for its terminal
        // drain test). Each round is an independent trial, so the mutation has to win every one of
        // them to pass.
        //
        // Mutation that must fail this: delete the WaitForQueueDrain call from the teardown ordering
        // (NativeProducer.StopPump / StopPumpAsync, mirrored by the fixture's Dispose). The
        // pre-existing behaviour returns — RunLoop breaks on _stopping with the group still queued
        // and DrainAndFaultRemaining faults it — so a round fails on RanToCompletion and on
        // ProcessedBatchCount == 0.
        const int Rounds = 48;
        const int Records = 8;

        for (int round = 0; round < Rounds; round++)
        {
            Harness harness = new Harness(new SendAccumulatorSettings(
                slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1000));

            // No DrainNow(): letting teardown do the drain is the whole point — it is what puts the
            // enqueue inside the window this slice closes.
            Task<RecordMetadata>[] sends = harness.Append(Records);

            TestTimeout.Run(harness.Dispose, s_deadline);

            // Every record reached Enqueue while the gate was open (the M11/P3.1 §3.8 witness, and
            // still a RECORD count after S3's grouping change).
            Assert.Equal(Records, harness.DrainedSendCount);

            // ...and the pump RESOLVED it: one send_batch call, so one group, so exactly one
            // get_all pass. Zero is what the fault path produces — DrainAndFaultRemaining dequeues
            // but never runs a pass.
            Assert.Equal(1, harness.ProcessedBatchCount);

            for (int i = 0; i < Records; i++)
            {
                Assert.Equal(TaskStatus.RanToCompletion, sends[i].Status);
            }
        }
    }

    [Fact]
    public void Teardown_WithAnUnresolvableSend_StillReturnsWithinTheBound()
    {
        // ⚠ M11/P3.2 §6 test 18 — THE BOUND. The wait must return even when the queue can NEVER
        // reach empty, because on expiry the defined outcome (Stop faults the remainder, exactly as
        // before S4) is the thing that keeps this from being a hang.
        //
        // The construction: a MANUAL mock, so nothing resolves until something drives it. Twelve
        // records at chunk 4 is three send_batch calls, so three groups. The pump takes group 1 and
        // parks in an uninterruptible get_all on records that nothing will complete while this test
        // runs — so groups 2 and 3 stay queued and the queue cannot reach empty, monotonically and
        // by construction, not by timing.
        //
        // Mutation that must fail this: make the wait unbounded (loop on !_queue.IsEmpty with no
        // deadline). It then never returns — which is why this test is written with a HARD TIMEOUT
        // that fails rather than hangs the suite, for the wait AND for the teardown that follows it.
        //
        // ⚠ WHY NOT THE LITERAL AsyncMockProducer.Clear() FORM (M11/P3.1 §6.3). Clear() drops the
        // core's completions without completing them, so the group the pump already holds becomes
        // permanently unresolvable — and then SendCompletionPump.Stop's UNBOUNDED _thread.Join()
        // never returns, whatever this wait does. That hang is pre-existing, belongs to Stop, and
        // M11/P3.2 §8.3's boundary table puts "Stop's own semantics" out of scope for this phase, so
        // asserting teardown-returns there would be asserting a fix this slice is not allowed to
        // make — the identical argument PublicProducerAccumulatorTeardownTests.
        // ClearWithRecordsInTheAccumulator_TheDrainStillCompletesPromptly already records. A Clear()
        // here would also strand the pump thread and leak the producer handle for the rest of the
        // run. What this test asserts instead is the property S4 actually owns: the wait itself is
        // bounded when the queue cannot drain. The expiry's fault-the-remainder outcome is unchanged
        // by S4 and is guarded by SendCompletionPumpDrainCapTests.
        // TeardownWithSeveralGroupsQueued_FaultsEverySendInEveryGroup.
        const int Total = 12;
        const int PerGroup = 4;
        TimeSpan bound = TimeSpan.FromMilliseconds(200);

        Harness harness = new Harness(
            () => new SendAccumulatorSettings(
                slotThreshold: 2000, batchWindowMs: 60_000, batchChunk: PerGroup),
            autoComplete: false);

        try
        {
            Task<RecordMetadata>[] sends = harness.Append(Total);
            harness.DrainNow();
            Assert.Equal(Total / PerGroup, harness.Accumulator.SendBatchCallCount);

            // Wait until the pump has actually TAKEN group 1 — DrainedSendCount counts records, so
            // it reaches PerGroup and then stops, because the pass is blocked inside get_all. This
            // makes "the queue still holds two groups" a precondition rather than a hope.
            Assert.True(
                SpinUntil(() => harness.DrainedSendCount >= PerGroup, s_deadline),
                "the pump never dequeued the first group, so the wait below would not be measured " +
                "against a stuck pump");
            Assert.Equal(PerGroup, harness.DrainedSendCount);

            bool drained = true;
            Stopwatch clock = Stopwatch.StartNew();
            TestTimeout.Run(() => drained = harness.WaitForPumpQueueDrain(bound), s_deadline);
            clock.Stop();

            Assert.False(
                drained,
                "the wait reported the queue empty although two groups were still queued behind a " +
                "pump parked in get_all");

            // Belt and braces alongside TestTimeout's hard deadline: the bound is what makes the
            // expiry a defined outcome rather than a hang.
            Assert.True(
                clock.Elapsed < TimeSpan.FromSeconds(10),
                $"the bounded wait took {clock.Elapsed} against a {bound} bound");

            // Nothing was drained by the WAIT itself — it observes the queue, it never consumes it
            // (the rejected "drain from the teardown thread" shape would show up here as a rising
            // count, and would have entered get_all on this thread).
            Assert.Equal(PerGroup, harness.DrainedSendCount);

            // Teardown still returns, and its own flush is what resolves the stuck group — so the
            // remainder is neither stranded nor left to the fault path in this (non-pathological)
            // case.
            TestTimeout.Run(harness.Dispose, s_deadline);

            foreach (Task<RecordMetadata> send in sends)
            {
                Assert.Equal(TaskStatus.RanToCompletion, send.Status);
            }

            Assert.Equal(Total, harness.DrainedSendCount);
        }
        finally
        {
            harness.Dispose();
        }
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or <paramref name="timeout"/> expires.
    /// </summary>
    private static bool SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        Stopwatch clock = Stopwatch.StartNew();
        SpinWait spinner = new SpinWait();
        while (clock.Elapsed < timeout)
        {
            if (condition())
            {
                return true;
            }

            spinner.SpinOnce();
        }

        return condition();
    }
}
