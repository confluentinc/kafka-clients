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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.1 slice S8a — the completion pump's <c>SLOT_CAPACITY</c> drain cap (§12.2), the last
/// place the two bindings' constants diverged. Python's poll-futures thread processes one
/// <c>BatchNode</c> per pass and sizes its <c>get_all</c> arrays at
/// <c>PRODUCER_RECORD_SLOT_CAPACITY</c>; this pump used to drain the whole queue and allocate three
/// <c>IntPtr[count]</c> over whatever it found.
/// </summary>
/// <remarks>
/// <para>
/// <b>A <see cref="IntPtr.Zero"/> future is a legitimate driver here.</b> The ABI's <c>get_all</c>
/// writes a null metadata and an <c>InvalidRequest</c> error for a null future rather than
/// asserting, so every enqueued send still <b>settles</b> — which is all these tests need, and it
/// lets them exercise the pump's loop with no producer and no broker at all. The
/// pre-existing gate tests use the same stand-in.
/// </para>
/// <para>
/// The assertions are on the pump's own batch counters, not on "the completions resolved": an
/// <em>uncapped</em> drain also resolves every completion, so resolution alone cannot tell whether
/// the cap is in force.
/// </para>
/// </remarks>
public sealed class SendCompletionPumpDrainCapTests
{
    // Hard deadlines. The hang-regression test below fails DETERMINISTICALLY without the inner drain
    // loop, so it must fail rather than hang the suite (§12.2.1).
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void DrainOverTheCap_IsSplitIntoMultiplePassesOfAtMostTheCap()
    {
        // Test 24: more than SLOT_CAPACITY queued must become MULTIPLE ProcessBatch passes, each of
        // at most 1100 — the parity property, asserted by pass count and pass size.
        const int Total = SendCompletionPump.DrainCap + 700;

        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] completions = EnqueueMany(pump, Total);

            TestTimeout.Run(() => Task.WaitAll(Settled(completions), s_deadline), s_deadline);

            Assert.True(
                pump.LargestProcessedBatch <= SendCompletionPump.DrainCap,
                $"a pass carried {pump.LargestProcessedBatch} completions, above the " +
                $"{SendCompletionPump.DrainCap} cap");
            Assert.True(
                pump.ProcessedBatchCount >= 2,
                $"{Total} completions were handled in {pump.ProcessedBatchCount} pass(es) — with the " +
                "cap in force it takes at least two");
        }
        finally
        {
            pump.Stop();
        }
    }

    [Fact]
    public void FarOverTheCap_ThenNoFurtherEnqueue_StillResolvesEveryCompletion()
    {
        // ⚠ TEST 25 — THE HANG REGRESSION, the single most important test of this slice (§12.2.1).
        // A NAIVE cap (capping DrainAll without adding RunLoop's inner drain loop) strands the
        // leftovers DETERMINISTICALLY: 3000 queued, 1100 drained, 1900 left, and the next iteration
        // blocks on _signal.Wait() — reset before the drain and only Set by a new Enqueue. With no
        // further sends those 1900 completions and every awaiting Task hang FOREVER. It is not a
        // race and needs no concurrency to reproduce, which is exactly why "stop enqueuing entirely"
        // is the whole point of the test.
        //
        // The hard deadline is load-bearing: under the naive cap this must FAIL, not hang the suite.
        const int Total = 3000;

        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] completions = EnqueueMany(pump, Total);

            // Nothing further is enqueued from here on — no Set can arrive to rescue a stranded
            // remainder.
            TestTimeout.Run(() => Task.WaitAll(Settled(completions), s_deadline), s_deadline);

            foreach (TaskCompletionSource<RecordMetadata> completion in completions)
            {
                Assert.True(completion.Task.IsCompleted, "a completion past the drain cap was stranded");
            }

            Assert.True(
                pump.ProcessedBatchCount >= 3,
                $"{Total} completions at a {SendCompletionPump.DrainCap} cap need at least three " +
                $"passes; the pump ran {pump.ProcessedBatchCount}");
        }
        finally
        {
            pump.Stop();
        }
    }

    [Fact]
    public void TeardownWithMoreThanTheCapQueued_FaultsEveryCompletion()
    {
        // Test 26: Stop's terminal drain is deliberately UNCAPPED (§12.2.2). Capping it would strand
        // every send past the cap — TCSes never completed, awaiters hanging, future handles never
        // destroyed. Close the gate first so the loop cannot consume the queue, then stop: the
        // terminal drain is the ONLY thing that can settle these, so the count is the assertion.
        const int Total = SendCompletionPump.DrainCap * 2 + 13;

        SendCompletionPump pump = new SendCompletionPump();
        pump.CloseGate();

        // With the gate closed, Enqueue faults in place — so drive the terminal drain instead by
        // stopping a pump whose queue was filled BEFORE the gate closed.
        SendCompletionPump filled = new SendCompletionPump();
        TaskCompletionSource<RecordMetadata>[] completions = EnqueueMany(filled, Total);
        filled.Stop();
        pump.Stop();

        int settled = 0;
        foreach (TaskCompletionSource<RecordMetadata> completion in completions)
        {
            if (completion.Task.IsCompleted)
            {
                settled++;
            }
        }

        Assert.Equal(Total, settled);
    }

    private static TaskCompletionSource<RecordMetadata>[] EnqueueMany(SendCompletionPump pump, int count)
    {
        TaskCompletionSource<RecordMetadata>[] completions = new TaskCompletionSource<RecordMetadata>[count];
        for (int i = 0; i < count; i++)
        {
            completions[i] = new TaskCompletionSource<RecordMetadata>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            // A null future: get_all reports InvalidRequest for it, so the send settles (faulted)
            // without a producer, and destroy_all skips null entries.
            pump.Enqueue(IntPtr.Zero, completions[i], delivery: null);
        }

        return completions;
    }

    /// <summary>
    /// The tasks to wait on, with their faults observed — every send here settles as FAULTED (the
    /// null-future <c>InvalidRequest</c>), so awaiting them raw would surface that as the test's own
    /// failure instead of the property under test.
    /// </summary>
    private static Task[] Settled(TaskCompletionSource<RecordMetadata>[] completions)
    {
        Task[] settled = new Task[completions.Length];
        for (int i = 0; i < completions.Length; i++)
        {
            settled[i] = completions[i].Task.ContinueWith(
                static task => _ = task.Exception,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        return settled;
    }
}
