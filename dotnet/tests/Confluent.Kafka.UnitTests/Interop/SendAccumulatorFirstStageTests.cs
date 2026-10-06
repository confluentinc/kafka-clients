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
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;
using Xunit.Abstractions;

using Harness = Confluent.Kafka.UnitTests.Interop.SendAccumulatorTests.Harness;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.5 S3 — the <b>first stage</b> of the two-stage async send, at the accumulator seam:
/// <see cref="SendAccumulator.SubmitAdmitted"/> appends first and then takes an admission permit
/// inline (the fast path), through a plain pending wait (a saturated bound, no cancelable token), or
/// through a cancelable stage (a saturated bound and a cancelable token — D2 (c): the token releases
/// the CALLER, the record is still sent).
/// </summary>
/// <remarks>
/// <para>
/// Every test here holds the batch thread back with a 60 s window and drains it explicitly, so the
/// moment capacity frees is the test's choice rather than a timer's. Observation is through the
/// accumulator's own test witnesses (<see cref="SendAccumulator.AvailableAdmissions"/>,
/// <see cref="SendAccumulator.AdmittedRecordCount"/>, the send-batch counters) and the mock core's
/// record count — never through "the awaiter returned", which an un-throttled or un-bounded
/// implementation satisfies equally well.
/// </para>
/// <para>
/// The public-surface half (Flush, teardown, the delivery task's own cancellation) is in
/// <c>PublicProducerFirstStageTests</c>; the caller-token test this slice extended (T16) is
/// <c>SendAccumulatorTests.Admission_CallerTokenFiresWhileWaiting_EndsTheFirstStageCanceled_AndTheRecordIsStillSent</c>.
/// </para>
/// </remarks>
public sealed class SendAccumulatorFirstStageTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // "Promptly", not "eventually": the bound a first stage gets to settle once its outcome is decided.
    // Not a latency claim — the decided stage settles in microseconds — only room for a loaded runner,
    // well short of the hang guard so a stage that waits for something else fails here.
    private static readonly TimeSpan s_prompt = TimeSpan.FromSeconds(5);

    private readonly ITestOutputHelper _output;

    public SendAccumulatorFirstStageTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task Admission_PermitAccounting_ReturnsToExactlyTheBound_AfterAFullDrain_IncludingCallerTokenCancels()
    {
        // M11/P3.5 T4 — the permit invariant. Every appended record takes exactly one permit (inline,
        // or later through its wait) and every take returns one per record, so once the chain is
        // drained and every wait has completed the gate holds EXACTLY MaxAdmittedRecords again —
        // including when callers' tokens fired while their first stages were pending.
        //
        // What this catches that "every record arrived" cannot: a caller token that ends the WAIT
        // (D2 option (b), linking the token into WaitAsync — rejected) makes that wait take no permit,
        // so the drain's releases overshoot by one per cancel; a token callback that hands a permit
        // back overshoots the same way. Both are SILENT in production — the int.MaxValue ceiling no
        // longer throws on an over-release — and only this count sees them. Two rounds on one
        // accumulator, so a drift too small to see once compounds into a second miss.
        const int Bound = 4;
        const int TokenWaiters = 3;
        const int PlainWaiters = 2;
        const int PerRound = Bound + TokenWaiters + PlainWaiters;

        using Harness harness = new Harness(Settings(Bound));

        for (int round = 0; round < 2; round++)
        {
            List<Task<RecordMetadata>> deliveries = new List<Task<RecordMetadata>>();
            deliveries.AddRange(harness.Append(Bound));
            Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

            CancellationTokenSource[] cancellations = new CancellationTokenSource[TokenWaiters];
            Task<Task<RecordMetadata>>[] tokenStages = new Task<Task<RecordMetadata>>[TokenWaiters];
            for (int i = 0; i < TokenWaiters; i++)
            {
                cancellations[i] = new CancellationTokenSource();
                tokenStages[i] = harness
                    .AppendStaged(0x40, callback: null, cancellations[i].Token, out TaskCompletionSource<RecordMetadata> completion)
                    .AsTask();
                deliveries.Add(completion.Task);
            }

            Task<Task<RecordMetadata>>[] plainStages = new Task<Task<RecordMetadata>>[PlainWaiters];
            for (int i = 0; i < PlainWaiters; i++)
            {
                plainStages[i] = harness
                    .AppendStaged(0x41, callback: null, CancellationToken.None, out TaskCompletionSource<RecordMetadata> completion)
                    .AsTask();
                deliveries.Add(completion.Task);
            }

            foreach (Task<Task<RecordMetadata>> stage in tokenStages)
            {
                Assert.False(stage.IsCompleted, $"round {round}: a first stage completed although the bound was full");
            }

            foreach (Task<Task<RecordMetadata>> stage in plainStages)
            {
                Assert.False(stage.IsCompleted, $"round {round}: a first stage completed although the bound was full");
            }

            // The tokens fire while their stages are pending: each stage ends Canceled with its own
            // token, and the gate does not move — a cancel neither takes nor returns a permit.
            for (int i = 0; i < TokenWaiters; i++)
            {
                cancellations[i].Cancel();
                OperationCanceledException canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => TestTimeout.Run(() => tokenStages[i], s_prompt));
                Assert.Equal(cancellations[i].Token, canceled.CancellationToken);
            }

            Assert.Equal(0, harness.Accumulator.AvailableAdmissions);
            Assert.Equal(PerRound * round, harness.Accumulator.SendBatchRecordCount);

            harness.DrainNow();
            await TestTimeout.Run(() => Task.WhenAll(deliveries), s_deadline);
            await TestTimeout.Run(() => Task.WhenAll(plainStages), s_deadline);

            // THE ASSERTION: exactly the bound — not one permit more for each cancelled waiter.
            await WaitUntil(() => harness.Accumulator.AvailableAdmissions == Bound, s_prompt);
            Assert.Equal(Bound, harness.Accumulator.AvailableAdmissions);
            Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);
            Assert.Equal(PerRound * (round + 1), harness.HistoryCount);

            // ...and it STAYS there: no late release lands after the drain settled.
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.Equal(Bound, harness.Accumulator.AvailableAdmissions);

            foreach (CancellationTokenSource cancellation in cancellations)
            {
                cancellation.Dispose();
            }
        }
    }

    [Fact]
    public async Task Admission_NonAwaitingBurstAcrossASaturatedBound_IsSentInCallOrder()
    {
        // M11/P3.5 T7 — append-first is what makes submission order call order WITHOUT a FIFO
        // submission queue: a record's place in the chain is fixed by its own call, before anyone can
        // wait behind it. So a caller that fires a burst past the bound without awaiting the first
        // stage — and with the batch thread held back, so nothing drains mid-burst — has every record
        // in the chain, in call order, while most of its first stages are still pending.
        //
        // Pinned two ways: by REFERENCE on the pending node's completion slots (the chain order), and
        // by the mock core's offsets, which it assigns per partition in the order send_batch hands it
        // records (the send order). A wait-then-append implementation fails both: the over-bound
        // records are not in the chain at all until a permit frees, and then join it in whatever
        // order their continuations happen to run.
        const int Bound = 4;
        const int Burst = 12;

        using Harness harness = new Harness(Settings(Bound));

        TaskCompletionSource<RecordMetadata>[] created = new TaskCompletionSource<RecordMetadata>[Burst];
        Task<Task<RecordMetadata>>[] stages = new Task<Task<RecordMetadata>>[Burst];
        for (int i = 0; i < Burst; i++)
        {
            stages[i] = harness
                .AppendStaged((byte)i, callback: null, CancellationToken.None, out created[i])
                .AsTask();
        }

        int pending = 0;
        foreach (Task<Task<RecordMetadata>> stage in stages)
        {
            pending += stage.IsCompleted ? 0 : 1;
        }

        Assert.Equal(Burst - Bound, pending);
        Assert.Equal(Burst, harness.PendingCount());
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        TaskCompletionSource<RecordMetadata>?[] slots = harness.PendingCompletions();
        for (int i = 0; i < Burst; i++)
        {
            Assert.Same(created[i], slots[i]);
        }

        harness.DrainNow();

        for (int i = 0; i < Burst; i++)
        {
            RecordMetadata metadata = await TestTimeout.Run(() => created[i].Task, s_deadline);
            Assert.Equal(i, metadata.Offset);
        }

        await TestTimeout.Run(() => Task.WhenAll(stages), s_deadline);
        for (int i = 0; i < Burst; i++)
        {
            Assert.Same(created[i].Task, await stages[i]);
        }

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(Burst, harness.Accumulator.SendBatchRecordCount);
    }

    [Fact]
    public async Task Admission_CancelableFirstStage_IsCompletedSuccessfullyByStopsCancel_WhenTheBatchThreadCannotDrain()
    {
        // M11/P3.5 T9, the deterministic half (D3). Through the public producer (PublicProducerFirstStageTests)
        // teardown's gate cancel races the final drain's releases, and the releases usually win — so
        // there a pending first stage mostly ends by a permit, and a stage that mishandled a CANCELLED
        // wait is caught only when the cancel happens to land first. Here the batch thread is parked
        // inside a delivery callback, so no permit can come back: Stop's gate cancel is the only thing
        // that can end the wait, and the cancelable first stage must still complete SUCCESSFULLY, with
        // its delivery task — the record was appended before it waited. The plain (no-token) stage's
        // twin is SendAccumulatorTests.Admission_WaitingFirstStage_IsCompletedByStopsCancel_WhenTheBatchThreadCannotDrain,
        // whose parking recipe this follows.
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        using CancellationTokenSource live = new CancellationTokenSource();

        Harness harness = new Harness(Settings(2));

        Task<RecordMetadata>? held = null;
        Task<RecordMetadata>[] filled = Array.Empty<Task<RecordMetadata>>();
        Task<Task<RecordMetadata>>? stage = null;
        TaskCompletionSource<RecordMetadata>? completion = null;
        try
        {
            // The closed core fails the send, and the failure fires the callback on the batch thread.
            harness.CloseCoreProducer();
            held = harness.AppendOne(0x90, new SendAccumulatorTests.BlockingDeliveryCallback(entered, release));
            harness.ForceDrainWithoutWaiting();
            Assert.True(entered.Wait(s_deadline), "the batch thread never entered the delivery callback");

            filled = harness.Append(2);
            Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

            stage = harness
                .AppendStaged(0x91, callback: null, live.Token, out TaskCompletionSource<RecordMetadata> appended)
                .AsTask();
            completion = appended;
            Assert.False(stage.IsCompleted, "the first stage completed although the bound was full");

            bool drained = true;
            TestTimeout.Run(
                () => drained = harness.Accumulator.Stop(TimeSpan.FromSeconds(2)),
                TimeSpan.FromSeconds(20));
            Assert.False(drained, "the parked batch thread cannot have exited");

            // THE ASSERTION: ended by the cancelled gate alone, and successfully (D3) — never
            // Canceled (the caller's token did not fire), never pending.
            Task settled = await Task.WhenAny(stage, Task.Delay(s_prompt));
            Assert.Same(stage, settled);
            Assert.Equal(TaskStatus.RanToCompletion, stage.Status);
            Assert.Same(appended.Task, await stage);
            Assert.False(live.IsCancellationRequested);
        }
        finally
        {
            release.Set();
        }

        // Released, the batch thread settles the rest against the closed core, as in the twin.
        Assert.NotNull(held);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => held!, s_deadline));
        foreach (Task<RecordMetadata> send in filled)
        {
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, s_deadline));
        }

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => completion!.Task, s_deadline));
        harness.Dispose();
    }

    [Theory]
    [InlineData("slot")]
    [InlineData("teardown")]
    public async Task Admission_CancelableFirstStage_ReleasesItsTokenRegistration_OnceTheWaitCompletes(string outcome)
    {
        // M11/P3.5 T18 — a cancelable first stage registers on the CALLER's token, and a caller's
        // token is routinely long-lived (one per application, one per request loop). A registration
        // that outlives its wait roots the whole stage — the stage object, its task, the delivery
        // task it carries — for the token's lifetime: a per-send leak on exactly the path a
        // well-behaved caller takes. So the registration must go when the wait completes, whether a
        // permit came back ("slot") or teardown cancelled the gate ("teardown").
        //
        // THE WITNESS: reachability. Once the wait has completed and its continuation has run, the
        // only thing that can still reach the stage's task is the token's registration, so the task
        // becomes collectable if and only if the registration was released. The test holds only a
        // WeakReference to it (taken in a non-inlined helper so no local keeps it alive) and collects
        // until it dies, while the token itself stays alive and uncancelled throughout.
        //
        // LIMITS, stated so the test is not over-read:
        //   * It relies on CancellationTokenRegistration.Dispose dropping the callback's state from
        //     the source. The control pair below checks that on this runtime, both ways: an
        //     undisposed registration keeps its state alive (so a missing Dispose WOULD be visible),
        //     a disposed one does not (so the witness CAN go dead).
        //   * It cannot tell "released" from "never registered". A stage that never registered on
        //     the token passes here — and fails T16, whose token must end the stage.
        //   * "Once the wait completes" is graded with the collection loop's bound, so a release that
        //     happens late but inside it passes.
        //   * The token-wins outcome is not graded: CancellationTokenSource.Cancel() unregisters
        //     every callback itself, so a skipped Dispose there is unobservable by any means.
        using Harness harness = new Harness(Settings(1));
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        (WeakReference kept, WeakReference released) = RegisterControlPair(cancellation.Token);
        Assert.True(CollectUntilDead(released, TimeSpan.FromSeconds(10)), "a disposed registration's state was not collected — the witness cannot work on this runtime");
        Assert.True(kept.IsAlive, "an undisposed registration's state was collected — the witness cannot see a missing Dispose on this runtime");

        Task<RecordMetadata> filled = harness.AppendOne(0x50);

        TaskCompletionSource<TaskStatus> observed =
            new TaskCompletionSource<TaskStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        WeakReference stage = StartObservedStage(harness, cancellation.Token, observed);
        Assert.True(stage.IsAlive);
        Assert.False(observed.Task.IsCompleted, "the first stage completed although the bound was full");

        if (outcome == "slot")
        {
            harness.DrainNow();
        }
        else
        {
            TestTimeout.Run(
                () => Assert.True(harness.Accumulator.Stop(TimeSpan.FromSeconds(10)), "the batch thread did not exit"),
                TimeSpan.FromSeconds(20));
        }

        // Both outcomes complete the stage successfully (a slot, or D3's teardown), and the token
        // never fired.
        TaskStatus status = await TestTimeout.Run(() => observed.Task, s_deadline);
        Assert.Equal(TaskStatus.RanToCompletion, status);
        Assert.False(cancellation.IsCancellationRequested);

        // THE ASSERTION: nothing roots the stage any more, although the token is alive and was
        // never cancelled.
        Assert.True(
            CollectUntilDead(stage, TimeSpan.FromSeconds(10)),
            $"the first stage was still reachable after its wait completed ({outcome}) — its caller-token registration was not released");

        await TestTimeout.Run(() => filled, s_deadline);
        Assert.Equal(2, harness.HistoryCount);
        GC.KeepAlive(cancellation);
    }

    [Fact]
    public async Task Admission_TokenRacingAFreedSlot_SettlesTheFirstStageExactlyOnce_AndTheRecordIsStillSent()
    {
        // M11/P3.5 T19 — the token firing and a permit coming back are two independent completions
        // of one first stage. Exactly one may win, the loser must be a no-op (not a throw on either
        // side), and either way the record is sent. Both sides are released from one barrier on
        // dedicated threads, with the start of one side staggered per repetition so the race lands on
        // both sides of the decision rather than always the same one; each repetition is a fresh
        // accumulator.
        //
        // What it catches: a Set* where a TrySet* belongs. On the token side that throws out of the
        // caller's own Cancel() (captured and asserted); on the wait side it faults the stage's
        // continuation, which nothing observes — so the test also listens for an unobserved task
        // fault raised from the stage's code, after forcing the collection that surfaces it.
        const int Repetitions = 8;

        List<string> unobserved = new List<string>();
        EventHandler<UnobservedTaskExceptionEventArgs> listener = (_, e) =>
        {
            string text = e.Exception.ToString();
            if (text.Contains("CancellableFirstStage", StringComparison.Ordinal))
            {
                lock (unobserved)
                {
                    unobserved.Add(text);
                }
            }
        };

        int tokenWins = 0;
        int slotWins = 0;
        TaskScheduler.UnobservedTaskException += listener;
        try
        {
            for (int rep = 0; rep < Repetitions; rep++)
            {
                // Even repetitions start the canceller late, odd ones the drainer: 0..600 µs.
                long stagger = (rep / 2) * Stopwatch.Frequency / 5_000;
                long cancelDelay = rep % 2 == 0 ? stagger : 0;
                long drainDelay = rep % 2 == 0 ? 0 : stagger;

                using Harness harness = new Harness(Settings(1));
                using CancellationTokenSource cancellation = new CancellationTokenSource();

                Task<RecordMetadata> filled = harness.AppendOne(0x60);
                Task<Task<RecordMetadata>> stage = harness
                    .AppendStaged(0x61, callback: null, cancellation.Token, out TaskCompletionSource<RecordMetadata> completion)
                    .AsTask();
                Assert.False(stage.IsCompleted, $"rep {rep}: the first stage completed although the bound was full");

                using Barrier barrier = new Barrier(2);
                Exception? cancelFailure = null;
                Thread canceller = new Thread(() =>
                {
                    barrier.SignalAndWait(s_deadline);
                    SpinFor(cancelDelay);
                    try
                    {
                        cancellation.Cancel();
                    }
                    catch (Exception e)
                    {
                        cancelFailure = e;
                    }
                })
                { IsBackground = true, Name = "first-stage-race-canceller" };
                Thread drainer = new Thread(() =>
                {
                    barrier.SignalAndWait(s_deadline);
                    SpinFor(drainDelay);
                    harness.ForceDrainWithoutWaiting();
                })
                { IsBackground = true, Name = "first-stage-race-drainer" };

                canceller.Start();
                drainer.Start();
                Assert.True(canceller.Join(s_deadline), $"rep {rep}: the canceller thread did not finish");
                Assert.True(drainer.Join(s_deadline), $"rep {rep}: the drainer thread did not finish");

                // The losing side was a no-op: the caller's Cancel() did not throw.
                Assert.Null(cancelFailure);

                // Exactly one outcome, promptly — Canceled with the caller's token, or the delivery task.
                Task settled = await Task.WhenAny(stage, Task.Delay(s_prompt));
                Assert.Same(stage, settled);
                if (stage.Status == TaskStatus.Canceled)
                {
                    tokenWins++;
                    OperationCanceledException canceled =
                        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stage);
                    Assert.Equal(cancellation.Token, canceled.CancellationToken);
                }
                else
                {
                    Assert.Equal(TaskStatus.RanToCompletion, stage.Status);
                    slotWins++;
                    Assert.Same(completion.Task, await stage);
                }

                // And the record is sent whichever side won, and the gate is back at rest.
                await TestTimeout.Run(() => Task.WhenAll(filled, completion.Task), s_deadline);
                Assert.Equal(2, harness.HistoryCount);
                Assert.Equal(2, harness.Accumulator.SendBatchRecordCount);
                await WaitUntil(() => harness.Accumulator.AvailableAdmissions == 1, s_prompt);
                Assert.Equal(1, harness.Accumulator.AvailableAdmissions);
            }

            for (int i = 0; i < 2; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= listener;
        }

        _output.WriteLine($"token won {tokenWins}, slot won {slotWins} of {Repetitions}");
        Assert.Equal(Repetitions, tokenWins + slotWins);
        lock (unobserved)
        {
            Assert.True(unobserved.Count == 0, "a first-stage continuation faulted unobserved:\n" + string.Join("\n", unobserved));
        }
    }

    [Fact]
    public async Task Admission_CallerTokenCancel_DoesNotResumeTheAwaiterOnTheCancellingThread()
    {
        // M11/P3.5 T20 (D5) — a first stage must never resume its awaiter inline on whoever completed
        // it. Here that is the caller's own Cancel(): the token callback settles the stage on the
        // cancelling thread, so an awaiter continuation run synchronously there would execute caller
        // code inside someone else's Cancel() — a reentrancy hazard, and a stall for every other
        // callback that Cancel() still has to run. The stage's completion source therefore runs
        // continuations asynchronously.
        //
        // The awaiter uses ConfigureAwait(false) so xUnit's synchronization context cannot hide an
        // inline resume by posting it; the canceller is a dedicated thread kept alive until the
        // awaiter has run, and the comparison is on the Thread object, so a recycled thread id cannot
        // make two threads look like one.
        using Harness harness = new Harness(Settings(1));
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Task<RecordMetadata> filled = harness.AppendOne(0x70);
        Task<Task<RecordMetadata>> stage = harness
            .AppendStaged(0x71, callback: null, cancellation.Token, out TaskCompletionSource<RecordMetadata> completion)
            .AsTask();
        Assert.False(stage.IsCompleted, "the first stage completed although the bound was full");

        Task<Thread> resumed = ResumeThreadOf(stage);
        Assert.False(resumed.IsCompleted);

        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        Exception? cancelFailure = null;
        Thread canceller = new Thread(() =>
        {
            try
            {
                cancellation.Cancel();
            }
            catch (Exception e)
            {
                cancelFailure = e;
            }

            _ = release.Wait(s_deadline);
        })
        { IsBackground = true, Name = "first-stage-canceller" };
        canceller.Start();

        Thread resumedOn = await TestTimeout.Run(() => resumed, s_prompt);
        release.Set();
        Assert.True(canceller.Join(s_deadline), "the canceller thread did not finish");

        Assert.Null(cancelFailure);
        Assert.Equal(TaskStatus.Canceled, stage.Status);
        Assert.NotSame(canceller, resumedOn);
        Assert.NotEqual(canceller.ManagedThreadId, resumedOn.ManagedThreadId);

        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(filled, completion.Task), s_deadline);
        Assert.Equal(2, harness.HistoryCount);
    }

#if NET8_0_OR_GREATER
    // GC.GetAllocatedBytesForCurrentThread has no net462 equivalent (net462 is build-verified only),
    // the PublicProducerSendAllocationBudgetTests precedent.

    private const int AllocationSendCount = 64;

    private const int AllocationAttempts = 4;

    private const int PrefilledRecords = 130;

    // Regression CEILINGS for the two saturated paths' own cost over the fast path, per send:
    // the larger of the net8.0 / net10.0 measurements (+680 / +688 B cancelable, +536 / +544 B
    // plain; the fast path itself is 128 B on both — the record's bytes, its completion source and
    // that source's task) plus 32 B, which is less than one more Task<T> (72 B). So a new
    // Task-sized allocation on either saturated path turns them red; the 8 B TFM difference does not.
    private const long CancelableStageMarginalCeilingBytes = 720;

    private const long PlainWaitMarginalCeilingBytes = 576;

    [Fact]
    public async Task Admission_SaturatedFirstStage_PerSendAllocation_IsMeasured_AndStaysUnderItsCeiling()
    {
        // M11/P3.5 T21 (i) — what a first stage costs when the bound is saturated, on the caller
        // thread. DEFINITION: caller-thread bytes per SubmitAdmitted call (the harness's AppendStaged:
        // one record, one completion, no callback) over 64 calls, best of 4 fresh accumulators, MINUS
        // the same on the fast path with the same live, already-registered-once token — so the
        // record, its completion and the append cancel out and what remains is the saturated path's
        // own cost: the admission wait, its continuation, and (for a cancelable token) the stage, its
        // completion source and the token registration. The token is warmed once per accumulator
        // outside the measured window, so the source's one-time registration table is not charged.
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        long fast = long.MaxValue;
        long cancelable = long.MaxValue;
        long plain = long.MaxValue;
        for (int attempt = 0; attempt < AllocationAttempts; attempt++)
        {
            fast = Math.Min(fast, await MeasurePerSend(maxAdmitted: 4096, cancellation.Token));
            cancelable = Math.Min(cancelable, await MeasurePerSend(maxAdmitted: 1, cancellation.Token));
            plain = Math.Min(plain, await MeasurePerSend(maxAdmitted: 1, CancellationToken.None));
        }

        long cancelableMarginal = cancelable - fast;
        long plainMarginal = plain - fast;
        string figures =
            $"per send: fast path {fast} B, saturated + cancelable token {cancelable} B (+{cancelableMarginal} B), " +
            $"saturated without a token {plain} B (+{plainMarginal} B)";
        _output.WriteLine(figures);

        Assert.True(
            cancelableMarginal <= CancelableStageMarginalCeilingBytes,
            $"the cancelable first stage's own cost exceeded its ceiling {CancelableStageMarginalCeilingBytes} B — {figures}");
        Assert.True(
            plainMarginal <= PlainWaitMarginalCeilingBytes,
            $"the plain saturated wait's own cost exceeded its ceiling {PlainWaitMarginalCeilingBytes} B — {figures}");
    }

    private static async Task<long> MeasurePerSend(int maxAdmitted, CancellationToken token)
    {
        using Harness harness = new Harness(Settings(maxAdmitted));

        // Outside the window: the bound (filled when it is 1), the node grown past the window (it
        // starts at 16 slots and doubles, so 130 records leave it at 256 and the 1 + 64 below never
        // grow it — otherwise the window would be charged the amortized growth), and one call down the
        // measured path to warm it and the token's registration table.
        Task<RecordMetadata>[] filled = harness.Append(PrefilledRecords);
        Task<Task<RecordMetadata>> warm = harness.AppendStaged(0x81, callback: null, token, out _).AsTask();
        ValueTask<Task<RecordMetadata>>[] stages = new ValueTask<Task<RecordMetadata>>[AllocationSendCount];

        long bytes = MeasureAppends(harness, token, stages);

        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
        await TestTimeout.Run(() => warm, s_deadline);
        foreach (ValueTask<Task<RecordMetadata>> stage in stages)
        {
            Task<RecordMetadata> delivery = await TestTimeout.Run(() => stage.AsTask(), s_deadline);
            await TestTimeout.Run(() => delivery, s_deadline);
        }

        return bytes / AllocationSendCount;
    }

    private static long MeasureAppends(Harness harness, CancellationToken token, ValueTask<Task<RecordMetadata>>[] stages)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < stages.Length; i++)
        {
            stages[i] = harness.AppendStaged(0x82, callback: null, token, out _);
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }
#endif

    private static SendAccumulatorSettings Settings(int maxAdmitted) =>
        new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: maxAdmitted);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Kept, WeakReference Released) RegisterControlPair(CancellationToken token)
    {
        object kept = new object();
        object released = new object();
        _ = token.Register(static _ => { }, kept);
        CancellationTokenRegistration registration = token.Register(static _ => { }, released);
        registration.Dispose();
        return (new WeakReference(kept), new WeakReference(released));
    }

    /// <summary>
    /// Starts a cancelable first stage on a saturated bound and hands back only a weak reference to
    /// it, plus its outcome through <paramref name="observed"/> — a completion source the stage's
    /// continuation completes with a status value, so nothing the test holds reaches the stage.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference StartObservedStage(
        Harness harness, CancellationToken token, TaskCompletionSource<TaskStatus> observed)
    {
        Task<Task<RecordMetadata>> stage = harness.AppendStaged(0x51, callback: null, token, out _).AsTask();
        _ = stage.ContinueWith(
            static (completed, state) => ((TaskCompletionSource<TaskStatus>)state!).TrySetResult(completed.Status),
            observed,
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
        return new WeakReference(stage);
    }

    private static bool CollectUntilDead(WeakReference reference, TimeSpan timeout)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (true)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            if (!reference.IsAlive)
            {
                return true;
            }

            if (watch.Elapsed > timeout)
            {
                return false;
            }

            Thread.Sleep(20);
        }
    }

    private static async Task<Thread> ResumeThreadOf(Task<Task<RecordMetadata>> stage)
    {
        try
        {
            _ = await stage.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The outcome under test; the thread this resumed on is the observation.
        }

        return Thread.CurrentThread;
    }

    private static void SpinFor(long stopwatchTicks)
    {
        long until = Stopwatch.GetTimestamp() + stopwatchTicks;
        while (Stopwatch.GetTimestamp() < until)
        {
            Thread.SpinWait(20);
        }
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (!condition() && watch.Elapsed < timeout)
        {
            await Task.Delay(10);
        }
    }
}
