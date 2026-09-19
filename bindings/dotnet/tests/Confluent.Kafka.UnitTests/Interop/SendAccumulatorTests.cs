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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.1 slice S3 — the concurrency core: the node chain, the free-running window, the
/// threshold wake, and the chunking rule (§3.3/§3.4). Driven against a real
/// <see cref="SendAccumulator"/> over a broker-free <c>MockProducer</c>, with explicit settings
/// rather than environment overrides, so the timing assertions are deterministic and no test
/// mutates process-wide state that a concurrently-running test class could observe.
/// </summary>
/// <remarks>
/// <b>Every timing assertion is a BOUND, never an expected value</b> (§3.3). The window is
/// free-running — the batch thread takes its deadline from its own clock at the top of each
/// iteration, unrelated to when a record arrived — so a sub-threshold batch waits
/// <c>0..window</c> <em>uniformly</em>. A test that asserted "about one window" would be asserting
/// a property the design deliberately does not have.
/// </remarks>
public sealed class SendAccumulatorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "accumulator-topic";

    // ------------------------------------------------------------------ timing / batching -----

    [Fact]
    public async Task SubThresholdBatch_IsReleasedByTheWindow()
    {
        // Well below the threshold, so the early wake never fires and only the free-running window
        // can release these records. The bound is generous on purpose: the delay is uniform over
        // 0..window, and the assertion that matters is "the timer releases it at all".
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 20, batchChunk: 1100));

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<RecordMetadata>[] sends = harness.Append(5);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        elapsed.Stop();

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(5, harness.Accumulator.SendBatchRecordCount);
        Assert.True(
            elapsed.Elapsed < TimeSpan.FromSeconds(5),
            $"a sub-threshold batch took {elapsed.Elapsed} to be released by the {20} ms window");
    }

    [Fact]
    public async Task ThresholdBatch_IsReleasedByTheSignal_WellInsideTheWindow()
    {
        // A window far longer than the test's patience, so ONLY the threshold wake can release
        // these records. If the early signal (anchor :825-827) were missing, this would wait out
        // the 60 s window and blow the assertion below.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 8, batchWindowMs: 60_000, batchChunk: 1100));

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<RecordMetadata>[] sends = harness.Append(8);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        elapsed.Stop();

        Assert.True(
            elapsed.Elapsed < TimeSpan.FromSeconds(10),
            $"an at-threshold batch took {elapsed.Elapsed} — the early wake did not fire, so the " +
            "60 s window released it instead");
    }

    [Fact]
    public void WithoutADrain_TheRecordsHaveNotReachedTheCore()
    {
        // The CONTROL for PublicProducerFlushDrainTests.Flush_DrainsAccumulatorRecords_…: without
        // it, "history is 16 after Flush" could just mean the window happened to elapse. It lives
        // HERE rather than beside the test it controls for because the public producer's window
        // comes from the environment (10 ms) and is free-running (§3.3), so the public form raced
        // its own 16 sends — see the note in that file. An explicit 60 s window makes both halves
        // deterministic: nothing reaches the core until a drain, and the drain is what moves it.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(16);

        Assert.Equal(0, harness.HistoryCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        harness.DrainNow();

        Assert.Equal(16, harness.HistoryCount);
        Assert.Equal(16, sends.Length);
    }

    // ------------------------------------------------------------------------- chunking (§3.4) -

    [Fact]
    public void DefaultChunk_OneNode_IsExactlyOneSendBatchCall()
    {
        // The default chunk IS node capacity, so ceil(count / chunk) is always 1 for a single node —
        // identical to the anchor, which issues one call per node. Asserted by CALL COUNT, not by
        // "the records arrived", which any batching whatsoever would satisfy.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(50);
        harness.DrainNow();

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(50, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(50, harness.Accumulator.LargestSendBatchCount);
        Assert.Equal(50, sends.Length);
    }

    [Fact]
    public void LoweredChunk_SplitsANodeIntoCeilCountOverChunkCalls()
    {
        // The ONLY axis that can reach the splitting branch: at the defaults the chunk equals node
        // capacity, so a node can never exceed it (§3.4 — "dead at the default BY CONSTRUCTION, and
        // load-bearing under an override"). 50 records at chunk 8 => ceil(50/8) == 7 calls, the last
        // one short.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 8));

        _ = harness.Append(50);
        harness.DrainNow();

        Assert.Equal(7, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(50, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(8, harness.Accumulator.LargestSendBatchCount);
    }

    [Fact]
    public async Task DeepChain_NeverExceedsOneChunkPerCall_AndSendsEveryRecord()
    {
        // A chain deep enough to span several nodes (capacity is threshold + 100 = 104 here) under
        // real batch-thread concurrency. Node formation is inherently load-dependent — a node can
        // only fill past the threshold if records arrive faster than the thread drains — so the
        // deterministic assertion is the INVARIANT §3.4 exists to guarantee: no single send_batch
        // ever carries more than one chunk, i.e. the loop is never "simplified" into a single
        // flattened call over the whole chain.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 4, batchWindowMs: 10, batchChunk: 104));

        Task<RecordMetadata>[] sends = harness.Append(500);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        harness.DrainNow();

        Assert.Equal(500, harness.Accumulator.SendBatchRecordCount);
        Assert.True(
            harness.Accumulator.LargestSendBatchCount <= 104,
            $"a send_batch carried {harness.Accumulator.LargestSendBatchCount} records, above the " +
            "104-record chunk — the per-chunk loop is what bounds how long one call holds the core's " +
            "coarse producer mutex (§3.4)");
        Assert.True(harness.Accumulator.SendBatchCallCount >= 5, "500 records at chunk 104 need at least 5 calls");
    }

    [Fact]
    public void ChunkLargerThanANode_IsClampedToCapacity()
    {
        // A chunk cannot span two nodes, so an over-large override is indistinguishable from a full
        // node; clamping keeps the ceil(count / chunk) formula honest rather than letting the
        // override read as a different mode.
        SendAccumulatorSettings settings = new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 10, batchChunk: 999_999);

        Assert.Equal(1100, settings.SlotCapacity);
        Assert.Equal(1100, settings.BatchChunk);
    }

    // ------------------------------------------------------- pin lifetime across the deferral --

    [Fact]
    public async Task ForcedGcBetweenAppendAndDrain_DoesNotCorruptAnyRecord()
    {
        // The whole point of pinning: the record's buffers are borrowed across a REAL deferral now,
        // so a GC between Send and the drain must not move them. The window is long enough that the
        // collection provably lands inside it.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(32);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        foreach (Task<RecordMetadata> send in sends)
        {
            RecordMetadata metadata = await send;
            Assert.Equal(Topic, metadata.Topic);
        }
    }

    // ---------------------------------------------------------------- settings (§3.2, test 4) --

    [Fact]
    public void Settings_Defaults_ArePythonsValues()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>());

        Assert.Equal(1000, settings.SlotThreshold);      // PRODUCER_RECORD_SLOT_THRESHOLD
        Assert.Equal(1100, settings.SlotCapacity);       // (THRESHOLD + 100)
        Assert.Equal(10, settings.BatchWindowMs);        // the bare 10 ms literal
        Assert.Equal(1100, settings.BatchChunk);         // Python's effective per-call maximum
    }

    [Fact]
    public void Settings_EnvironmentOverrides_TakeEffect()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "40",
            [SendAccumulatorSettings.WindowVariable] = "3",
            [SendAccumulatorSettings.ChunkVariable] = "9",
        });

        Assert.Equal(40, settings.SlotThreshold);
        Assert.Equal(140, settings.SlotCapacity);   // still threshold + 100
        Assert.Equal(3, settings.BatchWindowMs);
        Assert.Equal(9, settings.BatchChunk);
    }

    [Fact]
    public void Settings_ChunkDefaultsToTheEffectiveCapacity_NotTheConstant()
    {
        // The chunk default is derived from the EFFECTIVE threshold, so lowering only the threshold
        // must move it too — otherwise an over-large chunk would silently span what is now a
        // smaller node.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "25",
        });

        Assert.Equal(25, settings.SlotThreshold);
        Assert.Equal(125, settings.BatchChunk);   // and the chunk follows capacity
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-5")]
    public void Settings_InvalidOverride_FallsBackToTheDefault(string raw)
    {
        // An operator escape hatch must not be able to fail producer construction — and a ZERO
        // window in particular would turn the batch thread's wait loop into a spin loop.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = raw,
            [SendAccumulatorSettings.WindowVariable] = raw,
            [SendAccumulatorSettings.ChunkVariable] = raw,
        });

        Assert.Equal(1000, settings.SlotThreshold);
        Assert.Equal(10, settings.BatchWindowMs);
        Assert.Equal(1100, settings.BatchChunk);
    }

    [Fact]
    public void Settings_AreReadOnceAtConstruction_NotPerSend()
    {
        // "Read once at construction" is the recorded contract (§3.2): a later change to the
        // environment must not reach a producer that is already running.
        SendAccumulator accumulator;
        using Harness harness = new Harness(
            () => ReadSettingsWith(new Dictionary<string, string?>
            {
                [SendAccumulatorSettings.WindowVariable] = "37",
            }));

        accumulator = harness.Accumulator;
        Assert.Equal(37, accumulator.Settings.BatchWindowMs);

        string? previous = Environment.GetEnvironmentVariable(SendAccumulatorSettings.WindowVariable);
        Environment.SetEnvironmentVariable(SendAccumulatorSettings.WindowVariable, "12345");
        try
        {
            Assert.Equal(37, accumulator.Settings.BatchWindowMs);
        }
        finally
        {
            Environment.SetEnvironmentVariable(SendAccumulatorSettings.WindowVariable, previous);
        }
    }

    // ------------------------------------------------------------------- teardown (minimal) ----

    [Fact]
    public void Stop_DrainsTheAccumulatorBeforeExiting_NoRecordIsAbandoned()
    {
        // §3.8 steps 3–4: closing the accumulator must send what it already holds, not drop it. With
        // a 60 s window nothing can have been drained by the timer, so every one of these records is
        // still in the chain when Stop() runs — and every send must SETTLE (none left pending, which
        // is the shape that hangs an awaiting caller forever).
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(20);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        harness.Dispose();

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(20, harness.Accumulator.SendBatchRecordCount);
        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.True(send.IsCompleted, "a record held by the accumulator at teardown was abandoned");
        }
    }

    [Fact]
    public void AppendAfterStop_ThrowsSynchronously_AndTakesNoAdmissionPermit()
    {
        // Under append-first the append is the FIRST thing SubmitAdmitted does, so a send racing
        // teardown is refused by Append's `_closed` check and the ObjectDisposedException propagates
        // — synchronously, before the caller has been handed the record's awaiter. That is the
        // pre-existing disposed-producer contract (ffi §A5: precondition throws stay synchronous),
        // and since M11/P3.4 it is the ONLY refusal the send path has.
        //
        // The permit count is the second assertion and is not redundant: the throw happens BEFORE
        // the admission wait, so a refused append must neither take a permit nor hand one back. A
        // leak either way shrinks (or inflates) the bound permanently, and no behavioural witness
        // could see it — teardown has cancelled the gate, so a later caller reports "closed" either
        // way. It is also the ONE assertion that would catch an append charging itself twice.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 10,
            batchChunk: 1100,
            maxAdmittedRecords: 4));
        harness.Dispose();

        Assert.Equal(4, harness.Accumulator.AvailableAdmissions);

        // Func<object>, not a lambda returning the Task directly: the xUnit analyzer reads
        // `() => AppendOne(..)` as an async assertion and rejects it.
        for (int i = 0; i < 4; i++)
        {
            byte tag = (byte)(0xA0 + i);
            Func<object> refused = () => harness.AppendOne(tag);
            Assert.Throws<ObjectDisposedException>(refused);
        }

        Assert.Equal(4, harness.Accumulator.AvailableAdmissions);
        Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);
    }

    [Fact]
    public async Task StopWithAnExpiredWait_ReportsNotDrained_AndTheAbandonedThreadStillSettlesEveryRecord()
    {
        // §6.3: "the accumulator always reaches empty" is a PREMISE, not a fact — the batch thread
        // can be parked inside send_batch on a full buffer.memory for up to max.block.ms. So the wait
        // is bounded and its expiry has a defined outcome, which this drives with a zero timeout: the
        // thread is alive and waiting, so the join cannot succeed.
        //
        // The defined outcome is NOT "fault everything from the teardown thread" — that would mean
        // releasing pins the core may still be reading. It is "abandon it and let it finish", and the
        // assertion is that every record still settles.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 10, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(8);

        Assert.False(
            harness.Accumulator.Stop(TimeSpan.Zero),
            "a zero-timeout Stop must report the batch thread as NOT drained");

        // The abandoned thread drains once more and exits on its own (it saw _closed), so the records
        // settle — later, but they settle. That is the whole content of "defined outcome".
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        // And the teardown caller must NOT free the interned topic buffers on this path: they are
        // still what the abandoned thread's records point at. The harness applies the same rule
        // production does, so disposing it here is safe only because the thread has now finished.
        harness.Dispose();
    }

    // ------------------------------------- immediate errors: the phase's NEW firing site (§6.1) -

    [Fact]
    public async Task ImmediateError_FiresTheDeliveryCallbackExactlyOnce_AndFaultsThatSend()
    {
        // PLAN §6.1: "This phase adds ONE new firing site: the immediate-error compaction on the
        // batch thread (§4.5). It must fire exactly once for those indices, and those indices must
        // not also reach the pump." Plan tests 13 and 21. The site is CompleteNode's error branch,
        // and it had no coverage: PublicProducerDeliveryCallbackTests covers only the pump's and
        // the sync send's sites, and ProducerSendBatchTests proves the ABI's per-index (future,
        // error) contract without ever reaching CompleteNode.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        harness.CloseCoreProducer();

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata>[] sends = harness.Append(3, callback);
        harness.DrainNow();

        foreach (Task<RecordMetadata> send in sends)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => send, s_deadline));

            // The core's own per-record rejection, surfaced unchanged — asserted by CONTENT
            // (DoD §3), so this cannot pass on some other failure that also faults the send.
            Assert.Equal("MockProducer is already closed.", failure.Message);
        }

        // Exactly once per record, after a settle window: a first observation of 3 cannot rule out
        // a fourth invocation arriving late (ffi §A6 form C's exactly-once obligation).
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(3, callback.Count);

        // Java's -1 placeholder metadata, never null (D2/D6), carrying the record's own topic and
        // explicit partition.
        Assert.NotNull(callback.LastMetadata);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
        Assert.Equal(0, callback.LastMetadata.Partition);
        Assert.Equal(-1L, callback.LastMetadata.Offset);
        Assert.NotNull(callback.LastException);
        Assert.Equal("MockProducer is already closed.", callback.LastException!.Message);
    }

    [Fact]
    public async Task ImmediateError_TheFailedIndicesNeverReachThePump_ButTheSurvivorsDo()
    {
        // The compaction half of plan test 13. "Settle in place" must mean the errored index is
        // handled ENTIRELY on the batch thread — its future destroyed, its callback fired, its
        // awaiter faulted — and never also handed to the pump, which would fire the callback a
        // second time. A duplicate is strictly worse than a drop under the exactly-once obligation
        // (root CLAUDE.md §9.5).
        //
        // A mixed batch is not reachable broker-free: the injection closes the core, so from that
        // point EVERY record is rejected. The survivors are therefore an earlier batch, which is
        // enough to pin both halves — DrainedSendCount must equal the survivors and must not move
        // when the rejected batch drains.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        RecordingDeliveryCallback survivors = new RecordingDeliveryCallback();
        Task<RecordMetadata>[] accepted = harness.Append(4, survivors);
        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(accepted), s_deadline);

        Assert.Equal(4, harness.DrainedSendCount);
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(4, survivors.Count);

        harness.CloseCoreProducer();

        RecordingDeliveryCallback rejected = new RecordingDeliveryCallback();
        Task<RecordMetadata>[] failed = harness.Append(5, rejected);
        harness.DrainNow();
        foreach (Task<RecordMetadata> send in failed)
        {
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, s_deadline));
        }

        await Task.Delay(TimeSpan.FromMilliseconds(250));

        // The five rejected indices were settled on the batch thread and nowhere else.
        Assert.Equal(5, rejected.Count);
        Assert.Equal(4, harness.DrainedSendCount);
        Assert.Equal(4, survivors.Count);
    }

    // ------------------------------------------------- the batch thread's own failure path -----

    [Fact]
    public async Task BatchThreadFailure_SettlesTheChainItHadALREADYTaken_NotJustTheAccumulators()
    {
        // AbandonOnThreadFailure (RunLoop's catch) closes the accumulator, settles a chain, releases
        // permits, releases pins and fires delivery callbacks. The gap it exists to close: the
        // handler took the ACCUMULATOR's chain (_head) while the failure it handles can land after
        // the batch thread has already taken one, so every record in the taken chain was stranded
        // forever (its awaiter never completed, its pins never released, its futures never
        // destroyed) while Stop still reported the thread as exited and teardown then freed the
        // interned topic buffers those un-released pins still pointed at.
        //
        // ⚠ THE INJECTION CHANGED WITH THE WINDOW IT TARGETS (M11/P3.4). Reaching the gap needs a
        // throw BETWEEN the take and the send, and exactly one statement sits there now:
        // ReleaseAdmission. The predecessor injection (appending without a _space permit, so the
        // batch thread's ReleaseSpace over-released) died with that bound, so
        // InflateTheChainAccounting takes its place — the take hands ReleaseAdmission a count the
        // semaphore cannot absorb and it throws SemaphoreFullException with the taken chain held
        // only by _inFlight.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 8));

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata> send = harness.AppendOne(0x5A, callback);

        // The setup, asserted rather than assumed: one record in the chain and headroom on the
        // semaphore, which is what makes the injected release throw rather than being absorbed.
        Assert.Equal(1, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(7, harness.Accumulator.AvailableAdmissions);

        harness.InflateTheChainAccounting();
        harness.ForceDrainWithoutWaiting();

        // Without the fix this never completes and the deadline fires instead.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => send, TimeSpan.FromSeconds(10)));
        Assert.Equal("The producer send-batch thread failed to process a batch.", failure.Message);
        Assert.IsType<SemaphoreFullException>(failure.InnerException);

        // The record never reached the core, so the delivery notification IS owed here (§6.2:
        // faulting an un-accepted record invents nothing and can never duplicate) — and exactly
        // once, asserted after a settle window because a first observation of 1 cannot rule out 2.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(1, callback.Count);
        Assert.NotNull(callback.LastMetadata);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
        Assert.Equal(-1L, callback.LastMetadata.Offset);
        Assert.Same(failure, callback.LastException);

        // The thread exited and the accumulator refuses further appends — "settle what is in hand,
        // then let the thread die" rather than "keep accepting records nothing will ever drain".
        Assert.True(
            harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
            "the failed batch thread did not exit");
        Func<object> refused = () => harness.AppendWithoutAdmission(0x5B);
        Assert.Throws<ObjectDisposedException>(refused);

        harness.Dispose();
    }

    [Fact]
    public async Task BatchThreadFailure_INSIDESendNode_StillSettlesTheRestOfThatNode()
    {
        // 65.3 named TWO triggers for the stranded in-flight chain, and the test above reaches only
        // the first: InflateTheChainAccounting's over-release throws BETWEEN the take and the send, so
        // SendChain is never entered and _inFlight's advance ordering is never exercised at all.
        // Hoisting `_inFlight = node.Next` ABOVE `SendNode(node)` — the tempting simplification,
        // which SendChain's comment used to defend only against RecycleNode — therefore left the
        // whole suite green while re-opening 65.3 for the second trigger. This is that half's guard.
        //
        // Reaching it needs a throw that ESCAPES SendNode. SendNode's own catch swallows everything
        // that happens inside it (a SendBatch failure, a ReleasePins failure in its finally, a
        // CompleteNode failure) into FaultNode and returns normally, so the only escape is FaultNode
        // itself failing — which is what TruncateDeliveriesOfPendingNode injects, and why it is a
        // truncation of THAT array rather than of Natives.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata>[] sends = harness.Append(3, callback);

        // A 60 s window with the threshold far above three records means the batch thread is parked
        // in its wait loop, so the truncation lands before it has looked at the node.
        harness.TruncateDeliveriesOfPendingNode(keep: 1);

        Assert.True(
            harness.Accumulator.DrainPending(s_deadline), "the accumulator did not drain in time");

        // Index 0 is collected into this send_batch call's completion GROUP but not yet handed
        // over — grouping hands the whole call over in ONE Enqueue, after the walk (M11/P3.2 §3B.2),
        // and the walk never finishes. So ownership never transferred and FaultNode owns index 0
        // too: it destroys that future and faults this awaiter with the same batch-thread failure.
        // (Before S3 the hand-over was per record, so index 0 had already reached the pump here and
        // resolved normally. The change is the recorded residual-4 window widening from one index's
        // hand-over step to one send_batch call's — same site, same condition, stated at the axes on
        // IDeliveryCallback.)
        await AssertSettledByTheBatchThreadFailure(sends[0]);

        // Index 1 is where CompleteNode threw. SendNode's own FaultNode settles it and then throws
        // out of SendNode on its very next statement.
        await AssertSettledByTheBatchThreadFailure(sends[1]);

        // Index 2 is THE assertion of this test: FaultNode never reached it, so it settles only if
        // AbandonOnThreadFailure can still find this node through _inFlight. With the advance
        // hoisted above SendNode it cannot, and this await hits its deadline instead.
        await AssertSettledByTheBatchThreadFailure(sends[2]);

        // No delivery callback for ANY of the three: the core accepted all three and their futures
        // are destroyed unread, so firing here would invent a failure for a record that may still be
        // delivered (§6.2, recorded residual 4). Index 0 is included for the reason above — its
        // group was never handed to the pump, so no core completion was ever read for it either.
        // Asserted after a settle window, since observing 0 immediately cannot rule out a later
        // fire.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(0, callback.Count);

        // The abandon path really ran — it closes the accumulator and lets the thread exit. Without
        // this the guard could go inert without saying so: a refactor that stopped FaultNode from
        // throwing would leave every assertion above passing while exercising none of SendChain's
        // failure ordering.
        Assert.True(
            harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
            "the failed batch thread did not exit");
        Func<object> refused = () => harness.AppendWithoutAdmission(0x6A);
        Assert.Throws<ObjectDisposedException>(refused);

        harness.Dispose();
    }

    /// <summary>
    /// Asserts that <paramref name="send"/> was faulted by the batch thread's handler of last
    /// resort — <see cref="KafkaException"/> wrapping the injected
    /// <see cref="IndexOutOfRangeException"/>, under a deadline so a record that is never settled
    /// fails fast instead of hanging the run.
    /// </summary>
    private static async Task AssertSettledByTheBatchThreadFailure(Task<RecordMetadata> send)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => send, TimeSpan.FromSeconds(10)));
        Assert.Equal("The producer send-batch thread failed to process a batch.", failure.Message);
        Assert.IsType<IndexOutOfRangeException>(failure.InnerException);
    }

    // ------------------------------------- Flush's accumulator drain and its expiry (§3.5) ------

    [Fact]
    public async Task Flush_WhenTheAccumulatorDrainExpires_ThrowsRatherThanReportingSuccess()
    {
        // The sync Flush drains the accumulator first and SURFACES an expiry, because returning
        // success with records still buffered in the binding is §3.5's Observation-1 failure — the
        // very thing the drain exists to fix — reintroduced on a timer. This is that branch.
        //
        // It needs no broker and no parked send_batch: the throw fires whenever DrainPending returns
        // false, i.e. whenever the accumulator is not empty-and-idle at the deadline. The only thing
        // that used to block the test was that the bound is a private static, which
        // FlushWithAccumulatorDrainBound now supplies instead.
        //
        // The accumulator is held non-idle DETERMINISTICALLY rather than by racing the free-running
        // window: closing the CORE producer makes send_batch reject every record per index, and
        // CompleteNode fires the delivery callback for such a record on the BATCH THREAD — the
        // accumulator's one call-out into user code. Parked there, `_draining` stays set, so no
        // bound can observe empty-and-idle and a zero bound expires immediately.
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);

        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        Task<RecordMetadata>? send = null;
        try
        {
            NativeMethods.ProducerClose(producer.Handle.DangerousGetHandle(), out IntPtr closeError);
            _ = KafkaException.FromHandle(closeError);

            SerializedProducerRecord record =
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x7A, 0xAA, 0xBB });
            send = producer.SendViaPump(
                record,
                new DeliveryRegistration(new BlockingDeliveryCallback(entered, release), Topic, 0));

            Assert.True(
                entered.Wait(s_deadline), "the batch thread never entered the delivery callback");

            KafkaException failure = Assert.Throws<KafkaException>(
                () => producer.FlushWithAccumulatorDrainBound(TimeSpan.Zero));

            // Asserted by content (DoD §3): the message has to name the condition, or a caller who
            // catches it learns nothing about which half of the flush did not happen.
            Assert.Equal(
                "The producer's send accumulator did not drain within 0 seconds, so records " +
                "buffered in the binding have not reached the core and this flush did not " +
                "include them.",
                failure.Message);
        }
        finally
        {
            // Always release: the parked batch thread would otherwise hold teardown, and the event
            // it waits on is disposed on the way out of this method.
            release.Set();
        }

        // The record was rejected by the closed core, so it faults. Observed out here rather than
        // in the finally so it can be awaited (xUnit1031 forbids blocking on it).
        Assert.NotNull(send);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send!, s_deadline));

        producer.Dispose();
    }

    [Fact]
    public async Task Flush_WhenTheAccumulatorDrains_ReachesTheCoreFlushInstead()
    {
        // The control for the test above, and the half that makes its bound meaningful: the same
        // producer, the same zero bound, an accumulator that IS empty-and-idle — no throw. Without
        // this a Flush that threw unconditionally would pass the expiry test.
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        SerializedProducerRecord record =
            new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x7B, 0xAA, 0xBB });
        Task<RecordMetadata> send = producer.SendViaPump(record, delivery: null);

        Assert.True(producer.DrainPendingSends(s_deadline), "the accumulator did not drain in time");

        producer.FlushWithAccumulatorDrainBound(TimeSpan.Zero);

        await TestTimeout.Run(() => send, s_deadline);
        producer.Dispose();
    }

    // ------------------- the admission bound, append-first (M11/P3.3 / M11/P3.4 §8.1) -----------

    [Fact]
    public async Task Admission_SaturatedBound_ParksTheCaller_AfterAppendingIt_AndTheDrainReleasesIt()
    {
        // The blocking contract itself, written to the BLOCKING shape: the parked send is issued
        // from its OWN task and the assertion is on that task, never `parked.IsCompleted == false`
        // over an inline call — which is what hung about half the cap tests on a sibling branch.
        //
        // A 60 s window means only an explicit drain can free capacity, and the wait has no timeout
        // at all since M11/P3.4 — so "parked" and "released" are both properties of the gate.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 4));

        Task<RecordMetadata>[] filled = harness.Append(4);
        Assert.Equal(4, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(0, harness.Accumulator.AvailableAdmissions);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // The fifth send PARKS. A settle window, not a poll: the assertion is that something must
        // NOT have happened, so it needs time in which to have happened.
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF1, CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(
            admission.IsCompleted,
            "the fifth send returned although the producer already held its whole bound");

        // ⚠ FIVE, NOT FOUR — this is the assertion that discriminates append-first from the
        // permit-first shape it replaces. The parked caller's record is ALREADY IN THE CHAIN; only
        // its caller is waiting. Both witnesses are asserted because neither alone is enough: the
        // counter could be bumped without an append, and the node's slot count could be reached by
        // an append that never charged the bound.
        Assert.Equal(5, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(5, harness.PendingCount());
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // The drain takes the chain, which is what returns the admission permits.
        harness.DrainNow();

        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> parked = await admission;

        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
        await TestTimeout.Run(() => parked, s_deadline);
        Assert.Equal(5, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(5, harness.HistoryCount);

        // At rest the accounting is EXACT: the take gave back one permit per record, the parked
        // caller consumed exactly one, so the semaphore is back at its starting count.
        Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(4, harness.Accumulator.AvailableAdmissions);
    }

    [Fact]
    public void Admission_PermitsAreReturnedExactlyOncePerRecord_AcrossRepeatedDrains()
    {
        // ⚠ THE DRIFT DETECTOR, and it is deterministic rather than a stress test. Append-first
        // forced the admission semaphore's CEILING up to int.MaxValue (a record's permit is released
        // by the take, which can precede the take of the permit by the caller that appended it, so a
        // MaxAdmittedRecords ceiling throws SemaphoreFullException on the first drain). That removed
        // the guard which had been REPORTING the asymmetry, so the balance needs asserting directly:
        // if the accounting drifts net-positive by even one permit per drain, the count grows
        // monotonically, the gate stops blocking, and M11/P3.3's 2.04 GiB / p50 3.5 s regression
        // returns SILENTLY — every record is still delivered, in order, exactly once, so no
        // correctness assertion anywhere else in this suite can see it.
        //
        // Ten rounds rather than one, because a single round cannot distinguish "balanced" from
        // "drifts by a constant that happens to cancel once".
        const int Cap = 4;
        const int Rounds = 10;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: Cap));

        for (int round = 0; round < Rounds; round++)
        {
            // Exactly Cap sends: the last one takes the last permit and returns without parking, so
            // this never blocks the xUnit thread.
            Task<RecordMetadata>[] sends = harness.Append(Cap);
            Assert.Equal(Cap, sends.Length);
            Assert.Equal(Cap, harness.Accumulator.AdmittedRecordCount);
            Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

            harness.DrainNow();

            Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);
            Assert.Equal(
                Cap,
                harness.Accumulator.AvailableAdmissions);
            Assert.Equal(Cap * (round + 1), harness.Accumulator.SendBatchRecordCount);
        }
    }

    [Fact]
    public async Task Admission_IsBounded_WhenTheFloodOutrunsTheDrain()
    {
        // ⚠ THE PHASE'S PRIMARY GATE (§8.1). Before the bound the client accepted sends without
        // limit: 2.05 M records in flight, p50 3,524 ms, RSS 2.04 GiB, against 41 ms / 239 MB
        // immediately before. A correctness-only suite cannot see that — every record is still
        // delivered, in order, exactly once — so this asserts a POPULATION rather than an outcome.
        //
        // ⚠ THE BOUND IT ASSERTS IS Cap + Senders, NOT Cap (M11/P3.4). Append-first counts a record
        // from the moment it is appended, which is before its caller has taken a permit, so the
        // population legitimately overshoots by the number of concurrently parked callers. That is
        // the anchor's own shape (py_Producer_send appends unconditionally, so C concurrent senders
        // reach bound + C - 1) and the arithmetic is derived at SendAccumulator._admission.
        //
        // THE REGIME. A 60 s window means nothing drains on its own, and the drainer is PACED rather
        // than free-running, so an unbounded acceptance has a window in which to pile up. It runs on
        // its own Thread, not a pool task: the senders BLOCK, so a pool drainer could be starved by
        // the very thing under test. The senders are LongRunning for the same reason.
        //
        // The burst is repeated with a FRESH harness per attempt: an isolated PASS is not evidence a
        // guard is absent, nor a suite PASS that it is present, and a reused harness would start
        // attempts 2..K from the previous attempt's chain, spare node and permit state.
        const int Cap = 8;
        const int Senders = 4;
        const int PerSender = 10;
        const int Attempts = 8;

        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            using Harness harness = new Harness(new SendAccumulatorSettings(
                slotThreshold: 1000,
                batchWindowMs: 60_000,
                batchChunk: 1100,
                maxAdmittedRecords: Cap));

            int peak = 0;
            Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Senders * PerSender];

            using (CancellationTokenSource flooding = new CancellationTokenSource())
            {
                Thread drainer = new Thread(() =>
                {
                    while (!flooding.IsCancellationRequested)
                    {
                        harness.ForceDrainWithoutWaiting();
                        Thread.Sleep(2);
                    }
                })
                {
                    IsBackground = true,
                    Name = "admission-bound-drainer",
                };
                drainer.Start();

                // Sampled DURING the flood as well as asserted after it: the after-assertion alone
                // would pass an implementation that admitted everything and then shed records, and
                // the peak is what "the population stays within the bound" actually means. One
                // writer, read only after the sampler has been awaited.
                Task sampler = Task.Run(
                    async () =>
                    {
                        while (!flooding.IsCancellationRequested)
                        {
                            int observed = harness.Accumulator.AdmittedRecordCount;
                            if (observed > peak)
                            {
                                peak = observed;
                            }

                            await Task.Delay(1).ConfigureAwait(false);
                        }
                    },
                    CancellationToken.None);

                Task[] floods = new Task[Senders];
                for (int s = 0; s < Senders; s++)
                {
                    int sender = s;
                    floods[sender] = Task.Factory.StartNew(
                        () =>
                        {
                            for (int i = 0; i < PerSender; i++)
                            {
                                // Production's own entry point, blocking exactly as production's
                                // Send blocks (DoD §12 — the fixture holds no copy of the rule).
                                int index = (sender * PerSender) + i;
                                sends[index] = harness.AppendOne((byte)index);
                            }
                        },
                        CancellationToken.None,
                        TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                        TaskScheduler.Default);
                }

                await TestTimeout.Run(() => Task.WhenAll(floods), s_deadline);
                flooding.Cancel();
                await TestTimeout.Run(() => sampler, s_deadline);
                Assert.True(drainer.Join(TimeSpan.FromSeconds(10)), "the drainer thread did not exit");
            }

            harness.DrainNow();
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

            // (1) THE BOUND. 12 with the gate; without it the flood appends all 40 between two
            // paced drains.
            Assert.True(
                peak <= Cap + Senders,
                $"the admitted population peaked at {peak} against a bound of {Cap} + {Senders} " +
                "parked callers — the admission wait did not throttle the flood");

            // (2) THE DRIFT DETECTOR, at rest: nothing accepted is unforwarded, and every permit is
            // back. An over-release reads above Cap here, a leak below it, and the int.MaxValue
            // ceiling no longer reports either.
            Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);
            Assert.Equal(Cap, harness.Accumulator.AvailableAdmissions);

            // (3) Nothing was shed: bounding acceptance must not lose records.
            Assert.Equal(Senders * PerSender, harness.HistoryCount);
        }
    }

    [Fact]
    public async Task Admission_CallerTokenFiresWhileParked_DoesNotInterruptTheWait_AndTheRecordIsStillSent()
    {
        // ⚠ THE CONTRACT CHANGE M11/P3.4 MAKES DELIBERATELY, pinned so it cannot regress silently.
        // Before it, a token that fired while a send was parked aborted the admission wait and the
        // record was NOT sent. Append-first makes that unrepresentable: the record is in the chain
        // before the wait begins, so honouring the token there would either strand an awaiter the
        // caller was never handed, or leave a record in the chain whose caller was told nothing was
        // sent. The token still cancels the returned Task — through NativeProducer.SendViaPump's own
        // registration, one layer above this one — which is why the accumulator can ignore it here.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2));

        Task<RecordMetadata>[] filled = harness.Append(2);

        using CancellationTokenSource cancellation = new CancellationTokenSource();
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF5, cancellation.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send returned although the bound was full");
        Assert.Equal(3, harness.Accumulator.AdmittedRecordCount);

        cancellation.Cancel();

        // THE ASSERTION: the token does not end the wait. A settle window, since the property is
        // that something must NOT happen. With the token honoured this completes here — faulted.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(
            admission.IsCompleted,
            "the caller's token interrupted the admission wait, which append-first forbids");

        // Only capacity ends it, and the record — appended before the park — is sent like any other.
        // Asserted on the CORE's record count, not on an awaiter.
        harness.DrainNow();
        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> sent = await admission;

        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
        await TestTimeout.Run(() => sent, s_deadline);
        Assert.Equal(3, harness.HistoryCount);
        Assert.Equal(3, harness.Accumulator.SendBatchRecordCount);
    }

    [Fact]
    public async Task Admission_ParkedCaller_IsReleasedByStop_AndItsRecordIsStillSent()
    {
        // PLAN §11 risk 2 — a caller blocked on admission that teardown does not wake hangs Dispose.
        // The wait has no timeout at all since M11/P3.4, so nothing but the gate can release it.
        //
        // ⚠ AND THE RECORD IS SENT, NOT REFUSED. Stop cancels the gate at its step 1 and sets
        // _closed at step 2, so the parked caller's record — appended before it parked — is in the
        // FINAL chain and the batch thread's last drain ships it. That is the anchor's outcome for a
        // record that was already accumulated when its close arrived, and it is what the count
        // assertion below pins: three, not two.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2));

        Task<RecordMetadata>[] filled = harness.Append(2);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF4, CancellationToken.None, callback);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send returned although the bound was full");
        Assert.Equal(3, harness.PendingCount());

        // Teardown must RETURN rather than hang behind the parked caller — under a hard deadline so
        // a hang fails the run instead of blocking it.
        TestTimeout.Run(
            () => Assert.True(
                harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
                "the batch thread did not exit"),
            TimeSpan.FromSeconds(20));

        // (1) The caller is released and does NOT throw. Awaiting the OUTER task is that assertion:
        // it carries whatever SubmitAdmitted threw, and SubmitAdmitted must not throw here — its
        // record is already in the chain, so "nothing was sent" would be a lie.
        await TestTimeout.Run(() => admission, TimeSpan.FromSeconds(10));
        Task<RecordMetadata> parked = await admission;

        // (2) Three records reached the core, not two.
        Assert.Equal(3, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(3, harness.HistoryCount);

        // (3) Every awaiter settles — exactly once, and successfully. A stranded
        // TaskCompletionSource is the shape that hangs an awaiting caller forever.
        await TestTimeout.Run(() => Task.WhenAll(filled), TimeSpan.FromSeconds(10));
        await TestTimeout.Run(() => parked, TimeSpan.FromSeconds(10));
        RecordMetadata metadata = await parked;
        Assert.Equal(Topic, metadata.Topic);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(1, callback.Count);
        Assert.Null(callback.LastException);

        // And the whole teardown returns — the no-hang regression this test is named for.
        TestTimeout.Run(harness.Dispose, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Admission_ParkedCaller_IsReleasedByStopsCancel_WhenTheBatchThreadCannotDrain()
    {
        // ⚠ THE DISCRIMINATOR FOR Stop'S _spaceGate.Cancel(), and it needs its own setup because the
        // obvious one cannot fail. Under append-first the permit arithmetic normally releases every
        // parked caller by itself: a parked caller implies the semaphore is at zero, which implies
        // the chain holds at least as many records as there are parked callers, so the final drain's
        // ReleaseAdmission wakes all of them — which is why deleting the Cancel leaves
        // Admission_ParkedCaller_IsReleasedByStop_AndItsRecordIsStillSent green (measured: 8/8).
        //
        // The Cancel is load-bearing exactly when the batch thread CANNOT take another chain, so no
        // release will ever come. That is reachable deterministically: closing the CORE producer
        // makes send_batch reject every record per index, and CompleteNode fires the delivery
        // callback for such a record ON THE BATCH THREAD, so a blocking callback parks that thread
        // mid-drain. Refilling the bound behind it and then parking a caller leaves a send that only
        // Stop's Cancel can release. Deleting that Cancel turns this red 8/8 in-suite.
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);

        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2));

        Task<RecordMetadata>? held = null;
        Task<RecordMetadata>[] filled = Array.Empty<Task<RecordMetadata>>();
        Task<Task<RecordMetadata>>? admission = null;
        try
        {
            harness.CloseCoreProducer();
            held = harness.AppendOne(0x01, new BlockingDeliveryCallback(entered, release));
            harness.ForceDrainWithoutWaiting();
            Assert.True(
                entered.Wait(s_deadline), "the batch thread never entered the delivery callback");

            // That drain returned this record's permit before parking, so the bound is free again.
            // Refill it: everything appended from here sits in a chain the parked thread will never
            // take.
            filled = harness.Append(2);
            Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

            admission = harness.AppendOneFromAnotherThread(0x02, CancellationToken.None);
            await Task.Delay(TimeSpan.FromMilliseconds(250));
            Assert.False(admission.IsCompleted, "the send returned although the bound was full");

            bool drained = true;
            TestTimeout.Run(
                () => drained = harness.Accumulator.Stop(TimeSpan.FromSeconds(2)),
                TimeSpan.FromSeconds(20));
            Assert.False(drained, "the parked batch thread cannot have exited");

            // THE ASSERTION: the caller is released although nothing released a permit.
            await TestTimeout.Run(() => admission, TimeSpan.FromSeconds(10));
        }
        finally
        {
            // Always release: the parked batch thread would otherwise hold the harness's own
            // teardown, and the event it waits on is disposed on the way out of this method.
            release.Set();
        }

        // The abandoned thread finishes its drain and exits on its own (it saw _closed), so every
        // record still settles — faulted, because the core was closed before any of them was sent.
        Assert.NotNull(admission);
        Task<RecordMetadata> parked = await admission!;
        Assert.NotNull(held);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => held!, s_deadline));
        foreach (Task<RecordMetadata> send in filled)
        {
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, s_deadline));
        }

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => parked, s_deadline));

        harness.Dispose();
    }

    [Fact]
    public async Task Admission_ParkedCaller_IsReleasedByTheBatchThreadsFailureHandler_AndItsRecordIsSettled()
    {
        // The second teardown trigger: the batch thread DYING rather than being stopped. Its handler
        // must leave no caller parked and no awaiter unsettled, exactly as Stop does.
        //
        // ⚠ WHAT THIS TEST DOES *NOT* RELIABLY ISOLATE, stated so a later reader does not over-read
        // it. Under append-first the permit arithmetic alone usually releases every parked caller on
        // a successful take: a parked caller implies the semaphore is at zero, which implies the
        // chain holds at least as many records as there are parked callers, so the take's
        // ReleaseAdmission wakes all of them. So this is a deterministic CONTRACT assertion
        // (released, settled, teardown completes) whose MECHANISM coverage is incidental — deleting
        // AbandonOnThreadFailure's Cancel turned the full net10.0 suite red in only 1 of 8 in-suite
        // reps. The deterministic guard for that Cancel is the twin below,
        // Admission_ParkedCaller_IsReleasedByTheFailureHandlersCancel_WhenTheReleaseWakesNobody.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0x82, CancellationToken.None);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send returned although the bound was full");
        Assert.Equal(3, harness.PendingCount());

        // Kill the batch thread inside SendNode (the truncation injection — see its remarks for why
        // it is the only escape SendNode's own catch leaves open).
        harness.TruncateDeliveriesOfPendingNode(keep: 1);
        harness.ForceDrainWithoutWaiting();

        // THE ASSERTION, under a hard deadline so a caller that is never released FAILS rather than
        // hanging the run.
        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> parked = await admission;

        // Every record the handler held is settled exactly once, with the batch-thread failure —
        // the parked caller's included, which is what "no stranded awaiter" means here.
        await AssertSettledByTheBatchThreadFailure(filled[0]);
        await AssertSettledByTheBatchThreadFailure(filled[1]);
        await AssertSettledByTheBatchThreadFailure(parked);

        // And the thread really did take the failure path, rather than the test having proved a
        // property of a still-running accumulator.
        Assert.True(
            harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
            "the failed batch thread did not exit");
        Func<object> refused = () => harness.AppendWithoutAdmission(0x84);
        Assert.Throws<ObjectDisposedException>(refused);

        harness.Dispose();
    }

    [Fact]
    public async Task Admission_ParkedCaller_IsReleasedByTheFailureHandlersCancel_WhenTheReleaseWakesNobody()
    {
        // ⚠ THE DISCRIMINATOR FOR AbandonOnThreadFailure'S _spaceGate.Cancel(), and — like Stop's
        // twin above — it needs its own setup because the obvious one cannot fail. Under
        // append-first the permit arithmetic normally releases every parked caller by itself, so
        // the sibling test above only catches the deletion 1 rep in 8.
        //
        // The two obvious ways to deny that wake-up BOTH fail, and it is worth saying why:
        //   * a parked caller forces the semaphore to 0, and the chain then holds at least as many
        //     records as there are parked callers, so a truthful ReleaseAdmission always wakes them;
        //   * InflateTheChainAccounting cannot be composed with a parked caller either —
        //     SemaphoreSlim.Release(n) throws only when CurrentCount + n exceeds the ceiling, and
        //     with the ceiling at int.MaxValue and CurrentCount pinned at 0 by the parked caller,
        //     Release(int.MaxValue) SUCCEEDS and wakes it.
        //
        // The reachable case is the one the handler's own comment names: "a release that releases
        // nothing". ReleaseAdmission is a no-op for count <= 0, and that guard is checked BEFORE the
        // semaphore is touched, so a non-positive count is not mutually exclusive with a parked
        // caller the way the throwing form is. Inject one and the take wakes nobody; the batch
        // thread then dies inside SendNode, and its handler's unconditional Cancel is the ONLY
        // thing left that can release the caller (the handler's own ReleaseAdmission gets 0, because
        // the first take already zeroed the counter under _closed). Deleting that Cancel turns this
        // red 8/8 in-suite.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0x92, CancellationToken.None);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send returned although the bound was full");
        Assert.Equal(3, harness.PendingCount());
        Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

        // (1) Kill the batch thread inside SendNode (the truncation injection — see its remarks for
        // why it is the only escape SendNode's own catch leaves open).
        harness.TruncateDeliveriesOfPendingNode(keep: 1);

        // (2) And make the take's own ReleaseAdmission a no-op, so the parked caller is NOT woken on
        // the way past. Without this line the release below hands back three permits and the test
        // proves nothing about the Cancel.
        harness.SetChainAccounting(-1);

        harness.ForceDrainWithoutWaiting();

        // THE ASSERTION, under a hard deadline so a caller that is never released FAILS rather than
        // hanging the run. Nothing released a permit, before or after the failure.
        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> parked = await admission;

        // The gate did the releasing, so the permit accounting is still exactly where the injection
        // left it — the direct witness that no release woke the caller.
        Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

        // And every record is still settled exactly once, the parked caller's included.
        await AssertSettledByTheBatchThreadFailure(filled[0]);
        await AssertSettledByTheBatchThreadFailure(filled[1]);
        await AssertSettledByTheBatchThreadFailure(parked);

        Assert.True(
            harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
            "the failed batch thread did not exit");
        Func<object> refusedAfterFailure = () => harness.AppendWithoutAdmission(0x94);
        Assert.Throws<ObjectDisposedException>(refusedAfterFailure);

        harness.Dispose();
    }

    [Fact]
    public async Task Close_WithAParkedBatchThread_DoesNotExceedTheBoundedTeardownWait()
    {
        // Stop's join is BOUNDED (P3.1 §6.3: bounded waits with a stated expiry outcome, never a
        // hang), because the batch thread can be stuck for a long time inside send_batch. The lever
        // is the delivery-callback park: closing the CORE producer makes send_batch reject every
        // record per index, and CompleteNode fires the delivery callback for such a record ON THE
        // BATCH THREAD — the accumulator's only call-out into user code, and so the only broker-free
        // hold on that thread's progress.
        //
        // Make the wait unbounded and this never returns, which is why the whole call runs under a
        // hard timeout that FAILS rather than hangs.
        TimeSpan bound = TimeSpan.FromSeconds(2);

        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);

        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 4));
        Task<RecordMetadata>? parked = null;
        try
        {
            harness.CloseCoreProducer();
            parked = harness.AppendOne(0x01, new BlockingDeliveryCallback(entered, release));
            harness.ForceDrainWithoutWaiting();
            Assert.True(
                entered.Wait(s_deadline), "the batch thread never entered the delivery callback");

            bool drained = true;
            Stopwatch elapsed = Stopwatch.StartNew();
            TestTimeout.Run(
                () => drained = harness.Accumulator.Stop(bound), TimeSpan.FromSeconds(20));
            elapsed.Stop();

            Assert.False(drained, "the parked batch thread cannot have exited");
            Assert.True(
                elapsed.Elapsed < TimeSpan.FromSeconds(3),
                $"teardown took {elapsed.Elapsed} against a {bound} bound — the join is not bounded");
        }
        finally
        {
            // Always release: the parked batch thread would otherwise hold the harness's own
            // teardown, and the event it waits on is disposed on the way out of this method.
            release.Set();
        }

        // Observed out here rather than in the finally so it can be awaited (xUnit1031 forbids
        // blocking on it). The record was rejected by the closed core.
        Assert.NotNull(parked);
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => parked!, s_deadline));

        harness.Dispose();
    }

    [Fact]
    public async Task Flush_IncludesASendWhoseCallerIsStillParkedOnAdmission()
    {
        // M11/P3.1 §3.5's gap, re-pinned against the new shape. "Empty and idle" is ONE stage again
        // (M11/P3.4 removed the submission queue), and that single stage has to cover a record whose
        // caller has not yet returned — because append-first puts such a record in the chain BEFORE
        // its caller parks. A predicate that skipped it would let Flush return while a record the
        // caller believes is on its way has not reached the core.
        //
        // DrainPending IS Flush's accumulator drain (NativeProducer.FlushWithAccumulatorDrainBound
        // calls it), driven here with explicit settings so the bound actually saturates.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xB0, CancellationToken.None);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send returned although the bound was full");
        Assert.Equal(3, harness.PendingCount());
        Assert.Equal(0, harness.HistoryCount);

        harness.DrainNow();

        // Asserted SYNCHRONOUSLY after the drain returns — that is the contract being tested.
        Assert.Equal(3, harness.HistoryCount);
        Assert.Equal(3, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);

        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> parked = await admission;
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
        await TestTimeout.Run(() => parked, s_deadline);

        // The ASYNC drain — what the async Flush awaits — rides the same predicate but reaches it
        // through a different site: its waiters are released by SignalIdleLocked, not by
        // DrainPending's loop, so the blocking half above leaves that site ungraded.
        Task<RecordMetadata>[] more = harness.Append(2);
        Task drain = harness.Accumulator.DrainPendingAsync(CancellationToken.None);
        await TestTimeout.Run(() => drain, s_deadline);
        Assert.Equal(5, harness.HistoryCount);
        await TestTimeout.Run(() => Task.WhenAll(more), s_deadline);
    }

    // ----------------------------------------- submission order (M11/P3.2 §F1 x M11/P3.4) -------

    [Fact]
    public async Task SendAccumulator_SubmissionOrder_IsCallOrder_AcrossASaturatedBound()
    {
        // ⚠ THE F1 REGRESSION TEST. The defect it guards: a send that found capacity appending AHEAD
        // of an earlier one still waiting for it. Since SendChain, Append and send_batch_inner all
        // preserve order, the binding's append order IS the wire order; Java documents ordering as
        // preserved in the default configuration (ProducerConfig.java:274) and the reorder happens
        // before the core sees the records, so nothing downstream can repair it.
        //
        // M11/P3.2 fixed it with a routing count plus a FIFO submission queue drained by a single
        // appender. M11/P3.4 replaced that whole mechanism with the anchor's own: SubmitAdmitted
        // appends BEFORE it waits, so a record's place is fixed by the call that placed it and there
        // is no window in which one send can pass another. The property under test is unchanged.

        // ---- part (i): deterministic. The node's own Completions slots, compared BY REFERENCE
        // against the awaiters the calls created, in call order — the exact array send_batch's
        // companion loop walks, in the exact order it will walk it. A 60 s window and a bound wide
        // enough that nothing parks make this a pure statement about Append's slot assignment.
        const int Sends = 8;
        using (Harness ordered = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 32)))
        {
            TaskCompletionSource<RecordMetadata>[] created =
                new TaskCompletionSource<RecordMetadata>[Sends];
            Task<RecordMetadata>[] issued = new Task<RecordMetadata>[Sends];
            for (int i = 0; i < Sends; i++)
            {
                issued[i] = ordered.AppendOne(
                    (byte)(0xA0 + i), callback: null, CancellationToken.None, out created[i]);
            }

            Assert.Equal(Sends, ordered.PendingCount());
            TaskCompletionSource<RecordMetadata>?[] slots = ordered.PendingCompletions();
            for (int i = 0; i < Sends; i++)
            {
                Assert.Same(created[i], slots[i]);
            }

            ordered.DrainNow();
            await TestTimeout.Run(() => Task.WhenAll(issued), s_deadline);
        }

        // ---- part (ii): stress. The interleaving that produced the bug — a saturating burst on ONE
        // thread with permits being freed underneath it — and the observed order must equal the call
        // order every time.
        //
        // ⚠ The drains MUST overlap the send loop, and the batch thread's own window cannot achieve
        // that: a burst on one thread completes in well under a millisecond, so with any usable
        // window every send is issued before the first drain. So the window is 60 s — nothing drains
        // on its own — and a drainer forces drains as fast as it can while the sender sends. It is a
        // dedicated Thread, not a pool task: the sender BLOCKS on the bound, and the drainer is the
        // only thing that can release it.
        //
        // ⚠ THE BURST IS REPEATED WITH A FRESH HARNESS PER ATTEMPT, AND THE REPETITION IS THE GUARD
        // (Critic 71 FU-1). Measured on the predecessor mechanism: one burst FAILED 5/5 in isolation
        // under the routing mutation yet the full net10.0 suite PASSED 5/5 — the gate green with a
        // merge-blocking fix reverted. Fresh per attempt because a reused harness carries the
        // previous attempt's chain, spare node and permit state, so attempts 2..K would no longer
        // start from the saturating-burst-from-cold shape.
        //
        // The witness is the per-record delivery callback's firing order, not the pending node's
        // slots: 200 records across a bound of 4 span many nodes and a node is recycled long before
        // the burst ends. It observes the same property because the chain from append to callback is
        // order-preserving end to end — CompleteNode hands the pump one group per send_batch call
        // holding that call's records in index order, Enqueue appends groups to a FIFO queue,
        // DequeueGroup dequeues FIFO, and ProcessBatch fires each pass in index order.
        const int Burst = 200;
        const int Attempts = 8;

        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            using Harness harness = new Harness(new SendAccumulatorSettings(
                slotThreshold: 1000,
                batchWindowMs: 60_000,
                batchChunk: 1100,
                maxAdmittedRecords: 4));

            List<int> observed = new List<int>(Burst);
            Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Burst];

            using (CancellationTokenSource sending = new CancellationTokenSource())
            {
                Thread drainer = new Thread(() =>
                {
                    while (!sending.IsCancellationRequested)
                    {
                        harness.ForceDrainWithoutWaiting();
                    }
                })
                {
                    IsBackground = true,
                    Name = "submission-order-drainer",
                };
                drainer.Start();

                for (int i = 0; i < Burst; i++)
                {
                    sends[i] = harness.AppendOne((byte)i, new OrderRecordingDeliveryCallback(observed, i));
                }

                sending.Cancel();
                Assert.True(drainer.Join(TimeSpan.FromSeconds(10)), "the drainer thread did not exit");
            }

            harness.DrainNow();
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

            int[] order;
            lock (observed)
            {
                order = observed.ToArray();
            }

            Assert.Equal(Burst, order.Length);
            for (int i = 0; i < Burst; i++)
            {
                Assert.Equal(i, order[i]);
            }
        }
    }

    [Fact]
    public async Task SendAccumulator_ConcurrentCallers_KeepEachCallersOwnSendOrder()
    {
        // The claim is PER CALLER, and that is deliberate: Java's guarantee is per-producer-per-
        // partition as observed by the caller (ProducerConfig.java:274), concurrent callers are not
        // ordered against each other, and the anchor does not order them either (its interleaving is
        // whatever its mutex grants). So this asserts the subsequence belonging to each sender is
        // strictly increasing, never that the global order matches any particular interleaving.
        //
        // It is the half part (ii) above cannot reach: that one runs on ONE thread, where append
        // order is fixed by the caller's own program order. Here four threads contend for _gate and
        // for the bound at once, which is where an append that moved out from under the lock — or a
        // parked caller that resumed and appended after a later send from the same thread — would
        // show up.
        const int Senders = 4;
        const int PerSender = 25;
        const int Attempts = 8;

        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            using Harness harness = new Harness(new SendAccumulatorSettings(
                slotThreshold: 1000,
                batchWindowMs: 60_000,
                batchChunk: 1100,
                maxAdmittedRecords: 4));

            List<int> observed = new List<int>(Senders * PerSender);
            Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Senders * PerSender];

            using (CancellationTokenSource sending = new CancellationTokenSource())
            {
                Thread drainer = new Thread(() =>
                {
                    while (!sending.IsCancellationRequested)
                    {
                        harness.ForceDrainWithoutWaiting();
                    }
                })
                {
                    IsBackground = true,
                    Name = "concurrent-order-drainer",
                };
                drainer.Start();

                Task[] floods = new Task[Senders];
                for (int s = 0; s < Senders; s++)
                {
                    int sender = s;
                    floods[sender] = Task.Factory.StartNew(
                        () =>
                        {
                            for (int i = 0; i < PerSender; i++)
                            {
                                int index = (sender * PerSender) + i;
                                sends[index] = harness.AppendOne(
                                    (byte)index, new OrderRecordingDeliveryCallback(observed, index));
                            }
                        },
                        CancellationToken.None,
                        TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                        TaskScheduler.Default);
                }

                await TestTimeout.Run(() => Task.WhenAll(floods), s_deadline);
                sending.Cancel();
                Assert.True(drainer.Join(TimeSpan.FromSeconds(10)), "the drainer thread did not exit");
            }

            harness.DrainNow();
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

            int[] order;
            lock (observed)
            {
                order = observed.ToArray();
            }

            // The length assertion is what keeps the loop below from being vacuous.
            Assert.Equal(Senders * PerSender, order.Length);

            int[] last = new int[Senders];
            for (int s = 0; s < Senders; s++)
            {
                last[s] = -1;
            }

            foreach (int index in order)
            {
                int sender = index / PerSender;
                Assert.True(
                    index > last[sender],
                    $"sender {sender} delivered record {index} after {last[sender]} — one caller's " +
                    "own sends were reordered against each other");
                last[sender] = index;
            }
        }
    }

    // --------------------------------------- admission settings (M11/P3.3 D3) -------------------

    [Fact]
    public void Settings_AdmissionDefault_IsTheMeasuredKnee()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>());

        // The knee MEASURED ON THIS BRANCH in slice S2 (§8.3): 1000 → 603.2k msg/s, p50 75 ms,
        // 210 MB — best on every axis at once, with a sharp cliff below (500 → 43.6k msg/s) and
        // monotonic latency/RSS growth above for no throughput gain.
        Assert.Equal(1000, settings.MaxAdmittedRecords);

        // ⚠ There is deliberately NO `Assert.NotEqual(SlotThreshold, MaxAdmittedRecords)` here any
        // more. It used to assert the two defaults differ, as a PROXY for D3's "the admission bound
        // is not coupled to the accumulation stage" — and the measurement made the proxy false: the
        // knee is 1000, which is also DefaultSlotThreshold, so the two defaults now coincide BY
        // MEASUREMENT while remaining structurally independent (own field, own env variable,
        // independently settable). A value-inequality assertion cannot express that, and re-scoping
        // it would only re-break the next time either default moves. The decoupling is proved where
        // it is actually observable — by MOVING one and watching the other stay put:
        // Settings_AdmissionBound_IsNotCoupledToTheThreshold and
        // Settings_AdmissionOverride_TakesEffect.
    }

    [Fact]
    public void Settings_AdmissionBound_IsNotCoupledToTheThreshold()
    {
        // ⚠ THE D3 ASSERTION. The slot threshold and the chunk both follow the threshold override;
        // the admission bound must not, because coupling it to Python's 1000 is exactly what made
        // that value look transferable when the measurement says it is not. Lowering the threshold
        // moves those and not this one.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "25",
        });

        Assert.Equal(25, settings.SlotThreshold);
        Assert.Equal(125, settings.BatchChunk);
        Assert.Equal(1000, settings.MaxAdmittedRecords);
    }

    [Fact]
    public void Settings_AdmissionOverride_TakesEffect()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "17",
            [SendAccumulatorSettings.MaxAdmittedVariable] = "321",
        });

        // Its own variable, so the two move independently — the observable form of D3.
        Assert.Equal(17, settings.SlotThreshold);
        Assert.Equal(321, settings.MaxAdmittedRecords);
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-5")]
    public void Settings_InvalidAdmissionOverride_FallsBackToTheDefault(string raw)
    {
        // An operator escape hatch must not be able to fail producer construction — and a ZERO cap
        // in particular would park every send on an untimed admission wait that nothing can release.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.MaxAdmittedVariable] = raw,
        });

        Assert.Equal(1000, settings.MaxAdmittedRecords);
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or <paramref name="timeout"/> expires,
    /// then asserts it — so a property that never arrives fails with
    /// <paramref name="because"/> rather than hanging the run.
    /// </summary>
    private static async Task PollUntil(Func<bool> condition, TimeSpan timeout, string because)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!condition() && elapsed.Elapsed < timeout)
        {
            await Task.Delay(2);
        }

        Assert.True(condition(), because);
    }

    /// <summary>
    /// Records the order in which delivery notifications fire, each instance carrying the index of
    /// the send it was handed to — the cross-node submission-order witness, used by the stress half
    /// of <see cref="SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound"/> (the
    /// steady-state path) and by
    /// <see cref="Close_FlushesQueuedSubmissionsToSendBatchInCallOrder"/> (the teardown path).
    /// </summary>
    private sealed class OrderRecordingDeliveryCallback : IDeliveryCallback
    {
        private readonly List<int> _order;
        private readonly int _index;

        internal OrderRecordingDeliveryCallback(List<int> order, int index)
        {
            _order = order;
            _index = index;
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            lock (_order)
            {
                _order.Add(_index);
            }
        }
    }

    /// <summary>
    /// Parks the thread that fires it until the test releases it. Used to hold the <b>batch</b>
    /// thread inside a drain deterministically: <c>CompleteNode</c>'s immediate-error branch is the
    /// accumulator's only call-out into user code, so it is the only broker-free lever on that
    /// thread's progress.
    /// </summary>
    /// <remarks>
    /// The wait is bounded so a test that never releases it fails rather than hanging the run, and
    /// the whole body is inside <see cref="DeliveryRegistration.Fire"/>'s no-throw boundary, so a
    /// timeout here cannot unwind into the batch thread.
    /// </remarks>
    private sealed class BlockingDeliveryCallback : IDeliveryCallback
    {
        private readonly ManualResetEventSlim _entered;
        private readonly ManualResetEventSlim _release;

        internal BlockingDeliveryCallback(ManualResetEventSlim entered, ManualResetEventSlim release)
        {
            _entered = entered;
            _release = release;
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            _entered.Set();
            _ = _release.Wait(s_deadline);
        }
    }

    /// <summary>
    /// Records every <see cref="IDeliveryCallback"/> invocation, so "exactly once" can be asserted
    /// as a <b>count</b> rather than as "it ran".
    /// </summary>
    private sealed class RecordingDeliveryCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal RecordMetadata? LastMetadata { get; private set; }

        internal KafkaException? LastException { get; private set; }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            LastMetadata = metadata;
            LastException = exception;
            Interlocked.Increment(ref _count);
        }
    }

    /// <summary>
    /// Reads <see cref="SendAccumulatorSettings.FromEnvironment"/> with the given variables set,
    /// restoring the previous values afterwards. The read itself is pure — no producer is
    /// constructed inside the window — so a concurrently-running test class cannot observe it.
    /// </summary>
    private static SendAccumulatorSettings ReadSettingsWith(Dictionary<string, string?> overrides)
    {
        string[] all =
        {
            SendAccumulatorSettings.ThresholdVariable,
            SendAccumulatorSettings.WindowVariable,
            SendAccumulatorSettings.ChunkVariable,
            SendAccumulatorSettings.MaxAdmittedVariable,
        };

        Dictionary<string, string?> previous = new Dictionary<string, string?>();
        foreach (string variable in all)
        {
            previous[variable] = Environment.GetEnvironmentVariable(variable);
            overrides.TryGetValue(variable, out string? value);
            Environment.SetEnvironmentVariable(variable, value);
        }

        try
        {
            return SendAccumulatorSettings.FromEnvironment();
        }
        finally
        {
            foreach (string variable in all)
            {
                Environment.SetEnvironmentVariable(variable, previous[variable]);
            }
        }
    }

    /// <summary>
    /// A producer + pump + topic cache + accumulator wired exactly as
    /// <c>NativeProducer.EnsureAccumulator</c> wires them, torn down in the same §3.8 order
    /// (accumulator drain → pump gate → flush → pump join → producer destroy).
    /// </summary>
    /// <remarks>
    /// <b><see langword="internal"/> rather than <see langword="private"/> since M11/P3.2 S3</b>, so
    /// <c>SendCompletionGroupingTests</c> drives the completion-grouping properties through this
    /// same fixture instead of standing up a second one. DoD §12: a duplicate fixture is a fixture
    /// that can drift from production's wiring while every assertion still passes.
    /// </remarks>
    internal sealed class Harness : IDisposable
    {
        private readonly NativeProducer _producer;
        private readonly SendCompletionPump _pump;
        private bool _disposed;

        internal Harness(SendAccumulatorSettings settings)
            : this(() => settings)
        {
        }

        internal Harness(Func<SendAccumulatorSettings> settingsFactory)
            : this(settingsFactory, autoComplete: true)
        {
        }

        /// <param name="settingsFactory">The accumulator settings to build with.</param>
        /// <param name="autoComplete">
        /// The mock core's completion mode. <see langword="false"/> leaves every accepted record
        /// pending until <see cref="CompleteNext"/> (or the teardown flush) resolves it — the lever
        /// that makes "one group's completions do not wait on another group's" observable at all.
        /// </param>
        internal Harness(Func<SendAccumulatorSettings> settingsFactory, bool autoComplete)
        {
            _producer = NativeProducer.CreateMock(autoComplete);
            _pump = new SendCompletionPump();
            Topics = new PinnedTopicCache();
            Accumulator = new SendAccumulator(_producer.Handle, Topics, _pump, settingsFactory());
        }

        internal SendAccumulator Accumulator { get; }

        internal PinnedTopicCache Topics { get; }

        /// <summary>
        /// How many sends the completion pump has taken off its queue — the witness for "this
        /// index was handed to the pump" (and, by its absence, for "this index was not"). It counts
        /// <b>records</b>, not groups (M11/P3.2 §3B.4).
        /// </summary>
        internal long DrainedSendCount => _pump.DrainedSendCount;

        /// <summary>The number of <c>get_all</c> passes the pump has run — one per queued group.</summary>
        internal long ProcessedBatchCount => _pump.ProcessedBatchCount;

        /// <summary>The record count of the largest pass the pump has run.</summary>
        internal int LargestProcessedBatch => _pump.LargestProcessedBatch;

        /// <summary>The mock core's sent-record count — what "reached the core" means (§3.5).</summary>
        internal int HistoryCount => _producer.MockHistoryCount();

        /// <summary>
        /// Production's own bounded pre-stop pump drain (M11/P3.2 S4), reachable on its own so a
        /// test can assert the <b>bound</b> without also running the join that follows it in
        /// <see cref="Dispose"/>.
        /// </summary>
        internal bool WaitForPumpQueueDrain(TimeSpan timeout) => _pump.WaitForQueueDrain(timeout);

        /// <summary>
        /// Resolves the OLDEST record the manual mock is still holding (its pending queue is a
        /// FIFO: <c>src/producer/mock_producer.rs</c> pushes on send and pops the front here).
        /// Meaningless on an auto-completing mock.
        /// </summary>
        internal bool CompleteNext() => _producer.MockCompleteNext();

        /// <summary>
        /// <b>The injection for the per-record immediate-error branch</b>: closes the CORE producer
        /// underneath a live accumulator, so the mock's own <c>send</c> rejects every subsequent
        /// record with <c>illegal_state("MockProducer is already closed.")</c> and
        /// <c>send_batch</c> writes it into that index's error slot.
        /// </summary>
        /// <remarks>
        /// It is the only broker-free way to reach the branch. The accumulator's own marshalling
        /// cannot produce a rejected record — <c>Fill</c> always writes a non-null interned topic
        /// pointer and the §4.2 sentinel for an empty key/value, and <c>send_batch_inner</c>'s only
        /// other per-record failures are a null topic / key / value pointer — and the public surface
        /// cannot reach it either, because closing through the client also latches the managed
        /// closed flag, so the next <c>Send</c> throws <see cref="ObjectDisposedException"/> before
        /// anything is appended. This closes only the core, leaving the binding-side state alone.
        /// The call is production's own shape (<c>NativeProducer.Dispose</c>'s graceful close).
        /// </remarks>
        internal void CloseCoreProducer()
        {
            NativeMethods.ProducerClose(_producer.Handle.DangerousGetHandle(), out IntPtr error);
            _ = KafkaException.FromHandle(error);
        }

        /// <summary>
        /// Appends <paramref name="count"/> records through the SAME primitives
        /// <c>NativeProducer.SendViaPump</c> uses — the backpressure permit first, then
        /// <see cref="SendAccumulator.Submit"/>, which owns the pinning (DoD §12: a fixture that
        /// substituted its own pinning would be a proof about the fixture).
        /// </summary>
        internal Task<RecordMetadata>[] Append(int count, IDeliveryCallback? callback = null)
        {
            Task<RecordMetadata>[] sends = new Task<RecordMetadata>[count];
            for (int i = 0; i < count; i++)
            {
                sends[i] = AppendOne((byte)i, callback);
            }

            return sends;
        }

        /// <summary>
        /// Appends one record and then parks on the admission bound if it is saturated — through the
        /// <b>same single entry point</b> <c>NativeProducer.SendViaPump</c> uses, so the admission
        /// bound is production's (DoD §12; M11/P3.3 §7, M11/P3.4). This fixture used to
        /// re-implement "permit-then-<c>Submit</c>, else await-then-<c>Submit</c>", which would
        /// have left the ordering tests below proving a property of the fixture rather than of the
        /// code.
        /// </summary>
        /// <remarks>
        /// ⚠ <b>It can BLOCK the calling thread INDEFINITELY</b>, exactly as production's
        /// <c>Send</c> does: a saturated admission bound parks the caller <em>untimed</em> until a
        /// take returns permits, or until teardown cancels the gate (M11/P3.4). The wait has no
        /// timeout knob at all — <c>SendAccumulatorSettings</c> carries none, so there is nothing a
        /// test could set to bound it. Every test that saturates the bound on purpose must drive this
        /// from its own <see cref="Task"/> — see <c>AppendOneFromAnotherThread</c> — or use
        /// <see cref="AppendWithoutAdmission"/>, which skips the wait entirely.
        /// </remarks>
        internal Task<RecordMetadata> AppendOne(byte tag, IDeliveryCallback? callback = null) =>
            AppendOne(tag, callback, CancellationToken.None, out _);

        /// <summary>
        /// <see cref="AppendOne(byte, IDeliveryCallback?)"/>, handing back the record's
        /// <see cref="TaskCompletionSource{TResult}"/> so an ordering test can compare a pending
        /// node's <c>Completions</c> slots <b>by reference</b> against the call order.
        /// </summary>
        internal Task<RecordMetadata> AppendOne(
            byte tag,
            IDeliveryCallback? callback,
            CancellationToken cancellationToken,
            out TaskCompletionSource<RecordMetadata> completion)
        {
            SerializedProducerRecord record = NewRecord(tag);
            completion = NewCompletion();
            DeliveryRegistration? delivery = NewDelivery(callback);

            // ONE call, production's own: SubmitAdmitted appends the record and then takes the
            // admission permit, blocking when the bound is saturated. The fixture deliberately
            // holds no copy of that rule.
            Accumulator.SubmitAdmitted(record, completion, delivery, cancellationToken);

            return completion.Task;
        }

        /// <summary>
        /// <see cref="AppendOne(byte, IDeliveryCallback?)"/> issued from a <b>separate</b>
        /// <see cref="Task"/>, so a test can saturate the admission bound and then observe that the
        /// next send <em>blocks</em> without the xUnit test thread being the one that parks.
        /// </summary>
        /// <remarks>
        /// The <b>outer</b> task is the admission observable: it completes when
        /// <c>SubmitAdmitted</c> returns, or faults with whatever admission <em>threw</em> — which
        /// is the whole contract under test. The <b>inner</b> task is the record's own delivery
        /// future. ⚠ Since M11/P3.4 the wait has no expiry at all, so the outer task completes only
        /// when a take returns permits or teardown cancels the gate, and it carries no failure of
        /// its own on either path; a test of teardown or cancellation awaits the outer one for the
        /// release and the inner one for the record's fate.
        /// <para>
        /// <see cref="TaskFactory.StartNew{TResult}(Func{TResult}, CancellationToken, TaskCreationOptions, TaskScheduler)"/>
        /// rather than <see cref="Task.Run(Func{Task})"/> deliberately: <c>Task.Run</c> would
        /// <em>unwrap</em> the inner task, so awaiting it would wait for the record's delivery
        /// instead of for its admission — the two are exactly what this helper must keep apart.
        /// </para>
        /// </remarks>
        internal Task<Task<RecordMetadata>> AppendOneFromAnotherThread(
            byte tag,
            CancellationToken cancellationToken,
            IDeliveryCallback? callback = null) =>
            Task.Factory.StartNew(
                () => AppendOne(tag, callback, cancellationToken, out _),
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);

        /// <summary>
        /// Appends one record through <see cref="SendAccumulator.Submit"/> <b>without</b> the
        /// admission wait <c>SubmitAdmitted</c> puts after it — so the calling thread never parks,
        /// whatever the bound is doing.
        /// </summary>
        /// <remarks>
        /// It is production's own append primitive (DoD §12), just without the throttle, so it is
        /// the right shape for the assertions that only need "does an append succeed or throw here"
        /// — notably the post-<c>Stop</c> refusal checks, which must not park the xUnit thread on a
        /// gate that teardown has already cancelled.
        /// </remarks>
        internal Task<RecordMetadata> AppendWithoutAdmission(byte tag, IDeliveryCallback? callback = null)
        {
            TaskCompletionSource<RecordMetadata> completion = NewCompletion();
            Accumulator.Submit(NewRecord(tag), completion, NewDelivery(callback));
            return completion.Task;
        }

        /// <summary>
        /// <b>The injection for a batch-thread failure BETWEEN the take and the send</b>: sets the
        /// accumulator's <c>_chainRecords</c> counter to <see cref="int.MaxValue"/>, so the next
        /// <c>TakeChainLocked</c> hands <c>ReleaseAdmission</c> a release count the admission
        /// semaphore cannot absorb and it throws <see cref="SemaphoreFullException"/> — at the one
        /// point in <c>RunLoopCore</c> that sits after the chain has been taken and published to
        /// <c>_inFlight</c> and before <c>SendChain</c> runs.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Why it has to be an injection.</b> Since M11/P3.4 the accounting is single-sited in
        /// both directions — <c>Append</c> is the only writer that grows the count and
        /// <c>TakeChainLocked</c> the only one that clears it — so no production path can put the
        /// two out of step. The predecessor injection (appending without a <c>_space</c> permit)
        /// died with that bound. Corrupting the counter directly <em>is</em> the fault, which is the
        /// same warrant <see cref="TruncateDeliveriesOfPendingNode"/> has.
        /// </para>
        /// <para>
        /// <b>Caller's contract:</b> the admission semaphore must have at least one permit free
        /// (<see cref="SendAccumulator.AvailableAdmissions"/> &gt; 0) — <c>Release</c> throws only
        /// when the count plus the release would exceed the ceiling, so a fully saturated bound
        /// absorbs even this release. And no drain may be possible yet, so the write is not racing
        /// the batch thread.
        /// </para>
        /// </remarks>
        internal void InflateTheChainAccounting()
        {
            Assert.True(
                Accumulator.AvailableAdmissions > 0,
                "the injection needs headroom on the admission semaphore, or the release absorbs it");

            SetChainAccounting(int.MaxValue);
        }

        /// <summary>
        /// <b>The injection for "a release that releases nothing"</b> — the second reachable form of
        /// the hazard <c>AbandonOnThreadFailure</c>'s unconditional <c>_spaceGate.Cancel()</c> exists
        /// for, and the one <see cref="InflateTheChainAccounting"/> cannot reach. Writes
        /// <paramref name="value"/> straight into the accumulator's <c>_chainRecords</c> counter, so
        /// the next <c>TakeChainLocked</c> hands <c>ReleaseAdmission</c> exactly that count.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A <b>non-positive</b> count makes <c>ReleaseAdmission</c> a no-op at its <c>count &gt; 0</c>
        /// guard — it never touches the semaphore at all, so unlike the throwing (over-release) form
        /// it is <em>not</em> mutually exclusive with a caller parked on that same semaphore. That is
        /// what lets a test park a caller and then deny it the wake-up the permit arithmetic would
        /// otherwise hand it for free.
        /// </para>
        /// <para>
        /// <b>Caller's contract</b> (as for its two siblings): no drain may be possible yet, so the
        /// write is not racing the batch thread.
        /// </para>
        /// </remarks>
        /// <param name="value">The count the next take will report as owed back.</param>
        internal void SetChainAccounting(int value)
        {
            FieldInfo chainRecords = typeof(SendAccumulator).GetField(
                "_chainRecords", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException(
                    "SendAccumulator no longer exposes a _chainRecords counter — this injection " +
                    "needs to be re-derived against the new shape rather than silently skipped.");

            chainRecords.SetValue(Accumulator, value);
        }

        /// <summary>
        /// <b>The injection for a failure that ESCAPES <c>SendNode</c></b> — 65.3's second trigger,
        /// which <see cref="InflateTheChainAccounting"/> cannot reach because its over-release throws
        /// before <c>SendChain</c> is ever entered. Shortens the pending node's <c>Deliveries</c>
        /// array to <paramref name="keep"/> entries, leaving the other seven parallel arrays intact.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Why this array, and why a truncation.</b> <c>SendNode</c> wraps everything it does in
        /// a <c>catch (Exception) → FaultNode(...)</c>, so a failure of <c>send_batch</c>, of
        /// <c>ReleasePins</c> in its <c>finally</c>, or of <c>CompleteNode</c> is swallowed and
        /// <c>SendNode</c> returns normally — none of them escapes, and none of them exercises
        /// <c>SendChain</c>'s advance ordering. (The obvious-looking alternative, a short
        /// <c>Natives</c> array so the <c>send_batch</c> marshaller throws, is exactly one of those
        /// swallowed cases and would produce a test that passes with the ordering either way.) The
        /// only escape left is <c>FaultNode</c> itself failing, and a short <c>Deliveries</c> is the
        /// one way to make that happen with the arrays it walks otherwise intact.
        /// </para>
        /// <para>
        /// <b>Why it heals in time for the abandon path.</b> With <c>keep = 1</c> and three records,
        /// <c>CompleteNode</c> hands index 0 to the pump and then throws reading
        /// <c>Deliveries[1]</c>; <c>FaultNode</c> resumes at index 1, faults that awaiter, nulls
        /// <c>Completions[1]</c>, and only then throws on <c>Deliveries[1] = null</c> — the last
        /// statement of the body. <c>AbandonOnThreadFailure</c>'s own <c>FaultNode</c> then walks
        /// the node from 0, skips indices 0 and 1 on their now-null completions, and settles index
        /// 2 before hitting the same wall. So the injected fault is self-healing for exactly the
        /// indices already settled, which is what makes "index 2 completes" a clean witness for
        /// "the failing node was still reachable from <c>_inFlight</c>".
        /// </para>
        /// <para>
        /// <b>Reflection, and why the fixture is allowed it here.</b> The node type is private to
        /// <see cref="SendAccumulator"/>, and no production state can make one array shorter than
        /// its siblings (<c>EnsureSlot</c> grows all eight or none). Everything else stays
        /// production's own primitive — the records, the awaiters, the pins and <c>Submit</c> itself
        /// (DoD §12); only the one array is corrupted, because corrupting it <em>is</em> the
        /// injected fault. The read is unsynchronized, which is safe only under the caller's
        /// contract below.
        /// </para>
        /// </remarks>
        /// <param name="keep">
        /// How many <c>Deliveries</c> entries survive. Must be smaller than the node's record count.
        /// </param>
        internal void TruncateDeliveriesOfPendingNode(int keep)
        {
            // Caller's contract: no drain may be possible yet (a long window, a threshold above the
            // record count), so the batch thread is parked in its wait loop and this node is owned
            // by nobody.
            object node = PendingNode();
            FieldInfo deliveries = node.GetType().GetField(
                "Deliveries", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                ?? throw new InvalidOperationException(
                    "SendAccumulator's node no longer exposes a Deliveries array — this injection " +
                    "needs to be re-derived against the new shape rather than silently skipped.");

            DeliveryRegistration?[] full = (DeliveryRegistration?[])deliveries.GetValue(node)!;
            Assert.True(
                keep < full.Length,
                "the injection must actually shorten the array, or it injects nothing");

            DeliveryRegistration?[] truncated = new DeliveryRegistration?[keep];
            Array.Copy(full, truncated, keep);
            deliveries.SetValue(node, truncated);
        }

        /// <summary>The single node the accumulator is currently filling.</summary>
        private object PendingNode() =>
            PendingNodeOrNull()
                ?? throw new InvalidOperationException(
                    "the accumulator holds no pending node — it drained before the injection landed");

        private object? PendingNodeOrNull()
        {
            FieldInfo head = typeof(SendAccumulator).GetField(
                "_head", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException(
                    "SendAccumulator no longer exposes a _head field.");

            return head.GetValue(Accumulator);
        }

        /// <summary>
        /// How many records the pending node holds; 0 when there is no pending node.
        /// </summary>
        internal int PendingCount()
        {
            object? node = PendingNodeOrNull();
            if (node is null)
            {
                return 0;
            }

            PropertyInfo count = node.GetType().GetProperty(
                "Count", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                ?? throw new InvalidOperationException(
                    "SendAccumulator's node no longer exposes a Count property — this witness " +
                    "needs to be re-derived against the new shape rather than silently skipped.");

            return (int)count.GetValue(node)!;
        }

        /// <summary>
        /// <b>The submission-order witness.</b> The pending node's <c>Completions</c> slots — the
        /// exact array <c>send_batch</c>'s companion loop walks, in the exact order it will walk it
        /// — so a test compares slot <c>i</c> <b>by reference</b> against the
        /// <see cref="TaskCompletionSource{TResult}"/> its <c>i</c>-th call created.
        /// </summary>
        /// <remarks>
        /// Chosen over reading the tag byte back through <c>Natives[i]</c>: reference identity needs
        /// no <c>unsafe</c>, no pointer read, and no assumption about which field
        /// <c>ProducerSendBatchMarshal.Fill</c> writes first — it compares the object the caller
        /// handed in against the slot the accumulator stored it in. Both witnesses observe the same
        /// array; this one cannot be satisfied by a coincidence in the record bytes.
        /// <para>
        /// Reflection is warranted for the same reason
        /// <see cref="TruncateDeliveriesOfPendingNode"/>'s is: the node type is private to
        /// <see cref="SendAccumulator"/> and there is no production observable for slot order.
        /// The read is unsynchronized, so the caller must hold the node still — a window far longer
        /// than the test (no threshold reached, no forced drain).
        /// </para>
        /// </remarks>
        internal TaskCompletionSource<RecordMetadata>?[] PendingCompletions()
        {
            object node = PendingNode();
            return (TaskCompletionSource<RecordMetadata>?[])
                NodeField(node, "Completions").GetValue(node)!;
        }

        private static FieldInfo NodeField(object node, string name) =>
            node.GetType().GetField(
                name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                ?? throw new InvalidOperationException(
                    $"SendAccumulator's node no longer exposes a {name} member — this witness " +
                    "needs to be re-derived against the new shape rather than silently skipped.");

        private static SerializedProducerRecord NewRecord(byte tag) =>
            new SerializedProducerRecord(Topic, 0, null, null, new byte[] { tag, 0xAA, 0xBB });

        private static TaskCompletionSource<RecordMetadata> NewCompletion() =>
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

        private static DeliveryRegistration? NewDelivery(IDeliveryCallback? callback) =>
            callback is null ? null : new DeliveryRegistration(callback, Topic, 0);


        /// <summary>Forces one drain and waits for it, the way the test drain hook does.</summary>
        internal void DrainNow() =>
            Assert.True(Accumulator.DrainPending(s_deadline), "the accumulator did not drain in time");

        /// <summary>
        /// Arms one drain and returns immediately — <see cref="SendAccumulator.DrainPending"/> with
        /// a zero timeout sets the force flag, pulses the batch thread and gives up waiting.
        /// </summary>
        /// <remarks>
        /// The lever that makes drains overlap a send loop deterministically, instead of hoping the
        /// batch thread's free-running window lands inside it. Its <see langword="false"/> return
        /// ("not drained within zero") is the expected outcome and is deliberately ignored.
        /// </remarks>
        internal void ForceDrainWithoutWaiting() => Accumulator.DrainPending(TimeSpan.Zero);

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            // The §3.8 ordering: drain the accumulator into a STILL-OPEN pump gate, then close the
            // gate, then flush so the pump's blocking get_all can return, then wait (bounded) for
            // the pump to take what the drain just queued, then join it, then destroy. The
            // WaitForQueueDrain step is M11/P3.2 S4 and is production's own (NativeProducer.StopPump)
            // — a fixture that skipped it would make every teardown test here a proof about the
            // fixture rather than about the shipped ordering (DoD §12).
            bool drained = Accumulator.Stop(s_deadline);
            _pump.CloseGate();
            NativeMethods.ProducerFlush(_producer.Handle, out IntPtr flushError);
            _ = KafkaException.FromHandle(flushError);
            _pump.WaitForQueueDrain(s_deadline);
            _pump.Stop();
            if (drained)
            {
                Topics.Dispose();
            }

            _producer.Dispose();
        }
    }
}
