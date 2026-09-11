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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 20, batchChunk: 1100));

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
            slotThreshold: 8, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 8));

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
            slotThreshold: 4, maxAccumulatedRecords: 100_000, batchWindowMs: 10, batchChunk: 104));

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 10, batchChunk: 999_999);

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
        Assert.Equal(1000, settings.MaxAccumulatedRecords); // = SLOT_THRESHOLD
        Assert.Equal(10, settings.BatchWindowMs);        // the bare 10 ms literal
        Assert.Equal(1100, settings.BatchChunk);         // Python's effective per-call maximum
    }

    [Fact]
    public void Settings_EnvironmentOverrides_TakeEffect()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "40",
            [SendAccumulatorSettings.MaxAccumulatedVariable] = "17",
            [SendAccumulatorSettings.WindowVariable] = "3",
            [SendAccumulatorSettings.ChunkVariable] = "9",
        });

        Assert.Equal(40, settings.SlotThreshold);
        Assert.Equal(140, settings.SlotCapacity);   // still threshold + 100
        Assert.Equal(17, settings.MaxAccumulatedRecords);
        Assert.Equal(3, settings.BatchWindowMs);
        Assert.Equal(9, settings.BatchChunk);
    }

    [Fact]
    public void Settings_BoundDefaultsToTheEffectiveThreshold_NotTheConstant()
    {
        // Python DEFINES the bound as the threshold, so lowering only the threshold must move the
        // bound with it — otherwise the two silently decouple.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "25",
        });

        Assert.Equal(25, settings.SlotThreshold);
        Assert.Equal(25, settings.MaxAccumulatedRecords);
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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
    public void SubmitAfterStop_ThrowsAndReleasesBothThePinsAndThePermit()
    {
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 10, batchChunk: 1100));
        SendAccumulator accumulator = harness.Accumulator;
        harness.Dispose();

        // Four refused submits: each must give its permit back, or the bound would leak one slot per
        // rejection and the fifth attempt could not even acquire one.
        for (int i = 0; i < 4; i++)
        {
            Assert.True(accumulator.TryAcquireSpace());
            Assert.Throws<ObjectDisposedException>(() => accumulator.Submit(
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 1, 2, 3 }),
                new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously),
                delivery: null));
        }

        Assert.True(accumulator.TryAcquireSpace(), "a refused Submit did not return its permit");
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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 10, batchChunk: 1100));

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
        // AbandonOnThreadFailure (RunLoop's catch) had NO coverage at all, and it is not a trivial
        // helper: it closes the accumulator, settles a chain, releases permits, releases pins and
        // fires delivery callbacks. The gap hid a real defect — the handler took the ACCUMULATOR's
        // chain (_head) while the failure it handles can land after the batch thread has already
        // taken one, so every record in the taken chain was stranded forever (its awaiter never
        // completed, its pins never released, its futures never destroyed) while Stop still
        // reported the thread as exited and teardown then freed the interned topic buffers those
        // un-released pins still pointed at.
        //
        // Reaching that window needs a throw BETWEEN the take and the send. AppendWithoutAPermit is
        // the injection (see its remarks): the over-release throws in exactly that gap, with the
        // taken chain held only by the loop's local — which is why the record is the in-flight
        // chain's, never the accumulator's.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 10, batchChunk: 1100));

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata> send = harness.AppendWithoutAPermit(0x5A, callback);

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
        Assert.True(harness.Accumulator.TryAcquireSpace());
        Func<object> refused = () => harness.AppendWithoutAPermit(0x5B);
        Assert.Throws<ObjectDisposedException>(refused);

        harness.Dispose();
    }

    [Fact]
    public async Task BatchThreadFailure_INSIDESendNode_StillSettlesTheRestOfThatNode()
    {
        // 65.3 named TWO triggers for the stranded in-flight chain, and the test above reaches only
        // the first: AppendWithoutAPermit's over-release throws BETWEEN the take and the send, so
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
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

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
        Assert.True(harness.Accumulator.TryAcquireSpace());
        Func<object> refused = () => harness.AppendWithoutAPermit(0x6A);
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

    [Fact]
    public async Task BatchThreadFailure_ReleasesASendWaitingForSpace()
    {
        // ⚠ M11/P3.2 slice S5 (§F5 / decision D5) — the guard for the UNCONDITIONAL
        // `_spaceGate.Cancel()` in AbandonOnThreadFailure.
        //
        // THE GAP. Stop cancels the gate explicitly, so teardown releases anyone waiting for space.
        // The thread-death path did not: it sets _closed, settles both chains, sweeps the submission
        // queue and calls ReleaseSpace(freed) — and a waiter ALREADY DEQUEUED and parked in
        // WaitForSpaceAsync is in none of those. It was released only as a side effect of the
        // permits coming back, i.e. by arithmetic. The failure this handler exists for is an
        // OVER-RELEASE, and SemaphoreSlim.Release validates the whole count BEFORE releasing
        // anything — so on exactly that path the throwing call releases NOTHING and the waiter is
        // never woken. That is the hang this test fails on without the fix.
        //
        // WHY THE DETERMINISTIC WAITER CALLS WaitForSpaceAsync DIRECTLY. "The submitter has dequeued
        // this submission and parked it on the gate" is not observable from outside:
        // QueuedSubmissionCount counts queued-OR-appending and cannot separate the two, which is the
        // same transience SendAccumulator_SubmissionOrder_IsCallOrder... records for its own
        // predicate. So part (i) parks on production's OWN primitive — the exact method
        // AppendQueuedAsync awaits, not a substitute for it (DoD §12) — and asserts it is still
        // parked before the thread is killed. Part (ii) then adds a real queued send end to end; it
        // reaches the parked state often but not on every run, and both paths fault it with the same
        // ObjectDisposedException, so asserting its outcome is not asserting a race.
        //
        // THE SETUP IS DETERMINISTIC. Threshold 2 against a 60 s window: the filling record leaves
        // the batch thread parked in its wait loop, and the injected record takes the node to the
        // threshold and wakes it — so the thread dies when this test says so, never when a timer
        // fires.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 2, maxAccumulatedRecords: 1, batchWindowMs: 60_000, batchChunk: 1100));

        // Fill the bound: one permit taken, accumulated == 1 < threshold, so nothing drains.
        Task<RecordMetadata> filled = harness.AppendOne(0x71);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // (i) The deterministic waiter, parked on the exhausted gate.
        Task waiting = harness.Accumulator.WaitForSpaceAsync(CancellationToken.None);
        Assert.False(waiting.IsCompleted, "the backpressure gate was not exhausted");

        // (ii) The end-to-end waiter: a real send with no permit to take, so production's routing
        // sends it to the submission queue. The count is incremented under _gate inside
        // SubmitQueued, so this assertion cannot race the submitter.
        Task<RecordMetadata> queued = harness.AppendOne(0x72);
        Assert.Equal(1, harness.Accumulator.QueuedSubmissionCount);
        Assert.False(queued.IsCompleted);

        // Kill the batch thread. This record takes the node to the threshold and wakes it; the take
        // then reports two accumulated against the one permit ever taken, so ReleaseSpace
        // over-releases a semaphore of one and throws between the take and the send.
        Task<RecordMetadata> injected = harness.AppendWithoutAPermit(0x73);

        // THE ASSERTION, under a hard deadline so a waiter that is never released FAILS rather than
        // hanging the run. Without the cancel this is where the run stops.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => waiting, s_deadline));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => queued, s_deadline));

        // The records the handler actually holds are still settled exactly as before — S5 adds a
        // release path, it changes none of the existing ones.
        await AssertSettledByTheOverRelease(filled);
        await AssertSettledByTheOverRelease(injected);

        // And the thread really did take the failure path, rather than the test having proved a
        // property of a still-running accumulator.
        Assert.True(
            harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
            "the failed batch thread did not exit");
        Func<object> refused = () => harness.AppendWithoutAPermit(0x74);
        Assert.Throws<ObjectDisposedException>(refused);

        harness.Dispose();
    }

    /// <summary>
    /// Asserts that <paramref name="send"/> was faulted by the batch thread's handler of last
    /// resort after <c>AppendWithoutAPermit</c>'s injected over-release —
    /// <see cref="KafkaException"/> wrapping a <see cref="SemaphoreFullException"/>, under a
    /// deadline so a record that is never settled fails fast instead of hanging the run.
    /// </summary>
    private static async Task AssertSettledByTheOverRelease(Task<RecordMetadata> send)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => send, TimeSpan.FromSeconds(10)));
        Assert.Equal("The producer send-batch thread failed to process a batch.", failure.Message);
        Assert.IsType<SemaphoreFullException>(failure.InnerException);
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

    // ------------------------------------------------------------------- backpressure (§4.6) ----

    [Fact]
    public async Task Backpressure_BlocksAtTheBound_AndReleasesWhenTheDrainTakesTheChain()
    {
        // A 60 s window means only an explicit drain can free capacity, so "blocked" and "released"
        // are both deterministic rather than timing-dependent.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] filled = harness.Append(4);
        Assert.Equal(4, filled.Length);

        // The bound is full: a non-blocking attempt is refused.
        Assert.Null(harness.TryAppendOne(0xF0));

        // A blocking attempt parks rather than throwing or dropping the record.
        Task<RecordMetadata> blocked = harness.AppendOne(0xF1);
        Assert.False(blocked.IsCompleted, "the fifth send did not park on the backpressure bound");
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // The drain takes the chain and returns the permits (anchor :573/:579).
        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);

        // The released sender resumes on the thread pool, so its append is not ordered against the
        // drain that freed it — poll rather than assume. A bound that never released would never
        // satisfy this, so the deadline is the real assertion.
        await TestTimeout.Run(
            async () =>
            {
                while (!blocked.IsCompleted)
                {
                    harness.DrainNow();
                    await Task.Delay(5);
                }

                await blocked;
            },
            s_deadline);

        Assert.Equal(5, harness.Accumulator.SendBatchRecordCount);
    }

    [Fact]
    public async Task Backpressure_DrainBetweenTheCheckAndTheWait_DoesNotStrandTheSender()
    {
        // The lost-wakeup case. Python does the check-and-register under the same mutex its drain
        // holds; a SemaphoreSlim counts PERMITS instead, so a drain landing in this exact window
        // leaves a permit behind and the wait returns immediately rather than parking forever.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 2, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] filled = harness.Append(2);

        // The check fails...
        Assert.False(harness.Accumulator.TryAcquireSpace());

        // ...the drain completes HERE, entirely before the wait is even entered...
        harness.DrainNow();

        // ...and the wait must still return, not strand.
        await TestTimeout.Run(
            () => harness.Accumulator.WaitForSpaceAsync(CancellationToken.None),
            TimeSpan.FromSeconds(10));

        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
    }

    [Fact]
    public async Task Backpressure_TeardownCancelsTheGate_SoAParkedSendCompletesAndStopReturns()
    {
        // The one place this design is strictly better than the inline send it replaces: a caller
        // waiting for capacity is on a MANAGED, cancellable primitive, so teardown can wake it. Under
        // Option C the equivalent caller was blocked inside the core's coarse mutex and a concurrent
        // close could not wake it at all (§2.2/§4.6).
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 2, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Task<RecordMetadata> blocked = harness.AppendOne(0xF2);
        Assert.False(blocked.IsCompleted);

        // Teardown must return rather than hang behind the parked sender...
        TestTimeout.Run(
            () => Assert.True(
                harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
                "the batch thread did not exit"),
            TimeSpan.FromSeconds(20));

        // ...and since M11/P3.2 slice S2 the parked send is SENT, not faulted: the cancelled gate
        // releases it into the teardown bypass, which appends it without a permit, and the batch
        // thread's final drain hands it to the core. Whichever of the two racing paths released it
        // (the cancel, or a final-drain permit release letting it through normally) the outcome is
        // now the same one, which is why this no longer has to avoid asserting an outcome.
        //
        // Awaited BEFORE the harness's own Dispose, which stops the completion pump — a future
        // still queued there when it stops is faulted by the terminal drain (recorded residual 2).
        await TestTimeout.Run(() => blocked, TimeSpan.FromSeconds(10));
        await TestTimeout.Run(() => Task.WhenAll(filled), TimeSpan.FromSeconds(10));
        Assert.Equal(3, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(3, harness.HistoryCount);

        TestTimeout.Run(harness.Dispose, TimeSpan.FromSeconds(10));
    }

    // ----------------------------------------------------- submission order (M11/P3.2 §F1) ----

    [Fact]
    public async Task SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound()
    {
        // ⚠ THE F1 REGRESSION TEST. Before the fix, Send took the backpressure permit FIRST and
        // deferred the append to a thread-pool continuation when it could not get one, so a later
        // send that found a free permit appended AHEAD of an earlier one still waiting — and since
        // SendChain, Append and send_batch_inner all preserve order, the binding's append order IS
        // the wire order. Java documents ordering as preserved in the default configuration
        // (ProducerConfig.java:274), and the reorder happens before the core sees the records, so
        // nothing downstream can repair it. No test in the suite asserted append order at all.

        // ---- part (i): deterministic. With a submission queued, the inline path is REFUSED and
        // the next send lands behind it in the queue — nothing is appended and nothing is pinned.
        Harness frozen = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

        // Saturate the bound WITHOUT appending anything: a drain then frees `_accumulated == 0`
        // permits, so a queued submission stays queued for the whole test rather than racing a
        // release. That is what makes this half deterministic instead of a timing window.
        frozen.ConsumePermits(4);

        Task<RecordMetadata> firstQueued = frozen.AppendOne(0x01);
        Assert.Equal(1, frozen.Accumulator.QueuedSubmissionCount);

        Task<RecordMetadata> secondQueued = frozen.AppendOne(0x02);
        Assert.Equal(2, frozen.Accumulator.QueuedSubmissionCount);

        // The non-blocking probe is refused too, and the count is unchanged by a refusal.
        Assert.Null(frozen.TryAppendOne(0xFF));
        Assert.Equal(2, frozen.Accumulator.QueuedSubmissionCount);

        // Nothing appended => no node => no slot => NO PIN is held while a submission is queued
        // (M11/P3.1 §4.4: the pin must not be taken before the permit). The absence of a node is
        // the structural witness for that — pins live only in a node's slots.
        Assert.Equal(0, frozen.PendingCount());
        Assert.Equal(0, frozen.HistoryCount);
        Assert.Equal(0, frozen.Accumulator.SendBatchCallCount);
        Assert.False(firstQueued.IsCompleted);
        Assert.False(secondQueued.IsCompleted);

        // Teardown FLUSHES both into the chain rather than faulting them (M11/P3.2 §F2 / slice S2),
        // bypassing the frozen bound exactly as the anchor's already-accumulated record bypasses
        // it — so they are sent and complete successfully. Their order is asserted by the frozen
        // half above (nothing appended while queued); that they arrive at all is asserted here.
        //
        // Stop rather than Dispose, and the awaits BEFORE Dispose: the accumulator's teardown is
        // the step under test, and the records' futures resolve on a pump the harness's Dispose
        // would stop, whose terminal drain faults whatever it still holds (recorded residual 2).
        Assert.True(frozen.Accumulator.Stop(s_deadline), "the batch thread did not exit");
        Assert.Equal(2, frozen.Accumulator.SendBatchRecordCount);
        Assert.Equal(2, frozen.HistoryCount);
        await TestTimeout.Run(() => firstQueued, s_deadline);
        await TestTimeout.Run(() => secondQueued, s_deadline);
        frozen.Dispose();

        // ---- part (ii): stress. The interleaving that produced the bug — a saturating burst on ONE
        // thread, with permits being freed underneath it — and the observed order must equal the
        // call order every time. This is the half that fails on unmodified HEAD.
        //
        // ⚠ The drains MUST overlap the send loop, and leaving that to the batch thread's own
        // window does not achieve it: 200 sends on one thread complete in well under a millisecond,
        // so with any usable window every send is issued before the first drain, the bound never
        // un-saturates mid-burst, and the "a released permit is taken by a LATER send" step of the
        // bug is never reached. (Measured: with the routing predicate reverted, a burst against a
        // 5 ms window passes.) So the window is 60 s — nothing drains on its own — and a drainer
        // task forces a drain as fast as it can while the sender sends. Every send still comes from
        // ONE thread, which is what the per-caller ordering claim is about.
        //
        // ⚠ THE BURST IS REPEATED, AND THE REPETITION IS THE GUARD (Critic 71 FU-1). A single burst
        // detects the routing mutation reliably in ISOLATION but not in the DoD gate: measured with
        // the routing predicate removed from TrySubmitInline, one burst FAILED 5/5 isolated yet the
        // full net10.0 suite PASSED 5/5 — i.e. the gate was green with slice S1's merge-blocking fix
        // reverted, which is exactly the failure class this phase exists to remove. The reason is
        // that this half detects the mutation through a RACE (a permit freed mid-burst and taken by a
        // later send), and full-suite thread-pool contention shifts the drainer Task out of that
        // window; part (i) cannot compensate, because with the bound frozen TryAppendOne's refusal is
        // over-determined, and neither can slice S2's frozen-bound close test (measured PASS 3/3
        // under the same mutation, for the same reason). So the stress half is this predicate's only
        // detector and it has to bite under contention.
        //
        // Two candidate repairs were measured; only the second is used. Moving the drainer from a
        // pool Task to a dedicated Thread raised full-suite detection to 2/4 — still probabilistic,
        // so the thread type is not the lever. Repeating the burst with a FRESH HARNESS per attempt
        // reached 4/4 full-suite detection under the mutation and 4/4 green at HEAD, at unchanged
        // duration; that is what is implemented. Fresh per attempt because a reused harness carries
        // the previous attempt's node chain, spare node and permit state, so attempts 2..K would no
        // longer start from the saturating-burst-from-cold shape the bug needs. This is a probability
        // argument, not a proof: a DETERMINISTIC guard for this predicate is not reachable without a
        // white-box production seam, because the state it governs ("a permit is free AND a submission
        // is queued") is transient by construction — the parked submitter consumes the released
        // permit promptly. Adding such a seam is a design call, not a test fix, so it is not done
        // here. Keep the isolated re-run of the mutation as the primary evidence in any round that
        // touches the routing predicate; this loop is what keeps the gate honest between them.
        const int Burst = 200;
        const int Attempts = 8;

        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            using Harness harness = new Harness(new SendAccumulatorSettings(
                slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

            // The witness here is the per-record delivery callback's firing order, not the pending
            // node's slots: 200 records across a bound of 4 span ~50 nodes, and a node is recycled
            // long before the burst ends, so there is no single array to read. It observes the same
            // property because the chain from append to callback is order-preserving end to end —
            // CompleteNode hands the pump one group per send_batch call, holding that call's records
            // in index order (the same order send_batch read them), Enqueue appends the groups to a
            // FIFO queue, DequeueGroup dequeues FIFO, and ProcessBatch fires each pass in index
            // order. So callback order == append order == the order records reached send_batch.
            List<int> observed = new List<int>(Burst);
            Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Burst];

            using (CancellationTokenSource sending = new CancellationTokenSource())
            {
                Task drainer = Task.Run(() =>
                {
                    while (!sending.IsCancellationRequested)
                    {
                        harness.ForceDrainWithoutWaiting();
                    }
                });

                for (int i = 0; i < Burst; i++)
                {
                    sends[i] = harness.AppendOne((byte)i, new OrderRecordingDeliveryCallback(observed, i));
                }

                sending.Cancel();
                await TestTimeout.Run(() => drainer, s_deadline);
            }

            harness.DrainNow();
            await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

            // A settle window: the last callback fires immediately before its awaiter is released,
            // so WhenAll already implies all 200 ran — but read the list under its own lock anyway.
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
    public async Task SendAccumulator_TwoConsecutivelyParkedSends_AppendInCallOrder()
    {
        // The case a bare "someone is parked" counter does NOT fix, and therefore the evidence for
        // D1(a) over the cheaper option: ReleaseSpace hands out MANY permits at once, so every
        // parked send's continuation becomes runnable together and they race for _gate in Append.
        // A documented-FIFO queue drained by a SINGLE appender is what removes the race — it must
        // not rest on SemaphoreSlim fairness, which .NET explicitly does not guarantee.
        //
        // Eight parked sends rather than two, deliberately: with two, a broken implementation still
        // produces the right order half the time.
        const int Parked = 8;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: Parked, batchWindowMs: 3_000, batchChunk: 1100));

        // Fill the bound inline, so the next sends have no permit and must queue.
        Task<RecordMetadata>[] filled = harness.Append(Parked);

        // Setup precondition, asserted rather than assumed: the window has not elapsed, so no drain
        // has freed a permit yet. If it had, the sends below would go inline and this test would be
        // measuring nothing.
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        Task<RecordMetadata>[] parked = new Task<RecordMetadata>[Parked];
        TaskCompletionSource<RecordMetadata>[] queued = new TaskCompletionSource<RecordMetadata>[Parked];
        for (int i = 0; i < Parked; i++)
        {
            parked[i] = harness.AppendOne(
                (byte)(0xA0 + i), callback: null, CancellationToken.None, out queued[i]);
        }

        Assert.Equal(Parked, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // The window elapses, the batch thread takes the filled node and releases all eight permits
        // at once — the exact multi-permit release that makes independent continuations race — and
        // the single submitter appends the eight queued sends into a fresh node. The free-running
        // window then gives ~3 s before that node is taken, which is what the read below needs.
        //
        // Both halves of the predicate are load-bearing: the FILLED node also holds eight records,
        // so "the pending node holds eight" alone is satisfied before the drain has even happened.
        await PollUntil(
            () => harness.Accumulator.SendBatchCallCount == 1 && harness.PendingCount() == Parked,
            s_deadline,
            "the parked sends did not all reach a pending node after the first drain");

        // The witness: the node's Completions slots, compared BY REFERENCE against the awaiters the
        // calls created, in call order. This is the array send_batch's companion loop walks.
        TaskCompletionSource<RecordMetadata>?[] slots = harness.PendingCompletions();
        for (int i = 0; i < Parked; i++)
        {
            Assert.Same(queued[i], slots[i]);
        }

        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
        await TestTimeout.Run(() => Task.WhenAll(parked), s_deadline);
    }

    [Fact]
    public async Task Flush_IncludesASendStillQueuedForSpace()
    {
        // M11/P3.2 §3.4 item 1: "empty and idle" now has TWO stages, because a record whose Send
        // has ALREADY RETURNED can be sitting in the submission queue while the node chain is
        // empty. A predicate that looked only at the chain would let this drain return with those
        // records unsent — silently re-opening the gap M11/P3.1 §3.5 closed on purpose (flush()
        // returning while records the caller believes were sent have not reached the core).
        //
        // DrainPending IS Flush's accumulator drain (NativeProducer.FlushWithAccumulatorDrainBound
        // calls it), driven here with explicit settings so the bound actually saturates — the public
        // producer's bound is 1000 records.
        const int Queued = 20;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 2, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] appended = harness.Append(2);

        Task<RecordMetadata>[] queued = new Task<RecordMetadata>[Queued];
        for (int i = 0; i < Queued; i++)
        {
            queued[i] = harness.AppendOne((byte)(0xB0 + i));
        }

        // A bound of 2 against 20 queued sends: each drain frees two permits, so the chain empties
        // and refills many times over. A predicate that returned at the first empty chain would
        // have to guess right twenty times to pass this.
        Assert.Equal(Queued, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(0, harness.HistoryCount);

        harness.DrainNow();

        // Asserted SYNCHRONOUSLY after the drain returns — that is the contract being tested.
        Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(Queued + 2, harness.HistoryCount);
        Assert.Equal(Queued + 2, harness.Accumulator.SendBatchRecordCount);

        await TestTimeout.Run(() => Task.WhenAll(appended), s_deadline);
        await TestTimeout.Run(() => Task.WhenAll(queued), s_deadline);

        // The ASYNC drain — what the async Flush awaits — rides the same two-stage predicate, but
        // reaches it through a different site: its waiters are released by SignalIdleLocked, not by
        // DrainPending's loop, so the blocking half above leaves that site ungraded. Asserted on a
        // FROZEN bound, because that is the only shape that makes it deterministic: with the
        // permits consumed and nothing accumulated, no drain can ever free capacity, so the queued
        // submissions cannot append and SignalIdleLocked is guaranteed to be called with an empty
        // chain and a non-empty queue — the exact state whose stage-one term is under test. (On a
        // releasable bound the submitter usually appends before that call runs, so the single-stage
        // form passes; verified by mutation.)
        Harness frozen = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));
        frozen.ConsumePermits(4);

        Task<RecordMetadata>[] stuck = new Task<RecordMetadata>[3];
        for (int i = 0; i < stuck.Length; i++)
        {
            stuck[i] = frozen.AppendOne((byte)(0xD0 + i));
        }

        Assert.Equal(3, frozen.Accumulator.QueuedSubmissionCount);

        Task drain = frozen.Accumulator.DrainPendingAsync(CancellationToken.None);

        // A settle window, not a poll: the assertion is that something must NOT happen, so it needs
        // time to have happened. DrainPendingAsync armed the force flag and pulsed, so the batch
        // thread wakes at once, finds the chain empty, takes nothing and calls SignalIdleLocked —
        // the site under test — well inside this window.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(0, frozen.Accumulator.SendBatchCallCount);
        Assert.False(
            drain.IsCompleted,
            "the async drain reported idle while three sends were still queued for capacity");

        // Teardown empties the queue, which is also what releases the drain — it resolves rather
        // than hanging forever once the second stage empties. Since M11/P3.2 slice S2 it empties by
        // FLUSHING the three into the chain (bypassing the frozen bound) rather than by faulting
        // them, so the drain now resolves on genuine idle: the flush appends, the final take sends,
        // and only then is the chain empty with nothing queued.
        Assert.True(frozen.Accumulator.Stop(s_deadline), "the batch thread did not exit");
        await TestTimeout.Run(() => drain, s_deadline);
        Assert.Equal(0, frozen.Accumulator.QueuedSubmissionCount);
        Assert.Equal(3, frozen.HistoryCount);

        foreach (Task<RecordMetadata> send in stuck)
        {
            await TestTimeout.Run(() => send, s_deadline);
        }

        frozen.Dispose();
    }

    [Fact]
    public async Task Teardown_WhoseQueueFlushExpires_StillSettlesEverySubmissionExactlyOnce()
    {
        // M11/P3.2 §3.4 item 3: nothing may be left holding an unsettled TaskCompletionSource.
        // Slice S2 made the NORMAL teardown outcome a send (see
        // Close_CompletesASendThatWasQueuedForSpace_RatherThanFaultingIt), so what this test now
        // guards is the flush's DEFINED DEGRADED OUTCOME: when the flush gets no budget, every
        // queued submission is still settled exactly once — faulted, nothing reaching the core, no
        // delivery notification owed — rather than stranded. That is the property the bound exists
        // to buy, and without a test for it the expiry branch is unobserved.
        //
        // Both injections are needed to make it deterministic. The frozen bound (see part (i) of
        // the ordering test) keeps the submissions queued rather than appended; the stalled
        // submitter keeps them there THROUGH teardown, so the cancelled gate cannot let one slip
        // into a bypass append before _closed is set. Without the stall the submission the
        // submitter had already dequeued would be sent or faulted depending on that race, which is
        // not a contract to assert either way.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

        harness.ConsumePermits(4);
        harness.StallTheSubmitter();

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[3];
        for (int i = 0; i < sends.Length; i++)
        {
            sends[i] = harness.AppendOne((byte)(0xC0 + i), callback);
        }

        Assert.Equal(3, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(0, harness.HistoryCount);

        // Nothing is pinned while queued: pins live only in a node's slots, and there is no node.
        Assert.Equal(0, harness.PendingCount());

        // A zero bound leaves the flush no budget at all, so it expires immediately. The return
        // value is deliberately NOT asserted: the join also gets zero, and with an empty chain the
        // batch thread can wake on the _closed pulse and exit while the terminal sweep is still
        // running, so either answer is legitimate here. What the flush's expiry means is asserted
        // below, on the submissions themselves.
        _ = harness.Accumulator.Stop(TimeSpan.Zero);

        // Asserted synchronously: with the submitter stalled, Stop's own terminal sweep is the only
        // thing that can settle these, so the count is zero by the time it returns.
        Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);

        foreach (Task<RecordMetadata> send in sends)
        {
            ObjectDisposedException failure = await Assert.ThrowsAsync<ObjectDisposedException>(
                () => TestTimeout.Run(() => send, TimeSpan.FromSeconds(10)));
            Assert.Contains(nameof(NativeProducer), failure.Message, StringComparison.Ordinal);
        }

        // No record reached the core, so no delivery notification is owed (M11/P3.1 D5) — asserted
        // after a settle window, since a first observation of 0 cannot rule out a later one.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(0, callback.Count);
        Assert.Equal(0, harness.HistoryCount);
        Assert.Equal(0, harness.Accumulator.SendBatchRecordCount);

        // And the SEAL, which is what makes the flush wait terminate at all: a send arriving after
        // teardown sealed the queue is refused by SubmitQueued and settled there. With the
        // submitter stalled nothing else in the system can settle it, so this assertion is the
        // seal's own witness — drop the seal check and the task below is never completed.
        Task<RecordMetadata> afterTheSeal = harness.AppendOne(0xCF);
        Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => afterTheSeal, TimeSpan.FromSeconds(10)));

        harness.Dispose();
    }

    [Fact]
    public async Task Close_CompletesASendThatWasQueuedForSpace_RatherThanFaultingIt()
    {
        // ⚠ THE F2 REGRESSION TEST (M11/P3.2 slice S2). Before this slice, Stop cancelled the
        // backpressure gate and every submission still queued for capacity was FAULTED with an
        // ObjectDisposedException — the record never reached the core at all, although its Send had
        // already returned to the caller. The anchor keeps that record: py_Producer_send appends
        // unconditionally BEFORE any waiting (_confluentkafka.c:819-822, counter :823, `full` only
        // at :830), so on close it is already accumulated and py_Producer_shutdown's final
        // take-and-send ships it (closed=1 at :962, waiters taken :966, send thread signalled :967,
        // joined :969, waiters fired :972; py_Producer_on_space_available reports "available" the
        // moment closed is set, :857-861, so a waiter never parks through a close).
        //
        // ⚠ Python's version of this guarantee is almost unconditional, not unconditional; .NET's
        // IS unconditional, which is what the three assertions below pin. How Python's window
        // arises — which mutex, which unlock sites, and how wide it actually is — is derived ONCE,
        // in SendAccumulator.AppendQueuedAsync's remarks (M11/P3.2 PLAN §1.3), and is deliberately
        // NOT restated here: this comment was a third copy of that argument and outlived two
        // corrections of the canonical one (Critic 71 findings 71.10 / 71.11, and 71.12 for this
        // copy). One statement is the only statement.
        //
        // Three assertions, because any one alone passes on a wrong implementation: the record
        // reaches the core (a fault would not), its Task completes successfully (an appended record
        // whose awaiter was already faulted would not), and its delivery callback fires EXACTLY
        // once (a count, not "it ran" — exactly-once is what appending-after-faulting breaks).
        //
        // ⚠ No assertion on an exception message anywhere here: a faulted teardown send and an
        // accepted-residual send carry the identical ObjectDisposedException text containing
        // "closed", so no message can tell them apart (STATUS.md:20).
        const int Queued = 3;
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

        // Frozen bound: the permits are consumed with nothing accumulated, so no drain can ever
        // free capacity and these submissions stay queued until teardown. That also makes the
        // bypass's permit accounting observable — a bypass append that charged itself against the
        // bound would make the batch thread over-release and die with a SemaphoreFullException.
        harness.ConsumePermits(4);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Queued];
        for (int i = 0; i < Queued; i++)
        {
            sends[i] = harness.AppendOne((byte)(0xE0 + i), callback);
        }

        Assert.Equal(Queued, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(0, harness.HistoryCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // Teardown flushes the queue into the chain, then closes, then joins — so the batch thread
        // reports as drained rather than abandoned.
        Assert.True(harness.Accumulator.Stop(s_deadline), "the batch thread did not exit");

        // (1) The records reached the core. Asserted SYNCHRONOUSLY after Stop returns: the flush
        // and the final drain both completed inside it, which is the ordering under test.
        Assert.Equal(Queued, harness.HistoryCount);
        Assert.Equal(Queued, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);

        // (2) Every Task completed SUCCESSFULLY. This is the assertion the restored fault path
        // fails on.
        foreach (Task<RecordMetadata> send in sends)
        {
            // The guard first (so a never-settled send fails fast rather than hanging), then the
            // already-resolved await for the value.
            await TestTimeout.Run(() => send, s_deadline);
            RecordMetadata metadata = await send;
            Assert.Equal(Topic, metadata.Topic);
        }

        // (3) Exactly one delivery notification per record, and it came from the pump's success
        // path rather than from a fault. Asserted after a settle window, since a first observation
        // of three cannot rule out a fourth arriving late.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(Queued, callback.Count);
        Assert.Null(callback.LastException);
        Assert.Equal(Queued, harness.DrainedSendCount);

        harness.Dispose();
    }

    [Fact]
    public async Task Close_FlushesQueuedSubmissionsToSendBatchInCallOrder()
    {
        // ⚠ F1 ON THE TEARDOWN PATH (M11/P3.2 §F1 × slice S2; Critic 71 finding 71.8). S2's own
        // rationale claims — at SendAccumulator.Stop's step-2 item — that having the SUBMITTER do
        // the bypass appends rather than the teardown thread is what keeps S1's single-appender
        // invariant "and therefore its call-order property" true through teardown. Nothing asserted
        // it. Every S2 test asserts records ARRIVE (HistoryCount, SendBatchRecordCount, callback
        // counts, successful Tasks), and part (i) of the steady-state ordering test derives order
        // from "nothing was appended while queued" — which is the MECHANISM, not the order. So a
        // submitter mutated to dequeue LIFO once _queueSealed is set left the whole suite green:
        // every record still reached the core exactly once, every awaiter still succeeded, every
        // delivery callback still fired exactly once, a stalled submitter still appended nothing —
        // only the order was wrong. This is the assertion that bites on that mutation, on the one
        // append path S2 introduces (the bypass under a sealed queue).
        //
        // The witness is the per-record delivery-callback firing order — the same witness the
        // stress half of SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound
        // uses, valid for the same reason: the chain from append to callback is order-preserving end
        // to end (send_batch reads a node's slots in index order, CompleteNode hands the pump one
        // group per send_batch call holding that call's records in that order, Enqueue/DequeueGroup
        // are FIFO over the groups, ProcessBatch fires each pass in index order), so callback order
        // == append order == the order records reached send_batch. It is
        // also the only witness available here: the node the flush fills is taken, sent and recycled
        // INSIDE Stop, so there is no PendingCompletions() array left to read afterwards, and the
        // mock core exposes a record count rather than a history.
        const int Queued = 5;
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

        // The frozen bound, as in Close_CompletesASendThatWasQueuedForSpace_RatherThanFaultingIt:
        // the permits are consumed with nothing accumulated, so no drain can ever free capacity and
        // all five submissions stay queued until teardown flushes them. Deterministic — the order
        // under test is fixed by the enqueue, not by a race against a release.
        harness.ConsumePermits(4);

        List<int> observed = new List<int>(Queued);
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Queued];
        for (int i = 0; i < Queued; i++)
        {
            sends[i] = harness.AppendOne(
                (byte)(0xE0 + i), new OrderRecordingDeliveryCallback(observed, i));
        }

        Assert.Equal(Queued, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(0, harness.HistoryCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // Teardown: seal, flush the queue into the chain through the submitter, close, join.
        Assert.True(harness.Accumulator.Stop(s_deadline), "the batch thread did not exit");

        // Arrival first — not the property under test, but without it the order loop below could be
        // satisfied by a short list, and a zero-match assertion is not evidence.
        Assert.Equal(Queued, harness.HistoryCount);
        Assert.Equal(Queued, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);

        // Each callback fires immediately BEFORE its own awaiter is released (M14/P1 ordering), so
        // awaiting all five already implies all five ran; the settle window then rules out a sixth.
        foreach (Task<RecordMetadata> send in sends)
        {
            await TestTimeout.Run(() => send, s_deadline);
        }

        await Task.Delay(TimeSpan.FromMilliseconds(250));

        int[] order;
        lock (observed)
        {
            order = observed.ToArray();
        }

        // THE assertion: the flushed records reached send_batch in CALL order — not merely that all
        // of them did. The length assertion is the one that keeps the loop from being vacuous.
        Assert.Equal(Queued, order.Length);
        for (int i = 0; i < Queued; i++)
        {
            Assert.Equal(i, order[i]);
        }

        harness.Dispose();
    }

    [Fact]
    public async Task Close_WithQueuedSubmissions_DoesNotExceedTheBoundedTeardownWait()
    {
        // M11/P3.2 §F2(c) — the flush must not turn Stop's bounded wait into an unbounded one, and
        // must not push the join past its bound either. The bound is ONE budget for both stages
        // (P3.1 §6.3: bounded waits with a stated expiry outcome, never a hang), so this asserts
        // the TOTAL, which is the only thing a caller can observe.
        //
        // Two injections, and each makes one stage consume its share:
        //   * the stalled submitter means nothing can drain the submission queue, so the flush wait
        //     runs to its deadline instead of returning at once;
        //   * the parked batch thread (the delivery-callback park, the same lever
        //     Flush_WhenTheAccumulatorDrainExpires_… uses) means the join cannot succeed either.
        // With one shared deadline the total is ~Bound. Give each stage its own Bound instead and
        // it becomes ~2x; make the flush wait unbounded and it never returns — which is why the
        // whole call is run under a hard timeout that FAILS rather than hangs.
        TimeSpan bound = TimeSpan.FromSeconds(2);

        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);

        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));
        Task<RecordMetadata>? parked = null;
        Task<RecordMetadata>? queued = null;
        try
        {
            // Closing the CORE producer makes send_batch reject every record per index, and
            // CompleteNode fires the delivery callback for such a record ON THE BATCH THREAD — the
            // accumulator's only call-out into user code, and so the only broker-free lever on that
            // thread's progress.
            harness.CloseCoreProducer();
            parked = harness.AppendOne(0x01, new BlockingDeliveryCallback(entered, release));
            harness.ForceDrainWithoutWaiting();
            Assert.True(
                entered.Wait(s_deadline), "the batch thread never entered the delivery callback");

            // The drain above returned this record's permit before parking, so re-consume the
            // whole bound: the submission below must have no permit and must therefore queue.
            harness.ConsumePermits(4);
            harness.StallTheSubmitter();

            queued = harness.AppendOne(0x02);
            Assert.Equal(1, harness.Accumulator.QueuedSubmissionCount);

            bool drained = true;
            Stopwatch elapsed = Stopwatch.StartNew();
            TestTimeout.Run(
                () => drained = harness.Accumulator.Stop(bound), TimeSpan.FromSeconds(20));
            elapsed.Stop();

            Assert.False(drained, "the parked batch thread cannot have exited");
            Assert.True(
                elapsed.Elapsed < TimeSpan.FromSeconds(3),
                $"teardown took {elapsed.Elapsed} against a {bound} bound — the flush and the " +
                "join are not sharing one deadline");

            // The degraded outcome, and the reason expiry is not a leak: the terminal sweep settled
            // the submission the flush could not append, and nothing it settled reached the core.
            Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);
            Assert.True(queued.IsCompleted, "the queued submission was left unsettled");
            Assert.Equal(0, harness.HistoryCount);
        }
        finally
        {
            // Always release: the parked batch thread would otherwise hold the harness's own
            // teardown, and the event it waits on is disposed on the way out of this method.
            release.Set();
        }

        // Observed out here rather than in the finally so they can be awaited (xUnit1031 forbids
        // blocking on them). The parked record was rejected by the closed core; the queued one is
        // the flush's degraded fault.
        Assert.NotNull(parked);
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => parked!, s_deadline));
        Assert.NotNull(queued);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => queued!, s_deadline));

        harness.Dispose();
    }

    [Fact]
    public async Task QueuedSubmission_CancelledBeforeAppend_IsNotSentAndCancelsWithTheCallerToken()
    {
        // M11/P3.2 §3.4 item 4: a queued submission whose token fires before it is appended must
        // cancel WITH the caller's token and must NOT be appended — the pre-queue slow path's
        // behaviour, preserved.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 2, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] appended = harness.Append(2);

        using CancellationTokenSource cancellation = new CancellationTokenSource();
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<RecordMetadata> cancelled = harness.AppendOne(
            0xD0, callback, cancellation.Token, out _);

        Assert.Equal(1, harness.Accumulator.QueuedSubmissionCount);
        Assert.False(cancelled.IsCompleted);

        cancellation.Cancel();

        // The drain is taken FIRST, deliberately: it frees the capacity this submission was waiting
        // for, which is the only moment a cancellation that is merely *observed* rather than
        // *enforced* could let the record through. The drain also waits for the second idle stage,
        // so it does not return until the cancellation has been accounted out — which makes "two
        // records reached the core, not three" the deterministic discriminator, reached before any
        // await on the cancelled task could mask it.
        harness.DrainNow();
        Assert.Equal(0, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(2, harness.HistoryCount);
        Assert.Equal(2, harness.Accumulator.SendBatchRecordCount);

        OperationCanceledException failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => cancelled, s_deadline));
        Assert.Equal(cancellation.Token, failure.CancellationToken);

        await TestTimeout.Run(() => Task.WhenAll(appended), s_deadline);

        // Nothing reached the core for the cancelled send, so no delivery notification is owed.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(0, callback.Count);
    }

    // ------------------------------------------- the admission bound (M11/P3.3 §8.1) ------------

    [Fact]
    public async Task Admission_IsBounded_WhenTheFloodOutrunsTheDrain()
    {
        // ⚠ THE PHASE'S PRIMARY GATE, and the DoD item it exists to add (§8.1). M11/P3.2's ordering
        // fix made the submission queue THE path under sustained load — once one send is queued
        // TrySubmitInline refuses every later one — and nothing bounded its depth, so the client
        // accepted sends without limit: 2.05 M records in flight, p50 3,524 ms, RSS 2.04 GiB,
        // against 41 ms / 239 MB immediately before it. A correctness-only suite cannot see that:
        // every record is still delivered, in order, exactly once, so the suite passes and the
        // client bloats. That is why this asserts a POPULATION rather than an outcome.
        //
        // ⚠ AND WHY THE WITNESS IS NOT QueuedSubmissionCount. §2.3 falsified "cap the queue"
        // experimentally: with the queue path bypassed (MAX_ACCUMULATED=4000000, so TryAcquireSpace
        // always wins and _queued never leaves 0) the identical bloat reappeared in the NODE CHAIN —
        // p50 3,436 ms, RSS 2.04 GiB. The quantity of interest is the whole ADMITTED population,
        // which is what AdmittedRecordCount reports and what the bound covers.
        //
        // THE REGIME IS DETERMINISTIC, not a race. A 60 s window with no drainer means NOTHING ever
        // frees an admission permit, so exactly Cap sends can be admitted and every later one must
        // wait out max.block.ms and fail. The mutation (an admission that does not wait) admits all
        // Senders*PerSender of them, so the population reads 3x the cap.
        //
        // The burst is nonetheless repeated with a FRESH harness per attempt, for the reason
        // recorded at SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound: an
        // isolated PASS is not evidence a guard is absent, nor a suite PASS that it is present, and
        // a reused harness would start attempts 2..K from the previous attempt's chain, spare node
        // and permit state rather than from cold.
        const int Cap = 16;
        const int Space = 4;
        const int Senders = 4;
        const int PerSender = 12;
        const int Attempts = 8;

        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            using Harness harness = new Harness(new SendAccumulatorSettings(
                slotThreshold: 1000,
                maxAccumulatedRecords: Space,
                batchWindowMs: 60_000,
                batchChunk: 1100,
                maxAdmittedRecords: Cap,
                maxBlockMs: 20));

            // Sampled DURING the flood as well as asserted after it: the after-assertion alone
            // would pass an implementation that admitted everything and then shed records, and the
            // peak is what "the population stays within the bound" actually means. One writer, read
            // only after the sampler has been awaited.
            int peak = 0;
            using CancellationTokenSource flooding = new CancellationTokenSource();
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

            Task<int>[] floods = new Task<int>[Senders];
            for (int s = 0; s < Senders; s++)
            {
                int sender = s;
                floods[sender] = Task.Run(
                    () =>
                    {
                        int refused = 0;
                        for (int i = 0; i < PerSender; i++)
                        {
                            // Production's own entry point, blocking exactly as production's
                            // Send blocks (DoD §12 — the fixture holds no copy of the rule).
                            Task<RecordMetadata> send =
                                harness.AppendOne((byte)((sender * PerSender) + i));

                            // ⚠ A REFUSAL IS A FAULTED TASK, NOT A THROW (Critic 72 finding 72.1).
                            // This loop used to count `catch (KafkaException)`; the max.block.ms
                            // expiry now faults the record's Task and returns, which is Java's
                            // buffer-exhausted outcome. The state is observable the instant
                            // AppendOne returns — TrySetException transitions the Task
                            // synchronously, and RunContinuationsAsynchronously defers only the
                            // continuations — so this stays a deterministic count rather than a
                            // poll. Observing `Exception` also keeps the fault from surfacing later
                            // as an UnobservedTaskException; an ACCEPTED send's Task is still
                            // pending here (nothing drains under the 60 s window), which is exactly
                            // what makes IsFaulted the discriminator.
                            if (send.IsFaulted)
                            {
                                refused++;
                                AggregateException? fault = send.Exception;
                                if (fault?.InnerException is not KafkaException)
                                {
                                    // Fail the flood task loudly rather than silently counting a
                                    // refusal of the wrong kind.
                                    throw new InvalidOperationException(
                                        "a refused send faulted with something other than a " +
                                        "KafkaException: " + fault?.InnerException);
                                }
                            }
                        }

                        return refused;
                    },
                    CancellationToken.None);
            }

            int[] refusedPerSender = new int[Senders];
            await TestTimeout.Run(
                async () => refusedPerSender = await Task.WhenAll(floods).ConfigureAwait(false),
                s_deadline);

            flooding.Cancel();
            await TestTimeout.Run(() => sampler, s_deadline);

            int refusedTotal = 0;
            foreach (int refused in refusedPerSender)
            {
                refusedTotal += refused;
            }

            // (1) Exactly Cap sends were accepted — there are exactly Cap permits and nothing can
            // return one, so this is an equality rather than a bound. (A "refusal" is a faulted
            // returned Task, counted in the flood loop above — see the note there.)
            Assert.Equal(Cap, (Senders * PerSender) - refusedTotal);

            // (2) The population the bound is about. THE assertion: 16 with the gate, 48 without.
            Assert.Equal(Cap, harness.Accumulator.AdmittedRecordCount);

            // (3) And it never exceeded the bound while the flood was running. Cap + 1 rather than
            // Cap because the single submitter's in-flight submission is counted in both terms for
            // the window between its append and its accounting — see AdmittedRecordCount.
            Assert.True(
                peak <= Cap + 1,
                $"the admitted population peaked at {peak} against a bound of {Cap} — the " +
                "admission wait did not throttle the flood");

            // (4) Nothing has reached the core: the 60 s window is what makes the regime
            // deterministic, so if this is non-zero the setup drained and the test measured nothing.
            Assert.Equal(0, harness.HistoryCount);
        }
    }

    [Fact]
    public async Task Admission_SaturatedBound_ParksTheCaller_AndTheDrainReleasesIt()
    {
        // The blocking contract itself, written to the BLOCKING shape from the start (PLAN §11
        // risk 4): the parked send is issued from its OWN task and the assertion is on that task,
        // never `parked.IsCompleted == false` over an inline call — which is what hung about half
        // the cap tests on the sibling branch.
        //
        // A 60 s window means only an explicit drain can free capacity, and a 60 s max.block.ms
        // means the parked caller cannot time out instead — so "parked" and "released" are both
        // properties of the gate rather than of a timer.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 4,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 4,
            maxBlockMs: 60_000));

        Task<RecordMetadata>[] filled = harness.Append(4);
        Assert.Equal(4, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // The non-blocking probe is refused — and refusing it must not consume a permit, which the
        // two assertions around it pin.
        Assert.Null(harness.TryAppendOne(0xF0));
        Assert.Equal(4, harness.Accumulator.AdmittedRecordCount);

        // The blocking one PARKS. A settle window, not a poll: the assertion is that something must
        // NOT have happened, so it needs time in which to have happened.
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF1, CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(
            admission.IsCompleted,
            "the fifth send was admitted although the producer already held its whole bound");
        Assert.Equal(4, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // The drain takes the chain, which is what returns the admission permits.
        harness.DrainNow();

        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> parked = await admission;

        // It was admitted, not dropped: the record reaches the core like any other.
        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
        await TestTimeout.Run(() => parked, s_deadline);
        Assert.Equal(5, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(5, harness.HistoryCount);
    }

    [Fact]
    public async Task Admission_WhenTheBoundStaysSaturated_FaultsTheSendAfterMaxBlockMs_WithoutThrowing()
    {
        // ── JAVA'S BUFFER-EXHAUSTED OUTCOME, all three axes (Critic 72 finding 72.1) ────────────
        // BufferPool.allocate throws BufferExhaustedException on expiry (BufferPool.java:161),
        // reached from KafkaProducer.doSend's try via RecordAccumulator.java:333 <-
        // KafkaProducer.java:1029-1030 (remainingWaitMs from max.block.ms at :995). That exception
        // extends TimeoutException -> RetriableException -> ApiException, so doSend's
        // `catch (ApiException e)` at :1049-1061 handles it: it fires the callback with the -1
        // placeholder and returns a FAILED FUTURE (:1061) rather than rethrowing. So:
        //
        //   1. Send RETURNS (it does not throw)               — the `admission` task completing;
        //   2. the record's Task FAULTS with a KafkaException — asserted below;
        //   3. the exception is RETRIABLE with a real code    — asserted below.
        //
        // Before the fix the binding threw synchronously with Code == 0 / IsRetriable == false, and
        // nothing in the suite asserted the classification, so all three were unguarded.
        // The delivery callback (axis 4 of the same Java line) has its own test, below.
        const int MaxBlockMs = 250;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 2,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2,
            maxBlockMs: MaxBlockMs));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Assert.Equal(2, harness.Accumulator.AdmittedRecordCount);

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF3, CancellationToken.None);

        // ⚠ AWAITED FOR ITS *COMPLETION*, NOT FOR A FAULT — that IS assertion 1. The outer task
        // carries whatever `SubmitAdmitted` threw, so had the expiry stayed a synchronous throw
        // this await would itself have faulted with the KafkaException and the test would be red
        // here rather than measuring the record's Task below.
        await TestTimeout.Run(() => admission, s_deadline);
        elapsed.Stop();
        Task<RecordMetadata> refused = await admission;

        // ⚠ THE DISCRIMINATOR between "waited, then failed" and "failed immediately". Without it an
        // admission that never blocks at all passes every other assertion here. The lower bound is
        // deliberately loose (a scheduler can overshoot but not undershoot a semaphore timeout).
        Assert.True(
            elapsed.Elapsed >= TimeSpan.FromMilliseconds(MaxBlockMs / 2),
            $"the saturated send failed after {elapsed.Elapsed}, well inside the {MaxBlockMs} ms " +
            "max.block.ms — it did not wait for capacity at all");

        // Assertion 2: the record's own Task is what carries the failure.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => refused, s_deadline));

        // The message is asserted (DoD §3), because a flat KafkaException's code alone cannot tell
        // an operator which knob to move.
        Assert.Contains("max.block.ms", failure.Message, StringComparison.Ordinal);
        Assert.Contains("250 ms", failure.Message, StringComparison.Ordinal);
        Assert.Contains("2 records", failure.Message, StringComparison.Ordinal);

        // Assertion 3: Java's classification. `IsRetriable` is the load-bearing one — the idiomatic
        // `catch (KafkaException e) when (e.IsRetriable)` retry must match, as it does in Java —
        // and Code != 0 matters because 0 is the protocol's NONE, i.e. "success" on an exception.
        Assert.True(failure.IsRetriable, "Java's BufferExhaustedException is a RetriableException");
        Assert.False(failure.IsFatal);
        Assert.Equal(7, failure.Code);   // REQUEST_TIMED_OUT, whose Java exception IS TimeoutException

        // The refused send neither took a permit nor invented one — asserted on the permit count as
        // well as on the record count, because an expiry that released a permit it never held would
        // move only the first (and would over-release the semaphore on the batch thread later).
        Assert.Equal(2, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(0, harness.Accumulator.AvailableAdmissions);

        // And it was not sent — asserted on the CORE's record count, not on an awaiter.
        harness.DrainNow();
        Assert.Equal(2, harness.HistoryCount);
        Assert.Equal(2, harness.Accumulator.SendBatchRecordCount);
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
    }

    [Fact]
    public async Task Admission_WhenTheBoundStaysSaturated_FiresTheDeliveryCallback_WithThePlaceholder()
    {
        // Axis 4 of the same Java line (Critic 72 finding 72.1, item 2): doSend's
        // `catch (ApiException e)` fires the callback BEFORE returning the failed future
        // (KafkaProducer.java:1051-1055), with `new RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1,
        // -1)` — the -1 placeholder Callback.java:28-33 documents. Before the fix this outcome
        // fired NOTHING, on the rationale "neither accepted anything, so neither owes a
        // notification" — which is exactly the rationale Java rejects here: its buffer-exhausted
        // path accepted nothing either and still fires.
        //
        // ⚠ Async surface only, deliberately: the expiry is the ADMISSION bound's, and only the
        // async Send has an admission bound (the sync Send hands the record to the core inside the
        // call and blocks on the core's own buffer.memory). So ffi §A6 form C's "run every
        // behavioural test against both flavors" has nothing to run on the sync side here.
        const int MaxBlockMs = 250;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 2,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2,
            maxBlockMs: MaxBlockMs));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Assert.Equal(2, harness.Accumulator.AdmittedRecordCount);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF4, CancellationToken.None, callback);
        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> refused = await admission;

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => refused, s_deadline));

        // Exactly once, after a SETTLE WINDOW: a first observation of "1" cannot distinguish one
        // invocation from two (ffi §A6 form C's tests-required list).
        await Task.Delay(TimeSpan.FromMilliseconds(150));
        Assert.Equal(1, callback.Count);

        // The placeholder, NOT null — and built by DeliveryRegistration.Fire, the one site that
        // owns that construction. (M14/P1 shipped a second construction whose negative partition
        // tripped TopicPartition's guard inside Fire's no-throw swallow, making the callback's
        // effect silently absent; reusing Fire is what forecloses that here. The count assertion
        // above is what would catch it: an exception inside Fire is swallowed, so a hand-rolled
        // placeholder that threw would leave Count at 0 with everything else still green.)
        Assert.NotNull(callback.LastMetadata);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
        Assert.Equal(-1L, callback.LastMetadata.Offset);
        Assert.Equal(-1L, callback.LastMetadata.Timestamp);

        // The SAME object on both surfaces — one failure driving both, as in Java, where one
        // completeFutureAndFireCallbacks both resolves the future and fires the callbacks.
        Assert.Same(failure, callback.LastException);

        harness.DrainNow();
        Assert.Equal(2, harness.HistoryCount);
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
    }

    [Fact]
    public async Task Admission_WhenTheBoundStaysSaturated_FiresTheCallbackBeforeFaultingTheTask()
    {
        // ── D3 ORDERING AT THE EXPIRY FIRING SITE (Critic 72 finding 72.15) ─────────────────────
        // Java sets the future's value, fires the callbacks, and only THEN releases the future's
        // waiters (ProducerBatch.java:303-323 — produceFuture.done() is last), and CLAUDE.md §4
        // records that ordering as the binding's contract. SubmitAdmitted's expiry branch keeps it
        // (Fire, then TrySetException), but nothing asserted it: with the two statements swapped the
        // whole suite passed 921/921, 0/3 detected. This is the test that discriminates them.
        //
        // ⚠ THE PROBE IS THE AWAITER'S OWN COMPLETION STATE, READ FROM INSIDE THE CALLBACK — ffi
        // §A6 form C's "deterministic probe, not a ticket comparison". TrySetException transitions
        // the Task synchronously (RunContinuationsAsynchronously only defers the CONTINUATION), so
        // `IsCompleted` inside OnCompletion is true if and only if the awaiter was released first.
        //
        // ⚠ The public probe shape cannot reach this site. Ordering_Async_CallbackRunsBeforeThe-
        // TaskIsCompleted assigns `probe.Task = sendTask` AFTER Send returns, which works only
        // because the pump fires later; here the callback fires INSIDE the submit call, so the
        // awaiter has to be handed over before it — hence AppendOneObservingItsCompletion, which is
        // AppendOne with the TCS created first. Everything else stays production's own primitive:
        // the same SubmitAdmitted entry point, the same DeliveryRegistration (DoD §12).
        const int MaxBlockMs = 250;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 2,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2,
            maxBlockMs: MaxBlockMs));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Assert.Equal(2, harness.Accumulator.AdmittedRecordCount);

        CompletionStateProbeDeliveryCallback probe = new CompletionStateProbeDeliveryCallback();
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneObservingItsCompletionFromAnotherThread(0xF6, probe);

        await TestTimeout.Run(() => admission, s_deadline);
        Task<RecordMetadata> refused = await admission;

        // The expiry itself, so a green run cannot mean "the branch was never taken".
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => refused, s_deadline));

        // The vacuity guard: an uninvoked probe records nothing, and every assertion below would
        // then pass for the wrong reason (this is the same hazard TaskWasNull closes on the public
        // ordering test; here the awaiter is ctor-injected, so "observed a null Task" is not
        // reachable — only "was never invoked" is).
        Assert.True(probe.WasInvoked, "the expiry did not fire the delivery callback at all");
        Assert.Same(refused, probe.ObservedTask);

        // The assertion the mutation flips: the awaiter must NOT yet be completed.
        Assert.False(
            probe.TaskWasCompleted,
            "the record's Task was already completed when the delivery callback ran — the callback " +
            "must fire BEFORE the awaiter is released (ProducerBatch.java:303-323)");

        harness.DrainNow();
        Assert.Equal(2, harness.HistoryCount);
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
    }

    [Fact]
    public async Task Admission_CallerTokenFiresWhileParked_CancelsWithThatToken_AndNothingIsSent()
    {
        // The caller's token must abort the admission wait and be REPORTED, not a linked
        // substitute, so `catch (OperationCanceledException e) when (e.CancellationToken == ct)`
        // matches — the same contract QueuedSubmission already carries for the queued route.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 2,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2,
            maxBlockMs: 60_000));

        Task<RecordMetadata>[] filled = harness.Append(2);

        using CancellationTokenSource cancellation = new CancellationTokenSource();
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF5, cancellation.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send was admitted although the bound was full");

        cancellation.Cancel();

        OperationCanceledException failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => admission, s_deadline));
        Assert.Equal(cancellation.Token, failure.CancellationToken);

        // Nothing was appended, so nothing can reach the core — asserted on the core's record count
        // rather than only on the awaiter's state, and AFTER a drain that would have sent it.
        harness.DrainNow();
        Assert.Equal(2, harness.HistoryCount);
        Assert.Equal(2, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);

        // The cancelled caller took no permit, so the drain hands back exactly the two the filled
        // records held — a leak here would read 1, which the record count above cannot see.
        Assert.Equal(2, harness.Accumulator.AvailableAdmissions);
        await TestTimeout.Run(() => Task.WhenAll(filled), s_deadline);
    }

    [Fact]
    public async Task Admission_ParkedCaller_IsReleasedByStop_AndDisposeReturns()
    {
        // PLAN §11 risk 2 — the phase's own top risk: a caller blocked on admission that teardown
        // does not wake hangs Dispose. A 60 s max.block.ms means the timeout cannot be what
        // releases it, so this is a property of the gate.
        //
        // ⚠ NO ASSERTION ON THE MESSAGE, deliberately. Two paths produce an
        // ObjectDisposedException here and they carry different text: the gate's cancellation (the
        // expected one, since Stop cancels at step 2 while the batch thread is still parked in its
        // 60 s wait), and — if the final drain's permit release were to win the race instead — the
        // closed-accumulator refusal inside Append. The TYPE is the contract; the text is not.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 2,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2,
            maxBlockMs: 60_000));

        Task<RecordMetadata>[] filled = harness.Append(2);
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0xF4, CancellationToken.None);

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send was admitted although the bound was full");

        // Teardown must RETURN rather than hang behind the parked caller — under a hard deadline so
        // a hang fails the run instead of blocking it.
        TestTimeout.Run(
            () => Assert.True(
                harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
                "the batch thread did not exit"),
            TimeSpan.FromSeconds(20));

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => admission, TimeSpan.FromSeconds(10)));

        // Either way the record was never handed to the core: only the two filled ones were.
        Assert.Equal(2, harness.Accumulator.SendBatchRecordCount);
        await TestTimeout.Run(() => Task.WhenAll(filled), TimeSpan.FromSeconds(10));

        // And the whole teardown returns — the no-hang regression this test is named for.
        TestTimeout.Run(harness.Dispose, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Admission_ParkedCaller_IsReleasedByTheBatchThreadsFailureHandler()
    {
        // The admission twin of BatchThreadFailure_ReleasesASendWaitingForSpace, and it needs its
        // own test for the same reason that one did: the handler settles both chains and sweeps the
        // submission queue, and a caller parked on ADMISSION is in none of those. It is released
        // only by the unconditional Cancel() — never by permit arithmetic, which is precisely what
        // the failure this handler exists for (an over-release) has already corrupted.
        //
        // The setup mirrors that test's: threshold 2 against a 60 s window, so the batch thread is
        // parked in its wait loop until the injected record takes the node to the threshold and
        // wakes it. A 60 s max.block.ms means the parked caller cannot time out instead.
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 2,
            maxAccumulatedRecords: 1,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 1,
            maxBlockMs: 60_000));

        // The one admission permit and the one space permit both go to this record.
        Task<RecordMetadata> filled = harness.AppendOne(0x81);
        Assert.Equal(1, harness.Accumulator.AdmittedRecordCount);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        // Parked on admission, from its own task.
        Task<Task<RecordMetadata>> admission =
            harness.AppendOneFromAnotherThread(0x82, CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.False(admission.IsCompleted, "the send was admitted although the bound was full");

        // Kill the batch thread: this record takes the node to the threshold and wakes it, and the
        // take then reports two accumulated against the one space permit ever taken, so
        // ReleaseSpace over-releases a semaphore of one and throws between the take and the send.
        Task<RecordMetadata> injected = harness.AppendWithoutAPermit(0x83);

        // THE ASSERTION, under a hard deadline so a waiter that is never released FAILS rather than
        // hanging the run. Without the cancel this is where the run stops.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => admission, s_deadline));

        // The records the handler actually holds are still settled exactly as before.
        await AssertSettledByTheOverRelease(filled);
        await AssertSettledByTheOverRelease(injected);

        // And the thread really did take the failure path.
        Assert.True(
            harness.Accumulator.Stop(TimeSpan.FromSeconds(10)),
            "the failed batch thread did not exit");

        harness.Dispose();
    }

    [Fact]
    public void Admission_TeardownFlushedSendsReturnTheirPermits_NotJustThePermitBackedOnes()
    {
        // The bypass's own accounting (§F2 x M11/P3.3). A teardown-flushed submission is appended
        // WITHOUT a space permit, so it is deliberately excluded from _accumulated — but it DOES
        // hold an admission permit, taken when its Send was accepted long before teardown began. If
        // the two counts were folded together, one of them breaks: counting the bypassed record in
        // _accumulated makes the batch thread over-release _space (a SemaphoreFullException, i.e.
        // the very failure AbandonOnThreadFailure exists to survive, triggered by teardown itself),
        // and excluding it from the admission count leaks one permit per teardown-flushed send.
        //
        // The frozen SPACE bound is what forces every submission onto the bypass: the permits are
        // consumed with nothing accumulated, so no drain can ever free space capacity. Admission
        // permits are untouched by that, which is the whole point of their being separate.
        const int Queued = 3;
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 4,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 8,
            maxBlockMs: 60_000));

        harness.ConsumePermits(4);

        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[Queued];
        for (int i = 0; i < Queued; i++)
        {
            sends[i] = harness.AppendOne((byte)(0x90 + i));
        }

        Assert.Equal(Queued, harness.Accumulator.QueuedSubmissionCount);
        Assert.Equal(Queued, harness.Accumulator.AdmittedRecordCount);

        // Teardown flushes all three into the chain bypassing the space bound, and the final take
        // sends them. Each assertion below covers ONE direction of the accounting, and they are not
        // interchangeable:
        //
        //   * OVER-release (the bypassed record counted in _accumulated): a SemaphoreFullException
        //     on the batch thread makes Stop report `false`, so `Assert.True(Stop(...))` covers it.
        //   * LEAK (the bypassed record excluded from the admission count): only
        //     AvailableAdmissions can see it. ⚠ AdmittedRecordCount CANNOT — it is
        //     `_queued + _chainRecords`, and TakeChainLocked zeroes `_chainRecords` whether or not
        //     the ReleaseAdmission beside it runs, so it reads 0 either way. This test claimed that
        //     assertion measured the leak direction and it measured nothing: moving the
        //     unconditional `_chainRecords++` into Append's `if (chargedToBound)` block — precisely
        //     the leak this test is named for — went UNDETECTED 0/8 in-suite, while the
        //     AvailableAdmissions line below is red 3/3 under it (Expected 8, Actual 5 — the three
        //     leaked permits). Critic 72 finding 72.2.
        Assert.True(harness.Accumulator.Stop(s_deadline), "the batch thread did not exit");

        Assert.Equal(Queued, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(Queued, harness.HistoryCount);
        Assert.Equal(0, harness.Accumulator.AdmittedRecordCount);   // the chain is empty
        Assert.Equal(8, harness.Accumulator.AvailableAdmissions);   // maxAdmittedRecords: all back
        Assert.Equal(Queued, sends.Length);

        harness.Dispose();
    }

    [Fact]
    public async Task Admission_QueuedSubmissionSettledWithoutAppending_ReturnsItsPermit()
    {
        // The release site for a submission that never reaches a node — the cancel / fault / sweep
        // path through ReleaseQueuedSlot. It needs its own test because the witness counters cannot
        // see a semaphore leak: _queued is decremented either way, so a leaked permit shows up only
        // as a bound that has silently shrunk for every LATER send.
        //
        // So the observable is a subsequent send's admission SUCCEEDING. Space 1 against admission
        // 2 means the second send must queue, and a 60 s window means only an explicit drain can
        // free space — so the only thing that can hand the third send its admission permit is the
        // cancelled submission giving one back.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 1,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 2,
            maxBlockMs: 60_000));

        Task<RecordMetadata> appended = harness.AppendOne(0xB1);

        using CancellationTokenSource cancellation = new CancellationTokenSource();
        Task<RecordMetadata> queued = harness.AppendOne(0xB2, callback: null, cancellation.Token, out _);
        Assert.Equal(1, harness.Accumulator.QueuedSubmissionCount);

        // Saturated: both permits are out.
        Assert.Equal(2, harness.Accumulator.AdmittedRecordCount);
        Assert.Null(harness.TryAppendOne(0xBF));

        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => queued, s_deadline));

        // THE ASSERTION: the next send is admitted without waiting. With the release deleted this
        // parks for the whole 60 s max.block.ms, so the deadline — not an equality — is what fails.
        Task<Task<RecordMetadata>> readmitted =
            harness.AppendOneFromAnotherThread(0xB3, CancellationToken.None);
        await TestTimeout.Run(() => readmitted, TimeSpan.FromSeconds(10));
        Task<RecordMetadata> third = await readmitted;

        // It queued (space is still held by the first record), which is what confirms the permit it
        // consumed was an ADMISSION permit rather than a space one.
        Assert.Equal(1, harness.Accumulator.QueuedSubmissionCount);

        harness.DrainNow();
        await TestTimeout.Run(() => appended, s_deadline);
        await TestTimeout.Run(() => third, s_deadline);
        Assert.Equal(2, harness.HistoryCount);
    }

    [Fact]
    public async Task Admission_RefusedSubmit_ReturnsItsPermit_OnBothRoutes()
    {
        // The two release sites a refused submit can take, each with its own phase because they are
        // separate code: SubmitAdmitted's `finally` (the INLINE route threw) and SubmitQueued's
        // sealed-refusal (the QUEUED route, which settles the awaiter instead of throwing, so the
        // `finally` cannot see it). A leak at either site shrinks the bound permanently.
        //
        // ⚠ THE WITNESS IS THE PERMIT COUNT, AND IT HAS TO BE. The obvious behavioural witness — a
        // later send still being admissible — does NOT work here, and a first draft of this test
        // passed under both mutations because of it: BOTH refusal paths below run during teardown,
        // and teardown has also cancelled the backpressure gate, so a caller whose permit was
        // leaked reports "the producer is closing" rather than waiting out max.block.ms —
        // indistinguishable from the permit having come back. AvailableAdmissions separates them
        // (and the accumulator's doc says why it exists). ⚠ This is a fact about THESE two paths,
        // not about every refusal of an already-admitted record: a CANCELLED queued submission's
        // release (ReleaseQueuedSlot) is not a teardown path and does have a behavioural witness —
        // Admission_QueuedSubmissionSettledWithoutAppending_ReturnsItsPermit uses it. (The "every
        // reachable refusal … happens during teardown" wording here was false. Critic 72 / 72.7.)
        //
        // The same fact is why neither site has a behavioural consequence a user could observe: the
        // accumulator is dead either way. They are asserted because the arithmetic is a property in
        // its own right, not because a leak there would be user-visible.

        // ---- phase 1: the inline route. Space is free, so TrySubmitInline is chosen and Append
        // refuses it on _closed, which surfaces as a synchronous throw.
        using Harness inline = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 4,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 1,
            maxBlockMs: 250));
        _ = inline.Accumulator.Stop(s_deadline);
        Assert.Equal(1, inline.Accumulator.AvailableAdmissions);

        // Func<object>, not a lambda returning the Task directly: the xUnit analyzer reads
        // `() => AppendOne(..)` as an async assertion and rejects it (the AppendWithoutAPermit
        // precedent above uses the same shape).
        Func<object> refusedInline = () => inline.AppendOne(0xC1);
        Assert.Throws<ObjectDisposedException>(refusedInline);
        Assert.Equal(1, inline.Accumulator.AvailableAdmissions);

        // ---- phase 2: the sealed queue. The space permits are consumed, so TrySubmitInline must
        // refuse and SubmitQueued is reached with the seal already set.
        using Harness sealedQueue = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000,
            maxAccumulatedRecords: 2,
            batchWindowMs: 60_000,
            batchChunk: 1100,
            maxAdmittedRecords: 1,
            maxBlockMs: 250));
        sealedQueue.ConsumePermits(2);
        _ = sealedQueue.Accumulator.Stop(s_deadline);
        Assert.Equal(1, sealedQueue.Accumulator.AvailableAdmissions);

        // This route settles the awaiter rather than throwing, which is exactly why its release
        // cannot live in SubmitAdmitted's `finally`.
        Task<RecordMetadata> refused = sealedQueue.AppendOne(0xD1);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => refused, s_deadline));
        Assert.Equal(1, sealedQueue.Accumulator.AvailableAdmissions);
    }

    // --------------------------------------- admission settings (M11/P3.3 D3) -------------------

    [Fact]
    public void Settings_AdmissionDefaults_AreTheProvisionalCapAndKafkasMaxBlockMs()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>());

        // PROVISIONAL — slice S2 replaces it with a value measured on this branch (§8.3). It is an
        // order of magnitude above Python's 1000, which a sibling branch measured starving .NET to
        // 96.9k msg/s, and it is deliberately NOT the same number as MaxAccumulatedRecords.
        Assert.Equal(5000, settings.MaxAdmittedRecords);
        Assert.Equal(60_000, settings.MaxBlockMs);
        Assert.NotEqual(settings.MaxAccumulatedRecords, settings.MaxAdmittedRecords);
    }

    [Fact]
    public void Settings_AdmissionBound_IsNotCoupledToTheThreshold()
    {
        // ⚠ THE D3 ASSERTION. MaxAccumulatedRecords DEFAULTS TO the threshold, because Python
        // defines it that way; the admission bound must not, because coupling the two is exactly
        // what made Python's 1000 look transferable when the measurement says it is not. Lowering
        // the threshold moves one and not the other.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "25",
        });

        Assert.Equal(25, settings.SlotThreshold);
        Assert.Equal(25, settings.MaxAccumulatedRecords);
        Assert.Equal(5000, settings.MaxAdmittedRecords);
    }

    [Fact]
    public void Settings_AdmissionOverride_TakesEffect_AndIsItsOwnVariable()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.MaxAccumulatedVariable] = "17",
            [SendAccumulatorSettings.MaxAdmittedVariable] = "321",
        });

        // Separately named, so the two move independently — the observable form of D3.
        Assert.Equal(17, settings.MaxAccumulatedRecords);
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
        // in particular would make every send block for max.block.ms and then fail.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.MaxAdmittedVariable] = raw,
        });

        Assert.Equal(5000, settings.MaxAdmittedRecords);
    }

    [Fact]
    public void Settings_MaxBlockMs_ComesFromTheConfigDict()
    {
        // The one value here read from the user's CONFIG rather than the environment, because it is
        // a real Kafka producer key the core already knows — honouring an existing knob instead of
        // inventing a binding-only one.
        SendAccumulatorSettings settings = ReadSettingsWith(
            new Dictionary<string, string?>(),
            new Dictionary<string, string> { [SendAccumulatorSettings.MaxBlockMsKey] = "1234" });

        Assert.Equal(1234, settings.MaxBlockMs);
    }

    [Fact]
    public void Settings_MaxBlockMs_ZeroIsHonoured_AndMeansNeverBlock()
    {
        // Java's max.block.ms is atLeast(0), so zero is a meaningful value rather than a typo: the
        // fast path still admits when capacity is free, and a saturated bound fails at once.
        SendAccumulatorSettings settings = ReadSettingsWith(
            new Dictionary<string, string?>(),
            new Dictionary<string, string> { [SendAccumulatorSettings.MaxBlockMsKey] = "0" });

        Assert.Equal(0, settings.MaxBlockMs);
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    [InlineData("-1")]
    public void Settings_InvalidMaxBlockMs_FallsBackToKafkasDefault(string raw)
    {
        // Ignored rather than thrown, for a stronger reason than the environment overrides: the
        // core reads this key too, so a value it rejects fails producer construction there with the
        // core's own message — the binding must not pre-empt that with a worse one.
        SendAccumulatorSettings settings = ReadSettingsWith(
            new Dictionary<string, string?>(),
            new Dictionary<string, string> { [SendAccumulatorSettings.MaxBlockMsKey] = raw });

        Assert.Equal(60_000, settings.MaxBlockMs);
    }

    [Fact]
    public void Settings_MaxBlockMs_AboveIntRange_IsClampedRatherThanRejected()
    {
        // max.block.ms is a Java `long`, so a value above int.MaxValue is legal there and must not
        // read as a parse failure here. int.MaxValue ms is ~24 days, indistinguishable from the
        // unbounded wait the operator asked for.
        SendAccumulatorSettings settings = ReadSettingsWith(
            new Dictionary<string, string?>(),
            new Dictionary<string, string>
            {
                [SendAccumulatorSettings.MaxBlockMsKey] = "9223372036854775807",
            });

        Assert.Equal(int.MaxValue, settings.MaxBlockMs);
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
    /// Records the send awaiter's own completion state as seen <b>from inside</b>
    /// <see cref="IDeliveryCallback.OnCompletion"/> — the deterministic probe for the D3 ordering
    /// contract at a firing site that runs <em>inside</em> the submit call (Critic 72 finding
    /// 72.15).
    /// </summary>
    /// <remarks>
    /// The awaiter is injected through <see cref="Observe"/> before the submit call rather than
    /// assigned after it, which is what makes it readable at an in-call firing site; the public
    /// ordering test's after-the-fact assignment works only for the pump, which fires later. A
    /// ticket comparison would be racy (<c>RunContinuationsAsynchronously</c> only
    /// <em>schedules</em> the continuation), so the probe reads
    /// <see cref="Task.IsCompleted"/> instead — which <c>TrySetException</c> sets synchronously.
    /// </remarks>
    /// <remarks>
    /// <c>internal</c> rather than <c>private</c> like its siblings only because
    /// <see cref="Harness.AppendOneObservingItsCompletionFromAnotherThread"/> takes it by its own
    /// type — the awaiter has to reach the probe, not just an <see cref="IDeliveryCallback"/>.
    /// </remarks>
    internal sealed class CompletionStateProbeDeliveryCallback : IDeliveryCallback
    {
        internal Task<RecordMetadata>? ObservedTask { get; private set; }

        internal bool WasInvoked { get; private set; }

        internal bool TaskWasCompleted { get; private set; }

        private Task<RecordMetadata>? _awaiter;

        /// <summary>Hands the probe the awaiter it must read, before the submit call.</summary>
        internal void Observe(Task<RecordMetadata> awaiter) => _awaiter = awaiter;

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            Task<RecordMetadata>? awaiter = _awaiter;
            ObservedTask = awaiter;
            TaskWasCompleted = awaiter is not null && awaiter.IsCompleted;
            WasInvoked = true;
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
    private static SendAccumulatorSettings ReadSettingsWith(
        Dictionary<string, string?> overrides,
        IReadOnlyDictionary<string, string>? config = null)
    {
        string[] all =
        {
            SendAccumulatorSettings.ThresholdVariable,
            SendAccumulatorSettings.MaxAccumulatedVariable,
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
            return SendAccumulatorSettings.FromEnvironment(config);
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
        /// Appends one record, queueing it behind the backpressure bound if it is full — through the
        /// <b>same single entry point</b> <c>NativeProducer.SendViaPump</c> uses, so the admission
        /// bound and the routing rule are both production's (DoD §12; M11/P3.2 §3.3, M11/P3.3 §7).
        /// This fixture used to re-implement "permit-then-<c>Submit</c>, else
        /// await-then-<c>Submit</c>", which would have left the ordering tests below proving a
        /// property of the fixture rather than of the code.
        /// </summary>
        /// <remarks>
        /// ⚠ <b>It can BLOCK the calling thread</b> since M11/P3.3, exactly as production's
        /// <c>Send</c> does: a saturated admission bound parks the caller for up to
        /// <c>max.block.ms</c>. Every test that saturates the bound on purpose must therefore drive
        /// this from its own <see cref="Task"/> (or supply a short <c>maxBlockMs</c>) rather than
        /// from the xUnit test thread — see <c>AppendOneFromAnotherThread</c>.
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

            // ONE call, production's own: SubmitAdmitted takes the admission permit (blocking when
            // the bound is saturated) and then makes the inline-vs-queued routing decision. The
            // fixture deliberately holds no copy of either rule.
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
        /// future. ⚠ Admission <b>expiry</b> does not fault the outer task: it is Java's
        /// buffer-exhausted outcome, so the record's <em>inner</em> task is faulted and
        /// <c>SubmitAdmitted</c> returns normally (Critic 72 finding 72.1). So a test of the expiry
        /// awaits the outer task for the timing and the inner one for the failure; a test of
        /// teardown or cancellation still awaits the outer one for both.
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
        /// <see cref="AppendOneFromAnotherThread"/> for a firing site that runs <b>inside</b>
        /// <c>SubmitAdmitted</c>: the record's awaiter is created and handed to
        /// <paramref name="probe"/> <em>before</em> the submit call, so the probe can read its
        /// completion state from inside <c>OnCompletion</c>.
        /// </summary>
        /// <remarks>
        /// This is the only thing it adds — the record, the awaiter, the
        /// <see cref="DeliveryRegistration"/> and the single <c>SubmitAdmitted</c> entry point are
        /// all <see cref="AppendOne(byte, IDeliveryCallback?, CancellationToken, out TaskCompletionSource{RecordMetadata})"/>'s,
        /// i.e. production's (DoD §12). Ordering the two statements the other way round is exactly
        /// the bug under test, so the fixture must not be the thing that fixes it: it hands over the
        /// <see cref="TaskCompletionSource{TResult}.Task"/>, never touching the source itself.
        /// Issued from its own <see cref="Task"/> because a saturated bound parks the caller for
        /// <c>max.block.ms</c>.
        /// </remarks>
        internal Task<Task<RecordMetadata>> AppendOneObservingItsCompletionFromAnotherThread(
            byte tag,
            CompletionStateProbeDeliveryCallback probe) =>
            Task.Factory.StartNew(
                () =>
                {
                    SerializedProducerRecord record = NewRecord(tag);
                    TaskCompletionSource<RecordMetadata> completion = NewCompletion();
                    probe.Observe(completion.Task);

                    Accumulator.SubmitAdmitted(
                        record, completion, NewDelivery(probe), CancellationToken.None);

                    return completion.Task;
                },
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);

        /// <summary>
        /// <b>The injection for the batch thread's own failure path</b>, and the ONE place this
        /// fixture deliberately breaks the contract production keeps: it appends <em>without</em>
        /// first taking a backpressure permit. The accumulated counter then runs ahead of the
        /// permits taken, so the batch thread's next <c>ReleaseSpace</c> over-releases the
        /// <see cref="SemaphoreSlim"/> and throws <see cref="SemaphoreFullException"/> — an
        /// unexpected managed failure of the batch thread, between taking the chain and sending it,
        /// which is exactly the shape <c>RunLoop</c>'s <c>catch</c> exists for and the only one
        /// reachable without a production-side test hook.
        /// </summary>
        /// <remarks>
        /// Everything else stays production's own primitive — the record, the awaiter, the
        /// <see cref="DeliveryRegistration"/> and <see cref="SendAccumulator.Submit"/> itself
        /// (DoD §12); only the permit is skipped, because skipping it IS the injected fault.
        /// </remarks>
        internal Task<RecordMetadata> AppendWithoutAPermit(byte tag, IDeliveryCallback? callback = null)
        {
            TaskCompletionSource<RecordMetadata> completion = NewCompletion();
            Accumulator.Submit(NewRecord(tag), completion, NewDelivery(callback));
            return completion.Task;
        }

        /// <summary>
        /// <b>The injection for a failure that ESCAPES <c>SendNode</c></b> — 65.3's second trigger,
        /// which <see cref="AppendWithoutAPermit"/> cannot reach because its over-release throws
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

        /// <summary>
        /// <b>The injection for a submitter that cannot make progress</b> (M11/P3.2 slice S2):
        /// takes the accumulator's exclusive submitter token without starting a loop, so
        /// <c>EnsureSubmitterRunning</c>'s 0 → 1 CAS fails forever and nothing ever dequeues a
        /// queued submission.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Why it needs an injection at all.</b> The submitter is deliberately a thread-pool
        /// loop rather than a thread (ffi §A1's two-thread cap), so there is no production state
        /// that stalls it — starving it would mean starving the whole pool, which is process-wide
        /// and would be observed by every concurrently-running test class. This is the same
        /// warrant <see cref="TruncateDeliveriesOfPendingNode"/> has: the state is private to
        /// <see cref="SendAccumulator"/>, no production path can produce it, and producing it
        /// <em>is</em> the injected fault.
        /// </para>
        /// <para>
        /// <b>What it buys.</b> It makes <see cref="Stop"/>'s queue-flush expiry deterministic —
        /// both the expiry's own settlement contract and the fact that the flush and the join share
        /// one deadline. Neither is observable while the submitter drains the queue in microseconds.
        /// </para>
        /// <para>
        /// <b>Caller's contract:</b> call it before any submission is queued, so no loop is running
        /// and the token is genuinely free. It is never released — the accumulator is torn down
        /// with the harness.
        /// </para>
        /// </remarks>
        internal void StallTheSubmitter()
        {
            FieldInfo running = typeof(SendAccumulator).GetField(
                "_submitterRunning", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException(
                    "SendAccumulator no longer exposes a _submitterRunning token — this injection " +
                    "needs to be re-derived against the new shape rather than silently skipped.");

            Assert.Equal(0, (int)running.GetValue(Accumulator)!);
            running.SetValue(Accumulator, 1);
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

        /// <summary>
        /// Attempts one append <b>inline and without ever blocking</b>, through production's own
        /// non-blocking entry point; null when it was refused — because the admission bound is
        /// saturated, because the backpressure bound is full, <b>or</b> because a submission is
        /// already queued ahead of it (M11/P3.2 §F1: the third condition is the ordering fix, and
        /// this probe reports it the same way).
        /// </summary>
        internal Task<RecordMetadata>? TryAppendOne(byte tag)
        {
            SerializedProducerRecord record = NewRecord(tag);
            TaskCompletionSource<RecordMetadata> completion = NewCompletion();

            return Accumulator.TryAdmitAndSubmitInline(record, completion, delivery: null)
                ? completion.Task
                : null;
        }

        /// <summary>
        /// Consumes <paramref name="count"/> backpressure permits <b>without appending anything</b>,
        /// so the bound can be saturated while the node chain stays empty.
        /// </summary>
        /// <remarks>
        /// The one lever that makes a queued submission's wait <b>deterministic</b>: with the
        /// permits consumed and nothing accumulated, the batch thread's drain frees
        /// <c>_accumulated == 0</c> permits, so a queued submission stays queued until something
        /// settles it (teardown, or its own token). Safe in the other direction too — this creates a
        /// permanent permit <em>deficit</em>, never a surplus, so it cannot provoke the
        /// <see cref="SemaphoreFullException"/> over-release that
        /// <see cref="AppendWithoutAPermit"/> injects.
        /// </remarks>
        internal void ConsumePermits(int count)
        {
            for (int i = 0; i < count; i++)
            {
                Assert.True(
                    Accumulator.TryAcquireSpace(),
                    "the bound was already exhausted, so the fixture consumed nothing");
            }
        }

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
