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

        // Index 0 was handed to the pump before the throw, so it resolves normally.
        await TestTimeout.Run(() => sends[0], s_deadline);

        // Index 1 is where CompleteNode threw. SendNode's own FaultNode settles it and then throws
        // out of SendNode on its very next statement.
        await AssertSettledByTheBatchThreadFailure(sends[1]);

        // Index 2 is THE assertion of this test: FaultNode never reached it, so it settles only if
        // AbandonOnThreadFailure can still find this node through _inFlight. With the advance
        // hoisted above SendNode it cannot, and this await hits its deadline instead.
        await AssertSettledByTheBatchThreadFailure(sends[2]);

        // No delivery callback for 1 or 2: the core accepted both and their futures are destroyed
        // unread, so firing here would invent a failure for a record that may still be delivered
        // (§6.2, recorded residual). Only index 0's success fires, from the pump — asserted after a
        // settle window, since a first observation of 1 cannot rule out 2.
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        Assert.Equal(1, callback.Count);

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
        const int Burst = 200;
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 4, batchWindowMs: 60_000, batchChunk: 1100));

        // The witness here is the per-record delivery callback's firing order, not the pending
        // node's slots: 200 records across a bound of 4 span ~50 nodes, and a node is recycled long
        // before the burst ends, so there is no single array to read. It observes the same property
        // because the chain from append to callback is order-preserving end to end — CompleteNode
        // enqueues a node's records to the pump in index order (the same order send_batch read
        // them), Enqueue appends to a FIFO queue, DrainAll dequeues FIFO, and ProcessBatch fires
        // each batch in index order. So callback order == append order == the order records reached
        // send_batch.
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

        // A settle window: the last callback fires immediately before its awaiter is released, so
        // WhenAll already implies all 200 ran — but read the list under its own lock either way.
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
        // ⚠ Python's version of this guarantee is almost unconditional, not unconditional:
        // Producer_send_thread re-tests !closed at :529 OUTSIDE record_batches_mutex, so a record
        // appended in the narrow gap between its mtx_unlock (:577/:638) and that re-test is never
        // sent and never completed. So .NET now completes it "as Python does on every path except
        // one narrow race Python leaves open" — Python-aligned AND strictly better.
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
    /// the send it was handed to — the cross-node submission-order witness (see the stress half of
    /// <see cref="SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound"/>).
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
            SendAccumulatorSettings.MaxAccumulatedVariable,
            SendAccumulatorSettings.WindowVariable,
            SendAccumulatorSettings.ChunkVariable,
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
    private sealed class Harness : IDisposable
    {
        private readonly NativeProducer _producer;
        private readonly SendCompletionPump _pump;
        private bool _disposed;

        internal Harness(SendAccumulatorSettings settings)
            : this(() => settings)
        {
        }

        internal Harness(Func<SendAccumulatorSettings> settingsFactory)
        {
            _producer = NativeProducer.CreateMock(autoComplete: true);
            _pump = new SendCompletionPump();
            Topics = new PinnedTopicCache();
            Accumulator = new SendAccumulator(_producer.Handle, Topics, _pump, settingsFactory());
        }

        internal SendAccumulator Accumulator { get; }

        internal PinnedTopicCache Topics { get; }

        /// <summary>
        /// How many sends the completion pump has taken off its queue — the witness for "this
        /// index was handed to the pump" (and, by its absence, for "this index was not").
        /// </summary>
        internal long DrainedSendCount => _pump.DrainedSendCount;

        /// <summary>The mock core's sent-record count — what "reached the core" means (§3.5).</summary>
        internal int HistoryCount => _producer.MockHistoryCount();

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
        /// <b>same two entry points</b> <c>NativeProducer.SendViaPump</c> uses, so the routing rule
        /// itself is production's (DoD §12; M11/P3.2 §3.3). This fixture used to re-implement
        /// "permit-then-<c>Submit</c>, else await-then-<c>Submit</c>", which would have left the
        /// ordering tests below proving a property of the fixture rather than of the code.
        /// </summary>
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

            if (!Accumulator.TrySubmitInline(record, completion, delivery))
            {
                Accumulator.SubmitQueued(record, completion, delivery, cancellationToken);
            }

            return completion.Task;
        }

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
        /// Attempts one append <b>inline</b>, through production's own routing entry point; null
        /// when it was refused — because the bound is full, <b>or</b> because a submission is
        /// already queued ahead of it (M11/P3.2 §F1: the second condition is the ordering fix, and
        /// this probe reports it the same way).
        /// </summary>
        internal Task<RecordMetadata>? TryAppendOne(byte tag)
        {
            SerializedProducerRecord record = NewRecord(tag);
            TaskCompletionSource<RecordMetadata> completion = NewCompletion();

            return Accumulator.TrySubmitInline(record, completion, delivery: null)
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
            // gate, then flush so the pump's blocking get_all can return, then join it, then destroy.
            bool drained = Accumulator.Stop(s_deadline);
            _pump.CloseGate();
            NativeMethods.ProducerFlush(_producer.Handle, out IntPtr flushError);
            _ = KafkaException.FromHandle(flushError);
            _pump.Stop();
            if (drained)
            {
                Topics.Dispose();
            }

            _producer.Dispose();
        }
    }
}
