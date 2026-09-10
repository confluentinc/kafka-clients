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

        _ = harness.Append(2);
        Task<RecordMetadata> blocked = harness.AppendOne(0xF2);
        Assert.False(blocked.IsCompleted);

        // Teardown must return rather than hang behind the parked sender...
        TestTimeout.Run(harness.Dispose, TimeSpan.FromSeconds(10));

        // ...and the parked send must SETTLE (faulted — it never reached the core), not hang.
        //
        // Either of two racing paths can settle it, and both are the same observable outcome: the
        // cancelled gate releases the waiter directly, or the final drain's permit release lets it
        // through and the now-closed accumulator refuses the append. Asserting one specific message
        // would be asserting which side of that race won, which is not a contract.
        ObjectDisposedException failure = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => blocked, TimeSpan.FromSeconds(10)));
        Assert.Contains(nameof(NativeProducer), failure.Message, StringComparison.Ordinal);
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

        /// <summary>Appends one record, blocking on the backpressure bound if it is full.</summary>
        internal Task<RecordMetadata> AppendOne(byte tag, IDeliveryCallback? callback = null)
        {
            SerializedProducerRecord record = NewRecord(tag);
            TaskCompletionSource<RecordMetadata> completion = NewCompletion();

            if (Accumulator.TryAcquireSpace())
            {
                Accumulator.Submit(record, completion, NewDelivery(callback));
                return completion.Task;
            }

            return SubmitWhenSpaceAvailable(record, completion);
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

        /// <summary>The single node the accumulator is currently filling.</summary>
        private object PendingNode()
        {
            FieldInfo head = typeof(SendAccumulator).GetField(
                "_head", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException(
                    "SendAccumulator no longer exposes a _head field.");

            return head.GetValue(Accumulator)
                ?? throw new InvalidOperationException(
                    "the accumulator holds no pending node — it drained before the injection landed");
        }

        private static SerializedProducerRecord NewRecord(byte tag) =>
            new SerializedProducerRecord(Topic, 0, null, null, new byte[] { tag, 0xAA, 0xBB });

        private static TaskCompletionSource<RecordMetadata> NewCompletion() =>
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

        private static DeliveryRegistration? NewDelivery(IDeliveryCallback? callback) =>
            callback is null ? null : new DeliveryRegistration(callback, Topic, 0);

        /// <summary>Attempts one append WITHOUT blocking; null when the bound refused it.</summary>
        internal Task<RecordMetadata>? TryAppendOne(byte tag)
        {
            if (!Accumulator.TryAcquireSpace())
            {
                return null;
            }

            SerializedProducerRecord record = new SerializedProducerRecord(
                Topic, 0, null, null, new byte[] { tag, 0xAA, 0xBB });
            TaskCompletionSource<RecordMetadata> completion =
                new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

            Accumulator.Submit(record, completion, delivery: null);
            return completion.Task;
        }

        private async Task<RecordMetadata> SubmitWhenSpaceAvailable(
            SerializedProducerRecord record, TaskCompletionSource<RecordMetadata> completion)
        {
            try
            {
                await Accumulator.WaitForSpaceAsync(CancellationToken.None);
                Accumulator.Submit(record, completion, delivery: null);
            }
            catch (Exception exception)
            {
                completion.TrySetException(exception);
            }

            return await completion.Task;
        }

        /// <summary>Forces one drain and waits for it, the way the test drain hook does.</summary>
        internal void DrainNow() =>
            Assert.True(Accumulator.DrainPending(s_deadline), "the accumulator did not drain in time");

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
