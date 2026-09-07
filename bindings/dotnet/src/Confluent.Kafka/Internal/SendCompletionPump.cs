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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The producer send-completion pump (inline pull-pump — PLAN §3 Option C; ffi §A7's pull surface; §6.3): one
/// background thread per <see cref="NativeProducer"/> that drains an unbounded MPSC queue of
/// <c>(future, TaskCompletionSource)</c> pairs, blocks on a batched
/// <c>FutureRecordMetadata_get_all</c>, completes each <see cref="TaskCompletionSource{TResult}"/>
/// (built with <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>), and frees the
/// future handles with <c>FutureRecordMetadata_destroy_all</c>. There is no <b>native</b> per-send
/// callback and no producer handle here — the pump owns only the flat transient future / metadata /
/// error handles (ffi §A2 Category 2); the producer-outlives-pump invariant is enforced by
/// <see cref="NativeProducer"/>'s teardown ordering (stop + join the pump before
/// <c>Producer_destroy</c>, §A2), not by this type.
/// </summary>
/// <remarks>
/// <para>
/// <b>It DOES run one piece of managed user code (M14/P1).</b> A send made through
/// <c>Send(record, IDeliveryCallback)</c> carries a <see cref="DeliveryRegistration"/>, and this
/// pump thread is where that callback is invoked — deliberately, because it is .NET's analogue of
/// the "background I/O thread" Java documents for <c>Callback.onCompletion</c>
/// (<c>Callback.java:20-21</c>). It is a <b>managed-only</b> callback: it never crosses the C ABI,
/// so it adds no delegate rooting, no <c>GCHandle</c> and no <c>[DllImport]</c> (ffi §A6's third
/// callback family). The two properties that keep it from destabilizing the pump are that
/// <see cref="DeliveryRegistration.Fire"/> is a total no-throw boundary, and that the awaiter's own
/// continuation still runs off this thread via
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> — so an unbounded piece of user
/// code still cannot attach itself to the pump.
/// </para>
/// <para>
/// <b>Why a pump, not <c>Task.Run(get)</c> per send.</b> One thread batches many completions per
/// blocking <c>get_all</c> — O(1) threads for unbounded in-flight sends — instead of parking a
/// pool thread per message (the sync-over-async anti-pattern, ffi §A7 / CLAUDE.md §11). The
/// <c>get_all</c> <c>block_on</c> parks only this thread; the core's Sender keeps running on the
/// runtime's worker pool (ffi §A1), so a blocked pump delays result delivery but never sending.
/// </para>
/// <para>
/// <b>Backpressure — the conclusion holds, the mechanism changed (M11/P3.1 §12.4).</b> The queue is
/// structurally unbounded but remains practically bounded by the core's <c>buffer.memory</c>
/// backpressure, because <b>a future only enters this queue after the core has accepted its
/// record</b> — so whatever bounds acceptance bounds the queue. What changed is <em>who</em> waits:
/// this paragraph used to say the inline <c>Producer_send</c> blocks the <b>caller</b> thread up to
/// <c>max.block.ms</c>, and on the async path that is no longer true. The caller now appends to the
/// <see cref="SendAccumulator"/> and returns; it is the <b>send-batch thread</b> that calls
/// <c>send_batch</c> and blocks on a full core buffer, while the caller is bounded by the
/// accumulator instead. "No managed bound is added" is likewise no longer true — the accumulator has
/// its own (Python's <c>PRODUCER_MAX_ACCUMULATED_RECORDS</c>). The <b>sync</b> send still blocks its
/// own caller inline, exactly as described before.
/// </para>
/// <para>
/// <b>Teardown (<see cref="Stop"/>).</b> Called on the disposing thread after the
/// <see cref="NativeProducer"/> close latch is won: signals the loop to stop, joins the thread
/// (so no future handle is in use), then faults + frees anything still queued. A
/// <see cref="Enqueue"/> that races a completed <see cref="Stop"/> is caught under
/// <see cref="_stopLock"/> and faulted + freed in place — so no send is stranded or leaked
/// (deterministic — no <em>strand or leak</em> residual on the enqueue-vs-stop race). That branch
/// does drop the send's delivery notification, which is recorded residual 1 — see
/// <see cref="Enqueue"/>.
/// </para>
/// <para>
/// <b>The in-flight <c>get_all</c> is unblocked by a flush, NOT by close.</b> <c>get_all</c> blocks
/// until every future in its batch resolves and cannot be interrupted, so if the pump is inside
/// <c>get_all</c> on a not-yet-resolved send when teardown starts, <c>_thread.Join()</c> would hang
/// until that future resolves. <see cref="NativeProducer"/>'s teardown therefore flushes pending
/// sends (the sync <c>Producer_flush</c> on the blocking <c>Dispose</c> path, or an awaited
/// <c>Producer_flush_async</c> on the async paths — ffi §A7) <b>before</b> calling
/// <see cref="Stop"/>: the core's <c>Producer_close</c> does <b>not</b> drive pending sends (it only
/// marks the producer closed — verified <c>src/producer/mock_producer.rs</c>), so close cannot
/// unblock <c>get_all</c>; <c>flush</c> can, and does — completing a <c>MockProducer</c>'s pending
/// sends (their futures resolve), or delivering-or-timing-out a real producer's (the accepted
/// Option-C bounded residual, ffi §A7). Once the flush has resolved the pending sends, the in-flight
/// <c>get_all</c> returns and <see cref="Stop"/>'s join completes; the loop is never interrupted
/// mid-<c>get_all</c>.
/// </para>
/// <para>
/// <b>Background thread.</b> The pump thread is a background thread, so a producer leaked without
/// disposal does not keep the process alive; its resources are reclaimed at process exit. Normal
/// use disposes the producer, which stops and joins the pump deterministically.
/// </para>
/// </remarks>
internal sealed class SendCompletionPump
{
    /// <summary>
    /// The maximum number of completions one <c>get_all</c> pass handles — Python's
    /// <c>PRODUCER_RECORD_SLOT_CAPACITY</c> (1100), the size of the anchor's own <c>get_all</c>
    /// output arrays (<c>_confluentkafka.c:429-430</c>) and of one <c>BatchNode</c>, which is what
    /// its poll thread processes per iteration (<c>:480-513</c>).
    /// </summary>
    /// <remarks>
    /// Derived from the <b>default</b> constants, not from a producer's (possibly overridden)
    /// settings, exactly as the anchor's arrays are sized by a compile-time <c>#define</c>: this is a
    /// bound on one marshalling pass, not a tuning knob, and the pump is shared machinery that
    /// predates any per-producer settings.
    /// </remarks>
    internal const int DrainCap =
        SendAccumulatorSettings.DefaultSlotThreshold + SendAccumulatorSettings.SlotCapacityHeadroom;

    private readonly ConcurrentQueue<PendingSend> _queue = new ConcurrentQueue<PendingSend>();

    // The three marshalling arrays get_all reads and writes, allocated ONCE (§12.3) now that the
    // drain is capped — the anchor allocates nothing per batch either (its equivalents are
    // fixed-size stack arrays, _confluentkafka.c:429-430). Touched only by the pump thread inside
    // ProcessBatch, so they need no synchronization. ⚠ They retain the previous pass's values past
    // the current batch's count: see ProcessBatch's remarks for why every loop over them is bounded
    // by that count and never by Length.
    private readonly IntPtr[] _futures = new IntPtr[DrainCap];
    private readonly IntPtr[] _metadata = new IntPtr[DrainCap];
    private readonly IntPtr[] _errors = new IntPtr[DrainCap];

    // Signals "items may be available". Reset-before-drain (see RunLoop) avoids a lost wakeup.
    private readonly ManualResetEventSlim _signal = new ManualResetEventSlim(initialState: false);

    // Serializes Enqueue's stopped-check + enqueue against Stop's terminal drain, so an enqueue
    // racing teardown is never lost: it is either queued (and drained by Stop) or faulted in place.
    private readonly object _stopLock = new object();

    private readonly Thread _thread;

    // Instrumentation, written only by the pump thread: the number of ProcessBatch passes and the
    // largest batch any pass carried. The cap (§12.2) is asserted by these — "the completions all
    // resolved" would be satisfied by an uncapped drain too, so it cannot tell the cap is in force.
    private long _processedBatches;
    private int _largestProcessedBatch;

    // Instrumentation: how many sends this pump has taken OFF its queue, counted in DrainAll and so
    // covering both consumers — RunLoop's capped drain and Stop's terminal one.
    //
    // It exists for the M11/P3.1 §3.8 teardown-ordering guard, and it is counted here rather than in
    // Enqueue or ProcessBatch because only this point is reached on EVERY path a queued send can
    // take: a send that was enqueued is dequeued exactly once, by one of those two callers, whether
    // or not the pump loop won the race against Stop's _stopping. So "the accumulator's final drain
    // reached an OPEN gate" reads as "this equals the number of sends", with no dependence on that
    // out-of-scope race — whereas ProcessedBatchCount is zero whenever Stop's terminal drain got
    // there first, which would make the guard flaky in exactly the way it exists to stop being.
    // Written by the pump thread and by the disposing thread (under _stopLock), hence Interlocked.
    private long _drainedSends;

    // Set by Stop before waking the loop; read by the loop to break out of its wait.
    private volatile bool _stopping;

    // Set by Stop after the thread has joined and the terminal drain has run; makes a subsequent
    // Enqueue fault-in-place instead of queueing into a dead pump.
    private bool _stopped;

    internal SendCompletionPump()
    {
        _thread = new Thread(RunLoop)
        {
            IsBackground = true,
            Name = "confluent-kafka-producer-send-pump",
        };
        _thread.Start();
    }

    /// <summary>The number of <c>ProcessBatch</c> passes the pump thread has run.</summary>
    internal long ProcessedBatchCount => Interlocked.Read(ref _processedBatches);

    /// <summary>The record count of the largest batch any pass carried — never above <see cref="DrainCap"/>.</summary>
    internal int LargestProcessedBatch => Volatile.Read(ref _largestProcessedBatch);

    /// <summary>
    /// The number of sends this pump has taken off its queue — i.e. the number that reached
    /// <see cref="Enqueue"/> while the gate was still <b>open</b>. The witness for the M11/P3.1
    /// §3.8 teardown ordering (see <c>_drainedSends</c>).
    /// </summary>
    internal long DrainedSendCount => Interlocked.Read(ref _drainedSends);

    /// <summary>
    /// Enqueues a resolved-later send. On the normal path the pump completes
    /// <paramref name="completion"/>, invokes <paramref name="delivery"/> (when one was supplied)
    /// and frees <paramref name="future"/>; if the pump has already stopped (teardown raced this
    /// enqueue) the send is faulted and its future freed here, so it is never stranded or leaked.
    /// </summary>
    /// <remarks>
    /// <b>The teardown fault-in-place branch does NOT invoke <paramref name="delivery"/></b> —
    /// recorded residual 1 on <see cref="IDeliveryCallback"/>. The callback
    /// reports a <em>core</em> completion, and on this branch there is none: the record was handed
    /// to native but the pump that would collect its result is gone. Python behaves identically (its
    /// <c>close()</c> cancels the pending futures without invoking <c>on_delivery</c>). Firing a
    /// fabricated failure here would also reintroduce a double-fire hazard, because the record may
    /// still be delivered by the core.
    /// </remarks>
    /// <param name="future">The record's future handle (ownership transfers to the pump).</param>
    /// <param name="completion">The send's awaiter.</param>
    /// <param name="delivery">
    /// The user's delivery callback carrier, or <see langword="null"/> on the plain
    /// <c>Send(record)</c> path (M14/P1).
    /// </param>
    internal void Enqueue(
        IntPtr future,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery)
    {
        lock (_stopLock)
        {
            if (_stopped)
            {
                // The pump is torn down: fault + free in place rather than queue into a dead pump.
                completion.TrySetException(TeardownException());
                DestroyFutures(new[] { future }, 1);
                return;
            }

            _queue.Enqueue(new PendingSend(future, completion, delivery));
            _signal.Set();
        }
    }

    /// <summary>
    /// Closes the enqueue gate <b>without</b> stopping the loop, so a send that races teardown
    /// faults in place instead of being queued into a pump that is about to be joined (M11/P8,
    /// Major 5). Called by <see cref="NativeProducer"/> teardown <b>before</b> the teardown flush.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why the existing <c>_stopped</c> guard was unreachable when it mattered.</b>
    /// <see cref="Stop"/> sets <c>_stopped</c> only <em>after</em> <c>_thread.Join()</c> — i.e.
    /// after the very join that can hang. Neither flag was set before the teardown flush, so a
    /// send landing between "flush returned" and "Stop() called" was queued into the pump, picked
    /// up, and blocked the loop in an uninterruptible <c>get_all</c> that nothing would ever
    /// resolve; the join then waited forever (on a manual mock, permanently).
    /// </para>
    /// <para>
    /// <b>Airtight in both directions</b> once the gate closes before the flush:
    /// a send enqueued <em>before</em> the gate closed had its <c>Producer_send</c> ≺
    /// <c>Enqueue</c> ≺ <c>CloseGate</c> ≺ flush, so the flush resolves it and <c>get_all</c>
    /// returns; a send arriving <em>after</em> takes <see cref="Enqueue"/>'s fault-in-place branch
    /// (which faults the <see cref="Task"/> <b>and</b> frees the future). There is no third case.
    /// </para>
    /// <para>
    /// No new state and no new free site — this only makes the existing <c>_stopped</c> guard
    /// <em>reachable</em>. <see cref="Stop"/> re-setting <c>_stopped</c> under the same lock stays
    /// harmless (idempotent). The accepted semantic is unchanged from today's <c>_stopped</c>
    /// branch: a send faulted in place has already been handed to native, so the record may still
    /// be delivered while its <see cref="Task"/> faults — pre-existing teardown-race behavior.
    /// </para>
    /// </remarks>
    internal void CloseGate()
    {
        lock (_stopLock)
        {
            // Close the gate, leave the loop running: already-queued futures must still be drained
            // by the loop once the teardown flush resolves them.
            _stopped = true;
        }
    }

    /// <summary>
    /// Stops the pump: signals the loop, joins the thread (so no future handle is in use), then
    /// faults + frees anything still queued. Called by <see cref="NativeProducer"/> teardown after
    /// the close latch is won and before <c>Producer_destroy</c> (the producer-outlives-pump
    /// ordering, ffi §A2). Idempotent-safe under the one-shot latch (only the latch winner calls
    /// it).
    /// </summary>
    internal void Stop()
    {
        _stopping = true;
        _signal.Set();
        _thread.Join();

        // The pump thread has exited. Under the lock, latch "stopped" and drain the remainder so a
        // concurrent Enqueue either was queued (drained here) or will fault-in-place (sees _stopped).
        lock (_stopLock)
        {
            _stopped = true;
            DrainAndFaultRemaining();
        }

        _signal.Dispose();
    }

    private void RunLoop()
    {
        while (true)
        {
            _signal.Wait();

            // Reset BEFORE draining so an Enqueue during/after the drain re-sets the event (no lost
            // wakeup): a missed item's Set lands after this Reset, so the next Wait returns.
            //
            // ⚠ THIS ORDERING IS STILL LOAD-BEARING AND STILL DOES ITS OWN JOB, alongside the inner
            // loop below. The two cover different things: the inner loop handles leftovers this
            // thread already KNOWS about (the drain hit the cap), while the Reset ordering handles
            // items arriving CONCURRENTLY — their Set lands after this Reset, so the next Wait
            // returns. Neither subsumes the other; do not "simplify" the ordering away because the
            // inner loop looks like it covers the same ground.
            _signal.Reset();

            if (_stopping)
            {
                // Leave any queued items for Stop's terminal drain (fault, no blocking get_all).
                break;
            }

            // ⚠ INNER DRAIN LOOP — THE HANG FIX (M11/P3.1 §12.2.1). DrainAll is now CAPPED, and the
            // outer loop's correctness used to rest on it always emptying the queue. Capping without
            // this loop is a DETERMINISTIC hang, not a race, and needs no concurrency to reproduce:
            // a queue of 3000 leaves 1900 behind, the next iteration blocks on _signal.Wait() — reset
            // above and only Set by a new Enqueue — and with no further sends those 1900 completions
            // and every awaiting Task hang forever.
            //
            // Keep draining and processing capped batches until a drain comes back SHORT (fewer than
            // the cap, i.e. the queue is empty), and only then fall through to the Wait. That is
            // precisely the anchor's shape: its poll thread completes one node, advances to
            // next_batch, and cnd_waits ONLY while that is NULL (_confluentkafka.c:504-506).
            //
            // Deliberately NOT "call _signal.Set() when items remain": the loop is clearer, is what
            // the anchor does, and does not depend on reasoning about a self-signal racing a Reset.
            while (true)
            {
                List<PendingSend> batch = DrainAll(DrainCap);
                if (batch.Count == 0)
                {
                    break;
                }

                Interlocked.Increment(ref _processedBatches);
                if (batch.Count > _largestProcessedBatch)
                {
                    Volatile.Write(ref _largestProcessedBatch, batch.Count);
                }

                try
                {
                    ProcessBatch(batch);
                }
                catch (Exception exception)
                {
                    // ProcessBatch already freed the batch's future handles on every one of its own
                    // paths (its `finally`'s destroy_all — since M11/P3.1 §12.3 that is the ONLY
                    // future-free site, because the marshalling arrays are reused fields and there
                    // is no pre-`try` allocation left to fail), but a throw that escaped it — a native failure surfacing from
                    // get_all, or OOM either side of its per-index completion loop — left the
                    // batch's TCSes UNcompleted, so their awaiters would hang forever. Fault them
                    // here (TCS-only — the handles are already freed, so do NOT free them again).
                    // CONTINUE, don't break: faulting this batch and looping keeps the pump draining
                    // later Enqueues; breaking would exit the thread WITHOUT marking the pump
                    // stopped (only Stop sets _stopped), so subsequent Enqueues would queue into a
                    // dead pump and hang. (A
                    // truly process-corrupting AccessViolation is not catchable by design — the
                    // process terminates; this guards the catchable cases: OOM, or a managed
                    // marshalling throw that escapes ProcessBatch.)
                    FaultBatchCompletions(batch, exception);
                }

                if (batch.Count < DrainCap)
                {
                    // A SHORT drain means the queue ran out, so there is nothing known to be left —
                    // fall through to the Wait. Exactly-at-the-cap loops again, because the queue may
                    // hold more.
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Drains up to <paramref name="cap"/> queued sends into a batch (lock-free). Pass
    /// <see cref="int.MaxValue"/> for "everything".
    /// </summary>
    /// <remarks>
    /// <b>The cap is Python parity</b> (M11/P3.1 §12.2). The anchor's poll-futures thread processes
    /// <b>one <c>BatchNode</c> at a time</b> (<c>_confluentkafka.c:480-513</c>) and sizes its
    /// <c>get_all</c> output arrays at <c>PRODUCER_RECORD_SLOT_CAPACITY</c> (<c>:429-430</c>), so its
    /// per-call completion batch is at most 1100. This drain used to empty the whole
    /// <see cref="ConcurrentQueue{T}"/> and allocate three <c>IntPtr[count]</c> over whatever it
    /// found — unbounded by construction, and the one place the two bindings' constants diverged.
    /// <para>
    /// ⚠ <b>Capping this made <see cref="RunLoop"/>'s inner drain loop mandatory</b> — see the hang
    /// analysis there. And the cap belongs <b>here only</b>: see
    /// <see cref="DrainAndFaultRemaining"/> for why capping the terminal drain would be a defect
    /// rather than a symmetry.
    /// </para>
    /// </remarks>
    private List<PendingSend> DrainAll(int cap)
    {
        List<PendingSend> batch = new List<PendingSend>();
        while (batch.Count < cap && _queue.TryDequeue(out PendingSend pending))
        {
            batch.Add(pending);
        }

        if (batch.Count > 0)
        {
            // Instrumentation only (see _drainedSends) — the single point every queued send passes
            // through exactly once, whichever of the two callers takes it.
            Interlocked.Add(ref _drainedSends, batch.Count);
        }

        return batch;
    }

    /// <summary>
    /// Resolves a drained batch: blocks on <c>get_all</c>, completes each TCS from its per-index
    /// result (exactly one of metadata / error is non-null, per the header), and frees every
    /// native handle on every path — each consumed index's metadata/error as it is read, all
    /// future handles via <c>destroy_all</c> in the <c>finally</c>, plus a <c>finally</c> sweep of
    /// any metadata/error handles for indices left unconsumed by a partway throw (e.g. an OOM in
    /// the error branch). Consumed slots are nulled so the sweep never double-frees.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The three marshalling arrays are REUSED FIELDS, not per-batch allocations</b> (M11/P3.1
    /// §12.3), which is what the drain cap makes possible and is strictly more faithful: the anchor
    /// uses fixed-size <b>stack</b> arrays (<c>_confluentkafka.c:429-430</c>) and allocates nothing
    /// per batch. It also removes a per-drain allocation from the DoD §10 gate — and it removed a
    /// failure mode outright: this method used to allocate them <em>outside</em> its
    /// <c>try</c>/<c>finally</c> and needed an allocation <c>catch</c> to free the batch's futures on
    /// that one path. With no allocation left to fail, that <c>catch</c> is gone rather than left as
    /// misleading dead code, and the <c>finally</c> below is now the <em>only</em> future-free site.
    /// </para>
    /// <para>
    /// ⚠ <b>Reuse means stale values live past the batch count — so every loop is bounded by the
    /// batch's count, NEVER by <c>Length</c>.</b> A reused array still holds the previous pass's
    /// handle values beyond this pass's count; reading <c>Length</c> anywhere would treat those as
    /// live and free them a SECOND time. The <c>0..count</c> range is additionally cleared on entry,
    /// so a partially-filled batch cannot read a stale slot either. Both properties are covered by a
    /// large-drain-then-small-drain test, which is the only shape that can catch a <c>Length</c>
    /// bound.
    /// </para>
    /// <para>
    /// <b><see cref="RunLoop"/> is the only caller</b>, and its drain is capped at
    /// <see cref="DrainCap"/>, so <c>count</c> can never exceed the arrays' length. Were that ever
    /// to change, the <c>Array.Clear</c> below throws rather than overrunning, and
    /// <see cref="RunLoop"/>'s <c>catch</c> faults the batch — loud, not silent.
    /// </para>
    /// <b>This is the async surface's delivery-callback firing site (M14/P1).</b> Where an index
    /// carries a <see cref="DeliveryRegistration"/>, it is invoked <b>immediately before</b> that
    /// index's <c>TrySetResult</c> / <c>TrySetException</c> — Java's ordering exactly
    /// (<c>ProducerBatch.java:303-323</c>: the future's value is set, the callbacks fire, and only
    /// then does <c>produceFuture.done()</c> release the waiters). The invocation is
    /// <b>unconditional</b>, not gated on <c>TrySet*</c>'s <c>bool</c> return (decision D7): a send
    /// whose awaiter was already canceled still owes its delivery notification.
    /// <see cref="DeliveryRegistration.Fire"/> is a total no-throw boundary, which is what keeps a
    /// throwing user callback from aborting this loop, stranding the remaining indices' awaiters,
    /// or escaping into <see cref="RunLoop"/>'s <c>catch</c> (which would fault the whole batch).
    /// Do not wrap the <c>Fire</c> calls in a second <c>try</c>/<c>catch</c> — it would shadow the
    /// real guard without adding anything.
    /// </remarks>
    private void ProcessBatch(List<PendingSend> batch)
    {
        int count = batch.Count;
        IntPtr[] futures = _futures;
        IntPtr[] metadata = _metadata;
        IntPtr[] errors = _errors;

        // Wipe this pass's range in all three before anything reads it, so no slot can carry a
        // previous pass's handle into this one. Bounded by `count`, not Length: the slots past it
        // are stale by design and are never read (see the remarks).
        Array.Clear(futures, 0, count);
        Array.Clear(metadata, 0, count);
        Array.Clear(errors, 0, count);

        // Cannot throw: a List<T> indexer read below its own Count plus a readonly-struct property
        // read, with no allocation.
        for (int i = 0; i < count; i++)
        {
            futures[i] = batch[i].Future;
        }

        try
        {
            NativeMethods.FutureRecordMetadataGetAll(futures, count, metadata, errors);

            for (int i = 0; i < count; i++)
            {
                TaskCompletionSource<RecordMetadata> completion = batch[i].Completion;
                DeliveryRegistration? delivery = batch[i].Delivery;
                IntPtr meta = metadata[i];
                IntPtr error = errors[i];

                // Null the slots up front: this index's handle is now owned by the local
                // `meta` / `error` and is freed below on every path — success frees `meta` in
                // the inner finally, failure frees `error` inside FromHandle (even if FromHandle
                // OOMs, its own finally destroys the handle). Nulling here means the finally-sweep
                // frees ONLY the indices this loop never reached — the unprocessed tail past a
                // partway throw (e.g. FromHandle OOM) — never an already-consumed slot, so it can
                // never double-free. (The crux of this hardening.)
                metadata[i] = IntPtr.Zero;
                errors[i] = IntPtr.Zero;

                if (meta != IntPtr.Zero)
                {
                    // Success: copy out the fields, then free the metadata handle exactly once
                    // (finally), even if the copy-out throws (e.g. OOM decoding the topic).
                    RecordMetadata? result = null;
                    Exception? marshalFailure = null;
                    try
                    {
                        result = RecordMetadataMarshal.CopyOut(meta);
                    }
                    catch (Exception exception)
                    {
                        marshalFailure = exception;
                    }
                    finally
                    {
                        NativeMethods.RecordMetadataDestroy(meta);
                    }

                    if (marshalFailure is null)
                    {
                        // D3: the delivery callback runs BEFORE the awaiter is released, mirroring
                        // ProducerBatch.completeFutureAndFireCallbacks (callbacks, then done()).
                        delivery?.Fire(result!, null);
                        completion.TrySetResult(result!);
                    }
                    else
                    {
                        // The completion arrived (the send succeeded) but its metadata could not be
                        // marshalled — the callback is still owed. The two surfaces report the same
                        // FAILURE but not the same object here: the awaiter gets `marshalFailure`
                        // raw, while Fire coerces it into the KafkaException its signature demands
                        // (the original survives as InnerException). This is the ONLY path where the
                        // two differ; every KafkaException outcome is passed to both unwrapped.
                        delivery?.Fire(null, marshalFailure);
                        completion.TrySetException(marshalFailure);
                    }
                }
                else
                {
                    // Failure: FromHandle reads the values and frees the error handle exactly once.
                    KafkaException failure = KafkaException.FromHandle(error)
                        ?? new KafkaException("The send failed without an error handle.");

                    // D3 again, and D2/D6: Fire substitutes Java's -1 placeholder metadata, so the
                    // user callback never sees a null (Callback.java:28-33).
                    delivery?.Fire(null, failure);
                    completion.TrySetException(failure);
                }
            }
        }
        finally
        {
            // get_all does not consume the futures — free every future handle exactly once (§A2).
            NativeMethods.FutureRecordMetadataDestroyAll(futures, count);

            // Sweep any metadata/error handles for indices the loop did NOT consume — the
            // unprocessed tail past a partway throw (e.g. FromHandle OOM in the error branch).
            // Consumed indices nulled their slots above, so this frees ONLY the tail — never an
            // already-freed handle (no double-free). Both destroys are null-safe; the != Zero
            // guard skips needless P/Invokes on the normal path (every slot already nulled).
            // Completes the "free every handle on every path" pattern (ffi §A2) that the RunLoop
            // catch (faults the TCSes) established. Since the marshalling arrays became reused
            // fields (§12.3) there is no pre-`try` allocation left to fail, so this finally is now
            // the ONLY future-free site and no companion allocation-catch is needed.
            for (int i = 0; i < count; i++)
            {
                if (metadata[i] != IntPtr.Zero)
                {
                    NativeMethods.RecordMetadataDestroy(metadata[i]);
                }

                if (errors[i] != IntPtr.Zero)
                {
                    NativeMethods.ErrorDestroy(errors[i]);
                }
            }
        }
    }

    /// <summary>
    /// Faults + frees every still-queued send at teardown (no blocking <c>get_all</c>): the sends
    /// were accepted but the producer is closing, so their tasks fault with a teardown exception
    /// and their futures are destroyed. Runs under <see cref="_stopLock"/> from <see cref="Stop"/>.
    /// </summary>
    /// <remarks>
    /// <b>This path does NOT invoke the sends' <see cref="IDeliveryCallback"/>s</b> — recorded
    /// residual 2 on <see cref="IDeliveryCallback"/>, and the same reasoning
    /// as <see cref="Enqueue"/>'s fault-in-place branch (residual 1): the callback reports a
    /// <em>core</em> completion and there is none here, because this method deliberately issues no
    /// blocking <c>get_all</c> — these sends were never resolved. Python behaves identically (its
    /// <c>close()</c> cancels the pending futures without invoking <c>on_delivery</c>). Firing a
    /// fabricated failure here would also reintroduce a double-fire hazard, since the core may still
    /// deliver these records.
    /// <para>
    /// ⚠ <b>This drain is deliberately UNCAPPED, and capping it would be a defect, not a symmetry</b>
    /// (M11/P3.1 §12.2.2). Two reasons, either sufficient: it <b>faults</b> rather than calling
    /// <c>get_all</c>, so it allocates <b>none</b> of the three marshalling arrays the cap exists to
    /// bound; and it is the <b>last thing that ever touches the queue</b>, so a cap would strand
    /// every send past it — their <see cref="TaskCompletionSource{TResult}"/>s never completed, their
    /// awaiters hanging forever, and their future handles never destroyed.
    /// </para>
    /// </remarks>
    private void DrainAndFaultRemaining()
    {
        List<PendingSend> batch = DrainAll(int.MaxValue);
        if (batch.Count == 0)
        {
            return;
        }

        int count = batch.Count;
        IntPtr[] futures = new IntPtr[count];
        KafkaException teardown = TeardownException();
        for (int i = 0; i < count; i++)
        {
            futures[i] = batch[i].Future;
            batch[i].Completion.TrySetException(teardown);
        }

        DestroyFutures(futures, count);
    }

    /// <summary>
    /// Faults every TCS in <paramref name="batch"/> after a <see cref="ProcessBatch"/> throw — the
    /// no-hang guard for the batch that was in flight when the throw escaped. <b>TCS-only:</b>
    /// <see cref="ProcessBatch"/>'s <c>finally</c> already freed the future handles, so this must
    /// NOT free them again (no double-free).
    /// <see cref="TaskCompletionSource{TResult}.TrySetException(System.Exception)"/> is a no-op on
    /// an already-completed TCS, so faulting the whole batch is safe even if some indices completed
    /// before the throw.
    /// </summary>
    /// <remarks>
    /// <b>This path does NOT invoke the batch's <see cref="IDeliveryCallback"/>s</b> — recorded
    /// residual 3 on <see cref="IDeliveryCallback"/>. This residual
    /// spans <b>both sides of the completion's arrival</b>,
    /// because <see cref="ProcessBatch"/> can throw on either side of its <c>get_all</c> and both
    /// land here. <b>(a)</b> After <c>get_all</c> reported — it reported for the <em>whole</em>
    /// batch, so the core <em>did</em> report these completions; the indices the per-index loop had
    /// already reached fired normally and the rest are faulted here with none.
    /// <b>(b)</b> Before it reported — the throw came from the batch setup (the marshalling-array
    /// allocation) or out of the <c>get_all</c> P/Invoke itself, as <see cref="RunLoop"/>'s
    /// <c>catch</c> spells out, so no completion was ever in hand and the whole batch is faulted
    /// with none.
    /// <b>Firing them here is deliberately NOT the fix</b>, and sub-case (a) alone is enough to
    /// settle it: this method faults the batch <em>wholesale</em> (that is exactly why
    /// <c>TrySetException</c>'s no-op-on-completed behavior is load-bearing above) and it has no
    /// per-index record of which callbacks already fired, so firing would deliver a
    /// <em>duplicate</em> notification for every index that completed before the throw — trading a
    /// rare dropped notification for a rare double invocation, which the exactly-once-per-record
    /// obligation (root <c>CLAUDE.md</c> §9.5) makes strictly worse. In sub-case (b) nothing was
    /// reported at all, so anything fired would instead be an <em>invented</em> failure for a record
    /// the core may still deliver. A per-index "already fired" latch would close sub-case (a), at
    /// the cost of per-send state on a path reachable only under out-of-memory or an unexpected
    /// managed or native failure in the batch read; the drop is recorded on the public surface
    /// instead (ffi §A6 form C's at-most-once boundary).
    /// <para>
    /// How this residual compares with the others — teardown or not, completion arrived or not,
    /// a throw versus a faulted <see cref="Task"/> — is stated <b>once</b>, under <b>the
    /// distinguishing axes</b> in the remarks on <see cref="IDeliveryCallback"/>. Do not restate
    /// those axes here, and do not re-scope one either: several review rounds went on paraphrases of
    /// them that went stale one at a time, this note's own included.
    /// </para>
    /// </remarks>
    private static void FaultBatchCompletions(List<PendingSend> batch, Exception cause)
    {
        KafkaException failure = cause as KafkaException
            ?? new KafkaException("The producer send-completion pump failed to process a batch.", cause);
        foreach (PendingSend pending in batch)
        {
            pending.Completion.TrySetException(failure);
        }
    }

    private static void DestroyFutures(IntPtr[] futures, int count) =>
        NativeMethods.FutureRecordMetadataDestroyAll(futures, count);

    private static KafkaException TeardownException() =>
        new KafkaException("The producer was closed before the send completed.");

    /// <summary>
    /// An enqueued send awaiting resolution: its future handle, its awaiter, and — when the caller
    /// supplied an <see cref="IDeliveryCallback"/> — the carrier to invoke on completion.
    /// </summary>
    /// <remarks>
    /// <see cref="Delivery"/> is <b>one nullable reference field</b> and is <see langword="null"/>
    /// on the plain <c>Send(record)</c> path (M14/P1 decision D11): a
    /// <see cref="ConcurrentQueue{T}"/> stores its items in segment arrays, so widening the struct
    /// adds no per-send heap allocation and a null field adds nothing at all — the
    /// allocation-budgeted send path is unchanged.
    /// </remarks>
    private readonly struct PendingSend
    {
        internal PendingSend(
            IntPtr future,
            TaskCompletionSource<RecordMetadata> completion,
            DeliveryRegistration? delivery)
        {
            Future = future;
            Completion = completion;
            Delivery = delivery;
        }

        internal IntPtr Future { get; }

        internal TaskCompletionSource<RecordMetadata> Completion { get; }

        /// <summary>The user's delivery callback carrier, or <see langword="null"/> if none.</summary>
        internal DeliveryRegistration? Delivery { get; }
    }
}
