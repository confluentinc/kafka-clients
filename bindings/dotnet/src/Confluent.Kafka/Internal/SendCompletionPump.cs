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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The producer send-completion pump (inline pull-pump — PLAN §3 Option C; ffi §A7's pull surface; §6.3): one
/// background thread per <see cref="NativeProducer"/> that takes one <c>send_batch</c> group at a
/// time off an unbounded MPSC queue (M11/P3.2 §3B — the anchor's unit, one <c>BatchNode</c> per
/// <c>get_all</c>), blocks on a batched
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
    /// The size of the three reused <c>get_all</c> marshalling arrays, and the largest number of
    /// completions one <c>get_all</c> pass can therefore handle — Python's
    /// <c>PRODUCER_RECORD_SLOT_CAPACITY</c> (1100), the size of the anchor's own <c>get_all</c>
    /// output arrays (<c>_confluentkafka.c:429-430</c>) and of one <c>BatchNode</c>, which is what
    /// its poll thread processes per iteration (<c>:480-513</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Its job changed in M11/P3.2 S3 (§3B.4) — it is no longer a DRAIN cap.</b> Until then
    /// this queue held one entry per <b>record</b> and a pass drained up to <c>DrainCap</c> of them,
    /// so the constant was what bounded a pass. The queue now holds one entry per
    /// <c>send_batch</c> <b>group</b> and a pass takes exactly one group (<see cref="RunLoop"/>),
    /// so there is no drain left to cap and the bound follows from the <em>unit</em>. What survives
    /// is two jobs: (i) the <b>capacity</b> of <see cref="_futures"/> / <see cref="_metadata"/> /
    /// <see cref="_errors"/>, and (ii) the <b>sub-pass bound</b> <see cref="ProcessGroup"/> splits an
    /// oversized group by.
    /// </para>
    /// <para>
    /// <b>Why a group can be oversized at all, and why the split rather than bigger arrays.</b>
    /// This constant is derived from the <b>default</b> settings, exactly as the anchor's arrays are
    /// sized by a compile-time <c>#define</c> — but .NET's node capacity is <b>runtime</b>
    /// (<c>SlotCapacity = CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD + 100</c>,
    /// <see cref="SendAccumulatorSettings.SlotCapacity"/>), so a raised threshold produces groups
    /// larger than these arrays — the one case the anchor cannot have, recorded as deviation
    /// <b>DV-7</b>. The alternative considered and <b>rejected</b> was sizing the arrays from the
    /// owning producer's effective <c>SlotCapacity</c>: it would thread per-producer settings into
    /// shared machinery that predates them, and drop the const-sized rationale above, to buy
    /// nothing the split does not already buy. Splitting degrades an oversized group to the
    /// pre-S3 behaviour (several bounded passes) rather than to a fault, and grouping still holds
    /// exactly whenever a group fits — the default and every sane configuration.
    /// </para>
    /// <para>
    /// <b>Parity, now structural rather than numeric (M11/P3.2 §F3).</b> The anchor completes
    /// exactly one <c>BatchNode</c> per <c>get_all</c> (<c>_confluentkafka.c:487-495</c>) and one
    /// node <em>is</em> one <c>send_batch</c> (<c>:593</c>), so a completion batch there is one send
    /// call's worth of records and never mixes records from different sends. Before S3 this binding
    /// matched the <b>number</b> (this constant) but not the <b>shape</b>: a capped pass could span
    /// records from arbitrarily many <c>send_batch</c> calls, and <c>get_all</c> returns only once
    /// <em>every</em> future in the array resolves, so the first record's completion was gated on
    /// the slowest of up to 1100 records it was never sent with. S3 removed that (user decision
    /// <b>D2</b>), so it is a difference this binding <b>closed</b>, not a deviation it keeps —
    /// M11/P3.2 §10 records it as a deleted deviation entry rather than a filed one.
    /// </para>
    /// </remarks>
    internal const int DrainCap =
        SendAccumulatorSettings.DefaultSlotThreshold + SendAccumulatorSettings.SlotCapacityHeadroom;

    // One entry per send_batch CALL (M11/P3.2 §3B.1), not per record: the anchor's completion unit.
    private readonly ConcurrentQueue<PendingSendBatch> _queue = new ConcurrentQueue<PendingSendBatch>();

    // The three marshalling arrays get_all reads and writes, allocated ONCE (§12.3) — the anchor
    // allocates nothing per batch either (its equivalents are fixed-size stack arrays,
    // _confluentkafka.c:429-430). Touched only by the pump thread inside ProcessBatch, so they need
    // no synchronization. ⚠ They retain the previous pass's values past the current pass's count:
    // see ProcessBatch's remarks for why every loop over them is bounded by that count and never by
    // Length.
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
    // largest pass any of them carried. The GROUPING (M11/P3.2 §F3) is asserted by these — "the
    // completions all resolved" is satisfied by an ungrouped pump too, so it cannot tell whether a
    // pass carries one send_batch call's records or an arbitrary mixture of many.
    private long _processedBatches;
    private int _largestProcessedBatch;

    // Instrumentation: how many sends this pump has taken OFF its queue, counted in DequeueGroup and
    // so covering both consumers — RunLoop's group loop and Stop's terminal drain.
    //
    // ⚠ It counts RECORDS, not groups (M11/P3.2 §3B.4). The §3.8 guard below reads it as a record
    // count, so adding 1 per group instead of group.Count would silently turn a record assertion
    // into a group assertion and the guard would go on passing while measuring the wrong thing.
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
    /// <remarks>
    /// One per queued group, except that a group larger than <see cref="DrainCap"/> is split into
    /// <c>ceil(count / DrainCap)</c> passes (<see cref="ProcessGroup"/>).
    /// </remarks>
    internal long ProcessedBatchCount => Interlocked.Read(ref _processedBatches);

    /// <summary>The record count of the largest pass any of them carried — never above <see cref="DrainCap"/>.</summary>
    internal int LargestProcessedBatch => Volatile.Read(ref _largestProcessedBatch);

    /// <summary>
    /// The number of sends — <b>records</b>, not groups — this pump has taken off its queue, i.e.
    /// the number that reached <see cref="Enqueue"/> while the gate was still <b>open</b>. The
    /// witness for the M11/P3.1 §3.8 teardown ordering (see <c>_drainedSends</c>).
    /// </summary>
    internal long DrainedSendCount => Interlocked.Read(ref _drainedSends);

    /// <summary>
    /// Enqueues <b>one <c>send_batch</c> call's</b> accepted sends as a single completion group
    /// (M11/P3.2 §3B, user decision <b>D2</b>). On the normal path the pump resolves the whole group
    /// in one <c>get_all</c>, completes each awaiter, invokes each supplied delivery callback and
    /// frees every future; if the pump has already stopped (teardown raced this enqueue) the group
    /// is faulted and its futures freed here, so no send is stranded or leaked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The group is the anchor's completion unit.</b> Python's poll thread completes exactly one
    /// <c>BatchNode</c> per <c>get_all</c> (<c>_confluentkafka.c:487-495</c>) and one node <em>is</em>
    /// one <c>send_batch</c> (<c>:593</c>). The arity here is what makes that structural rather than
    /// numeric: a pass can no longer mix records from different sends, so one slow record can only
    /// delay records it was actually sent with.
    /// </para>
    /// <para>
    /// <b>Ownership of all four arguments transfers on return</b>, and only on return — see the
    /// hand-over note in <c>SendAccumulator.CompleteNode</c> for why the caller must not release its
    /// own claim on the futures until this method has returned normally. The three arrays are the
    /// caller's and are never mutated here; <paramref name="count"/> may be shorter than their
    /// length, because the caller compacts out the indices the core rejected outright.
    /// </para>
    /// <para>
    /// <b>The teardown fault-in-place branch does NOT invoke the group's delivery callbacks</b> —
    /// recorded residual 1 on <see cref="IDeliveryCallback"/>. The callback
    /// reports a <em>core</em> completion, and on this branch there is none: the records were handed
    /// to native but the pump that would collect their results is gone. Python behaves identically
    /// (its <c>close()</c> cancels the pending futures without invoking <c>on_delivery</c>). Firing
    /// a fabricated failure here would also reintroduce a double-fire hazard, because the records
    /// may still be delivered by the core.
    /// </para>
    /// </remarks>
    /// <param name="futures">
    /// The group's future handles, in <c>send_batch</c> index order (ownership transfers to the
    /// pump).
    /// </param>
    /// <param name="completions">The sends' awaiters, index-aligned with <paramref name="futures"/>.</param>
    /// <param name="deliveries">
    /// The users' delivery callback carriers, index-aligned with <paramref name="futures"/>;
    /// <see langword="null"/> at an index whose send took the plain <c>Send(record)</c> path
    /// (M14/P1).
    /// </param>
    /// <param name="count">How many leading entries of the three arrays this group holds.</param>
    internal void Enqueue(
        IntPtr[] futures,
        TaskCompletionSource<RecordMetadata>[] completions,
        DeliveryRegistration?[] deliveries,
        int count)
    {
        lock (_stopLock)
        {
            if (_stopped)
            {
                // The pump is torn down: fault + free in place rather than queue into a dead pump.
                // Bounded by `count`, never by Length — the tail past it is the caller's compaction
                // slack and holds no handle and no awaiter.
                KafkaException teardown = TeardownException();
                for (int i = 0; i < count; i++)
                {
                    completions[i].TrySetException(teardown);
                }

                DestroyFutures(futures, count);
                return;
            }

            _queue.Enqueue(new PendingSendBatch(futures, completions, deliveries, count));
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
            // thread already KNOWS about (another group is already queued), while the Reset ordering
            // handles items arriving CONCURRENTLY — their Set lands after this Reset, so the next
            // Wait returns. Neither subsumes the other; do not "simplify" the ordering away because
            // the inner loop looks like it covers the same ground.
            _signal.Reset();

            if (_stopping)
            {
                // Leave any queued items for Stop's terminal drain (fault, no blocking get_all).
                break;
            }

            // ⚠ INNER GROUP LOOP — THE HANG FIX (M11/P3.1 §12.2.1), carried into the grouped shape
            // (M11/P3.2 §3B.4). A pass takes exactly ONE group, so the outer loop's correctness
            // would otherwise rest on there never being a second one queued — and leaving it behind
            // is a DETERMINISTIC hang, not a race, needing no concurrency to reproduce: three groups
            // queued, one processed, two left, and the next iteration blocks on _signal.Wait() —
            // reset above and only Set by a new Enqueue — so with no further sends those two groups'
            // completions and every awaiting Task hang forever.
            //
            // Keep taking groups until the queue comes back EMPTY, and only then fall through to the
            // Wait. That is precisely the anchor's shape: its poll thread completes one node,
            // advances to next_batch, and cnd_waits ONLY while that is NULL
            // (_confluentkafka.c:504-506).
            //
            // Deliberately NOT "call _signal.Set() when items remain": the loop is clearer, is what
            // the anchor does, and does not depend on reasoning about a self-signal racing a Reset.
            PendingSendBatch? group;
            while ((group = DequeueGroup()) is not null)
            {
                try
                {
                    ProcessGroup(group);
                }
                catch (Exception exception)
                {
                    // ProcessGroup already freed every one of the group's future handles on every
                    // one of its own paths (ProcessBatch's `finally`'s destroy_all for the passes it
                    // ran, and ProcessGroup's own `finally` for the tail of a group whose split was
                    // cut short — since M11/P3.1 §12.3 those are the ONLY future-free sites, because
                    // the marshalling arrays are reused fields and there is no pre-`try` allocation
                    // left to fail), but a throw that escaped it — a native failure surfacing from
                    // get_all, or OOM either side of its per-index completion loop — left the
                    // group's TCSes UNcompleted, so their awaiters would hang forever. Fault them
                    // here (TCS-only — the handles are already freed, so do NOT free them again).
                    // CONTINUE, don't break: faulting this group and looping keeps the pump draining
                    // later Enqueues; breaking would exit the thread WITHOUT marking the pump
                    // stopped (only Stop sets _stopped), so subsequent Enqueues would queue into a
                    // dead pump and hang. (A
                    // truly process-corrupting AccessViolation is not catchable by design — the
                    // process terminates; this guards the catchable cases: OOM, or a managed
                    // marshalling throw that escapes ProcessGroup.)
                    FaultGroupCompletions(group, exception);
                }
            }
        }
    }

    /// <summary>
    /// Takes the next queued <c>send_batch</c> group, or <see langword="null"/> when the queue is
    /// empty (lock-free).
    /// </summary>
    /// <remarks>
    /// <b>The one point every queued send passes through exactly once</b>, whichever of the two
    /// consumers takes it — <see cref="RunLoop"/>'s group loop or
    /// <see cref="DrainAndFaultRemaining"/> — which is why <c>_drainedSends</c> is counted here.
    /// It is incremented by the group's <b>record</b> count, never by one per group: see
    /// <c>_drainedSends</c>.
    /// </remarks>
    private PendingSendBatch? DequeueGroup()
    {
        if (!_queue.TryDequeue(out PendingSendBatch? group))
        {
            return null;
        }

        Interlocked.Add(ref _drainedSends, group.Count);
        return group;
    }

    /// <summary>
    /// Resolves one queued group: normally a single <see cref="ProcessBatch"/> pass over the whole
    /// group, which is what makes a completion batch exactly one <c>send_batch</c> call's records.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>The one case that needs more than one pass, and the one hazard grouping introduced</b>
    /// (M11/P3.2 §3B.3, deviation <b>DV-7</b>). The three marshalling arrays are
    /// <see cref="DrainCap"/> long — a compile-time constant — while a group is as large as the
    /// producer's <em>runtime</em> node capacity, so a raised
    /// <c>CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD</c> can hand this method more records than the
    /// arrays hold. Splitting into <c>ceil(count / DrainCap)</c> bounded sub-passes degrades that
    /// case to the pre-S3 behaviour; <b>not</b> splitting would let
    /// <see cref="ProcessBatch"/>'s <c>Array.Clear</c> throw and
    /// <see cref="RunLoop"/>'s <c>catch</c> fault the whole group — every send failing under a
    /// legitimate, documented override. See <see cref="DrainCap"/> for the alternative considered
    /// (runtime-sized arrays) and why it was rejected.
    /// </para>
    /// <para>
    /// <b>Why the <c>finally</c>, and why it is bounded by <c>done</c>.</b> Each
    /// <see cref="ProcessBatch"/> pass frees its own sub-range's futures in its own <c>finally</c>,
    /// so <c>done</c> is advanced <em>before</em> the call that takes ownership of the sub-range. A
    /// throw therefore leaves exactly <c>[done, Count)</c> — the sub-passes that never ran — still
    /// holding futures nobody would free, and this <c>finally</c> is the site that frees them. It
    /// allocates nothing (a per-handle destroy rather than a copied slice), because the throw it
    /// covers may itself be an <see cref="OutOfMemoryException"/>. On the normal path
    /// <c>done == Count</c> and the loop body never runs.
    /// </para>
    /// </remarks>
    private void ProcessGroup(PendingSendBatch group)
    {
        int done = 0;
        try
        {
            while (done < group.Count)
            {
                int start = done;
                int length = Math.Min(DrainCap, group.Count - start);

                // Advance BEFORE the call: from here the pass owns [start, start + length) and frees
                // those futures on every one of its own paths, so the finally below must not.
                done = start + length;

                Interlocked.Increment(ref _processedBatches);
                if (length > _largestProcessedBatch)
                {
                    Volatile.Write(ref _largestProcessedBatch, length);
                }

                ProcessBatch(group, start, length);
            }
        }
        finally
        {
            for (int i = done; i < group.Count; i++)
            {
                NativeMethods.FutureRecordMetadataDestroy(group.Futures[i]);
            }
        }
    }

    /// <summary>
    /// Resolves one pass over <paramref name="count"/> of <paramref name="group"/>'s sends, starting
    /// at <paramref name="start"/>: blocks on <c>get_all</c>, completes each TCS from its per-index
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
    /// ⚠ <b>Reuse means stale values live past this pass's count — so every loop is bounded by
    /// <paramref name="count"/>, NEVER by <c>Length</c>.</b> A reused array still holds the previous
    /// pass's handle values beyond this pass's count; reading <c>Length</c> anywhere would treat
    /// those as live and free them a SECOND time. The same rule governs
    /// <paramref name="group"/>'s own arrays, whose tail past <c>Count</c> is the accumulator's
    /// compaction slack. The <c>0..count</c> range of the two output arrays is additionally cleared
    /// on entry (the futures range is wholly overwritten by the copy-in, so clearing it as well
    /// would be dead work), so a partially-filled pass cannot read a stale slot either. Both
    /// properties are covered by a large-group-then-small-group test, which is the only shape that
    /// can catch a <c>Length</c> bound.
    /// </para>
    /// <para>
    /// <b><see cref="ProcessGroup"/> is the only caller</b>, and it bounds every pass by
    /// <see cref="DrainCap"/> — splitting an oversized group rather than handing one down whole — so
    /// <paramref name="count"/> can never exceed the arrays' length. ⚠ That bound is now the
    /// <em>split</em>, not a drain cap: before M11/P3.2 S3 it came from the queue being drained
    /// <see cref="DrainCap"/> items at a time, and a group is not drained at all. Were the split
    /// ever removed, the <c>Array.Clear</c> below throws rather than overrunning, and
    /// <see cref="RunLoop"/>'s <c>catch</c> faults the group — loud, not silent, but every send in
    /// that group fails, which is why the split exists.
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
    /// or escaping into <see cref="RunLoop"/>'s <c>catch</c> (which would fault the whole group).
    /// Do not wrap the <c>Fire</c> calls in a second <c>try</c>/<c>catch</c> — it would shadow the
    /// real guard without adding anything.
    /// </remarks>
    private void ProcessBatch(PendingSendBatch group, int start, int count)
    {
        IntPtr[] futures = _futures;
        IntPtr[] metadata = _metadata;
        IntPtr[] errors = _errors;

        // Wipe this pass's range in the two OUTPUT arrays before anything reads it, so no slot can
        // carry a previous pass's handle into this one. Bounded by `count`, not Length: the slots
        // past it are stale by design and are never read (see the remarks).
        Array.Clear(metadata, 0, count);
        Array.Clear(errors, 0, count);

        // Cannot throw: an in-range copy between two IntPtr[] with no allocation. It also fully
        // overwrites `futures[0..count)`, which is why that array is not cleared above.
        Array.Copy(group.Futures, start, futures, 0, count);

        try
        {
            NativeMethods.FutureRecordMetadataGetAll(futures, count, metadata, errors);

            for (int i = 0; i < count; i++)
            {
                TaskCompletionSource<RecordMetadata> completion = group.Completions[start + i];
                DeliveryRegistration? delivery = group.Deliveries[start + i];
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
    /// (M11/P3.1 §12.2.2, unchanged by the M11/P3.2 §3B.5 rewrite to groups). Two reasons, either
    /// sufficient: it <b>faults</b> rather than calling <c>get_all</c>, so it needs <b>none</b> of
    /// the three marshalling arrays whose capacity <see cref="DrainCap"/> now is; and it is the
    /// <b>last thing that ever touches the queue</b>, so a cap would strand every send past it —
    /// their <see cref="TaskCompletionSource{TResult}"/>s never completed, their awaiters hanging
    /// forever, and their future handles never destroyed.
    /// <para>
    /// ⚠ <b>Which is why this must NOT reuse <see cref="ProcessGroup"/>'s sub-pass split.</b> That
    /// helper is <em>bounded</em> by <see cref="DrainCap"/> because it marshals into the fixed
    /// arrays; this one is not, because it marshals nothing. They are kept as separate functions
    /// deliberately — folding them together would import the bound and reintroduce the defect above
    /// as an accident. This loop takes <b>every</b> group and faults <b>every</b> record of each.
    /// </para>
    /// </para>
    /// </remarks>
    private void DrainAndFaultRemaining()
    {
        KafkaException? teardown = null;

        PendingSendBatch? group;
        while ((group = DequeueGroup()) is not null)
        {
            teardown ??= TeardownException();

            // Bounded by the group's `Count`, never by its arrays' `Length`: the tail past it is the
            // accumulator's compaction slack and holds no handle and no awaiter.
            for (int i = 0; i < group.Count; i++)
            {
                group.Completions[i].TrySetException(teardown);
            }

            DestroyFutures(group.Futures, group.Count);
        }
    }

    /// <summary>
    /// Faults every TCS in <paramref name="group"/> after a <see cref="ProcessGroup"/> throw — the
    /// no-hang guard for the group that was in flight when the throw escaped. <b>TCS-only:</b>
    /// <see cref="ProcessBatch"/>'s <c>finally</c> and <see cref="ProcessGroup"/>'s own already
    /// freed every future handle between them, so this must NOT free them again (no double-free).
    /// <see cref="TaskCompletionSource{TResult}.TrySetException(System.Exception)"/> is a no-op on
    /// an already-completed TCS, so faulting the whole group is safe even if some indices completed
    /// before the throw — including a whole earlier sub-pass, when an oversized group was split.
    /// </summary>
    /// <remarks>
    /// <b>This path does NOT invoke the group's <see cref="IDeliveryCallback"/>s</b> — recorded
    /// residual 3 on <see cref="IDeliveryCallback"/>. <b>Firing them here is deliberately not the
    /// fix</b>, and the local reason is this method's own shape: it faults the group
    /// <em>wholesale</em> (that is exactly why <c>TrySetException</c>'s no-op-on-completed behavior
    /// is load-bearing above) and keeps no per-index record of which callbacks already fired, so
    /// firing here would deliver a <em>duplicate</em> notification for every index that completed
    /// before the throw — trading a rare dropped notification for a rare double invocation, which
    /// the exactly-once-per-record obligation (root <c>CLAUDE.md</c> §9.5) makes strictly worse. A
    /// per-index "already fired" latch is what would close that, at the cost of per-send state on a
    /// path reachable only under an unexpected managed or native failure in the batch read; the drop
    /// is recorded on the public surface instead (ffi §A6 form C's at-most-once boundary).
    /// <para>
    /// The conditions this residual spans, how it compares with the others — teardown or not,
    /// completion arrived or not, a throw versus a faulted <see cref="Task"/> — and why firing is
    /// not the fix on the side where nothing had been reported either, are stated <b>once</b>, under
    /// <b>the distinguishing axes</b> in the remarks on <see cref="IDeliveryCallback"/>. Do not
    /// restate them here, and do not re-scope them either: several review rounds went on paraphrases
    /// that went stale one at a time — this note's own included, which named the per-batch
    /// marshalling-array allocation as a live trigger after §12.3 had removed it, and cited
    /// <see cref="RunLoop"/>'s <c>catch</c> as spelling that out when the same slice had rewritten
    /// it to say the opposite.
    /// </para>
    /// </remarks>
    private static void FaultGroupCompletions(PendingSendBatch group, Exception cause)
    {
        KafkaException failure = cause as KafkaException
            ?? new KafkaException("The producer send-completion pump failed to process a batch.", cause);

        // Bounded by `Count`, never by `Length` — the tail is compaction slack with no awaiter.
        for (int i = 0; i < group.Count; i++)
        {
            group.Completions[i].TrySetException(failure);
        }
    }

    private static void DestroyFutures(IntPtr[] futures, int count) =>
        NativeMethods.FutureRecordMetadataDestroyAll(futures, count);

    private static KafkaException TeardownException() =>
        new KafkaException("The producer was closed before the send completed.");

    /// <summary>
    /// One <c>send_batch</c> call's accepted sends awaiting resolution: their future handles, their
    /// awaiters, and — where the caller supplied an <see cref="IDeliveryCallback"/> — the carriers to
    /// invoke on completion. The pump's completion unit (M11/P3.2 §3B.2).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why a batch object rather than the accumulator's node.</b> Python hands its poll thread the
    /// <c>BatchNode</c> itself and then <b>frees</b> it (<c>_confluentkafka.c:510</c>). This binding
    /// cannot: M11/P3.1 made nodes <em>recycled</em> (one spare kept for reuse, so the steady-state
    /// send path allocates no nodes), so handing one over would either kill that recycling or create
    /// cross-thread ownership of a recycled object plus a return channel — strictly more machinery
    /// for no behavioural gain. A small immutable carrier gives the same grouping with the node's
    /// lifecycle untouched.
    /// </para>
    /// <para>
    /// <b><see cref="Count"/> may be shorter than the arrays.</b> The accumulator settles the
    /// indices the core rejected outright in place and hands over only the accepted ones — the
    /// anchor's compaction (<c>:600-621</c>) — so the tail past <see cref="Count"/> is slack holding
    /// no handle and no awaiter. <b>Every</b> loop over these arrays is bounded by
    /// <see cref="Count"/>, never by <c>Length</c>.
    /// </para>
    /// <para>
    /// <b>Allocation direction (DoD §10).</b> One carrier plus three arrays per <c>send_batch</c>
    /// call replaces up to 1100 per-record enqueues and their <see cref="ConcurrentQueue{T}"/>
    /// segment churn, and it removed the per-drain <c>List</c> the old flat drain accumulated into
    /// (M11/P3.1 §12.3's recorded reuse follow-up, closed by construction). It is <b>not</b> on the
    /// per-record send path — it is allocated on the batch thread, once per send call. A
    /// carrier <b>pool</b> is the new deliberately-not-taken item: recorded, not built, because it
    /// would turn a per-call allocation into a free-list to reason about for no measured gain.
    /// </para>
    /// </remarks>
    private sealed class PendingSendBatch
    {
        internal PendingSendBatch(
            IntPtr[] futures,
            TaskCompletionSource<RecordMetadata>[] completions,
            DeliveryRegistration?[] deliveries,
            int count)
        {
            Futures = futures;
            Completions = completions;
            Deliveries = deliveries;
            Count = count;
        }

        internal IntPtr[] Futures { get; }

        internal TaskCompletionSource<RecordMetadata>[] Completions { get; }

        /// <summary>The users' delivery callback carriers; <see langword="null"/> at an index with none.</summary>
        internal DeliveryRegistration?[] Deliveries { get; }

        /// <summary>How many leading entries of the three arrays this group holds.</summary>
        internal int Count { get; }
    }
}
