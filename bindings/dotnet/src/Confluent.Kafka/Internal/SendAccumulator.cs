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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The async producer's binding-side send accumulator and its batch thread — the .NET realization of
/// the Python binding's <c>Producer_send_thread</c> (M11/P3.1, anchor
/// <c>bindings/python/_confluentkafka.c:523-655</c>). <c>Send</c> pins the record's buffers, appends
/// them to a node chain and returns immediately; this thread waits for a threshold or a free-running
/// window, takes the whole chain, and drives one <c>kafka_producer_Producer_send_batch</c> per node,
/// then unpins and hands the resulting futures to the existing <see cref="SendCompletionPump"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two background threads, not three</b> (§1.2): this batch thread and the completion pump. The
/// caller's thread is not counted. The completion side is unchanged — this type ends at
/// <see cref="SendCompletionPump.Enqueue"/>.
/// </para>
/// <para>
/// <b>The node mirrors the anchor's parallel arrays</b> (<c>BatchNode</c>,
/// <c>_confluentkafka.c:360-369</c>), each sized <see cref="SendAccumulatorSettings.SlotCapacity"/>,
/// with a new node allocated only when the current one fills — exactly the anchor's rule
/// (<c>:806</c>). Two arrays have no Python counterpart and both are forced by .NET: the
/// <c>MemoryHandle</c> / topic pins the deferred send has to keep alive (Python keeps a refcount on
/// the record object and an owned topic copy instead), and the blittable
/// <see cref="ProducerRecordNative"/> array the P/Invoke reads (Python builds the equivalent flat
/// array on its stack, <c>:585</c>).
/// </para>
/// <para>
/// <b>Pin ownership transfers on a successful <see cref="Append"/>.</b> Before it, the caller owns
/// the pins and releases them if the append throws; after it, this type owns them and releases every
/// one exactly once — in a <c>finally</c>, after <c>send_batch</c> returns and before the futures
/// reach the pump (§4.4). Never in the pump, never across the returned <see cref="Task"/>.
/// </para>
/// <para>
/// <b>Submission order is call order</b> (M11/P3.2 §F1, decision D1(a)). The anchor appends
/// <em>unconditionally</em> under its mutex and computes "full" only afterwards
/// (<c>_confluentkafka.c:819-830</c>), so its append order is its call order by construction. .NET
/// cannot copy that mechanism — <c>IAsyncProducer.Send</c> returns the <em>record's delivery</em>
/// <see cref="Task"/> (Java's shape), so there is no post-append suspension point to carry the
/// throttle (deviation DV-1) — so it reproduces the <em>property</em> instead, with the two-part
/// mechanism <see cref="TrySubmitInline"/> / <see cref="SubmitQueued"/> documents: the inline path
/// is refused while anything is queued ahead of it, and everything queued is appended by a
/// <b>single</b> submitter walking a documented-FIFO queue. Both entry points live here rather than
/// in <c>NativeProducer</c> so the test fixture routes through the same decision production does
/// (DoD §12).
/// </para>
/// <para>
/// ⚠ <b>The submitter is NOT a third background thread.</b> It is a task-based loop, started by
/// whichever caller queued the first submission, with <b>at most one</b> in flight per accumulator,
/// and it does not poll — it awaits a backpressure permit and appends. <c>ffi-marshalling.md</c>
/// §A1's "at most two background threads" cap (this batch thread + the completion pump) is
/// therefore unaffected.
/// </para>
/// <para>
/// <b>Teardown is minimal-but-correct in this slice.</b> <see cref="Stop"/> closes the accumulator
/// to new appends, wakes the thread, and joins it — the thread performs one final drain before
/// exiting, so no accepted record is abandoned. It is called <b>before</b>
/// <see cref="SendCompletionPump.CloseGate"/> so the final drain's futures arrive at an <em>open</em>
/// gate: reversing those two would route normal teardown through the pump's fault-in-place branch,
/// which is recorded residual 1 and fires no delivery callback (§3.8). The full handshake — a
/// cancellable backpressure gate and <b>bounded</b> waits with a defined outcome on expiry — is
/// slice S5; the join here is unbounded.
/// </para>
/// </remarks>
internal sealed class SendAccumulator
{
    private readonly SafeProducerHandle _handle;
    private readonly PinnedTopicCache _topics;
    private readonly SendCompletionPump _pump;
    private readonly SendAccumulatorSettings _settings;

    // Guards the node chain, the accumulated counter, the closed/force/draining flags, and doubles
    // as the batch thread's condition variable (Monitor.Wait/PulseAll) — the direct analogue of the
    // anchor's record_batches_mutex + record_batches_new_record_cnd pair.
    private readonly object _gate = new object();

    private static readonly Task s_alreadyDrained = Task.FromResult(true);

    private readonly Thread _thread;
    private readonly long _windowTicks;

    // The stage-1 backpressure bound (§4.6): one permit per record appended-but-not-yet-taken. The
    // anchor's own comment is the Java-faithfulness argument for having it at all — "one complete
    // batch beyond the one being filled — mirrors Java's send() blocking once buffer.memory is
    // full, applied here at batch granularity in front of the Rust accumulator"
    // (_confluentkafka.c:21-26). Unlike Option C's native block inside the coarse FFI mutex, this is
    // a MANAGED, cancellable wait, which is what lets teardown wake a blocked sender (§2.2).
    private readonly SemaphoreSlim _space;

    // Cancelled by Stop() so a Send parked on _space is released instead of pinning teardown behind
    // it (§3.8 step 2, which must precede closing the accumulator), and — unconditionally — by
    // AbandonOnThreadFailure, so the batch thread dying releases those waiters too rather than
    // leaving them to permit arithmetic the failure may already have corrupted (M11/P3.2 slice S5).
    // Never disposed: a
    // CancellationTokenSource with no timer holds no unmanaged resource, and disposing one while a
    // linked registration is being torn down is its own hazard.
    private readonly CancellationTokenSource _spaceGate = new CancellationTokenSource();

    // The chain the batch thread TOOK from the accumulator and has not finished sending yet.
    // Written and read ONLY by the batch thread (RunLoopCore publishes it, SendChain advances it,
    // AbandonOnThreadFailure settles whatever is left), so it needs no lock.
    //
    // It exists because the local variable holding the taken chain used to be the ONLY reference to
    // it: a throw between the take and the end of the send — ReleaseSpace over-releasing, or
    // ReleasePins / FaultNode failing inside SendNode — reached RunLoop's catch, which settles the
    // ACCUMULATOR's chain (_head) and could not see the in-flight one. Every record in it was then
    // stranded forever: its awaiter never completed, its pins never released, its future handles
    // never destroyed — and Stop still reported the thread as exited, so the interned topic buffers
    // those un-released pins point at were freed.
    private Node? _inFlight;

    // The FIFO submission queue and its single appender (M11/P3.2 §3.3). ConcurrentQueue<T> is
    // documented FIFO, which is exactly what SemaphoreSlim is NOT: the .NET documentation states
    // there is "no guaranteed order, such as FIFO or LIFO, in which blocked threads enter the
    // semaphore", so ordering must not rest on the permit primitive.
    private readonly ConcurrentQueue<QueuedSubmission> _submissions =
        new ConcurrentQueue<QueuedSubmission>();

    // Submissions queued-or-appending. Incremented SYNCHRONOUSLY by SubmitQueued before Send
    // returns (what makes the routing correct for one caller), decremented under _gate once the
    // submission has settled. Read lock-free by TrySubmitInline (one volatile read, DoD §10) and
    // under _gate by the two-stage idle predicate.
    //
    // The increment precedes the enqueue, so `_queued == 0` implies BOTH "the queue is empty" and
    // "no submitter is mid-append" — it is the single authority for both questions.
    private int _queued;

    // 0/1 token: at most ONE submitter loop exists at a time, which is what makes the appends
    // ordered without depending on how many permits ReleaseSpace hands out at once. BOTH sides of
    // the start/stop handshake use Interlocked, so both are full fences: that symmetry is what
    // makes the loop's exit re-check sound against store→load reordering (see RunSubmitterAsync).
    private int _submitterRunning;

    // Teardown SEALED the submission queue (M11/P3.2 §F2 / slice S2). One flag, two jobs, both
    // load-bearing:
    //
    //   * no NEW submission may join the queue — which is what makes Stop's flush wait MONOTONE,
    //     and therefore bounded. Without it a caller hammering Send keeps _queued above zero and
    //     the flush waits out its whole share of the teardown bound.
    //   * every submission ALREADY queued is appended BYPASSING the bound instead of being
    //     faulted, so a send whose caller already returned reaches the core rather than being
    //     dropped — the anchor's outcome, since it appends before waiting.
    //
    // Set once by Stop under _gate and never cleared. Read under _gate by SubmitQueued (so the
    // monotonicity is a guarantee rather than a likelihood) and lock-free by the submitter, which
    // only needs the monotone direction: a stale `false` there just means it waits, and Stop
    // cancels _spaceGate AFTER setting this, so the cancellation that wakes the waiter is ordered
    // after the write and the re-read in AppendQueuedAsync's handler sees it.
    private bool _queueSealed;

    private Node? _head;      // oldest node, the one the wait loop measures (anchor: next_batches_to_send)
    private Node? _tail;      // the node being filled (anchor: last_accumulating_batch)
    private Node? _spare;     // one fully-settled node kept for reuse (see RecycleNode)
    private int _accumulated; // records appended but not yet taken by the batch thread
    private bool _closed;     // no further appends; the thread drains once more and exits
    private bool _forceDrain; // a DrainPending caller is waiting; skip the rest of the window
    private bool _draining;   // a taken chain is being sent right now (outside the lock)
    // Awaiters of DrainPendingAsync, released when the accumulator next reaches empty-and-idle.
    // Guarded by _gate.
    private readonly List<TaskCompletionSource<bool>> _idleWaiters = new List<TaskCompletionSource<bool>>();

    private long _sendBatchCalls;
    private long _sendBatchRecords;
    private int _largestSendBatch;

    internal SendAccumulator(
        SafeProducerHandle handle,
        PinnedTopicCache topics,
        SendCompletionPump pump,
        SendAccumulatorSettings settings)
    {
        _handle = handle;
        _topics = topics;
        _pump = pump;
        _settings = settings;
        _windowTicks = (long)(Stopwatch.Frequency * (settings.BatchWindowMs / 1000.0));
        _space = new SemaphoreSlim(settings.MaxAccumulatedRecords, settings.MaxAccumulatedRecords);

        _thread = new Thread(RunLoop)
        {
            IsBackground = true,
            Name = "confluent-kafka-producer-send-batch",
        };
        _thread.Start();
    }

    /// <summary>The settings this accumulator was constructed with (read once, §3.2).</summary>
    internal SendAccumulatorSettings Settings => _settings;

    /// <summary>
    /// The number of <c>send_batch</c> calls issued so far. Exposed so the chunking rule (§3.4) can
    /// be asserted by <b>call count</b> rather than by "the records arrived", which any batching
    /// would satisfy.
    /// </summary>
    internal long SendBatchCallCount => Interlocked.Read(ref _sendBatchCalls);

    /// <summary>
    /// The record count of the largest <c>send_batch</c> call issued so far. This is the direct
    /// witness for "a chunk never spans two nodes": it can never exceed
    /// <see cref="SendAccumulatorSettings.BatchChunk"/>, however deep the drained chain was.
    /// </summary>
    internal int LargestSendBatchCount => Volatile.Read(ref _largestSendBatch);

    /// <summary>The total records passed to <c>send_batch</c> so far (the calls' sizes summed).</summary>
    internal long SendBatchRecordCount => Interlocked.Read(ref _sendBatchRecords);

    /// <summary>
    /// Takes a backpressure permit without waiting — the <b>fast path</b>, which must stay
    /// allocation-free so the common send does not regress the DoD §10 budget. Returns
    /// <see langword="false"/> when the accumulator already holds
    /// <see cref="SendAccumulatorSettings.MaxAccumulatedRecords"/> records that the batch thread has
    /// not taken yet, in which case the caller must go through
    /// <see cref="WaitForSpaceAsync"/> (and, unlike this method, yield).
    /// </summary>
    internal bool TryAcquireSpace() => _space.Wait(0);

    /// <summary>
    /// Waits for a backpressure permit — the <b>slow path</b>. Completes when the batch thread's
    /// next take frees capacity, faults with <see cref="ObjectDisposedException"/> if teardown
    /// cancels the gate first, or cancels if <paramref name="cancellationToken"/> fires.
    /// </summary>
    /// <remarks>
    /// <b>No lost wakeup, by a different mechanism than the anchor's.</b> Python does the
    /// check-and-register under the <em>same</em> mutex its drain holds
    /// (<c>py_Producer_on_space_available</c>), because its "space available" signal is a callback
    /// list that a drain running in between would simply miss. A <see cref="SemaphoreSlim"/> counts
    /// <em>permits</em> instead, so a drain that completes between this caller's failed
    /// <see cref="TryAcquireSpace"/> and its arrival here has already left a permit behind and this
    /// returns immediately. The property is the anchor's; the mechanism is the one .NET already
    /// provides. That is also why the permits can be released <b>outside</b> the accumulator's lock
    /// (as the anchor fires its space callbacks after unlocking, <c>:581</c>), which matters:
    /// releasing under the lock could run a waiter's continuation inline on the batch thread, and
    /// that continuation appends.
    /// <para>
    /// ⚠ <b>Deviation DV-2 (M11/P3.2 §F2) — the substituted primitive also makes this bound HARD,
    /// where the anchor's is SOFT. That is deliberate and must not be "fixed".</b> The
    /// <see cref="SemaphoreSlim"/> is constructed with exactly
    /// <see cref="SendAccumulatorSettings.MaxAccumulatedRecords"/> permits, so the accumulation can
    /// never exceed it. The anchor's cannot hold that: <c>py_Producer_send</c> appends
    /// <em>unconditionally</em> under its mutex (<c>_confluentkafka.c:819-823</c>) and computes
    /// <c>full</c> only afterwards (<c>:830</c>), so with C concurrent senders it can reach
    /// <c>bound + C - 1</c> before anybody waits. The softness is an artefact of checking after
    /// appending, not a design goal; a hard bound is strictly more conservative, and overshooting a
    /// <em>memory</em> bound to imitate that artefact buys nothing and costs predictability. The
    /// no-lost-wakeup note above covers the primitive's <em>equivalence</em>; it does not cover this
    /// strictness difference, which is why the difference is recorded here as well as in the
    /// deviation list (M11/P3.2 §10, mirrored at M11/P3.1 PLAN §3.10).
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Teardown cancelled the gate; nothing was appended.</exception>
    internal async Task WaitForSpaceAsync(CancellationToken cancellationToken)
    {
        CancellationToken gate = _spaceGate.Token;
        if (!cancellationToken.CanBeCanceled)
        {
            try
            {
                await _space.WaitAsync(gate).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (gate.IsCancellationRequested)
            {
                throw ClosedDuringBackpressure();
            }

            return;
        }

        using CancellationTokenSource linked =
            CancellationTokenSource.CreateLinkedTokenSource(gate, cancellationToken);
        try
        {
            await _space.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (gate.IsCancellationRequested)
        {
            // Teardown wins over the caller's token when both fired: the producer is going away, so
            // "closed" is the more accurate answer than "you cancelled".
            throw ClosedDuringBackpressure();
        }
    }

    /// <summary>
    /// How many submissions are queued-or-appending — the witness for the second stage of
    /// "empty and idle", and for "this send was routed to the queue rather than appended inline".
    /// </summary>
    internal int QueuedSubmissionCount => Volatile.Read(ref _queued);

    /// <summary>
    /// <b>The routing decision, slice 1 of the M11/P3.2 §F1 ordering fix.</b> Appends
    /// <paramref name="record"/> inline — on the caller's thread, before returning — and reports
    /// <see langword="true"/>; or reports <see langword="false"/>, in which case the caller must
    /// hand the same submission to <see cref="SubmitQueued"/> and nothing has been appended,
    /// pinned, or charged against the bound.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Two conditions, and the first is the fix.</b> The inline path is taken only when
    /// (i) <b>nothing is queued ahead of this send</b> and (ii) a backpressure permit is free. Test
    /// (ii) alone — which is what this path used to be — lets a later send overtake an earlier one
    /// that is still waiting for capacity: <see cref="ReleaseSpace"/> hands out <em>many</em>
    /// permits at once, so the waiter's continuation and a fresh inline send become runnable
    /// together, and the inline one can reach <see cref="Append"/> first. The records then reach
    /// <c>send_batch</c> — and therefore the wire — in an order the caller never asked for, which
    /// no core-side or broker-side setting can repair, because the reorder happened <b>before</b>
    /// the core saw the records. Java documents ordering as preserved in the default configuration
    /// (<c>ProducerConfig.java:274</c>).
    /// </para>
    /// <para>
    /// <b>Why the count is enough for one caller, and why it is only claimed for one caller.</b>
    /// <see cref="SubmitQueued"/> increments the count <em>synchronously</em>, so a caller's next
    /// <c>Send</c> — which cannot start until the previous one returned — always observes it. Two
    /// <em>different</em> threads racing here are not ordered against each other, and are not
    /// claimed to be: the anchor does not order them either (its interleaving is whatever its mutex
    /// grants), and Java's ordering guarantee is per-producer-per-partition as observed by the
    /// caller, not across concurrent callers.
    /// </para>
    /// <para>
    /// <b>The count is read first, deliberately.</b> Reading it before touching
    /// <see cref="TryAcquireSpace"/> keeps the refusal allocation-free and permit-neutral (no
    /// permit is taken only to be given back), and it is one volatile read on the steady-state
    /// send path (DoD §10 — no new allocation).
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">
    /// The producer is closing — nothing was appended (the same synchronous outcome
    /// <see cref="Submit"/> produces, propagated rather than swallowed).
    /// </exception>
    internal bool TrySubmitInline(
        in SerializedProducerRecord record,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery)
    {
        if (Volatile.Read(ref _queued) != 0)
        {
            return false;
        }

        if (!TryAcquireSpace())
        {
            return false;
        }

        Submit(record, completion, delivery);
        return true;
    }

    /// <summary>
    /// <b>The FIFO submission queue, slice 2 of the M11/P3.2 §F1 ordering fix.</b> Takes ownership
    /// of a submission <see cref="TrySubmitInline"/> refused: enqueues it, and ensures the single
    /// submitter loop that will wait for a permit and append it is running. Returns as soon as the
    /// submission is queued — the caller's awaitable is <paramref name="completion"/>'s
    /// <see cref="Task"/>, identical on both routes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>One appender, so there is no race to order.</b> The naive fix — a "someone is parked"
    /// counter with a per-send <see cref="WaitForSpaceAsync"/> continuation each — still inverts two
    /// consecutively parked sends from the same caller, because a multi-permit
    /// <see cref="ReleaseSpace"/> makes both continuations runnable at once and they then race for
    /// <see cref="_gate"/>. Nor can that be repaired by leaning on <see cref="SemaphoreSlim"/>
    /// fairness: .NET explicitly guarantees no ordering among semaphore waiters. So the queue is a
    /// documented-FIFO <see cref="ConcurrentQueue{T}"/> and exactly one loop drains it.
    /// </para>
    /// <para>
    /// <b>The increment is synchronous and precedes the enqueue.</b> Synchronous, because that is
    /// what makes <see cref="TrySubmitInline"/>'s refusal correct for the caller's <em>next</em>
    /// send. Before the enqueue, because a submitter that dequeued an item whose increment had not
    /// landed would decrement below zero and, worse, would let the idle predicate report empty
    /// while a submission was in flight.
    /// </para>
    /// <para>
    /// <b>Pins are still taken after the permit</b> (M11/P3.1 §4.4, preserved verbatim). A queued
    /// submission holds the <em>unpinned</em> <see cref="SerializedProducerRecord"/> only; all
    /// pinning happens inside <see cref="Submit"/>, after the permit is in hand. The invariant that
    /// rule states — <em>the pin must not be taken before the backpressure permit, or a blocked
    /// sender holds pins while waiting</em> — is therefore unchanged by this fix: what waits in the
    /// queue is a record reference, never a pin.
    /// </para>
    /// <para>
    /// <b>The loop runs on the thread pool, not on the caller's thread.</b> Started via
    /// <see cref="Task.Run(Func{Task})"/> so a caller's <c>Send</c> never synchronously appends
    /// <em>other</em> callers' queued submissions. Ordering does not depend on when it starts: the
    /// queue position is fixed by the enqueue above.
    /// </para>
    /// <para>
    /// <b>A submission arriving after teardown sealed the queue is refused</b> (M11/P3.2 §F2 —
    /// <see cref="_queueSealed"/>), and settled with the identical
    /// <see cref="ObjectDisposedException"/> the submitter would have produced for it, so the
    /// observable outcome for a <c>Send</c> racing <c>Close</c> is unchanged. The check is under
    /// <see cref="_gate"/> — the same lock <see cref="Stop"/> takes to seal — which is what makes
    /// <see cref="_queued"/> strictly decreasing from the seal onward and therefore makes
    /// <see cref="Stop"/>'s flush wait terminate.
    /// </para>
    /// </remarks>
    internal void SubmitQueued(
        in SerializedProducerRecord record,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery,
        CancellationToken cancellationToken)
    {
        bool sealedForTeardown;
        lock (_gate)
        {
            sealedForTeardown = _queueSealed;
            if (!sealedForTeardown)
            {
                Interlocked.Increment(ref _queued);
                _submissions.Enqueue(
                    new QueuedSubmission(record, completion, delivery, cancellationToken));
            }
        }

        if (sealedForTeardown)
        {
            // Settled outside the lock: the accumulator's one call-out into user code runs from a
            // completion, and nothing that can run user code belongs under _gate.
            completion.TrySetException(ClosedDuringBackpressure());
            return;
        }

        EnsureSubmitterRunning();
    }

    /// <summary>
    /// Starts the submitter loop if one is not already running. The 0 → 1 transition is the
    /// exclusive token, so at most one loop exists per accumulator (see <see cref="SubmitQueued"/>).
    /// </summary>
    private void EnsureSubmitterRunning()
    {
        if (Interlocked.CompareExchange(ref _submitterRunning, 1, 0) == 0)
        {
            _ = Task.Run(RunSubmitterAsync);
        }
    }

    /// <summary>
    /// The single submitter: dequeues one submission at a time, waits for a backpressure permit,
    /// and appends — so queued submissions reach <see cref="Append"/> in enqueue order.
    /// </summary>
    /// <remarks>
    /// <b>What the queued route actually saves, and what it does not (DoD §10).</b> Only
    /// <em>this loop's own</em> state machine is amortised across a burst — the per-submission body
    /// is its own <c>async</c> helper (<see cref="AppendQueuedAsync"/>, awaited once per
    /// submission), and in the regime the queued route exists for — the bound saturated — that
    /// helper and the <see cref="WaitForSpaceAsync"/> inside it both suspend, so both box. Per
    /// saturated submission the cost is therefore those two state machines plus one value-type
    /// queue entry amortised over a <see cref="ConcurrentQueue{T}"/> segment. The saving is on the
    /// <em>caller-facing</em> side, where the pre-fix slow path had a per-send
    /// <c>async Task&lt;RecordMetadata&gt;</c> carrier of its own whose <see cref="Task"/> was what
    /// the caller awaited, and which ended in <c>return await completion.Task</c> — a second
    /// <see cref="Task"/> and a continuation chained onto the awaiter. Both are gone: the caller
    /// gets <c>completion.Task</c> itself on both routes.
    /// <para>
    /// It never throws: every failure is routed to that submission's own awaiter, because this is a
    /// fire-and-forget task and an escaping exception would both go unobserved and kill the loop,
    /// stranding every submission behind it.
    /// </para>
    /// </remarks>
    private async Task RunSubmitterAsync()
    {
        while (true)
        {
            while (_submissions.TryDequeue(out QueuedSubmission submission))
            {
                try
                {
                    await AppendQueuedAsync(submission).ConfigureAwait(false);
                }
                finally
                {
                    // This submission is no longer queued-or-appending. Under _gate, because the
                    // idle predicate reads the count and the drain waiters must be released the
                    // moment the second stage empties.
                    ReleaseQueuedSlot();
                }
            }

            // Interlocked.Exchange, NOT Volatile.Write — the release needs a store→load fence.
            // This release plus the IsEmpty read below are one side of Dekker's pattern against
            // SubmitQueued's enqueue-then-CAS; a Volatile.Write is a RELEASE store, which orders
            // earlier writes against the store and says NOTHING about a later load. On a target
            // that buffers stores (x86-64 does) "the submitter read the queue empty" and "the
            // producer read the token still taken" could then both hold, and the submission would
            // sit queued with no submitter — a strand that keeps _queued >= 1, so the idle
            // predicate never holds, DrainPending waits out its whole bound, and the inline fast
            // path stays refused until some later queued send restarts the loop. The `||` below
            // short-circuits past the CAS on exactly the path where it matters, so the CAS cannot
            // supply the missing barrier: it has to be on the store.
            Interlocked.Exchange(ref _submitterRunning, 0);

            // A submission enqueued between the failed dequeue and that release would have found
            // the token taken and started no loop, so it would sit there with nothing to append it.
            // Re-check, and take the token back if it is still free. With the fence above this is
            // sound by contract rather than by timing: SubmitQueued's CompareExchange is itself a
            // full barrier placed after its Enqueue, so if it read a stale 1 then its enqueue is
            // ordered before this read and the queue is observed non-empty here.
            if (_submissions.IsEmpty
                || Interlocked.CompareExchange(ref _submitterRunning, 1, 0) != 0)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Waits for a permit and appends one queued submission, settling its awaiter on every failure
    /// path so no <see cref="Task"/> is left pending.
    /// </summary>
    /// <remarks>
    /// <b>Cancellation must not append</b> (M11/P3.2 §3.4 item 4 — today's
    /// <c>SendWhenSpaceAvailable</c> behaviour, preserved). The caller's token both aborts the wait
    /// (so a cancelled submission stops holding up the ones behind it) and is re-checked with the
    /// permit in hand, because <see cref="SemaphoreSlim"/> resolves a cancel racing a
    /// <see cref="SemaphoreSlim.Release(int)"/> either way — a wait can therefore return
    /// successfully for a submission whose token has already fired, and that record must not reach
    /// the core. Cancellation carries the <em>caller's</em> token, so the idiomatic
    /// <c>catch (OperationCanceledException e) when (e.CancellationToken == ct)</c> matches.
    /// <para>
    /// <b>Teardown appends instead of faulting</b> (M11/P3.2 §F2 / slice S2). Once
    /// <see cref="Stop"/> has sealed the queue, this submission is appended <b>bypassing the
    /// bound</b> — no permit is taken and none is owed back — rather than faulted, because the
    /// anchor appends before it waits and so a record whose <c>Send</c> already returned is
    /// <em>already accumulated</em> when its close arrives: <c>py_Producer_send</c> writes the slot
    /// unconditionally (<c>_confluentkafka.c:819-822</c>) and only then computes <c>full</c>
    /// (<c>:830</c>), <c>py_Producer_shutdown</c> sets <c>closed</c>, signals the send thread and
    /// joins it (<c>:962-969</c>), and <c>py_Producer_on_space_available</c> reports "available" the
    /// moment <c>closed</c> is set (<c>:857-861</c>) so a waiter never parks through a close. The
    /// send thread's final iteration then drains and sends the record. ⚠ Python's guarantee is
    /// <em>almost</em> unconditional rather than unconditional: <c>Producer_send_thread</c> tests
    /// <c>!closed</c> at <c>:529</c> <b>outside</b> <c>record_batches_mutex</c>, so a record
    /// appended after the thread's last <c>mtx_unlock</c> of that mutex (<c>:577</c>, or
    /// <c>:556</c>/<c>:563</c> on the two early-continues) and before that re-test is never taken,
    /// never sent and never completed. Why the common path is nonetheless safe is a <b>timing</b>
    /// argument, not a structural one: the thread spends almost all of its time parked in
    /// <c>cnd_timedwait</c> (<c>:548</c>), which <b>atomically releases</b>
    /// <c>record_batches_mutex</c> for the duration of the wait and reacquires it on wake — and
    /// that release is precisely how <c>py_Producer_shutdown</c> takes the lock at <c>:961</c> to
    /// set <c>closed</c> at <c>:962</c>. Shutdown winning the lock <em>there</em> is the safe case:
    /// the thread wakes holding the mutex, falls out of the inner wait (<c>:539</c>'s
    /// <c>!closed</c> guard) and performs one final take-and-send. It is <b>not</b> the only
    /// window, though — the thread also holds no <c>record_batches_mutex</c> from <c>:577</c>
    /// through the whole send loop (<c>:581-638</c>, every <c>send_batch</c> call plus its GIL
    /// acquisition) to the next <c>:533</c>, and a close landing <em>there</em> skips the final
    /// take entirely. For a full <c>PRODUCER_RECORD_SLOT_CAPACITY</c> batch that window lasts as
    /// long as a <c>send_batch</c> call, so Python's gap is materially wider than one narrow race.
    /// That <b>strengthens</b> this slice rather than weakening it: .NET completes such a record on
    /// every path, Python only when close lands while its send thread is parked. So this is
    /// Python-aligned <b>and</b> closes a window Python leaves open — not a claim that Python is
    /// airtight. (⚠ Corrected twice, and both errors reached this comment. It first said the
    /// thread parks <em>holding</em> the mutex, which inverts <c>cnd_timedwait</c> and makes its
    /// own conclusion unreachable — shutdown could then never have set <c>closed</c> at all
    /// (Critic 71 finding 71.10). The correction then claimed the thread holds the mutex whenever
    /// it is not parked, hence that the park was the only window shutdown could win the lock —
    /// also false, per the three unlock sites and the unlocked send loop above; it also mispaired
    /// the cites, <c>:638</c> being a <c>pending_batches_mutex</c> unlock rather than a
    /// <c>record_batches_mutex</c> one (finding 71.11). The surviving argument is <b>timing</b>,
    /// never structure. M11/P3.2 PLAN §1.3.)
    /// </para>
    /// <para>
    /// Two things the bypass does <b>not</b> change. It ignores the <em>bound</em>, never
    /// <c>_closed</c>: <see cref="Append"/>'s closed check still refuses, which is what keeps a
    /// late append from being stranded in a chain whose batch thread has already taken its final
    /// one (and is the degraded outcome when the flush wait expires). And a submission whose
    /// caller token has already fired still cancels and is still <b>not</b> appended — today's
    /// semantics for a cancelled parked send (§3.4 item 4), unchanged.
    /// </para>
    /// </remarks>
    private async Task AppendQueuedAsync(QueuedSubmission submission)
    {
        CancellationToken cancellationToken = submission.CancellationToken;
        try
        {
            bool holdsPermit;
            if (Volatile.Read(ref _queueSealed))
            {
                // Sealed before this submission was even dequeued: skip the wait entirely. The
                // gate is already cancelled at this point, so waiting could only throw.
                holdsPermit = false;
            }
            else
            {
                try
                {
                    await WaitForSpaceAsync(cancellationToken).ConfigureAwait(false);
                    holdsPermit = true;
                }
                catch (ObjectDisposedException) when (Volatile.Read(ref _queueSealed))
                {
                    // The common teardown shape: this submission was PARKED on the gate and Stop
                    // cancelled it. Stop sets the seal before cancelling, so the flag write is
                    // ordered before the cancellation that produced this throw and this re-read
                    // observes it.
                    holdsPermit = false;
                }
            }

            if (cancellationToken.IsCancellationRequested)
            {
                // The permit is in hand but this record must not be appended: give it back, or the
                // bound leaks one slot per cancelled submission.
                //
                // ⚠ This branch is DEFENSIVE and is deliberately not claimed as test-covered.
                // SemaphoreSlim.WaitAsync checks the token before granting a permit, so the
                // deterministically-reachable cancellations are caught by the `catch` below, not
                // here; what reaches here is only a cancel that raced a concurrent
                // SemaphoreSlim.Release, which the primitive may resolve either way and which no
                // broker-free test can schedule. Removing the whole cancellation handling IS
                // covered (the record then reaches the core); removing only this branch is not.
                //
                // Nothing to give back on the teardown bypass — it took no permit.
                if (holdsPermit)
                {
                    ReleaseSpace(1);
                }

                submission.Completion.TrySetCanceled(cancellationToken);
                return;
            }

            SubmitCore(
                submission.Record, submission.Completion, submission.Delivery, holdsPermit);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            submission.Completion.TrySetCanceled(cancellationToken);
        }
        catch (Exception exception)
        {
            // Teardown cancelled the backpressure gate before the queue was sealed, or the
            // accumulator refused the append (`_closed` — including the teardown bypass arriving
            // after Stop set it, which is the flush's defined degraded outcome). Nothing reached the
            // core either way, so no delivery callback fires (M11/P3.1 D5) — the same outcome, and
            // the same exception, the pre-queue slow path produced.
            submission.Completion.TrySetException(exception);
        }
    }

    /// <summary>
    /// Accounts one settled submission out of the queued-or-appending count, and releases the drain
    /// waiters if that emptied the accumulator's second stage.
    /// </summary>
    private void ReleaseQueuedSlot()
    {
        lock (_gate)
        {
            Interlocked.Decrement(ref _queued);

            // Wakes DrainPending, which waits on _gate while only queued submissions remain.
            Monitor.PulseAll(_gate);
            SignalIdleLocked();
        }
    }

    /// <summary>
    /// Settles every submission still queued for capacity, faulting each exactly once. Called by
    /// <see cref="Stop"/> after <c>_closed</c> is set — where it is the <b>terminal sweep</b> that
    /// catches whatever the flush could not append — and by the batch thread's failure handler.
    /// </summary>
    /// <remarks>
    /// <b>Exactly once, even against a live submitter.</b> Both this and
    /// <see cref="RunSubmitterAsync"/> dequeue from the same <see cref="ConcurrentQueue{T}"/>, so
    /// each submission is taken by exactly one of them; whichever wins settles it with the same
    /// outcome (this method's fault, or the submitter's — after <c>_closed</c> the bypass append is
    /// refused and <see cref="AppendQueuedAsync"/> faults it with the identical exception). The
    /// count is decremented by the taker, so <see cref="ReleaseQueuedSlot"/> runs here too.
    /// <para>
    /// <b>Anything enqueued after this ran still settles</b>, without a second sweep — but the
    /// reason differs by caller, so it is stated per caller rather than once.
    /// <list type="bullet">
    /// <item>From <see cref="Stop"/>: the queue is sealed, so <see cref="SubmitQueued"/> refuses a
    /// late submission outright and settles it there; and anything already dequeued by the
    /// submitter is refused by <c>_closed</c> inside <see cref="Append"/>.</item>
    /// <item>From <see cref="AbandonOnThreadFailure"/>: that path does not <em>seal</em> the queue,
    /// so the first clause above does not hold there and the <c>_closed</c> refusal carries a late
    /// submission. But it does now <b>cancel the gate, unconditionally</b> (M11/P3.2 slice S5), so
    /// the settlement no longer depends on a permit becoming free — which matters because the
    /// failure that path handles can be the permit accounting itself breaking. See the comment at
    /// that call site.</item>
    /// </list>
    /// This sweep is what makes the queue <em>empty when <see cref="Stop"/> returns</em> rather
    /// than eventually.
    /// </para>
    /// <para>
    /// <b>Faulting is no longer the normal teardown outcome</b> (M11/P3.2 §F2 / slice S2).
    /// <see cref="Stop"/> now flushes the queue into the node chain first, so a send whose caller
    /// already returned is <em>sent</em>, as the anchor sends it. What still reaches this sweep is
    /// the remainder: whatever the flush could not append before its share of the teardown bound
    /// expired, and whatever the batch thread's failure handler was holding. Nothing reached the
    /// core on either of those, so no delivery callback fires (M11/P3.1 D5).
    /// </para>
    /// </remarks>
    private void SettleQueuedSubmissions()
    {
        while (_submissions.TryDequeue(out QueuedSubmission submission))
        {
            try
            {
                submission.Completion.TrySetException(ClosedDuringBackpressure());
            }
            finally
            {
                ReleaseQueuedSlot();
            }
        }
    }

    /// <summary>
    /// Pins the record's buffers and appends it, taking ownership of the permit the caller acquired.
    /// The one primitive both the production send path and the tests use, so a fixture cannot
    /// diverge from what production does (DoD §12).
    /// </summary>
    /// <remarks>
    /// <b>The permit is acquired BEFORE the pins, never after</b> (§4.4): a sender blocked on
    /// backpressure while holding pins would turn a bound on <em>records</em> into an unbounded pin
    /// window. On any failure this releases both the pins and the permit, so a rejected submit
    /// leaves no trace.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The producer is closing — nothing was appended.</exception>
    internal void Submit(
        in SerializedProducerRecord record,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery) =>
        SubmitCore(record, completion, delivery, holdsPermit: true);

    /// <summary>
    /// <see cref="Submit"/>'s body, parameterised by whether the caller is holding a backpressure
    /// permit for this record.
    /// </summary>
    /// <remarks>
    /// <b><paramref name="holdsPermit"/> is <see langword="false"/> only on the teardown bypass</b>
    /// (M11/P3.2 §F2 — <see cref="AppendQueuedAsync"/> under a sealed queue), and it governs two
    /// things that must move together or the bound's accounting breaks:
    /// <list type="bullet">
    /// <item>a refused submit gives a permit back only if one was taken — otherwise the release
    /// would <em>create</em> a permit that no acquire matched;</item>
    /// <item><see cref="Append"/> charges the record against <c>_accumulated</c> only if it is
    /// permit-backed. <c>_accumulated</c> exists solely to tell the batch thread how many permits
    /// to hand back when it takes the chain (<see cref="TakeChainLocked"/>), so counting a bypassed
    /// record there would over-release the <see cref="SemaphoreSlim"/> —
    /// a <see cref="SemaphoreFullException"/> on the batch thread, i.e. the very failure
    /// <see cref="AbandonOnThreadFailure"/> exists to survive, triggered by teardown itself.</item>
    /// </list>
    /// </remarks>
    private void SubmitCore(
        in SerializedProducerRecord record,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery,
        bool holdsPermit)
    {
        MemoryHandle keyPin = default;
        MemoryHandle valuePin = default;
        PinnedTopicCache.TopicPin topicPin = default;
        bool appended = false;
        try
        {
            // key / value -> ReadOnlyMemory<byte>.Pin() (NOT GCHandle.Alloc, which cannot pin a
            // ReadOnlyMemory); PinIfNeeded also decides that an ABSENT or EMPTY buffer needs no pin,
            // the empty case using the marshaller's process-wide static sentinel (§4.2). topic -> one
            // permanently-pinned buffer per DISTINCT topic from the interning cache (§4.1).
            keyPin = ProducerSendBatchMarshal.PinIfNeeded(record.Key);
            valuePin = ProducerSendBatchMarshal.PinIfNeeded(record.Value);
            topicPin = _topics.Rent(record.Topic);

            Append(record, topicPin, keyPin, valuePin, completion, delivery, holdsPermit);
            appended = true;
        }
        finally
        {
            if (!appended)
            {
                // Nothing was stored, so this frame is still the sole owner of all three pins — and
                // of the permit, which must go back or the bound leaks one slot per rejected submit.
                topicPin.Release();
                valuePin.Dispose();
                keyPin.Dispose();
                if (holdsPermit)
                {
                    ReleaseSpace(1);
                }
            }
        }
    }

    /// <summary>
    /// Appends one already-pinned record. On return the accumulator owns
    /// <paramref name="topicPin"/> / <paramref name="keyPin"/> / <paramref name="valuePin"/> and
    /// will release each exactly once after the record's <c>send_batch</c>; if this throws, it owns
    /// none of them and the caller must release them.
    /// </summary>
    /// <param name="record">The record whose blittable slot is filled here.</param>
    /// <param name="topicPin">The interned topic pin ownership transfers with.</param>
    /// <param name="keyPin">The key buffer's pin ownership transfers with.</param>
    /// <param name="valuePin">The value buffer's pin ownership transfers with.</param>
    /// <param name="completion">The record's awaiter.</param>
    /// <param name="delivery">The record's delivery-callback carrier, or <see langword="null"/>.</param>
    /// <param name="chargedToBound">
    /// Whether this record holds a backpressure permit, and so must be counted in
    /// <c>_accumulated</c> for the batch thread to release. <see langword="false"/> only for the
    /// teardown bypass — see <see cref="SubmitCore"/>.
    /// </param>
    /// <exception cref="ObjectDisposedException">The producer is closing — nothing was appended.</exception>
    private void Append(
        in SerializedProducerRecord record,
        PinnedTopicCache.TopicPin topicPin,
        MemoryHandle keyPin,
        MemoryHandle valuePin,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery,
        bool chargedToBound)
    {
        lock (_gate)
        {
            if (_closed)
            {
                // Teardown already closed the accumulator. Nothing is stored, so the caller still
                // owns the pins — and the record never reached the core, so throwing here is the
                // same "nothing was sent" outcome the disposed guard above this layer produces.
                throw new ObjectDisposedException(nameof(NativeProducer));
            }

            Node? tail = _tail;
            if (tail is null || tail.Count == _settings.SlotCapacity)
            {
                // A new node only when the current one is full — the anchor's rule (:806). Allocate
                // BEFORE mutating anything, so an allocation failure leaves the chain untouched and
                // the caller's pins un-transferred. A recycled node costs nothing at all, which is
                // what keeps the steady-state send path free of node allocations (see _spare).
                Node fresh = TakeSpareLocked() ?? new Node(_settings.SlotCapacity);
                if (tail is null)
                {
                    _head = fresh;
                }
                else
                {
                    tail.Next = fresh;
                }

                _tail = fresh;
                tail = fresh;
            }

            // Grow the node's parallel arrays if this record needs a new slot. Also allocation
            // BEFORE any mutation: a failure here leaves Count untouched, so the caller still owns
            // its pins and the chain is unchanged.
            tail.EnsureSlot();

            int slot = tail.Count;
            // Marshal the record into its blittable slot HERE, on the caller's thread, rather than
            // keeping the SerializedProducerRecord until the drain. Two reasons, and neither changes
            // behavior: the node then needs no record array at all (its widest slot, ~80 B of the
            // ~232 B a stored record cost), and the batch thread's critical section shrinks to
            // "call send_batch" — a per-record marshal on the drain would be work the caller could
            // have done in parallel. Fill only records POINTERS and LENGTHS; the core still reads the
            // bytes at drain time, so the §4.7 mutation window is exactly as documented.
            ProducerSendBatchMarshal.Fill(
                ref tail.Natives[slot], record, topicPin.Pointer, keyPin, valuePin);

            tail.TopicPins[slot] = topicPin;
            tail.KeyPins[slot] = keyPin;
            tail.ValuePins[slot] = valuePin;
            tail.Completions[slot] = completion;
            tail.Deliveries[slot] = delivery;
            tail.Count = slot + 1;

            // Permit accounting only — see the chargedToBound parameter. A teardown-bypassed
            // record is in the chain and will be sent, but it never took a permit, so counting it
            // here would make TakeChainLocked over-release the bound.
            if (chargedToBound)
            {
                _accumulated++;
            }

            // Early wake at the threshold (anchor :825-827). Below it the batch thread is purely
            // timer-driven, which is what makes the sub-threshold delay 0..window uniform.
            if (tail.Count >= _settings.SlotThreshold)
            {
                Monitor.PulseAll(_gate);
            }
        }
    }

    /// <summary>
    /// Blocks until every send that had <b>returned</b> before this call has been handed to
    /// <c>send_batch</c> and its future enqueued to the completion pump — i.e. the accumulator is
    /// empty and idle in <see cref="IsEmptyAndIdleLocked"/>'s two-stage sense. Wakes the batch
    /// thread immediately rather than waiting out its window.
    /// </summary>
    /// <remarks>
    /// This is the primitive behind two things: the test-only "drain now and wait" hook that
    /// replaces a <c>Thread.Sleep</c> in the mock manual-completion tests (§9), and — from slice S6 —
    /// <c>Flush</c>'s accumulator drain, which is the deliberate divergence <em>toward</em> Java that
    /// the anchor lacks (§3.5: Python's <c>flush()</c> never signals its send thread, so it can
    /// return with records still buffered in the binding).
    /// </remarks>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <returns><see langword="true"/> if the accumulator reached empty-and-idle in time.</returns>
    internal bool DrainPending(TimeSpan timeout)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * timeout.TotalSeconds);

        lock (_gate)
        {
            while (!IsEmptyAndIdleLocked())
            {
                if (_head is not null || _draining)
                {
                    // Re-arm the force flag on EVERY iteration, not once before the loop. The batch
                    // thread consumes it when it takes a chain, and a record can land after that — a
                    // backpressure waiter released by the very drain this forced is the ordinary case
                    // — so a single arming would leave the newcomer waiting out the full window while
                    // this caller waits out its whole timeout. "Drain until empty AND idle" is the
                    // contract; one drain is not it.
                    _forceDrain = true;
                    Monitor.PulseAll(_gate);
                }

                // Otherwise only STAGE TWO is outstanding — submissions waiting for a permit. There
                // is nothing for the batch thread to take, so forcing another drain would just
                // ping-pong the two threads; the wake comes from ReleaseQueuedSlot's pulse as each
                // submission appends or settles, and the next iteration then forces the drain that
                // its append made necessary.
                int remainingMs = RemainingMilliseconds(deadline);
                if (remainingMs <= 0)
                {
                    return false;
                }

                Monitor.Wait(_gate, remainingMs);
            }

            return true;
        }
    }

    /// <summary>
    /// The awaitable form of <see cref="DrainPending"/>: completes once every record appended before
    /// the call has been handed to <c>send_batch</c> and its future enqueued to the completion pump.
    /// Used by <c>Flush</c>, which must not block its caller's thread.
    /// </summary>
    /// <remarks>
    /// <b>This is the deliberate divergence TOWARD Java that the anchor lacks (§3.5).</b> Python's
    /// <c>flush()</c> never signals its send thread — <c>record_batches_new_record_cnd</c> has three
    /// signal sites and none is a flush entry point — so it can return with records still buffered
    /// inside the binding, although from the caller's view those records <em>were</em> sent, because
    /// <c>send()</c> returned. Java's contract is that <c>flush()</c> blocks until every previously
    /// sent record completes, so the .NET flush drains first.
    /// </remarks>
    /// <param name="cancellationToken">Cancels this caller's wait only; the drain itself continues.</param>
    internal Task DrainPendingAsync(CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> waiter;
        lock (_gate)
        {
            if (IsEmptyAndIdleLocked())
            {
                return s_alreadyDrained;
            }

            // RunContinuationsAsynchronously so completing this under the accumulator's lock cannot
            // run a continuation inline on the batch thread.
            waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _idleWaiters.Add(waiter);
            _forceDrain = true;
            Monitor.PulseAll(_gate);
        }

        if (!cancellationToken.CanBeCanceled)
        {
            return waiter.Task;
        }

        // Per-waiter cancellation: a cancelled waiter simply stops observing, and the drain it asked
        // for carries on for everyone else. Its TCS stays in the list and the idle signal's
        // TrySetResult on it is a harmless no-op.
        CancellationTokenRegistration registration = cancellationToken.Register(
            static state =>
            {
                (TaskCompletionSource<bool> tcs, CancellationToken token) =
                    ((TaskCompletionSource<bool>, CancellationToken))state!;
                tcs.TrySetCanceled(token);
            },
            (waiter, cancellationToken));

        waiter.Task.ContinueWith(
            static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
            registration,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return waiter.Task;
    }

    /// <summary>
    /// "Empty and idle", in the <b>two stages</b> a submission passes through: no submission is
    /// queued-or-appending (stage one — <see cref="SubmitQueued"/>), and the node chain is empty
    /// with no drain in flight (stage two). Caller holds <see cref="_gate"/>.
    /// </summary>
    /// <remarks>
    /// <b>Stage one is what the M11/P3.2 §F1 fix added, and omitting it silently re-opens the gap
    /// M11/P3.1 §3.5 closed on purpose.</b> A record whose <c>Send</c> has already <em>returned</em>
    /// can now be sitting in the submission queue while the chain is empty, so a predicate that
    /// looked only at the chain would let <c>Flush</c> return without it — flush() reporting
    /// completion for records the caller believes were sent and that have not reached the core,
    /// which is precisely the divergence <em>toward</em> Java that the accumulator drain exists to
    /// remove.
    /// </remarks>
    private bool IsEmptyAndIdleLocked() =>
        _head is null && !_draining && Volatile.Read(ref _queued) == 0;

    /// <summary>
    /// Releases every <see cref="DrainPendingAsync"/> waiter if the accumulator is now empty and
    /// idle, and re-arms the force flag if it is not. Caller holds <see cref="_gate"/>.
    /// </summary>
    private void SignalIdleLocked()
    {
        if (_idleWaiters.Count == 0)
        {
            return;
        }

        if (!IsEmptyAndIdleLocked())
        {
            if (_head is not null || _draining)
            {
                // Not idle yet — a record landed after the drain that was forced for these waiters
                // took its chain. Re-arm rather than let them wait out the window (the same reason
                // DrainPending re-arms on every iteration).
                _forceDrain = true;
                Monitor.PulseAll(_gate);
            }

            // With only stage one outstanding there is nothing to force: the next
            // ReleaseQueuedSlot re-enters here, and it re-arms then if that submission appended.
            return;
        }

        foreach (TaskCompletionSource<bool> waiter in _idleWaiters)
        {
            waiter.TrySetResult(true);
        }

        _idleWaiters.Clear();
    }

    /// <summary>
    /// Teardown steps 2–4 of the §3.8 handshake: seal the submission queue and cancel the
    /// backpressure gate, <b>flush every queued submission into the node chain</b>, close the
    /// accumulator to new appends, settle whatever the flush could not take, wake the batch thread,
    /// and wait — <b>bounded</b> — for it to finish its final drain and exit. Idempotent.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The four steps, and why they are in this order</b> (M11/P3.2 §F2(c)). This sits inside
    /// §3.8's handshake, between its step 2 and step 3, so the accumulator drain still completes
    /// <em>before</em> <see cref="SendCompletionPump.CloseGate"/> — see
    /// <c>NativeProducer.StopAccumulator</c>'s remarks for why that ordering is load-bearing; the
    /// flush added below happens strictly inside this method and so cannot move it.
    /// <list type="number">
    /// <item><b>Seal the queue</b> (<see cref="_queueSealed"/>) and cancel the backpressure gate.
    /// The seal stops new submissions, which is what makes step 2's wait monotone; the cancel
    /// releases whichever submission is parked on a permit so it can take the bypass. The seal is
    /// set <em>before</em> the cancel, so the waiter it wakes observes it.</item>
    /// <item><b>Flush the queue into the chain</b>, bypassing the bound — the submitter appends
    /// each queued submission with no permit (<see cref="AppendQueuedAsync"/>), and this waits for
    /// <c>_queued</c> to reach zero. The <em>submitter</em> does the appending, not this thread:
    /// keeping the single-appender invariant is what preserves S1's call-order property through
    /// teardown, and it is also the only way the submission already dequeued and parked on the gate
    /// is included. ⚠ That call-order half is asserted by
    /// <c>Close_FlushesQueuedSubmissionsToSendBatchInCallOrder</c> — it was claimed here and by no
    /// test until Critic 71 finding 71.8, and a submitter mutated to dequeue LIFO once the queue is
    /// sealed left the whole suite green.</item>
    /// <item><b><c>_closed = true</c> + pulse.</b> Strictly after the flush, because <c>_closed</c>
    /// is what tells the batch thread the chain it takes next is its <em>final</em> one — set it
    /// first and the thread can take an empty chain and exit before the flush's appends land,
    /// stranding them.</item>
    /// <item><b>Bounded join</b> — unchanged, except that it shares one deadline with step 2 (see
    /// below).</item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>Why the wait is bounded, and what expiry means.</b> The batch thread can be stuck for a
    /// long time inside <c>send_batch</c>: that call takes the core's coarse producer mutex and can
    /// block on a full <c>buffer.memory</c> for up to <c>max.block.ms</c> (default 60 s), for a whole
    /// chunk of records rather than one (§3.6). An unbounded join would make <c>Dispose</c> inherit
    /// that, and the §6.3 re-enumeration asks for a defined outcome instead of a hang.
    /// </para>
    /// <para>
    /// <b>The flush and the join share ONE deadline, deliberately.</b> <paramref name="timeout"/>
    /// bounds <see cref="Stop"/> as a whole, not each stage, so adding the flush cannot push
    /// teardown past the bound its caller already accepted. Its own expiry outcome is the
    /// pre-S2 behaviour: <c>_closed</c> is set and <see cref="SettleQueuedSubmissions"/> faults
    /// whatever is left, so the flush degrades to a fault rather than to a hang. In the steady
    /// case the flush costs a single lock acquisition — the wait's predicate is already false —
    /// so the join keeps effectively the whole bound.
    /// </para>
    /// <para>
    /// <b>On expiry this ABANDONS the batch thread rather than tearing its state down, deliberately.</b>
    /// The thread owns the node chain it took, and everything in that chain is either pinned (its
    /// buffers) or in flight (its futures). Faulting those records from here would mean releasing
    /// pins the core may still be reading and writing slots the thread is reading — a use-after-free
    /// and a data race, traded for a marginally earlier <see cref="Task"/> completion. So the defined
    /// outcome is: leave it running, and let the properties that already hold carry it. It is a
    /// background thread; <c>_closed</c> is set, so it drains once more and exits on its own; every
    /// record it holds still settles, only later. It stays memory-safe throughout, because
    /// <c>send_batch</c> takes the <see cref="SafeProducerHandle"/> as a P/Invoke parameter, so the
    /// marshaller's ref keeps <c>Producer_destroy</c> from running underneath an in-flight call and
    /// a handle already closed surfaces as <see cref="ObjectDisposedException"/>, which
    /// <see cref="SendNode"/> turns into a faulted send. A future that reaches the pump after its
    /// gate closed is faulted and freed in place — recorded residual 1, which this is the path that
    /// makes reachable.
    /// </para>
    /// <para>
    /// The caller uses the return value for exactly one decision: whether it is safe to free the
    /// interned topic buffers eagerly (they are pointed at by whatever the abandoned thread still
    /// holds), which is why <see cref="PinnedTopicCache"/> keeps its finalizer as the fallback.
    /// </para>
    /// </remarks>
    /// <param name="timeout">
    /// How long to wait for the submission-queue flush and the batch thread's final drain,
    /// <b>together</b>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the batch thread finished and exited; <see langword="false"/> if it
    /// was abandoned still running.
    /// </returns>
    internal bool Stop(TimeSpan timeout)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * timeout.TotalSeconds);

        // Step 1. Seal the queue: no new submission joins it, and the ones in it are appended
        // rather than faulted. Read _closed under the SAME acquisition, because an accumulator that
        // is already closed has no chain to flush into — the batch thread's failure handler got
        // there first, or a previous Stop did — and the flush would then just wait for the bypass
        // appends to be refused one by one.
        bool flush;
        lock (_gate)
        {
            _queueSealed = true;
            flush = !_closed;
        }

        // §3.8 step 2 BEFORE step 3: cancel the backpressure gate, so a Send parked on a permit is
        // released rather than holding teardown behind it. It is the one place this design is
        // strictly better than the inline send it replaces, where a caller blocked in the core
        // could not be woken by a concurrent close at all (§2.2 / §4.6). AFTER the seal, so the
        // submission it wakes takes the bypass instead of faulting.
        //
        // ⚠ There are now TWO _spaceGate.Cancel() call sites: this one and the unconditional one in
        // AbandonOnThreadFailure (M11/P3.2 slice S5 / §F5 / decision D5, landed). Until S5 this was
        // the only one, and the reason was SEQUENCING, not safety — the behaviour change was held
        // back so it stayed attributable to its own slice, NOT because cancelling there was unsafe.
        //
        // It was not. The bypass is gated on _queueSealed, which ONLY this method writes (in
        // step 1 above, under _gate), so a waiter that handler releases reaches AppendQueuedAsync's
        // `catch (ObjectDisposedException) when (_queueSealed)` filter with the filter FALSE: the
        // throw propagates to the outer catch and that submission is FAULTED, never appended. And
        // where a concurrent Stop HAS sealed, the record still cannot be stranded, because
        // AbandonOnThreadFailure sets _closed and calls TakeChainLocked in ONE _gate acquisition —
        // so the bypass append is either refused by _closed or lands in a node that same acquisition
        // takes, and SettleAbandonedChain settles it.
        //
        // ⚠ This comment previously stated that hazard as real ("it must not start, or a submission
        // it releases would take the bypass into an accumulator whose thread is already gone") — a
        // correctness prohibition against an approved decision, which would have misdirected S5.
        // Critic 71 finding 71.9.
        _spaceGate.Cancel();

        if (flush)
        {
            // Step 2. The submitter appends everything queued, bypassing the bound; wait for it.
            // EnsureSubmitterRunning covers the window where a caller had enqueued but not yet
            // started the loop, so progress does not depend on that caller's next instruction.
            EnsureSubmitterRunning();
            FlushQueuedSubmissions(deadline);
        }

        // Step 3. Only now: the chain the batch thread takes next is its final one.
        lock (_gate)
        {
            _closed = true;
            Monitor.PulseAll(_gate);
        }

        // The terminal sweep, AFTER _closed so nothing it misses can still be appended, and BEFORE
        // the join so Stop returns with the queue empty rather than relying on a thread-pool
        // continuation to get there (M11/P3.2 §3.4 item 3). After a completed flush it is a no-op;
        // it is what makes the flush's expiry a fault rather than a hang.
        SettleQueuedSubmissions();

        // Step 4. The bounded join, on whatever is left of the one shared deadline.
        return _thread.Join(RemainingMilliseconds(deadline));
    }

    /// <summary>
    /// Waits — bounded by <paramref name="deadline"/> — for every queued submission to be appended
    /// (bypassing the bound) or otherwise settled, i.e. for <c>_queued</c> to reach zero.
    /// </summary>
    /// <remarks>
    /// <b>It terminates for two independent reasons</b>, and both are needed. The count is
    /// monotonically decreasing, because <see cref="Stop"/> sealed the queue under
    /// <see cref="_gate"/> before calling this and <see cref="SubmitQueued"/> checks the seal under
    /// the same lock — so a caller still hammering <c>Send</c> cannot keep the predicate false. And
    /// the wait itself is bounded, so even a submitter that never gets scheduled costs at most the
    /// remainder of <see cref="Stop"/>'s own budget.
    /// <para>
    /// The wake comes from <see cref="ReleaseQueuedSlot"/>'s pulse as each submission settles.
    /// <see cref="Monitor.Wait(object, int)"/> releases <see cref="_gate"/> while it waits, so the
    /// submitter can take it for its appends — this cannot deadlock against the submitter or the
    /// batch thread.
    /// </para>
    /// <para>
    /// ⚠ <b>This is a blocking wait on work that runs on the thread pool</b> — the submitter is a
    /// <see cref="Task.Run(Func{Task})"/> loop, not a thread (ffi §A1's two-thread cap) — so on a
    /// saturated pool it can wait without the submitter being scheduled. The <b>bound is what makes
    /// that safe</b>, and is the reason it is not optional here: the worst case degrades to the
    /// pre-flush behaviour (the terminal sweep faults the remainder) rather than to a stalled
    /// teardown. Draining the queue from <em>this</em> thread instead would remove the dependency
    /// but break the single-appender invariant S1's call-order property rests on, and would still
    /// not reach the submission already dequeued and parked on the gate.
    /// </para>
    /// </remarks>
    private void FlushQueuedSubmissions(long deadline)
    {
        lock (_gate)
        {
            while (Volatile.Read(ref _queued) != 0)
            {
                int remainingMs = RemainingMilliseconds(deadline);
                if (remainingMs <= 0)
                {
                    return;
                }

                Monitor.Wait(_gate, remainingMs);
            }
        }
    }

    private void RunLoop()
    {
        try
        {
            RunLoopCore();
        }
        catch (Exception exception)
        {
            // The batch thread must not die silently: everything it holds would then hang forever,
            // and appends would keep accumulating into a thread that will never drain them. Close
            // the accumulator so further appends are refused, and settle what is in hand — we are on
            // the batch thread, so this is the sole owner and touching the chain is safe here in a
            // way it is not from Stop's expiry path.
            //
            // SendNode already catches per-node failures, so reaching here means something outside
            // that — which is exactly why it is worth handling rather than assuming unreachable.
            AbandonOnThreadFailure(exception);
        }
    }

    /// <summary>
    /// Settles everything the batch thread owns after an unexpected failure of the thread itself,
    /// then lets it exit. Runs ON the batch thread, so it is the sole owner of both chains.
    /// </summary>
    /// <remarks>
    /// <b>Both chains, not just the accumulator's.</b> The failure can land while the thread holds a
    /// chain it has already taken — <see cref="ReleaseSpace"/> over-releasing between the take and
    /// the send, or <see cref="ReleasePins"/> / <see cref="FaultNode"/> throwing inside
    /// <see cref="SendNode"/> — so <see cref="_inFlight"/> is settled first, then whatever is still
    /// in the accumulator. Settling only the latter left every in-flight record stranded forever
    /// (awaiter never completed, pins never released, futures never destroyed) while
    /// <see cref="Stop"/> still reported the thread as exited.
    /// </remarks>
    private void AbandonOnThreadFailure(Exception cause)
    {
        Node? inFlight = _inFlight;
        _inFlight = null;

        Node? chain;
        int freed;
        lock (_gate)
        {
            // Refuse further appends: an accumulator whose thread is gone can only strand them.
            _closed = true;
            chain = TakeChainLocked(out freed);
            _draining = false;
            Monitor.PulseAll(_gate);
            SignalIdleLocked();
        }

        SettleAbandonedChain(inFlight, cause);
        SettleAbandonedChain(chain, cause);

        // The submission queue too: with the batch thread gone, a submission still waiting behind
        // another for a permit has nothing that will ever append it, so faulting it here is what
        // keeps this handler's "settle what is in hand" property true of BOTH stages. A submission
        // already DEQUEUED and parked in WaitForSpaceAsync is not in _submissions and so is not
        // reached here; the unconditional _spaceGate.Cancel() below is what releases that one
        // (M11/P3.2 slice S5). Before S5 it was left to the ReleaseSpace arithmetic — see the
        // comment there for why that could not be relied on.
        //
        // Faulting stays right even when this races Stop's teardown flush: _closed is set above,
        // so a bypass append (M11/P3.2 §F2) is refused and AppendQueuedAsync faults the submission
        // with the same exception. Appending here instead would strand the record — the thread that
        // would have drained it is the one that just died.
        SettleQueuedSubmissions();

        // M11/P3.2 slice S5 (§F5 / decision D5). UNCONDITIONAL — and that is the point: a waiter is
        // released by an EXPLICIT event, never by a coincidence of permit accounting.
        //
        // It covers BOTH stages of "waiting for space", which is why it is not redundant with the
        // sweep above. A submission the submitter has already DEQUEUED and parked in
        // WaitForSpaceAsync is no longer in _submissions, so SettleQueuedSubmissions cannot reach it;
        // this is the only thing that does. One still queued behind it is reached by both, harmlessly
        // — TrySetException is idempotent and the two paths produce the same exception.
        //
        // ⚠ WHY NOT THE PERMIT ARITHMETIC. Before S5 the parked case was left to ReleaseSpace(freed)
        // below, on an UNSTATED invariant: a waiter can exist only when the permits are exhausted
        // (_accumulated == bound), so freed > 0 must release at least one and wake it. The failure
        // this handler exists for is an OVER-RELEASE (SemaphoreFullException) — i.e. precisely a case
        // where that accounting is ALREADY known broken — and SemaphoreSlim.Release validates the
        // whole count BEFORE releasing anything, so the throwing call releases NOTHING and the waiter
        // is never woken. Resting a liveness property on arithmetic the handler's own trigger has
        // already corrupted is the hang this line removes.
        //
        // Released waiters fault with ClosedDuringBackpressure, which is the right answer: the
        // accumulator is closed and their record will never be sent.
        //
        // ⚠ SAFE, and established by measurement rather than asserted (Critic 71 finding 71.9). The
        // teardown bypass is gated on _queueSealed, which ONLY Stop writes, so a waiter released from
        // HERE reaches AppendQueuedAsync's `catch (ObjectDisposedException) when (_queueSealed)`
        // filter with the filter FALSE: the throw propagates to the outer catch and that submission
        // is FAULTED, never appended into an accumulator whose thread is gone. Where a concurrent
        // Stop HAS sealed, the record still cannot be stranded, because _closed is set and the chain
        // taken in the ONE _gate acquisition above — so a bypass append is either refused by _closed
        // or lands in a node that same acquisition took, and SettleAbandonedChain settled it.
        //
        // Ordered AFTER the settles, so a pathological throw out of Cancel cannot strand the chains
        // this handler exists to settle; and BEFORE ReleaseSpace, so it owes that call nothing.
        _spaceGate.Cancel();

        try
        {
            ReleaseSpace(freed);
        }
        catch (Exception)
        {
            // Last-resort handler: an over-release is exactly one of the failures that gets us
            // here, and the permit accounting is already unrecoverable at this point. Settling the
            // records is what matters, and it has already happened above — so swallow rather than
            // let this escape and kill the thread with an unhandled exception.
        }
    }

    /// <summary>
    /// Releases the pins and faults the awaiters of every node in <paramref name="chain"/>, node by
    /// node, on the batch thread's failure path.
    /// </summary>
    /// <remarks>
    /// Both halves are idempotent per index — <see cref="ReleasePins"/> resets each slot to its
    /// <c>default</c> and <see cref="FaultNode"/> skips an index whose completion was already
    /// nulled — so a node that <see cref="SendNode"/> had partly settled is finished here rather
    /// than settled twice. The per-node <c>catch</c> is what keeps one failing node from stranding
    /// its successors: this is the handler of last resort, so it must not itself be the thing that
    /// abandons records.
    /// </remarks>
    private static void SettleAbandonedChain(Node? chain, Exception cause)
    {
        while (chain is not null)
        {
            Node node = chain;
            chain = node.Next;
            node.Next = null;

            try
            {
                int count = node.Count;
                ReleasePins(node, count);

                // Where nothing reached the core (both result slots zero) FaultNode fires the
                // delivery callback — §6.2's "correct and complete": a failure notification for an
                // un-accepted record invents nothing and can never duplicate. Where the core DID
                // accept a record, FaultNode destroys its future unread and fires nothing.
                FaultNode(node, settled: 0, count: count, cause: cause);
            }
            catch (Exception)
            {
                // See the remarks: keep going, so one node cannot strand the rest of the chain.
            }
        }
    }

    private void RunLoopCore()
    {
        while (true)
        {
            Node? chain;
            bool stopping;
            int freed;

            lock (_gate)
            {
                // FREE-RUNNING window (anchor :535): the deadline comes from THIS loop's own clock
                // at the top of the iteration, unrelated to when any record arrived. That is what
                // makes a sub-threshold batch wait 0..window UNIFORMLY rather than a fixed window —
                // an accepted cost of Python parity (§3.3), not a bug to "fix" by restarting the
                // timer on the first append.
                long deadline = Stopwatch.GetTimestamp() + _windowTicks;

                while (!_closed
                    && !_forceDrain
                    && (_head is null || _head.Count < _settings.SlotThreshold))
                {
                    int remainingMs = RemainingMilliseconds(deadline);
                    if (remainingMs <= 0)
                    {
                        break;
                    }

                    Monitor.Wait(_gate, remainingMs);
                }

                _forceDrain = false;
                stopping = _closed;
                chain = TakeChainLocked(out freed);
                if (chain is not null)
                {
                    _draining = true;

                    // Publish the taken chain BEFORE leaving the lock, and so before anything that
                    // can throw between the take and the send (ReleaseSpace below is the first).
                    // Until this assignment the local `chain` is the only reference to it, and a
                    // throw there would strand every record it holds — see _inFlight.
                    _inFlight = chain;
                }
                else
                {
                    // Idle: release any drain waiter (and, when closing, do it before exiting).
                    Monitor.PulseAll(_gate);
                    SignalIdleLocked();
                }
            }

            // Capacity is free again — release OUTSIDE the lock, as the anchor fires its space
            // callbacks after unlocking (:581). Releasing under the lock could run a waiter's
            // continuation inline on this thread, and that continuation appends.
            ReleaseSpace(freed);

            if (chain is not null)
            {
                try
                {
                    SendChain();
                }
                finally
                {
                    lock (_gate)
                    {
                        _draining = false;
                        Monitor.PulseAll(_gate);
                        SignalIdleLocked();
                    }
                }
            }

            if (stopping)
            {
                // The chain taken above was final: _closed was set under the same lock that
                // Append checks, so nothing can have been appended after it.
                break;
            }
        }
    }

    /// <summary>
    /// Takes the whole chain and resets the accumulation (anchor :562-579), reporting how many
    /// backpressure permits the caller must release once it has left the lock.
    /// </summary>
    private Node? TakeChainLocked(out int freed)
    {
        Node? chain = _head;
        _head = null;
        _tail = null;

        freed = _accumulated;
        _accumulated = 0;
        return chain;
    }

    /// <summary>
    /// Returns <paramref name="count"/> backpressure permits. Never called with the accumulator's
    /// lock held (see <see cref="RunLoop"/>), and a no-op for zero because
    /// <see cref="SemaphoreSlim.Release(int)"/> rejects a zero count.
    /// </summary>
    private void ReleaseSpace(int count)
    {
        if (count > 0)
        {
            _space.Release(count);
        }
    }

    private static ObjectDisposedException ClosedDuringBackpressure() =>
        new ObjectDisposedException(
            nameof(NativeProducer),
            "The producer was closed while this send was waiting for accumulator space.");

    /// <summary>
    /// Sends every node of the chain <see cref="_inFlight"/> names, advancing it as each node is
    /// fully settled so a throw leaves exactly the unfinished remainder published.
    /// </summary>
    private void SendChain()
    {
        // One send_batch per chunk, walking the chain node by node (anchor :593 issues one call per
        // node). Never one call for the whole chain: chunking is ALSO what bounds how long the
        // core's coarse producer mutex is held in a single stretch (§3.4), so collapsing this loop
        // into a single flattened call would silently remove that cap.
        while (_inFlight is not null)
        {
            Node node = _inFlight;

            // SendNode settles (or faults) every slot of this node, and its own catch swallows
            // everything that happens inside it into FaultNode — so the ONLY throw that escapes is
            // FaultNode itself failing. When it does, this node is left partly unsettled and
            // _inFlight must still name IT, which is what lets AbandonOnThreadFailure finish it
            // instead of stranding every record it holds.
            SendNode(node);

            // The advance is pinned between TWO neighbours and BOTH directions are load-bearing:
            //
            //   * AFTER SendNode — hoisting it above the call drops the failing node out of
            //     _inFlight the instant SendNode throws, so the handler of last resort cannot see
            //     it and its remaining records are stranded forever (awaiters never completed,
            //     futures never destroyed). That is the second of 65.3's two triggers, and
            //     BatchThreadFailure_INSIDESendNode_StillSettlesTheRestOfThatNode guards it.
            //   * BEFORE RecycleNode — deferring it past the recycle lets the failure handler
            //     re-enter a node that is already back in the spare slot and re-fillable by Append.
            _inFlight = node.Next;
            node.Next = null;
            RecycleNode(node);
        }
    }

    /// <summary>
    /// Offers a fully-settled node back to <see cref="Append"/> for reuse, keeping <b>one</b> spare.
    /// </summary>
    /// <remarks>
    /// <b>A second deliberate deviation from the anchor, and the one that makes the steady-state
    /// send path allocation-free.</b> The anchor <c>PyMem_RawFree</c>s each node once its completions
    /// are read, and mallocs a fresh one per drain; a node there is five pointer arrays. Here a node
    /// is eight arrays of by-value slots, so allocating one per drain would charge the <em>caller</em>
    /// thread a fresh growth sequence every window — a per-record cost on the hot path that the DoD
    /// §10 audit is precisely about. Reuse is safe because by the time <see cref="SendNode"/> has
    /// returned, every slot of the node is settled: the pins were released, each future was either
    /// destroyed or handed to the pump, and the completion / delivery references were nulled. The
    /// recycle happens on the batch thread under the same lock <see cref="Append"/> takes, so the
    /// node can never be filled while it is still being sent.
    /// <para>
    /// One spare, not a pool: it removes the steady-state allocation without turning node lifetime
    /// into a free-list to reason about. A burst deep enough to need several nodes at once still
    /// allocates the extra ones and lets the GC take them.
    /// </para>
    /// </remarks>
    private void RecycleNode(Node node)
    {
        lock (_gate)
        {
            _spare ??= node;
        }
    }

    /// <summary>Takes the spare node, if any, reset for refilling. Caller holds <see cref="_gate"/>.</summary>
    private Node? TakeSpareLocked()
    {
        Node? spare = _spare;
        if (spare is null)
        {
            return null;
        }

        _spare = null;

        // Only Count and Next need resetting: every other slot is written before it is read —
        // Natives by Fill, the pins / completion / delivery by Append, and Futures / Errors by the
        // Array.Clear in SendNode. ReleasePins already reset the pin slots to `default`, so no stale
        // GCHandle survives into the reused node.
        spare.Count = 0;
        spare.Next = null;
        return spare;
    }

    private void SendNode(Node node)
    {
        int count = node.Count;
        if (count == 0)
        {
            return;
        }

        // How many indices CompleteNode has fully handled. Read by FaultNode so a partial throw
        // faults only the tail — an index already handed to the pump must not be touched again.
        int settled = 0;

        // Read ONCE and shared by the send loop and the completion loop below, so the two provably
        // walk the same chunk boundaries: a completion group IS one send_batch call's records
        // (M11/P3.2 §3B.1), and recomputing the bound twice from the settings would let a future
        // edit silently move one of them.
        int chunk = _settings.BatchChunk;
        try
        {
            try
            {
                // The natives were filled at Append time, on each caller's own thread.
                //
                // The callee writes both slots for every index it processes, but a chunk that never
                // runs (an earlier chunk threw) would leave stale values behind, so start from zero
                // and let "both slots zero" mean "the core never saw this record" (anchor :599-600
                // memsets the same two arrays).
                Array.Clear(node.Futures, 0, count);
                Array.Clear(node.Errors, 0, count);

                for (int offset = 0; offset < count; offset += chunk)
                {
                    int length = Math.Min(chunk, count - offset);
                    _ = ProducerSendBatchMarshal.SendBatch(
                        _handle, node.Natives, offset, length, node.Futures, node.Errors);

                    // Instrumentation only — written by this thread alone (§3.4 is asserted by CALL
                    // COUNT and by the largest call, not by "the records arrived").
                    Interlocked.Increment(ref _sendBatchCalls);
                    Interlocked.Add(ref _sendBatchRecords, length);
                    if (length > _largestSendBatch)
                    {
                        Volatile.Write(ref _largestSendBatch, length);
                    }
                }
            }
            finally
            {
                // UNPIN every buffer in this node, exactly once, on every path (§4.4) — after
                // send_batch returned (the core copied everything synchronously inside it) and
                // before any future reaches the pump. Never in the pump; never across the Task.
                ReleasePins(node, count);
            }

            // ONE COMPLETION GROUP PER send_batch CALL (M11/P3.2 §3B.1) — the same boundaries the
            // send loop above used, walked in the same order. `settled` is a whole-node cursor, so
            // each call resumes where the previous one stopped and FaultNode's "the tail from
            // `settled`" contract is unchanged.
            //
            // Why the unit is the CALL and not the node, even though they coincide at the defaults
            // (chunk == SlotCapacity == node capacity, so this loop runs once and is identical to
            // the anchor's one get_all per BatchNode): a LOWERED CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK
            // splits a node into ceil(count / chunk) calls, and grouping per node would then re-mix
            // several send_batch calls into one get_all — reintroducing under an override exactly the
            // shape grouping exists to remove. Do not "simplify" this back to one call per node.
            for (int offset = 0; offset < count; offset += chunk)
            {
                CompleteNode(node, offset + Math.Min(chunk, count - offset), ref settled);
            }
        }
        catch (Exception exception)
        {
            FaultNode(node, settled, count, exception);
        }
    }

    /// <summary>
    /// Walks the per-record results of <b>one <c>send_batch</c> call</b> — the node's indices from
    /// <paramref name="settled"/> up to <paramref name="end"/>: faults the immediate-error indices
    /// here and hands the rest to the completion pump as a <b>single group</b> — the anchor's
    /// compaction (<c>:600-621</c>), expressed as "settle in place, collect the survivors" rather
    /// than "shift the survivors down", because .NET has no reason to compact arrays it is about to
    /// drop.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is the phase's new delivery-callback firing site (§6.1).</b> An immediate error is a
    /// core completion — the core reported that it would not take this record — so the callback is
    /// owed exactly once here, and that index deliberately does not also reach the pump.
    /// </para>
    /// <para>
    /// <b>The group is built as the walk goes, so compaction costs nothing extra</b> (M11/P3.2
    /// §3B.2): only the accepted indices are written into it, and its <c>count</c> is what the pump
    /// bounds every loop by. The three arrays are sized for the whole call because the accepted
    /// count is not known until the walk ends; they are allocated lazily, so a call the core
    /// rejected outright allocates nothing.
    /// </para>
    /// <para>
    /// ⚠ <b>THE HAND-OVER ORDER IS THE MOST DELICATE THING HERE.</b> See the note at the
    /// <c>Enqueue</c> call below — it is the one that must not be reordered.
    /// </para>
    /// </remarks>
    /// <param name="node">The node being sent.</param>
    /// <param name="end">
    /// One past the last index of this <c>send_batch</c> call — the walk runs from
    /// <paramref name="settled"/> to here.
    /// </param>
    /// <param name="settled">
    /// The whole-node cursor: how many indices have been fully handled. Advanced to
    /// <paramref name="end"/> only once this call's group has been handed over, so
    /// <see cref="FaultNode"/> can still finish everything this call did not.
    /// </param>
    private void CompleteNode(Node node, int end, ref int settled)
    {
        int start = settled;

        // This call's completion group, allocated on the first accepted index. Sized for the whole
        // call; `accepted` is the count that travels with it.
        IntPtr[]? futures = null;
        TaskCompletionSource<RecordMetadata>[]? completions = null;
        DeliveryRegistration?[]? deliveries = null;
        int accepted = 0;

        for (int i = start; i < end; i++)
        {
            IntPtr error = node.Errors[i];
            IntPtr future = node.Futures[i];
            TaskCompletionSource<RecordMetadata> completion = node.Completions[i]!;
            DeliveryRegistration? delivery = node.Deliveries[i];

            if (error != IntPtr.Zero)
            {
                // Per-record rejection. Null both slots FIRST: this frame now owns both handles and
                // frees both below, so a throw from here lands in FaultNode with nothing left to
                // double-free. The ABI writes a null future alongside an error, but free
                // defensively rather than assume — a leaked future is invisible to every managed
                // assertion (ffi §A2).
                node.Errors[i] = IntPtr.Zero;
                node.Futures[i] = IntPtr.Zero;
                if (future != IntPtr.Zero)
                {
                    NativeMethods.FutureRecordMetadataDestroy(future);
                }

                KafkaException failure = KafkaException.FromHandle(error)
                    ?? new KafkaException("The send failed without an error handle.");

                // D3 ordering: the callback runs BEFORE the awaiter is released, and unconditionally
                // (never gated on TrySet*'s bool) — a canceled awaiter still owes its notification.
                delivery?.Fire(null, failure);
                completion.TrySetException(failure);
            }
            else if (future == IntPtr.Zero)
            {
                // Defensive: the ABI writes exactly one of the pair per index, so neither being set
                // is a core contract violation. The record never reached the core, so the callback
                // IS owed here (§6.2 — faulting an un-accepted record is correct and complete).
                KafkaException failure = new KafkaException(
                    "kafka_producer_Producer_send_batch returned a null future without an error.");
                delivery?.Fire(null, failure);
                completion.TrySetException(failure);
            }
            else
            {
                // Accepted: this index joins the group whose ownership transfers to the pump below.
                // Its Futures / Completions / Deliveries slots are deliberately LEFT AS THEY ARE —
                // see the hand-over note at the Enqueue call.
                if (futures is null)
                {
                    int capacity = end - start;
                    futures = new IntPtr[capacity];
                    completions = new TaskCompletionSource<RecordMetadata>[capacity];
                    deliveries = new DeliveryRegistration?[capacity];
                }

                futures[accepted] = future;
                completions![accepted] = completion;
                deliveries![accepted] = delivery;
                accepted++;
                continue;
            }

            // Settled in place: drop the references so a node held anywhere does not retain user
            // objects. A null Completions slot is also how FaultNode recognizes an index it must
            // leave alone, which is what makes a throw later in this walk safe to recover from.
            node.Completions[i] = null;
            node.Deliveries[i] = null;
        }

        if (accepted > 0)
        {
            // ⚠ OWNERSHIP TRANSFERS HERE, AND THE NULLING BELOW MUST STAY BELOW IT.
            //
            // If Enqueue throws (its queue growing under out-of-memory — its own _stopped branch
            // frees the futures and returns normally), ownership never transferred, so FaultNode
            // must still see a live future at each of these indices and free it. Nulling first would
            // make those indices look like "the core never saw this record", and FaultNode would
            // then fire delivery callbacks the pump is also about to fire — DUPLICATES, which the
            // exactly-once obligation makes strictly worse than the drop (root CLAUDE.md §9.5).
            //
            // Unchanged in substance from the pre-grouping form, which nulled one slot after one
            // per-record Enqueue: same invariant, one enqueue per send_batch call instead of one per
            // record.
            _pump.Enqueue(futures!, completions!, deliveries!, accepted);

            // Transferred: release this node's claim on exactly the handed-over indices. A non-zero
            // future in [start, end) identifies them precisely — the settle-in-place branches above
            // zeroed theirs, and SendNode cleared the whole range before send_batch wrote it.
            for (int i = start; i < end; i++)
            {
                if (node.Futures[i] != IntPtr.Zero)
                {
                    node.Futures[i] = IntPtr.Zero;
                    node.Completions[i] = null;
                    node.Deliveries[i] = null;
                }
            }
        }

        // Only now: everything in [start, end) is either settled in place or owned by the pump, so
        // FaultNode has nothing left to do for this call's range.
        settled = end;
    }

    /// <summary>
    /// The no-hang guard for a node whose processing threw: frees whatever native handle each
    /// unsettled index still holds and faults its awaiter, so no <see cref="Task"/> is left pending.
    /// Indices <c>[0, settled)</c> are untouched — they already completed, or the pump owns them.
    /// </summary>
    /// <remarks>
    /// <b>Delivery callbacks fire only where the core never accepted the record</b> — both result
    /// slots zero, i.e. the throw preceded this record's <c>send_batch</c>. That case is §6.2's
    /// "correct and complete": nothing was sent, so a failure notification invents nothing and can
    /// never duplicate. Where the core <em>did</em> accept the record (a live future this method
    /// destroys unread), no callback is fired, for the reason recorded residuals 1–3 give: there is
    /// no core completion in hand and the record may still be delivered, so a fabricated failure
    /// would be an invented one. Reachable only under an unexpected managed or native failure on the
    /// batch thread (out of memory, or a P/Invoke against a torn-down producer).
    /// </remarks>
    private static void FaultNode(Node node, int settled, int count, Exception cause)
    {
        KafkaException failure = cause as KafkaException
            ?? new KafkaException("The producer send-batch thread failed to process a batch.", cause);

        for (int i = settled; i < count; i++)
        {
            IntPtr future = node.Futures[i];
            IntPtr error = node.Errors[i];
            node.Futures[i] = IntPtr.Zero;
            node.Errors[i] = IntPtr.Zero;

            bool acceptedByCore = future != IntPtr.Zero;
            if (acceptedByCore)
            {
                NativeMethods.FutureRecordMetadataDestroy(future);
            }

            if (error != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(error);
            }

            TaskCompletionSource<RecordMetadata>? completion = node.Completions[i];
            if (completion is null)
            {
                continue;
            }

            if (!acceptedByCore && error == IntPtr.Zero)
            {
                node.Deliveries[i]?.Fire(null, failure);
            }

            completion.TrySetException(failure);
            node.Completions[i] = null!;
            node.Deliveries[i] = null;
        }
    }

    /// <summary>
    /// Releases every pin the node owns, exactly once. Each slot is reset to its <c>default</c> as
    /// it goes, so a second call (or a later sweep) cannot double-free — <c>GCHandle.Free</c> throws
    /// on a freed handle, which on the batch thread would be an unhandled exception.
    /// </summary>
    private static void ReleasePins(Node node, int count)
    {
        for (int i = 0; i < count; i++)
        {
            node.TopicPins[i].Release();
            node.TopicPins[i] = default;

            node.ValuePins[i].Dispose();
            node.ValuePins[i] = default;

            node.KeyPins[i].Dispose();
            node.KeyPins[i] = default;
        }
    }

    private static int RemainingMilliseconds(long deadlineTimestamp)
    {
        long remaining = deadlineTimestamp - Stopwatch.GetTimestamp();
        if (remaining <= 0)
        {
            return 0;
        }

        double ms = remaining * 1000.0 / Stopwatch.Frequency;
        return ms >= 1.0 ? (int)Math.Min(ms, int.MaxValue) : 1;
    }

    /// <summary>
    /// One send waiting for backpressure capacity — everything <see cref="Submit"/> needs, held
    /// until a permit is free (M11/P3.2 §3.3). No anchor counterpart: the anchor appends first and
    /// throttles afterwards, in Python, so it never queues a submission (deviation DV-1).
    /// </summary>
    /// <remarks>
    /// <b>A struct, and it carries the record UNPINNED.</b> A struct because
    /// <see cref="ConcurrentQueue{T}"/> stores value types inline in its segments, so a queued send
    /// costs no object of its own — the queued path allocates strictly less than the per-send
    /// <c>async</c> state machine it replaces (DoD §10). Unpinned because pinning belongs after the
    /// permit, inside <see cref="Submit"/>: a pin taken here would be held for the whole wait,
    /// turning a bound on <em>records</em> into an unbounded pin window (M11/P3.1 §4.4).
    /// </remarks>
    private readonly struct QueuedSubmission
    {
        internal QueuedSubmission(
            in SerializedProducerRecord record,
            TaskCompletionSource<RecordMetadata> completion,
            DeliveryRegistration? delivery,
            CancellationToken cancellationToken)
        {
            Record = record;
            Completion = completion;
            Delivery = delivery;
            CancellationToken = cancellationToken;
        }

        internal SerializedProducerRecord Record { get; }

        internal TaskCompletionSource<RecordMetadata> Completion { get; }

        internal DeliveryRegistration? Delivery { get; }

        /// <summary>The <em>caller's</em> token — cancellation is reported with it, not a
        /// substitute, so <c>e.CancellationToken == ct</c> matches at the call site.</summary>
        internal CancellationToken CancellationToken { get; }
    }

    /// <summary>
    /// One accumulator node — the .NET twin of the anchor's <c>BatchNode</c>
    /// (<c>_confluentkafka.c:360-369</c>), holding at most
    /// <see cref="SendAccumulatorSettings.SlotCapacity"/> records, after which
    /// <see cref="SendAccumulator.Append"/> starts a new one (the anchor's rule, <c>:806</c>).
    /// </summary>
    /// <remarks>
    /// <b>Deliberate deviation from the anchor: the arrays GROW to that cap instead of being
    /// allocated at it</b> — every constant is unchanged (a node still holds at most
    /// <c>SLOT_CAPACITY</c> records, and the chunk still equals it), only the allocation strategy
    /// differs. The anchor's arrays are fixed because a C struct has no other option, and its five
    /// slots are <em>pointers</em> — 44 KB per node (5 × 1100 × 8). .NET's eight slots are mostly
    /// by-value: a <see cref="PinnedTopicCache.TopicPin"/> (16 B), two
    /// <see cref="MemoryHandle"/>s (24 B each), a <see cref="ProducerRecordNative"/> (56 B), two
    /// references and two <see cref="IntPtr"/> result slots (8 B each) — <b>~152 B per slot, so
    /// ~167 KB per node, roughly 3.8× the anchor's</b>. (Not ~232 B: that figure counts a
    /// <see cref="SerializedProducerRecord"/> slot this node does <em>not</em> have — §4.3's type
    /// sketch had one and <see cref="Append"/> marshals in its place, which is where its ~80 B
    /// went.) A node is allocated
    /// per drain once the previous chain is taken, so at a low send rate (a handful of records per
    /// window) a fixed node would turn ~100 drains/second into ~17 MB/second of garbage for a
    /// handful of records — a .NET-only cost with no counterpart in the design being mirrored.
    /// Growing from <see cref="InitialCapacity"/> makes the node cost track what the drain actually
    /// used, and a full node still ends up at exactly the anchor's capacity.
    /// </remarks>
    private sealed class Node
    {
        // Small enough that a low-rate drain allocates almost nothing, large enough that the
        // doubling to SLOT_CAPACITY costs a handful of copies (7 growths, ~2x total slot copies).
        private const int InitialCapacity = 16;

        private readonly int _maxCapacity;

        internal Node(int maxCapacity)
        {
            _maxCapacity = maxCapacity;
            int initial = Math.Min(InitialCapacity, maxCapacity);

            TopicPins = new PinnedTopicCache.TopicPin[initial];
            KeyPins = new MemoryHandle[initial];
            ValuePins = new MemoryHandle[initial];
            Completions = new TaskCompletionSource<RecordMetadata>?[initial];
            Deliveries = new DeliveryRegistration?[initial];
            Natives = new ProducerRecordNative[initial];
            Futures = new IntPtr[initial];
            Errors = new IntPtr[initial];
        }

        /// <summary>No Python counterpart: Python owns a <c>topic_owned</c> copy per record.</summary>
        internal PinnedTopicCache.TopicPin[] TopicPins;

        /// <summary>No Python counterpart: Python holds a refcount on the record's <c>bytes</c>.</summary>
        internal MemoryHandle[] KeyPins;

        internal MemoryHandle[] ValuePins;

        /// <summary>Nulled per index as it settles, so a node retains no user objects it is done with.</summary>
        internal TaskCompletionSource<RecordMetadata>?[] Completions;

        internal DeliveryRegistration?[] Deliveries;

        /// <summary>The blittable array the P/Invoke reads (anchor: a stack array at <c>:585</c>).</summary>
        internal ProducerRecordNative[] Natives;

        internal IntPtr[] Futures;

        internal IntPtr[] Errors;

        internal int Count { get; set; }

        internal Node? Next { get; set; }

        /// <summary>
        /// Grows every parallel array so index <see cref="Count"/> is writable. Called under the
        /// accumulator's lock, and only from <see cref="SendAccumulator.Append"/> — never while the
        /// batch thread is sending this node, which happens only after the node has been taken out of
        /// the chain. Grows all eight together or not at all: a partial growth would leave the
        /// parallel arrays at different lengths, and the index that walks them assumes they match.
        /// </summary>
        internal void EnsureSlot()
        {
            int required = Count + 1;

            // Gate on the LAST array resized below, not the first: an out-of-memory part way through
            // the sequence leaves the earlier arrays longer than the later ones, and gating on
            // `TopicPins` (the first) would then wave through a write that indexes past `Errors`.
            // Errors reaching `required` means all eight did.
            if (required <= Errors.Length)
            {
                return;
            }

            int grown = Math.Min(Math.Max(Errors.Length * 2, required), _maxCapacity);

            Array.Resize(ref TopicPins, grown);
            Array.Resize(ref KeyPins, grown);
            Array.Resize(ref ValuePins, grown);
            Array.Resize(ref Completions, grown);
            Array.Resize(ref Deliveries, grown);
            Array.Resize(ref Natives, grown);
            Array.Resize(ref Futures, grown);
            Array.Resize(ref Errors, grown);
        }
    }
}
