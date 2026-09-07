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
    private readonly ConcurrentQueue<PendingSend> _queue = new ConcurrentQueue<PendingSend>();

    // Signals "items may be available". Reset-before-drain (see RunLoop) avoids a lost wakeup.
    private readonly ManualResetEventSlim _signal = new ManualResetEventSlim(initialState: false);

    // Serializes Enqueue's stopped-check + enqueue against Stop's terminal drain, so an enqueue
    // racing teardown is never lost: it is either queued (and drained by Stop) or faulted in place.
    private readonly object _stopLock = new object();

    private readonly Thread _thread;

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
            _signal.Reset();

            if (_stopping)
            {
                // Leave any queued items for Stop's terminal drain (fault, no blocking get_all).
                break;
            }

            List<PendingSend> batch = DrainAll();
            if (batch.Count > 0)
            {
                try
                {
                    ProcessBatch(batch);
                }
                catch (Exception exception)
                {
                    // ProcessBatch already freed the batch's future handles on every one of its own
                    // paths (its `finally`'s destroy_all, or — for a throw that precedes that try,
                    // i.e. an allocation failure building its marshalling arrays — its allocation
                    // `catch`), but a throw that escaped it — a native failure surfacing from
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
            }
        }
    }

    /// <summary>Drains every currently-queued send into a batch (lock-free).</summary>
    private List<PendingSend> DrainAll()
    {
        List<PendingSend> batch = new List<PendingSend>();
        while (_queue.TryDequeue(out PendingSend pending))
        {
            batch.Add(pending);
        }

        return batch;
    }

    /// <summary>
    /// Resolves a drained batch: blocks on <c>get_all</c>, completes each TCS from its per-index
    /// result (exactly one of metadata / error is non-null, per the header), and frees every
    /// native handle on every path — each consumed index's metadata/error as it is read, all
    /// future handles via <c>destroy_all</c> in the <c>finally</c>, plus a <c>finally</c> sweep of
    /// any metadata/error handles for indices left unconsumed by a partway throw (e.g. an OOM in
    /// the error branch). Consumed slots are nulled so the sweep never double-frees. A throw that
    /// precedes that <c>try</c> altogether — an allocation failure building the three marshalling
    /// arrays — frees the batch's future handles from <paramref name="batch"/> itself, in the
    /// allocation <c>catch</c>; exactly one of those two future-free sites can ever run, because
    /// that <c>catch</c> rethrows and so the <c>try</c>/<c>finally</c> below is never entered.
    /// </summary>
    /// <remarks>
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
    private static void ProcessBatch(List<PendingSend> batch)
    {
        int count = batch.Count;
        IntPtr[] futures;
        IntPtr[] metadata;
        IntPtr[] errors;
        try
        {
            futures = new IntPtr[count];
            metadata = new IntPtr[count];
            errors = new IntPtr[count];
        }
        catch
        {
            // These three arrays are allocated OUTSIDE the try/finally below, so a failure here
            // (OOM) used to skip that finally and leak the whole batch's future handles — the one
            // hole in the "free every handle on every path" pattern (ffi §A2) this method otherwise
            // completes. Free them from `batch`, the one source that is valid before anything is
            // allocated, using the SINGULAR destroy per element: building the array `destroy_all`
            // requires is exactly what just failed, so the recovery path must not allocate.
            //
            // Freed EXACTLY once, and never double-freed: reaching the try/finally below requires
            // this try to complete normally, and this catch always rethrows — so of the two
            // future-free sites precisely one ever runs. No metadata/error handle exists yet either
            // (get_all has not been called), so there is nothing else to release here.
            for (int i = 0; i < count; i++)
            {
                NativeMethods.FutureRecordMetadataDestroy(batch[i].Future);
            }

            throw;
        }

        // Cannot throw: a List<T> indexer read below its own Count plus a readonly-struct property
        // read, with no allocation. So `futures` is either fully populated or never allocated.
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
            // catch (faults the TCSes) and Send's orphaned-future catch established — together with
            // the allocation catch above, which covers the one window this finally cannot reach
            // (a throw before the try was entered).
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
    /// </remarks>
    private void DrainAndFaultRemaining()
    {
        List<PendingSend> batch = DrainAll();
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
