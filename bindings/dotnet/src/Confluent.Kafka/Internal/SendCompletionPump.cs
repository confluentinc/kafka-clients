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
/// The producer send-completion pump (ffi §A7 Option A — inline pull-pump; PLAN §3/§6.3): one
/// background thread per <see cref="NativeProducer"/> that drains an unbounded MPSC queue of
/// <c>(future, TaskCompletionSource)</c> pairs, blocks on a batched
/// <c>FutureRecordMetadata_get_all</c>, completes each <see cref="TaskCompletionSource{TResult}"/>
/// (built with <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>), and frees the
/// future handles with <c>FutureRecordMetadata_destroy_all</c>. There is no per-send callback
/// and no producer handle here — the pump owns only the flat transient future / metadata / error
/// handles (ffi §A2 Category 2); the producer-outlives-pump invariant is enforced by
/// <see cref="NativeProducer"/>'s teardown ordering (stop + join the pump before
/// <c>Producer_destroy</c>, §A2), not by this type.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a pump, not <c>Task.Run(get)</c> per send.</b> One thread batches many completions per
/// blocking <c>get_all</c> — O(1) threads for unbounded in-flight sends — instead of parking a
/// pool thread per message (the sync-over-async anti-pattern, ffi §A7 / CLAUDE.md §11). The
/// <c>get_all</c> <c>block_on</c> parks only this thread; the core's Sender keeps running on the
/// runtime's worker pool (ffi §A1), so a blocked pump delays result delivery but never sending.
/// </para>
/// <para>
/// <b>Backpressure.</b> The queue is structurally unbounded but practically bounded by the core's
/// <c>buffer.memory</c> backpressure: the inline <c>Producer_send</c> on the caller thread blocks
/// up to <c>max.block.ms</c> when the core buffer is full, so callers cannot outrun the drain
/// (PLAN §4 decision 4). No managed bound / hand-cap is added.
/// </para>
/// <para>
/// <b>Teardown (<see cref="Stop"/>).</b> Called on the disposing thread after the
/// <see cref="NativeProducer"/> close latch is won: signals the loop to stop, joins the thread
/// (so no future handle is in use), then faults + frees anything still queued. A
/// <see cref="Enqueue"/> that races a completed <see cref="Stop"/> is caught under
/// <see cref="_stopLock"/> and faulted + freed in place — so no send is stranded or leaked
/// (deterministic, no accepted residual on the enqueue-vs-stop race).
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
    /// <paramref name="completion"/> and frees <paramref name="future"/>; if the pump has already
    /// stopped (teardown raced this enqueue) the send is faulted and its future freed here, so it
    /// is never stranded or leaked.
    /// </summary>
    internal void Enqueue(IntPtr future, TaskCompletionSource<RecordMetadata> completion)
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

            _queue.Enqueue(new PendingSend(future, completion));
            _signal.Set();
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
                ProcessBatch(batch);
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
    /// result (exactly one of metadata / error is non-null, per the header), frees every metadata
    /// / error handle, then frees all future handles with <c>destroy_all</c> — on every path.
    /// </summary>
    private static void ProcessBatch(List<PendingSend> batch)
    {
        int count = batch.Count;
        IntPtr[] futures = new IntPtr[count];
        IntPtr[] metadata = new IntPtr[count];
        IntPtr[] errors = new IntPtr[count];
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
                IntPtr meta = metadata[i];
                IntPtr error = errors[i];

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
                        completion.TrySetResult(result!);
                    }
                    else
                    {
                        completion.TrySetException(marshalFailure);
                    }
                }
                else
                {
                    // Failure: FromHandle reads the values and frees the error handle exactly once.
                    KafkaException failure = KafkaException.FromHandle(error)
                        ?? new KafkaException("The send failed without an error handle.");
                    completion.TrySetException(failure);
                }
            }
        }
        finally
        {
            // get_all does not consume the futures — free every future handle exactly once (§A2).
            NativeMethods.FutureRecordMetadataDestroyAll(futures, count);
        }
    }

    /// <summary>
    /// Faults + frees every still-queued send at teardown (no blocking <c>get_all</c>): the sends
    /// were accepted but the producer is closing, so their tasks fault with a teardown exception
    /// and their futures are destroyed. Runs under <see cref="_stopLock"/> from <see cref="Stop"/>.
    /// </summary>
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

    private static void DestroyFutures(IntPtr[] futures, int count) =>
        NativeMethods.FutureRecordMetadataDestroyAll(futures, count);

    private static KafkaException TeardownException() =>
        new KafkaException("The producer was closed before the send completed.");

    /// <summary>An enqueued send awaiting resolution: its future handle and its awaiter.</summary>
    private readonly struct PendingSend
    {
        internal PendingSend(IntPtr future, TaskCompletionSource<RecordMetadata> completion)
        {
            Future = future;
            Completion = completion;
        }

        internal IntPtr Future { get; }

        internal TaskCompletionSource<RecordMetadata> Completion { get; }
    }
}
