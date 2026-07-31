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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The per-operation bridge context for a void-result async consumer op
/// (<c>kafka_consumer_Consumer_op_callback_t</c>, ffi-marshalling.md §B6/§B7): it
/// couples a <see cref="TaskCompletionSource{TResult}"/> to the C completion
/// callback, releasing the managed access guard and freeing its own
/// <see cref="GCHandle"/> exactly once.
/// </summary>
/// <remarks>
/// <para>
/// <b>Rooting.</b> From submit until the callback fires — the whole op, not a
/// synchronous call — this context is kept alive by a <see cref="GCHandle"/>
/// (allocated by the submitter, stored here, passed to native as
/// <c>user_data</c>). The delegate itself is rooted separately by a
/// <c>static readonly</c> field (see <c>ConsumerCallbacks</c>).
/// </para>
/// <para>
/// <b>Foreign-thread completion.</b> The callback fires on the core's
/// callback-dispatcher thread (or inline on the caller thread if the core rejects
/// the op at its own guard), never on the awaiter's thread. The
/// <see cref="TaskCompletionSource{TResult}"/> is therefore built with
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> so the
/// awaiter's continuation does not run on — and stall — that dispatcher thread
/// (ffi §B7). This is mandatory, not an optimization.
/// </para>
/// <para>
/// <b>Who frees the <see cref="GCHandle"/>.</b> The completion callback
/// (<see cref="Complete"/>) is the <b>sole owner</b> of the free on the normal path,
/// matching the in-repo Python and confluent-kafka-dotnet siblings (the callback
/// frees; teardown drains — it never reclaims). <see cref="AbandonBeforeSubmit"/>
/// frees it only when the submitting P/Invoke threw so native never ran and the
/// callback can never fire. <see cref="FaultTaskOnly"/> (the synchronous
/// <c>Dispose</c> path) faults the awaiter's <c>Task</c> so it cannot strand but
/// deliberately does <b>not</b> free the <see cref="GCHandle"/> — a completion
/// callback already queued before <c>Consumer_destroy</c> may still fire after it and
/// must recover a live handle (no use-after-free). All three release the guard and
/// dispose the cancellation registration idempotently; the free itself is
/// <see cref="Interlocked"/>-guarded, so a straggler callback frees exactly once.
/// </para>
/// </remarks>
internal sealed class OperationCompletionSource
{
    // A void result; the generic TaskCompletionSource is used because the
    // non-generic TaskCompletionSource does not exist on the netstandard2.0 floor.
    private readonly TaskCompletionSource<bool> _tcs =
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    // Null for lifecycle ops (close) that do not participate in the one-op guard;
    // non-null for user ops (subscribe / seek).
    private readonly ConsumerAccessGuard? _guard;

    private GCHandle _gcHandle;
    private int _gcHandleFreed;

    private CancellationToken _cancellationToken;
    private CancellationTokenRegistration _registration;
    private int _cancellationRequested;

    internal OperationCompletionSource(ConsumerAccessGuard? guard)
    {
        _guard = guard;
    }

    /// <summary>The awaitable completed by the C completion callback.</summary>
    internal Task Task => _tcs.Task;

    /// <summary>
    /// Records the <see cref="GCHandle"/> that roots this context for native. Set by
    /// the submitter immediately after allocation and before the P/Invoke.
    /// </summary>
    internal void SetGcHandle(GCHandle handle) => _gcHandle = handle;

    /// <summary>
    /// Wires best-effort cancellation: on <paramref name="cancellationToken"/>
    /// firing, the op is marked canceled and <paramref name="wakeup"/> is invoked
    /// (mapping <see cref="CancellationToken"/> to the consumer's <c>wakeup()</c>,
    /// ffi §B7). If a subsequent completion carries an error and cancellation was
    /// requested, <see cref="Complete"/> maps the fault to an
    /// <see cref="OperationCanceledException"/> instead of a
    /// <see cref="KafkaException"/>.
    /// </summary>
    internal void RegisterCancellation(CancellationToken cancellationToken, Action wakeup)
    {
        _cancellationToken = cancellationToken;
        if (cancellationToken.CanBeCanceled)
        {
            _registration = cancellationToken.Register(
                static state =>
                {
                    var (self, wake) = ((OperationCompletionSource, Action))state!;
                    Interlocked.Exchange(ref self._cancellationRequested, 1);
                    wake();
                },
                (this, wakeup));
        }
    }

    /// <summary>
    /// Completes the awaiter from the C callback. Releases the guard <b>before</b>
    /// completing the <c>Task</c> (mirroring the core, which releases its own guard
    /// before firing the callback), maps a non-null <paramref name="error"/> handle
    /// to a <see cref="KafkaException"/> via <see cref="KafkaException.FromHandle"/>
    /// (which frees the handle exactly once), and translates a fault to
    /// <see cref="OperationCanceledException"/> when the op was canceled via its
    /// token.
    /// </summary>
    internal void Complete(IntPtr error)
    {
        // Stop the cancellation registration from firing wakeup() after the op has
        // resolved (minimizes the intrinsic wakeup-vs-next-op race — CLAUDE.md /
        // consumer-threading §11 documents this race as intentional/Java-faithful).
        _registration.Dispose();

        // Release BEFORE completing the Task: the awaited op is done, so the
        // consumer is free; a continuation may immediately resubmit without hitting
        // the one-op rejection (ffi §B7).
        _guard?.Release();

        if (error != IntPtr.Zero)
        {
            // FromHandle copies the values out and frees the KafkaError handle in a
            // finally (freed exactly once), even if construction throws.
            KafkaException failure = KafkaException.FromHandle(error)!;
            if (Volatile.Read(ref _cancellationRequested) != 0)
            {
                _tcs.TrySetCanceled(_cancellationToken);
            }
            else
            {
                _tcs.TrySetException(failure);
            }
        }
        else
        {
            _tcs.TrySetResult(true);
        }
    }

    /// <summary>
    /// Faults the awaiter with <paramref name="exception"/>. Used only by the
    /// callback's no-throw boundary to surface an unexpected managed failure without
    /// unwinding into native.
    /// </summary>
    internal void TrySetException(Exception exception) => _tcs.TrySetException(exception);

    /// <summary>
    /// Cleanup for the case where the submitting P/Invoke throws before native could
    /// have fired the callback (so ownership never transferred): disposes the
    /// cancellation registration, releases the guard, and frees the
    /// <see cref="GCHandle"/>. The awaiter is faulted by the submitter's rethrow.
    /// </summary>
    internal void AbandonBeforeSubmit()
    {
        _registration.Dispose();
        _guard?.Release();
        FreeGcHandle();
    }

    /// <summary>
    /// Faults the awaiter's <c>Task</c> from the <b>synchronous</b> <c>Dispose</c>
    /// teardown path (ffi §B7) <b>without</b> freeing the rooting
    /// <see cref="GCHandle"/>. There, a <em>guarded</em> <c>close_with_timeout</c> is
    /// rejected while this op still holds the core access guard, so it does <b>not</b>
    /// drain the op; the following <c>Consumer_destroy</c> cancels the op's future so
    /// — in the common case — its completion callback never fires. Faulting the
    /// <c>Task</c> with <paramref name="exception"/> here stops a fire-and-forget
    /// awaiter from stranding (the Finding-1 strand fix).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Frees nothing.</b> Unlike <see cref="Complete"/> /
    /// <see cref="AbandonBeforeSubmit"/>, this does <b>not</b> free the
    /// <see cref="GCHandle"/>. The completion callback is the sole owner of that free
    /// (matching the in-repo Python + confluent-kafka-dotnet siblings). If the op's
    /// completion job was already enqueued before <c>Consumer_destroy</c>, the
    /// callback still fires <em>after</em> destroy (the ABI closes the completion
    /// channel without joining the dispatcher, which drains any queued jobs); it then
    /// recovers a <b>live</b> context via <c>GCHandle.FromIntPtr(userData).Target</c>
    /// and frees the handle itself — so freeing it from here would be a
    /// use-after-free (Critic N=5 Finding 3, case B).
    /// </para>
    /// <para>
    /// <b>Accepted residual (case A).</b> If destroy cancels the op's future
    /// <em>before</em> its completion job is enqueued, the callback never fires and
    /// that one op's <see cref="GCHandle"/> (+ this context / its TCS) leaks — a rare,
    /// one-time, teardown-only leak in a misuse case (an unawaited in-flight op +
    /// synchronous <c>Dispose</c>). This mirrors the residual the Python (bare
    /// <c>_destroy</c> after drain) and confluent-kafka-dotnet (unflushed-at-destroy)
    /// siblings accept; steer users to <c>DisposeAsync</c>, which drains and has no
    /// leak.
    /// </para>
    /// <para>
    /// Race-safe:
    /// <see cref="TaskCompletionSource{TResult}.TrySetException(System.Exception)"/>
    /// no-ops once the <c>Task</c> is completed, and the guard release / registration
    /// dispose are idempotent — so a callback that fired before <c>Dispose</c> makes
    /// this a harmless no-op. Never sync-over-async: it does not wait on the op
    /// <c>Task</c>.
    /// </para>
    /// </remarks>
    internal void FaultTaskOnly(Exception exception)
    {
        _registration.Dispose();
        _guard?.Release();
        _tcs.TrySetException(exception);

        // Deliberately NOT FreeGcHandle: the completion callback is the sole owner of
        // the free (see remarks — a straggler callback firing after Consumer_destroy
        // must recover a live GCHandle, not a freed/recycled one).
    }

    /// <summary>
    /// Frees the rooting <see cref="GCHandle"/> exactly once, on every completion
    /// path (including the inline core-guard-rejection error path). Idempotent.
    /// </summary>
    internal void FreeGcHandle()
    {
        if (Interlocked.Exchange(ref _gcHandleFreed, 1) == 0 && _gcHandle.IsAllocated)
        {
            _gcHandle.Free();
        }
    }
}
