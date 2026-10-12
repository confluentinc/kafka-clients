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
/// The per-operation bridge context for a <b>result-returning</b> async consumer op
/// (an owned-handle completion, ffi-marshalling.md §B6/§B7): it couples a
/// <see cref="TaskCompletionSource{TResult}"/> to the C completion callback and frees
/// its own <see cref="GCHandle"/> exactly once. The <b>void</b>-result path
/// (<c>op_callback_t</c>) is the specialization <see cref="OperationCompletionSource"/>
/// (<c><typeparamref name="TResult"/> = bool</c>) below.
/// </summary>
/// <remarks>
/// <para>
/// <b>Generalized (M3/P3, PLAN decision 4).</b> M3/P1 shipped a void-only bridge;
/// M3/P3 adds the owned-handle (result-returning) shape used by <c>poll</c> and the
/// five future owned-handle ops (<c>committed</c> / <c>offsetsForTimes</c> /
/// <c>beginning|endOffsets</c> / <c>partitionsFor</c> / <c>listTopics</c>). Rather
/// than duplicate the 5-invariant machinery per op, the bridge is generalized to
/// carry a result <typeparamref name="TResult"/>; the void path is expressed as
/// <c>&lt;bool&gt;</c> (the thin <see cref="OperationCompletionSource"/> subclass),
/// keeping every M3/P1+P2 call site and its observable semantics byte-for-byte
/// (<see cref="Complete(IntPtr)"/> still maps null error → success, non-null →
/// <see cref="KafkaException"/> / <see cref="OperationCanceledException"/>).
/// </para>
/// <para>
/// <b>The marshalling lives in the trampoline, not here.</b> On success the
/// callback (on the core's dispatcher thread) marshals the native result into an
/// owned managed <typeparamref name="TResult"/> and hands it to
/// <see cref="CompleteWithResult(TResult)"/>. This context does <b>no</b> native
/// reads and stays result-type-agnostic — the copy-out (ffi §6.4) is the
/// trampoline's job (see <c>ConsumerCallbacks</c>).
/// </para>
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
/// (ffi §B7). This is <b>mandatory</b>, not an optimization.
/// </para>
/// <para>
/// <b>Who frees the <see cref="GCHandle"/>.</b> The completion callback
/// (<see cref="Complete"/> / <see cref="CompleteWithResult"/>) is the <b>sole
/// owner</b> of the free on the normal path (invariant #2), matching the in-repo
/// Python (<c>Py_DECREF</c> in the op trampoline) and confluent-kafka-dotnet
/// (<c>gch.Free()</c> in the delivery-report callback) siblings: the callback frees;
/// teardown drains — it never reclaims. <see cref="AbandonBeforeSubmit"/> frees it
/// only when the submitting P/Invoke threw so native never ran and the callback can
/// never fire. Both dispose the cancellation registration idempotently; the free
/// itself is <see cref="Interlocked"/>-guarded, so it runs exactly once. Under the
/// single-owner (not-thread-safe) model there is no teardown-side fault/reclaim of a
/// separately-submitted op — the awaiter of an op is its disposer, so there is no
/// third path to free the handle.
/// </para>
/// </remarks>
/// <typeparam name="TResult">
/// The already-marshalled managed result type (e.g. <c>ConsumerRecords</c> for poll,
/// <c>bool</c> for the void path).
/// </typeparam>
internal class OperationCompletionSource<TResult>
{
    private readonly TaskCompletionSource<TResult> _tcs =
        new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);

    private GCHandle _gcHandle;
    private int _gcHandleFreed;

    // The consumer's SafeHandle, ref-counted for the WHOLE async op (span-the-op):
    // DangerousAddRef at submit keeps the handle's count above zero while the op is in
    // flight, and DangerousRelease in FreeGcHandle drops it when the op completes. Because
    // SafeHandle.ReleaseHandle (→ Consumer_destroy) fires only at count zero, this defers
    // the guardless native destroy until the in-flight op is done — closing the
    // destroy-vs-in-flight-op use-after-free (ffi §B2/§B7). Null when no ref was taken.
    private SafeHandle? _handleRef;

    private CancellationToken _cancellationToken;
    private CancellationTokenRegistration _registration;
    private int _cancellationRequested;

    /// <summary>The awaitable completed by the C completion callback.</summary>
    internal Task<TResult> Task => _tcs.Task;

    /// <summary>
    /// Records the <see cref="GCHandle"/> that roots this context for native. Set by
    /// the submitter immediately after allocation and before the P/Invoke.
    /// </summary>
    internal void SetGcHandle(GCHandle handle) => _gcHandle = handle;

    /// <summary>
    /// Records the consumer <see cref="SafeHandle"/> whose ref-count the submitter bumped
    /// (<see cref="SafeHandle.DangerousAddRef(ref bool)"/>) for the lifetime of this op.
    /// <see cref="FreeGcHandle"/> releases it exactly once on completion, so
    /// <c>ReleaseHandle → Consumer_destroy</c> cannot run while the op is still in flight
    /// (ffi §B2/§B7). Set by the submitter immediately after <see cref="SetGcHandle"/>.
    /// </summary>
    internal void SetHandleRef(SafeHandle handle) => _handleRef = handle;

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
                    var (self, wake) = ((OperationCompletionSource<TResult>, Action))state!;
                    Interlocked.Exchange(ref self._cancellationRequested, 1);
                    wake();
                },
                (this, wakeup));
        }
    }

    /// <summary>
    /// Completes the awaiter <b>successfully</b> with an already-marshalled managed
    /// <paramref name="result"/> (the owned copy-out produced on the dispatcher
    /// thread, ffi §6.4). Stops the cancellation registration first so a late
    /// <c>wakeup()</c> is not fired after the op resolved. The result-returning analog
    /// of the void path's success branch in <see cref="Complete(IntPtr)"/>.
    /// </summary>
    internal void CompleteWithResult(TResult result)
    {
        // Stop the cancellation registration from firing wakeup() after the op has
        // resolved (minimizes the intrinsic wakeup-vs-next-op race — CLAUDE.md /
        // consumer-threading §11 documents this race as intentional/Java-faithful).
        _registration.Dispose();
        _tcs.TrySetResult(result);
    }

    /// <summary>
    /// Completes the awaiter from the C callback on the <b>failure/cancel</b> path.
    /// Maps a non-null <paramref name="error"/> handle to a
    /// <see cref="KafkaException"/> via <see cref="KafkaException.FromHandle"/> (which
    /// frees the handle exactly once), and translates a fault to
    /// <see cref="OperationCanceledException"/> when the op was canceled via its
    /// token. A concurrent op rejected by the <b>core</b> arrives here (fired inline by
    /// the core with a <c>ConcurrentModification</c> error) and faults the <c>Task</c>
    /// — the observable "concurrent async op → <see cref="KafkaException"/>" contract
    /// (ffi §B5), now delivered by the core, not a managed pre-check.
    /// </summary>
    /// <remarks>
    /// A <b>null</b> <paramref name="error"/> is the <b>void</b> success case (the
    /// specialization <see cref="OperationCompletionSource"/> overrides how success is
    /// realized). For a result-returning op the trampoline never calls this with a
    /// null error — it calls <see cref="CompleteWithResult(TResult)"/> instead — so the
    /// base intentionally leaves the null-error branch to the subclass.
    /// </remarks>
    internal void Complete(IntPtr error)
    {
        // Stop the cancellation registration from firing wakeup() after the op has
        // resolved (minimizes the intrinsic wakeup-vs-next-op race — CLAUDE.md /
        // consumer-threading §11 documents this race as intentional/Java-faithful).
        _registration.Dispose();

        if (error != IntPtr.Zero)
        {
            // FromHandle copies the values out and frees the Error handle in a
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
            // Null error = success. The base cannot fabricate a TResult, so it defers
            // to the subclass (the void path completes with `true`); a
            // result-returning op never reaches here with a null error (it uses
            // CompleteWithResult). See OperationCompletionSource (the <bool> subclass).
            CompleteWithSuccessNoResult();
        }
    }

    /// <summary>
    /// Realizes the null-error (success) case of <see cref="Complete(IntPtr)"/> when
    /// there is no marshalled result to carry. The result-returning base has no
    /// natural <typeparamref name="TResult"/> value here, so it does nothing; the void
    /// specialization overrides this to <c>TrySetResult(true)</c>. A result-returning
    /// op never hits this path (it completes via <see cref="CompleteWithResult"/>).
    /// </summary>
    private protected virtual void CompleteWithSuccessNoResult()
    {
    }

    /// <summary>
    /// Faults the awaiter with <paramref name="exception"/>. Used only by the
    /// callback's no-throw boundary to surface an unexpected managed failure without
    /// unwinding into native.
    /// </summary>
    internal void TrySetException(Exception exception) => _tcs.TrySetException(exception);

    /// <summary>
    /// Cancels the awaiter directly — the realization of best-effort cancellation for a client
    /// that has <b>no native abort path</b> (the producer, which has no <c>wakeup()</c>, unlike
    /// the consumer; ffi-marshalling.md §A7). Wired as the <c>wakeup</c> action of
    /// <see cref="RegisterCancellation"/> by that client, so a canceled token cancels the
    /// .NET-side wait immediately; the native op continues to completion and its callback's
    /// <see cref="Complete(IntPtr)"/> / <see cref="CompleteWithResult(TResult)"/> is then a safe
    /// no-op on the already-canceled <c>TaskCompletionSource</c>. Disposes the cancellation
    /// registration first (idempotent; safe to call re-entrantly from inside the registration's
    /// own callback — <see cref="CancellationTokenRegistration.Dispose"/> does not block there).
    /// </summary>
    /// <remarks>
    /// <b>Additive, consumer path untouched.</b> The consumer wires
    /// <see cref="RegisterCancellation"/>'s <c>wakeup</c> to the native <c>wakeup()</c> (which
    /// faults the op with a Wakeup error, routed to a canceled task by
    /// <see cref="Complete(IntPtr)"/>'s cancellation branch); it never calls this method. This
    /// method exists solely for the no-abort producer path, so it changes no existing behavior.
    /// It does <b>not</b> free the rooting <see cref="GCHandle"/> / span-the-op ref — those stay
    /// owned by the eventual completion callback (<see cref="FreeGcHandle"/>), so the native op's
    /// straggler callback still runs and cleans up exactly once (§A7 "frees its own rooting").
    /// </remarks>
    internal void CancelAwaiter()
    {
        _registration.Dispose();
        _tcs.TrySetCanceled(_cancellationToken);
    }

    /// <summary>
    /// Cleanup for the case where the submitting P/Invoke throws before native could
    /// have fired the callback (so ownership never transferred): disposes the
    /// cancellation registration and frees the <see cref="GCHandle"/>. The awaiter is
    /// faulted by the submitter's rethrow. This is the one non-callback path that
    /// frees the handle, and it is safe precisely because native never ran (so the
    /// callback — the normal sole owner — can never fire for this op).
    /// </summary>
    internal void AbandonBeforeSubmit()
    {
        _registration.Dispose();
        FreeGcHandle();
    }

    /// <summary>
    /// Frees the rooting <see cref="GCHandle"/> and releases the span-the-op consumer
    /// <see cref="SafeHandle"/> ref (<see cref="SetHandleRef"/>) exactly once, on every
    /// completion path (including the inline core-guard-rejection error path). Idempotent.
    /// Releasing the handle ref here — at op completion — is what lets a deferred
    /// <c>Consumer_destroy</c> finally run, now that the op no longer touches the consumer
    /// (ffi §B2/§B7). Paired 1:1 with the submitter's <see cref="SafeHandle.DangerousAddRef(ref bool)"/>.
    /// </summary>
    internal void FreeGcHandle()
    {
        if (Interlocked.Exchange(ref _gcHandleFreed, 1) == 0)
        {
            if (_gcHandle.IsAllocated)
            {
                _gcHandle.Free();
            }

            _handleRef?.DangerousRelease();
            _handleRef = null;
        }
    }
}

/// <summary>
/// The <b>void</b>-result specialization of the completion bridge
/// (<c>kafka_consumer_Consumer_op_callback_t</c>, ffi-marshalling.md §B6/§B7): a
/// thin <c>OperationCompletionSource&lt;bool&gt;</c> whose <c>Task</c> is a plain
/// <see cref="System.Threading.Tasks.Task"/> and whose null-error success completes
/// with <c>true</c>. Every M3/P1+P2 call site (<c>new OperationCompletionSource()</c>,
/// <c>context.Complete(error)</c>, <c>context.Task</c>) keeps working unchanged.
/// </summary>
internal sealed class OperationCompletionSource : OperationCompletionSource<bool>
{
    /// <summary>
    /// The awaitable completed by the C completion callback, as a non-generic
    /// <see cref="System.Threading.Tasks.Task"/> (the void-op surface M3/P1+P2 use).
    /// </summary>
    internal new Task Task => base.Task;

    /// <inheritdoc/>
    private protected override void CompleteWithSuccessNoResult() => CompleteWithResult(true);
}
