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
/// callback and frees its own <see cref="GCHandle"/> exactly once.
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
/// (ffi §B7). This is <b>mandatory</b>, not an optimization.
/// </para>
/// <para>
/// <b>Who frees the <see cref="GCHandle"/>.</b> The completion callback
/// (<see cref="Complete"/>) is the <b>sole owner</b> of the free on the normal path
/// (invariant #2), matching the in-repo Python (<c>Py_DECREF</c> in the op
/// trampoline) and confluent-kafka-dotnet (<c>gch.Free()</c> in the delivery-report
/// callback) siblings: the callback frees; teardown drains — it never reclaims.
/// <see cref="AbandonBeforeSubmit"/> frees it only when the submitting P/Invoke
/// threw so native never ran and the callback can never fire. Both dispose the
/// cancellation registration idempotently; the free itself is
/// <see cref="Interlocked"/>-guarded, so it runs exactly once. Under the
/// single-owner (not-thread-safe) model there is no teardown-side fault/reclaim of
/// a separately-submitted op — the awaiter of an op is its disposer, so there is no
/// third path to free the handle.
/// </para>
/// </remarks>
internal sealed class OperationCompletionSource
{
    // A void result; the generic TaskCompletionSource is used because the
    // non-generic TaskCompletionSource does not exist on the netstandard2.0 floor.
    private readonly TaskCompletionSource<bool> _tcs =
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private GCHandle _gcHandle;
    private int _gcHandleFreed;

    private CancellationToken _cancellationToken;
    private CancellationTokenRegistration _registration;
    private int _cancellationRequested;

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
    /// Completes the awaiter from the C callback. Maps a non-null
    /// <paramref name="error"/> handle to a <see cref="KafkaException"/> via
    /// <see cref="KafkaException.FromHandle"/> (which frees the handle exactly once),
    /// and translates a fault to <see cref="OperationCanceledException"/> when the op
    /// was canceled via its token. A concurrent op rejected by the <b>core</b>
    /// arrives here (fired inline by the core with a <c>ConcurrentModification</c>
    /// error) and faults the <c>Task</c> — the observable "concurrent async op →
    /// <see cref="KafkaException"/>" contract (ffi §B5), now delivered by the core,
    /// not a managed pre-check.
    /// </summary>
    internal void Complete(IntPtr error)
    {
        // Stop the cancellation registration from firing wakeup() after the op has
        // resolved (minimizes the intrinsic wakeup-vs-next-op race — CLAUDE.md /
        // consumer-threading §11 documents this race as intentional/Java-faithful).
        _registration.Dispose();

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
