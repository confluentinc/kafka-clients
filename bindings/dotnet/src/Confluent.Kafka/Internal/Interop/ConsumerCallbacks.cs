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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The managed side of the C ABI's void-result completion callback
/// (<c>kafka_consumer_Consumer_op_callback_t</c>, ffi-marshalling.md §B6). It is a
/// kept-alive, classic Cdecl delegate — the only portable mechanism on the
/// netstandard2.0 floor, which has no function pointers / <c>[UnmanagedCallersOnly]</c>
/// (§0.1).
/// </summary>
/// <remarks>
/// <para>
/// The delegate <see cref="Operation"/> is held in a <c>static readonly</c> field
/// so the GC cannot collect the native thunk while the core still holds it. The
/// per-op context (an <see cref="OperationCompletionSource"/>) is rooted separately
/// by a <see cref="GCHandle"/> passed as <c>user_data</c>.
/// </para>
/// <para>
/// The body is a strict <b>no-throw boundary</b>: the callback fires on the core's
/// foreign callback-dispatcher thread (or inline on the caller thread if the core
/// rejects at its own guard), so there is no managed frame to catch an escaping
/// exception — one would be undefined behavior across the FFI. Every exception is
/// caught and surfaced through the <c>Task</c>, and the rooting
/// <see cref="GCHandle"/> is freed exactly once in a <c>finally</c>, on every path.
/// </para>
/// </remarks>
internal static class ConsumerCallbacks
{
    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_op_callback_t</c>:
    /// <c>void (*)(kafka_common_KafkaError_t* error, void* user_data)</c>. A non-null
    /// <paramref name="error"/> is failure; null is success — there is no result
    /// handle for void-result ops.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void OperationCallback(IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every void-result <c>*_async</c>
    /// submission. Rooted for the process lifetime, so the native thunk never
    /// dangles (ffi §B6 keep-alive).
    /// </summary>
    internal static readonly OperationCallback Operation = OnOperation;

    private static void OnOperation(IntPtr error, IntPtr userData)
    {
        OperationCompletionSource? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource)handle.Target!;
            context.Complete(error);
        }
        catch (Exception exception)
        {
            // No-throw boundary: never unwind into native. Surface via the Task if
            // the context was recovered; otherwise there is nothing to fault (and
            // the finally still frees the GCHandle if we have the context).
            context?.TrySetException(exception);
        }
        finally
        {
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_poll_callback_t</c>:
    /// <c>void (*)(kafka_consumer_ConsumerRecords_t* records,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — the <b>owned-handle</b>
    /// completion shape (ffi-marshalling.md §B6/§B7). On success
    /// <paramref name="records"/> is a non-null owned batch and <paramref name="error"/>
    /// is null; on failure (incl. the inline core-guard rejection)
    /// <paramref name="records"/> is null and <paramref name="error"/> is non-null. The
    /// callback <b>takes ownership</b> of whichever handle is non-null and frees it.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void PollCallback(IntPtr records, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>poll_async</c> submission. Rooted
    /// for the process lifetime, so the native thunk never dangles (ffi §B6 keep-alive).
    /// </summary>
    internal static readonly PollCallback Poll = OnPoll;

    /// <summary>
    /// The poll completion trampoline. Runs on the core's foreign dispatcher thread
    /// (or inline on the caller thread on a core-guard rejection). It performs the
    /// batch <b>copy-out on this (dispatcher) thread</b> (the M3/P3 key decision) and
    /// completes the awaiter with an owned <see cref="ConsumerRecords"/>.
    /// </summary>
    /// <remarks>
    /// <b>Free-exactly-once, every path (the phase's central correctness obligation).</b>
    /// The <c>finally</c> — which also runs on the no-throw path — frees, on <b>every</b>
    /// path (success / failure / inline core-rejection / no-throw / submit-threw is
    /// handled by <c>AbandonBeforeSubmit</c> instead, since native never ran here):
    /// <list type="number">
    /// <item>the owned <c>ConsumerRecords_t</c> batch, <b>after</b> the copy-out —
    /// via the null-safe <see cref="NativeMethods.ConsumerRecordsDestroy"/> (a no-op
    /// when <paramref name="records"/> is null, i.e. failure / rejection);</item>
    /// <item>the <c>KafkaError</c> on failure — inside <see cref="OperationCompletionSource{TResult}.Complete(IntPtr)"/>
    /// via <see cref="KafkaException.FromHandle(IntPtr)"/>, which <c>_destroy</c>s it in
    /// its own <c>finally</c>;</item>
    /// <item>the per-op rooting <see cref="GCHandle"/> — via
    /// <see cref="OperationCompletionSource{TResult}.FreeGcHandle"/> (idempotent).</item>
    /// </list>
    /// The batch destroy is safe only because the copy-out retains no borrowed pointer
    /// (see <see cref="ConsumerRecordsMarshal"/>).
    /// </remarks>
    private static void OnPoll(IntPtr records, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<ConsumerRecords>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<ConsumerRecords>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                // Failure (records is null). Complete maps to KafkaException /
                // OperationCanceledException and frees the error handle via FromHandle.
                context.Complete(error);
            }
            else
            {
                // Success (records is a non-null owned borrow-root). Copy out on THIS
                // (dispatcher) thread, then the finally destroys the batch — the whole
                // §6.4 copy-out default.
                ConsumerRecords marshalled = ConsumerRecordsMarshal.CopyOut(records);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary: never unwind into native. Surface via the Task; the
            // finally still frees the batch + GCHandle if we recovered the context.
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of BOTH frees, on EVERY path (ffi §B6). Destroy is null-safe,
            // so it is a no-op when records is null (failure / inline rejection).
            NativeMethods.ConsumerRecordsDestroy(records);
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_position_callback_t</c>:
    /// <c>void (*)(int64_t position, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — the <b>scalar</b> completion shape (ffi-marshalling.md
    /// §B6/§B7), the third bridge shape after the void (<c>op</c>) and owned-handle
    /// (<c>poll</c>) forms. On success <paramref name="position"/> is the offset and
    /// <paramref name="error"/> is null; on failure (incl. the inline core-guard
    /// rejection) <paramref name="position"/> is 0 and <paramref name="error"/> is
    /// non-null. Position is never absent on success (no presence flag) and the result
    /// carries <b>no owned handle</b>, so the trampoline has nothing to <c>_destroy</c>
    /// on success — the sole structural difference from <see cref="OnPoll"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void PositionCallback(long position, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>position_async</c> submission.
    /// Rooted for the process lifetime, so the native thunk never dangles (ffi §B6
    /// keep-alive).
    /// </summary>
    internal static readonly PositionCallback Position = OnPosition;

    /// <summary>
    /// The position completion trampoline. Runs on the core's foreign dispatcher thread
    /// (or inline on the caller thread on a core-guard rejection). The scalar
    /// <paramref name="position"/> is blittable, so the "marshalling" is trivial — no
    /// copy-out, no native read — and success completes the awaiter with the offset
    /// directly.
    /// </summary>
    /// <remarks>
    /// <b>Free-exactly-once, every path (the phase's central correctness obligation).</b>
    /// The <c>finally</c> — which also runs on the no-throw path — frees the per-op
    /// rooting <see cref="GCHandle"/> on <b>every</b> path (success / operational
    /// failure / inline core-rejection / no-throw; submit-threw is handled by
    /// <c>AbandonBeforeSubmit</c> instead, since native never ran here) via
    /// <see cref="OperationCompletionSource{TResult}.FreeGcHandle"/> (idempotent). The
    /// <c>KafkaError</c> on the failure path is freed exactly once inside
    /// <see cref="OperationCompletionSource{TResult}.Complete(IntPtr)"/> via
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// <para>
    /// <b>The one structural difference from <see cref="OnPoll"/>:</b> there is
    /// <b>no</b> <c>*Destroy</c> call in this <c>finally</c> — the scalar result carries
    /// no owned handle, so there is nothing to destroy on success.
    /// </para>
    /// </remarks>
    private static void OnPosition(long position, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<long>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<long>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                // Failure (position is 0). Complete maps to KafkaException /
                // OperationCanceledException and frees the error handle via FromHandle.
                context.Complete(error);
            }
            else
            {
                // Success: the scalar offset is the result directly — no owned handle,
                // no copy-out, no _destroy. (Position is never absent on success.)
                context.CompleteWithResult(position);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary (foreign dispatcher thread): never unwind into native.
            // Surface via the Task; the finally still frees the GCHandle if we recovered
            // the context.
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of the GCHandle free, on EVERY path (ffi §B6), incl. the inline
            // core-guard rejection. NO batch destroy here — the scalar shape owns no
            // result handle (the ONLY structural difference from OnPoll).
            context?.FreeGcHandle();
        }
    }
}
