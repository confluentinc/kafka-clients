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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

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
}
