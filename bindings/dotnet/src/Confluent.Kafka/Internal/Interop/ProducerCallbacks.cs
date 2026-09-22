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
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The managed side of the producer's C ABI completion callbacks (M11/P2, the async
/// peripherals) — a thin mirror of <see cref="ConsumerCallbacks"/>. Each is a
/// kept-alive, classic Cdecl delegate, the only portable mechanism on the
/// netstandard2.0 floor, which has no function pointers / <c>[UnmanagedCallersOnly]</c>
/// (ffi-marshalling.md §0.1).
/// </summary>
/// <remarks>
/// <para>
/// The producer's <c>flush</c> / <c>close</c> completion is the <b>void</b>-result shape
/// (<c>kafka_producer_Producer_flush_callback_t</c> /
/// <c>kafka_producer_Producer_close_callback_t</c>: <c>(KafkaError*, void*)</c>) — signature-
/// identical to the consumer's <c>op_callback_t</c>. The <c>partitions_for</c> completion is
/// the <b>owned-handle</b> shape (<c>kafka_producer_Producer_partitions_for_callback_t</c>:
/// <c>(PartitionInfoList*, KafkaError*, void*)</c>) — signature-identical to the consumer's
/// <c>partitions_for_callback_t</c> and reusing the consumer's shared
/// <c>kafka_consumer_PartitionInfoList_t</c> (the header names it after the consumer sibling
/// deliberately), so the shipped <see cref="PartitionInfoListMarshal"/> applies verbatim. A
/// thin producer-local mirror (rather than reusing the consumer delegate types) keeps the
/// producer module self-documenting (PLAN §3, "recommend the mirror").
/// </para>
/// <para>
/// Each delegate instance is held in a <c>static readonly</c> field so the GC cannot collect
/// the native thunk while the core still holds it (ffi §A6/§B6 keep-alive). The per-op context
/// (an <see cref="OperationCompletionSource"/> / <see cref="OperationCompletionSource{TResult}"/>)
/// is rooted separately by a <see cref="GCHandle"/> passed as <c>user_data</c>.
/// </para>
/// <para>
/// Every body is a strict <b>no-throw boundary</b>: the callback fires on the producer's
/// foreign dispatcher thread (ffi §A1 / the header's "fires on the producer's dispatcher
/// thread"), so there is no managed frame to catch an escaping exception — one would be
/// undefined behavior across the FFI. Every exception is caught and surfaced through the
/// <c>Task</c>, and the rooting <see cref="GCHandle"/> (plus the span-the-op
/// <see cref="SafeHandle"/> ref) is freed exactly once in a <c>finally</c>, on every path.
/// </para>
/// </remarks>
internal static class ProducerCallbacks
{
    /// <summary>
    /// The C signature for <c>kafka_producer_Producer_flush_callback_t</c> /
    /// <c>kafka_producer_Producer_close_callback_t</c>:
    /// <c>void (*)(kafka_common_Error_t* error, void* user_data)</c>. A non-null
    /// <paramref name="error"/> is failure; null is success — there is no result handle for
    /// these void-result ops (the producer twin of the consumer's <c>OperationCallback</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void OperationCallback(IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every void-result producer <c>*_async</c>
    /// submission (<c>flush_async</c> / <c>close_async</c>). Rooted for the process lifetime,
    /// so the native thunk never dangles (ffi §A6 keep-alive).
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
            // No-throw boundary: never unwind into native. Surface via the Task if the
            // context was recovered; otherwise there is nothing to fault (and the finally
            // still frees the GCHandle if we have the context).
            context?.TrySetException(exception);
        }
        finally
        {
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The C signature for <c>kafka_producer_Producer_partitions_for_callback_t</c>:
    /// <c>void (*)(kafka_consumer_PartitionInfoList_t* list,
    /// kafka_common_Error_t* error, void* user_data)</c> — the <b>owned-handle</b>
    /// completion shape (ffi §A6/§B6). On success <paramref name="list"/> is a non-null owned
    /// list and <paramref name="error"/> is null; on failure <paramref name="list"/> is null
    /// and <paramref name="error"/> is non-null. The callback <b>takes ownership</b> of
    /// whichever handle is non-null and frees it (the header: "the caller owns whichever handle
    /// is non-null"). The list is the consumer's shared <c>PartitionInfoList_t</c>, so the
    /// shipped <see cref="PartitionInfoListMarshal"/> / <see cref="NativeMethods.PartitionInfoListDestroy"/>
    /// apply directly.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void PartitionInfoListCallback(IntPtr list, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>partitions_for_async</c> submission.
    /// Rooted for the process lifetime, so the native thunk never dangles (ffi §A6 keep-alive).
    /// </summary>
    internal static readonly PartitionInfoListCallback PartitionsFor = OnPartitionsFor;

    /// <summary>
    /// The <c>partitionsFor</c> completion trampoline — the producer twin of
    /// <see cref="ConsumerCallbacks.PartitionsFor"/>. Copies out the whole borrowed tree on
    /// this (dispatcher) thread via <see cref="PartitionInfoListMarshal.CopyOut"/>, then the
    /// <c>finally</c> destroys the list root via the null-safe
    /// <see cref="NativeMethods.PartitionInfoListDestroy"/>. Every <c>PartitionInfo</c> /
    /// <c>Node</c> / nested string is <b>borrowed</b> (ffi §B2 Category 4) and never freed —
    /// only the list root is destroyed.
    /// </summary>
    private static void OnPartitionsFor(IntPtr list, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<IReadOnlyList<PartitionInfo>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<IReadOnlyList<PartitionInfo>>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                context.Complete(error);
            }
            else
            {
                // Success (list is a non-null owned borrow-root). Copy out the whole tree on
                // THIS (dispatcher) thread; the finally then destroys the root.
                IReadOnlyList<PartitionInfo> marshalled = PartitionInfoListMarshal.CopyOut(list);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary: never unwind into native. Surface via the Task; the finally
            // still frees the list + GCHandle if we recovered the context.
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of BOTH frees, on EVERY path (ffi §A6/§B6). Destroy is null-safe, so it
            // is a no-op when list is null (failure); the borrowed PartitionInfo / Node / string
            // ELEMENTS are never destroyed (ffi §B2 Category 4).
            NativeMethods.PartitionInfoListDestroy(list);
            context?.FreeGcHandle();
        }
    }
}
