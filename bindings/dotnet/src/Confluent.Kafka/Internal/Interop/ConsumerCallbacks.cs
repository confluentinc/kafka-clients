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
    /// <c>void (*)(kafka_common_Error_t* error, void* user_data)</c>. A non-null
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
    /// kafka_common_Error_t* error, void* user_data)</c> — the <b>owned-handle</b>
    /// completion shape (ffi-marshalling.md §B6/§B7). On success
    /// <paramref name="records"/> is a non-null owned batch and <paramref name="error"/>
    /// is null; on failure (incl. the inline core-guard rejection)
    /// <paramref name="records"/> is null and <paramref name="error"/> is non-null. The
    /// callback <b>takes ownership</b> of whichever handle is non-null and frees it.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void PollCallback(IntPtr records, IntPtr error, IntPtr userData);

    // The poll completion trampoline is GENERIC (M6/P1b, PLAN §5): it must deserialize
    // each record's key/value with the consumer's IDeserializer<TKey>/IDeserializer<TValue>
    // to build a typed ConsumerRecords<TKey, TValue>, so it lives in the closed-generic
    // TypedPollCallbacks<TKey, TValue> (which supplies the rooted PollCallback of THIS
    // delegate type and does the copy-out via ConsumerRecordsMarshal.CopyOut<K,V> on the
    // dispatcher thread). The owned-handle offset-map / partition-metadata trampolines
    // below share this delegate SHAPE (records*, error*, ud) and every correctness
    // invariant of that poll trampoline (no-throw boundary, copy-out-before-destroy,
    // null-safe container destroy in the finally, error via
    // OperationCompletionSource<T>.Complete which frees the error handle, per-op GCHandle
    // freed exactly once, RunContinuationsAsynchronously); they differ only in result type,
    // marshaller, and which container _destroy runs.

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_position_callback_t</c>:
    /// <c>void (*)(int64_t position, kafka_common_Error_t* error,
    /// void* user_data)</c> — the <b>scalar</b> completion shape (ffi-marshalling.md
    /// §B6/§B7), the third bridge shape after the void (<c>op</c>) and owned-handle
    /// (<c>poll</c>) forms. On success <paramref name="position"/> is the offset and
    /// <paramref name="error"/> is null; on failure (incl. the inline core-guard
    /// rejection) <paramref name="position"/> is 0 and <paramref name="error"/> is
    /// non-null. Position is never absent on success (no presence flag) and the result
    /// carries <b>no owned handle</b>, so the trampoline has nothing to <c>_destroy</c>
    /// on success — the sole structural difference from <see cref="TypedPollCallbacks{TKey, TValue}"/>.
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
    /// <c>Error</c> on the failure path is freed exactly once inside
    /// <see cref="OperationCompletionSource{TResult}.Complete(IntPtr)"/> via
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// <para>
    /// <b>The one structural difference from <see cref="TypedPollCallbacks{TKey, TValue}"/>:</b> there is
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
            // result handle (the ONLY structural difference from the typed poll trampoline).
            context?.FreeGcHandle();
        }
    }

    // ---- Owned-handle offset-map completions (ffi §B6/§B7) — M5/P4 ----
    //
    // Three typed-poll-trampoline clones for the offset-map query family (§4.1). Each
    // differs from it ONLY in (a) the OperationCompletionSource<TResult> result type, (b) the
    // copy-out marshaller called on success, and (c) which container _destroy runs in
    // the finally. Every poll-trampoline correctness invariant is preserved verbatim: no-throw
    // boundary, copy-out on THIS (dispatcher) thread BEFORE _destroy, container _destroy
    // null-safe in the finally (a no-op on the failure/null path), error via
    // OperationCompletionSource<T>.Complete (which frees the error handle), per-op
    // GCHandle freed exactly once via FreeGcHandle, and RunContinuationsAsynchronously
    // (via the OperationCompletionSource<T>). The three ABI callback typedefs are
    // distinct C function-pointer types but share the (container*, error*, ud) =
    // (IntPtr, IntPtr, IntPtr) layout; each gets its own delegate type so the matching
    // NativeMethods DllImport binds a strongly-typed parameter (self-documenting; the
    // shared submit helper in NativeConsumer casts via the poll-shaped delegate).
    // beginning/end share one callback (OnLongOffsets) since they share
    // long_offsets_callback_t.

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_committed_callback_t</c>:
    /// <c>void (*)(kafka_consumer_OffsetMap_t* map, kafka_common_Error_t* error,
    /// void* user_data)</c> — the owned-handle completion shape (§B6/§B7), the
    /// <c>OffsetMap_t</c> analog of <see cref="PollCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void OffsetMapCallback(IntPtr map, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>committed_async</c> submission.
    /// Rooted for the process lifetime, so the native thunk never dangles (§B6 keep-alive).
    /// </summary>
    internal static readonly OffsetMapCallback Committed = OnCommitted;

    /// <summary>
    /// The <c>committed</c> completion trampoline — an <see cref="TypedPollCallbacks{TKey, TValue}"/> clone for the
    /// owned <c>OffsetMap_t</c>. Copies out on this (dispatcher) thread via
    /// <see cref="OffsetMapMarshal.CopyOut"/>, then the <c>finally</c> destroys the map
    /// root via the null-safe <see cref="NativeMethods.OffsetMapDestroy"/>. The map's
    /// key/value elements are <b>borrowed</b> (Category 4) and never freed — only the map
    /// root is destroyed (ffi §B2).
    /// </summary>
    private static void OnCommitted(IntPtr map, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                context.Complete(error);
            }
            else
            {
                // Success (map is a non-null owned borrow-root). Copy out on THIS
                // (dispatcher) thread; the finally then destroys the root.
                IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> marshalled = OffsetMapMarshal.CopyOut(map);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary: never unwind into native. Surface via the Task; the
            // finally still frees the map + GCHandle if we recovered the context.
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of BOTH frees, on EVERY path (§B6). Destroy is null-safe, so it
            // is a no-op when map is null (failure / inline rejection); the borrowed
            // key/value ELEMENTS are never destroyed (§B2 Category 4).
            NativeMethods.OffsetMapDestroy(map);
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_offsets_for_times_callback_t</c>:
    /// <c>void (*)(kafka_consumer_OffsetAndTimestampMap_t* map,
    /// kafka_common_Error_t* error, void* user_data)</c> — the owned-handle shape
    /// (§B6/§B7), the <c>OffsetAndTimestampMap_t</c> analog of <see cref="PollCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void OffsetAndTimestampMapCallback(IntPtr map, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>offsets_for_times_async</c>
    /// submission. Rooted for the process lifetime (§B6 keep-alive).
    /// </summary>
    internal static readonly OffsetAndTimestampMapCallback OffsetsForTimes = OnOffsetsForTimes;

    /// <summary>
    /// The <c>offsetsForTimes</c> completion trampoline — an <see cref="TypedPollCallbacks{TKey, TValue}"/> clone
    /// for the owned <c>OffsetAndTimestampMap_t</c>. Copies out on this (dispatcher)
    /// thread via <see cref="OffsetAndTimestampMapMarshal.CopyOut"/>, then the
    /// <c>finally</c> destroys the map root via the null-safe
    /// <see cref="NativeMethods.OffsetAndTimestampMapDestroy"/>. Borrowed elements are
    /// never freed (§B2 Category 4).
    /// </summary>
    private static void OnOffsetsForTimes(IntPtr map, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                context.Complete(error);
            }
            else
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> marshalled =
                    OffsetAndTimestampMapMarshal.CopyOut(map);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            context?.TrySetException(exception);
        }
        finally
        {
            NativeMethods.OffsetAndTimestampMapDestroy(map);
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_long_offsets_callback_t</c>:
    /// <c>void (*)(kafka_consumer_LongOffsetMap_t* map, kafka_common_Error_t* error,
    /// void* user_data)</c> — the owned-handle shape (§B6/§B7), <b>shared</b> by
    /// <c>beginning_offsets_async</c> and <c>end_offsets_async</c> (both return a
    /// <c>LongOffsetMap_t</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void LongOffsetMapCallback(IntPtr map, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>beginning_offsets_async</c> and
    /// <c>end_offsets_async</c> submission (they share <c>long_offsets_callback_t</c>, so
    /// <b>one</b> trampoline serves both). Rooted for the process lifetime (§B6 keep-alive).
    /// </summary>
    internal static readonly LongOffsetMapCallback LongOffsets = OnLongOffsets;

    /// <summary>
    /// The shared <c>beginningOffsets</c> / <c>endOffsets</c> completion trampoline — an
    /// <see cref="TypedPollCallbacks{TKey, TValue}"/> clone for the owned <c>LongOffsetMap_t</c>. Copies out on this
    /// (dispatcher) thread via <see cref="LongOffsetMapMarshal.CopyOut"/>, then the
    /// <c>finally</c> destroys the map root via the null-safe
    /// <see cref="NativeMethods.LongOffsetMapDestroy"/>. The map's <c>TopicPartition_t</c>
    /// keys are borrowed (Category 4) and never freed; the <c>int64</c> values are
    /// by-value scalars (nothing to free). One instance serves both queries.
    /// </summary>
    private static void OnLongOffsets(IntPtr map, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<IReadOnlyDictionary<TopicPartition, long>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<IReadOnlyDictionary<TopicPartition, long>>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                context.Complete(error);
            }
            else
            {
                IReadOnlyDictionary<TopicPartition, long> marshalled = LongOffsetMapMarshal.CopyOut(map);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            context?.TrySetException(exception);
        }
        finally
        {
            NativeMethods.LongOffsetMapDestroy(map);
            context?.FreeGcHandle();
        }
    }

    // ---- Owned-handle partition-metadata completions (ffi §B6/§B7) — M5/P5 ----
    //
    // Two more typed-poll-trampoline clones for the partition-metadata query family
    // (Category E2). Each differs from it ONLY in (a) the OperationCompletionSource<TResult> result type,
    // (b) the nested copy-out marshaller called on success (PartitionInfoListMarshal /
    // TopicPartitionInfoMapMarshal), and (c) which container _destroy runs in the finally
    // (PartitionInfoListDestroy / TopicPartitionInfoMapDestroy). Every poll-trampoline correctness
    // invariant is preserved verbatim: no-throw boundary, copy-out on THIS (dispatcher)
    // thread BEFORE _destroy, container _destroy null-safe in the finally (a no-op on the
    // failure/null path), error via OperationCompletionSource<T>.Complete (which frees the
    // error handle), per-op GCHandle freed exactly once via FreeGcHandle, and
    // RunContinuationsAsynchronously (via the OperationCompletionSource<T>). The only new
    // dimension vs the E1 offset-map clones is the DEPTH of the copy-out — the whole tree
    // (list/map → PartitionInfo → leader/replica Nodes → node strings) is copied out before
    // the single root _destroy, and NO borrowed element (PartitionInfo, Node, nested list)
    // is ever freed (Category 4). Each ABI callback typedef is a distinct C function-pointer
    // type sharing the (container*, error*, ud) = (IntPtr, IntPtr, IntPtr) layout; each gets
    // its own delegate type so the matching NativeMethods DllImport binds a strongly-typed,
    // self-documenting parameter.

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_partitions_for_callback_t</c>:
    /// <c>void (*)(kafka_consumer_PartitionInfoList_t* list,
    /// kafka_common_Error_t* error, void* user_data)</c> — the owned-handle completion
    /// shape (§B6/§B7), the <c>PartitionInfoList_t</c> analog of <see cref="PollCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void PartitionInfoListCallback(IntPtr list, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>partitions_for_async</c> submission.
    /// Rooted for the process lifetime, so the native thunk never dangles (§B6 keep-alive).
    /// </summary>
    internal static readonly PartitionInfoListCallback PartitionsFor = OnPartitionsFor;

    /// <summary>
    /// The <c>partitionsFor</c> completion trampoline — an <see cref="TypedPollCallbacks{TKey, TValue}"/> clone for
    /// the owned <c>PartitionInfoList_t</c>. Copies out the whole borrowed tree on this
    /// (dispatcher) thread via <see cref="PartitionInfoListMarshal.CopyOut"/>, then the
    /// <c>finally</c> destroys the list root via the null-safe
    /// <see cref="NativeMethods.PartitionInfoListDestroy"/>. Every <c>PartitionInfo</c> /
    /// <c>Node</c> / nested string is <b>borrowed</b> (Category 4) and never freed — only the
    /// list root is destroyed (ffi §B2).
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
            // Sole owner of BOTH frees, on EVERY path (§B6). Destroy is null-safe, so it is a
            // no-op when list is null (failure / inline rejection); the borrowed PartitionInfo
            // / Node / string ELEMENTS are never destroyed (§B2 Category 4).
            NativeMethods.PartitionInfoListDestroy(list);
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_list_topics_callback_t</c>:
    /// <c>void (*)(kafka_consumer_TopicPartitionInfoMap_t* map,
    /// kafka_common_Error_t* error, void* user_data)</c> — the owned-handle completion
    /// shape (§B6/§B7), the <c>TopicPartitionInfoMap_t</c> analog of
    /// <see cref="PollCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void TopicPartitionInfoMapCallback(IntPtr map, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>list_topics_async</c> submission.
    /// Rooted for the process lifetime, so the native thunk never dangles (§B6 keep-alive).
    /// </summary>
    internal static readonly TopicPartitionInfoMapCallback ListTopics = OnListTopics;

    /// <summary>
    /// The <c>listTopics</c> completion trampoline — an <see cref="TypedPollCallbacks{TKey, TValue}"/> clone for the
    /// owned <c>TopicPartitionInfoMap_t</c>. Copies out the whole borrowed tree (map →
    /// per-topic nested list → info → nodes) on this (dispatcher) thread via
    /// <see cref="TopicPartitionInfoMapMarshal.CopyOut"/>, then the <c>finally</c> destroys
    /// the map root via the null-safe
    /// <see cref="NativeMethods.TopicPartitionInfoMapDestroy"/>. Every topic string, nested
    /// list, <c>PartitionInfo</c>, and <c>Node</c> is <b>borrowed</b> (Category 4) and never
    /// freed — only the map root is destroyed (ffi §B2).
    /// </summary>
    private static void OnListTopics(IntPtr map, IntPtr error, IntPtr userData)
    {
        OperationCompletionSource<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                context.Complete(error);
            }
            else
            {
                IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> marshalled =
                    TopicPartitionInfoMapMarshal.CopyOut(map);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            context?.TrySetException(exception);
        }
        finally
        {
            NativeMethods.TopicPartitionInfoMapDestroy(map);
            context?.FreeGcHandle();
        }
    }

    // ---- Rebalance-listener registration (ffi §B6) — M9/P6 ----
    //
    // The one MULTI-SHOT callback family in this file. Everything above is a one-shot
    // per-operation completion whose GCHandle the trampoline itself frees; these fire N
    // times for one registration, so the trampolines NEVER free the GCHandle — that is
    // ListenerRegistration.Release(), driven only by the ABI's user_data_destroy hook
    // (P6-D3 option (a); the evidence chain is on ListenerRegistration). Freeing here
    // would be a use-after-free on fire 2..N.
    //
    // The other three differences from the completion trampolines, all forced by the ABI:
    //   * they RETURN a value — NULL for success, or an owned Error* whose ownership
    //     transfers to the core (confluent_kafka.h:200-207), so it must NOT be destroyed;
    //   * the delivered TopicPartitionList_t is owned by the callback and destroyed by
    //     TopicPartitionListMarshal.CopyOutAndDestroy (which frees it in a finally, so the
    //     handle is released exactly once even when the copy-out throws);
    //   * there is no TaskCompletionSource, so a managed exception has nowhere to surface
    //     except the returned error handle.

    /// <summary>
    /// The C signature shared by all three listener callbacks
    /// (<c>kafka_consumer_ConsumerRebalanceListener_on_partitions_{revoked,assigned,lost}_callback_t</c>):
    /// <c>kafka_common_Error_t* (*)(kafka_consumer_TopicPartitionList_t* partitions,
    /// void* user_data)</c>. The three ABI typedefs are distinct C types with an identical
    /// layout, so one delegate type binds all three parameters of
    /// <see cref="NativeMethods.ConsumerRebalanceListenerNew"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate IntPtr RebalanceListenerCallback(IntPtr partitions, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_consumer_ConsumerRebalanceListener_user_data_destroy_t</c>:
    /// <c>void (*)(void* user_data)</c> — the release hook, fired <b>exactly once</b> when
    /// the core drops its last reference to the registration, and it "may run on any thread"
    /// (<c>confluent_kafka.h:553-560</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListenerUserDataDestroyCallback(IntPtr userData);

    /// <summary>
    /// The rooted <c>on_partitions_revoked</c> thunk. <c>static readonly</c> — a per-subscribe
    /// delegate instance or an inline lambda would be collectible while the core still holds
    /// the thunk, and a registration outlives its subscribe call by definition (§B6 keep-alive).
    /// </summary>
    internal static readonly RebalanceListenerCallback PartitionsRevoked = OnPartitionsRevoked;

    /// <summary>The rooted <c>on_partitions_assigned</c> thunk (see <see cref="PartitionsRevoked"/>).</summary>
    internal static readonly RebalanceListenerCallback PartitionsAssigned = OnPartitionsAssigned;

    /// <summary>The rooted <c>on_partitions_lost</c> thunk (see <see cref="PartitionsRevoked"/>).</summary>
    internal static readonly RebalanceListenerCallback PartitionsLost = OnPartitionsLost;

    /// <summary>The rooted <c>user_data_destroy</c> thunk (see <see cref="PartitionsRevoked"/>).</summary>
    internal static readonly ListenerUserDataDestroyCallback ListenerUserDataDestroy = OnListenerUserDataDestroy;

    /// <summary>
    /// The error code a throwing listener is reported with. Python pins the observable
    /// contract as code <c>-1</c> plus the exception's message verbatim; <c>-1</c> is outside
    /// the protocol range, so the core maps it to <c>UnknownServerError</c> exactly as Java's
    /// <c>Errors.forCode</c> does.
    /// </summary>
    private const int ListenerErrorCode = -1;

    private enum RebalanceCallbackKind
    {
        Revoked,
        Assigned,
        Lost,
    }

    private static IntPtr OnPartitionsRevoked(IntPtr partitions, IntPtr userData) =>
        InvokeListener(partitions, userData, RebalanceCallbackKind.Revoked);

    private static IntPtr OnPartitionsAssigned(IntPtr partitions, IntPtr userData) =>
        InvokeListener(partitions, userData, RebalanceCallbackKind.Assigned);

    private static IntPtr OnPartitionsLost(IntPtr partitions, IntPtr userData) =>
        InvokeListener(partitions, userData, RebalanceCallbackKind.Lost);

    /// <summary>
    /// The shared listener trampoline body. Runs on the core's callback-dispatcher thread
    /// (never concurrently with another callback of the same consumer), with the rebalance —
    /// and the operation that triggered it — blocked until it returns.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Total no-throw boundary.</b> The frame above is Rust, so there is no managed caller
    /// to catch anything: an escaping exception is undefined behavior (§B6). The
    /// <c>try</c> therefore covers the copy-out, the <c>GCHandle</c> recovery <b>and</b> the
    /// user call, and the <c>catch</c> converts whatever escapes into the error handle the
    /// core expects.
    /// </para>
    /// <para>
    /// <b>The delivered list is owned by the callback</b> ("callbacks own the handles
    /// delivered to them", <c>confluent_kafka.h:193-197</c>).
    /// <see cref="TopicPartitionListMarshal.CopyOutAndDestroy"/> copies every element out and
    /// destroys the borrow-root in a <c>finally</c> — so it is freed exactly once even if the
    /// copy-out itself throws, and no borrowed pointer escapes into the managed snapshot
    /// (§B2 Category 3/4).
    /// </para>
    /// <para>
    /// <b>No <c>GCHandle.Free</c> here.</b> See the section comment above: this registration
    /// fires again.
    /// </para>
    /// </remarks>
    private static IntPtr InvokeListener(IntPtr partitions, IntPtr userData, RebalanceCallbackKind kind)
    {
        try
        {
            // Copy out + destroy the delivered borrow-root before touching anything else, so
            // the handle cannot leak down a later failure path.
            IReadOnlyCollection<TopicPartition> delivered =
                TopicPartitionListMarshal.CopyOutAndDestroy(partitions);

            IConsumerRebalanceListener listener = ListenerRegistration.FromUserData(userData).Listener;
            switch (kind)
            {
                case RebalanceCallbackKind.Revoked:
                    listener.OnPartitionsRevoked(delivered);
                    break;
                case RebalanceCallbackKind.Assigned:
                    listener.OnPartitionsAssigned(delivered);
                    break;
                default:
                    listener.OnPartitionsLost(delivered);
                    break;
            }

            // NULL is success. The core turns a non-null return into the Result::Err it sees.
            return IntPtr.Zero;
        }
        catch (Exception exception)
        {
            return ListenerError(exception);
        }
    }

    /// <summary>
    /// Converts a listener's exception into the owned <c>kafka_common_Error_t</c> the
    /// core expects back — the C equivalent of a Java listener throwing. <b>Ownership
    /// transfers to the core: the handle must not be destroyed here</b>
    /// (<c>confluent_kafka.h:200-207</c>).
    /// </summary>
    /// <remarks>
    /// Itself no-throw, because it runs from the <c>catch</c> of a no-throw boundary: if
    /// encoding or pinning the message fails it retries with a null message (explicitly
    /// allowed by the ABI, yielding an empty message) rather than downgrading a failure to
    /// success, and only reports success if even that is impossible — still preferable to
    /// unwinding into native.
    /// </remarks>
    private static IntPtr ListenerError(Exception exception)
    {
        try
        {
            // Exception.Message is non-null for every framework type, but an override could
            // return null and Utf8Marshal.Pin would then throw inside this catch path.
            string message = exception.Message ?? string.Empty;
            using (Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(message))
            {
                return NativeMethods.KafkaErrorNew(ListenerErrorCode, pin.Pointer);
            }
        }
        catch (Exception)
        {
            try
            {
                return NativeMethods.KafkaErrorNew(ListenerErrorCode, IntPtr.Zero);
            }
            catch (Exception)
            {
                return IntPtr.Zero;
            }
        }
    }

    /// <summary>
    /// The registration release hook — <b>the single sanctioned site that frees a
    /// registration's <c>GCHandle</c></b> (see <see cref="ListenerRegistration"/>). The core
    /// fires it exactly once per registration, on any thread, for every one of its five
    /// release triggers.
    /// </summary>
    private static void OnListenerUserDataDestroy(IntPtr userData)
    {
        try
        {
            ListenerRegistration.FromUserData(userData).Release();
        }
        catch (Exception)
        {
            // No-throw boundary: the caller is Rust `Drop`, on an arbitrary thread (possibly
            // the finalizer thread, when SafeConsumerHandle.ReleaseHandle runs without an
            // explicit Dispose). There is nothing to surface it through and nowhere to
            // propagate it to, so swallowing is the only safe action.
        }
    }

    // ---- Offset-commit callback registration (ffi §B6) — M9/P7 ----
    //
    // The THIRD callback family: a ONE-SHOT completion (it fires "exactly once per successful
    // call", confluent_kafka.h:2466-2467) whose GCHandle is freed by a RELEASE HOOK, not by
    // the callback. It is neither of the two families above, and copying either one is wrong:
    //
    //   * the ~8 one-shot completions free the per-op GCHandle in the trampoline's `finally`.
    //     Doing that here LEAKS, because this callback is NOT invoked when the call fails:
    //     src/ffi/consumer.rs:4001-4009 builds the adapter (which owns `user_data`) at :4004
    //     BEFORE the fallible read_offset_map at :4005, so the early return at :4007 drops it
    //     — firing the hook without ever calling the callback. The header says so outright.
    //   * the multi-shot listener fires N times; this fires at most once.
    //
    // So the hook is the SINGLE free site (the only one that runs on every path), while the
    // callback owns the two handles delivered to it. The evidence chain and the safety
    // argument (ref-counted SafeConsumerHandle + single serialised dispatcher — NOT an owned
    // Arc, which is disproved) live on CommitCallbackRegistration.
    //
    // The header also pins the ORDERING that rules the trampoline out on the SUCCESS path,
    // not just the failure one (confluent_kafka.h:524-528): the hook fires "after the commit
    // completed and the callback returned". So a trampoline-side free would release a GCHandle
    // the core is still about to hand to the hook.
    //
    // Two further differences from the completion trampolines, both forced by the ABI:
    //   * there is NO TaskCompletionSource and NO Task — this is fire-and-forget, so a managed
    //     exception has nowhere to surface. It is swallowed and traced (P7-D3 option (b)),
    //     attributed to the site that actually failed rather than blanket-blamed on the user
    //     callback (see TraceSwallowed).
    //   * the delivered OffsetMap_t is owned by the callback and destroyed via
    //     OffsetMapMarshal.CopyOutAndDestroy — the twin of the
    //     TopicPartitionListMarshal.CopyOutAndDestroy the listener trampolines use. That twin
    //     was ADDED in the M9/P7 review round: before it, OffsetMapMarshal exposed only
    //     CopyOut (which does not destroy), so copying the listener's shape verbatim leaked
    //     the map on every commit. The asymmetry was removed rather than documented around.

    /// <summary>
    /// The C signature for <c>kafka_consumer_Consumer_commit_async_callback_t</c>:
    /// <c>void (*)(kafka_consumer_OffsetMap_t* offsets, kafka_common_Error_t* error,
    /// void* user_data)</c> (<c>confluent_kafka.h:264</c>). It shares the
    /// <c>(handle*, error*, ud)</c> layout of <see cref="OffsetMapCallback"/> but is a
    /// distinct ABI typedef with a completely different contract — <c>offsets</c> is
    /// <b>always non-null</b> (it is the map the commit applied to, not a query result), the
    /// callback <b>owns both</b> delivered handles, and it carries a registration rather than
    /// a per-op completion. It gets its own delegate type so the two
    /// <c>NativeMethods.ConsumerCommitAsync*WithCallback</c> declarations bind a
    /// strongly-typed, self-documenting parameter.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CommitCallback(IntPtr offsets, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_consumer_Consumer_commit_async_user_data_destroy_t</c>:
    /// <c>void (*)(void* user_data)</c> — the release hook, fired <b>exactly once</b> after
    /// the registration is dropped, on <b>any</b> thread, and fired <b>even when the
    /// submitting call returns an error</b> (<c>confluent_kafka.h:515-537</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CommitUserDataDestroyCallback(IntPtr userData);

    /// <summary>
    /// The rooted commit-completion thunk, used whenever the caller supplied an
    /// <see cref="IOffsetCommitCallback"/>. <c>static readonly</c> — a per-call delegate
    /// instance or an inline lambda would be collectible while the core still holds the thunk,
    /// and this registration outlives its submitting call whenever the consumer is real
    /// (§B6 keep-alive).
    /// </summary>
    internal static readonly CommitCallback Commit = OnCommit;

    /// <summary>
    /// The rooted <b>discard</b> thunk — the .NET equivalent of C's
    /// <c>discard_commit_complete</c> (<c>bindings/c/grpc_server/server.cc:376-380</c>). It
    /// exists because the ABI's <c>callback</c> parameter is <b>not nullable</b> and there is
    /// no plain <c>Consumer_commit_async_offsets</c>, so Java's legal
    /// <c>commitAsync(Map, null)</c> can only be expressed by supplying a no-op that still
    /// frees the two handles it is given.
    /// </summary>
    internal static readonly CommitCallback CommitDiscard = OnCommitDiscard;

    /// <summary>The rooted <c>user_data_destroy</c> thunk (see <see cref="Commit"/>).</summary>
    internal static readonly CommitUserDataDestroyCallback CommitUserDataDestroy = OnCommitUserDataDestroy;

    /// <summary>
    /// The commit-completion trampoline. Runs on the core's callback-dispatcher thread — or
    /// <b>inline on the caller's thread</b> during the commit call when the consumer is a
    /// <c>MockConsumer</c> (<c>confluent_kafka.h:2482-2485</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Total no-throw boundary, with nowhere to surface.</b> The frame above is Rust and
    /// there is no <c>Task</c> to fault — Java's <c>onComplete</c> returns <c>void</c> and the
    /// ABI typedef returns <c>void</c>, so there is no error channel at all. An escaping
    /// exception would be undefined behavior, so everything (the copy-out, the
    /// <c>GCHandle</c> recovery <em>and</em> the user call) is caught and <b>swallowed</b>,
    /// after being written to <see cref="System.Diagnostics.Trace"/> so the failure is not
    /// strictly silent (P7-D3 option (b); Python logs and swallows, <c>consumer.py:363-372</c>).
    /// The user call is caught <em>separately</em> from everything before it, so the trace says
    /// which one failed instead of blaming the user's callback for a marshalling fault.
    /// </para>
    /// <para>
    /// <b>Both delivered handles are owned by the callback</b>
    /// (<c>confluent_kafka.h:243-248</c>). The <c>Error_t</c> is consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>, which frees it exactly once in its own
    /// <c>finally</c> and returns <see langword="null"/> on success — so it is called
    /// <b>first</b>, before anything that can fail. The <c>OffsetMap_t</c> is then handed to
    /// <see cref="OffsetMapMarshal.CopyOutAndDestroy"/>, which copies out and destroys the root
    /// in its own <c>finally</c> — leaving nothing native-backed in the snapshot handed to the
    /// user. The local ownership baton (see the body) keeps "destroyed exactly once on every
    /// path" true even on the paths that never reach that helper.
    /// </para>
    /// <para>
    /// <b>No <c>GCHandle.Free</c> here.</b> See the section comment above: on the
    /// marshal-failure path this trampoline is never called at all, so it cannot be the free
    /// site.
    /// </para>
    /// </remarks>
    private static void OnCommit(IntPtr offsets, IntPtr error, IntPtr userData)
    {
        // The map's ownership baton. It starts here and is handed to CopyOutAndDestroy — which
        // then owns the destroy on EVERY path, including a throwing copy-out. Zeroing it
        // BEFORE the call (not after) is what makes that exact: if the helper throws, it has
        // already destroyed, and the finally below must not destroy again; if we never reach
        // the helper at all (a throw from FromHandle), the baton is still held and the finally
        // releases it. So the map is destroyed exactly once on every path, provably by reading.
        IntPtr unhandedMap = offsets;
        try
        {
            // FIRST, and unconditionally: FromHandle frees the delivered error handle exactly
            // once (in its own finally) and yields null on success. Doing it before the
            // fallible copy-out is what keeps the ERROR handle from leaking if that throws.
            KafkaException? exception = KafkaException.FromHandle(error);

            // The map is always non-null here and is a Category-3 handle the callback OWNS
            // (confluent_kafka.h:243-248): copy every key/value out on THIS thread, then
            // destroy the root — both done by the helper, which is the OffsetMap twin of the
            // TopicPartitionListMarshal.CopyOutAndDestroy the listener trampolines use.
            unhandedMap = IntPtr.Zero;
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> delivered =
                OffsetMapMarshal.CopyOutAndDestroy(offsets);

            IOffsetCommitCallback callback = CommitCallbackRegistration.FromUserData(userData).Callback;
            try
            {
                callback.OnComplete(delivered, exception);
            }
            catch (Exception userFailure)
            {
                // The user callback itself threw — the case P7-D3 is actually about. Reported
                // as such, separately from a trampoline-side failure below, so the diagnostic
                // points at the right code.
                TraceSwallowed(UserCallbackSite, userFailure);
            }
        }
        catch (Exception failure)
        {
            // NOT the user callback: a failure marshalling the delivered handles or recovering
            // the registration from `user_data` (e.g. a bad GCHandle → InvalidCastException).
            // Attributing this to the user's callback would send a debugger to the wrong file.
            TraceSwallowed(TrampolineSite, failure);
        }
        finally
        {
            // Null-safe, and non-null ONLY when the baton was never handed over — i.e. we
            // never reached CopyOutAndDestroy. No double free.
            NativeMethods.OffsetMapDestroy(unhandedMap);
        }
    }

    /// <summary>
    /// The discard trampoline — frees the two delivered handles and invokes nothing, mirroring
    /// C's <c>discard_commit_complete</c>. Used for Java's <c>commitAsync(Map, null)</c>,
    /// which the ABI cannot express directly.
    /// </summary>
    /// <remarks>
    /// <paramref name="userData"/> is <see cref="IntPtr.Zero"/> by construction on this path
    /// and is deliberately never dereferenced: with no user callback there is nothing managed
    /// to root, so no <see cref="System.Runtime.InteropServices.GCHandle"/> is allocated and
    /// no release hook is registered. (This is the one place C's
    /// <c>user_data_destroy = nullptr</c> is the right model for .NET too — not because C
    /// lacks a <c>GCHandle</c>, but because on this path .NET has none either.)
    /// </remarks>
    private static void OnCommitDiscard(IntPtr offsets, IntPtr error, IntPtr userData)
    {
        try
        {
            // Both destroys are null-safe. Neither can throw, but the boundary is mandatory:
            // the frame above is Rust, so an escaping exception would be UB.
            NativeMethods.ErrorDestroy(error);
        }
        catch (Exception failure)
        {
            // There is no IOffsetCommitCallback in existence on this path, so the diagnostic
            // must not claim one threw.
            TraceSwallowed(DiscardTrampolineSite, failure);
        }
        finally
        {
            NativeMethods.OffsetMapDestroy(offsets);
        }
    }

    /// <summary>
    /// The commit-registration release hook — <b>the single sanctioned site that frees a
    /// commit registration's <c>GCHandle</c></b> (see <see cref="CommitCallbackRegistration"/>).
    /// The core fires it exactly once, on any thread, including when the submitting call
    /// returned an error and the callback never fired.
    /// </summary>
    private static void OnCommitUserDataDestroy(IntPtr userData)
    {
        try
        {
            CommitCallbackRegistration.FromUserData(userData).Release();
        }
        catch (Exception)
        {
            // No-throw boundary: the caller is Rust `Drop`, on an arbitrary thread. There is
            // nothing to surface it through, so swallowing is the only safe action.
        }
    }

    /// <summary>
    /// The <see cref="TraceSwallowed"/> site for the user's <see cref="IOffsetCommitCallback"/>
    /// throwing — the case P7-D3 is about.
    /// </summary>
    private const string UserCallbackSite = "an IOffsetCommitCallback (OnComplete) threw";

    /// <summary>
    /// The <see cref="TraceSwallowed"/> site for a failure <b>before</b> the user callback was
    /// reached: marshalling the delivered handles, or recovering the registration from
    /// <c>user_data</c>.
    /// </summary>
    private const string TrampolineSite =
        "the offset-commit completion trampoline failed before reaching the callback";

    /// <summary>
    /// The <see cref="TraceSwallowed"/> site for the callback-less discard path, where <b>no</b>
    /// <see cref="IOffsetCommitCallback"/> exists at all.
    /// </summary>
    private const string DiscardTrampolineSite =
        "the offset-commit discard trampoline (no user callback) failed";

    /// <summary>
    /// Writes a swallowed failure to <see cref="System.Diagnostics.Trace"/> — the binding's
    /// only diagnostics sink, deliberately minimal: no logging abstraction, no dependency, no
    /// public API (P7-D3 option (b)). Python's adapter logs and swallows; swallowing
    /// <em>silently</em> would match only half of that and leave a debugging cliff.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="site"/> is mandatory because this helper serves <b>three</b> catch
    /// sites, only one of which is the user's callback: a fixed "an IOffsetCommitCallback
    /// threw" message would misattribute a marshalling failure — and, on the discard path,
    /// would name a callback that does not exist. A confidently-wrong diagnostic is a worse
    /// debugging cliff than a silent one, which would defeat the point of tracing at all.
    /// </para>
    /// <para>
    /// Itself totally no-throw, because it runs from the <c>catch</c> of a no-throw boundary:
    /// a host-installed <see cref="System.Diagnostics.TraceListener"/> can throw, and
    /// diagnostics must never escalate into an unwind across the FFI boundary.
    /// </para>
    /// </remarks>
    private static void TraceSwallowed(string site, Exception exception)
    {
        try
        {
            System.Diagnostics.Trace.TraceError(
                "Confluent.Kafka: {0}; the exception was swallowed. Java's " +
                "OffsetCommitCallback.onComplete returns void, so there is no channel to report " +
                "it on. Details: {1}",
                site,
                exception);
        }
        catch (Exception)
        {
            // See the remarks: a throwing TraceListener must not become an FFI unwind.
        }
    }
}
