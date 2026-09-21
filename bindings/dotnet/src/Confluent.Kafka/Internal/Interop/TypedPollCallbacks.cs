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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The <b>generic</b> typed-poll completion trampoline (PLAN M6/P1b §5) — the
/// <c>OnPoll&lt;TKey, TValue&gt;</c> analog of the void / scalar / owned-handle bridges in
/// <see cref="ConsumerCallbacks"/>. It supplies the rooted <see cref="ConsumerCallbacks.PollCallback"/>
/// that <c>poll_async</c> is submitted with, and on success runs the <b>typed copy-out on
/// the core's foreign dispatcher thread</b> (deserializing each record's key/value via
/// <see cref="ConsumerRecordsMarshal.CopyOut"/>) before the batch is destroyed.
/// </summary>
/// <remarks>
/// <para>
/// <b>One rooted delegate per closed generic type.</b> <see cref="Poll"/> is a
/// <c>static readonly</c> field of <c>TypedPollCallbacks&lt;TKey, TValue&gt;</c>, so each
/// instantiation (e.g. <c>&lt;byte[], byte[]&gt;</c>, <c>&lt;string, long&gt;</c>) has its
/// own process-lifetime-rooted delegate — the native thunk never dangles (ffi §B6
/// keep-alive). The per-op serdes are recovered from the
/// <see cref="TypedPollCompletionSource{TKey, TValue}"/> the <c>GCHandle</c> roots (a static
/// delegate cannot capture them).
/// </para>
/// <para>
/// <b>Every invariant of the owned-handle bridge is preserved verbatim</b> (ffi §B6/§B7):
/// the body is a strict <b>no-throw boundary</b> (it runs on the foreign dispatcher thread,
/// so an escaping managed exception would be UB — including a
/// <see cref="SerializationException"/> from a user deserializer, which
/// <see cref="ConsumerRecordsMarshal.CopyOut"/> already wraps and which is caught here as the
/// final backstop and surfaced as a faulted <c>Task</c>); the copy-out runs on THIS
/// (dispatcher) thread BEFORE the batch <c>_destroy</c>; the batch is destroyed exactly once
/// via the null-safe <see cref="NativeMethods.ConsumerRecordsDestroy"/> in the
/// <c>finally</c> (a no-op on the failure / inline-rejection null path); the
/// <c>Error</c> on failure is freed inside
/// <see cref="OperationCompletionSource{TResult}.Complete(IntPtr)"/>; and the per-op
/// <see cref="GCHandle"/> is freed exactly once via
/// <see cref="OperationCompletionSource{TResult}.FreeGcHandle"/>. The awaiter's continuation
/// runs off-thread (<c>RunContinuationsAsynchronously</c>, from the base).
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
internal static class TypedPollCallbacks<TKey, TValue>
{
    /// <summary>
    /// The single rooted <see cref="ConsumerCallbacks.PollCallback"/> for this closed
    /// generic type, passed to every <c>poll_async</c> submission for a
    /// <c>&lt;TKey, TValue&gt;</c> consumer. Rooted for the process lifetime (§B6 keep-alive).
    /// </summary>
    internal static readonly ConsumerCallbacks.PollCallback Poll = OnPoll;

    private static void OnPoll(IntPtr records, IntPtr error, IntPtr userData)
    {
        TypedPollCompletionSource<TKey, TValue>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (TypedPollCompletionSource<TKey, TValue>)handle.Target!;
            if (error != IntPtr.Zero)
            {
                // Failure (records is null). Complete maps to KafkaException /
                // OperationCanceledException and frees the error handle via FromHandle.
                context.Complete(error);
            }
            else
            {
                // Success (records is a non-null owned borrow-root). Deserialize + copy out
                // on THIS (dispatcher) thread, then the finally destroys the batch — the
                // §6.4 copy-out default. CopyOut wraps any user-deserializer throw in a
                // SerializationException (PLAN §6); should one still escape, the catch below
                // faults the Task rather than unwinding into native.
                ConsumerRecords<TKey, TValue> marshalled = ConsumerRecordsMarshal.CopyOut(
                    records, context.KeyDeserializer, context.ValueDeserializer);
                context.CompleteWithResult(marshalled);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary (foreign dispatcher thread): never unwind into native.
            // Surface via the Task; the finally still frees the batch + GCHandle if we
            // recovered the context. This is where a SerializationException from the
            // copy-out faults the awaiting poll Task (PLAN §6, the dispatcher-thread safety).
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of BOTH frees, on EVERY path (ffi §B6). Destroy is null-safe, so it
            // is a no-op when records is null (failure / inline rejection).
            NativeMethods.ConsumerRecordsDestroy(records);
            context?.FreeGcHandle();
        }
    }
}
