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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The shared walker over an admin <c>*Result_t</c> — the flattened, index-addressed,
/// fully-settled table the ABI substitutes for Java's
/// <c>Map&lt;K, KafkaFuture&lt;V&gt;&gt;</c>. It reads
/// <c>count</c> / <c>get_key(i)</c> / <c>get_error(i)</c> / <c>get_value(i)</c>,
/// copies every borrowed value out, and resolves the matching per-key
/// <see cref="System.Threading.Tasks.Task"/> on a
/// <see cref="KeyedAdminOperation{TValue}"/> — once per RPC family rather than once per
/// RPC.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two result shapes today.</b> Shape 1 (per-key value) supplies a
/// <see cref="Accessors.GetValue"/> and a value marshaller; shape 2 (per-key
/// <c>KafkaFuture&lt;Void&gt;</c>) supplies <b>neither</b>, because for those RPCs the
/// ABI exposes no <c>_get_value</c> function at all — the header states it outright:
/// "Java's per-key future is <c>KafkaFuture&lt;Void&gt;</c>, so a null error <b>is</b>
/// the success value." The remaining shapes are later phases; nothing here forecloses
/// them, and nothing here builds them speculatively.
/// </para>
/// <para>
/// ⚠ <b>The per-key error is BORROWED (ffi §B2 Category 4).</b> It is read with
/// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/> and <b>never</b> destroyed:
/// it dies with the result root, which the completion trampoline destroys exactly once.
/// Reaching for <see cref="KafkaException.FromHandle(IntPtr)"/> here — the reflex, since
/// it is the same C type used everywhere else — frees it a second time, and a double
/// free aborts the process where no managed assertion can see it. The
/// <em>callback's</em> <c>error</c> parameter is the opposite case: non-const, owned,
/// and freed with <see cref="KafkaException.FromHandle(IntPtr)"/>. Const-ness in the
/// header is the only signal.
/// </para>
/// <para>
/// <b>Values are copied out before the root dies.</b> Every <c>get_value(i)</c> is a
/// borrowed child of the same root, so the caller's marshaller must produce a fully
/// owned managed object; nothing native-backed may survive
/// <c>*Result_destroy</c> (ffi §B4 / CLAUDE.md §6.4). All strings here are the
/// NUL-terminated, callee-owned form (ffi §B3 row 2) — admin has no length-delimited
/// slices.
/// </para>
/// </remarks>
internal static class KeyedResultMarshal
{
    /// <summary><c>int32_t (*)(const *Result_t *)</c>.</summary>
    internal delegate int CountAccessor(IntPtr result);

    /// <summary>
    /// <c>const T *(*)(const *Result_t *, int32_t index)</c> — the shape shared by
    /// <c>get_key</c>, <c>get_error</c> and <c>get_value</c>.
    /// </summary>
    internal delegate IntPtr IndexedAccessor(IntPtr result, int index);

    /// <summary>
    /// One RPC's <c>*Result_t</c> accessor set. Built once per RPC as a
    /// <c>static readonly</c> field (method groups bind directly to the delegate
    /// types), so walking a result allocates no delegates.
    /// </summary>
    internal sealed class Accessors
    {
        /// <summary>
        /// Creates an accessor set. Pass <see langword="null"/> for
        /// <paramref name="getValue"/> to describe a <b>shape 2</b> (per-key void)
        /// result — those RPCs have no <c>_get_value</c> function to point at.
        /// </summary>
        internal Accessors(
            CountAccessor count,
            IndexedAccessor getKey,
            IndexedAccessor getError,
            IndexedAccessor? getValue)
        {
            Count = count;
            GetKey = getKey;
            GetError = getError;
            GetValue = getValue;
        }

        /// <summary><c>*Result_count</c>.</summary>
        internal CountAccessor Count { get; }

        /// <summary><c>*Result_get_key</c> — a borrowed, NUL-terminated UTF-8 key.</summary>
        internal IndexedAccessor GetKey { get; }

        /// <summary>
        /// <c>*Result_get_error</c> — that key's error, <b>borrowed</b>, or null when
        /// the key succeeded.
        /// </summary>
        internal IndexedAccessor GetError { get; }

        /// <summary>
        /// <c>*Result_get_value</c> — that key's value, borrowed, or null when the key
        /// failed. <see langword="null"/> for a shape-2 result, which has no such
        /// function.
        /// </summary>
        internal IndexedAccessor? GetValue { get; }
    }

    /// <summary>
    /// Walks <paramref name="result"/> and resolves every per-key awaiter on
    /// <paramref name="operation"/>. Runs on whichever thread the completion callback
    /// fired on, and must complete <b>before</b> the caller destroys the result root.
    /// </summary>
    /// <param name="result">
    /// The owned result root. Never null on the path that reaches here (a non-null
    /// callback <c>error</c> means there is no result and the caller faults every key
    /// instead).
    /// </param>
    /// <param name="accessors">That RPC's accessor set.</param>
    /// <param name="operation">The per-key bridge holding one source per requested key.</param>
    /// <param name="marshalValue">
    /// Copies one borrowed <c>get_value(i)</c> into an owned managed
    /// <typeparamref name="TValue"/>. <see langword="null"/> for a shape-2 result,
    /// where success carries no value.
    /// </param>
    /// <typeparam name="TValue">The managed per-key result type.</typeparam>
    internal static void Complete<TValue>(
        IntPtr result,
        Accessors accessors,
        KeyedAdminOperation<TValue> operation,
        Func<IntPtr, TValue>? marshalValue)
    {
        int count = accessors.Count(result);
        for (int index = 0; index < count; index++)
        {
            // Borrowed, NUL-terminated (ffi §B3 row 2) — copied out here.
            string? key = Utf8Marshal.PtrToString(accessors.GetKey(result, index));
            if (key is null)
            {
                // Defensive: guarded by `count`, so the ABI never returns null here.
                // A key we cannot name has no source to resolve; FailUncompleted then
                // faults whichever requested key went unaccounted for.
                continue;
            }

            try
            {
                IntPtr error = accessors.GetError(result, index);
                if (error != IntPtr.Zero)
                {
                    // ⚠ BORROWED — read, never destroy. See the class remarks.
                    operation.SetException(key, KafkaException.FromBorrowedHandle(error)!);
                    continue;
                }

                if (accessors.GetValue is null || marshalValue is null)
                {
                    // Shape 2: a null error IS the success value.
                    operation.CompleteWithSuccessNoValue(key);
                    continue;
                }

                operation.SetResult(key, marshalValue(accessors.GetValue(result, index)));
            }
            catch (Exception exception)
            {
                // A marshalling failure belongs to THIS key: faulting only its own Task
                // keeps every other key's outcome intact, which is the whole point of
                // per-key granularity. The trampoline's no-throw boundary still stands
                // above this as the last resort.
                operation.SetException(key, exception);
            }
        }
    }
}
