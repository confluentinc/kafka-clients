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
/// <c>count</c> / <c>get_error(i)</c> / <c>get_value(i)</c> plus the RPC's own key
/// reader, copies every borrowed value out, and resolves the matching per-key
/// <see cref="System.Threading.Tasks.Task"/> on a
/// <see cref="KeyedAdminOperation{TKey, TValue}"/> — once per RPC family rather than once
/// per RPC.
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
/// ⚠ <b>The key is read by a delegate over <c>(result, index)</c>, and that is
/// deliberate.</b> The obvious seam — parse a <c>string</c> the walker read from
/// <c>get_key(i)</c> — fits every RPC bound so far and <em>cannot</em> fit
/// <c>deleteRecords</c>: <c>kafka_admin_DeleteRecordsResult_t</c> declares no
/// <c>get_key</c> whatsoever, and its key is composed from two accessors
/// (<c>get_topic(i)</c> and <c>get_partition(i)</c>). Because <c>get_key</c> is therefore
/// not universal, it is <b>not</b> part of <see cref="Accessors"/> at all; the key reader
/// is the single place a key comes from. A reader is expected to be a
/// <c>static readonly</c> field beside its accessor set, so walking a result allocates no
/// delegates.
/// </para>
/// <para>
/// ⚠ <b>Known limitation — the generalization above was applied to the KEY axis only.
/// The VALUE axis is still pointer-shaped.</b> <see cref="Accessors.GetValue"/> is an
/// <see cref="IndexedAccessor"/> (it returns an <see cref="IntPtr"/>) and
/// <c>Complete</c>'s <c>marshalValue</c> is <c>Func&lt;IntPtr, TValue&gt;</c>, so both can
/// only point at an accessor that returns a <em>pointer</em> to a borrowed child. A result
/// whose per-key value is an <b>inline scalar</b> cannot be described:
/// <c>kafka_admin_DeleteRecordsResult_t</c>'s value is
/// <c>int64_t …_get_low_watermark(result, i)</c>, not a child handle. Passing
/// <c>getValue: null</c> is <b>not</b> an escape — the walker reads a null
/// <see cref="Accessors.GetValue"/> as shape 2 and calls
/// <c>CompleteWithSuccessNoValue</c>, which would discard that watermark silently rather
/// than fail. The fix is a value reader over <c>(result, index)</c> symmetric with the key
/// reader; unlike the key change it is purely <b>additive</b> (a new overload, no existing
/// call site touched), which is why it is left to the phase that binds such an RPC instead
/// of being built here speculatively.
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
    /// <remarks>
    /// The <b>key</b> accessor is deliberately absent — see the key-reader note in the
    /// class remarks. Only <c>count</c>, <c>get_error</c> and the optional
    /// <c>get_value</c> are universal across the keyed shapes.
    /// </remarks>
    internal sealed class Accessors
    {
        /// <summary>
        /// Creates an accessor set. Pass <see langword="null"/> for
        /// <paramref name="getValue"/> to describe a <b>shape 2</b> (per-key void)
        /// result — those RPCs have no <c>_get_value</c> function to point at.
        /// </summary>
        internal Accessors(CountAccessor count, IndexedAccessor getError, IndexedAccessor? getValue)
        {
            Count = count;
            GetError = getError;
            GetValue = getValue;
        }

        /// <summary><c>*Result_count</c>.</summary>
        internal CountAccessor Count { get; }

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
        /// <remarks>
        /// Being an <see cref="IndexedAccessor"/> this can only name a <em>pointer</em>-
        /// returning accessor, and <see langword="null"/> here means shape 2 — so an
        /// inline-scalar value is not expressible on this axis and must not be described
        /// by leaving this null. See the value-axis note in the class remarks.
        /// </remarks>
        internal IndexedAccessor? GetValue { get; }
    }

    /// <summary>
    /// Copies a borrowed, NUL-terminated <c>const char*</c> key out into an owned
    /// <see cref="string"/> (ffi §B3 row 2), rejecting a null pointer rather than
    /// returning one.
    /// </summary>
    /// <param name="key">The borrowed key pointer from that RPC's key accessor.</param>
    /// <returns>The owned key.</returns>
    /// <exception cref="KafkaException">
    /// The ABI produced no key for an index inside its own <c>count</c>. Unreachable in
    /// practice; throwing rather than returning <see langword="null"/> is what lets the
    /// walker treat "no key" uniformly for every <c>TKey</c>, including value types where
    /// <see langword="null"/> is not expressible.
    /// </exception>
    internal static string ReadStringKey(IntPtr key) =>
        Utf8Marshal.PtrToString(key)
        ?? throw new KafkaException("The admin result produced no key for an index within its own count.");

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
    /// <param name="readKey">
    /// Reads the key at one index straight out of the result root — see the key-reader
    /// note in the class remarks.
    /// </param>
    /// <param name="marshalValue">
    /// Copies one borrowed <c>get_value(i)</c> into an owned managed
    /// <typeparamref name="TValue"/>. <see langword="null"/> for a shape-2 result,
    /// where success carries no value.
    /// </param>
    /// <typeparam name="TKey">The managed per-key key type.</typeparam>
    /// <typeparam name="TValue">The managed per-key result type.</typeparam>
    internal static void Complete<TKey, TValue>(
        IntPtr result,
        Accessors accessors,
        KeyedAdminOperation<TKey, TValue> operation,
        Func<IntPtr, int, TKey> readKey,
        Func<IntPtr, TValue>? marshalValue)
        where TKey : notnull
    {
        int count = accessors.Count(result);
        for (int index = 0; index < count; index++)
        {
            TKey key;
            try
            {
                key = readKey(result, index);
            }
            catch (Exception)
            {
                // Defensive: the loop is bounded by `count`, so the ABI always has a key
                // here. A key we cannot name has no source to resolve, and attributing the
                // failure to some *other* key would be worse than not attributing it;
                // FailUncompleted then faults whichever requested key went unaccounted for.
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
