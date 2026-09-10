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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The shared walker over an admin <c>*Result_t</c> — the flattened, index-addressed,
/// fully-settled table the ABI substitutes for Java's
/// <c>Map&lt;K, KafkaFuture&lt;V&gt;&gt;</c>. It reads <c>count</c> plus the RPC's own
/// key, value and error readers, copies every borrowed value out, and resolves the
/// awaiters on an <see cref="AdminOperation"/> — once per RPC family rather than once per
/// RPC.
/// </summary>
/// <remarks>
/// <para>
/// <b>Three result shapes, and each one is named by the method it goes through.</b>
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Shape 1</b> — a per-key value: <c>count</c> / <c>get_key</c> / <c>get_value</c> /
/// <c>get_error</c>. <c>createTopics</c>, <c>describeTopics</c>. Goes through
/// <see cref="Complete{TKey, TValue}(IntPtr, Accessors, KeyedAdminOperation{TKey, TValue}, Func{IntPtr, int, TKey}, Func{IntPtr, int, TValue})"/>.
/// </item>
/// <item>
/// <b>Shape 2</b> — a per-key <c>KafkaFuture&lt;Void&gt;</c>: the ABI exposes <b>no</b>
/// <c>_get_value</c> at all, and the header states it outright — "Java's per-key future
/// is <c>KafkaFuture&lt;Void&gt;</c>, so a null error <b>is</b> the success value".
/// <c>deleteTopics</c>, <c>createPartitions</c>. Goes through
/// <see cref="Complete{TKey}(IntPtr, Accessors, VoidKeyedAdminOperation{TKey}, Func{IntPtr, int, TKey})"/>,
/// which has <b>no value parameter at all</b>.
/// </item>
/// <item>
/// <b>Shape 3</b> — one aggregate future over the whole map, and structurally <b>no</b>
/// per-key error: <c>kafka_admin_ListTopicsResult_t</c> declares no <c>get_error</c>
/// whatsoever, so either the call fails (through the callback's own <c>error</c>) or the
/// whole map succeeds. Goes through
/// <see cref="CompleteAggregate{TKey, TValue}"/>, which has <b>no error parameter at
/// all</b>.
/// </item>
/// <item>
/// <b>Sub-shape 3b</b> — one aggregate future over an ordered <b>collection</b>, with no
/// per-key error <em>and no key at all</em>: <c>kafka_admin_ListConfigResourcesResult_t</c>
/// exposes <c>count</c> / <c>get_type</c> / <c>get_name</c> / <c>destroy</c>, and
/// <c>kafka_admin_ListClientMetricsResourcesResult_t</c> only <c>count</c> /
/// <c>get_name</c> / <c>destroy</c>. Java's accessors are
/// <c>KafkaFuture&lt;Collection&lt;ConfigResource&gt;&gt;</c>
/// (<c>ListConfigResourcesResult.java:42</c>) and
/// <c>KafkaFuture&lt;Collection&lt;ClientMetricsResourceListing&gt;&gt;</c>
/// (<c>ListClientMetricsResourcesResult.java:45</c>) — a LIST, not a map. Goes through
/// <see cref="CompleteList{TValue}"/>, which has <b>no accessor set, no key reader and no
/// error channel</b>.
/// </item>
/// </list>
/// <para>
/// ⚠ <b>Neither the key nor the value is part of <see cref="Accessors"/>, and that is
/// deliberate — neither is universal.</b> The obvious key seam (parse a <c>string</c>
/// read from <c>get_key(i)</c>) fits every RPC keyed by a name or a base64 id and
/// <em>cannot</em> fit <c>deleteRecords</c>: <c>kafka_admin_DeleteRecordsResult_t</c>
/// declares no <c>get_key</c> whatsoever, and its key is composed from two accessors
/// (<c>get_topic(i)</c> and <c>get_partition(i)</c>). The obvious value seam (a pointer
/// to a borrowed child) has the identical problem on the other axis: the same result's
/// value is an <b>inline scalar</b>, <c>int64_t …_get_low_watermark(result, i)</c>, not a
/// child handle. So both are read by a <c>Func&lt;IntPtr, int, T&gt;</c> over the result
/// handle and the index, and <see cref="Accessors"/> carries only what every keyed shape
/// really has in common: <c>count</c> and <c>get_error</c>. Each reader is expected to be
/// a <c>static readonly</c> field beside its accessor set, so walking a result allocates
/// no delegates.
/// </para>
/// <para>
/// ⚠ <b>There is no nullable value channel, and that is the point (M15/P2b).</b> P2a left
/// the value axis pointer-shaped, with <c>getValue: null</c> meaning "shape 2" — so an RPC
/// whose value is an inline scalar could be described by nulling it out, and the walker
/// would have taken the shape-2 branch and <em>silently discarded the number</em>: no
/// exception, no failing test, a wrong answer handed to the caller. That misuse is now
/// <b>unrepresentable</b> rather than merely warned about. There is no <c>null</c> to
/// pass: the value reader is required on the value-carrying overload, and shape 2 is
/// selected by calling an overload that has no value parameter and accepts only a
/// <see cref="VoidKeyedAdminOperation{TKey}"/>. A value-carrying operation cannot be
/// passed to it, and a value-less one cannot reach the other overload without a reader.
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
/// <b>Values are copied out before the root dies.</b> Every value a reader produces is
/// derived from the same root, so the reader must produce a fully owned managed object;
/// nothing native-backed may survive <c>*Result_destroy</c> (ffi §B4 / CLAUDE.md §6.4).
/// All strings here are the NUL-terminated, callee-owned form (ffi §B3 row 2) — admin has
/// no length-delimited slices.
/// </para>
/// </remarks>
internal static class KeyedResultMarshal
{
    /// <summary><c>int32_t (*)(const *Result_t *)</c>.</summary>
    internal delegate int CountAccessor(IntPtr result);

    /// <summary>
    /// <c>const T *(*)(const *Result_t *, int32_t index)</c> — the shape of
    /// <c>get_error</c>, and of the pointer-returning <c>get_key</c> / <c>get_value</c>
    /// an individual RPC's reader may call.
    /// </summary>
    internal delegate IntPtr IndexedAccessor(IntPtr result, int index);

    /// <summary>
    /// The success token for a shape-2 walk. Hoisted to a single instance so the
    /// value-less overload allocates no delegate, and expressed as a reader like every
    /// other value so that "success carries no value" is a value the walker
    /// <em>produces</em> rather than a branch it <em>infers from a null</em>.
    /// </summary>
    private static readonly Func<IntPtr, int, bool> s_voidSuccess = static (result, index) => true;

    /// <summary>
    /// One RPC's <b>universal</b> <c>*Result_t</c> accessors. Built once per RPC as a
    /// <c>static readonly</c> field (method groups bind directly to the delegate
    /// types), so walking a result allocates no delegates.
    /// </summary>
    /// <remarks>
    /// The <b>key</b> and <b>value</b> accessors are deliberately absent — see the
    /// reader note in the class remarks. Only <c>count</c> and <c>get_error</c> are
    /// common to every <em>keyed</em> shape, and shape 3 does not use this type at all
    /// because it has no <c>get_error</c> either.
    /// </remarks>
    internal sealed class Accessors
    {
        /// <summary>Creates an accessor set for a keyed result.</summary>
        internal Accessors(CountAccessor count, IndexedAccessor getError)
        {
            Count = count;
            GetError = getError;
        }

        /// <summary><c>*Result_count</c>.</summary>
        internal CountAccessor Count { get; }

        /// <summary>
        /// <c>*Result_get_error</c> — that key's error, <b>borrowed</b>, or null when
        /// the key succeeded. Required: every keyed shape has one, and a shape that does
        /// not (shape 3) goes through <see cref="CompleteAggregate{TKey, TValue}"/>,
        /// whose signature has no error channel to leave null.
        /// </summary>
        internal IndexedAccessor GetError { get; }
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
    /// <b>Shape 1 and the inline-scalar sub-shape.</b> Walks <paramref name="result"/> and
    /// resolves every per-key awaiter on <paramref name="operation"/>, reading each key's
    /// own value. Runs on whichever thread the completion callback fired on, and must
    /// complete <b>before</b> the caller destroys the result root.
    /// </summary>
    /// <param name="result">
    /// The owned result root. Never null on the path that reaches here (a non-null
    /// callback <c>error</c> means there is no result and the caller faults every key
    /// instead).
    /// </param>
    /// <param name="accessors">That RPC's universal accessors.</param>
    /// <param name="operation">The per-key bridge holding one source per requested key.</param>
    /// <param name="readKey">
    /// Reads the key at one index straight out of the result root — see the reader note in
    /// the class remarks.
    /// </param>
    /// <param name="readValue">
    /// Reads the value at one index straight out of the result root and copies it into an
    /// owned managed <typeparamref name="TValue"/>. <b>Required</b>: whether the value is
    /// a borrowed child handle (<c>get_value(i)</c> then a copy-out) or an inline scalar
    /// (<c>get_low_watermark(i)</c>) is the reader's business, not the walker's. See the
    /// no-nullable-value-channel note in the class remarks.
    /// </param>
    /// <typeparam name="TKey">The managed per-key key type.</typeparam>
    /// <typeparam name="TValue">The managed per-key result type.</typeparam>
    internal static void Complete<TKey, TValue>(
        IntPtr result,
        Accessors accessors,
        KeyedAdminOperation<TKey, TValue> operation,
        Func<IntPtr, int, TKey> readKey,
        Func<IntPtr, int, TValue> readValue)
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
                    //
                    // ⚠ This — not any in-band sentinel — is the authoritative
                    // success/failure signal. `deleteRecords` documents "-1 if that
                    // partition failed", but the very same sentence gives -1 a second
                    // meaning ("or `index` is out of range"), and -1 is also a legitimate
                    // low watermark. So the sentinel is read as a value, never as a
                    // verdict: a -1 with a NULL error is a SUCCESS carrying -1.
                    operation.SetException(key, KafkaException.FromBorrowedHandle(error)!);
                    continue;
                }

                operation.SetResult(key, readValue(result, index));
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

    /// <summary>
    /// <b>Shape 2 — the per-key <c>KafkaFuture&lt;Void&gt;</c> form.</b> Identical walk,
    /// except that a null per-key error <em>is</em> the success value: these RPCs have no
    /// <c>_get_value</c> function to point at.
    /// </summary>
    /// <remarks>
    /// This overload exists so that "this result has no per-key value" is stated by
    /// <em>which method you call</em>, not by nulling a parameter out. It takes a
    /// <see cref="VoidKeyedAdminOperation{TKey}"/> rather than the base type, so a
    /// value-carrying operation cannot be routed here and have its value dropped — the
    /// exact silent failure P2a's nullable value channel allowed.
    /// </remarks>
    /// <param name="result">The owned result root.</param>
    /// <param name="accessors">That RPC's universal accessors.</param>
    /// <param name="operation">The per-key void bridge.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <typeparam name="TKey">The managed per-key key type.</typeparam>
    internal static void Complete<TKey>(
        IntPtr result,
        Accessors accessors,
        VoidKeyedAdminOperation<TKey> operation,
        Func<IntPtr, int, TKey> readKey)
        where TKey : notnull =>
        Complete(result, accessors, operation, readKey, s_voidSuccess);

    /// <summary>
    /// <b>Shape 3 — one aggregate future over the whole map.</b> Builds the entire
    /// <c>Map&lt;K, V&gt;</c> out of the result table and resolves the operation's single
    /// awaiter with it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>There is no per-key error parameter, because the ABI has no per-key error
    /// function.</b> <c>kafka_admin_ListTopicsResult_t</c> exposes <c>count</c> /
    /// <c>get_key</c> / <c>get_value</c> / <c>destroy</c> and nothing else — that absence
    /// is the ABI stating shape 3's semantics: either the call fails, through the
    /// callback's own <c>error</c> parameter, or the whole map succeeds. Expressing that
    /// by leaving a nullable <c>getError</c> null would re-create, on the error axis, the
    /// null-as-shape-discriminator hazard this phase removed from the value axis; giving
    /// the shape its own signature states it structurally instead.
    /// </para>
    /// <para>
    /// <b>Any failure faults the whole task.</b> There is no per-key channel to fault
    /// into, so a throw from either reader propagates to the trampoline's no-throw
    /// boundary, which faults the one awaiter. That is Java's shape: a single
    /// <c>KafkaFuture&lt;Map&lt;…&gt;&gt;</c>.
    /// </para>
    /// </remarks>
    /// <param name="result">The owned result root.</param>
    /// <param name="count">That RPC's <c>*Result_count</c>.</param>
    /// <param name="operation">The single-awaiter bridge.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="readValue">That RPC's value reader.</param>
    /// <param name="keyComparer">
    /// The comparer the assembled map is keyed by — required for the same reason
    /// <see cref="KeyedAdminOperation{TKey, TValue}"/>'s is: so the map and any view
    /// derived from it cannot disagree about key identity.
    /// </param>
    /// <typeparam name="TKey">The managed key type.</typeparam>
    /// <typeparam name="TValue">The managed value type.</typeparam>
    internal static void CompleteAggregate<TKey, TValue>(
        IntPtr result,
        CountAccessor count,
        SingleAdminOperation<IReadOnlyDictionary<TKey, TValue>> operation,
        Func<IntPtr, int, TKey> readKey,
        Func<IntPtr, int, TValue> readValue,
        IEqualityComparer<TKey> keyComparer)
        where TKey : notnull
    {
        int total = count(result);
        Dictionary<TKey, TValue> entries =
            new Dictionary<TKey, TValue>(Math.Max(total, 0), keyComparer);
        for (int index = 0; index < total; index++)
        {
            // Add, not the indexer: the ABI's keys come from a Map and are documented
            // sorted, so a duplicate is a core contract violation. Faulting the one
            // awaiter loudly beats silently collapsing two entries into one.
            entries.Add(readKey(result, index), readValue(result, index));
        }

        operation.SetResult(entries);
    }

    /// <summary>
    /// <b>Sub-shape 3b — one aggregate future over an ordered collection.</b> Builds the
    /// whole <c>Collection&lt;V&gt;</c> out of the result table and resolves the
    /// operation's single awaiter with it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Its own callable, with no <see cref="Accessors"/>, no key reader and no error
    /// channel — because the ABI has none of the three.</b> This is P2b's general rule
    /// applied once more: when a defect would be "the shape is encoded in whether a field
    /// is null", make each shape a <em>distinct callable</em> rather than adding another
    /// nullable discriminator. So <see cref="Accessors"/> is untouched by this addition —
    /// it still carries exactly <c>count</c> plus a <b>required</b> <c>getError</c>, and
    /// gaining a nullable field to spell "no key" or "no error" would re-open, on two more
    /// axes, the hazard P2b removed from the value axis.
    /// </para>
    /// <para>
    /// <b>Why <see cref="CompleteAggregate{TKey, TValue}"/> cannot serve.</b> It builds a
    /// <c>Dictionary&lt;TKey, TValue&gt;</c>, and these results have no key: Java's
    /// accessors yield a <c>Collection</c>. Forcing a map — say
    /// <c>IReadOnlyDictionary&lt;ConfigResource, bool&gt;</c> — would invent a public
    /// surface Java does not have (<c>definition-of-done.md</c> §7).
    /// <c>listClientMetricsResources</c> makes that plainest: its only per-index accessor
    /// is <c>get_name(i)</c>, and the listing <em>is</em> the name.
    /// </para>
    /// <para>
    /// <b>Any failure faults the one task.</b> There is no per-key channel to fault into,
    /// so a throw from <paramref name="readValue"/> propagates to the trampoline's
    /// no-throw boundary, which faults the single awaiter. That is Java's shape: one
    /// <c>KafkaFuture&lt;Collection&lt;…&gt;&gt;</c>.
    /// </para>
    /// <para>
    /// <b>Order is preserved exactly as the ABI delivers it.</b> The header documents
    /// <c>listConfigResources</c> entries "sorted by <c>(type id, name)</c>" and
    /// <c>listClientMetricsResources</c> "sorted by name". Java returns a
    /// <c>Collection</c>, so order is not part of the contract — but neither shuffle it
    /// nor sort it again.
    /// </para>
    /// </remarks>
    /// <param name="result">The owned result root.</param>
    /// <param name="count">That RPC's <c>*Result_count</c>.</param>
    /// <param name="operation">The single-awaiter bridge.</param>
    /// <param name="readValue">
    /// Reads the element at one index straight out of the result root and copies it into an
    /// owned managed <typeparamref name="TValue"/> — the same <c>(result, index)</c> reader
    /// shape the keyed overloads use, so an element assembled from several accessors
    /// (<c>get_type(i)</c> + <c>get_name(i)</c>) is expressible.
    /// </param>
    /// <typeparam name="TValue">The managed element type.</typeparam>
    internal static void CompleteList<TValue>(
        IntPtr result,
        CountAccessor count,
        SingleAdminOperation<IReadOnlyCollection<TValue>> operation,
        Func<IntPtr, int, TValue> readValue)
    {
        int total = count(result);
        List<TValue> entries = new List<TValue>(Math.Max(total, 0));
        for (int index = 0; index < total; index++)
        {
            entries.Add(readValue(result, index));
        }

        operation.SetResult(entries);
    }
}
