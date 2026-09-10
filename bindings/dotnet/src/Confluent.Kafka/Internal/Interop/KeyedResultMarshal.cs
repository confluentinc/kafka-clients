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
/// <b>Four entries — three shapes plus a sub-shape — each named by the method it goes
/// through.</b>
/// </para>
/// <para>
/// ⚠⚠ <b>Membership is decided by the JAVA RETURN TYPE, never by the ABI accessor set
/// (M15/P4, finding 70.1).</b> The header decides the <em>mechanics</em> — which accessor
/// to call, and who owns what it returns. Java decides the <em>shape</em> — how many
/// awaitables there are and what a per-key outcome means. Each bullet below therefore
/// leads with Java's accessor; where an ABI signature is mentioned it is a <em>typical</em>
/// consequence, not the criterion, and the known counter-examples are named so that no
/// reader can generalize from one member again. That generalization is the specific
/// mistake this block used to invite: the shape-3 bullet was written when
/// <c>listTopics</c> was its only member, so a fact about <em>that result</em> read as a
/// rule about <em>the shape</em> — and pointed at the correction M15/P4 measured at 10
/// failing tests.
/// </para>
/// <para>
/// ⚠⚠ <b>Read the future the Java result HOLDS, not one a public accessor derives.</b>
/// The two differ in exactly one bound result, and it is the first one in the list:
/// <c>CreateTopicsResult</c> stores
/// <c>Map&lt;String, KafkaFuture&lt;TopicMetadataAndConfig&gt;&gt;</c>
/// (<c>CreateTopicsResult.java:33</c>) — shape 1 — while <c>values()</c> publishes
/// <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c> narrowed by
/// <c>thenApply(v -&gt; null)</c> (<c>:43-45</c>), which reads as the shape-2 signature.
/// The stored field decides: <c>createTopics</c> routes through
/// <see cref="Complete{TKey, TValue}(IntPtr, Accessors, KeyedAdminOperation{TKey, TValue}, Func{IntPtr, int, TKey}, Func{IntPtr, int, TValue})"/>,
/// and this binding mirrors Java's own erasure at the same place — its public
/// <c>Values</c> is likewise an <c>IReadOnlyDictionary&lt;string, Task&gt;</c>, while
/// <c>config()</c> / <c>topicId()</c> / <c>numPartitions()</c> /
/// <c>replicationFactor()</c> project from the value the future really carries.
/// <b>Swept, so this need not be re-derived:</b> of the 16 bound Java result classes, six
/// use <c>thenApply</c>, and in the other five it builds an aggregate or a projection
/// (<c>allTopicNames</c>, <c>all</c>, <c>allDescriptions</c>, <c>listings</c>,
/// <c>names</c>) — never a <em>per-key</em> accessor whose element type differs from the
/// stored future. So this is one known member, not a family; expect the family to grow
/// with the group RPCs.
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Shape 1</b> — Java gives <b>one future per key, carrying a value</b>
/// (<c>Map&lt;K, KafkaFuture&lt;V&gt;&gt;</c> with a non-<c>Void</c> <c>V</c>). Goes
/// through
/// <see cref="Complete{TKey, TValue}(IntPtr, Accessors, KeyedAdminOperation{TKey, TValue}, Func{IntPtr, int, TKey}, Func{IntPtr, int, TValue})"/>.
/// ⚠ Do <b>not</b> read this as the accessor set <c>count</c> / <c>get_key</c> /
/// <c>get_value</c> / <c>get_error</c>. <b>Declaring a <c>get_key</c> is the MINORITY
/// case: 2 of the 7 bound shape-1 results do</b> (<c>createTopics</c>,
/// <c>describeTopics</c>) and five do not — <c>deleteRecords</c> (a composite
/// <c>get_topic</c>/<c>get_partition</c> key, and no <c>get_value</c> either: its value
/// is an inline <c>int64_t</c>), <c>describeConfigs</c> (<c>get_key_type</c> +
/// <c>get_key_name</c>), both log-dir results, and <c>listOffsets</c> (a composite
/// <c>get_topic</c>/<c>get_partition</c> key). Counted from the header, not recalled —
/// and recounted when M15/P4 Stage 2 added the seventh member.
/// ⚠ The shape-1 bullet is also where the stored-versus-derived future matters — see
/// <c>createTopics</c> in the paragraph above.
/// </item>
/// <item>
/// <b>Shape 2</b> — the per-key future Java <b>holds</b> is
/// <c>KafkaFuture&lt;Void&gt;</c>, so success carries nothing and a null per-key error
/// <em>is</em> the success value. ⚠ "Holds", not "publishes":
/// <c>CreateTopicsResult.values()</c> has this exact signature and is <b>shape 1</b> —
/// see the stored-versus-derived paragraph above. Goes through
/// <see cref="Complete{TKey}(IntPtr, Accessors, VoidKeyedAdminOperation{TKey}, Func{IntPtr, int, TKey})"/>,
/// which has <b>no value parameter at all</b>. The ABI agrees today — no shape-2 result
/// declares a <c>_get_value</c>, and the header says why: "Java's per-key future is
/// <c>KafkaFuture&lt;Void&gt;</c>, so a null error <b>is</b> the success value" — but that
/// is the header <em>restating Java</em>, not an independent test to apply.
/// </item>
/// <item>
/// <b>Shape 3</b> — Java gives <b>one</b> future over the whole map
/// (<c>KafkaFuture&lt;Map&lt;K, V&gt;&gt;</c>), so <b>no per-key outcome faults
/// anything</b>: either the call fails, through the callback's own <c>error</c>, or the
/// whole map succeeds. Goes through <see cref="CompleteAggregate{TKey, TValue}"/>, which
/// has <b>no error parameter at all</b>.
/// ⚠⚠ <b>A member MAY declare <c>get_error</c> and still belong here.</b>
/// <c>listTopics</c> declares none; <c>electLeaders</c> declares
/// <c>kafka_admin_ElectLeadersResult_get_error</c> and is still shape 3, because Java's
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;&gt;</c>
/// (<c>ElectLeadersResult.java:36, :47</c>) makes that accessor the map's <em>value</em> —
/// it is passed as <c>readValue</c>. Routing it onto the shape-2 overload on the strength
/// of its accessor set compiles, and was measured at <b>10</b> failing tests. See
/// <c>AdminCallbacks.ElectLeadersOptionalError</c>.
/// </item>
/// <item>
/// <b>Sub-shape 3b</b> — Java gives one future over a <b>collection</b>
/// (<c>KafkaFuture&lt;Collection&lt;V&gt;&gt;</c>: <c>ListConfigResourcesResult.java:42</c>,
/// <c>ListClientMetricsResourcesResult.java:45</c>) — a LIST, not a map, so there is no key
/// to report an outcome against. Goes through <see cref="CompleteList{TValue}"/>, which
/// has <b>no accessor set, no key reader and no error channel</b>.
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
/// header is the only signal for <b>which of the two to call</b>.
/// </para>
/// <para>
/// ⚠ <b>Const-ness answers ownership and nothing else.</b> It does not say whether that
/// error is a <em>fault</em> or a <em>value</em> — both are <c>const</c>, and the two P4
/// results that differ on exactly this question expose byte-identical accessor sets. That
/// second question is answered by the Java return type; see the shape-3 bullet above.
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
    /// ⚠ <b>There is no per-key error parameter, because in this shape no per-key outcome
    /// faults anything</b> — either the call fails, through the callback's own
    /// <c>error</c> parameter, or the whole map succeeds. Expressing that by leaving a
    /// nullable <c>getError</c> null would re-create, on the error axis, the
    /// null-as-shape-discriminator hazard M15/P2b removed from the value axis; giving the
    /// shape its own signature states it structurally instead.
    /// </para>
    /// <para>
    /// ⚠⚠ <b>That is a statement about the COMPLETION, not about the accessor set, and
    /// reading it the other way costs real defects (M15/P4).</b> Whether the ABI declares a
    /// <c>get_error</c> does not decide membership here. <c>listTopics</c> declares none;
    /// <c>electLeaders</c> declares one — <c>kafka_admin_ElectLeadersResult_get_error</c> —
    /// and still belongs, because Java's
    /// <c>KafkaFuture&lt;Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;&gt;</c>
    /// (<c>ElectLeadersResult.java:36, :47</c>) makes that accessor the map's
    /// <em>value</em>: it is passed as <paramref name="readValue"/>, not as an error
    /// channel. <b>The Java return type decides the shape; the header decides the
    /// mechanics.</b> "Correcting" <c>electLeaders</c> onto
    /// <see cref="Complete{TKey}(IntPtr, Accessors, VoidKeyedAdminOperation{TKey}, Func{IntPtr, int, TKey})"/>
    /// on the strength of its accessor set compiles, and was measured at <b>10</b> failing
    /// tests. See <c>AdminCallbacks.ElectLeadersOptionalError</c>.
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
