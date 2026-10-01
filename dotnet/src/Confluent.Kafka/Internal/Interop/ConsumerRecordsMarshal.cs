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
/// The receive-path <b>typed, zero-copy</b> copy-out marshaller (ffi-marshalling.md
/// §B3/§B4, §6.4; PLAN M6/P1b §5). It turns a borrowed native poll batch
/// (<c>ConsumerRecords_t</c>, a Category-3 borrow-root) into an owned managed
/// <see cref="ConsumerRecords{TKey, TValue}"/>, <b>deserializing</b> each key/value with
/// the caller-supplied <see cref="IDeserializer{T}"/> and copying the topic / headers, so
/// <b>nothing native-backed escapes</b>. The caller (the poll path) invokes this and then
/// destroys the batch — safely, because after <see cref="CopyOut"/> returns no borrowed
/// pointer is retained.
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero-copy — the efficiency win (PLAN §5, DoD §10/§11).</b> The key/value are read
/// through an <b>unsafe <see cref="ReadOnlySpan{T}"/> over the native slice</b>
/// (<see cref="DeserializeSpan"/>) and handed straight to the deserializer, which returns
/// an owned <c>TKey</c> / <c>TValue</c> — there is
/// <b>no intermediate per-record <c>byte[]</c></b> on the key/value path. The
/// <c>ref struct</c> span cannot be stored, boxed, awaited, or sent across a thread, so it
/// provably cannot outlive the batch; the <c>unsafe</c> is contained here in
/// <c>Internal/Interop/</c> (CLAUDE.md §2). The only per-record allocations are the owned
/// values the user receives — the deserialized <c>TKey</c> /
/// <c>TValue</c>, the topic <see cref="string"/>, and the materialized
/// header keys / values — plus the record objects and the backing list. Batch traversal
/// and the borrowed-pointer reads allocate nothing.
/// </para>
/// <para>
/// <b>Three-state null model (PLAN decision C).</b> For each key/value the ABI hands back
/// a <c>(ptr, len)</c> pair. <c>len &lt; 0</c> (or a null pointer) is <b>absent</b> (an
/// absent key / a tombstone value) → <c>default(T)</c>, <b>deserializer not invoked</b>.
/// <c>len == 0</c> with a non-null pointer is <b>present-but-empty</b> → the deserializer
/// gets a zero-length span. <c>len &gt; 0</c> is <b>present</b> → the deserializer gets the
/// span.
/// </para>
/// <para>
/// <b>Mandatory <see cref="SerializationException"/> wrap (PLAN §6).</b> Any throw from a
/// user <see cref="IDeserializer{T}.Deserialize"/> is caught in <see cref="DeserializeField"/>
/// and wrapped in a <see cref="SerializationException"/> carrying topic / partition / offset
/// (the original as the inner exception). This is mandatory, not optional: on the async poll
/// path this copy-out runs inside the completion callback on the core's <b>foreign</b>
/// dispatcher thread (ffi §B6), where a managed exception escaping into native is undefined
/// behavior. The sync poll path deserializes on the caller's thread, where the same wrap
/// gives a uniform <see cref="SerializationException"/> surface.
/// </para>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> Each <c>ConsumerRecord_t</c> and every
/// key / value / topic / header slice is <b>borrowed</b> — read only during the copy, never
/// freed here. Strings use the <b>length-delimited</b> form
/// (<see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> with <c>out_len</c>) — NEVER a
/// NUL-scan (§B3), which would over-read past the field into the batch.
/// </para>
/// </remarks>
internal static class ConsumerRecordsMarshal
{
    /// <summary>
    /// Copies the borrowed native batch <paramref name="records"/> into an owned
    /// <see cref="ConsumerRecords{TKey, TValue}"/>, deserializing each key with
    /// <paramref name="keyDeserializer"/> and each value with
    /// <paramref name="valueDeserializer"/>. The caller retains ownership of
    /// <paramref name="records"/> and must destroy it <b>after</b> this returns (it is
    /// never null on the success path the poll callers use).
    /// </summary>
    /// <exception cref="SerializationException">
    /// A user deserializer threw while decoding a key/value (wrapped with topic / partition
    /// / offset context, PLAN §6).
    /// </exception>
    internal static ConsumerRecords<TKey, TValue> CopyOut<TKey, TValue>(
        IntPtr records,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        int count = NativeMethods.ConsumerRecordsCount(records);
        if (count <= 0)
        {
            // Empty (or defensively, a non-positive count): a valid, non-null result
            // with Count == 0 — success, not failure.
            return new ConsumerRecords<TKey, TValue>(Array.Empty<ConsumerRecord<TKey, TValue>>());
        }

        // Per-invocation topic memo (M9/P4 M6) — see TopicMemo. A LOCAL, deliberately: never a
        // static or [ThreadStatic] field. CopyOut runs on two different threads (the caller's
        // for the sync poll, the core's foreign dispatcher thread for the async poll), so
        // shared state would be a data race AND would hold batch-borrowed pointers past the
        // batch's lifetime (invariant I6). Being a ref struct local, it provably cannot escape
        // this frame.
        TopicMemo memo = default;

        List<ConsumerRecord<TKey, TValue>> list = new List<ConsumerRecord<TKey, TValue>>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr record = NativeMethods.ConsumerRecordsGet(records, i);
            if (record == IntPtr.Zero)
            {
                // Defensive: get() returns null only out of range, which count guards
                // against — skip rather than deref a null borrowed view.
                continue;
            }

            list.Add(CopyRecord(record, keyDeserializer, valueDeserializer, ref memo));
        }

        return new ConsumerRecords<TKey, TValue>(list);
    }

    /// <summary>
    /// A one-entry <c>(pointer, length) → decoded topic</c> memo, live only for the duration of
    /// a single <see cref="CopyOut{TKey, TValue}"/> call (M9/P4 M6).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why it pays.</b> Records in a poll batch arrive <b>grouped by partition</b>, so the
    /// same topic repeats contiguously; a one-entry memo therefore hits for every record after
    /// the first of each group, turning one managed <see cref="string"/> per record into one per
    /// distinct topic per batch. That is what the Rust core already does
    /// (<c>ConsumerRecord::topic</c> is an <c>Arc&lt;str&gt;</c> allocated once per
    /// <c>CompletedFetch</c> and cloned per record) and what Java does; the binding was throwing
    /// it away at the boundary, which <c>consumer-threading.md §27</c> names verbatim as an
    /// anti-pattern and <c>ffi §B4</c>'s allocation budget forbids ("no allocation attributable
    /// to <b>topic name</b>").
    /// </para>
    /// <para>
    /// <b>Why a pointer check is not enough.</b> On the real fetch path all records in a
    /// partition group return the identical <c>Arc&lt;str&gt;</c> pointer, so pointer identity
    /// alone would hit ~100%. But <c>MockConsumer_add_record</c> builds a <b>fresh</b>
    /// <c>Arc&lt;str&gt;</c> per call, so on the mock the pointers all differ and a
    /// pointer-keyed memo hits 0% — and every allocation-budget test in the suite is
    /// MockConsumer-based. A pointer-only memo would therefore be both ineffective in tests and
    /// <em>unprovable</em> broker-free. So the pointer test is a fast path and a byte comparison
    /// is the fallback: allocation-free, and safe because every topic pointer in a batch points
    /// into the same live borrow-root.
    /// </para>
    /// <para>
    /// <b><c>ref struct</c> on purpose.</b> It holds a pointer borrowed from the batch, so it
    /// must not outlive <see cref="CopyOut{TKey, TValue}"/>. Being by-ref-like makes that a
    /// compile-time guarantee: it cannot be boxed, stored in a field, captured, or sent across a
    /// thread (invariant I6, the same argument that makes the key/value span safe).
    /// </para>
    /// </remarks>
    private ref struct TopicMemo
    {
        /// <summary>The topic pointer the memoized string was decoded from (batch-borrowed).</summary>
        internal IntPtr Pointer;

        /// <summary>The <c>out_len</c> that accompanied <see cref="Pointer"/>.</summary>
        internal int Length;

        /// <summary>The decoded topic, or <see langword="null"/> when the memo is empty.</summary>
        internal string? Topic;
    }

    /// <summary>
    /// Copies one borrowed <c>ConsumerRecord_t</c> into an owned
    /// <see cref="ConsumerRecord{TKey, TValue}"/>. Reads scalars directly, marshals the
    /// length-delimited topic + copy-out headers, and deserializes the key/value in place
    /// from a span over the native slice (no intermediate <c>byte[]</c>).
    /// </summary>
    private static ConsumerRecord<TKey, TValue> CopyRecord<TKey, TValue>(
        IntPtr record,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        ref TopicMemo memo)
    {
        int partition = NativeMethods.ConsumerRecordPartition(record);
        long offset = NativeMethods.ConsumerRecordOffset(record);
        long timestamp = NativeMethods.ConsumerRecordTimestamp(record);
        TimestampType timestampType = (TimestampType)NativeMethods.ConsumerRecordTimestampType(record);

        // Topic: length-delimited slice → owned string (§B3, never NUL-scan). A record
        // always has a topic, so a null pointer would be a core contract violation; the
        // length-delimited PtrToString maps (Zero, _) → null, which we normalize to
        // empty to keep Topic non-null.
        //
        // Memoized per batch (M9/P4 M6, see TopicMemo): pointer identity is the fast path (the
        // real fetch path shares one Arc<str> across a partition group), then an allocation-free
        // byte comparison so the mock path — where every add_record allocates a fresh Arc<str> —
        // hits too. Reference reuse is semantically identical: `topic` flows on into the
        // deserializer calls and the SerializationException message below, and both the Rust
        // core and Java already share one topic string per partition group.
        IntPtr topicPtr = NativeMethods.ConsumerRecordTopic(record, out int topicLen);
        string topic;
        if (memo.Topic is not null
            && topicLen == memo.Length
            && (topicPtr == memo.Pointer
                || (topicLen > 0
                    && topicPtr != IntPtr.Zero
                    && memo.Pointer != IntPtr.Zero
                    && SameBytes(topicPtr, memo.Pointer, topicLen))))
        {
            topic = memo.Topic;
        }
        else
        {
            topic = Utf8Marshal.PtrToString(topicPtr, topicLen) ?? string.Empty;
            memo.Pointer = topicPtr;
            memo.Length = topicLen;
            memo.Topic = topic;
        }

        // Key / value: the typed zero-copy path (PLAN §5). Read the borrowed (ptr, len)
        // and deserialize in place — no intermediate byte[]. The three-state null model +
        // mandatory SerializationException wrap live in DeserializeField.
        IntPtr keyPtr = NativeMethods.ConsumerRecordKey(record, out int keyLen);
        TKey key = DeserializeField(keyDeserializer, topic, partition, offset, keyPtr, keyLen, isKey: true);

        IntPtr valuePtr = NativeMethods.ConsumerRecordValue(record, out int valueLen);
        TValue value = DeserializeField(valueDeserializer, topic, partition, offset, valuePtr, valueLen, isKey: false);

        Headers headers = CopyHeaders(record);

        // Scalar accessor reads (M9/P1) — no borrowed pointer, so nothing to copy out or
        // free; the receive-path zero-copy / copy-out contract (§B4) is untouched. The
        // serialized sizes are plain int32 (-1 when the key/value is null); the leader epoch
        // is presence-style (§0.1): true + out param when present, false → int? null (legacy
        // record formats). Mirrors the OffsetAndMetadata_leader_epoch presence read.
        int serializedKeySize = NativeMethods.ConsumerRecordSerializedKeySize(record);
        int serializedValueSize = NativeMethods.ConsumerRecordSerializedValueSize(record);
        int? leaderEpoch = NativeMethods.ConsumerRecordLeaderEpoch(record, out int le) ? le : (int?)null;

        return new ConsumerRecord<TKey, TValue>(
            topic, partition, offset, timestamp, timestampType, key, value, headers,
            leaderEpoch, serializedKeySize, serializedValueSize);
    }

    /// <summary>
    /// Applies the three-state null model (PLAN decision C) and deserializes one key/value
    /// field, wrapping any deserializer throw in a <see cref="SerializationException"/>
    /// (PLAN §6, mandatory). Returns <c>default(T)</c> for an absent field without invoking
    /// the deserializer.
    /// </summary>
    private static T DeserializeField<T>(
        IDeserializer<T> deserializer,
        string topic,
        int partition,
        long offset,
        IntPtr ptr,
        int length,
        bool isKey)
    {
        // Absent (tombstone value / no key): len < 0 (or a null pointer). Return
        // default(T) WITHOUT invoking the deserializer (PLAN decision C).
        if (ptr == IntPtr.Zero || length < 0)
        {
            return default!;
        }

        try
        {
            // Present (len >= 0, ptr != Zero): deserialize the span. len == 0 gives a
            // zero-length span (present-but-empty, distinct from absent).
            return DeserializeSpan(deserializer, topic, ptr, length);
        }
        catch (Exception exception)
        {
            // MANDATORY catch-and-wrap (PLAN §6): any user-deserializer throw becomes a
            // SerializationException carrying topic / partition / offset, the original as
            // the inner exception. On the async poll path this is also what keeps a managed
            // exception from unwinding into the native dispatcher frame (UB); the poll
            // trampoline's outer no-throw boundary is the final backstop. Wrapped verbatim
            // (a wrong-length built-in serde already throws SerializationException — the
            // outer wrap adds the record-location context uniformly).
            throw new SerializationException(
                $"Error deserializing {(isKey ? "key" : "value")} for topic '{topic}' " +
                $"[partition {partition}, offset {offset}].",
                exception);
        }
    }

    /// <summary>
    /// Deserializes a present field from an <b>unsafe span over the borrowed native
    /// slice</b> (§B4, PLAN §5). The <c>ref struct</c> <see cref="ReadOnlySpan{T}"/> borrows
    /// the batch bytes in place (no copy) and provably cannot escape the call; the
    /// <c>unsafe</c> is contained here in <c>Internal/Interop/</c> (CLAUDE.md §2). Split out
    /// of <see cref="DeserializeField"/> so the span never lives in a <c>try</c> that has a
    /// <c>catch</c> referencing it (a ref-struct restriction).
    /// </summary>
    private static unsafe T DeserializeSpan<T>(IDeserializer<T> deserializer, string topic, IntPtr ptr, int length)
    {
        ReadOnlySpan<byte> span = new ReadOnlySpan<byte>((void*)ptr, length);
        return deserializer.Deserialize(topic, span);
    }

    /// <summary>
    /// Allocation-free byte comparison of two <b>live batch-borrowed</b> slices of the same
    /// <paramref name="length"/> — the topic memo's fallback when the pointers differ but the
    /// bytes may not (M9/P4 M6, see <see cref="TopicMemo"/>). Both pointers point into the same
    /// borrow-root, which is still alive for the whole of
    /// <see cref="CopyOut{TKey, TValue}"/>, so reading them is safe.
    /// </summary>
    /// <remarks>
    /// The caller guarantees <paramref name="length"/> is positive and neither pointer is
    /// <see cref="IntPtr.Zero"/>; a negative length would make the span constructor throw.
    /// <c>SequenceEqual</c> over a byte span is vectorized and allocates nothing, so the memo
    /// cannot cost the very allocations it exists to remove.
    /// </remarks>
    private static unsafe bool SameBytes(IntPtr a, IntPtr b, int length) =>
        new ReadOnlySpan<byte>((void*)a, length).SequenceEqual(new ReadOnlySpan<byte>((void*)b, length));

    /// <summary>
    /// Copies all headers of a borrowed record into an owned <see cref="Headers"/>,
    /// each key from the length-delimited slice (§B3) and each value copied out (or
    /// <see langword="null"/>). Returns the shared empty <see cref="Headers"/> when the
    /// record has no headers (no allocation). Headers stay <b>materialized</b> owned bytes
    /// (PLAN §1) — they are not routed through a deserializer this phase.
    /// </summary>
    private static Headers CopyHeaders(IntPtr record)
    {
        int headerCount = NativeMethods.ConsumerRecordHeaderCount(record);
        if (headerCount <= 0)
        {
            return Headers.Empty;
        }

        List<Header> headers = new List<Header>(headerCount);
        for (int i = 0; i < headerCount; i++)
        {
            // Header key: length-delimited slice → owned string (§B3). A missing key
            // (out of range) is guarded by headerCount; normalize a null to empty.
            IntPtr keyPtr = NativeMethods.ConsumerRecordHeaderKey(record, i, out int keyLen);
            string key = Utf8Marshal.PtrToString(keyPtr, keyLen) ?? string.Empty;

            byte[]? value =
                CopyBytes(NativeMethods.ConsumerRecordHeaderValue(record, i, out int valueLen), valueLen);

            headers.Add(new Header(key, value));
        }

        return new Headers(headers);
    }

    /// <summary>
    /// Copies a borrowed <c>(ptr, len)</c> byte slice into an owned <c>byte[]</c>, or
    /// returns <see langword="null"/> when the slice is absent (<c>len &lt; 0</c> or a
    /// null pointer — the ABI's absent / null sentinel). A non-null pointer with
    /// <c>len == 0</c> is a genuine empty array (distinct from absent). Used only for
    /// header values now (the key/value path deserializes in place, §5).
    /// </summary>
    /// <remarks>
    /// The array is an owned copy: the borrowed slice is invalidated by
    /// <c>ConsumerRecords_destroy</c> right after the copy-out (§B4 copy-out default).
    /// <see cref="Marshal.Copy(IntPtr, byte[], int, int)"/> pins nothing and does not
    /// over-read (exactly <paramref name="length"/> bytes), so no <c>unsafe</c> is needed.
    /// </remarks>
    private static byte[]? CopyBytes(IntPtr ptr, int length)
    {
        if (ptr == IntPtr.Zero || length < 0)
        {
            return null;
        }

        byte[] buffer = new byte[length];
        if (length > 0)
        {
            Marshal.Copy(ptr, buffer, 0, length);
        }

        return buffer;
    }
}
