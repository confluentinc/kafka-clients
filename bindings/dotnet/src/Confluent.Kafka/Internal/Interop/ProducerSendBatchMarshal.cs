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
using System.Buffers;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The <c>unsafe</c> send-path marshalling for the async producer's <b>batch</b> send
/// (<c>kafka_producer_Producer_send_batch</c>) — M11/P3.1, the Python binding's shape
/// (<c>bindings/python/_confluentkafka.c</c>'s <c>Producer_send_thread</c>). Fills the blittable
/// <see cref="ProducerRecordNative"/> array from already-pinned buffers, applies the ffi §A4
/// absent / empty / present sentinels, and issues the call. The sibling of
/// <see cref="ProducerSendMarshal"/>, which stays in place for the <b>sync</b> send path
/// (M11/P3.1 §3.1: the sync path is deliberately unchanged and still calls the singular
/// <c>Producer_send</c> with call-scoped <c>fixed</c> pins). Lives in <c>Internal/Interop/</c>
/// because it is the only new send-path code that needs <c>unsafe</c>, keeping <c>unsafe</c>
/// quarantined here (CLAUDE.md §2).
/// </summary>
/// <remarks>
/// <para>
/// <b>It does not own the pins — the caller does (M11/P3.1 §4.4).</b> The key / value
/// <see cref="MemoryHandle"/>s and the topic pointer arrive already pinned and are released by the
/// caller, in a <c>finally</c>, <b>after</b> the native call returns and <b>before</b> the future
/// reaches the completion pump. That split is the whole point of the deferred design: the pin has
/// to span <c>Send</c> → …accumulator… → <c>send_batch</c> returns, which no <c>fixed</c> block
/// inside this type could express. It still never spans the returned
/// <see cref="System.Threading.Tasks.Task"/> — the core copies every buffer synchronously inside
/// <c>send_batch</c> (verified: <c>send_batch_inner</c> runs <c>producer_send</c>, i.e.
/// <c>rt.block_on(producer.send(record, None))</c>, per record, and copies the topic with
/// <c>to_string_lossy().into_owned()</c>), so ffi §A4's "unpin right after the call" still holds,
/// merely applied to <c>send_batch</c> instead of <c>send</c>.
/// </para>
/// <para>
/// <b>Per-record results, not a single out-param.</b> Unlike <c>Producer_send</c>, which reports a
/// synchronous failure through one <c>out_error</c>, <c>send_batch</c> writes a
/// <c>(future, error)</c> pair for <b>every</b> index and returns the accepted count. The caller
/// therefore owns <c>count</c> result pairs and must free every non-null handle on every path
/// (ffi §A2). At <c>n == 1</c> that reduces to today's behavior: a non-null error becomes the
/// <see cref="KafkaException"/> the caller already expects.
/// </para>
/// </remarks>
internal static class ProducerSendBatchMarshal
{
    // A process-wide, permanently pinned 1-byte buffer for the EMPTY (present but zero-length)
    // key/value case (M11/P3.1 §4.2). ffi §A4 requires a NON-NULL pointer with length 0 there,
    // because the core rejects (null, len >= 0) — and both of the obvious ways to produce one are
    // wrong for a deferred send: `fixed` over an empty span yields NULL, and the stack sentinel the
    // sync path uses (ProducerSendMarshal) is a stack address that is dead by the time a deferred
    // batch reads it. That failure mode "works" almost always and corrupts rarely, which is the
    // worst kind, so the sentinel is hoisted to a static that outlives every send.
    //
    // Never freed, deliberately: one byte for the process, and freeing it would reintroduce exactly
    // the dangling-pointer question it exists to remove. The pin field is read by the pointer
    // initializer below (static field initializers run in textual order, so the pin exists first).
    private static readonly GCHandle s_emptySentinelPin =
        GCHandle.Alloc(new byte[1], GCHandleType.Pinned);

    private static readonly IntPtr s_emptySentinel = s_emptySentinelPin.AddrOfPinnedObject();

    /// <summary>
    /// Pins <paramref name="buffer"/> for a deferred send, or returns a <c>default</c>
    /// (nothing-pinned) handle when there is nothing to pin — i.e. when the buffer is <b>absent</b>
    /// (<see langword="null"/>) or <b>empty</b>. Dispose the result exactly once, on every path;
    /// <see cref="MemoryHandle.Dispose"/> is safe on the <c>default</c> value.
    /// </summary>
    /// <remarks>
    /// <b>The decision to pin and the interpretation of the pin live in one place</b> (DoD §12): the
    /// empty case is handled by <see cref="s_emptySentinel"/> in <see cref="Fill"/>, so pinning an
    /// empty buffer here would allocate a <see cref="GCHandle"/> whose pointer is never read — and,
    /// worse, if the two ever disagreed the record would go out as (null, len 0) and the core would
    /// reject it as <c>InvalidRequest</c>. Keeping both branches in this file is what stops that
    /// drifting apart.
    /// <para>
    /// <see cref="System.ReadOnlyMemory{T}.Pin"/> — not <c>GCHandle.Alloc(Pinned)</c>: the record's
    /// key/value is a <see cref="System.ReadOnlyMemory{T}"/>, which <c>GCHandle</c> cannot pin (it
    /// pins <em>objects</em>), while <c>Pin()</c> handles every backing store (array, string, native
    /// memory, a custom <c>MemoryManager</c>) and exists on all three TFMs — on
    /// <c>netstandard2.0</c> through the already-referenced <c>System.Memory</c> package
    /// (M11/P3.1 §3.9).
    /// </para>
    /// </remarks>
    /// <param name="buffer">The record's key or value.</param>
    internal static MemoryHandle PinIfNeeded(ReadOnlyMemory<byte>? buffer) =>
        buffer.HasValue && buffer.Value.Length > 0 ? buffer.Value.Pin() : default;

    /// <summary>
    /// Fills one <see cref="ProducerRecordNative"/> from a record plus its already-pinned buffers,
    /// applying the ffi §A4 sentinels: <b>absent</b> → <see cref="IntPtr.Zero"/> + <c>len -1</c>;
    /// <b>empty</b> → the static non-null sentinel + <c>len 0</c>; <b>present</b> → the pinned
    /// pointer + its length. The ABI's own <c>-1</c> sentinels are applied to a null partition /
    /// timestamp.
    /// </summary>
    /// <param name="native">The slot to fill (overwritten in full).</param>
    /// <param name="record">The already-serialized record.</param>
    /// <param name="topic">A pinned NUL-terminated UTF-8 topic pointer (see <see cref="PinnedTopicCache"/>).</param>
    /// <param name="keyPin">The key pin from <see cref="PinIfNeeded"/> (<c>default</c> when absent or empty).</param>
    /// <param name="valuePin">The value pin from <see cref="PinIfNeeded"/> (<c>default</c> when absent or empty).</param>
    internal static unsafe void Fill(
        ref ProducerRecordNative native,
        in SerializedProducerRecord record,
        IntPtr topic,
        in MemoryHandle keyPin,
        in MemoryHandle valuePin)
    {
        native.Topic = topic;

        // The ABI maps a null partition / timestamp to its own -1 sentinels.
        native.Partition = record.Partition ?? -1;
        native.Timestamp = record.Timestamp ?? -1L;

        if (!record.Key.HasValue)
        {
            native.Key = IntPtr.Zero;                   // absent (no key)
            native.KeyLength = -1;
        }
        else if (record.Key.Value.Length == 0)
        {
            native.Key = s_emptySentinel;               // empty: non-null pointer, length 0
            native.KeyLength = 0;
        }
        else
        {
            native.Key = (IntPtr)keyPin.Pointer;        // present
            native.KeyLength = record.Key.Value.Length;
        }

        if (!record.Value.HasValue)
        {
            native.Value = IntPtr.Zero;                 // absent (a tombstone)
            native.ValueLength = -1;
        }
        else if (record.Value.Value.Length == 0)
        {
            native.Value = s_emptySentinel;
            native.ValueLength = 0;
        }
        else
        {
            native.Value = (IntPtr)valuePin.Pointer;
            native.ValueLength = record.Value.Value.Length;
        }
    }

    /// <summary>
    /// Sends exactly one record through <c>kafka_producer_Producer_send_batch</c>
    /// (<c>count == 1</c>) from already-pinned buffers, and returns its
    /// <c>FutureRecordMetadata_t</c> handle. Allocation-free: the record and the one result pair
    /// live on the stack, so the send path's DoD §10 budget is unchanged.
    /// </summary>
    /// <param name="producer">
    /// The owned producer handle, passed as the <see cref="SafeProducerHandle"/> so the P/Invoke
    /// marshaler auto-<c>DangerousAddRef</c>/<c>Release</c>s it around the synchronous
    /// <c>send_batch</c> — the call-scoped guard against a concurrent <c>Producer_destroy</c>
    /// (ffi §A2 sync-op form). A closed handle marshals to <see cref="ObjectDisposedException"/>.
    /// </param>
    /// <param name="record">The already-serialized record.</param>
    /// <param name="topic">A pinned NUL-terminated UTF-8 topic pointer, valid for this call.</param>
    /// <param name="keyPin">The key pin, valid for this call (the caller releases it afterwards).</param>
    /// <param name="valuePin">The value pin, valid for this call (the caller releases it afterwards).</param>
    /// <returns>A non-null <c>FutureRecordMetadata_t</c> handle on success.</returns>
    /// <exception cref="KafkaException">The core reported a per-record send failure.</exception>
    internal static unsafe IntPtr SendOne(
        SafeProducerHandle producer,
        in SerializedProducerRecord record,
        IntPtr topic,
        in MemoryHandle keyPin,
        in MemoryHandle valuePin)
    {
        ProducerRecordNative native = default;
        Fill(ref native, record, topic, keyPin, valuePin);

        // Stack slots for the one result pair — no managed array, so the send path stays
        // allocation-free (DoD §10). Definitely assigned before `&` per C#'s rules; the callee
        // overwrites both.
        IntPtr future = IntPtr.Zero;
        IntPtr error = IntPtr.Zero;

        _ = NativeMethods.ProducerSendBatch(producer, &native, 1, &future, &error);

        // Per-record failure: null future + non-null error. FromHandle frees the error exactly
        // once (null-safe) and returns null on success.
        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }

        if (future == IntPtr.Zero)
        {
            // Defensive only: the ABI writes exactly one of the pair per index, so a null future
            // with a null error is a core contract violation. Surfaced as a KafkaException rather
            // than handed to the pump, which would enqueue a null future (mirrors the
            // NativeProducer.Create "null handle without an error" guard).
            throw new KafkaException(
                "kafka_producer_Producer_send_batch returned a null future without an error.");
        }

        return future;
    }

    /// <summary>
    /// Issues one <c>kafka_producer_Producer_send_batch</c> over
    /// <paramref name="records"/><c>[offset .. offset + count)</c>, writing the per-record results
    /// into <paramref name="outFutures"/> / <paramref name="outErrors"/> at the <b>same</b>
    /// indices, and returns the number of records the core accepted. The three arrays are pinned
    /// with <c>fixed</c> for exactly the call's duration; the buffers the records point at are
    /// pinned by the caller for at least the same window (ffi §A4).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The <paramref name="offset"/> is what makes the chunking rule expressible</b> (PLAN §3.4:
    /// "one <c>send_batch</c> per chunk, and a chunk never spans two nodes" —
    /// <c>ceil(count / chunk)</c> calls within a node). At the default chunk
    /// (<c>SLOT_CAPACITY</c>, i.e. node capacity) the formula always yields exactly one call per
    /// node, identical to the Python anchor; a lowered <c>CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK</c>
    /// is the only way to reach a non-zero offset. Do not "simplify" the parameter away because it
    /// looks unused at the default — it is unused at the default <em>by construction</em>, and
    /// load-bearing under an override.
    /// </para>
    /// <para>
    /// <b>Every index is written by the callee</b> (verified <c>send_batch_inner</c>: each
    /// early-continue branch assigns both slots), so on return the caller owns
    /// <paramref name="count"/> result pairs and must free every non-null handle (ffi §A2). This
    /// method frees nothing — the caller's compaction step does, because only it knows which
    /// completion each index belongs to.
    /// </para>
    /// </remarks>
    /// <param name="producer">The owned producer handle (the sync-op <c>SafeHandle</c> auto-ref, ffi §A2).</param>
    /// <param name="records">The filled record array; its topic / key / value pointers must be pinned by the caller.</param>
    /// <param name="offset">The index of the first record in this chunk.</param>
    /// <param name="count">The number of records in this chunk.</param>
    /// <param name="outFutures">Receives one future handle (or <see cref="IntPtr.Zero"/>) per index.</param>
    /// <param name="outErrors">Receives one error handle (or <see cref="IntPtr.Zero"/>) per index.</param>
    /// <returns>The number of records the core accepted (<c>0..count</c>).</returns>
    internal static unsafe int SendBatch(
        SafeProducerHandle producer,
        ProducerRecordNative[] records,
        int offset,
        int count,
        IntPtr[] outFutures,
        IntPtr[] outErrors)
    {
        if (count == 0)
        {
            // `fixed` over a zero-length array yields a null pointer, which the ABI asserts against
            // (records must not be null). Nothing to send, so skip the call entirely.
            return 0;
        }

        fixed (ProducerRecordNative* recordPtr = records)
        fixed (IntPtr* futurePtr = outFutures)
        fixed (IntPtr* errorPtr = outErrors)
        {
            return NativeMethods.ProducerSendBatch(
                producer, recordPtr + offset, count, futurePtr + offset, errorPtr + offset);
        }
    }
}
