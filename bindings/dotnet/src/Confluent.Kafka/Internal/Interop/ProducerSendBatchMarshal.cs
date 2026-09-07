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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The <c>unsafe</c> send-path marshalling for the async producer's <b>batch</b> send
/// (<c>kafka_producer_Producer_send_batch</c>) — M11/P3.1, the Python binding's shape
/// (<c>bindings/python/_confluentkafka.c</c>'s <c>Producer_send_thread</c>). Fills the blittable
/// <see cref="ProducerRecordNative"/> array, applies the §A4 absent / empty / present sentinels, and
/// reads the ABI's <b>per-record</b> result pair. The sibling of
/// <see cref="ProducerSendMarshal"/>, which stays in place for the <b>sync</b> send path
/// (M11/P3.1 §3.1: the sync path is deliberately unchanged and still calls the singular
/// <c>Producer_send</c>). Lives in <c>Internal/Interop/</c> because it is the only new send-path
/// code that needs <c>unsafe</c>, keeping <c>unsafe</c> quarantined here (CLAUDE.md §2).
/// </summary>
/// <remarks>
/// <para>
/// <b>Slice S1 scope — pins are still CALL-SCOPED.</b> This entry point pins the topic / key / value
/// inside the call and unpins on return, exactly as <see cref="ProducerSendMarshal"/> does, so the
/// buffer-lifetime contract is unchanged from Option C while the mirror struct, the sentinels and
/// the per-record error semantics are proven against the real ABI. The deferred-pin machinery (a
/// pin taken in <c>Send</c> and released after <c>send_batch</c> returns) is slice S2, and the
/// accumulator that makes a batch bigger than one record is slice S3.
/// </para>
/// <para>
/// <b>Per-record results, not a single out-param.</b> Unlike <c>Producer_send</c>, which reports a
/// synchronous failure through one <c>out_error</c>, <c>send_batch</c> writes a
/// <c>(future, error)</c> pair for <b>every</b> index and returns the success count. The caller
/// therefore owns <c>count</c> pairs and must free every non-null handle on every path (ffi §A2).
/// At <c>n == 1</c> that reduces to today's behavior: a non-null error becomes the
/// <see cref="KafkaException"/> the caller already expects.
/// </para>
/// </remarks>
internal static class ProducerSendBatchMarshal
{
    /// <summary>
    /// Sends exactly one record through <c>kafka_producer_Producer_send_batch</c> (<c>count == 1</c>)
    /// and returns its <c>FutureRecordMetadata_t</c> handle. Behaviorally identical to
    /// <see cref="ProducerSendMarshal.Send"/>: the topic and the key / value buffers are pinned only
    /// for the duration of the P/Invoke — the core copies them into the batch buffer during the call
    /// (ffi §A4) — and a per-record error is raised as a <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// Sentinels (ffi §A4, unchanged): an <b>absent</b> (<see langword="null"/>) key/value passes
    /// <see cref="IntPtr.Zero"/> + <c>len -1</c>; an <b>empty</b> (zero-length) one passes a
    /// <b>non-null stack sentinel</b> + <c>len 0</c> (a <c>fixed</c> over an empty span yields a null
    /// pointer, which the core rejects for a non-negative length); a <b>present</b> one passes the
    /// pinned pointer + its length. The stack sentinel is correct <em>because the pin is
    /// call-scoped</em> — slice S2 replaces it with a process-wide statically-pinned byte when the
    /// send is deferred, at which point a stack address would be a use-after-free (PLAN §4.2).
    /// </remarks>
    /// <param name="producer">
    /// The owned producer handle, passed as the <see cref="SafeProducerHandle"/> so the P/Invoke
    /// marshaler auto-<c>DangerousAddRef</c>/<c>Release</c>s it around the synchronous
    /// <c>send_batch</c> — the call-scoped guard against a concurrent <c>Producer_destroy</c>
    /// (ffi §A2 sync-op form). A closed handle marshals to <see cref="ObjectDisposedException"/>.
    /// </param>
    /// <param name="record">The already-serialized record (its topic is non-null; validated above).</param>
    /// <returns>A non-null <c>FutureRecordMetadata_t</c> handle on success.</returns>
    /// <exception cref="KafkaException">The core reported a per-record send failure.</exception>
    internal static unsafe IntPtr SendOne(SafeProducerHandle producer, in SerializedProducerRecord record)
    {
        // Call-scoped topic pin: send_batch copies the topic synchronously, via
        // to_string_lossy().into_owned() inside send_batch_inner (ffi §A3).
        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(record.Topic);

        ReadOnlySpan<byte> keySpan = record.Key.HasValue ? record.Key.Value.Span : default;
        ReadOnlySpan<byte> valueSpan = record.Value.HasValue ? record.Value.Value.Span : default;

        // A non-null stack sentinel for the empty (Length == 0) case: `fixed` over an empty span
        // yields a NULL pointer, and the core rejects (null, len >= 0). Its address is guaranteed
        // non-null, so it distinguishes empty (non-null ptr, len 0) from absent (null ptr, len -1).
        byte emptySentinel = 0;

        fixed (byte* keyPtr = keySpan)
        fixed (byte* valuePtr = valueSpan)
        {
            ProducerRecordNative native = default;
            native.Topic = topicPin.Pointer;

            // The ABI maps null partition / timestamp to its own -1 sentinels.
            native.Partition = record.Partition ?? -1;
            native.Timestamp = record.Timestamp ?? -1L;

            if (!record.Key.HasValue)
            {
                native.Key = IntPtr.Zero;       // absent
                native.KeyLength = -1;
            }
            else if (keySpan.Length == 0)
            {
                native.Key = (IntPtr)(&emptySentinel);  // empty: non-null pointer, length 0
                native.KeyLength = 0;
            }
            else
            {
                native.Key = (IntPtr)keyPtr;    // present
                native.KeyLength = keySpan.Length;
            }

            if (!record.Value.HasValue)
            {
                native.Value = IntPtr.Zero;     // absent (tombstone)
                native.ValueLength = -1;
            }
            else if (valueSpan.Length == 0)
            {
                native.Value = (IntPtr)(&emptySentinel);
                native.ValueLength = 0;
            }
            else
            {
                native.Value = (IntPtr)valuePtr;
                native.ValueLength = valueSpan.Length;
            }

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
    }

    /// <summary>
    /// Issues one <c>kafka_producer_Producer_send_batch</c> over
    /// <paramref name="records"/><c>[offset .. offset + count)</c>, writing the per-record results
    /// into <paramref name="outFutures"/> / <paramref name="outErrors"/> at the <b>same</b>
    /// indices, and returns the number of records the core accepted. The three arrays are pinned
    /// with <c>fixed</c> for exactly the call's duration; the callee borrows every buffer the
    /// records point at only for that same window (ffi §A4).
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
