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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The single P/Invoke boundary over the Rust core's C ABI
/// (<c>confluent_kafka.h</c>). Classic <c>[DllImport]</c> declarations, uniform
/// across every TFM (netstandard2.0 is the floor, so no <c>[LibraryImport]</c> /
/// <c>LPUTF8Str</c> / <c>Marshal.PtrToStringUTF8</c> — ffi-marshalling.md §0.1).
///
/// Type map (ffi §0.1, verbatim): opaque <c>*_t</c> → <see cref="IntPtr"/> for
/// transient / borrowed handles, wrapped in a <c>SafeHandle</c> one layer up
/// (§A2/§B2); <c>const char*</c> (in and out) → <see cref="IntPtr"/> (hand-marshalled
/// via <see cref="Utf8Marshal"/>, §A3/§B3); <c>int32_t</c> → <see cref="int"/>;
/// <c>bool</c> → <c>[MarshalAs(UnmanagedType.I1)]</c> (C <c>bool</c> is 1 byte, not a
/// 4-byte Win32 <c>BOOL</c>).
///
/// Constructors of <b>owned</b> handles (<see cref="ConsumerPropertiesNew"/>,
/// <see cref="KafkaConsumerNew"/>, <see cref="MockConsumerNew"/>) are declared to
/// return their <c>SafeHandle</c> subtype <b>directly</b> rather than a raw
/// <see cref="IntPtr"/>: the interop marshaller invokes the (private) parameterless
/// ctor and sets the handle inside a constrained region, so there is no
/// allocation-gap window in which the native pointer could leak on an async abort /
/// OOM before a managed <c>SetHandle</c> runs (M2/P2 hardening). SafeHandle-return is
/// a classic <c>[DllImport]</c> feature, fully supported on the netstandard2.0 floor
/// (incl. net462) — no <c>[LibraryImport]</c> needed.
///
/// The C# method names drop the <c>kafka_&lt;pkg&gt;_</c> prefix (CLAUDE.md §6.3),
/// so each declaration carries the full ABI symbol as its <c>EntryPoint</c> —
/// otherwise the marshaller probes the short name and throws
/// <see cref="EntryPointNotFoundException"/> at runtime.
///
/// The shared <c>KafkaError</c> foundation (declared in M1/P1) is now live:
/// <see cref="KafkaException.FromHandle(IntPtr)"/> reads the four accessors and
/// frees the handle (ffi §A5/§B5) — the first runtime validation of their
/// <c>EntryPoint</c>s and the <c>I1</c> bools. M2/P1 adds the consumer client
/// lifecycle (<c>KafkaConsumer_new</c> / <c>MockConsumer_new</c> / <c>close</c> /
/// <c>close_with_timeout</c> / <c>destroy</c>, ffi §B2) plus the group-metadata
/// getter trio used to round-trip a UTF-8 config value (ffi §B3).
/// </summary>
internal static class NativeMethods
{
    /// <summary>
    /// The bare DLL name. The runtime maps it per-OS to
    /// <c>confluent_kafka.dll</c> / <c>libconfluent_kafka.so</c> /
    /// <c>libconfluent_kafka.dylib</c> (ffi §0.2). Never a hardcoded filename or
    /// absolute path — the MSBuild native-copy target places the matching binary
    /// in the output dir where default probing resolves it.
    /// </summary>
    private const string DllName = "confluent_kafka";

    // ---- kafka_common_KafkaError_t — the shared error handle (ffi §A5/§B5) ----

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_code", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int Code(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_message", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr Message(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_is_retriable", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool IsRetriable(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_is_fatal", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool IsFatal(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ErrorDestroy(IntPtr error);

    // ---- kafka_consumer_ConsumerProperties_t — config (ffi §0.1 "put") ----

    /// <summary>
    /// <c>kafka_consumer_ConsumerProperties_new</c> — allocates an empty, owned
    /// properties handle. Declared to return the
    /// <see cref="SafeConsumerPropertiesHandle"/> directly so the marshaller
    /// creates-and-sets it atomically (M2/P2); the ABI always returns non-null.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeConsumerPropertiesHandle ConsumerPropertiesNew();

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_put", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPropertiesPut(IntPtr props, IntPtr key, IntPtr value);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPropertiesDestroy(IntPtr props);

    // ---- kafka_consumer_Consumer_t — client lifecycle (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_KafkaConsumer_new</c> — creates a real (KIP-848) consumer
    /// from a properties handle, returning an owned <see cref="SafeConsumerHandle"/>
    /// directly (the marshaller creates-and-sets it atomically, M2/P2). Fallible: on
    /// failure the native returns null → the marshaller hands back an
    /// <b>IsInvalid</b> <see cref="SafeConsumerHandle"/> AND writes a non-null error
    /// handle to <paramref name="outError"/> (null <paramref name="outError"/> =
    /// success). Disposing an IsInvalid handle skips <c>ReleaseHandle</c>, so there is
    /// no spurious <c>Consumer_destroy</c>. <paramref name="props"/> is typed as the
    /// <see cref="SafeConsumerPropertiesHandle"/> so the marshaller keeps it alive
    /// across the call (DangerousAddRef/Release — PLAN D6); the caller retains
    /// ownership and frees it afterward.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_KafkaConsumer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeConsumerHandle KafkaConsumerNew(SafeConsumerPropertiesHandle props, out IntPtr outError);

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_new</c> — creates a broker-less mock consumer.
    /// <paramref name="autoOffsetReset"/> is a NUL-terminated reset-strategy name
    /// or <see cref="IntPtr.Zero"/> for the default (<c>"latest"</c>). Non-fallible:
    /// returns an owned <see cref="SafeConsumerHandle"/> directly (always valid; the
    /// marshaller creates-and-sets it atomically, M2/P2).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeConsumerHandle MockConsumerNew(IntPtr autoOffsetReset);

    /// <summary>
    /// <c>kafka_consumer_Consumer_close</c> — graceful close with the default
    /// timeout (sync; joins the background task). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerClose(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_close_with_timeout</c> — graceful close bounded by
    /// <paramref name="timeoutMs"/> (sync; joins the background task). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCloseWithTimeout(IntPtr consumer, long timeoutMs);

    /// <summary>
    /// <c>kafka_consumer_Consumer_destroy</c> — fire-and-forget free (cancels any
    /// in-flight op, does NOT join the background task). Graceful teardown routes
    /// through <see cref="ConsumerClose"/> / <see cref="ConsumerCloseWithTimeout"/>
    /// first (ffi §B2); this is the last-resort release. Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerDestroy(IntPtr consumer);

    // ---- Async void-result ops + wakeup (ffi §B5/§B6/§B7) ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_subscribe_async</c> — subscribes to
    /// <paramref name="topics"/> (an array of <paramref name="count"/> pinned,
    /// NUL-terminated UTF-8 <c>const char*</c> = <c>const char* const*</c>). The core
    /// reads the topic strings <b>synchronously</b> during the call (into an owned
    /// <c>Vec&lt;String&gt;</c>) before spawning the op, so the pinned buffers are
    /// call-scoped — freed once this returns (ffi §A4 call-scoped pin). The
    /// completion fires later via <paramref name="callback"/> on the core's
    /// dispatcher thread (null error = success), or inline on the caller thread if
    /// the core rejects at its own access guard. <paramref name="userData"/> is a
    /// <see cref="System.Runtime.InteropServices.GCHandle"/> over the per-op context.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSubscribeAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek_async</c> — seeks <c>(topic, partition)</c> to
    /// <paramref name="offset"/>. <paramref name="topic"/> is a pinned,
    /// NUL-terminated UTF-8 buffer read <b>synchronously</b> during the call
    /// (call-scoped pin). Completion semantics match
    /// <see cref="ConsumerSubscribeAsync"/> (seeking an unassigned partition is a
    /// genuine broker-free failure — the void bridge's error path).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekAsync(
        IntPtr consumer,
        IntPtr topic,
        int partition,
        long offset,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_unsubscribe_async</c> — unsubscribes from all
    /// topics / partitions (async). Reuses the same void-result completion callback
    /// as <see cref="ConsumerSubscribeAsync"/> (null error = success); takes no other
    /// arguments. <paramref name="userData"/> is a
    /// <see cref="System.Runtime.InteropServices.GCHandle"/> over the per-op context.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_unsubscribe_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerUnsubscribeAsync(
        IntPtr consumer,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_wakeup</c> — interrupts a blocked op. Sync,
    /// <b>bypasses</b> the access guard, callable from any thread (ffi §B5 /
    /// consumer-threading §11); null-safe. Fires the consumer's one-shot wakeup.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_wakeup", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerWakeup(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_close_async</c> — graceful async close (default
    /// timeout; joins the background task). Uses the same void-result completion
    /// callback and takes the core access guard; under the single-owner model the
    /// awaiter of an op is its disposer, so the guard is free at teardown — there is
    /// no separate-op drain (ffi §B7). The primary teardown path (<c>DisposeAsync</c>).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCloseAsync(
        IntPtr consumer,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    // ---- kafka_consumer_ConsumerGroupMetadata_t — owned result (ffi §B2/§B3) ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_group_metadata</c> — returns an owned
    /// (Category-3) group-metadata handle, or <see cref="IntPtr.Zero"/> on a
    /// concurrent-access rejection. Free it with
    /// <see cref="ConsumerGroupMetadataDestroy"/> after reading.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_group_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadata(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_ConsumerGroupMetadata_group_id</c> — the group id as a
    /// NUL-terminated <c>const char*</c> owned by the metadata handle (borrowed;
    /// copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_group_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadataGroupId(IntPtr meta);

    /// <summary>
    /// <c>kafka_consumer_ConsumerGroupMetadata_generation_id</c> — the group
    /// generation id (an <c>int32_t</c> scalar; <c>-1</c> when the consumer has not
    /// joined a group).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_generation_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerGroupMetadataGenerationId(IntPtr meta);

    /// <summary>
    /// <c>kafka_consumer_ConsumerGroupMetadata_member_id</c> — the member id as a
    /// NUL-terminated <c>const char*</c> owned by the metadata handle (borrowed;
    /// copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy). Empty
    /// before the consumer has joined a group.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_member_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadataMemberId(IntPtr meta);

    /// <summary>
    /// <c>kafka_consumer_ConsumerGroupMetadata_group_instance_id</c> — the static
    /// group instance id as a NUL-terminated <c>const char*</c> owned by the metadata
    /// handle, or <see cref="IntPtr.Zero"/> when absent (the ABI returns null for a
    /// non-static member). Copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/>
    /// (null → <see langword="null"/>) before destroy.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_group_instance_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadataGroupInstanceId(IntPtr meta);

    /// <summary>
    /// <c>kafka_consumer_ConsumerGroupMetadata_destroy</c> — frees an owned
    /// group-metadata handle. Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerGroupMetadataDestroy(IntPtr meta);

    // ---- Async poll (owned-handle completion, ffi §B6/§B7) — M3/P3 ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_poll_async</c> — polls for records asynchronously
    /// (one-operation-in-flight). The completion fires via <paramref name="callback"/>
    /// on the core's dispatcher thread: on success <c>records</c> is a non-null owned
    /// <c>ConsumerRecords_t</c> (Category-3 borrow-root) and <c>error</c> is null; on
    /// failure <c>records</c> is null and <c>error</c> is non-null. If the core rejects
    /// at its own access guard the callback fires inline on the caller thread with a
    /// <c>ConcurrentModification</c> error. The callback <b>takes ownership</b> of
    /// whichever handle is non-null and frees it (records via
    /// <see cref="ConsumerRecordsDestroy"/> after copy-out, error via
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>). <paramref name="userData"/> is
    /// a <see cref="GCHandle"/> over the per-op context. The only <c>_async</c> fn
    /// taking a timeout (<paramref name="timeoutMs"/>, Java <c>Duration</c> →
    /// <c>int64_t</c> ms).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_poll_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPollAsync(
        IntPtr consumer,
        long timeoutMs,
        ConsumerCallbacks.PollCallback callback,
        IntPtr userData);

    // ---- Async position (scalar completion, ffi §B6/§B7) — M5/P2 ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_position_async</c> — returns the current position of
    /// <c>(topic, partition)</c> asynchronously (one-operation-in-flight). The completion
    /// fires via <paramref name="callback"/> on the core's dispatcher thread with the
    /// <b>scalar</b> shape (ffi §B6/§B7): on success the <c>int64_t</c> position is the
    /// offset and <c>error</c> is null; on failure the position is 0 and <c>error</c> is
    /// non-null. If the core rejects at its own access guard the callback fires inline on
    /// the caller thread with a <c>ConcurrentModification</c> error. The scalar carries
    /// <b>no owned result handle</b> — the callback frees only the <c>error</c> on failure
    /// (via <see cref="KafkaException.FromHandle(IntPtr)"/>). <paramref name="topic"/> is a
    /// pinned, NUL-terminated UTF-8 buffer read <b>synchronously</b> during the call
    /// (call-scoped pin; the header's safety note requires only that <c>topic</c> be a
    /// valid C string for the duration of the call — the ABI does not borrow it past the
    /// return). <paramref name="userData"/> is a <see cref="GCHandle"/> over the per-op
    /// context. There is no timeout parameter — the timed <c>position(tp, Duration)</c>
    /// overload has no async ABI form yet (deferred, PLAN §2).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_position_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPositionAsync(
        IntPtr consumer,
        IntPtr topic,
        int partition,
        ConsumerCallbacks.PositionCallback callback,
        IntPtr userData);

    // ---- ConsumerRecords_t — the owned poll batch (Category 3, ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecords_count</c> — the number of records in the
    /// batch. Null-safe (→ 0).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordsCount(IntPtr records);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecords_is_empty</c> — whether the batch is empty.
    /// Null-safe (→ true).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_is_empty", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool ConsumerRecordsIsEmpty(IntPtr records);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecords_get</c> — the record at <paramref name="index"/>,
    /// <b>borrowed</b> (Category 4) and valid until the batch is destroyed, or
    /// <see cref="IntPtr.Zero"/> if out of range. Never freed by the binding.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordsGet(IntPtr records, int index);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecords_destroy</c> — frees the owned batch (the
    /// Category-3 borrow-root; every borrowed record / byte / string slice from it is
    /// invalidated). Null-safe (no-op). Called by the poll callback <b>after</b> the
    /// copy-out completes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRecordsDestroy(IntPtr records);

    // ---- ConsumerRecord_t — borrowed view accessors (Category 4, ffi §B2/§B3/§B4) ----

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_partition</c> — the record's partition.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordPartition(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_offset</c> — the record's offset.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerRecordOffset(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_timestamp</c> — the record's timestamp
    /// (milliseconds since epoch, or <c>-1</c> = <c>NO_TIMESTAMP</c>).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_timestamp", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerRecordTimestamp(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_timestamp_type</c> — the timestamp type as its
    /// numeric id (<c>-1</c> NoTimestampType / <c>0</c> CreateTime /
    /// <c>1</c> LogAppendTime).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_timestamp_type", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordTimestampType(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_topic</c> — the topic as a
    /// <b>length-delimited</b>, NON-NUL-terminated <c>(ptr, out_len)</c> slice
    /// borrowing into the batch (ffi §B3: marshal with
    /// <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> using
    /// <paramref name="outLen"/>, NEVER a NUL-scan). Valid until the batch is
    /// destroyed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordTopic(IntPtr record, out int outLen);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_key</c> — the key bytes as a
    /// <c>(ptr, out_len)</c> pair borrowing into the batch, or
    /// <c>(<see cref="IntPtr.Zero"/>, -1)</c> if the key is absent. Copied out into an
    /// owned managed array during the callback (ffi §B4 copy-out).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordKey(IntPtr record, out int outLen);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_value</c> — the value bytes as a
    /// <c>(ptr, out_len)</c> pair borrowing into the batch, or
    /// <c>(<see cref="IntPtr.Zero"/>, -1)</c> if the value is absent (tombstone).
    /// Copied out into an owned managed array during the callback (ffi §B4 copy-out).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordValue(IntPtr record, out int outLen);

    // ---- ConsumerRecord_t headers (Category 4, in scope M3/P3 — internal only) ----

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_header_count</c> — the number of headers on
    /// the record.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_header_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordHeaderCount(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_header_key</c> — the header key at
    /// <paramref name="index"/> as a <b>length-delimited</b>, NON-NUL-terminated
    /// <c>(ptr, out_len)</c> slice borrowing into the batch (ffi §B3: use
    /// <paramref name="outLen"/>, NEVER a NUL-scan), or
    /// <c>(<see cref="IntPtr.Zero"/>, -1)</c> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_header_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordHeaderKey(IntPtr record, int index, out int outLen);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_header_value</c> — the header value at
    /// <paramref name="index"/> as a <c>(ptr, out_len)</c> pair borrowing into the
    /// batch, or <c>(<see cref="IntPtr.Zero"/>, -1)</c> if out of range or the value is
    /// null. Copied out into an owned managed array (ffi §B4 copy-out).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_header_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordHeaderValue(IntPtr record, int index, out int outLen);

    // ---- Consumer_assign + MockConsumer broker-free drivers (ffi §B2, mock only) ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_assign</c> — assigns the consumer to
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs from the parallel
    /// arrays <paramref name="topics"/> (pinned NUL-terminated UTF-8 <c>const char*</c>
    /// = <c>const char* const*</c>) and <paramref name="partitions"/>. Read
    /// synchronously during the call (call-scoped pin, ffi §A4). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assign", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerAssign(IntPtr consumer, IntPtr[] topics, int[] partitions, int count);

    // ---- Async void ops on partition collections (op_callback_t, ffi §B6/§B7) — M5/P3 ----
    //
    // Five identically-shaped void-result async ops (assign / pause / resume /
    // seekToBeginning / seekToEnd), each over the parallel (topics[], partitions[], count)
    // arrays. All reuse the SAME void-result completion callback as
    // ConsumerSubscribeAsync (op_callback_t = (KafkaError*, void*)) — NO new callback type
    // this phase. The core reads the topic strings + partition ints SYNCHRONOUSLY during the
    // call (into an owned Vec<TopicPartition>, via read_topic_partitions in src/ffi/consumer.rs)
    // BEFORE spawning the op, so the pinned buffers + the partitions int[] are call-scoped —
    // freed once each returns (ffi §A4/§B4 call-scoped pin), matching ConsumerSubscribeAsync.
    // A count == 0 (empty collection) is a valid pass-through: assign([]) clears the
    // assignment, the others are a no-op — the binding never spuriously rejects empty (§B5).

    /// <summary>
    /// <c>kafka_consumer_Consumer_assign_async</c> — assigns the consumer to
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs from the parallel arrays
    /// <paramref name="topics"/> (pinned NUL-terminated UTF-8 <c>const char*</c> =
    /// <c>const char* const*</c>) and <paramref name="partitions"/>. Read synchronously
    /// during the call (call-scoped pin, ffi §A4). The completion fires via
    /// <paramref name="callback"/> (the shared <c>op_callback_t</c>: null error = success)
    /// on the core's dispatcher thread, or inline on the caller thread if the core rejects
    /// at its own access guard. <paramref name="userData"/> is a
    /// <see cref="GCHandle"/> over the per-op context. An empty collection
    /// (<paramref name="count"/> <c>== 0</c>) clears the assignment (Java parity).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assign_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerAssignAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_pause_async</c> — pauses fetching for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (parallel arrays, as
    /// <see cref="ConsumerAssignAsync"/>). Same shared <c>op_callback_t</c> completion; an
    /// empty collection is a no-op success (Java iterates an empty collection).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_pause_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPauseAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_resume_async</c> — resumes fetching for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (parallel arrays, as
    /// <see cref="ConsumerAssignAsync"/>). Same shared <c>op_callback_t</c> completion; an
    /// empty collection is a no-op success.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_resume_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerResumeAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek_to_beginning_async</c> — requests an EARLIEST offset
    /// reset for the <paramref name="count"/> <c>(topic, partition)</c> pairs (parallel
    /// arrays, as <see cref="ConsumerAssignAsync"/>). Same shared <c>op_callback_t</c>
    /// completion. On the mock this sets only the reset <em>strategy</em> (it does NOT read
    /// the beginning offsets), so it resolves broker-free with no offset setup; the actual
    /// reset offset is consulted lazily on the next <c>poll</c> (from the map populated by
    /// <see cref="MockConsumerUpdateBeginningOffsets"/>). An empty collection is a no-op.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_beginning_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekToBeginningAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek_to_end_async</c> — requests a LATEST offset reset for
    /// the <paramref name="count"/> <c>(topic, partition)</c> pairs (parallel arrays, as
    /// <see cref="ConsumerAssignAsync"/>). Same shared <c>op_callback_t</c> completion; the
    /// LATEST analog of <see cref="ConsumerSeekToBeginningAsync"/> (lazily consults the map
    /// populated by <see cref="MockConsumerUpdateEndOffsets"/> on the next <c>poll</c>). An
    /// empty collection is a no-op.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_end_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekToEndAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    // ---- MockConsumer offset-update helpers (mock only, ffi §B2) — M5/P3 ----
    //
    // Per-(topic, partition, offset) — a single offset each, NOT a map. Populate the
    // mock's beginning/end offset maps that poll's reset_offset_position consults after a
    // SeekToBeginning/SeekToEnd, so the seek is observable end-to-end via a follow-up poll
    // (§6.6). One DllImport each + one loop over the caller's collection on the forwarder.

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_update_beginning_offsets</c> — sets the beginning
    /// (EARLIEST) offset used by a subsequent <c>seekToBeginning</c> reset on a mock consumer
    /// (mock only; mirrors Java <c>updateBeginningOffsets(Map)</c>, one entry at a time).
    /// <paramref name="topic"/> is a pinned NUL-terminated UTF-8 buffer. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_update_beginning_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerUpdateBeginningOffsets(
        IntPtr consumer,
        IntPtr topic,
        int partition,
        long offset);

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_update_end_offsets</c> — sets the end (LATEST) offset
    /// used by a subsequent <c>seekToEnd</c> reset on a mock consumer (mock only; mirrors Java
    /// <c>updateEndOffsets(Map)</c>, one entry at a time). Same shape as
    /// <see cref="MockConsumerUpdateBeginningOffsets"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_update_end_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerUpdateEndOffsets(
        IntPtr consumer,
        IntPtr topic,
        int partition,
        long offset);

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_add_record</c> — queues a record on a mock
    /// consumer (mock only; errors on a real consumer). The record's partition must
    /// already be assigned (via <see cref="ConsumerAssign"/>) or this errors.
    /// <paramref name="key"/> / <paramref name="value"/> are <c>(ptr, len)</c> pairs;
    /// pass <c>len &lt; 0</c> (or a null ptr) for an absent key/value. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_add_record", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerAddRecord(
        IntPtr consumer,
        IntPtr topic,
        int partition,
        long offset,
        IntPtr key,
        int keyLen,
        IntPtr value,
        int valueLen);

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_set_poll_error</c> — injects an
    /// <c>illegal_state</c> error returned by the <b>next</b> poll on a mock consumer
    /// (mock only; mirrors Java <c>setPollException</c>). Drives the FAILURE test
    /// broker-free. <paramref name="message"/> is a pinned NUL-terminated UTF-8 buffer.
    /// Returns a <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_set_poll_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerSetPollError(IntPtr consumer, IntPtr message);

    // ---- Sync consumer state reads + enforce_rebalance (M5/P1, ffi §B2/§B5) ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_assignment</c> — the current assignment as an owned
    /// (Category-3) <c>TopicPartitionList_t</c> borrow-root, or <see cref="IntPtr.Zero"/>
    /// on a concurrent-access rejection (the core's own guard could not be acquired). Map
    /// the null to <see cref="InvalidOperationException"/> (ffi §B5), else copy every
    /// element out and free the root with <see cref="TopicPartitionListDestroy"/>
    /// (<see cref="TopicPartitionListMarshal"/>).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assignment", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerAssignment(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_subscription</c> — the current topic subscription as an
    /// owned (Category-3) <c>StringList_t</c> borrow-root, or <see cref="IntPtr.Zero"/> on
    /// a concurrent-access rejection. Copy out then free with
    /// <see cref="StringListDestroy"/> (<see cref="StringListMarshal"/>).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscription", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSubscription(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_paused</c> — the currently paused partitions as an owned
    /// (Category-3) <c>TopicPartitionList_t</c> borrow-root, or <see cref="IntPtr.Zero"/>
    /// on a concurrent-access rejection. Same accessors as
    /// <see cref="ConsumerAssignment"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_paused", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPaused(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_enforce_rebalance</c> — triggers a rebalance (sync).
    /// <paramref name="reason"/> is a pinned NUL-terminated UTF-8 buffer or
    /// <see cref="IntPtr.Zero"/> (the ABI accepts a null reason). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>. Under the current KIP-848 core the
    /// returned handle is always null — a logged no-op that returns success (Java
    /// <c>AsyncKafkaConsumer.enforceRebalance</c> throws nothing; the core's
    /// <c>enforce_rebalance</c> returns <c>Ok(())</c>). The still-null-checked error path
    /// is the uniform sync-op discipline (ffi §B5) and reserves a real error for a future
    /// classic-protocol arm without a .NET change.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_enforce_rebalance", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerEnforceRebalance(IntPtr consumer, IntPtr reason);

    // ---- TopicPartitionList_t — owned borrow-root + borrowed elements (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionList_count</c> — the number of topic-partitions in
    /// the owned list.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionList_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionListCount(IntPtr list);

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionList_get</c> — the topic-partition at
    /// <paramref name="index"/>, <b>borrowed</b> (Category 4) and valid until the list is
    /// destroyed, or <see cref="IntPtr.Zero"/> if out of range. Never freed by the binding
    /// (the list root's <see cref="TopicPartitionListDestroy"/> invalidates it).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionList_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionListGet(IntPtr list, int index);

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionList_destroy</c> — frees the owned
    /// topic-partition-list root (every borrowed element from it is invalidated).
    /// Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionList_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TopicPartitionListDestroy(IntPtr list);

    /// <summary>
    /// <c>kafka_consumer_TopicPartition_topic</c> — the topic of a borrowed
    /// topic-partition element as a NUL-terminated <c>const char*</c> owned by the element
    /// (valid until the list is destroyed). Copy via
    /// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy — this is the
    /// NUL-terminated form (§B3), NOT the length-delimited receive-path form.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartition_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionTopic(IntPtr tp);

    /// <summary>
    /// <c>kafka_consumer_TopicPartition_partition</c> — the partition of a borrowed
    /// topic-partition element.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartition_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionPartition(IntPtr tp);

    // ---- StringList_t — owned borrow-root + borrowed elements (ffi §B2/§B3) ----

    /// <summary>
    /// <c>kafka_consumer_StringList_count</c> — the number of strings in the owned list.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_StringList_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int StringListCount(IntPtr list);

    /// <summary>
    /// <c>kafka_consumer_StringList_get</c> — the string at <paramref name="index"/> as a
    /// NUL-terminated <c>const char*</c> owned by the list (borrowed; valid until the list
    /// is destroyed), or <see cref="IntPtr.Zero"/> if out of range. Copy via
    /// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy — NUL-terminated form
    /// (§B3), NOT the length-delimited form.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_StringList_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr StringListGet(IntPtr list, int index);

    /// <summary>
    /// <c>kafka_consumer_StringList_destroy</c> — frees the owned string-list root (every
    /// borrowed string from it is invalidated). Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_StringList_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void StringListDestroy(IntPtr list);
}
