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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

/// <summary>
/// The single P/Invoke boundary over the Rust core's C ABI
/// (<c>confluent_kafka.h</c>). Classic <c>[DllImport]</c> declarations, uniform
/// across every TFM (netstandard2.0 is the floor, so no <c>[LibraryImport]</c> /
/// <c>LPUTF8Str</c> / <c>Marshal.PtrToStringUTF8</c> — ffi-marshalling.md §0.1).
///
/// Type map (ffi §0.1, verbatim): opaque <c>*_t</c> → <see cref="IntPtr"/>;
/// <c>const char*</c> (in and out) → <see cref="IntPtr"/> (hand-marshalled via
/// <see cref="Utf8Marshal"/>, §A3/§B3); <c>int32_t</c> → <see cref="int"/>; <c>bool</c> →
/// <c>[MarshalAs(UnmanagedType.I1)]</c> (C <c>bool</c> is 1 byte, not a 4-byte
/// Win32 <c>BOOL</c>).
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

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPropertiesNew();

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_put", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPropertiesPut(IntPtr props, IntPtr key, IntPtr value);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPropertiesDestroy(IntPtr props);

    // ---- kafka_consumer_Consumer_t — client lifecycle (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_KafkaConsumer_new</c> — creates a real (KIP-848) consumer
    /// from a properties handle. Fallible: on failure returns
    /// <see cref="IntPtr.Zero"/> and writes a non-null error handle to
    /// <paramref name="outError"/> (null = success). <paramref name="props"/> is
    /// typed as the <see cref="SafeConsumerPropertiesHandle"/> so the marshaller
    /// keeps it alive across the call (DangerousAddRef/Release — PLAN D6); the
    /// caller retains ownership and frees it afterward.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_KafkaConsumer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaConsumerNew(SafeConsumerPropertiesHandle props, out IntPtr outError);

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_new</c> — creates a broker-less mock consumer.
    /// <paramref name="autoOffsetReset"/> is a NUL-terminated reset-strategy name
    /// or <see cref="IntPtr.Zero"/> for the default (<c>"latest"</c>). Returns a
    /// non-null consumer handle.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerNew(IntPtr autoOffsetReset);

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
    /// <c>kafka_consumer_ConsumerGroupMetadata_destroy</c> — frees an owned
    /// group-metadata handle. Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerGroupMetadataDestroy(IntPtr meta);
}
