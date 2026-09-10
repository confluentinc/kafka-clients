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
/// <b>The <c>SafeHandle</c>-as-parameter convention (ffi §A2; M9/P4 H1).</b> Every
/// <b>synchronous</b> consumer function declares its handle parameter as
/// <see cref="SafeConsumerHandle"/> rather than a raw <see cref="IntPtr"/> — the one
/// sanctioned exception to "opaque <c>*_t</c> → <see cref="IntPtr"/>", precedented
/// in-branch by <see cref="KafkaConsumerNew"/>'s <c>props</c> parameter. The interop
/// marshaller then does the <c>DangerousAddRef</c> before the native call and the
/// <c>DangerousRelease</c> in a <c>finally</c> after it, so the consumer cannot be
/// destroyed out from under a call in progress — including a
/// <see cref="ConsumerPoll"/> that blocks for a caller-supplied timeout, which
/// previously handed native a raw pointer that a concurrent <c>Dispose</c> could free
/// mid-call (a use-after-free with no managed exception). <c>ThrowIfClosed()</c> still
/// runs first at every call site, so for an already-closed consumer the observable
/// exception type and message are unchanged; the marshaller's
/// <see cref="ObjectDisposedException"/> is observable only in the narrow race that was
/// previously the use-after-free. The convention extends to the M9/P8
/// <see cref="SafeConsumerReentrancyHandle"/> family: all 21 non-destroy
/// <c>ConsumerHandle_*</c> declarations are synchronous and take that
/// <c>SafeHandle</c> as their parameter. <b>Four</b> declarations deliberately keep
/// <see cref="IntPtr"/> — see <see cref="ConsumerClose"/>,
/// <see cref="ConsumerCloseWithTimeout"/>, <see cref="ConsumerDestroy"/> and
/// <see cref="ConsumerHandleDestroy"/> (the last two structurally: each is called
/// from its own <c>SafeHandle</c>'s <c>ReleaseHandle</c>, where passing <c>this</c>
/// would AddRef a handle already mid-release). The 18
/// <c>_async</c> declarations also keep <see cref="IntPtr"/>: they need a
/// <b>span-the-op</b> reference (taken explicitly at submit, released in
/// <c>FreeGcHandle</c>), and a call-scoped marshaller AddRef would be the wrong
/// lifetime — it would release when submit returns, long before the callback fires.
/// <c>SafeHandle</c>-as-parameter marshalling is a classic <c>[DllImport]</c> feature
/// supported on the netstandard2.0 floor (incl. net462), so this is floor-safe.
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
///
/// <b><c>partial</c> (M15/P1).</b> CA1060 requires the P/Invoke declarations to live
/// in <em>one</em> class, but this file already holds 218 of them; the admin family
/// therefore lands in <c>NativeMethods.Admin.cs</c> as another part of this same
/// class, keeping both files navigable without splitting the boundary type.
/// </summary>
internal static partial class NativeMethods
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

    /// <summary>
    /// <c>kafka_common_KafkaError_new</c> — the <b>inverse</b> of the accessors above:
    /// builds an error handle a managed callback can <b>return</b> to the Rust core. The
    /// rebalance-listener trampolines are the motivating (and only) caller: a listener that
    /// throws must hand the core an error rather than unwind into native (ffi §B6), and the
    /// core turns the returned handle back into the <c>Result::Err</c> that propagates out
    /// of the operation which triggered the rebalance.
    /// <para>
    /// <paramref name="message"/> is a pinned NUL-terminated UTF-8 buffer, or
    /// <see cref="IntPtr.Zero"/> for an empty message. <paramref name="code"/> is looked up
    /// as a Kafka protocol error code; anything unknown (including <c>-1</c>) maps to
    /// <c>UnknownServerError</c>, mirroring Java's <c>Errors.forCode</c>.
    /// </para>
    /// <para>
    /// <b>Ownership.</b> The returned handle is owned by the caller — but a handle
    /// <em>returned to a listener callback</em> transfers to the core, so the trampoline
    /// must <b>not</b> destroy it (<c>confluent_kafka.h:200-207</c>). This is the one
    /// declaration here whose result is deliberately never passed to
    /// <see cref="ErrorDestroy"/>.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaErrorNew(int code, IntPtr message);

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

    // ⚠ THE CLOSE FAMILY IS DELIBERATELY EXEMPT FROM THE SafeHandle-PARAM CONVENTION
    // (M9/P4 decision Q2, plan §3.4). Consumer_close and Consumer_close_with_timeout keep
    // IntPtr, and Consumer_destroy is structurally excluded. Do NOT "finish the job" here —
    // converting them would change invariant I2 (close-before-destroy teardown ordering),
    // which is the main design constraint on H1. The per-declaration reasons are on each
    // one below.

    /// <summary>
    /// <c>kafka_consumer_Consumer_close</c> — graceful close with the default
    /// timeout (sync; joins the background task). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </summary>
    /// <remarks>
    /// <b>EXEMPT from the <c>SafeHandle</c>-param convention (decision Q2) — do not
    /// convert.</b> Its only caller is <c>NativeConsumer.CloseSync</c>, which has already
    /// won the one-shot <c>TryBeginClose</c> latch and then releases the handle in its own
    /// <c>finally</c>, on the same thread in program order. The close therefore provably
    /// precedes its own destroy, and since <c>Consumer_destroy</c> is reachable only from
    /// <c>SafeConsumerHandle.ReleaseHandle</c> ← <c>_handle.Dispose()</c> ← the latch
    /// winner, no concurrent destroy can race it — the thing H1 protects against does not
    /// exist here. Converting would buy nothing and would cost the <c>Dispose</c>
    /// must-not-throw contract on the shared teardown path.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerClose(IntPtr consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_close_with_timeout</c> — graceful close bounded by
    /// <paramref name="timeoutMs"/> (sync; joins the background task). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success) consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </summary>
    /// <remarks>
    /// <b>EXEMPT from the <c>SafeHandle</c>-param convention (decision Q2) — do not
    /// convert.</b> Same latch argument as <see cref="ConsumerClose"/>, and additionally
    /// this one is called from <c>NativeConsumer.Dispose</c>: if the marshaller threw
    /// <see cref="ObjectDisposedException"/> from inside that <c>try</c>, it would
    /// propagate out of <c>Dispose</c> (the existing <c>finally</c> does not swallow),
    /// violating the .NET <c>Dispose</c> must-not-throw contract.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCloseWithTimeout(IntPtr consumer, long timeoutMs);

    /// <summary>
    /// <c>kafka_consumer_Consumer_destroy</c> — fire-and-forget free (cancels any
    /// in-flight op, does NOT join the background task). Graceful teardown routes
    /// through <see cref="ConsumerClose"/> / <see cref="ConsumerCloseWithTimeout"/>
    /// first (ffi §B2); this is the last-resort release. Null-safe (no-op).
    /// </summary>
    /// <remarks>
    /// <b>STRUCTURALLY EXCLUDED from the <c>SafeHandle</c>-param convention — it cannot be
    /// converted.</b> Its only caller is <c>SafeConsumerHandle.ReleaseHandle()</c>, which
    /// passes the protected <c>handle</c> field. Declaring the parameter as
    /// <see cref="SafeConsumerHandle"/> would make the marshaller <c>DangerousAddRef</c> a
    /// handle that is already mid-release.
    /// </remarks>
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

    // NOTE (M5/P7): `Consumer_seek_async` is intentionally NOT declared. Seek is now a
    // SYNC member (Python parity) that calls the sync ABI directly — see the sync-op
    // declarations `ConsumerSeek` / `ConsumerSeekWithMetadata` below. The Rust
    // `Consumer_seek_async` symbol still exists in the header (Rust-owned; Mode A = no
    // Rust change), we simply stop declaring it on the C# side.

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
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1d). Because this is the one
    /// deliberately cross-thread call, it is also the one whose caller must catch the
    /// marshaller's <see cref="ObjectDisposedException"/> — <c>Wakeup</c> is documented as a
    /// no-op once closed, so <c>NativeConsumer.Wakeup</c> swallows it rather than surfacing a
    /// new exception from a documented no-op.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_wakeup", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerWakeup(SafeConsumerHandle consumer);

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
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_group_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadata(SafeConsumerHandle consumer);

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

    // ---- kafka_consumer_MetricMap_t + client_id — sync state reads (ffi §B2/§B3, M9/P2) ----

    // Managed mirror of the value-kind discriminator returned by
    // MetricMap_get_value_kind (a plain int32_t — the generated header has no enums,
    // src/ffi/consumer.rs KAFKA_CONSUMER_METRIC_VALUE_*). It selects which get_value_*
    // accessor is valid, and thus the boxed CLR type of the public IMetric.Value.

    /// <summary>Value kind: use <see cref="MetricMapGetValueDouble"/> (<see cref="double"/>).</summary>
    internal const int MetricValueKindDouble = 0;

    /// <summary>Value kind: use <see cref="MetricMapGetValueString"/> (<see cref="string"/>).</summary>
    internal const int MetricValueKindString = 1;

    /// <summary>Value kind: use <see cref="MetricMapGetValueLong"/> (<see cref="long"/>).</summary>
    internal const int MetricValueKindLong = 2;

    /// <summary>Value kind: use <see cref="MetricMapGetValueInt"/> (<see cref="int"/>).</summary>
    internal const int MetricValueKindInt = 3;

    /// <summary>
    /// <c>kafka_consumer_Consumer_metrics</c> — returns an owned (Category-3)
    /// metric-map handle (a point-in-time snapshot), or <see cref="IntPtr.Zero"/> on a
    /// concurrent-access rejection. Free it with <see cref="MetricMapDestroy"/> after
    /// reading; every borrowed string it hands out dies with it.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_metrics", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerMetrics(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_count</c> — the number of metric entries in the map.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MetricMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_name</c> — the metric name at <paramref name="index"/>
    /// as a NUL-terminated <c>const char*</c> borrowed from the map (copy via
    /// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy), or
    /// <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMapGetName(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_group</c> — the metric group at
    /// <paramref name="index"/> (borrowed NUL-terminated <c>const char*</c>), or
    /// <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_group", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMapGetGroup(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_description</c> — the metric description at
    /// <paramref name="index"/> (borrowed NUL-terminated <c>const char*</c>), or
    /// <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_description", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMapGetDescription(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_tag_count</c> — the number of tags on the metric
    /// at <paramref name="index"/>, or <c>-1</c> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_tag_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MetricMapGetTagCount(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_tag_key</c> — the <paramref name="tagIndex"/>-th
    /// tag key of the metric at <paramref name="index"/> (borrowed NUL-terminated
    /// <c>const char*</c>), or <see cref="IntPtr.Zero"/> if either index is out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_tag_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMapGetTagKey(IntPtr map, int index, int tagIndex);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_tag_value</c> — the <paramref name="tagIndex"/>-th
    /// tag value of the metric at <paramref name="index"/> (borrowed NUL-terminated
    /// <c>const char*</c>), or <see cref="IntPtr.Zero"/> if either index is out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_tag_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMapGetTagValue(IntPtr map, int index, int tagIndex);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_value_kind</c> — which <c>get_value_*</c> accessor
    /// is valid for the metric at <paramref name="index"/> (one of the
    /// <c>MetricValueKind*</c> constants). Defaults to
    /// <see cref="MetricValueKindDouble"/> when out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_value_kind", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MetricMapGetValueKind(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_value_double</c> — the <see cref="double"/> reading
    /// of the metric at <paramref name="index"/> (<c>0.0</c> if out of range or a different
    /// kind).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_value_double", CallingConvention = CallingConvention.Cdecl)]
    internal static extern double MetricMapGetValueDouble(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_value_string</c> — the <see cref="string"/> reading
    /// of the metric at <paramref name="index"/> as a borrowed NUL-terminated
    /// <c>const char*</c> (copy before destroy), or <see cref="IntPtr.Zero"/> if out of
    /// range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_value_string", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMapGetValueString(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_value_long</c> — the <see cref="long"/> (<c>Int64</c>)
    /// reading of the metric at <paramref name="index"/> (<c>0</c> if out of range or a
    /// different kind).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_value_long", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long MetricMapGetValueLong(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_get_value_int</c> — the <see cref="int"/> (<c>Int32</c>)
    /// reading of the metric at <paramref name="index"/> (<c>0</c> if out of range or a
    /// different kind).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_get_value_int", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MetricMapGetValueInt(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_MetricMap_destroy</c> — frees an owned metric-map handle.
    /// Null-safe (no-op). Every borrowed string handed out by the accessors is invalid
    /// after this.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MetricMap_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MetricMapDestroy(IntPtr map);

    /// <summary>
    /// <c>kafka_consumer_Consumer_client_id</c> — the client id as an <b>owned</b>
    /// NUL-terminated <c>char*</c> (copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/>
    /// then free with <see cref="ConsumerStringDestroy"/>), or <see cref="IntPtr.Zero"/>
    /// on a concurrent-access rejection. <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1) — the owned <c>char*</c> it returns is unaffected and is
    /// still copied out then freed exactly once (invariant I4).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_client_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerClientId(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_string_destroy</c> — frees an owned <c>char*</c> returned by the
    /// ABI (e.g. <see cref="ConsumerClientId"/>). Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_string_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerStringDestroy(IntPtr s);

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

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_serialized_key_size</c> — the serialized,
    /// uncompressed key size in bytes, or <c>-1</c> if the key is null (a plain
    /// <c>int32_t</c> scalar — no borrowed pointer, so nothing to marshal or free).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_serialized_key_size", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordSerializedKeySize(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_serialized_value_size</c> — the serialized,
    /// uncompressed value size in bytes, or <c>-1</c> if the value is null (a plain
    /// <c>int32_t</c> scalar — no borrowed pointer, so nothing to marshal or free).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_serialized_value_size", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordSerializedValueSize(IntPtr record);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRecord_leader_epoch</c> — the leader epoch via
    /// <paramref name="outEpoch"/>; returns <see langword="true"/> and writes the epoch
    /// when present, or <see langword="false"/> (leaving <paramref name="outEpoch"/>
    /// untouched) when absent — legacy record formats (→ <c>int?</c> null). The 1-byte
    /// C <c>bool</c> return needs <c>[MarshalAs(I1)]</c> (§0.1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_leader_epoch", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool ConsumerRecordLeaderEpoch(IntPtr record, out int outEpoch);

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
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assign", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerAssign(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

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
        SafeConsumerHandle consumer,
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
        SafeConsumerHandle consumer,
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
        SafeConsumerHandle consumer,
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
    internal static extern IntPtr MockConsumerSetPollError(SafeConsumerHandle consumer, IntPtr message);

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_rebalance</c> — simulates a rebalance on a mock
    /// consumer (mock only; mirrors Java <c>MockConsumer.rebalance(Collection)</c>), the
    /// broker-free driver for <see cref="IConsumerRebalanceListener"/>.
    /// <paramref name="topics"/> / <paramref name="partitions"/> are the familiar
    /// <b>parallel arrays</b> of length <paramref name="count"/> (as
    /// <see cref="ConsumerAssign"/>) describing the <b>new full assignment</b> — <em>not</em>
    /// a <c>TopicPartitionList_t</c>. Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success), including <c>illegal_state</c> for a real consumer.
    /// <para>
    /// Semantics the core pins (<c>confluent_kafka.h:1228-1250</c>): it requires a
    /// <b>topic subscription</b> (a manually assigned consumer fails with "manual assignment
    /// in use"); it fires <c>on_partitions_revoked</c> only when something is removed, and
    /// <c>on_partitions_assigned</c> unconditionally with the <em>added</em> list (possibly
    /// empty) while a listener is registered; it <b>never</b> fires
    /// <c>on_partitions_lost</c>; and it <b>does not return until the callbacks have
    /// returned</b>, propagating a callback's error as its own return value — which is what
    /// makes the "rebalance does not advance until the listener returns" regression test
    /// (consumer-threading.md §31 #2) directly observable.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_rebalance", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerRebalance(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

    // ---- kafka_consumer_ConsumerRebalanceListener_t — the multi-shot registration (M9/P6) ----
    //
    // Java's `subscribe(Collection, ConsumerRebalanceListener)`. Unlike every other callback
    // in this file these are NOT one-shot per-operation completions: one registration fires N
    // times, and the `user_data` GCHandle is freed by the release hook alone — never by a
    // listener callback (see ListenerRegistration).

    /// <summary>
    /// <c>kafka_consumer_ConsumerRebalanceListener_new</c> — builds the listener handle that
    /// <see cref="ConsumerSubscribeWithListener"/> / <see cref="ConsumerSubscribeWithListenerAsync"/>
    /// consume. <paramref name="onPartitionsRevoked"/> and
    /// <paramref name="onPartitionsAssigned"/> are required; <paramref name="onPartitionsLost"/>
    /// and <paramref name="userDataDestroy"/> are nullable (pass <see langword="null"/> for the
    /// ABI's <c>NULL</c>). Passing <c>NULL</c> for lost reproduces Java's default (delegate to
    /// revoked) inside the core — this binding instead always supplies all three, because the
    /// Java default lives on <see cref="ConsumerRebalanceListenerBase"/> here (the
    /// netstandard2.0 floor has no default interface methods). <paramref name="userData"/>
    /// ownership transfers to the listener.
    /// <para>
    /// <b>The two nullable parameters are spelled inline as raw function pointers in the
    /// header</b>, not via their <c>_t</c> aliases — cbindgen only emits a nullable C function
    /// pointer for a literally-written <c>Option&lt;fn&gt;</c>. The C signature is identical
    /// either way, so the strongly-typed delegates below bind correctly.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRebalanceListenerNew(
        ConsumerCallbacks.RebalanceListenerCallback onPartitionsRevoked,
        ConsumerCallbacks.RebalanceListenerCallback onPartitionsAssigned,
        ConsumerCallbacks.RebalanceListenerCallback? onPartitionsLost,
        IntPtr userData,
        ConsumerCallbacks.ListenerUserDataDestroyCallback? userDataDestroy);

    /// <summary>
    /// <c>kafka_consumer_ConsumerRebalanceListener_destroy</c> — releases a listener handle
    /// that was <b>never passed to a subscribe call</b> (firing its <c>user_data_destroy</c>
    /// hook). Null-safe.
    /// <para>
    /// <b>A listener handed to either subscribe has already been consumed — destroying it
    /// afterwards is a double free</b> (<c>confluent_kafka.h:2081-2094</c>), and consumption
    /// is <b>unconditional, including on the error path</b>. The only sanctioned call site is
    /// therefore the narrow "the subscribe P/Invoke itself threw, so native never ran" catch.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRebalanceListenerDestroy(IntPtr listener);

    /// <summary>
    /// <c>kafka_consumer_Consumer_subscribe_with_listener</c> — Java's
    /// <c>subscribe(Collection&lt;String&gt;, ConsumerRebalanceListener)</c> (sync).
    /// <paramref name="topics"/> is the parallel array of pinned NUL-terminated UTF-8
    /// <c>const char*</c> read synchronously during the call (call-scoped pin, ffi §A3), and
    /// <paramref name="listener"/> is <b>consumed unconditionally</b>. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1) — load-bearing here, since
    /// the listener callbacks fire <em>inside</em> this call and that reference is what keeps
    /// a concurrent teardown from racing them.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_with_listener", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSubscribeWithListener(
        SafeConsumerHandle consumer, IntPtr[] topics, int count, IntPtr listener);

    /// <summary>
    /// <c>kafka_consumer_Consumer_subscribe_with_listener_async</c> — the async form of
    /// <see cref="ConsumerSubscribeWithListener"/>, sharing the void-result
    /// <c>op_callback_t</c> of <see cref="ConsumerSubscribeAsync"/> (null error = success).
    /// The topic strings are read synchronously before the op is spawned, so the pins stay
    /// call-scoped; <paramref name="listener"/> is <b>consumed unconditionally</b>.
    /// <paramref name="consumer"/> stays a raw <see cref="IntPtr"/> like every other
    /// <c>_async</c> submit: it needs the <b>span-the-op</b> reference taken explicitly at
    /// submit and released in <c>FreeGcHandle</c>, which a call-scoped marshaller AddRef
    /// cannot express.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_with_listener_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSubscribeWithListenerAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int count,
        IntPtr listener,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    // ---- Sync consumer state reads + enforce_rebalance (M5/P1, ffi §B2/§B5) ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_assignment</c> — the current assignment as an owned
    /// (Category-3) <c>TopicPartitionList_t</c> borrow-root, or <see cref="IntPtr.Zero"/>
    /// on a concurrent-access rejection (the core's own guard could not be acquired). Map
    /// the null to <see cref="InvalidOperationException"/> (ffi §B5), else copy every
    /// element out and free the root with <see cref="TopicPartitionListDestroy"/>
    /// (<see cref="TopicPartitionListMarshal"/>).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assignment", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerAssignment(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_subscription</c> — the current topic subscription as an
    /// owned (Category-3) <c>StringList_t</c> borrow-root, or <see cref="IntPtr.Zero"/> on
    /// a concurrent-access rejection. Copy out then free with
    /// <see cref="StringListDestroy"/> (<see cref="StringListMarshal"/>).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscription", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSubscription(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_paused</c> — the currently paused partitions as an owned
    /// (Category-3) <c>TopicPartitionList_t</c> borrow-root, or <see cref="IntPtr.Zero"/>
    /// on a concurrent-access rejection. Same accessors as
    /// <see cref="ConsumerAssignment"/>.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_paused", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPaused(SafeConsumerHandle consumer);

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
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_enforce_rebalance", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerEnforceRebalance(SafeConsumerHandle consumer, IntPtr reason);

    // ---- Sync seek + current lag (ffi §B5) — M5/P7 ----

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek</c> — seeks <c>(topic, partition)</c> to
    /// <paramref name="offset"/> (sync). <paramref name="topic"/> is a pinned
    /// NUL-terminated UTF-8 buffer read <b>synchronously</b> during the call
    /// (call-scoped pin). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success) consumed by <see cref="KafkaException.FromHandle(IntPtr)"/> — the
    /// shipped <see cref="ConsumerEnforceRebalance"/> sync-op shape. Seeking an unassigned
    /// partition is a genuine broker-free failure (a non-null error handle).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSeek(SafeConsumerHandle consumer, IntPtr topic, int partition, long offset);

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek_with_metadata</c> — seeks <c>(topic, partition)</c>
    /// to <paramref name="offset"/> carrying a commit metadata string and leader epoch
    /// (sync). <paramref name="topic"/> and <paramref name="metadata"/> are pinned
    /// NUL-terminated UTF-8 buffers read <b>synchronously</b> during the call (call-scoped
    /// pins). Per the header contract, <paramref name="leaderEpoch"/> <c>&lt; 0</c> means
    /// "no leader epoch" and <paramref name="metadata"/> == <see cref="IntPtr.Zero"/> means
    /// "no metadata" — the binding always passes a valid pointer, since
    /// <see cref="OffsetAndMetadata.Metadata"/> is never null. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success), the same sync-op shape as
    /// <see cref="ConsumerSeek"/>. <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_with_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSeekWithMetadata(
        SafeConsumerHandle consumer, IntPtr topic, int partition, long offset, int leaderEpoch, IntPtr metadata);

    /// <summary>
    /// <c>kafka_consumer_Consumer_current_lag</c> — the current lag of
    /// <c>(topic, partition)</c> (sync, never blocks — a local read). <paramref name="topic"/>
    /// is a pinned NUL-terminated UTF-8 buffer read during the call. Writes the lag to
    /// <paramref name="outLag"/> and returns <see langword="true"/> when the lag is known;
    /// returns <see langword="false"/> when the lag is unknown OR the access guard could not
    /// be acquired — the binding maps <b>both</b> to <c>null</c> (Java's
    /// <c>OptionalLong.empty</c> / the Python sibling). There is no error handle. The
    /// <c>bool</c> return is marshalled as <see cref="UnmanagedType.I1"/> (a 1-byte C bool,
    /// not a 4-byte Win32 <c>BOOL</c>; ffi §0.1) — the <c>I1</c> is preserved across the
    /// M9/P4 H1 parameter retype. <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_current_lag", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool ConsumerCurrentLag(
        SafeConsumerHandle consumer, IntPtr topic, int partition, out long outLag);

    // ---- Sync consumer ops (the blocking mirror of the async surface) — M5/P8a ----
    //
    // The synchronous variant of every blocking-in-Java op, each calling the sync C ABI
    // DIRECTLY (NO completion callback, NO GCHandle): the core's block_on parks the caller
    // thread inside the Rust multi-thread runtime (deadlock-free, ffi §B1) — the shipped
    // Seek / CurrentLag / EnforceRebalance sync-op precedent, NOT sync-over-async. The
    // result ops return a kafka_common_KafkaError_t handle (null = success) consumed by
    // KafkaException.FromHandle; poll additionally returns a ConsumerRecords_t* + an
    // out_error; position writes the offset to an out param. The parallel-array input
    // shapes are IDENTICAL to the async DllImports above (same call-scoped pinning).
    // Consumer_assign (sync), Consumer_close, and Consumer_close_with_timeout are declared
    // above and reused. The blocking sync Consumer_poll observes Consumer_wakeup — its
    // block_on drives the SAME poll() future the async path awaits, and wakeup fires the
    // same rotating token — so a cross-thread Wakeup() faults a blocking Poll (one-shot).

    /// <summary>
    /// <c>kafka_consumer_Consumer_poll</c> — polls for records (sync). Drives the consumer's
    /// <c>poll(timeout)</c> under the access guard via <c>block_on</c>. Returns a non-null
    /// <c>ConsumerRecords_t</c> (Category-3 borrow-root; free with
    /// <see cref="ConsumerRecordsDestroy"/> after copy-out) on success with
    /// <paramref name="outError"/> null; on failure returns null with
    /// <paramref name="outError"/> set to a <c>kafka_common_KafkaError_t</c> consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>. A <c>wakeup()</c> from another thread
    /// makes the in-flight poll return a Wakeup error (one-shot). <paramref name="timeoutMs"/>
    /// is the Java <c>Duration</c> → <c>int64_t</c> ms.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1) — <b>the highest-value site
    /// of that migration</b>: this call parks inside the core's <c>block_on</c> for the
    /// caller-supplied timeout, so before H1 a concurrent <c>Dispose</c> from the wakeup
    /// thread could free the consumer for up to that entire window while native was still
    /// executing on it.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_poll", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPoll(SafeConsumerHandle consumer, long timeoutMs, out IntPtr outError);

    /// <summary>
    /// <c>kafka_consumer_Consumer_subscribe</c> — subscribes to <paramref name="count"/>
    /// topics (sync). <paramref name="topics"/> is the parallel array of pinned NUL-terminated
    /// UTF-8 <c>const char*</c> (= <c>const char* const*</c>) read synchronously during the
    /// call (call-scoped pin, ffi §A3). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success) consumed by <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSubscribe(SafeConsumerHandle consumer, IntPtr[] topics, int count);

    /// <summary>
    /// <c>kafka_consumer_Consumer_unsubscribe</c> — unsubscribes from all topics / partitions
    /// (sync). Returns a <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_unsubscribe", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerUnsubscribe(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_pause</c> — pauses fetching for the <paramref name="count"/>
    /// <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success). <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/>
    /// so the marshaller holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_pause", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPause(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_Consumer_resume</c> — resumes fetching for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success). <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/>
    /// so the marshaller holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_resume", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerResume(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek_to_beginning</c> — requests an EARLIEST offset reset for
    /// the <paramref name="count"/> <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_beginning", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSeekToBeginning(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_Consumer_seek_to_end</c> — requests a LATEST offset reset for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_end", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSeekToEnd(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_Consumer_position</c> — the current position of
    /// <c>(topic, partition)</c> (sync). On success writes the offset to
    /// <paramref name="outPosition"/> and returns null; on failure returns a non-null
    /// <c>kafka_common_KafkaError_t</c> handle (consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>) and leaves
    /// <paramref name="outPosition"/> untouched. <paramref name="topic"/> is a pinned
    /// NUL-terminated UTF-8 buffer read synchronously during the call.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_position", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPosition(
        SafeConsumerHandle consumer, IntPtr topic, int partition, out long outPosition);

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_sync</c> — commits the current positions (sync; Java
    /// <c>commitSync()</c>). No offsets argument means commit the current positions. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitSync(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_sync_offsets</c> — commits specific offsets (sync;
    /// Java <c>commitSync(Map)</c>) from the parallel input arrays <c>(topics[],
    /// partitions[], offsets[], leader_epochs[], metadata[], count)</c>. Per the header
    /// contract, <paramref name="metadata"/> entries may be null and a
    /// <paramref name="leaderEpochs"/> entry <c>&lt; 0</c> means "no epoch". Both string
    /// arrays map C's <c>const char* const*</c> (same shape as
    /// <see cref="ConsumerCommitSyncOffsetsAsync"/>). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitSyncOffsets(
        SafeConsumerHandle consumer,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        int[] leaderEpochs,
        IntPtr[] metadata,
        int count);

    // ---- Sync consumer query family (owned-container out-param, ffi §B2) — M5/P8b ----
    //
    // The synchronous variant of the six blocking-in-Java query ops, each calling the sync C
    // ABI DIRECTLY (NO completion callback, NO GCHandle): the core's block_on parks the caller
    // thread inside the Rust multi-thread runtime (deadlock-free, ffi §B1) — the shipped M5/P8a
    // sync core-loop precedent, NOT sync-over-async. Each returns a kafka_common_KafkaError_t
    // handle (null = success) consumed by KafkaException.FromHandle AND writes an owned
    // container handle to an out-param on success. Per the header contract, on FAILURE the
    // out-param is LEFT UNTOUCHED — the binding pre-initializes it to IntPtr.Zero, so the
    // container _destroy (null-safe) is a no-op on the error path. The parallel-array input
    // shapes are IDENTICAL to the async DllImports above (same call-scoped pinning); the only
    // difference is the KafkaError* return + out-param handle in place of the async callback.

    /// <summary>
    /// <c>kafka_consumer_Consumer_committed</c> — the last committed offsets for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). On success writes an owned <c>OffsetMap_t</c>
    /// (Category-3 borrow-root; free with <see cref="OffsetMapDestroy"/> after copy-out) to
    /// <paramref name="outMap"/> and returns null; on failure returns a non-null
    /// <c>kafka_common_KafkaError_t</c> handle (consumed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>) and leaves <paramref name="outMap"/>
    /// untouched. The sync mirror of <see cref="ConsumerCommittedAsync"/>.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_committed", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitted(
        SafeConsumerHandle consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_Consumer_offsets_for_times</c> — offsets by timestamp for the parallel
    /// <c>(topics[], partitions[], timestamps[], count)</c> arrays (sync). On success writes an
    /// owned <c>OffsetAndTimestampMap_t</c> (Category-3; free with
    /// <see cref="OffsetAndTimestampMapDestroy"/>) to <paramref name="outMap"/> and returns
    /// null; on failure returns a non-null error handle and leaves <paramref name="outMap"/>
    /// untouched. Negative timestamps are Kafka-valid sentinels (EARLIEST/LATEST) and are passed
    /// through, not rejected. The sync mirror of <see cref="ConsumerOffsetsForTimesAsync"/>.
    /// <paramref name="consumer"/> is the <see cref="SafeConsumerHandle"/> so the marshaller
    /// holds a reference for the whole call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_offsets_for_times", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerOffsetsForTimes(
        SafeConsumerHandle consumer,
        IntPtr[] topics,
        int[] partitions,
        long[] timestamps,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_Consumer_beginning_offsets</c> — the earliest offsets for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). On success writes an owned <c>LongOffsetMap_t</c>
    /// (Category-3; free with <see cref="LongOffsetMapDestroy"/>) to <paramref name="outMap"/>
    /// and returns null; on failure returns a non-null error handle and leaves
    /// <paramref name="outMap"/> untouched. The sync mirror of
    /// <see cref="ConsumerBeginningOffsetsAsync"/>. <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_beginning_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerBeginningOffsets(
        SafeConsumerHandle consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_Consumer_end_offsets</c> — the latest offsets for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (sync; parallel arrays as
    /// <see cref="ConsumerAssign"/>). The LATEST analog of <see cref="ConsumerBeginningOffsets"/>,
    /// sharing the <c>LongOffsetMap_t</c> result. On success writes it to
    /// <paramref name="outMap"/> and returns null; on failure returns a non-null error handle and
    /// leaves <paramref name="outMap"/> untouched. <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_end_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerEndOffsets(
        SafeConsumerHandle consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_Consumer_partitions_for</c> — the partition metadata for
    /// <paramref name="topic"/> (sync). <paramref name="topic"/> is a pinned NUL-terminated
    /// UTF-8 buffer read synchronously during the call. On success writes an owned
    /// <c>PartitionInfoList_t</c> (Category-3; free with <see cref="PartitionInfoListDestroy"/>)
    /// to <paramref name="outList"/> and returns null; on failure returns a non-null error handle
    /// and leaves <paramref name="outList"/> untouched. The sync mirror of
    /// <see cref="ConsumerPartitionsForAsync"/>. <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_partitions_for", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPartitionsFor(
        SafeConsumerHandle consumer, IntPtr topic, out IntPtr outList);

    /// <summary>
    /// <c>kafka_consumer_Consumer_list_topics</c> — metadata for all topics the consumer is
    /// authorized to view (sync). Takes <b>no input</b>. On success writes an owned
    /// <c>TopicPartitionInfoMap_t</c> (Category-3; free with
    /// <see cref="TopicPartitionInfoMapDestroy"/>) to <paramref name="outMap"/> and returns null;
    /// on failure returns a non-null error handle and leaves <paramref name="outMap"/> untouched.
    /// The sync mirror of <see cref="ConsumerListTopicsAsync"/>. <paramref name="consumer"/> is
    /// the <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_list_topics", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerListTopics(SafeConsumerHandle consumer, out IntPtr outMap);

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

    // ---- Async offset-map queries (owned-handle completion, ffi §B6/§B7) — M5/P4 ----
    //
    // All four resolve via the owned-handle completion shape (the poll analog):
    // on success `map` is a non-null owned container (a Category-3 borrow-root, freed
    // by the trampoline's copy-out-then-destroy) and `error` is null; on failure (incl.
    // the inline core-guard rejection) `map` is null and `error` is non-null. The
    // callback takes ownership of whichever is non-null. `beginning`/`end` share the one
    // long-offsets callback. All input arrays are pinned call-scoped (the core reads them
    // synchronously into an owned Vec before spawning, ffi §A4/§B4).

    /// <summary>
    /// <c>kafka_consumer_Consumer_committed_async</c> — the last committed offsets for
    /// <c>(topics[], partitions[], count)</c> asynchronously (one-operation-in-flight).
    /// The completion fires via <paramref name="callback"/> with an owned
    /// <c>OffsetMap_t</c> (Category-3), copied out then destroyed by the trampoline.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_committed_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommittedAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OffsetMapCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_sync_async</c> — commits the current positions
    /// asynchronously (Java <c>commitSync()</c>; the async dispatch of the sync
    /// <c>commit_sync</c>). One-operation-in-flight; reuses the same void-result completion
    /// callback as <see cref="ConsumerSubscribeAsync"/> / <see cref="ConsumerUnsubscribeAsync"/>
    /// (null error = success), taking no offsets. <paramref name="userData"/> is a
    /// <see cref="System.Runtime.InteropServices.GCHandle"/> over the per-op context (M5/P6).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncAsync(
        IntPtr consumer,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_sync_offsets_async</c> — commits specific offsets
    /// asynchronously (Java <c>commitSync(Map)</c>) from the parallel input arrays
    /// <c>(topics[], partitions[], offsets[], leader_epochs[], metadata[], count)</c>. Per
    /// the header contract, <paramref name="metadata"/> entries may be null and a
    /// <paramref name="leaderEpochs"/> entry <c>&lt; 0</c> means "no epoch". Both string
    /// arrays map C's <c>const char*const*</c> (same as <see cref="ConsumerCommittedAsync"/>'s
    /// <paramref name="topics"/>). One-operation-in-flight; reuses the void-result completion
    /// callback (null error = success). <paramref name="userData"/> is a
    /// <see cref="System.Runtime.InteropServices.GCHandle"/> over the per-op context (M5/P6).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_offsets_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncOffsetsAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        int[] leaderEpochs,
        IntPtr[] metadata,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_async</c> — commits the consumed offsets
    /// fire-and-forget (Java <c>commitAsync()</c>). A <b>sync</b> call that returns once the
    /// async commit is initiated, yielding a <c>kafka_common_KafkaError_t*</c>
    /// (<see cref="IntPtr"/>) — null = success, non-null = error (the shipped
    /// <see cref="ConsumerEnforceRebalance"/> sync-op shape). No callback, no offsets, no
    /// user data (M5/P6). <paramref name="consumer"/> is the
    /// <see cref="SafeConsumerHandle"/> so the marshaller holds a reference for the whole
    /// call (ffi §A2; M9/P4 H1) — note this is an <c>_async</c> <b>name</b> but a
    /// <b>sync</b> ABI function, so it takes the call-scoped sync convention, not the
    /// span-the-op one the 18 genuine <c>_async</c> declarations use.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitAsync(SafeConsumerHandle consumer);

    // ---- The commit-callback registrations (M9/P7) — Java commitAsync(cb) / (Map, cb) ----
    //
    // Both are SYNC ABI functions (they return a KafkaError* the moment the commit is
    // *initiated*), so both take the SafeConsumerHandle parameter per the M9/P4 H1 sync
    // convention — load-bearing, because on a MockConsumer the completion callback fires
    // INLINE inside the call and the marshaller's reference is what keeps a concurrent
    // teardown from racing it.
    //
    // ⚠ THREE asymmetries that the shipped one-shot completions do NOT have:
    //   * `callback` is spelled as the NON-nullable `_t` alias in both, and there is NO plain
    //     `Consumer_commit_async_offsets`. Passing null for it is UB, so a callback-less
    //     commit-with-offsets must supply ConsumerCallbacks.CommitDiscard.
    //   * `user_data_destroy` IS present (the ~8 one-shot ops have no such hook) and is the
    //     single free site for the registration GCHandle — see CommitCallbackRegistration.
    //   * the callback is NOT invoked when the call returns an error, but the hook still is.

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_async_with_callback</c> — commits the consumed
    /// offsets, notifying <paramref name="callback"/> when the commit completes (Java
    /// <c>commitAsync(OffsetCommitCallback)</c>). A <b>sync</b> call returning a
    /// <c>kafka_common_KafkaError_t*</c> (null = success) as soon as the commit is
    /// <em>initiated</em>; the commit's own outcome arrives at
    /// <paramref name="callback"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="callback"/> is <b>not nullable</b> (the header spells it as the
    /// <c>_t</c> alias) and fires <b>exactly once per successful call</b>, on the consumer's
    /// dispatcher thread — inline during the call on a <c>MockConsumer</c>. It <b>owns</b> the
    /// delivered <c>OffsetMap_t</c> and any non-null <c>KafkaError_t</c>.
    /// </para>
    /// <para>
    /// <paramref name="userData"/> ownership transfers <b>unconditionally</b>:
    /// <paramref name="userDataDestroy"/> fires exactly once, on an unspecified thread,
    /// <b>even when this function returns an error</b>. Pass <see langword="null"/> for the
    /// hook only when there is nothing managed to release (the discard path).
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitAsyncWithCallback(
        SafeConsumerHandle consumer,
        ConsumerCallbacks.CommitCallback callback,
        IntPtr userData,
        ConsumerCallbacks.CommitUserDataDestroyCallback? userDataDestroy);

    /// <summary>
    /// <c>kafka_consumer_Consumer_commit_async_offsets_with_callback</c> — commits the
    /// specific offsets in the parallel input arrays <c>(topics[], partitions[], offsets[],
    /// leader_epochs[], metadata[], count)</c>, notifying <paramref name="callback"/> when the
    /// commit completes (Java <c>commitAsync(Map, OffsetCommitCallback)</c>). The array shape
    /// is byte-identical to <see cref="ConsumerCommitSyncOffsets"/>, so the shipped
    /// five-array pin path marshals it unchanged.
    /// </summary>
    /// <remarks>
    /// Same callback / <c>user_data</c> contracts as
    /// <see cref="ConsumerCommitAsyncWithCallback"/>, plus one of its own: if the offsets fail
    /// to marshal (e.g. a negative offset) this returns the error <b>without registering the
    /// callback</b> — <paramref name="callback"/> never fires, but
    /// <paramref name="userDataDestroy"/> still does. That asymmetry is exactly why the hook,
    /// not the callback, is the registration's free site.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_offsets_with_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitAsyncOffsetsWithCallback(
        SafeConsumerHandle consumer,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        int[] leaderEpochs,
        IntPtr[] metadata,
        int count,
        ConsumerCallbacks.CommitCallback callback,
        IntPtr userData,
        ConsumerCallbacks.CommitUserDataDestroyCallback? userDataDestroy);

    /// <summary>
    /// <c>kafka_consumer_Consumer_offsets_for_times_async</c> — offsets by timestamp for
    /// the parallel <c>(topics[], partitions[], timestamps[], count)</c> arrays
    /// asynchronously (one-operation-in-flight). The completion fires via
    /// <paramref name="callback"/> with an owned <c>OffsetAndTimestampMap_t</c>
    /// (Category-3), copied out then destroyed by the trampoline. The extra
    /// <paramref name="timestamps"/> array (blittable <c>int64_t</c>) is the only input
    /// shape difference from the other three; negative timestamps are Kafka-valid
    /// sentinels (EARLIEST/LATEST) and are passed through, not rejected.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_offsets_for_times_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerOffsetsForTimesAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        long[] timestamps,
        int count,
        ConsumerCallbacks.OffsetAndTimestampMapCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_beginning_offsets_async</c> — the earliest offsets for
    /// <c>(topics[], partitions[], count)</c> asynchronously (one-operation-in-flight).
    /// The completion fires via <paramref name="callback"/> (the shared long-offsets
    /// shape) with an owned <c>LongOffsetMap_t</c> (Category-3), copied out then
    /// destroyed by the trampoline.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_beginning_offsets_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerBeginningOffsetsAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.LongOffsetMapCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_end_offsets_async</c> — the latest offsets for
    /// <c>(topics[], partitions[], count)</c> asynchronously (one-operation-in-flight).
    /// The LATEST analog of <see cref="ConsumerBeginningOffsetsAsync"/>; it shares the
    /// same <c>long_offsets_callback_t</c> and owned <c>LongOffsetMap_t</c> result.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_end_offsets_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerEndOffsetsAsync(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.LongOffsetMapCallback callback,
        IntPtr userData);

    // ---- OffsetMap_t — owned borrow-root + borrowed key/value elements (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_OffsetMap_count</c> — the number of entries in the owned map.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int OffsetMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_consumer_OffsetMap_get_key</c> — the <c>TopicPartition_t</c> key at
    /// <paramref name="index"/>, <b>borrowed</b> (Category 4; valid until the map is
    /// destroyed), or <see cref="IntPtr.Zero"/> if out of range. Never freed by the
    /// binding.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetMap_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetMapGetKey(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_OffsetMap_get_value</c> — the <c>OffsetAndMetadata_t</c> value at
    /// <paramref name="index"/>, <b>borrowed</b> (Category 4; valid until the map is
    /// destroyed), or <see cref="IntPtr.Zero"/> if out of range. Never freed by the
    /// binding.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetMap_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetMapGetValue(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_OffsetMap_destroy</c> — frees the owned map root (every borrowed
    /// key/value element from it is invalidated). Null-safe (no-op). Called by the
    /// trampoline <b>after</b> the copy-out completes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetMap_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OffsetMapDestroy(IntPtr map);

    // ---- OffsetAndTimestampMap_t — owned borrow-root + borrowed elements (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestampMap_count</c> — the number of entries.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestampMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int OffsetAndTimestampMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestampMap_get_key</c> — the borrowed
    /// <c>TopicPartition_t</c> key at <paramref name="index"/> (Category 4), or
    /// <see cref="IntPtr.Zero"/> if out of range. Never freed by the binding.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestampMap_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetAndTimestampMapGetKey(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestampMap_get_value</c> — the borrowed
    /// <c>OffsetAndTimestamp_t</c> value at <paramref name="index"/> (Category 4), or
    /// <see cref="IntPtr.Zero"/> if out of range. Never freed by the binding.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestampMap_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetAndTimestampMapGetValue(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestampMap_destroy</c> — frees the owned map root
    /// (every borrowed element from it is invalidated). Null-safe (no-op). Called by the
    /// trampoline <b>after</b> the copy-out completes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestampMap_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OffsetAndTimestampMapDestroy(IntPtr map);

    // ---- LongOffsetMap_t — owned borrow-root; value is a by-value int64 (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_LongOffsetMap_count</c> — the number of entries.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_LongOffsetMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int LongOffsetMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_consumer_LongOffsetMap_get_key</c> — the borrowed <c>TopicPartition_t</c>
    /// key at <paramref name="index"/> (Category 4), or <see cref="IntPtr.Zero"/> if out
    /// of range. Never freed by the binding.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_LongOffsetMap_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LongOffsetMapGetKey(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_LongOffsetMap_get_value</c> — the offset at <paramref name="index"/>
    /// returned <b>by value</b> as an <c>int64_t</c> (no handle, nothing borrowed to copy
    /// out beyond the scalar). Returns 0 if out of range (guarded by the count).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_LongOffsetMap_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LongOffsetMapGetValue(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_LongOffsetMap_destroy</c> — frees the owned map root (every
    /// borrowed key element from it is invalidated). Null-safe (no-op). Called by the
    /// trampoline <b>after</b> the copy-out completes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_LongOffsetMap_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void LongOffsetMapDestroy(IntPtr map);

    // ---- OffsetAndMetadata_t / OffsetAndTimestamp_t — borrowed map-value accessors ----
    //
    // These value types are BORROWED elements of an owned OffsetMap_t /
    // OffsetAndTimestampMap_t (Category 4) — the binding NEVER destroys them; only the
    // owning map root is destroyed. The metadata string is NUL-terminated, handle-owned
    // (§B3 NUL-scan form), copied out before the map root is destroyed. The leader-epoch
    // accessor is the presence-flag pattern: a 1-byte C bool return (I1) plus an out
    // int32 — false ⇒ absent (→ int? null), true ⇒ *out_epoch (§0.1 I1 rule).

    /// <summary>
    /// <c>kafka_consumer_OffsetAndMetadata_offset</c> — the committed offset of a borrowed
    /// (Category-4) <c>OffsetAndMetadata_t</c> element.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long OffsetAndMetadataOffset(IntPtr oam);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndMetadata_metadata</c> — the commit metadata as a
    /// NUL-terminated <c>const char*</c> owned by the element (empty string when unset,
    /// never null). Copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before the map
    /// root is destroyed — the NUL-terminated form (§B3), NOT the length-delimited form.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetAndMetadataMetadata(IntPtr oam);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndMetadata_leader_epoch</c> — the leader epoch via
    /// <paramref name="outEpoch"/>; returns <see langword="false"/> when absent (→
    /// <c>int?</c> null). The 1-byte C <c>bool</c> return needs <c>[MarshalAs(I1)]</c>
    /// (§0.1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_leader_epoch", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool OffsetAndMetadataLeaderEpoch(IntPtr oam, out int outEpoch);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestamp_offset</c> — the resolved offset of a borrowed
    /// (Category-4) <c>OffsetAndTimestamp_t</c> element.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestamp_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long OffsetAndTimestampOffset(IntPtr oat);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestamp_timestamp</c> — the timestamp (milliseconds
    /// since epoch) of a borrowed (Category-4) <c>OffsetAndTimestamp_t</c> element.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestamp_timestamp", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long OffsetAndTimestampTimestamp(IntPtr oat);

    /// <summary>
    /// <c>kafka_consumer_OffsetAndTimestamp_leader_epoch</c> — the leader epoch via
    /// <paramref name="outEpoch"/>; returns <see langword="false"/> when absent (→
    /// <c>int?</c> null). The 1-byte C <c>bool</c> return needs <c>[MarshalAs(I1)]</c>
    /// (§0.1).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestamp_leader_epoch", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool OffsetAndTimestampLeaderEpoch(IntPtr oat, out int outEpoch);

    // ---- Async partition-metadata queries (owned-handle completion, ffi §B6/§B7) — M5/P5 ----
    //
    // Both resolve via the owned-handle completion shape (the poll analog): on success the
    // container is a non-null owned root (a Category-3 borrow-root, freed by the trampoline's
    // copy-out-then-destroy) and `error` is null; on failure (incl. the inline core-guard
    // rejection) the container is null and `error` is non-null. The callback takes ownership
    // of whichever is non-null. `partitions_for` pins its one topic call-scoped (the core
    // copies it synchronously during the submit, ffi §A3/§B3); `list_topics` takes no input.

    /// <summary>
    /// <c>kafka_consumer_Consumer_partitions_for_async</c> — the partition metadata for
    /// <paramref name="topic"/> asynchronously (one-operation-in-flight). The completion
    /// fires via <paramref name="callback"/> with an owned <c>PartitionInfoList_t</c>
    /// (Category-3), copied out then destroyed by the trampoline. <paramref name="topic"/>
    /// is a pinned NUL-terminated UTF-8 buffer, valid for the duration of the call (the
    /// core copies it synchronously during the submit).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_partitions_for_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPartitionsForAsync(
        IntPtr consumer,
        IntPtr topic,
        ConsumerCallbacks.PartitionInfoListCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_consumer_Consumer_list_topics_async</c> — metadata for all topics the
    /// consumer is authorized to view asynchronously (one-operation-in-flight). Takes
    /// <b>no input</b>. The completion fires via <paramref name="callback"/> with an owned
    /// <c>TopicPartitionInfoMap_t</c> (Category-3), copied out then destroyed by the
    /// trampoline.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_list_topics_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerListTopicsAsync(
        IntPtr consumer,
        ConsumerCallbacks.TopicPartitionInfoMapCallback callback,
        IntPtr userData);

    // ---- PartitionInfoList_t — owned borrow-root + borrowed PartitionInfo elements (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_PartitionInfoList_count</c> — the number of partition-info
    /// entries in the owned list. NOT null-safe (only <c>_destroy</c> is) — call only on a
    /// non-null root (the success branch of the trampoline).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfoList_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionInfoListCount(IntPtr list);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfoList_get</c> — the <c>PartitionInfo_t</c> at
    /// <paramref name="index"/>, <b>borrowed</b> (Category 4; a <c>const *</c> return valid
    /// until the list is destroyed), or <see cref="IntPtr.Zero"/> if out of range. Never
    /// freed by the binding (there is a <c>PartitionInfo_destroy</c>, but it is only for a
    /// standalone-owned info — a list element is a borrowed view; freeing it is a UAF).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfoList_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoListGet(IntPtr list, int index);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfoList_destroy</c> — frees the owned partition-info-list
    /// root (every borrowed <c>PartitionInfo</c> / <c>Node</c> / string from it is
    /// invalidated). Null-safe (no-op). Called by the trampoline <b>after</b> the copy-out
    /// completes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfoList_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void PartitionInfoListDestroy(IntPtr list);

    // ---- TopicPartitionInfoMap_t — owned borrow-root + borrowed topic / list elements (ffi §B2) ----

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionInfoMap_count</c> — the number of topics in the owned
    /// map. NOT null-safe (only <c>_destroy</c> is).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionInfoMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionInfoMap_get_topic</c> — the topic name at
    /// <paramref name="index"/> as a <b>NUL-terminated</b> <c>const char*</c> owned by the
    /// map (borrowed; valid until the map is destroyed), or <see cref="IntPtr.Zero"/> if out
    /// of range. Copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy —
    /// NUL-terminated form (§B3), NOT the length-delimited form (contrast
    /// <see cref="NodeHost"/> / <see cref="NodeRack"/>, which ARE length-delimited).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionInfoMap_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoMapGetTopic(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionInfoMap_get_partitions</c> — the
    /// <c>PartitionInfoList_t</c> for the topic at <paramref name="index"/>, <b>borrowed</b>
    /// (Category 4; a nested <c>const *</c> container valid until the map is destroyed), or
    /// <see cref="IntPtr.Zero"/> if out of range. Copied out (via
    /// <see cref="PartitionInfoListCount"/> / <see cref="PartitionInfoListGet"/>) before the
    /// map root is destroyed; <b>never</b> <c>PartitionInfoList_destroy</c>'d as a map value
    /// (it is borrowed here — only the map root is destroyed).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionInfoMap_get_partitions", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoMapGetPartitions(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_consumer_TopicPartitionInfoMap_destroy</c> — frees the owned map root (every
    /// borrowed topic string, nested list, and its <c>PartitionInfo</c> / <c>Node</c>
    /// elements are invalidated). Null-safe (no-op). Called by the trampoline <b>after</b>
    /// the copy-out completes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_TopicPartitionInfoMap_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TopicPartitionInfoMapDestroy(IntPtr map);

    // ---- PartitionInfo_t accessors — borrowed views into the owning root (ffi §B2 Category 4) ----
    //
    // Every accessor return is a borrowed view: scalars by value, `_leader` / `_replica(i)` /
    // etc. return `const Node_t*` (borrowed), `_topic` returns a NUL-terminated `const char*`
    // (borrowed, handle-owned). The binding NEVER calls PartitionInfo_destroy on a list/map
    // element (a borrowed Category-4 view — freeing it is a double-free/UAF). Copied out
    // before the owning root is destroyed.

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_topic</c> — the topic name as a <b>NUL-terminated</b>
    /// <c>const char*</c> owned by the handle (§B3 NUL-scan form). Copy via
    /// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before the owning root is destroyed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoTopic(IntPtr info);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_partition</c> — the partition id (by value).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionInfoPartition(IntPtr info);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_leader</c> — the leader <c>Node_t</c>, <b>borrowed</b>
    /// (Category 4), or <see cref="IntPtr.Zero"/> if the partition has no leader. Never
    /// freed (there is no <c>Node_destroy</c>). A null pointer maps to a null
    /// <see cref="Node"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_leader", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoLeader(IntPtr info);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_replica_count</c> — the number of replica nodes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionInfoReplicaCount(IntPtr info);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_replica</c> — the replica <c>Node_t</c> at
    /// <paramref name="index"/>, <b>borrowed</b> (Category 4), or <see cref="IntPtr.Zero"/>
    /// if out of range. Never freed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoReplica(IntPtr info, int index);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_in_sync_replica_count</c> — the number of in-sync
    /// replica nodes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_in_sync_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionInfoInSyncReplicaCount(IntPtr info);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_in_sync_replica</c> — the in-sync replica
    /// <c>Node_t</c> at <paramref name="index"/>, <b>borrowed</b> (Category 4), or
    /// <see cref="IntPtr.Zero"/> if out of range. Never freed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_in_sync_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoInSyncReplica(IntPtr info, int index);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_offline_replica_count</c> — the number of offline
    /// replica nodes.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_offline_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionInfoOfflineReplicaCount(IntPtr info);

    /// <summary>
    /// <c>kafka_consumer_PartitionInfo_offline_replica</c> — the offline replica
    /// <c>Node_t</c> at <paramref name="index"/>, <b>borrowed</b> (Category 4), or
    /// <see cref="IntPtr.Zero"/> if out of range. Never freed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_PartitionInfo_offline_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoOfflineReplica(IntPtr info, int index);

    // ---- Node_t (kafka_common_Node) accessors — borrowed views (ffi §B2 Category 4) ----
    //
    // A Node is a borrowed view: it has NO _destroy (freed with its owning PartitionInfo,
    // which is freed with the root container). `_id` / `_port` are scalars by value.
    // `_host` and `_rack` are LENGTH-DELIMITED (`const char*` + `out int32_t len`), NOT
    // NUL-terminated (§B3): use Utf8Marshal.PtrToString(ptr, len), NEVER a NUL-scan (the
    // slice borrows into the container with no terminator — a scan over-reads). `_rack`
    // returns (null, -1) when absent → Node.Rack == null.

    /// <summary>
    /// <c>kafka_common_Node_id</c> — the node (broker) id (by value).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_common_Node_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int NodeId(IntPtr node);

    /// <summary>
    /// <c>kafka_common_Node_host</c> — the node host as a <b>length-delimited</b>
    /// <c>(const char*, out int32_t len)</c> pair (NOT NUL-terminated; borrows into the
    /// owning <c>PartitionInfo</c>). Copy via <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/>
    /// using <paramref name="outLen"/> — NEVER a NUL-scan (§B3 over-read trap) — before the
    /// owning root is destroyed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_common_Node_host", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NodeHost(IntPtr node, out int outLen);

    /// <summary>
    /// <c>kafka_common_Node_port</c> — the node port (by value).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_common_Node_port", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int NodePort(IntPtr node);

    /// <summary>
    /// <c>kafka_common_Node_rack</c> — the node rack as a <b>length-delimited</b>
    /// <c>(const char*, out int32_t len)</c> pair, or <c>(null, -1)</c> when absent. Copy via
    /// <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> using <paramref name="outLen"/> —
    /// NEVER a NUL-scan (§B3) — before the owning root is destroyed. The absent
    /// <c>(null, -1)</c> maps to a null <see cref="Node.Rack"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_common_Node_rack", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NodeRack(IntPtr node, out int outLen);

    // ---- MockConsumer_update_partitions — mock-only partition-metadata driver (ffi §B5) ----

    /// <summary>
    /// <c>kafka_consumer_MockConsumer_update_partitions</c> — registers
    /// <paramref name="partitionCount"/> partitions for <paramref name="topic"/> on a mock
    /// consumer (mock only), each with a single leader node
    /// <c>(leaderId, leaderHost, leaderPort)</c> that also serves as its sole replica and
    /// in-sync replica (offline replicas empty, no rack). Drives
    /// <see cref="ConsumerPartitionsForAsync"/> / <see cref="ConsumerListTopicsAsync"/>
    /// broker-free. Both strings are pinned NUL-terminated UTF-8 buffers. Returns null on
    /// success, or a non-null error handle (incl. <c>illegal_state</c> for an async
    /// consumer).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_update_partitions", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerUpdatePartitions(
        SafeConsumerHandle consumer,
        IntPtr topic,
        int partitionCount,
        int leaderId,
        IntPtr leaderHost,
        int leaderPort);

    // ==== Producer (M11/P1 foundation + M11/P2 async peripherals) ====
    //
    // M11/P1 declared the construct + lifecycle subset (below). M11/P2 adds the async
    // PERIPHERALS — flush / close / partitions-for (over the push completion bridge) plus
    // the sync flush/close counterparts for the graceful Dispose upgrade. The SEND
    // DllImports (Producer_send, FutureRecordMetadata_*, RecordMetadata_*, the
    // MockProducer send-control helpers) remain deliberately NOT declared here — they land
    // additively in the later send phase (and the ffi §A7 pull-vs-push decision with them).

    // ---- kafka_producer_ProducerProperties_t — config (ffi §0.1 "put") ----

    /// <summary>
    /// <c>kafka_producer_ProducerProperties_new</c> — allocates an empty, owned
    /// properties handle. Declared to return the
    /// <see cref="SafeProducerPropertiesHandle"/> directly so the marshaller
    /// creates-and-sets it atomically (M2/P2); the ABI always returns non-null.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerProperties_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeProducerPropertiesHandle ProducerPropertiesNew();

    /// <summary>
    /// <c>kafka_producer_ProducerProperties_put</c> — adds/overwrites one config
    /// key/value pair. <paramref name="key"/> / <paramref name="value"/> are pinned
    /// NUL-terminated UTF-8 buffers (ffi §A3); a null argument is a no-op ABI-side.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerProperties_put", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerPropertiesPut(IntPtr props, IntPtr key, IntPtr value);

    /// <summary>
    /// <c>kafka_producer_ProducerProperties_destroy</c> — frees a properties handle.
    /// Null-safe (no-op). Backs <see cref="SafeProducerPropertiesHandle"/>'s
    /// <c>ReleaseHandle</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerProperties_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerPropertiesDestroy(IntPtr props);

    // ---- kafka_producer_Producer_t — client lifecycle (ffi §A2) ----

    /// <summary>
    /// <c>kafka_producer_KafkaProducer_new</c> — creates a real producer from a
    /// properties handle, returning an owned <see cref="SafeProducerHandle"/>
    /// directly (the marshaller creates-and-sets it atomically, M2/P2). Fallible: on
    /// failure the native returns null → the marshaller hands back an
    /// <b>IsInvalid</b> <see cref="SafeProducerHandle"/> AND writes a non-null error
    /// handle to <paramref name="outError"/> (null <paramref name="outError"/> =
    /// success). Disposing an IsInvalid handle skips <c>ReleaseHandle</c>, so there is
    /// no spurious <c>Producer_destroy</c>. <paramref name="props"/> is typed as the
    /// <see cref="SafeProducerPropertiesHandle"/> so the marshaller keeps it alive
    /// across the call; the caller retains ownership and frees it afterward (the
    /// header: "must free it separately").
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_KafkaProducer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeProducerHandle KafkaProducerNew(SafeProducerPropertiesHandle props, out IntPtr outError);

    /// <summary>
    /// <c>kafka_producer_MockProducer_new</c> — creates a broker-less mock producer.
    /// <paramref name="autoComplete"/> is a C <c>bool</c> (<c>[MarshalAs(I1)]</c>,
    /// 1 byte — never the default 4-byte Win32 <c>BOOL</c>; ffi §0.1): when true the
    /// mock resolves sends automatically. Non-fallible: returns an owned
    /// <see cref="SafeProducerHandle"/> directly (always valid; the marshaller
    /// creates-and-sets it atomically, M2/P2).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeProducerHandle MockProducerNew([MarshalAs(UnmanagedType.I1)] bool autoComplete);

    /// <summary>
    /// <c>kafka_producer_Producer_destroy</c> — frees a producer handle. Blocks: it
    /// drops the runtime and joins the background Sender task (ffi §A2), so it runs on
    /// a caller thread via <see cref="SafeProducerHandle"/>'s <c>ReleaseHandle</c>
    /// (never the finalizer). Null-safe (no-op).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerDestroy(IntPtr producer);

    // ---- kafka_producer_Producer_t — async peripherals (M11/P2, push bridge) ----
    //
    // flush / close are the void-result completion shape (the producer twin of the
    // consumer's op_callback_t): the callback carries only (KafkaError*, void*). partitions_for
    // is the owned-handle shape (PartitionInfoList*, KafkaError*, void*) — the caller owns
    // whichever handle is non-null (the header). Each pins its topic (partitions_for) or nothing
    // else call-scoped; the completion fires on the producer's foreign dispatcher thread.

    /// <summary>
    /// <c>kafka_producer_Producer_flush_async</c> — flushes all pending records
    /// asynchronously, invoking <paramref name="callback"/> (the void-result completion shape)
    /// on the producer's dispatcher thread with a null error on success or a non-null
    /// <c>KafkaError</c> the callback must free on failure. Backs the public async <c>Flush</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_flush_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerFlushAsync(
        IntPtr producer,
        ProducerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_producer_Producer_close_async</c> — closes the producer asynchronously,
    /// invoking <paramref name="callback"/> (the void-result completion shape) on the
    /// producer's dispatcher thread. The async counterpart of <see cref="ProducerClose"/> and
    /// the graceful close leg of the Dispose upgrade (ffi §A7). A null producer is a no-op
    /// success reported via the callback.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_close_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerCloseAsync(
        IntPtr producer,
        ProducerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_producer_Producer_partitions_for_async</c> — the partition metadata for
    /// <paramref name="topic"/> asynchronously. The completion fires via
    /// <paramref name="callback"/> (the owned-handle shape) with a non-null shared
    /// <c>PartitionInfoList_t</c> (Category-3, copied out then destroyed by the trampoline via
    /// <see cref="PartitionInfoListMarshal"/> / <see cref="PartitionInfoListDestroy"/>) and null
    /// error on success, or a null list and non-null error on failure — the caller owns
    /// whichever is non-null. <paramref name="topic"/> is a pinned NUL-terminated UTF-8 buffer,
    /// valid for the duration of the call (the core copies it synchronously during the submit).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_partitions_for_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerPartitionsForAsync(
        IntPtr producer,
        IntPtr topic,
        ProducerCallbacks.PartitionInfoListCallback callback,
        IntPtr userData);

    // ---- kafka_producer_Producer_t — sync flush + close (M11/P2.1 + M11/P3, teardown) ----
    //
    // The synchronous flush/close counterparts, used by the graceful teardown. There is no
    // Producer_close_with_timeout ABI (unlike the consumer), so the producer has no timed close —
    // the M11/P2 Close(TimeSpan) overload was dropped in M11/P2.1 for Python-producer parity.
    // Each writes an error handle via out_error (null = success) which the caller reads-and-frees;
    // both block. The sync Producer_flush is the teardown flush leg (M11/P3): it resolves pending
    // sends so the completion pump's in-flight get_all can return before the pump is joined (the
    // core's Producer_close only marks closed — it does NOT drive pending sends, unlike Java's
    // close() which flushes; verified src/producer/mock_producer.rs close vs flush).

    /// <summary>
    /// <c>kafka_producer_Producer_flush</c> — flushes all pending records synchronously, writing a
    /// non-null error handle to <paramref name="outError"/> on failure (null = success). Blocks
    /// until pending sends resolve. Two callers, both synchronous: teardown (M11/P3) uses it to
    /// resolve pending sends so the send pump's blocking <c>get_all</c> returns before the pump is
    /// joined; the public sync <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.Flush"/> (M11/P4) uses it as
    /// Java <c>Producer.flush()</c>. For a <c>MockProducer</c> this completes pending sends, for a
    /// real producer it delivers-or-times-out (the accepted Option-C bounded residual, ffi §A7).
    /// <para>
    /// <paramref name="producer"/> is typed as the <see cref="SafeProducerHandle"/> (not a raw
    /// <see cref="IntPtr"/>) so the marshaler auto-<c>DangerousAddRef</c>/<c>DangerousRelease</c>s it
    /// <b>around this synchronous call</b> — the call-scoped guard the ffi §A2 sync-op ref rule
    /// prescribes (retyped from <c>IntPtr</c> in M11/P4, decision #3): a closed handle marshals to
    /// <see cref="ObjectDisposedException"/>, and <c>ReleaseHandle → Producer_destroy</c> cannot run
    /// while a <c>Producer_flush</c> is in flight. The teardown caller (<c>StopPump</c>) passes
    /// <c>_handle</c> before releasing it (single-winner latch), where the call-scoped ref is safe
    /// and strictly preferable too. Null-safe (no-op) on the producer.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_flush", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerFlush(SafeProducerHandle producer, out IntPtr outError);

    /// <summary>
    /// <c>kafka_producer_Producer_close</c> — closes the producer synchronously, writing a
    /// non-null error handle to <paramref name="outError"/> on failure (null = success).
    /// Blocks. The synchronous graceful-close leg of the blocking <c>Dispose</c> upgrade
    /// (ffi §A7) and the sync producer's <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.Close"/>
    /// (M11/P4), run before <see cref="ProducerDestroy"/>. Kept a raw <see cref="IntPtr"/> (not
    /// a <see cref="SafeProducerHandle"/> param) because it is the teardown leg that <em>releases</em>
    /// the handle: its callers win the one-shot latch and pass <c>_handle.DangerousGetHandle()</c>
    /// immediately before <c>_handle.Dispose()</c> (→ <c>Producer_destroy</c>) in the same
    /// single-winner sequence (decision #3). Null-safe (no-op) on the producer.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_close", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerClose(IntPtr producer, out IntPtr outError);

    /// <summary>
    /// <c>kafka_producer_Producer_partitions_for</c> — the partition metadata for
    /// <paramref name="topic"/> synchronously (Java <c>Producer.partitionsFor(String)</c>; the sync
    /// counterpart of <see cref="ProducerPartitionsForAsync"/>). Returns the error handle directly
    /// (null = success), writing an owned <c>PartitionInfoList_t</c> to <paramref name="outList"/> on
    /// success only — on failure it is left <see cref="IntPtr.Zero"/> (the C# <c>out</c> zero-inits
    /// the temporary; the core writes it only on the <c>Ok</c> branch, verified <c>src/ffi/producer.rs</c>).
    /// The caller copies the list out via <see cref="PartitionInfoListMarshal"/> and frees the root
    /// with <see cref="PartitionInfoListDestroy"/> (null-safe on the failure path).
    /// <paramref name="topic"/> is a pinned NUL-terminated UTF-8 buffer read synchronously during the
    /// call (call-scoped, ffi §A3). <paramref name="producer"/> is the <see cref="SafeProducerHandle"/>
    /// (the sync-op auto-ref, ffi §A2 / decision #3).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_partitions_for", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerPartitionsFor(SafeProducerHandle producer, IntPtr topic, out IntPtr outList);

    // ---- kafka_producer_Producer_t — the SEND path (M11/P3, ffi §A4/§A7 Option C) ----
    //
    // Option C — inline pull-pump (PLAN §3): the SINGULAR Producer_send is called INLINE on
    // the caller thread (the core copies key/value SYNCHRONOUSLY during the call — verified
    // src/ffi/producer.rs L262-281 — so the k/v pin is call-scoped, ffi §A4); a single pump
    // thread per NativeProducer drains a batched FutureRecordMetadata_get_all and destroys the
    // futures with FutureRecordMetadata_destroy_all. No ProducerRecord_t mirror struct
    // (that is send_batch / Option A), no per-send callback (that is send_async / Option B).
    // The optional fast-path FutureRecordMetadata_is_done and the RecordMetadata_copy callback
    // are deliberately NOT declared — the pump enqueues every send and reads the per-field
    // metadata accessors (no dead/unused DllImport, PLAN §6.1).

    /// <summary>
    /// <c>kafka_producer_Producer_send</c> — sends a single record (sync enqueue), returning a
    /// non-null <c>FutureRecordMetadata_t</c> handle on success or null with a non-null
    /// <paramref name="outError"/> on a synchronous validation failure. The core copies
    /// <paramref name="key"/> / <paramref name="value"/> into the batch buffer
    /// <b>synchronously during the call</b> (verified <c>src/ffi/producer.rs</c>), so the
    /// pinned buffers are <b>call-scoped</b> — freed once this returns (ffi §A4). Sentinels:
    /// <paramref name="key"/> / <paramref name="value"/> absent → <see cref="IntPtr.Zero"/> +
    /// <c>len -1</c>; empty → a non-null pointer + <c>len 0</c>; present → pointer + length.
    /// <paramref name="partition"/> is <c>-1</c> for no hint; <paramref name="timestamp"/> is
    /// <c>-1</c> to let the producer stamp the record. <paramref name="topic"/> is a pinned
    /// NUL-terminated UTF-8 buffer read synchronously during the call.
    /// <para>
    /// <paramref name="producer"/> is typed as the <see cref="SafeProducerHandle"/> (not a raw
    /// <see cref="IntPtr"/>) so the marshaler auto-<c>DangerousAddRef</c>/<c>DangerousRelease</c>s it
    /// <b>around this call</b> — the call-scoped guard a <b>synchronous</b> op needs (the core copies
    /// key/value during the call, ffi §A4), so <c>ReleaseHandle → Producer_destroy</c> cannot run
    /// while a <c>Producer_send</c> is in flight, and a closed handle marshals to
    /// <see cref="ObjectDisposedException"/>. This is the sync-op form of the ffi §A2 ref rule (a
    /// synchronous native call passes the <c>SafeHandle</c>; an async <c>*_async</c> op cannot use
    /// the auto ref — it needs a manual span-the-op <c>DangerousAddRef</c> held until its completion
    /// callback fires). The remaining params stay <see cref="IntPtr"/> (from the caller's
    /// <c>fixed</c> pins) / scalars / <c>out</c>, which coexist with a <c>SafeHandle</c> param.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_send", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerSend(
        SafeProducerHandle producer,
        IntPtr topic,
        int partition,
        long timestamp,
        IntPtr key,
        int keyLen,
        IntPtr value,
        int valueLen,
        out IntPtr outError);

    /// <summary>
    /// <c>kafka_producer_FutureRecordMetadata_get_all</c> — blocks until every future in
    /// <paramref name="futures"/> (<paramref name="count"/> entries) resolves, writing the
    /// parallel results into <paramref name="outMetadata"/> / <paramref name="outErrors"/>:
    /// per index exactly one is non-null (metadata on success, error on failure). The future
    /// handles are <b>not</b> consumed — the caller still owns them and frees them with
    /// <see cref="FutureRecordMetadataDestroyAll"/>. The caller frees every non-null metadata
    /// (via <see cref="RecordMetadataDestroy"/>) and every non-null error (via
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>). Called only on the pump thread — its
    /// <c>block_on</c> parks just that thread; the Sender keeps running on the runtime's worker
    /// pool (ffi §A1). The two output arrays are blittable <see cref="IntPtr"/> arrays the
    /// callee writes in place (<c>[Out]</c>; the runtime pins them).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_FutureRecordMetadata_get_all", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FutureRecordMetadataGetAll(
        IntPtr[] futures,
        int count,
        [Out] IntPtr[] outMetadata,
        [Out] IntPtr[] outErrors);

    /// <summary>
    /// <c>kafka_producer_FutureRecordMetadata_destroy_all</c> — frees the
    /// <paramref name="count"/> future handles in <paramref name="futures"/> (null entries
    /// skipped). Called by the pump after <see cref="FutureRecordMetadataGetAll"/> reads each
    /// result (<c>get_all</c> does not consume the futures, ffi §A2).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_FutureRecordMetadata_destroy_all", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FutureRecordMetadataDestroyAll(IntPtr[] futures, int count);

    // ---- kafka_producer_Producer_t — the SYNC SEND path (M11/P4, ffi §A1/§A5, PLAN §3 decision #2) ----
    //
    // The sync producer's Send blocks on the caller's OWN thread — Producer_send (inline, shared with
    // the async path) then the BLOCKING FutureRecordMetadata_get, with no pump / TCS / callback. The
    // block happens inside the Rust core's own multi-thread runtime (deadlock-free, ffi §A1) — this is
    // the direct-sync-ABI pattern, NOT sync-over-async. Both symbols already exist in the checked-in
    // header (Mode A). The singular FutureRecordMetadata_destroy (below) is used instead of the
    // pump's _destroy_all wherever one future is freed or the free path must not allocate (no
    // 1-element array); it is a genuinely-used DllImport (not dead — see its own remarks for the
    // three call sites).

    /// <summary>
    /// <c>kafka_producer_FutureRecordMetadata_get</c> — <b>blocks</b> until <paramref name="future"/>
    /// resolves, returning a non-null <c>RecordMetadata_t</c> handle + null <paramref name="outError"/>
    /// on success, or a null return + non-null <paramref name="outError"/> on failure (exactly one is
    /// non-null). The blocking get for the sync producer's <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.Send(Confluent.Kafka.ProducerRecord{TKey, TValue})"/>
    /// (PLAN §3): the block runs inside the core's multi-thread runtime (<c>block_on</c>), which parks
    /// only the calling thread and is deadlock-free (ffi §A1) — NOT sync-over-async. The future is
    /// <b>not</b> consumed — the caller still owns it and frees it with
    /// <see cref="FutureRecordMetadataDestroy"/> after reading. The returned metadata handle is owned
    /// by the caller and freed with <see cref="RecordMetadataDestroy"/>; the error (if any) is freed by
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>. Takes a raw <see cref="IntPtr"/> future (a flat
    /// transient, ffi §A2 Category 2), not the producer handle — the future is Arc-backed and
    /// independent of the producer's lifetime.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_FutureRecordMetadata_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FutureRecordMetadataGet(IntPtr future, out IntPtr outError);

    /// <summary>
    /// <c>kafka_producer_FutureRecordMetadata_destroy</c> — frees a single future handle. Null-safe
    /// (no-op). Used wherever exactly one future is freed, or wherever the free path must not
    /// allocate, because the singular form avoids the 1-element array
    /// <see cref="FutureRecordMetadataDestroyAll"/> would need: the sync
    /// <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.Send(Confluent.Kafka.ProducerRecord{TKey, TValue})"/>
    /// path (<c>NativeProducer.Send</c>, freeing the one future after the blocking
    /// <see cref="FutureRecordMetadataGet"/> reads its result), the async path's orphaned-future
    /// <c>catch</c> (<c>NativeProducer.SendViaPump</c>), and the pump's own marshalling-array
    /// allocation <c>catch</c> (<c>SendCompletionPump.ProcessBatch</c>) — the latter two reachable
    /// only under out-of-memory, where allocating in order to free would risk leaking the handle.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_FutureRecordMetadata_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FutureRecordMetadataDestroy(IntPtr future);

    // ---- kafka_producer_RecordMetadata_t — flat transient result (ffi §A2 Category 2) ----
    //
    // Per-field accessors (offset / partition / topic / timestamp) + destroy — the default over
    // the RecordMetadata_copy callback (PLAN §6.1). The topic is a NUL-terminated const char*
    // owned by the handle (§A3 NUL-scan form), copied out before RecordMetadata_destroy.

    /// <summary>
    /// <c>kafka_producer_RecordMetadata_offset</c> — the record's offset (or <c>-1</c> if the
    /// handle is null).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long RecordMetadataOffset(IntPtr metadata);

    /// <summary>
    /// <c>kafka_producer_RecordMetadata_partition</c> — the record's partition (or <c>-1</c> if
    /// the handle is null).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int RecordMetadataPartition(IntPtr metadata);

    /// <summary>
    /// <c>kafka_producer_RecordMetadata_topic</c> — the topic name as a <b>NUL-terminated</b>
    /// <c>const char*</c> owned by the handle (valid until <see cref="RecordMetadataDestroy"/>).
    /// Copy via <see cref="Utf8Marshal.PtrToString(IntPtr)"/> (NUL-scan form, §A3) before the
    /// handle is destroyed; null if the handle is null.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RecordMetadataTopic(IntPtr metadata);

    /// <summary>
    /// <c>kafka_producer_RecordMetadata_timestamp</c> — the record's timestamp in milliseconds
    /// (or <c>-1</c> if the handle is null or no timestamp was set).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_timestamp", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long RecordMetadataTimestamp(IntPtr metadata);

    /// <summary>
    /// <c>kafka_producer_RecordMetadata_destroy</c> — frees a record-metadata handle. Null-safe
    /// (no-op). Called by the pump after the per-field copy-out.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RecordMetadataDestroy(IntPtr metadata);

    // ---- kafka_producer_MetricMap_t — the producer metrics snapshot (M11/P8, ffi §A2/§A3) ----
    //
    // These are DISTINCT native symbols from the consumer's kafka_consumer_MetricMap_* family
    // (the FFI ships one metric-map surface per client), so the consumer declarations above are
    // NOT reusable — these carry a Producer* managed prefix and sit alongside them. The
    // value-kind discriminator IS shared, though: both surfaces return the same
    // crate::ffi::common::METRIC_VALUE_* constants, so the MetricValueKind* constants declared
    // with the consumer block are reused here rather than redeclared.

    /// <summary>
    /// <c>kafka_producer_Producer_metrics</c> — returns an owned (Category-3) metric-map handle:
    /// a point-in-time snapshot of the producer's metrics (Java
    /// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c>). Free it with
    /// <see cref="ProducerMetricMapDestroy"/> after reading; every borrowed string it hands out
    /// dies with it. <paramref name="producer"/> is the <see cref="SafeProducerHandle"/> so the
    /// marshaller holds a call-scoped reference around this synchronous call (the ffi §A2 sync-op
    /// convention), and a closed handle marshals to <see cref="ObjectDisposedException"/>.
    /// <para>
    /// <b>No concurrent-access null (unlike the consumer).</b> The generated header documents a
    /// null return only for a <b>null handle</b>: this takes the core producer <c>Mutex</c> and
    /// <b>blocks</b> rather than rejecting concurrent access, so there is no
    /// <c>kafka_consumer_Consumer_metrics</c>-style "null on a concurrent-access rejection" case
    /// to map (ffi §A1 — "concurrent access is safe, don't add your own lock").
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_metrics", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetrics(SafeProducerHandle producer);

    /// <summary>
    /// <c>kafka_producer_MetricMap_count</c> — the number of metric entries in the map.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerMetricMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_name</c> — the metric name at <paramref name="index"/>
    /// as a NUL-terminated <c>const char*</c> borrowed from the map (copy via
    /// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> before destroy), or
    /// <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetricMapGetName(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_group</c> — the metric group at <paramref name="index"/>
    /// (borrowed NUL-terminated <c>const char*</c>), or <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_group", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetricMapGetGroup(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_description</c> — the metric description at
    /// <paramref name="index"/> (borrowed NUL-terminated <c>const char*</c>), or
    /// <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_description", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetricMapGetDescription(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_tag_count</c> — the number of tags on the metric at
    /// <paramref name="index"/>, or <c>-1</c> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_tag_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerMetricMapGetTagCount(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_tag_key</c> — the <paramref name="tagIndex"/>-th tag key
    /// of the metric at <paramref name="index"/> (borrowed NUL-terminated <c>const char*</c>), or
    /// <see cref="IntPtr.Zero"/> if either index is out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_tag_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetricMapGetTagKey(IntPtr map, int index, int tagIndex);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_tag_value</c> — the <paramref name="tagIndex"/>-th tag
    /// value of the metric at <paramref name="index"/> (borrowed NUL-terminated
    /// <c>const char*</c>), or <see cref="IntPtr.Zero"/> if either index is out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_tag_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetricMapGetTagValue(IntPtr map, int index, int tagIndex);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_value_kind</c> — which <c>get_value_*</c> accessor is
    /// valid for the metric at <paramref name="index"/> (one of the shared
    /// <c>MetricValueKind*</c> constants). Defaults to <see cref="MetricValueKindDouble"/> when
    /// out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_value_kind", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerMetricMapGetValueKind(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_value_double</c> — the <see cref="double"/> reading of the
    /// metric at <paramref name="index"/> (<c>0.0</c> if out of range or a different kind).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_value_double", CallingConvention = CallingConvention.Cdecl)]
    internal static extern double ProducerMetricMapGetValueDouble(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_value_string</c> — the <see cref="string"/> reading of the
    /// metric at <paramref name="index"/> as a borrowed NUL-terminated <c>const char*</c> (copy
    /// before destroy), or <see cref="IntPtr.Zero"/> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_value_string", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetricMapGetValueString(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_value_long</c> — the <see cref="long"/> (<c>Int64</c>)
    /// reading of the metric at <paramref name="index"/> (<c>0</c> if out of range or a different
    /// kind).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_value_long", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ProducerMetricMapGetValueLong(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_get_value_int</c> — the <see cref="int"/> (<c>Int32</c>)
    /// reading of the metric at <paramref name="index"/> (<c>0</c> if out of range or a different
    /// kind).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_get_value_int", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerMetricMapGetValueInt(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_producer_MetricMap_destroy</c> — frees an owned metric-map handle. Null-safe
    /// (no-op). Every borrowed string handed out by the accessors is invalid after this.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MetricMap_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerMetricMapDestroy(IntPtr map);

    // ---- kafka_producer_MockProducer_t — send-control helpers (M11/P3, mock only) ----
    //
    // Inherent on the public AsyncMockProducer (NOT on IAsyncProducer): Java MockProducer /
    // Python _MockProducerMixin parity (complete_next / error_next / history_count / clear).

    /// <summary>
    /// <c>kafka_producer_MockProducer_complete_next</c> — completes the next pending send
    /// successfully on a mock producer. Returns <see langword="false"/> if there is no pending
    /// completion or the producer is null / not a mock. The 1-byte C <c>bool</c> return needs
    /// <c>[MarshalAs(I1)]</c> (§0.1).
    /// <para>
    /// <paramref name="producer"/> is the <see cref="SafeProducerHandle"/>, not a raw
    /// <see cref="IntPtr"/> (M11/P8, Minor 7): this is a <b>synchronous</b> native call, so the
    /// marshaller's call-scoped auto-<c>DangerousAddRef</c>/<c>DangerousRelease</c> is exactly the
    /// guard it needs (the ffi §A2 sync-call convention, the <see cref="ProducerSend"/> precedent).
    /// A raw pointer here was a use-after-free against a concurrent <c>Producer_destroy</c> — and
    /// reachable from public API, on a pattern <c>IProducer</c> documents as the intended
    /// cross-thread use (one thread blocked in <c>Send</c>, another driving the mock). A closed
    /// handle now marshals to <see cref="ObjectDisposedException"/>; each call site already calls
    /// <c>ThrowIfClosed()</c> first, so the common post-<c>Dispose</c> case is unchanged.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_complete_next", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool MockProducerCompleteNext(SafeProducerHandle producer);

    /// <summary>
    /// <c>kafka_producer_MockProducer_error_next</c> — completes the next pending send with a
    /// <paramref name="errorCode"/> error on a mock producer. <paramref name="errorMessage"/> is
    /// a pinned NUL-terminated UTF-8 buffer, or <see cref="IntPtr.Zero"/> to use the default
    /// message for the code. Returns <see langword="true"/> if there was a pending completion
    /// (the 1-byte C <c>bool</c> needs <c>[MarshalAs(I1)]</c>).
    /// <para>
    /// <paramref name="producer"/> is the <see cref="SafeProducerHandle"/>, not a raw
    /// <see cref="IntPtr"/> (M11/P8, Minor 7): this is a <b>synchronous</b> native call, so the
    /// marshaller's call-scoped auto-<c>DangerousAddRef</c>/<c>DangerousRelease</c> is exactly the
    /// guard it needs (the ffi §A2 sync-call convention, the <see cref="ProducerSend"/> precedent).
    /// A raw pointer here was a use-after-free against a concurrent <c>Producer_destroy</c> — and
    /// reachable from public API, on a pattern <c>IProducer</c> documents as the intended
    /// cross-thread use (one thread blocked in <c>Send</c>, another driving the mock). A closed
    /// handle now marshals to <see cref="ObjectDisposedException"/>; each call site already calls
    /// <c>ThrowIfClosed()</c> first, so the common post-<c>Dispose</c> case is unchanged.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_error_next", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool MockProducerErrorNext(SafeProducerHandle producer, int errorCode, IntPtr errorMessage);

    /// <summary>
    /// <c>kafka_producer_MockProducer_history_count</c> — the number of records in the mock's
    /// sent history (or <c>0</c> if the producer is null / not a mock).
    /// <para>
    /// <paramref name="producer"/> is the <see cref="SafeProducerHandle"/>, not a raw
    /// <see cref="IntPtr"/> (M11/P8, Minor 7): this is a <b>synchronous</b> native call, so the
    /// marshaller's call-scoped auto-<c>DangerousAddRef</c>/<c>DangerousRelease</c> is exactly the
    /// guard it needs (the ffi §A2 sync-call convention, the <see cref="ProducerSend"/> precedent).
    /// A raw pointer here was a use-after-free against a concurrent <c>Producer_destroy</c> — and
    /// reachable from public API, on a pattern <c>IProducer</c> documents as the intended
    /// cross-thread use (one thread blocked in <c>Send</c>, another driving the mock). A closed
    /// handle now marshals to <see cref="ObjectDisposedException"/>; each call site already calls
    /// <c>ThrowIfClosed()</c> first, so the common post-<c>Dispose</c> case is unchanged.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_history_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MockProducerHistoryCount(SafeProducerHandle producer);

    /// <summary>
    /// <c>kafka_producer_MockProducer_clear</c> — clears the mock's sent history and pending
    /// completions. Null-safe (no-op).
    /// <para>
    /// <paramref name="producer"/> is the <see cref="SafeProducerHandle"/>, not a raw
    /// <see cref="IntPtr"/> (M11/P8, Minor 7): this is a <b>synchronous</b> native call, so the
    /// marshaller's call-scoped auto-<c>DangerousAddRef</c>/<c>DangerousRelease</c> is exactly the
    /// guard it needs (the ffi §A2 sync-call convention, the <see cref="ProducerSend"/> precedent).
    /// A raw pointer here was a use-after-free against a concurrent <c>Producer_destroy</c> — and
    /// reachable from public API, on a pattern <c>IProducer</c> documents as the intended
    /// cross-thread use (one thread blocked in <c>Send</c>, another driving the mock). A closed
    /// handle now marshals to <see cref="ObjectDisposedException"/>; each call site already calls
    /// <c>ThrowIfClosed()</c> first, so the common post-<c>Dispose</c> case is unchanged.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_clear", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockProducerClear(SafeProducerHandle producer);
    // ---- ConsumerHandle_t — the in-callback reentrancy handle (M9/P8, ffi §B1/§B2/§B5) ----
    //
    // 23 declarations: `Consumer_handle` plus the 22 `ConsumerHandle_*`. Three properties set
    // this family apart from every `Consumer_*` declaration above, and all three come from
    // src/ffi/consumer_handle.rs:27-86 (quoted in the per-declaration docs):
    //
    //   1. NO ACCESS GUARD, by design. "Nothing in this module acquires the single-owner
    //      access guard ... That is deliberate and is the whole reason the type exists." So a
    //      handle op SUCCEEDS while a consumer operation is in flight, where the equivalent
    //      `Consumer_*` call is rejected with ConcurrentModification. That asymmetry is the
    //      whole point (it is what makes a listener able to call back into its consumer), and
    //      it is what PublicConsumerReentrancyHandleTests' guard-bypass test asserts.
    //   2. EVERY op is SYNCHRONOUS — the core drives its future to completion with
    //      `block_on` on the CALLING thread. So each of the 21 NON-DESTROY declarations
    //      takes the SafeConsumerReentrancyHandle as its parameter (the M9/P4 H1
    //      convention), never a raw DangerousGetHandle(). ConsumerHandleDestroy is the
    //      single exclusion and keeps IntPtr, for the same structural reason as
    //      ConsumerDestroy — it is called from that SafeHandle's own ReleaseHandle, where
    //      passing `this` would AddRef a handle already mid-release (see the class doc).
    //      Called from inside a tokio runtime these return an IllegalStateError handle
    //      rather than panicking across the FFI boundary; it surfaces as an ordinary flat
    //      KafkaException, NOT an InvalidOperationException (ffi §B5).
    //   3. On a MOCK-derived handle `wakeup` and the three sync getters work (the getters
    //      return EMPTY, never null — unlike `Consumer_assignment`, which returns null on a
    //      guard rejection) while every ASYNC op fails with UnsupportedVersionError. That is
    //      documented CORE behavior, not an FFI limitation.
    //
    // NONE of these entry points takes a `user_data_destroy`, and none takes a callback at
    // all — so the §B6 free-site rule ("hook present ⇒ the hook is the sole free site") does
    // not engage anywhere in this family, and there is no GCHandle in it.
    //
    // The result shapes reuse the SHIPPED marshallers verbatim: TopicPartitionListMarshal /
    // StringListMarshal for the getters, KafkaException.FromHandle for the error-only ops,
    // and OffsetMapMarshal / LongOffsetMapMarshal / OffsetAndTimestampMapMarshal for the
    // owned-container out-params. No new marshaller exists or is needed. Like the sync query
    // family above, the out-param is LEFT UNTOUCHED on failure, so every call site
    // pre-initializes it to IntPtr.Zero.

    /// <summary>
    /// <c>kafka_consumer_Consumer_handle</c> — a new reentrancy handle for
    /// <paramref name="consumer"/> (Java's captured <c>consumer</c> variable inside a
    /// callback). Per the header it "does <b>not</b> acquire the guard, so this call never
    /// fails with <c>ConcurrentModificationError</c>" and returns "a <b>non-null</b> handle"
    /// — there is no error out-param. The caller owns it and must destroy it
    /// <b>before</b> the consumer (which <see cref="SafeConsumerReentrancyHandle"/> enforces
    /// by ref-counting the parent).
    /// </summary>
    /// <remarks>
    /// <b>Name deviation, deliberate.</b> The mechanical prefix-drop (CLAUDE.md §6.3) would
    /// give <c>ConsumerHandle</c>, which collides in reading with the public
    /// <see cref="Confluent.Kafka.ConsumerHandle"/> type and with the 22
    /// <c>ConsumerHandle*</c> declarations below. <c>ConsumerGetHandle</c> keeps the
    /// declarations legible; the <c>EntryPoint</c> carries the exact ABI symbol, which is
    /// what the marshaller resolves.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_handle", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeConsumerReentrancyHandle ConsumerGetHandle(SafeConsumerHandle consumer);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_destroy</c> — frees a reentrancy handle. Null-safe
    /// (a no-op), and "never affects the owning consumer or any other handle".
    /// </summary>
    /// <remarks>
    /// ⚠ Takes a raw <see cref="IntPtr"/>, not the <see cref="SafeConsumerReentrancyHandle"/>
    /// — the same structural exclusion from the SafeHandle-param convention as
    /// <see cref="ConsumerDestroy"/>: it is called from
    /// <see cref="SafeConsumerReentrancyHandle.ReleaseHandle"/>, where passing <c>this</c>
    /// would make the marshaller <c>DangerousAddRef</c> a handle that is already mid-release.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleDestroy(IntPtr handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_wakeup</c> — wakes the owning consumer, exactly like
    /// <see cref="ConsumerWakeup"/>. "Callable from any thread (it neither blocks nor takes
    /// the guard)" and works on a mock-derived handle.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_wakeup", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleWakeup(SafeConsumerReentrancyHandle handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_assignment</c> — the owning consumer's assignment as
    /// an owned (Category-3) <c>TopicPartitionList_t</c> borrow-root, copied out and freed by
    /// <see cref="TopicPartitionListMarshal.CopyOutAndDestroy"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Documented <b>non-null</b>, unlike <see cref="ConsumerAssignment"/></b>, which
    /// returns <see cref="IntPtr.Zero"/> "on a concurrent-access rejection". This handle takes
    /// no guard, so there is no rejection to signal — the very asymmetry that makes the handle
    /// usable from inside a callback where the consumer's own getter is not. "Always empty on
    /// a <c>MockConsumer</c>-derived handle (core behavior)."
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_assignment", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleAssignment(SafeConsumerReentrancyHandle handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_subscription</c> — the owning consumer's topic
    /// subscription as an owned (Category-3) <c>StringList_t</c> borrow-root, copied out and
    /// freed by <see cref="StringListMarshal.CopyOutAndDestroy"/>. Non-null; always empty on a
    /// mock-derived handle.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_subscription", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleSubscription(SafeConsumerReentrancyHandle handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_paused</c> — the owning consumer's paused partitions
    /// as an owned (Category-3) <c>TopicPartitionList_t</c> borrow-root. Same accessors and
    /// non-null contract as <see cref="ConsumerHandleAssignment"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_paused", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandlePaused(SafeConsumerReentrancyHandle handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_assign</c> — assigns the owning consumer to the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs (parallel arrays as
    /// <see cref="ConsumerAssign"/>). Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success).
    /// </summary>
    /// <remarks>
    /// ⚠ An <b>empty</b> assignment is <b>rejected</b> here, where
    /// <see cref="ConsumerAssign"/> accepts it: "on the owning consumer <c>assign([])</c>
    /// leaves the group, which the reentrancy handle deliberately does not expose"
    /// (consumer-threading.md §31, Phase-41 Issue 4). The core error is surfaced unchanged.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_assign", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleAssign(
        SafeConsumerReentrancyHandle handle, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_seek</c> — seeks <c>(topic, partition)</c> to
    /// <paramref name="offset"/>. <paramref name="topic"/> is a pinned NUL-terminated UTF-8
    /// buffer read synchronously during the call (call-scoped pin). Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success). The
    /// <see cref="ConsumerSeek"/> shape.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleSeek(
        SafeConsumerReentrancyHandle handle, IntPtr topic, int partition, long offset);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_seek_with_metadata</c> — seeks
    /// <c>(topic, partition)</c> to <paramref name="offset"/> carrying commit metadata and a
    /// leader epoch. Per the header, <paramref name="leaderEpoch"/> <c>&lt; 0</c> means "no
    /// leader epoch" and a null <paramref name="metadata"/> means "no metadata" — the binding
    /// always passes a valid pointer, since <see cref="OffsetAndMetadata.Metadata"/> is never
    /// null. The <see cref="ConsumerSeekWithMetadata"/> shape.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_with_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleSeekWithMetadata(
        SafeConsumerReentrancyHandle handle,
        IntPtr topic,
        int partition,
        long offset,
        int leaderEpoch,
        IntPtr metadata);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_seek_to_beginning</c> — requests an EARLIEST offset
    /// reset for the <paramref name="count"/> <c>(topic, partition)</c> pairs. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_to_beginning", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleSeekToBeginning(
        SafeConsumerReentrancyHandle handle, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_seek_to_end</c> — the LATEST analog of
    /// <see cref="ConsumerHandleSeekToBeginning"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_to_end", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleSeekToEnd(
        SafeConsumerReentrancyHandle handle, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_pause</c> — pauses fetching for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_pause", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandlePause(
        SafeConsumerReentrancyHandle handle, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_resume</c> — resumes fetching for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs. Returns a
    /// <c>kafka_common_KafkaError_t</c> handle (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_resume", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleResume(
        SafeConsumerReentrancyHandle handle, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_position</c> — the current position of
    /// <c>(topic, partition)</c> using the consumer's <c>default.api.timeout.ms</c>. On
    /// success writes the offset to <paramref name="outPosition"/> and returns null; on
    /// failure returns a non-null error handle and <b>leaves <paramref name="outPosition"/>
    /// untouched</b>. The <see cref="ConsumerPosition"/> shape.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_position", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandlePosition(
        SafeConsumerReentrancyHandle handle, IntPtr topic, int partition, out long outPosition);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_position_timeout</c> — the timeout-bounded twin of
    /// <see cref="ConsumerHandlePosition"/> (<paramref name="timeoutMs"/> in milliseconds),
    /// mirroring the consumer's own <c>position</c> / <c>position_timeout</c> overload pair.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_position_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandlePositionTimeout(
        SafeConsumerReentrancyHandle handle, IntPtr topic, int partition, long timeoutMs, out long outPosition);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_committed</c> — the last committed offsets for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs. On success writes an owned
    /// <c>OffsetMap_t</c> (Category-3; copied out and freed by
    /// <see cref="OffsetMapMarshal.CopyOut"/> + <see cref="OffsetMapDestroy"/>) to
    /// <paramref name="outMap"/>; on failure returns a non-null error handle and leaves
    /// <paramref name="outMap"/> untouched.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_committed", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleCommitted(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_beginning_offsets</c> — the earliest offsets for the
    /// <paramref name="count"/> <c>(topic, partition)</c> pairs, as an owned
    /// <c>LongOffsetMap_t</c> written to <paramref name="outMap"/> on success.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_beginning_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleBeginningOffsets(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_end_offsets</c> — the LATEST analog of
    /// <see cref="ConsumerHandleBeginningOffsets"/>, sharing the <c>LongOffsetMap_t</c> result.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_end_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleEndOffsets(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_offsets_for_times</c> — offsets by timestamp for the
    /// parallel <c>(topics[], partitions[], timestamps[], count)</c> arrays. On success writes
    /// an owned <c>OffsetAndTimestampMap_t</c> to <paramref name="outMap"/>; "unresolved
    /// partitions are omitted from the map".
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_offsets_for_times", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleOffsetsForTimes(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        long[] timestamps,
        int count,
        out IntPtr outMap);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_commit_sync</c> — commits the consumed offsets
    /// synchronously (Java <c>commitSync()</c>). Per the header this is "the operation a
    /// rebalance listener calls to flush offsets before its partitions are taken away
    /// (<c>consumer-threading.md</c> §31)". Returns a <c>kafka_common_KafkaError_t</c> handle
    /// (null = success).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_sync", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleCommitSync(SafeConsumerReentrancyHandle handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_commit_sync_offsets</c> — commits specific offsets
    /// synchronously, from the same five parallel input arrays as
    /// <see cref="ConsumerCommitSyncOffsets"/> (metadata entries may be null; a leader epoch
    /// <c>&lt; 0</c> means "no epoch").
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_sync_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleCommitSyncOffsets(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        int[] leaderEpochs,
        IntPtr[] metadata,
        int count);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_commit_async</c> — commits the consumed offsets
    /// fire-and-forget (Java <c>commitAsync()</c>); returns once the commit has been
    /// initiated. There is deliberately <b>no callback-taking variant on the handle</b>,
    /// matching the core — "register an <c>OffsetCommitCallback</c> on the owning consumer
    /// with <c>Consumer_commit_async_with_callback</c> instead" (roadmap D9).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleCommitAsync(SafeConsumerReentrancyHandle handle);

    /// <summary>
    /// <c>kafka_consumer_ConsumerHandle_commit_async_offsets</c> — commits specific offsets
    /// fire-and-forget, with the same array shape as
    /// <see cref="ConsumerHandleCommitSyncOffsets"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_async_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleCommitAsyncOffsets(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        int[] leaderEpochs,
        IntPtr[] metadata,
        int count);
}
