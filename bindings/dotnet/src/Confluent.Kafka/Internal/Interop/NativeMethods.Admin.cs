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
/// The <b>admin</b> half of the P/Invoke boundary (M15/P1) — another <c>partial</c>
/// part of the single <see cref="NativeMethods"/> class CA1060 requires, split out so
/// neither file becomes unnavigable. Every convention of the main part applies
/// verbatim: the full ABI symbol as <c>EntryPoint</c> (the C# names drop the
/// <c>kafka_admin_</c> prefix, CLAUDE.md §6.3), <c>Cdecl</c>, opaque <c>*_t</c> →
/// <see cref="IntPtr"/>, and <c>[MarshalAs(UnmanagedType.I1)]</c> on every <c>bool</c>
/// (C <c>bool</c> is one byte — the default marshals a 4-byte Win32 <c>BOOL</c> and
/// silently corrupts the following argument).
/// </summary>
/// <remarks>
/// <para>
/// <b>Only what M15/P1 uses is declared</b> — the client lifecycle, the properties
/// builder, <c>NewTopic</c>, <c>create_topics_async</c>, and the
/// <c>CreateTopicsResult</c> / <c>TopicMetadataAndConfig</c> accessors. Later phases
/// add their own blocks here.
/// </para>
/// <para>
/// <b>The sync-vs-async handle convention (ffi §A2), applied to admin.</b> A
/// <b>synchronous</b> entry point takes its <see cref="SafeAdminHandle"/> as the
/// P/Invoke parameter, so the interop marshaller holds a <b>call-scoped</b>
/// <c>DangerousAddRef</c> for the duration of the native call
/// (<see cref="AdminClientClose"/>). An <c>_async</c> submit keeps a raw
/// <see cref="IntPtr"/> instead, because it needs a <b>span-the-op</b> reference —
/// taken explicitly at submit, released by the completion callback — and a
/// call-scoped ref would be released when submit returns, long before the callback
/// fires. <see cref="AdminClientDestroy"/> is structurally exempt: it is called from
/// <see cref="SafeAdminHandle.ReleaseHandle"/>, where passing <c>this</c> would make
/// the marshaller <c>DangerousAddRef</c> a handle that is already mid-release.
/// </para>
/// <para>
/// ⚠ <b><c>AdminClient_destroy</c> is not ref-counted and does not drain.</b> The
/// header states that destroying concurrently with an in-flight <c>_async</c>
/// operation is a C lifetime precondition <em>the caller</em> must uphold — unlike the
/// consumer ABI, which ref-counts internally. The span-the-op
/// <c>DangerousAddRef</c> on <see cref="SafeAdminHandle"/> (taken in
/// <c>NativeAdminClient</c>'s submit, released in <c>AdminOperation.FreeGcHandle</c>)
/// is therefore <b>the</b> mechanism that upholds it.
/// </para>
/// </remarks>
internal static partial class NativeMethods
{
    // ---- kafka_admin_AdminClientProperties_t — config (ffi §0.1 "put") ----

    /// <summary>
    /// <c>kafka_admin_AdminClientProperties_new</c> — allocates an empty, owned
    /// properties handle. Declared to return the
    /// <see cref="SafeAdminPropertiesHandle"/> directly so the marshaller
    /// creates-and-sets it atomically (the M2/P2 hardening); the ABI always returns
    /// non-null.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClientProperties_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeAdminPropertiesHandle AdminClientPropertiesNew();

    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClientProperties_put", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientPropertiesPut(IntPtr props, IntPtr key, IntPtr value);

    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClientProperties_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientPropertiesDestroy(IntPtr props);

    // ---- kafka_admin_AdminClient_t — client lifecycle (ffi §B2 Category 1) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_new</c> — creates a real admin client from a
    /// properties handle (Java's <c>Admin.create(Properties)</c>), returning an owned
    /// <see cref="SafeAdminHandle"/> directly. Fallible: on failure the native returns
    /// null → the marshaller hands back an <b>IsInvalid</b> handle AND writes a
    /// non-null error to <paramref name="outError"/> (null = success). Disposing an
    /// IsInvalid handle skips <c>ReleaseHandle</c>, so there is no spurious
    /// <c>AdminClient_destroy</c>. <paramref name="props"/> is typed as the
    /// <see cref="SafeAdminPropertiesHandle"/> so the marshaller keeps it alive across
    /// the call; the caller retains ownership and frees it afterward.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeAdminHandle AdminClientNew(SafeAdminPropertiesHandle props, out IntPtr outError);

    /// <summary>
    /// <c>kafka_admin_MockAdminClient_new</c> — creates a broker-less mock admin
    /// client. It returns the <b>same</b> <c>kafka_admin_AdminClient_t*</c> type as the
    /// real constructor, so the whole RPC surface works against it unchanged.
    /// <para>
    /// ⚠ Returns <b>null</b> when <paramref name="numBrokers"/> is less than 1 (Java's
    /// <c>MockAdminClient.Builder.build()</c> throw, expressed in the FFI idiom) or if
    /// the tokio runtime cannot be created — the marshaller then yields an
    /// <b>IsInvalid</b> handle, which the caller must map to an exception rather than
    /// dereference.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeAdminHandle MockAdminClientNew(int numBrokers);

    /// <summary>
    /// <c>kafka_admin_AdminClient_destroy</c> — frees the client. Null-safe.
    /// <b>Not</b> ref-counted and it does <b>not</b> drain: the header makes
    /// "do not destroy concurrently with an in-flight <c>_async</c> op" a caller
    /// precondition, upheld here by the span-the-op ref on
    /// <see cref="SafeAdminHandle"/>. Takes a raw <see cref="IntPtr"/> because it is
    /// called from that handle's own <c>ReleaseHandle</c> (see the class remarks).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDestroy(IntPtr admin);

    /// <summary>
    /// <c>kafka_admin_AdminClient_close</c> — the <b>synchronous</b> graceful close,
    /// awaiting the background task for up to <paramref name="timeoutMs"/> (negative =
    /// Java's no-argument <c>close()</c>, i.e. wait indefinitely). Returns nothing:
    /// Java's <c>Admin.close(Duration)</c> is <c>void</c>. Takes the
    /// <see cref="SafeAdminHandle"/> as its parameter per the sync convention
    /// (ffi §A2), so the marshaller holds a call-scoped ref for the whole blocking call.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_close", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientClose(SafeAdminHandle admin, long timeoutMs);

    /// <summary>
    /// <c>kafka_admin_AdminClient_close_async</c> — the push form of the graceful
    /// close. Raw <see cref="IntPtr"/> handle: an <c>_async</c> op needs the
    /// span-the-op ref, not the marshaller's call-scoped one (see the class remarks).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_close_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientCloseAsync(
        IntPtr admin, long timeoutMs, AdminCallbacks.CloseCallback callback, IntPtr userData);

    // ---- kafka_admin_NewTopic_t — an INPUT handle the caller retains (ffi §B2) ----

    /// <summary>
    /// <c>kafka_admin_NewTopic_new</c> — one <c>NewTopic</c> request entry. Pass a
    /// negative <paramref name="numPartitions"/> / <paramref name="replicationFactor"/>
    /// to leave it unset (Java's <c>Optional.empty()</c>), so the broker's
    /// <c>num.partitions</c> / <c>default.replication.factor</c> applies. Returns null
    /// if <paramref name="name"/> is null — which the binding prevents by validating
    /// before the call (ffi §B5).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewTopicNew(IntPtr name, int numPartitions, short replicationFactor);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_put_config", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewTopicPutConfig(IntPtr topic, IntPtr key, IntPtr value);

    /// <summary>
    /// <c>kafka_admin_NewTopic_set_replicas_assignment</c> — assigns the replica broker
    /// ids for one partition. ⚠ Setting <em>any</em> assignment switches the entry to
    /// Java's <c>NewTopic(name, Map&lt;Integer, List&lt;Integer&gt;&gt;)</c> constructor,
    /// in which <c>num_partitions</c> / <c>replication_factor</c> are <b>not sent</b> —
    /// which is why the managed <see cref="Admin.NewTopic"/> models the two forms as
    /// mutually exclusive constructors rather than as independently settable state.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_set_replicas_assignment", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewTopicSetReplicasAssignment(IntPtr topic, int partition, int[] brokerIds, int count);

    /// <summary>
    /// <c>kafka_admin_NewTopic_destroy</c> — null-safe. The ABI <b>copies out</b> of the
    /// entries during the submit and "the caller retains ownership", so every built
    /// entry must be destroyed after the submit call returns or each RPC leaks.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewTopicDestroy(IntPtr topic);

    // ---- createTopics (ffi §B6 hookless one-shot; the per-key bridge) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_create_topics_async</c>. The binding drives the
    /// <c>_async</c> entry point even though the C# method is synchronous: the sync ABI
    /// twin blocks until every per-topic future resolves, which would make
    /// <c>CreateTopics</c> block — the very contract Java's non-blocking
    /// <c>createTopics</c> does not have.
    /// <para>
    /// A negative <paramref name="timeoutMs"/> means <b>unset</b> (the client default
    /// applies), <em>not</em> a zero timeout.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_create_topics_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientCreateTopicsAsync(
        IntPtr admin,
        IntPtr[] topics,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool validateOnly,
        [MarshalAs(UnmanagedType.I1)] bool retryOnQuotaViolation,
        AdminCallbacks.CreateTopicsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_CreateTopicsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int CreateTopicsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_CreateTopicsResult_get_key</c> — the topic name at
    /// <paramref name="index"/>, <b>borrowed</b> from the result (NUL-terminated,
    /// ffi §B3 row 2). Entries are sorted by topic name.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultGetKey(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_CreateTopicsResult_get_value</c> — the metadata for the topic at
    /// <paramref name="index"/>, <b>borrowed</b>, or null if that topic failed. Exactly
    /// one of <c>get_value</c> / <c>get_error</c> is non-null per index.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultGetValue(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_CreateTopicsResult_get_error</c> — that topic's error, or null if
    /// it was created successfully.
    /// <para>
    /// ⚠ <b>The pointer is BORROWED from the result handle.</b> The header is explicit:
    /// read it with the <c>kafka_common_KafkaError_*</c> accessors, but do <b>not</b>
    /// destroy it. Use <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/> — never
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>, which destroys in its
    /// <c>finally</c> and would double-free when the result root is destroyed.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultGetError(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_CreateTopicsResult_destroy</c> — frees the result root,
    /// invalidating every borrowed sub-handle taken from it. Null-safe, so the
    /// completion trampoline can call it unconditionally in its <c>finally</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateTopicsResultDestroy(IntPtr result);

    // ---- kafka_admin_TopicMetadataAndConfig_t — a borrowed child of the result ----

    /// <summary>
    /// <c>kafka_admin_TopicMetadataAndConfig_error</c> — Java's
    /// <c>TopicMetadataAndConfig.ensureSuccess()</c> condition: the topic <em>was</em>
    /// created (so the per-key error is null) but the broker returned no metadata.
    /// ⚠ <b>Borrowed</b> — read it, never destroy it.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicMetadataAndConfigError(IntPtr metadataAndConfig);

    /// <summary>
    /// <c>kafka_admin_TopicMetadataAndConfig_topic_id</c> — the topic id as Java's
    /// <c>Uuid.toString()</c> base64 form (borrowed), or an empty string when the
    /// metadata is unavailable.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_topic_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicMetadataAndConfigTopicId(IntPtr metadataAndConfig);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_num_partitions", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicMetadataAndConfigNumPartitions(IntPtr metadataAndConfig);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_replication_factor", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicMetadataAndConfigReplicationFactor(IntPtr metadataAndConfig);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_config_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicMetadataAndConfigConfigCount(IntPtr metadataAndConfig);

    /// <summary>
    /// <c>kafka_admin_TopicMetadataAndConfig_config_name</c> — the config entry name at
    /// <paramref name="index"/> (borrowed), or null if out of range. Entries are sorted
    /// by name.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_config_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicMetadataAndConfigConfigName(IntPtr metadataAndConfig, int index);

    /// <summary>
    /// <c>kafka_admin_TopicMetadataAndConfig_config_value</c> — the config entry value
    /// at <paramref name="index"/> (borrowed), or null if out of range <b>or</b> if the
    /// entry's value is null (Java's <c>ConfigEntry.value()</c> is nullable).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_config_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicMetadataAndConfigConfigValue(IntPtr metadataAndConfig, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_config_is_default", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicMetadataAndConfigConfigIsDefault(IntPtr metadataAndConfig, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_config_is_sensitive", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicMetadataAndConfigConfigIsSensitive(IntPtr metadataAndConfig, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicMetadataAndConfig_config_is_read_only", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicMetadataAndConfigConfigIsReadOnly(IntPtr metadataAndConfig, int index);
}
