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

    // ---- deleteTopics (M15/P2a) — the by-name and by-id entry points ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_delete_topics_async</c> — Java's
    /// <c>deleteTopics(TopicCollection.ofTopicNames(names), options)</c>.
    /// <paramref name="names"/> is the <c>const char *const *</c> array of pinned,
    /// NUL-terminated UTF-8 topic names; the caller keeps them pinned for the whole call.
    /// <para>
    /// A negative <paramref name="timeoutMs"/> means <b>unset</b> (the client default
    /// applies), <em>not</em> a zero timeout.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_delete_topics_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDeleteTopicsAsync(
        IntPtr admin,
        IntPtr[] names,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool retryOnQuotaViolation,
        AdminCallbacks.DeleteTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_delete_topics_by_ids_async</c> — Java's
    /// <c>deleteTopics(TopicCollection.ofTopicIds(ids), options)</c>.
    /// <para>
    /// ⚠ <paramref name="topicIds"/> are <b>base64 topic-id strings</b> (Java's
    /// <c>Uuid.toString()</c> form), not binary UUIDs, and the result's keys come back as
    /// the same base64. ⚠ An unparseable or NULL id makes the callback fire
    /// <b>synchronously on the calling thread, before this function returns</b> — the
    /// inline path P1 built for and could not exercise.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_delete_topics_by_ids_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDeleteTopicsByIdsAsync(
        IntPtr admin,
        IntPtr[] topicIds,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool retryOnQuotaViolation,
        AdminCallbacks.DeleteTopicsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_DeleteTopicsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DeleteTopicsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DeleteTopicsResult_get_key</c> — the key at
    /// <paramref name="index"/>, <b>borrowed</b> (NUL-terminated, ffi §B3 row 2): the
    /// topic <em>name</em> for the by-name entry point, the base64 topic <em>id</em> for
    /// the by-id one.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteTopicsResultGetKey(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DeleteTopicsResult_get_error</c> — that topic's error, or null if it
    /// was deleted successfully. There is deliberately <b>no</b> <c>get_value</c>: Java's
    /// per-key future is <c>KafkaFuture&lt;Void&gt;</c>, so a null error <em>is</em> the
    /// success value (result shape 2).
    /// <para>
    /// ⚠ <b>The pointer is BORROWED from the result handle</b> — read it with the
    /// <c>kafka_common_KafkaError_*</c> accessors, never destroy it. Use
    /// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/>, never
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteTopicsResultGetError(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DeleteTopicsResult_destroy</c> — frees the result root,
    /// invalidating every borrowed sub-handle taken from it. Null-safe, so the completion
    /// trampoline can call it unconditionally in its <c>finally</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteTopicsResultDestroy(IntPtr result);

    // ---- describeTopics (M15/P2a) — the by-name and by-id entry points ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_topics_async</c> — Java's
    /// <c>describeTopics(TopicCollection.ofTopicNames(names), options)</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_describe_topics_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDescribeTopicsAsync(
        IntPtr admin,
        IntPtr[] names,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool includeAuthorizedOperations,
        int partitionSizeLimitPerResponse,
        AdminCallbacks.DescribeTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_topics_by_ids_async</c> — Java's
    /// <c>describeTopics(TopicCollection.ofTopicIds(ids), options)</c>. The same base64
    /// topic-id contract, and the same inline-callback path on an unparseable id, as
    /// <see cref="AdminClientDeleteTopicsByIdsAsync"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_describe_topics_by_ids_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDescribeTopicsByIdsAsync(
        IntPtr admin,
        IntPtr[] topicIds,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool includeAuthorizedOperations,
        int partitionSizeLimitPerResponse,
        AdminCallbacks.DescribeTopicsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_DescribeTopicsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeTopicsResultCount(IntPtr result);

    /// <inheritdoc cref="DeleteTopicsResultGetKey"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTopicsResultGetKey(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeTopicsResult_get_value</c> — the description for the topic
    /// at <paramref name="index"/>, <b>borrowed</b>, or null if that topic failed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTopicsResultGetValue(IntPtr result, int index);

    /// <inheritdoc cref="DeleteTopicsResultGetError"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTopicsResultGetError(IntPtr result, int index);

    /// <inheritdoc cref="DeleteTopicsResultDestroy"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTopicsResultDestroy(IntPtr result);

    // ---- kafka_admin_TopicDescription_t — a borrowed child of the result ----

    /// <summary><c>kafka_admin_TopicDescription_name</c> — borrowed, NUL-terminated.</summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionName(IntPtr description);

    /// <summary>
    /// <c>kafka_admin_TopicDescription_topic_id</c> — the topic id as Java's
    /// <c>Uuid.toString()</c> base64 form (borrowed).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_topic_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionTopicId(IntPtr description);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_is_internal", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicDescriptionIsInternal(IntPtr description);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_partition_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicDescriptionPartitionCount(IntPtr description);

    /// <summary>
    /// <c>kafka_admin_TopicDescription_partition</c> — the partition at
    /// <paramref name="index"/>, <b>borrowed</b>, or null if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionPartition(IntPtr description, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_authorized_operation_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicDescriptionAuthorizedOperationCount(IntPtr description);

    /// <summary>
    /// <c>kafka_admin_TopicDescription_has_authorized_operations</c> — ⚠ the
    /// absent-versus-empty <b>discriminant</b>. <see langword="false"/> is Java's
    /// <c>authorizedOperations() == null</c>; <see langword="true"/> with a count of 0 is
    /// a reported-but-empty set. The count alone cannot tell them apart, which is why this
    /// function exists and why the marshaller must read it.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_has_authorized_operations", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicDescriptionHasAuthorizedOperations(IntPtr description);

    /// <summary>
    /// <c>kafka_admin_TopicDescription_authorized_operation</c> — the
    /// <c>AclOperation</c> <b>wire code</b> at <paramref name="index"/> (Java's
    /// <c>AclOperation.code()</c>), or -1 if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_authorized_operation", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicDescriptionAuthorizedOperation(IntPtr description, int index);

    // ---- kafka_admin_TopicPartitionInfo_t — a borrowed child of the description ----

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoPartition(IntPtr info);

    /// <summary>
    /// <c>kafka_admin_TopicPartitionInfo_leader</c> — the leader, <b>borrowed</b>, or null
    /// if there is none. The same <c>kafka_common_Node_t</c> the consumer surface already
    /// marshals, so <see cref="NodeMarshal"/> is reused unchanged.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_leader", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoLeader(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoReplicaCount(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoReplica(IntPtr info, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_isr_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoIsrCount(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_isr", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoIsr(IntPtr info, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_elr_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoElrCount(IntPtr info);

    /// <summary>
    /// <c>kafka_admin_TopicPartitionInfo_has_elr</c> — ⚠ the absent-versus-empty
    /// <b>discriminant</b> for the eligible-leader-replica set, exactly as
    /// <see cref="TopicDescriptionHasAuthorizedOperations"/> is for authorized operations.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_has_elr", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicPartitionInfoHasElr(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_elr", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoElr(IntPtr info, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_last_known_elr_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoLastKnownElrCount(IntPtr info);

    /// <inheritdoc cref="TopicPartitionInfoHasElr"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_has_last_known_elr", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicPartitionInfoHasLastKnownElr(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicPartitionInfo_last_known_elr", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoLastKnownElr(IntPtr info, int index);

    // ---- listTopics (M15/P2b) — result shape 3: ONE aggregate future, no per-key error ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_list_topics_async</c> — Java's
    /// <c>listTopics(ListTopicsOptions)</c>.
    /// <para>
    /// ⚠ <b>No key array.</b> Unlike every other admin RPC bound so far, the keys are
    /// discovered from the response rather than supplied by the caller — which is why
    /// this one cannot use the per-key bridge (it has no keys to pre-register) and uses
    /// <c>SingleAdminOperation</c> instead.
    /// </para>
    /// <para>
    /// A negative <paramref name="timeoutMs"/> means <b>unset</b> (the client default
    /// applies), <em>not</em> a zero timeout.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_list_topics_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientListTopicsAsync(
        IntPtr admin,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool listInternal,
        AdminCallbacks.ListTopicsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_ListTopicsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListTopicsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_ListTopicsResult_get_key</c> — the topic name at
    /// <paramref name="index"/>, <b>borrowed</b> (NUL-terminated, ffi §B3 row 2).
    /// Entries are sorted by topic name.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsResult_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListTopicsResultGetKey(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_ListTopicsResult_get_value</c> — the listing at
    /// <paramref name="index"/>, <b>borrowed</b> from the result root.
    /// </summary>
    /// <remarks>
    /// ⚠ There is deliberately no <c>ListTopicsResult_get_error</c> to declare beside
    /// this: the ABI does not have one. That absence <em>is</em> the shape — either the
    /// whole call fails (through the callback's own <c>error</c>) or the whole map
    /// succeeds.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListTopicsResultGetValue(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTopicsResultDestroy(IntPtr result);

    // ---- kafka_admin_TopicListing_t — a Category-4 borrowed view (no destroy) ----

    /// <summary><c>kafka_admin_TopicListing_name</c> — borrowed, NUL-terminated.</summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicListing_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicListingName(IntPtr listing);

    /// <summary>
    /// <c>kafka_admin_TopicListing_topic_id</c> — the topic id as a <b>base64</b> string
    /// (Java's <c>Uuid.toString()</c>), borrowed. Parsed back to a <see cref="Uuid"/> so
    /// the public shape matches Java's <c>TopicListing.topicId()</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_TopicListing_topic_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicListingTopicId(IntPtr listing);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicListing_is_internal", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool TopicListingIsInternal(IntPtr listing);

    // ---- kafka_admin_NewPartitions_t — the second (and last) admin INPUT handle ----

    /// <summary>
    /// <c>kafka_admin_NewPartitions_new</c> — Java's two <c>NewPartitions</c> factories,
    /// selected by <paramref name="hasAssignments"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b><paramref name="hasAssignments"/> is an explicit discriminant, never inferred
    /// from how many assignments were appended.</b> <c>increaseTo(n, emptyList())</c> is
    /// legal Java and is a <em>different request</em> from <c>increaseTo(n)</c>:
    /// <c>CreatePartitionsRequest.json:36</c> marks <c>Assignments</c>
    /// <c>"nullableVersions": "0+"</c>, and the broker rejects a present-but-empty list
    /// with <c>INVALID_REPLICA_ASSIGNMENT</c> where a null one succeeds. So a
    /// <c>?? Array.Empty&lt;…&gt;()</c> anywhere on this path silently converts a
    /// broker-rejected request into an accepted one, or vice versa.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewPartitionsNew(
        int totalCount,
        [MarshalAs(UnmanagedType.I1)] bool hasAssignments);

    /// <summary>
    /// <c>kafka_admin_NewPartitions_add_assignment</c> — appends one new partition's
    /// replica broker ids. The blittable <c>int[]</c> is pinned by the marshaller for the
    /// duration of the call; the ABI copies out during it (ffi §A4 call-scoped).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_add_assignment", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewPartitionsAddAssignment(IntPtr partitions, int[] brokerIds, int count);

    /// <summary>
    /// <c>kafka_admin_NewPartitions_destroy</c> — null-safe. The submit copies out and
    /// "the caller retains ownership", so every handle built must be destroyed after the
    /// submit returns.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewPartitionsDestroy(IntPtr partitions);

    // ---- createPartitions (M15/P2b) — result shape 2 (per-key void) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_create_partitions_async</c> — Java's
    /// <c>createPartitions(Map&lt;String, NewPartitions&gt;, options)</c>. Java's map
    /// becomes <b>two parallel arrays</b>: entry <c>i</c> pairs <paramref name="topics"/>
    /// with <paramref name="newPartitions"/>.
    /// <para>
    /// A negative <paramref name="timeoutMs"/> means <b>unset</b> (the client default
    /// applies), <em>not</em> a zero timeout.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_create_partitions_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientCreatePartitionsAsync(
        IntPtr admin,
        IntPtr[] topics,
        IntPtr[] newPartitions,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool validateOnly,
        [MarshalAs(UnmanagedType.I1)] bool retryOnQuotaViolation,
        AdminCallbacks.CreatePartitionsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_CreatePartitionsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int CreatePartitionsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_CreatePartitionsResult_get_key</c> — the topic name at
    /// <paramref name="index"/>, <b>borrowed</b> (NUL-terminated).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsResult_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreatePartitionsResultGetKey(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_CreatePartitionsResult_get_error</c> — that topic's error, or null
    /// if its partitions were created.
    /// <para>
    /// ⚠ <b>BORROWED</b> (<c>const</c>): it dies with the result root and must
    /// <b>never</b> be destroyed — <see cref="KafkaException.FromBorrowedHandle"/>, not
    /// <see cref="KafkaException.FromHandle"/>. There is deliberately no
    /// <c>_get_value</c> to declare beside it: Java's per-key future is
    /// <c>KafkaFuture&lt;Void&gt;</c>, so a null error <em>is</em> the success value.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreatePartitionsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreatePartitionsResultDestroy(IntPtr result);

    // ---- deleteRecords (M15/P2b) — composite key + INLINE-SCALAR value ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_delete_records_async</c> — Java's
    /// <c>deleteRecords(Map&lt;TopicPartition, RecordsToDelete&gt;, options)</c>. Java's
    /// map becomes <b>three parallel arrays</b>: entry <c>i</c> is
    /// <c>(topics[i], partitions[i]) → RecordsToDelete.beforeOffset(beforeOffsets[i])</c>.
    /// <para>
    /// <c>RecordsToDelete</c> carries only that offset, so it needs no input handle. A
    /// <c>beforeOffsets</c> entry of <c>-1</c> truncates that partition to its high
    /// watermark (Java's documented <c>beforeOffset(-1)</c> behavior) — it is a
    /// <em>value</em>, not a sentinel this binding interprets.
    /// </para>
    /// <para>
    /// A negative <paramref name="timeoutMs"/> means <b>unset</b>;
    /// <c>DeleteRecordsOptions</c> has no other field in Java.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_delete_records_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDeleteRecordsAsync(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        long[] beforeOffsets,
        int count,
        int timeoutMs,
        AdminCallbacks.DeleteRecordsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_DeleteRecordsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DeleteRecordsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DeleteRecordsResult_get_topic</c> — <b>half</b> of the composite
    /// key at <paramref name="index"/>, borrowed (NUL-terminated). Entries are sorted by
    /// topic name then partition id.
    /// </summary>
    /// <remarks>
    /// ⚠ This result declares <b>no <c>get_key</c> at all</b>: its key is composed from
    /// this and <see cref="DeleteRecordsResultGetPartition"/>. That is why the walker's
    /// key reader takes <c>(result, index)</c> rather than a string (M15/P2a's G1).
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteRecordsResultGetTopic(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DeleteRecordsResult_get_partition</c> — the other half of the
    /// composite key, or <c>-1</c> if <paramref name="index"/> is out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DeleteRecordsResultGetPartition(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DeleteRecordsResult_get_low_watermark</c> — Java's
    /// <c>DeletedRecords.lowWatermark()</c> for the entry at <paramref name="index"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This is an INLINE SCALAR, not a pointer to a borrowed child.</b> It is the
    /// reason the walker's value axis had to be generalized in M15/P2b: there is no
    /// handle to copy out of, so a pointer-shaped value accessor cannot describe it.
    /// <para>
    /// ⚠ <b><c>-1</c> is NOT the failure signal.</b> The header gives it three meanings
    /// at once — "or -1 if that partition failed … or <c>index</c> is out of range" — and
    /// <c>-1</c> is also a legitimate low watermark. The authoritative signal is
    /// <see cref="DeleteRecordsResultGetError"/><c> != IntPtr.Zero</c>; a <c>-1</c>
    /// watermark with a <b>null</b> error is a <b>success</b> carrying <c>-1</c>.
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_get_low_watermark", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long DeleteRecordsResultGetLowWatermark(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DeleteRecordsResult_get_error</c> — that partition's error, or null
    /// if it succeeded. <b>BORROWED</b> (<c>const</c>) — read, never destroy.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteRecordsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteRecordsResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 1: describeCluster (result shape 5) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_cluster_async</c> — Java's
    /// <c>describeCluster(DescribeClusterOptions)</c>. No key array: Java's result holds
    /// four attribute futures rather than a per-key map, which the header states outright
    /// ("unlike the batch RPCs there are no per-key errors: any failure is returned").
    /// <para>
    /// A negative <paramref name="timeoutMs"/> means <b>unset</b>. The two booleans are
    /// <c>DescribeClusterOptions.includeAuthorizedOperations</c> and
    /// <c>.includeFencedBrokers</c>; both carry <c>[MarshalAs(I1)]</c> because C
    /// <c>bool</c> is one byte and the default marshalling of a 4-byte Win32 <c>BOOL</c>
    /// would corrupt the following argument.
    /// </para>
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_describe_cluster_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDescribeClusterAsync(
        IntPtr admin,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool includeAuthorizedOperations,
        [MarshalAs(UnmanagedType.I1)] bool includeFencedBrokers,
        AdminCallbacks.DescribeClusterCallback callback,
        IntPtr userData);

    // ---- kafka_admin_DescribeClusterResult_t — a Category-3 owned borrow-root ----

    /// <summary>
    /// <c>kafka_admin_DescribeClusterResult_cluster_id</c> — Java's <c>clusterId()</c>,
    /// borrowed and NUL-terminated (ffi §B3 row 2).
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_cluster_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultClusterId(IntPtr result);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_node_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeClusterResultNodeCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DescribeClusterResult_get_node</c> — the node at
    /// <paramref name="index"/>, or null if out of range. <b>BORROWED</b> (<c>const</c>)
    /// — read with the <c>kafka_common_Node_*</c> accessors through
    /// <see cref="NodeMarshal"/>; never destroyed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_get_node", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultGetNode(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeClusterResult_controller</c> — the controller node, or
    /// <b>null if there is none</b>, which the header ties to Java: "Java's
    /// <c>controller()</c> yields null". <b>BORROWED</b>. A null here is a successful
    /// outcome, not a failure.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_controller", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultController(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DescribeClusterResult_authorized_operation_count</c> — always
    /// non-negative.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This count cannot express Java's null.</b> The header: "0 covers both 'the
    /// broker did not report them' (Java yields null) and 'reported, but none authorized';
    /// use <c>…_has_authorized_operations</c> to tell them apart." Reading the count alone
    /// silently collapses null into empty.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_authorized_operation_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeClusterResultAuthorizedOperationCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DescribeClusterResult_has_authorized_operations</c> — the
    /// discriminant the count cannot carry: "<c>false</c> is Java's null, <c>true</c> with
    /// a count of 0 is a reported-but-empty set".
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_has_authorized_operations", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool DescribeClusterResultHasAuthorizedOperations(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DescribeClusterResult_authorized_operation</c> — the
    /// <c>AclOperation</c> wire code at <paramref name="index"/>, or <c>-1</c> if out of
    /// range or absent. Decoded through <see cref="AuthorizedOperationsMarshal"/>, which
    /// mirrors Java's <c>AclOperation.fromCode</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_authorized_operation", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeClusterResultAuthorizedOperation(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClusterResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 1: listConfigResources (result sub-shape 3b) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_list_config_resources_async</c> — Java's
    /// <c>listConfigResources(Set&lt;ConfigResource.Type&gt;, options)</c>. The set of
    /// types becomes one <c>const int32_t*</c> array of
    /// <c>ConfigResource.Type.id()</c> codes.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>An empty filter is NOT an error — it is the "every supported type" request.</b>
    /// The header: "pass NULL or <c>count == 0</c> for Java's empty set, which means 'every
    /// supported type'", matching Java's no-argument <c>listConfigResources()</c>, which
    /// delegates with <c>Set.of()</c> (<c>Admin.java:1812</c>). So the binding must never
    /// reject an empty or absent collection here.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_list_config_resources_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientListConfigResourcesAsync(
        IntPtr admin,
        int[] resourceTypes,
        int count,
        int timeoutMs,
        AdminCallbacks.ListConfigResourcesCallback callback,
        IntPtr userData);

    // ---- kafka_admin_ListConfigResourcesResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListConfigResourcesResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_ListConfigResourcesResult_get_type</c> — the
    /// <c>ConfigResource.Type.id()</c> at <paramref name="index"/>, or <c>-1</c> if out of
    /// range. Entries are sorted by <c>(type id, name)</c>.
    /// </summary>
    /// <remarks>
    /// The walk is bounded by <see cref="ListConfigResourcesResultCount"/>, so the
    /// out-of-range <c>-1</c> is unreachable here. How a code becomes a
    /// <see cref="ConfigResourceType"/> — including why <c>-1</c> is not
    /// <see cref="ConfigResourceType.Unknown"/> — is stated in
    /// <see cref="ConfigResourceMarshal.TypeFromId"/>.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesResult_get_type", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListConfigResourcesResultGetType(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_ListConfigResourcesResult_get_name</c> — the resource name at
    /// <paramref name="index"/>, borrowed and NUL-terminated, or null if out of range.
    /// </summary>
    /// <remarks>
    /// ⚠ There is deliberately no <c>ListConfigResourcesResult_get_error</c> to declare
    /// beside these: Java has a single future here, so any failure is a call failure and
    /// arrives through the callback's own <c>error</c> parameter.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesResult_get_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListConfigResourcesResultGetName(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConfigResourcesResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 1: listClientMetricsResources (result sub-shape 3b) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_list_client_metrics_resources_async</c> — Java's
    /// <c>listClientMetricsResources(options)</c>, which Java deprecates
    /// (<c>@Deprecated(since = "4.1", forRemoval = true)</c>, <c>Admin.java:1821-1824</c>)
    /// in favour of <c>listConfigResources</c> filtered to <c>CLIENT_METRICS</c>. No
    /// arrays at all: the only inputs are the timeout and the completion.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_list_client_metrics_resources_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientListClientMetricsResourcesAsync(
        IntPtr admin,
        int timeoutMs,
        AdminCallbacks.ListClientMetricsResourcesCallback callback,
        IntPtr userData);

    // ---- kafka_admin_ListClientMetricsResourcesResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_ListClientMetricsResourcesResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListClientMetricsResourcesResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_ListClientMetricsResourcesResult_get_name</c> — the resource name at
    /// <paramref name="index"/>, borrowed and NUL-terminated, or null if out of range.
    /// Entries are sorted by name. This is the result's <b>only</b> per-index accessor:
    /// the listing <em>is</em> the name.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListClientMetricsResourcesResult_get_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListClientMetricsResourcesResultGetName(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListClientMetricsResourcesResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListClientMetricsResourcesResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 2: kafka_admin_Config_t / kafka_admin_ConfigEntry_t ----
    //
    // Both are BORROWED views hanging off a result root (ffi §B2 Category 4): a
    // `Config_t` comes from `DescribeConfigsResult_get_value(i)`, a `ConfigEntry_t` from
    // `Config_get_entry(j)`, and every string below borrows from the same root. Neither
    // has a `_destroy`; both die with the root. Everything is copied out eagerly by
    // ConfigMarshal before that happens.

    [DllImport(DllName, EntryPoint = "kafka_admin_Config_entry_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConfigEntryCount(IntPtr config);

    /// <summary>
    /// <c>kafka_admin_Config_get_entry</c> — the entry at <paramref name="index"/>
    /// (borrowed), or null if out of range. Entries are sorted by name.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_Config_get_entry", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigGetEntry(IntPtr config, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryName(IntPtr entry);

    /// <summary>
    /// <c>kafka_admin_ConfigEntry_value</c> — the value (borrowed), or <b>null when the
    /// value is null</b>: the header ties that to Java's nullable <c>value()</c>, noting
    /// "sensitive configs come back null". The null must round-trip as
    /// <see langword="null"/>, never as <c>""</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryValue(IntPtr entry);

    /// <summary>
    /// <c>kafka_admin_ConfigEntry_source</c> — the source as <b>Java's enum constant
    /// name</b> (borrowed), e.g. <c>"DYNAMIC_TOPIC_CONFIG"</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>A NAME, not an id.</b> The header: "<c>ConfigEntry.ConfigSource</c> has no
    /// numeric id in Java, so the name is the contract" — the opposite convention from
    /// <see cref="ListConfigResourcesResultGetType"/> and the <c>op_types</c> array, which
    /// carry <c>int32_t</c> wire codes. Decoded by
    /// <see cref="ConfigMarshal.SourceFromName"/>.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_source", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntrySource(IntPtr entry);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_is_sensitive", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool ConfigEntryIsSensitive(IntPtr entry);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_is_read_only", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool ConfigEntryIsReadOnly(IntPtr entry);

    /// <summary>
    /// <c>kafka_admin_ConfigEntry_type</c> — the data type as <b>Java's enum constant
    /// name</b> (borrowed), e.g. <c>"STRING"</c>. Decoded by
    /// <see cref="ConfigMarshal.TypeFromName"/>; see
    /// <see cref="ConfigEntrySource"/> for why it is a name.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_type", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryType(IntPtr entry);

    /// <summary>
    /// <c>kafka_admin_ConfigEntry_documentation</c> — the documentation (borrowed), or
    /// <b>null when the broker did not report it</b> (Java's <c>documentation()</c> is
    /// nullable). The null must round-trip as <see langword="null"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_documentation", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryDocumentation(IntPtr entry);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_synonym_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConfigEntrySynonymCount(IntPtr entry);

    /// <summary>
    /// <c>kafka_admin_ConfigEntry_synonym_name</c> — the synonym's name at
    /// <paramref name="index"/> (borrowed), or null if out of range. ⚠ Synonyms "keep
    /// Java's precedence order and are not sorted" — the order is the contract, which is
    /// why Java's accessor returns a <c>List</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_synonym_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntrySynonymName(IntPtr entry, int index);

    /// <summary>
    /// <c>kafka_admin_ConfigEntry_synonym_value</c> — the synonym's value at
    /// <paramref name="index"/> (borrowed), or null <b>if the value is null OR the index is
    /// out of range</b>.
    /// </summary>
    /// <remarks>
    /// ⚠ An overloaded null, the same class as <c>deleteRecords</c>' <c>-1</c>. The walk is
    /// bounded by <see cref="ConfigEntrySynonymCount"/>, so out-of-range is unreachable and
    /// a null inside the bound is a <b>genuine null value</b>.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_synonym_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntrySynonymValue(IntPtr entry, int index);

    /// <inheritdoc cref="ConfigEntrySource"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_synonym_source", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntrySynonymSource(IntPtr entry, int index);

    // ---- M15/P3 Stage 2: describeConfigs (result shape 1) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_configs_async</c> — Java's
    /// <c>describeConfigs(Collection&lt;ConfigResource&gt;, options)</c>. Java's collection
    /// becomes <b>two parallel arrays</b>: entry <c>i</c> is the resource
    /// <c>(resource_types[i], resource_names[i])</c>, the type being a
    /// <c>ConfigResource.Type.id()</c> code.
    /// </summary>
    /// <remarks>
    /// ⚠ The header warns that "an entry with a NULL name is skipped" — silently — so the
    /// null check happens at the C# boundary before any pin (ffi §B5). Both booleans carry
    /// <c>[MarshalAs(I1)]</c>: C <c>bool</c> is one byte, and the default marshalling of a
    /// 4-byte Win32 <c>BOOL</c> would corrupt the argument beside it.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_describe_configs_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDescribeConfigsAsync(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool includeSynonyms,
        [MarshalAs(UnmanagedType.I1)] bool includeDocumentation,
        AdminCallbacks.DescribeConfigsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_DescribeConfigsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeConfigsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DescribeConfigsResult_get_key_type</c> — <b>half</b> of the composite
    /// key at <paramref name="index"/>: the <c>ConfigResource.Type.id()</c>, or <c>-1</c>
    /// if out of range. Decoded through <see cref="ConfigResourceMarshal.TypeFromId"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_get_key_type", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeConfigsResultGetKeyType(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeConfigsResult_get_key_name</c> — the other half of the
    /// composite key, borrowed and NUL-terminated, or null if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_get_key_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConfigsResultGetKeyName(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeConfigsResult_get_value</c> — that resource's configuration,
    /// <b>borrowed</b>, or null if the resource failed or the index is out of range. The
    /// walker reads the per-key error first, so a null reaching the value reader is the
    /// unreachable case.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConfigsResultGetValue(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeConfigsResult_get_error</c> — that resource's error, or null
    /// if it succeeded. <b>BORROWED</b> (<c>const</c>, and the header adds "do not destroy
    /// it") — read with <see cref="KafkaException.FromBorrowedHandle"/>, never destroyed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConfigsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConfigsResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 2: incrementalAlterConfigs (result shape 2) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_incremental_alter_configs_async</c> — Java's
    /// <c>incrementalAlterConfigs(Map&lt;ConfigResource, Collection&lt;AlterConfigOp&gt;&gt;, options)</c>.
    /// Java's map becomes <b>five parallel arrays, one row per operation</b>: row <c>i</c>
    /// applies <c>(config_names[i] -&gt; config_values[i], op_types[i])</c> to the resource
    /// <c>(resource_types[i], resource_names[i])</c>, with rows naming the same resource
    /// grouped in order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>A NULL <c>config_values</c> entry is the null value <c>DELETE</c> uses</b> —
    /// the header says so — so the array carries <see cref="IntPtr.Zero"/> for it and
    /// nothing on the path substitutes an empty string. A row with a NULL resource name or
    /// config name is <em>silently skipped</em> by the ABI, so both are rejected at the C#
    /// boundary instead (ffi §B5).
    /// </para>
    /// <para>
    /// ⚠⚠ <b>An unknown <c>op_types</c> code fails the whole call — on the INLINE callback
    /// path.</b> This entry point's own async doc extends the inline trigger set beyond a
    /// NULL handle: the callback runs "synchronously on the calling thread, before this
    /// function returns, when the RPC cannot be submitted at all (a NULL <c>admin</c>
    /// handle, or an unknown <c>AlterConfigOp.OpType</c> code)". So ordinary bad input
    /// reaches the inline path here, which makes
    /// <c>TaskCreationOptions.RunContinuationsAsynchronously</c> load-bearing and the
    /// <c>GCHandle</c> free on that path a correctness requirement rather than a corner.
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_incremental_alter_configs_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientIncrementalAlterConfigsAsync(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        IntPtr[] configNames,
        IntPtr[] configValues,
        int[] opTypes,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool validateOnly,
        AdminCallbacks.IncrementalAlterConfigsCallback callback,
        IntPtr userData);

    // ---- kafka_admin_AlterConfigsResult_t — a Category-3 owned borrow-root ----

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterConfigsResultCount(IntPtr result);

    /// <inheritdoc cref="DescribeConfigsResultGetKeyType"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_get_key_type", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterConfigsResultGetKeyType(IntPtr result, int index);

    /// <inheritdoc cref="DescribeConfigsResultGetKeyName"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_get_key_name", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigsResultGetKeyName(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_AlterConfigsResult_get_error</c> — that resource's error, or null if
    /// it was altered successfully. <b>BORROWED</b> — read, never destroy.
    /// </summary>
    /// <remarks>
    /// ⚠ There is deliberately no <c>_get_value</c> to declare beside this, and the header
    /// states why: "Java's per-resource future is <c>KafkaFuture&lt;Void&gt;</c>, so a null
    /// error <em>is</em> the success value" — result shape 2.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConfigsResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 3: the log-dir value tree (borrowed views, ffi §B2 Category 4) ----
    //
    // A `LogDirDescriptionMap_t` comes from `DescribeLogDirsResult_get_value(i)`, a
    // `LogDirDescription_t` from `LogDirDescriptionMap_get_value(j)`, and a
    // `ReplicaLogDirInfo_t` from `DescribeReplicaLogDirsResult_get_value(i)`. None has a
    // `_destroy`; all die with the one result root, and LogDirMarshal copies everything out
    // before that happens.

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescriptionMap_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int LogDirDescriptionMapCount(IntPtr map);

    /// <summary>
    /// <c>kafka_admin_LogDirDescriptionMap_get_key</c> — the log-dir path at
    /// <paramref name="index"/> (borrowed), or null if out of range. Entries are sorted by
    /// path.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescriptionMap_get_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LogDirDescriptionMapGetKey(IntPtr map, int index);

    /// <inheritdoc cref="LogDirDescriptionMapGetKey"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescriptionMap_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LogDirDescriptionMapGetValue(IntPtr map, int index);

    /// <summary>
    /// <c>kafka_admin_LogDirDescription_error</c> — ⚠⚠ the <b>SECOND borrowed error</b> in
    /// <c>describeLogDirs</c>, nested inside the value tree and distinct from the per-broker
    /// <see cref="DescribeLogDirsResultGetError"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>BORROWED</b> (<c>const</c>, and the header adds "do not destroy it") — read with
    /// <see cref="KafkaException.FromBorrowedHandle"/>, never destroyed. ⚠ It also means
    /// something different: the header states "it is <em>not</em> the per-broker error …
    /// the broker answered, but this particular directory is offline or unreadable", so the
    /// per-broker task <b>succeeds</b> carrying a description whose error is non-null.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LogDirDescriptionError(IntPtr description);

    /// <summary>
    /// <c>kafka_admin_LogDirDescription_total_bytes</c> — the volume's total size, or
    /// <b><c>-1</c> when the broker did not report it</b> (Java's empty
    /// <c>OptionalLong</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ Here <c>-1</c> <b>is</b> a sentinel and maps to <see langword="null"/>, which is the
    /// opposite of <see cref="DeleteRecordsResultGetLowWatermark"/>, where <c>-1</c> is a
    /// legitimate value. <c>0</c> is a real size. See <see cref="LogDirMarshal.VolumeBytes"/>.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_total_bytes", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LogDirDescriptionTotalBytes(IntPtr description);

    /// <inheritdoc cref="LogDirDescriptionTotalBytes"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_usable_bytes", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LogDirDescriptionUsableBytes(IntPtr description);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int LogDirDescriptionReplicaCount(IntPtr description);

    /// <summary>
    /// <c>kafka_admin_LogDirDescription_replica_topic</c> — one of six <b>flattened</b>
    /// indexed replica accessors on the parent description; there is no
    /// <c>ReplicaInfo_t</c> handle. Replicas are sorted by <c>(topic, partition)</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LogDirDescriptionReplicaTopic(IntPtr description, int index);

    /// <inheritdoc cref="LogDirDescriptionReplicaTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int LogDirDescriptionReplicaPartition(IntPtr description, int index);

    /// <summary>
    /// <c>kafka_admin_LogDirDescription_replica_size</c> — the replica's on-disk size.
    /// ⚠ Its documented <c>-1</c> is the <b>out-of-range</b> return only, which the
    /// <c>replica_count</c>-bounded loop cannot reach — it is <b>not</b> the
    /// volume-size sentinel, so the value passes through unchanged.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_size", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LogDirDescriptionReplicaSize(IntPtr description, int index);

    /// <inheritdoc cref="LogDirDescriptionReplicaSize"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_offset_lag", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LogDirDescriptionReplicaOffsetLag(IntPtr description, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_is_future", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool LogDirDescriptionReplicaIsFuture(IntPtr description, int index);

    /// <summary>
    /// <c>kafka_admin_ReplicaLogDirInfo_current_replica_log_dir</c> — borrowed, or <b>null
    /// when the broker hosts no replica of that partition</b>. The null must round-trip as
    /// <see langword="null"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaLogDirInfo_current_replica_log_dir", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ReplicaLogDirInfoCurrentReplicaLogDir(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaLogDirInfo_current_replica_offset_lag", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ReplicaLogDirInfoCurrentReplicaOffsetLag(IntPtr info);

    /// <summary>
    /// <c>kafka_admin_ReplicaLogDirInfo_future_replica_log_dir</c> — borrowed, or <b>null
    /// when the replica is not being moved</b>, which is the ordinary case for a replica at
    /// rest. The null must round-trip as <see langword="null"/>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaLogDirInfo_future_replica_log_dir", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ReplicaLogDirInfoFutureReplicaLogDir(IntPtr info);

    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaLogDirInfo_future_replica_offset_lag", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ReplicaLogDirInfoFutureReplicaOffsetLag(IntPtr info);

    // ---- M15/P3 Stage 3: describeLogDirs (result shape 1, SCALAR int key) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_log_dirs_async</c> — Java's
    /// <c>describeLogDirs(Collection&lt;Integer&gt;, options)</c>. One array of broker ids;
    /// <c>DescribeLogDirsOptions</c> has no field of its own in Java beyond the timeout.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_describe_log_dirs_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDescribeLogDirsAsync(
        IntPtr admin,
        int[] brokers,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeLogDirsCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeLogDirsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_DescribeLogDirsResult_get_broker</c> — the key at
    /// <paramref name="index"/>: a <b>bare scalar</b> broker id, the first such key in M15.
    /// <c>-1</c> if out of range. Entries are sorted by broker id.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_get_broker", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeLogDirsResultGetBroker(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeLogDirsResult_get_value</c> — that broker's log-dir map,
    /// <b>borrowed</b>, or null if the broker failed. The walker reads the per-key error
    /// first, so a null reaching the value reader is the unreachable case.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeLogDirsResultGetValue(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeLogDirsResult_get_error</c> — <b>that broker's</b> error, or
    /// null if it answered. <b>BORROWED</b> — read, never destroy.
    /// </summary>
    /// <remarks>
    /// ⚠ Distinct from <see cref="LogDirDescriptionError"/>, the second borrowed error
    /// nested in the value tree. This one faults the key's task; that one does not.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeLogDirsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeLogDirsResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 3: alterReplicaLogDirs (result shape 2, 3-part key) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_alter_replica_log_dirs_async</c> — Java's
    /// <c>alterReplicaLogDirs(Map&lt;TopicPartitionReplica, String&gt;, options)</c>. Java's
    /// map becomes <b>four parallel arrays</b>: entry <c>i</c> moves the replica
    /// <c>(topics[i], partitions[i], broker_ids[i])</c> to <c>log_dirs[i]</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ The header warns that "an entry with a NULL topic or NULL log dir is skipped" —
    /// silently — so both are rejected at the C# boundary before any pin (ffi §B5). ⚠ Note
    /// the map is <c>K → V</c>, not <c>K → Collection&lt;V&gt;</c>, so every key produces
    /// exactly one row and the zero-row shape of §9.1 item 9 cannot arise here.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_alter_replica_log_dirs_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientAlterReplicaLogDirsAsync(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int[] brokerIds,
        IntPtr[] logDirs,
        int count,
        int timeoutMs,
        AdminCallbacks.AlterReplicaLogDirsCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterReplicaLogDirsResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_AlterReplicaLogDirsResult_get_topic</c> — one <b>third</b> of the
    /// composite key at <paramref name="index"/>, borrowed and NUL-terminated. This result
    /// declares no <c>get_key</c>; the key is
    /// <c>(get_topic(i), get_partition(i), get_broker_id(i))</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterReplicaLogDirsResultGetTopic(IntPtr result, int index);

    /// <inheritdoc cref="AlterReplicaLogDirsResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterReplicaLogDirsResultGetPartition(IntPtr result, int index);

    /// <inheritdoc cref="AlterReplicaLogDirsResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_get_broker_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterReplicaLogDirsResultGetBrokerId(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_AlterReplicaLogDirsResult_get_error</c> — that replica's error, or
    /// null if the move was accepted. <b>BORROWED</b> — read, never destroy.
    /// </summary>
    /// <remarks>
    /// ⚠ There is deliberately no <c>_get_value</c>, and the header says why: "Java's
    /// per-replica future is <c>KafkaFuture&lt;Void&gt;</c>, so a null error <em>is</em> the
    /// success value" — result shape 2.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterReplicaLogDirsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterReplicaLogDirsResultDestroy(IntPtr result);

    // ---- M15/P3 Stage 3: describeReplicaLogDirs (result shape 1, 3-part key) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_replica_log_dirs_async</c> — Java's
    /// <c>describeReplicaLogDirs(Collection&lt;TopicPartitionReplica&gt;, options)</c>.
    /// Java's collection becomes <b>three parallel arrays</b>.
    /// </summary>
    /// <remarks>
    /// ⚠ "An entry with a NULL topic is skipped" — silently — so it is rejected at the C#
    /// boundary instead (ffi §B5).
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_describe_replica_log_dirs_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientDescribeReplicaLogDirsAsync(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int[] brokerIds,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeReplicaLogDirsCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeReplicaLogDirsResultCount(IntPtr result);

    /// <inheritdoc cref="AlterReplicaLogDirsResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeReplicaLogDirsResultGetTopic(IntPtr result, int index);

    /// <inheritdoc cref="AlterReplicaLogDirsResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeReplicaLogDirsResultGetPartition(IntPtr result, int index);

    /// <inheritdoc cref="AlterReplicaLogDirsResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_get_broker_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int DescribeReplicaLogDirsResultGetBrokerId(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeReplicaLogDirsResult_get_value</c> — that replica's log-dir
    /// info, <b>borrowed</b>, or null if the replica failed.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeReplicaLogDirsResultGetValue(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_DescribeReplicaLogDirsResult_get_error</c> — that replica's error, or
    /// null if it was described successfully. <b>BORROWED</b> — read, never destroy.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeReplicaLogDirsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeReplicaLogDirsResultDestroy(IntPtr result);

    // ---- M15/P4 Stage 1: electLeaders (result shape 3 — an AGGREGATE over the map) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_elect_leaders_async</c> — Java's
    /// <c>electLeaders(ElectionType, Set&lt;TopicPartition&gt;, ElectLeadersOptions)</c>.
    /// Java's set becomes <b>two parallel arrays</b>: entry <c>i</c> is
    /// <c>(topics[i], partitions[i])</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <paramref name="allPartitions"/> is the explicit discriminant for Java's
    /// <b>null</b> set — "conduct an election for every partition in the cluster"
    /// (<c>Admin.java:1099-1100</c>). The header says the other three arguments are then
    /// "ignored", and that the flag exists "so 'all partitions' and 'an empty selection'
    /// stay distinguishable". A binding that mapped an empty collection onto
    /// <see langword="true"/> would turn a request for nothing into a cluster-wide
    /// election.
    /// </para>
    /// <para>
    /// ⚠ "An entry with a NULL topic is skipped" — silently — so a null topic is rejected
    /// at the C# boundary before any pin (ffi §B5).
    /// </para>
    /// <para>
    /// ⚠ <paramref name="electionType"/> carries Java's <c>ElectionType.value</c> byte as
    /// an <c>int32_t</c>; the header rejects anything that is neither 0 nor 1, and that
    /// rejection fires the completion callback <b>inline on the submitting thread</b>.
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_elect_leaders_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientElectLeadersAsync(
        IntPtr admin,
        int electionType,
        [MarshalAs(UnmanagedType.I1)] bool allPartitions,
        IntPtr[] topics,
        int[] partitions,
        int count,
        int timeoutMs,
        AdminCallbacks.ElectLeadersCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ElectLeadersResultCount(IntPtr result);

    /// <summary>
    /// <c>kafka_admin_ElectLeadersResult_get_topic</c> — one <b>half</b> of the composite
    /// key at <paramref name="index"/>, borrowed and NUL-terminated. This result declares
    /// no <c>get_key</c>; the key is <c>(get_topic(i), get_partition(i))</c>.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ElectLeadersResultGetTopic(IntPtr result, int index);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ElectLeadersResultGetPartition(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_ElectLeadersResult_get_error</c> — that partition's election
    /// outcome, or null if its election succeeded. <b>BORROWED</b> — read, never destroy.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>Despite the name, this is the map's VALUE for this RPC, not an error
    /// channel.</b> Java's future resolves to
    /// <c>Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;</c> and its javadoc says
    /// "If the election succeeded then the value for a topic partition will be the empty
    /// Optional. Otherwise the election failed and the Optional will be set with the
    /// error" (<c>ElectLeadersResult.java:43-46</c>). So a non-null pointer here becomes a
    /// <see cref="KafkaException"/> stored <em>in the map</em>, and does not fault
    /// anything. Contrast
    /// <see cref="AlterPartitionReassignmentsResultGetError"/>, whose accessor set is
    /// byte-identical and whose semantics are the ordinary per-key failure.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ElectLeadersResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ElectLeadersResultDestroy(IntPtr result);

    // ---- M15/P4 Stage 1: alterPartitionReassignments (result shape 2, composite key) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_alter_partition_reassignments_async</c> — Java's
    /// <c>alterPartitionReassignments(Map&lt;TopicPartition, Optional&lt;NewPartitionReassignment&gt;&gt;,
    /// options)</c>. Java's map becomes <b>five parallel arrays</b>: entry <c>i</c> is
    /// <c>(topics[i], partitions[i])</c> with <c>cancel[i]</c> and, when that is
    /// <see langword="false"/>, <c>targetReplicas[i]</c> pointing at
    /// <c>targetReplicaCounts[i]</c> broker ids.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <paramref name="cancel"/> is the explicit discriminant for Java's
    /// <c>Optional.empty()</c>, which <b>reverts</b> that partition's reassignment
    /// (<c>Admin.java:1142-1143</c>). The header states the design intent verbatim: "A
    /// separate flag rather than a NULL replica pointer, so cancelling stays distinct from
    /// 'present but empty', which Java rejects." Nothing on this path may coalesce the two
    /// — a <c>?? Array.Empty&lt;int&gt;()</c> would turn a cancellation into a
    /// present-but-empty request, which the ABI rejects outright.
    /// </para>
    /// <para>
    /// ⚠ <b>The <c>bool</c> ARRAY needs its element type spelled out.</b> The class-wide
    /// <c>[MarshalAs(UnmanagedType.I1)]</c> convention applies to scalar <c>bool</c>s; for
    /// an array the element size is carried by <c>ArraySubType</c>, and without it the
    /// marshaller writes 4-byte Win32 <c>BOOL</c>s into a buffer the core reads as C
    /// <c>bool</c>. Every flag after the first would then be read out of the wrong byte.
    /// </para>
    /// <para>
    /// ⚠ "An entry with a NULL topic is skipped" — silently — so a null topic is rejected
    /// at the C# boundary (ffi §B5). A non-cancelled entry with no target replicas is
    /// rejected by the ABI itself, and that rejection fires the completion callback
    /// <b>inline on the submitting thread</b>.
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_alter_partition_reassignments_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientAlterPartitionReassignmentsAsync(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1)] bool[] cancel,
        IntPtr[] targetReplicas,
        int[] targetReplicaCounts,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool allowReplicationFactorChange,
        AdminCallbacks.AlterPartitionReassignmentsCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterPartitionReassignmentsResultCount(IntPtr result);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterPartitionReassignmentsResultGetTopic(IntPtr result, int index);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AlterPartitionReassignmentsResultGetPartition(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_AlterPartitionReassignmentsResult_get_error</c> — that partition's
    /// error, or null if its reassignment was initiated. <b>BORROWED</b> — read, never
    /// destroy.
    /// </summary>
    /// <remarks>
    /// ⚠ There is deliberately no <c>_get_value</c>: Java's per-partition future is
    /// <c>KafkaFuture&lt;Void&gt;</c> (<c>AlterPartitionReassignmentsResult.java:29</c>),
    /// so a null error <em>is</em> the success value — result shape 2, and a non-null one
    /// faults that partition's own awaitable. ⚠⚠ Contrast
    /// <see cref="ElectLeadersResultGetError"/>: the two accessor sets are byte-identical
    /// and only the Java return type separates their meanings.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterPartitionReassignmentsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterPartitionReassignmentsResultDestroy(IntPtr result);

    // ---- M15/P4 Stage 2: listPartitionReassignments (result shape 3 — NO per-key error) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_list_partition_reassignments_async</c> — Java's
    /// <c>listPartitionReassignments(Optional&lt;Set&lt;TopicPartition&gt;&gt;, options)</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <paramref name="allPartitions"/> is the explicit discriminant for Java's
    /// <c>Optional.empty()</c> — "list every ongoing reassignment in the cluster"
    /// (<c>Admin.java:1246-1247</c>). The header says the other three arguments are then
    /// ignored, and that the flag exists so "'all partitions' and 'an empty selection' stay
    /// distinguishable". Same discipline as <c>elect_leaders</c>' flag.
    /// </para>
    /// <para>
    /// ⚠ "An entry with a NULL topic is skipped" — silently — so a null topic is rejected
    /// at the C# boundary before any pin (ffi §B5).
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_list_partition_reassignments_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientListPartitionReassignmentsAsync(
        IntPtr admin,
        [MarshalAs(UnmanagedType.I1)] bool allPartitions,
        IntPtr[] topics,
        int[] partitions,
        int count,
        int timeoutMs,
        AdminCallbacks.ListPartitionReassignmentsCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListPartitionReassignmentsResultCount(IntPtr result);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListPartitionReassignmentsResultGetTopic(IntPtr result, int index);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListPartitionReassignmentsResultGetPartition(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_ListPartitionReassignmentsResult_get_value</c> — that partition's
    /// reassignment, <b>borrowed</b> (valid until the root is destroyed).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This result declares no <c>get_error</c> at all</b> — Java holds a single
    /// future, so any failure is a call failure and arrives as the callback's own owned
    /// <c>error</c>. That absence is why this RPC routes through the aggregate walker.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListPartitionReassignmentsResultGetValue(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListPartitionReassignmentsResultDestroy(IntPtr result);

    // ---- kafka_admin_PartitionReassignment_t — six FLATTENED list accessors ----

    /// <summary>
    /// <c>kafka_admin_PartitionReassignment_replica_count</c> — Java's
    /// <c>replicas().size()</c>. The three lists are read through count/element pairs with
    /// <b>no child handle per list</b>, the same flattened pattern as
    /// <see cref="LogDirDescriptionReplicaCount"/>'s family.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionReassignmentReplicaCount(IntPtr reassignment);

    /// <summary>
    /// <c>kafka_admin_PartitionReassignment_replica</c> — the broker id at
    /// <paramref name="index"/>, or <c>-1</c> if out of range.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionReassignmentReplica(IntPtr reassignment, int index);

    /// <inheritdoc cref="PartitionReassignmentReplicaCount"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_adding_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionReassignmentAddingReplicaCount(IntPtr reassignment);

    /// <inheritdoc cref="PartitionReassignmentReplica"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_adding_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionReassignmentAddingReplica(IntPtr reassignment, int index);

    /// <inheritdoc cref="PartitionReassignmentReplicaCount"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_removing_replica_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionReassignmentRemovingReplicaCount(IntPtr reassignment);

    /// <inheritdoc cref="PartitionReassignmentReplica"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_removing_replica", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionReassignmentRemovingReplica(IntPtr reassignment, int index);

    // ---- M15/P4 Stage 2: listOffsets (result shape 1 — per-key value AND per-key error) ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_list_offsets_async</c> — Java's
    /// <c>listOffsets(Map&lt;TopicPartition, OffsetSpec&gt;, ListOffsetsOptions)</c>. Java's
    /// map becomes <b>four parallel arrays</b>: entry <c>i</c> is
    /// <c>(topics[i], partitions[i])</c> with the spec in
    /// <c>isTimestamp[i]</c> + <c>specTimestamps[i]</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b><paramref name="isTimestamp"/> is load-bearing and the header says why: the
    /// projection is NOT injective.</b> "<c>forTimestamp(-2)</c> and <c>earliest()</c> both
    /// yield <c>-2</c>, yet Java treats them differently up to that point." When the flag is
    /// true the value is a timestamp <em>for any value at all</em>; when false it selects
    /// one of six no-argument factories through the <c>ListOffsets</c> wire sentinel —
    /// <c>-1</c> latest, <c>-2</c> earliest, <c>-3</c> max-timestamp, <c>-4</c>
    /// earliest-local, <c>-5</c> latest-tiered, <c>-6</c> earliest-pending-upload. Dropping
    /// the flag is a silent wrong-answer defect, not a style choice.
    /// </para>
    /// <para>
    /// ⚠ <b>The <c>bool</c> ARRAY needs its element type spelled out</b>, for the same
    /// reason as <c>alter_partition_reassignments</c>' <c>cancel</c>: without
    /// <c>ArraySubType</c> the marshaller writes four-byte Win32 <c>BOOL</c>s into a buffer
    /// the core reads as one-byte C <c>bool</c>s, and array elements really are packed, so
    /// every flag after the first is read out of the wrong byte.
    /// </para>
    /// <para>
    /// ⚠ <paramref name="isolationLevel"/> carries Java's <c>IsolationLevel.id()</c>
    /// (<c>0</c> = <c>READ_UNCOMMITTED</c>, <c>1</c> = <c>READ_COMMITTED</c>). An unknown
    /// id, and an unrecognised sentinel with <c>isTimestamp</c> false, are both <b>rejected
    /// by the ABI</b> — and that rejection fires the completion callback <b>inline on the
    /// submitting thread</b>.
    /// </para>
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_list_offsets_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientListOffsetsAsync(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1)] bool[] isTimestamp,
        long[] specTimestamps,
        int count,
        int timeoutMs,
        int isolationLevel,
        AdminCallbacks.ListOffsetsCallback callback,
        IntPtr userData);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_count", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListOffsetsResultCount(IntPtr result);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_get_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListOffsetsResultGetTopic(IntPtr result, int index);

    /// <inheritdoc cref="ElectLeadersResultGetTopic"/>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_get_partition", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListOffsetsResultGetPartition(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_ListOffsetsResult_get_value</c> — that partition's offset
    /// information, <b>borrowed</b>, or null if that partition failed.
    /// </summary>
    /// <remarks>
    /// The walker reads the per-key error first, so a null reaching the value reader is the
    /// unreachable case.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_get_value", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListOffsetsResultGetValue(IntPtr result, int index);

    /// <summary>
    /// <c>kafka_admin_ListOffsetsResult_get_error</c> — that partition's error, or null if
    /// it succeeded. <b>BORROWED</b> — read, never destroy.
    /// </summary>
    /// <remarks>
    /// ⚠ Unlike <see cref="ElectLeadersResultGetError"/>, this one really is a per-key
    /// <em>failure</em>: Java stores one future per partition
    /// (<c>ListOffsetsResult.java:32</c>), so a non-null error here faults that partition's
    /// own awaitable.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_get_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListOffsetsResultGetError(IntPtr result, int index);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListOffsetsResultDestroy(IntPtr result);

    // ---- kafka_admin_ListOffsetsResultInfo_t ----

    /// <summary><c>kafka_admin_ListOffsetsResultInfo_offset</c> — Java's <c>offset()</c>.</summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResultInfo_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ListOffsetsResultInfoOffset(IntPtr info);

    /// <summary>
    /// <c>kafka_admin_ListOffsetsResultInfo_timestamp</c> — Java's <c>timestamp()</c>.
    /// <c>-1</c> means the broker reported none, which is what every non-<c>forTimestamp</c>
    /// query returns.
    /// </summary>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResultInfo_timestamp", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ListOffsetsResultInfoTimestamp(IntPtr info);

    /// <summary>
    /// <c>kafka_admin_ListOffsetsResultInfo_leader_epoch</c> — writes the epoch and returns
    /// <see langword="true"/>, or returns <see langword="false"/> when Java's
    /// <c>leaderEpoch()</c> is <c>Optional.empty()</c>.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>The RETURN is the presence signal — there is no sentinel.</b> A negative epoch
    /// written through <paramref name="outEpoch"/> is a <em>present</em> value, so reading
    /// absence as "negative" would be a wrong answer. Contrast
    /// <see cref="LogDirDescriptionTotalBytes"/>, where <c>-1</c> genuinely is the
    /// documented sentinel — the header is what tells the two apart.
    /// </remarks>
    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResultInfo_leader_epoch", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool ListOffsetsResultInfoLeaderEpoch(IntPtr info, out int outEpoch);
}
