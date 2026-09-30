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
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The managed side of the admin C ABI's completion callbacks — kept-alive, classic
/// <c>Cdecl</c> delegates, the only portable mechanism on the netstandard2.0 floor
/// (ffi §0.1). Admin is ffi §B6's <b>hookless one-shot per-operation</b> family: no
/// admin entry point takes a <c>user_data_destroy</c>, so the callback is the
/// <b>sole owner</b> of the per-operation <c>GCHandle</c> free.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>An admin callback can fire on one of three threads, and one of them is
/// yours.</b> Normally it runs on the client's dispatcher thread. It runs
/// <b>synchronously on the submitting thread, before the entry point returns</b>, when
/// the RPC cannot be submitted at all. And it runs on a tokio worker thread if the
/// dispatcher died. The header therefore disclaims serialisation outright: "callbacks
/// are not guaranteed to be serialised on one thread. Do not hold a lock across this
/// call and re-acquire it in the callback, and publish everything the callback needs
/// (including <c>user_data</c>) before calling rather than after."
/// </para>
/// <para>
/// <b>How wide the inline path is, cited precisely.</b> For the entry points this class
/// serves, the header documents exactly one trigger: <em>a NULL <c>admin</c>
/// handle</em>. The family-wide trigger set is larger and does include
/// argument-marshaling failure on ordinary bad input — an unparseable base64 topic id,
/// an unknown <c>AlterConfigOp.OpType</c> code — but that statement lives in
/// <c>src/ffi/admin.rs:56-63</c>, a <c>//!</c> module doc <b>cbindgen does not emit</b>,
/// and its triggers belong to entry points later phases will declare. This class is the
/// family-wide one, so it is written for the wider set deliberately: every consequence
/// below is a no-cost invariant, and being ready for an inline callback that cannot
/// happen yet costs nothing.
/// </para>
/// <para>
/// Three consequences are enforced here and at the submit site: the delegates are
/// <c>static readonly</c> so the GC cannot collect a thunk native still holds; each body
/// is a <b>total no-throw boundary</b>, because an exception unwinding into Rust is
/// undefined behaviour and — on the inline path — there is no caller frame willing to
/// catch it; and every source is built with <c>RunContinuationsAsynchronously</c>,
/// without which an awaiter's continuation would run inside the caller's own P/Invoke.
/// </para>
/// <para>
/// <b>Ownership of what the callback is handed.</b> The <c>error</c>
/// <em>parameter</em> is a non-const, <b>owned</b> handle and is freed via
/// <see cref="KafkaException.FromHandle(IntPtr)"/>; the <c>result</c> is an owned
/// borrow-root destroyed exactly once in the <c>finally</c>; and every value read out of
/// that result — including a <b>per-key error</b> — is <b>borrowed</b> and must never be
/// destroyed (see <see cref="KeyedResultMarshal"/>).
/// </para>
/// <para>
/// <b>Readers, not accessors, carry the key and the value (M15/P2a, M15/P2b).</b> Each
/// RPC's key and value are read by a hoisted <c>Func&lt;IntPtr, int, T&gt;</c> over the
/// result handle and the index, because neither axis is universal: <c>deleteRecords</c>
/// has no <c>get_key</c> (its key is composed from two accessors) and no pointer-shaped
/// value (its value is an inline <c>int64_t</c>). Hoisting them into
/// <c>static readonly</c> fields is what keeps a walk allocation-free.
/// </para>
/// </remarks>
internal static class AdminCallbacks
{
    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_close_callback_t</c>:
    /// <c>void (*)(kafka_common_KafkaError_t* error, void* user_data)</c>. Null
    /// <paramref name="error"/> is success.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CloseCallback(IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_topics_callback_t</c>:
    /// <c>void (*)(const char* key, kafka_admin_TopicMetadataAndConfig_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per topic</b> as that topic's own future resolves, possibly
    /// concurrently and in any order.
    /// </summary>
    /// <remarks>
    /// ⚠ There is no result root. <paramref name="key"/> is borrowed for the call only;
    /// exactly one of <paramref name="value"/> / <paramref name="error"/> is non-null and
    /// both are <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreateTopicsCallback(IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_topics_callback_t</c>:
    /// <c>void (*)(const char* key, kafka_common_KafkaError_t* error, void* user_data)</c>
    /// — result shape <b>4b</b>, fired <b>once per topic</b>; a null
    /// <paramref name="error"/> <em>is</em> the success value. Shared by <b>both</b> delete
    /// entry points, the by-name and the by-id one.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="key"/> is borrowed for the call only (the topic name, or the
    /// base64 topic id); <paramref name="error"/> is <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteTopicsCallback(IntPtr key, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_topics_callback_t</c>:
    /// <c>void (*)(const char* key, kafka_admin_TopicDescription_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per topic</b>. Shared by both describe entry points for the same
    /// reason as <see cref="DeleteTopicsCallback"/>; by id the key is the base64 topic id.
    /// </summary>
    /// <remarks>
    /// ⚠ No result root. <paramref name="key"/> is borrowed for the call only; exactly one
    /// of <paramref name="value"/> / <paramref name="error"/> is non-null and both are
    /// <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeTopicsCallback(IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_list_topics_callback_t</c>.
    /// ⚠ Unlike every other admin callback here, <paramref name="error"/> is the
    /// <b>only</b> failure channel: the result type has no <c>get_error</c>, so there is
    /// no per-topic failure to carry (result shape 3).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_partitions_callback_t</c>:
    /// <c>void (*)(const char* key, kafka_common_KafkaError_t* error, void* user_data)</c>
    /// — result shape <b>4b</b>, fired <b>once per topic</b>; a null
    /// <paramref name="error"/> <em>is</em> the success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="key"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreatePartitionsCallback(IntPtr key, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_records_callback_t</c>:
    /// <c>void (*)(const char* topic, int32_t partition, kafka_admin_DeletedRecords_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per distinct partition</b>, with the key decomposed into two scalars.
    /// </summary>
    /// <remarks>
    /// ⚠ No result root. <paramref name="topic"/> is borrowed for the call only;
    /// <paramref name="value"/> and <paramref name="error"/> are both <b>owned</b>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteRecordsCallback(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_cluster_callback_t</c>
    /// (result shape 5). ⚠ <paramref name="error"/> is the <b>only</b> failure channel:
    /// Java's result holds four attribute futures rather than a per-key map, and the
    /// header says so — "unlike the batch RPCs there are no per-key errors: any failure is
    /// returned".
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeClusterCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_list_config_resources_callback_t</c> (result sub-shape
    /// 3b). ⚠ <paramref name="error"/> is the <b>only</b> failure channel: the result type
    /// has no <c>get_error</c>, and no key either.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListConfigResourcesCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_list_client_metrics_resources_callback_t</c> (result
    /// sub-shape 3b). ⚠ <paramref name="error"/> is the <b>only</b> failure channel: the
    /// result type has no <c>get_error</c>, and no key either.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListClientMetricsResourcesCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_configs_callback_t</c>:
    /// <c>void (*)(int32_t resource_type, const char* resource_name,
    /// kafka_admin_Config_t* value, kafka_common_KafkaError_t* error, void* user_data)</c>
    /// — result shape <b>4a</b>, fired <b>once per distinct resource</b>. The only CP3 key
    /// that is composite: the <see cref="ConfigResource"/> Java keys the map by is
    /// reassembled from the two leading arguments.
    /// </summary>
    /// <remarks>
    /// ⚠ No result root. <paramref name="resourceName"/> is borrowed for the call only;
    /// exactly one of <paramref name="value"/> / <paramref name="error"/> is non-null and
    /// both are <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeConfigsCallback(
        int resourceType, IntPtr resourceName, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_incremental_alter_configs_callback_t</c>:
    /// <c>void (*)(int32_t resource_type, const char* resource_name,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4b</b>,
    /// fired <b>once per distinct resource named across the input rows</b> (not once per
    /// row); a null <paramref name="error"/> <em>is</em> the success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="resourceName"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback. An <b>unknown op-type
    /// code</b> still fans out over every named resource, on the inline path.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void IncrementalAlterConfigsCallback(
        int resourceType, IntPtr resourceName, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_log_dirs_callback_t</c>:
    /// <c>void (*)(int32_t broker, kafka_admin_LogDirDescriptionMap_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per distinct broker</b>, with a bare scalar key.
    /// </summary>
    /// <remarks>
    /// ⚠ No result root; <paramref name="value"/> and <paramref name="error"/> are both
    /// <b>owned</b> by this callback. A <b>per-log-directory</b> error still arrives inside
    /// <paramref name="value"/> and does not fault anything.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeLogDirsCallback(int broker, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_alter_replica_log_dirs_callback_t</c>:
    /// <c>void (*)(const char* topic, int32_t partition, int32_t broker_id,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4b</b>,
    /// fired <b>once per replica</b>; a null <paramref name="error"/> <em>is</em> the
    /// success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="topic"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterReplicaLogDirsCallback(
        IntPtr topic, int partition, int brokerId, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_describe_replica_log_dirs_callback_t</c>:
    /// <c>void (*)(const char* topic, int32_t partition, int32_t broker_id,
    /// kafka_admin_ReplicaLogDirInfo_t* value, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — result shape <b>4a</b>, fired <b>once per distinct replica</b>,
    /// with the <c>TopicPartitionReplica</c> key decomposed into three scalars.
    /// </summary>
    /// <remarks>
    /// ⚠ No result root. <paramref name="topic"/> is borrowed for the call only;
    /// <paramref name="value"/> and <paramref name="error"/> are both <b>owned</b>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeReplicaLogDirsCallback(
        IntPtr topic, int partition, int brokerId, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_elect_leaders_callback_t</c>
    /// (result shape 3 — one aggregate future over the map).
    /// ⚠⚠ <paramref name="error"/> is the <b>only</b> failure channel here, even though
    /// <c>kafka_admin_ElectLeadersResult_t</c> does declare a <c>get_error</c>: that
    /// accessor carries the map's per-partition <em>value</em>, not a failure — see
    /// <see cref="ElectLeadersOptionalError"/>. A non-null <paramref name="error"/> means
    /// the election could not be run at all, and is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ElectLeadersCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_alter_consumer_group_offsets_callback_t</c>: Java's
    /// <b>single</b> <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c>, so it fires
    /// <b>exactly once</b> per submission — also for an empty request.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ Exactly one of <paramref name="result"/> / <paramref name="error"/> is non-null and
    /// the callback <b>owns</b> it. A per-partition failure arrives <b>inside</b>
    /// <paramref name="result"/> (read through <see cref="AlterConsumerGroupOffsetsOptionalError"/>,
    /// a <em>value</em>, not a fault); a non-null <paramref name="error"/> means the whole
    /// request failed or could not be submitted at all.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterConsumerGroupOffsetsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_delete_consumer_group_offsets_callback_t</c> — the same
    /// one-shot <c>(result, error, user_data)</c> contract as
    /// <see cref="AlterConsumerGroupOffsetsCallback"/>, over
    /// <c>kafka_admin_DeleteConsumerGroupOffsetsResult_t</c>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteConsumerGroupOffsetsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_alter_partition_reassignments_callback_t</c>:
    /// <c>void (*)(const char* topic, int32_t partition,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4b</b>,
    /// fired <b>once per partition</b>; a null <paramref name="error"/> <em>is</em> the
    /// success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="topic"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback. A <b>non-cancelled entry
    /// with no target replicas</b> fans out over every key, on the inline path.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterPartitionReassignmentsCallback(
        IntPtr topic, int partition, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_list_partition_reassignments_callback_t</c> (result
    /// shape 3). ⚠ <paramref name="error"/> is the <b>only</b> failure channel: Java holds
    /// a single future, and the result type declares no <c>get_error</c> at all. It is
    /// <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListPartitionReassignmentsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_list_offsets_callback_t</c>:
    /// <c>void (*)(const char* topic, int32_t partition,
    /// kafka_admin_ListOffsetsResultInfo_t* value, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — result shape <b>4a</b>, fired <b>once per distinct
    /// partition</b>, with the key decomposed into two scalars.
    /// </summary>
    /// <remarks>
    /// ⚠ No result root. <paramref name="topic"/> is borrowed for the call only;
    /// <paramref name="value"/> and <paramref name="error"/> are both <b>owned</b>. An
    /// unknown isolation level or an unrecognised offset sentinel still fails the whole
    /// call, delivered inline as that same owned per-key error for every key.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListOffsetsCallback(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_list_groups_callback_t</c> (result
    /// shape 1). ⚠ A <b>per-broker</b> listing failure arrives inside
    /// <paramref name="result"/> (<c>kafka_admin_ListGroupsResult_get_error</c>), borrowed;
    /// a non-null <paramref name="error"/> means the request could not be submitted at all
    /// and is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListGroupsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_list_consumer_groups_callback_t</c>
    /// (result shape 1). ⚠ A <b>per-broker</b> listing failure arrives inside
    /// <paramref name="result"/> (<c>kafka_admin_ListConsumerGroupsResult_get_error</c>),
    /// borrowed; a non-null <paramref name="error"/> means the request could not be
    /// submitted at all and is <b>owned</b>.
    /// </summary>
    /// <remarks>
    /// A separate delegate type from <see cref="ListGroupsCallback"/> although the three
    /// parameters are identical: each is the managed spelling of one C typedef, and the two
    /// result roots they carry are different types that must be destroyed by different
    /// functions. Sharing one delegate would let a <c>listGroups</c> root reach
    /// <c>ListConsumerGroupsResultDestroy</c>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListConsumerGroupsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_describe_consumer_groups_callback_t</c>:
    /// <c>void (*)(const char* group_id, kafka_admin_ConsumerGroupDescription_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per group</b>.
    /// </summary>
    /// <remarks>
    /// Its own delegate type, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>: each is the managed spelling of one C
    /// typedef, and the values they carry are different native types destroyed by
    /// different functions. Sharing one would let a <c>describeConsumerGroups</c> value
    /// reach another RPC's destroy.
    /// ⚠ No result root. <paramref name="key"/> is borrowed for the call only;
    /// <paramref name="value"/> and <paramref name="error"/> are both <b>owned</b>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeConsumerGroupsCallback(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_describe_classic_groups_callback_t</c>:
    /// <c>void (*)(const char* group_id, kafka_admin_ClassicGroupDescription_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per group</b>.
    /// </summary>
    /// <remarks>
    /// Its own delegate type, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>. Sharing the structurally identical
    /// <see cref="DescribeConsumerGroupsCallback"/> would let a
    /// <c>ClassicGroupDescription_t</c> reach
    /// <c>kafka_admin_ConsumerGroupDescription_destroy</c>.
    /// ⚠ No result root. <paramref name="key"/> is borrowed for the call only;
    /// <paramref name="value"/> and <paramref name="error"/> are both <b>owned</b>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeClassicGroupsCallback(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_list_consumer_group_offsets_callback_t</c>:
    /// <c>void (*)(const char* group_id, kafka_admin_OffsetAndMetadataMap_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per group</b>.
    /// </summary>
    /// <remarks>
    /// Its own delegate type, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>: sharing a structurally identical sibling
    /// would let an <c>OffsetAndMetadataMap_t</c> reach some other RPC's destroy.
    /// ⚠ No result root. <paramref name="key"/> is borrowed for the call only;
    /// <paramref name="value"/> and <paramref name="error"/> are both <b>owned</b>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListConsumerGroupOffsetsCallback(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_consumer_groups_callback_t</c>
    /// — result shape <b>4b</b>, byte-identical to <see cref="DeleteTopicsCallback"/>,
    /// fired <b>once per group</b>; a null <paramref name="error"/> <em>is</em> the success
    /// value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="key"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteConsumerGroupsCallback(IntPtr key, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_remove_members_from_consumer_group_callback_t</c>: Java's
    /// <b>single</b> <c>KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt;</c>, so it fires
    /// <b>exactly once</b> per submission — in <c>removeAll</c> mode too.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ Exactly one of <paramref name="result"/> / <paramref name="error"/> is non-null and
    /// the callback <b>owns</b> it. A per-member failure arrives <b>inside</b>
    /// <paramref name="result"/> (read through
    /// <see cref="RemoveMembersFromConsumerGroupOptionalError"/>, a <em>value</em>, not a
    /// fault); a non-null <paramref name="error"/> means the whole request failed or could
    /// not be submitted at all. In <c>removeAll</c> mode the result's member list is empty and
    /// its <c>all()</c> is the only carrier of a member failure.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void RemoveMembersFromConsumerGroupCallback(
        IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_acls_callback_t</c>:
    /// <c>void (*)(kafka_common_AclBinding_t* binding, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — result shape <b>4b</b>, fired <b>exactly <c>count</c>
    /// times</b>; a null <paramref name="error"/> <em>is</em> the success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Both</b> <paramref name="binding"/> and <paramref name="error"/> are
    /// <b>owned</b> by this callback — one of the three RPCs whose key must be destroyed.
    /// A binding the core rejects locally still arrives here with an
    /// <c>INVALID_REQUEST</c> error, on the inline path.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreateAclsCallback(IntPtr binding, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_acls_callback_t</c>.
    /// ⚠ No result root: <paramref name="filter"/>, <paramref name="value"/> and
    /// <paramref name="error"/> are <b>all three owned</b> by this callback — the key
    /// included, which no other per-key RPC has.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteAclsCallback(
        IntPtr filter, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_acls_callback_t</c>.
    /// ⚠ <c>describeAcls</c> has a <b>single</b> future for the whole call, so <em>any</em>
    /// failure arrives as <paramref name="error"/>, <b>owned</b>; the result declares no
    /// <c>get_error</c> at all (header, <c>kafka_admin_AdminClient_describe_acls_callback_t</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeAclsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_client_quotas_callback_t</c>.
    /// ⚠ <c>describeClientQuotas</c> has a <b>single</b> future for the whole call, so
    /// <em>any</em> failure arrives as <paramref name="error"/>, <b>owned</b>; the result
    /// declares no <c>get_error</c> at all (header,
    /// <c>kafka_admin_AdminClient_describe_client_quotas_callback_t</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeClientQuotasCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_alter_client_quotas_callback_t</c>:
    /// <c>void (*)(kafka_common_ClientQuotaEntity_t* entity,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4b</b>,
    /// fired <b>once per entity</b>; a null <paramref name="error"/> <em>is</em> the
    /// success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Both</b> <paramref name="entity"/> and <paramref name="error"/> are
    /// <b>owned</b> by this callback — one of the three RPCs whose key must be destroyed.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterClientQuotasCallback(IntPtr entity, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>close_async</c> submission. Rooted
    /// for the process lifetime, so the native thunk never dangles (ffi §B6 keep-alive).
    /// </summary>
    internal static readonly CloseCallback Close = OnClose;

    /// <summary>
    /// The single rooted instance passed to every <c>create_topics_async</c>
    /// submission.
    /// </summary>
    internal static readonly CreateTopicsCallback CreateTopics = OnCreateTopics;

    /// <summary>
    /// Builds a per-key reader over a <b>borrowed</b> <c>*Result_get_error(result, i)</c>,
    /// for the RPCs whose Java map value <em>is</em> an optional error.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>A factory rather than a lambda per RPC, so the body can be TESTED (M15/P4
    /// round 1, finding 70.2).</b> <c>electLeaders</c>' own reader is unreachable without a
    /// real <c>kafka_admin_ElectLeadersResult_t</c>, which no mock can produce — Java's
    /// <c>MockAdminClient.electLeaders</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c>
    /// (<c>MockAdminClient.java:797</c>) and the core mirrors that. Measured on the
    /// hand-written form: swapping <see cref="KafkaException.FromBorrowedHandle"/> for
    /// <see cref="KafkaException.FromHandle"/> inside it left the suite at
    /// <b>1231/1231 green</b>. Built here from an injected accessor, the <em>same body</em>
    /// is driven over the byte-identical
    /// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> twin, where the swap aborts
    /// the host.
    /// </para>
    /// <para>
    /// ⚠ The handle is <b>BORROWED</b> — it dies with the result root, so it goes through
    /// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/> and is never destroyed. A
    /// null pointer is <see langword="null"/>: Java's empty <c>Optional</c>, i.e. that key
    /// succeeded.
    /// </para>
    /// <para>
    /// Called once per RPC at static initialization, so the closure it allocates is not on
    /// any walk — the hoisting invariant is unchanged.
    /// </para>
    /// </remarks>
    /// <param name="getError">That RPC's <c>*Result_get_error</c>.</param>
    /// <returns>The reader.</returns>
    internal static Func<IntPtr, int, KafkaException?> BorrowedOptionalError(
        KeyedResultMarshal.IndexedAccessor getError) =>
        (result, index) => KafkaException.FromBorrowedHandle(getError(result, index));

    /// <summary>
    /// Builds a <b>composite</b> key reader over a result whose key is
    /// <c>(get_topic(i), get_partition(i))</c> — the shape M15/P2a's <c>(result, index)</c>
    /// key seam exists for.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>One body instead of a copy per RPC, for the same testability reason as
    /// <see cref="BorrowedOptionalError"/>.</b> <c>deleteRecords</c> and
    /// <c>alterPartitionReassignments</c> both drive this body against a real result root,
    /// so a defect in the composition — a missing
    /// <see cref="KeyedResultMarshal.ReadStringKey"/>, a transposed pair — is caught there
    /// and therefore for every instance built on it. What that does not reach is
    /// <em>which accessors</em> a given instance is built on; that is pinned structurally by
    /// <c>AdminP4ReaderWiringTests</c>.
    /// </remarks>
    /// <param name="getTopic">That RPC's <c>*Result_get_topic</c>, borrowed and NUL-terminated.</param>
    /// <param name="getPartition">That RPC's <c>*Result_get_partition</c>.</param>
    /// <returns>The reader.</returns>
    internal static Func<IntPtr, int, TopicPartition> TopicPartitionKey(
        KeyedResultMarshal.IndexedAccessor getTopic, Func<IntPtr, int, int> getPartition) =>
        (result, index) => new TopicPartition(
            KeyedResultMarshal.ReadStringKey(getTopic(result, index)), getPartition(result, index));

    /// <summary>
    /// Builds a single-future group RPC's <b>outcome</b> reader: the per-key map walked off
    /// the result root, paired with the core's own <c>all()</c> outcome read once off the same
    /// root — the value <see cref="Admin.AlterConsumerGroupOffsetsResult"/>,
    /// <see cref="Admin.DeleteConsumerGroupOffsetsResult"/> and
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult"/> are resolved with.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Two ownership regimes on one root.</b> Each per-key error is
    /// <b>BORROWED</b> — <paramref name="readError"/> is a <see cref="BorrowedOptionalError"/>
    /// reader, never destroyed. The <c>all()</c> error is <b>OWNED</b> — it goes through
    /// <see cref="KafkaException.FromHandle"/>, which frees it before this returns. So nothing
    /// the reader returns points into the root, and the caller may destroy the root as soon as
    /// it returns.
    /// </para>
    /// <para>
    /// The walk runs <b>before</b> <paramref name="all"/>: a throw mid-walk then never calls
    /// it, so there is no owned error to leak. The per-key map is keyed by the core's rows,
    /// which cover every requested key (the header documents <c>count</c> as the number of
    /// partitions, or members, in the request).
    /// </para>
    /// <para>
    /// Called once per RPC at static initialization, like the readers it composes, so its
    /// closure is not on any completion.
    /// </para>
    /// </remarks>
    /// <typeparam name="TKey">The map's key type.</typeparam>
    /// <param name="count">That RPC's <c>*Result_count</c>.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="readError">That RPC's borrowed per-key error reader.</param>
    /// <param name="all">That RPC's <c>*Result_all</c>, returning an <b>owned</b> error.</param>
    /// <param name="keyComparer">The key equality the map is built with.</param>
    /// <returns>The outcome reader.</returns>
    internal static Func<IntPtr, (IReadOnlyDictionary<TKey, KafkaException?> PerKey, KafkaException? All)>
        KeyedOutcomes<TKey>(
            KeyedResultMarshal.CountAccessor count,
            Func<IntPtr, int, TKey> readKey,
            Func<IntPtr, int, KafkaException?> readError,
            Func<IntPtr, IntPtr> all,
            IEqualityComparer<TKey> keyComparer)
        where TKey : notnull =>
        result =>
        {
            IReadOnlyDictionary<TKey, KafkaException?> perKey = KeyedResultMarshal.ReadAggregate(
                result, count, readKey, readError, keyComparer);

            // ⚠ OWNED — FromHandle frees it exactly once.
            return (perKey, KafkaException.FromHandle(all(result)));
        };

    /// <summary>
    /// <c>createTopics</c>' per-key value reader: <c>get_value(i)</c> yields a
    /// <b>borrowed child handle</b>, which is copied out into an owned managed object
    /// before the root dies. Hoisted for the same reason as the accessors — and shared
    /// with the tests that drive the walker directly.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicMetadataAndConfig> TopicMetadataAndConfigValue =
        static (result, index) =>
            TopicMetadataAndConfigMarshal.CopyOut(NativeMethods.CreateTopicsResultGetValue(result, index));

    /// <summary>
    /// The <b>shape-4</b> key reader: the callback's <c>key</c> parameter <em>is</em> the
    /// borrowed <c>const char*</c>, with no result root and no index to go through.
    /// Hoisted so a per-key callback allocates no delegate.
    /// </summary>
    private static readonly Func<IntPtr, string> s_stringKey = KeyedResultMarshal.ReadStringKey;

    /// <summary>
    /// <c>createTopics</c>' <b>shape-4a</b> value reader. The same copy-out as
    /// <see cref="TopicMetadataAndConfigValue"/>, over the <b>owned</b> handle the
    /// callback is handed directly instead of a borrowed child of a result root.
    /// </summary>
    internal static readonly Func<IntPtr, TopicMetadataAndConfig> TopicMetadataAndConfigPerKeyValue =
        TopicMetadataAndConfigMarshal.CopyOut;

    /// <summary>
    /// The <b>shape-4</b> by-id key reader: the callback's <c>key</c> is the same base64
    /// topic id the request supplied, parsed straight back into a <see cref="Uuid"/>.
    /// </summary>
    private static readonly Func<IntPtr, Uuid> s_uuidKey =
        static key => Uuid.Parse(KeyedResultMarshal.ReadStringKey(key));

    /// <summary>
    /// The <b>shape-4</b> scalar key reader — <c>describeLogDirs</c>' broker id arrives as
    /// an <c>int32_t</c> argument, so there is nothing to copy out.
    /// </summary>
    private static readonly Func<int, int> s_int32Key = static key => key;

    /// <summary>
    /// The <b>shape-4</b> <b>composite</b> key reader: <c>describeConfigs</c>' callback
    /// delivers the key as <c>(resource_type, resource_name)</c>, reassembled into the
    /// <see cref="ConfigResource"/> Java keys the map by. The type id goes through
    /// <see cref="ConfigResourceMarshal.TypeFromId"/> so an unknown id degrades as Java's
    /// <c>Type.forId</c> does.
    /// </summary>
    /// <remarks>
    /// The two parts travel in a <see cref="KeyValuePair{TKey, TValue}"/> so the reader can
    /// stay a hoisted static — a lambda closing over the callback's own arguments would
    /// allocate a delegate per key.
    /// </remarks>
    private static readonly Func<KeyValuePair<int, IntPtr>, ConfigResource> s_configResourceKey =
        static key => new ConfigResource(
            ConfigResourceMarshal.TypeFromId(key.Key), KeyedResultMarshal.ReadStringKey(key.Value));

    /// <summary>
    /// <c>describeTopics</c>' <b>shape-4a</b> value reader, over the <b>owned</b> handle the
    /// callback is handed directly instead of a borrowed child of a result root.
    /// </summary>
    internal static readonly Func<IntPtr, TopicDescription> TopicDescriptionPerKeyValue =
        TopicDescriptionMarshal.CopyOut;

    /// <inheritdoc cref="TopicDescriptionPerKeyValue"/>
    internal static readonly Func<IntPtr, Config> ConfigPerKeyValue = ConfigMarshal.CopyOut;

    /// <inheritdoc cref="TopicDescriptionPerKeyValue"/>
    internal static readonly Func<IntPtr, IReadOnlyDictionary<string, LogDirDescription>>
        LogDirDescriptionsPerKeyValue = LogDirMarshal.CopyOutMap;

    /// <summary>
    /// The <b>shape-4</b> key source for the partition- and replica-keyed RPCs, whose key
    /// arrives decomposed into two or three scalar arguments.
    /// </summary>
    /// <remarks>
    /// A named struct rather than nested <see cref="KeyValuePair{TKey, TValue}"/>s, for the
    /// same reason <see cref="s_configResourceKey"/> uses one pair: it keeps the reader a
    /// hoisted static instead of a per-key closure. <see cref="BrokerId"/> is unused by the
    /// two-part partition keys.
    /// </remarks>
    private readonly struct PartitionKeySource
    {
        internal PartitionKeySource(IntPtr topic, int partition, int brokerId = 0)
        {
            Topic = topic;
            Partition = partition;
            BrokerId = brokerId;
        }

        /// <summary>The borrowed topic pointer, valid for the callback only.</summary>
        internal IntPtr Topic { get; }

        internal int Partition { get; }

        internal int BrokerId { get; }
    }

    /// <summary>
    /// The <b>shape-4</b> <c>(topic, partition)</c> key reader, reassembling the
    /// <see cref="TopicPartition"/> Java keys the map by.
    /// </summary>
    private static readonly Func<PartitionKeySource, TopicPartition> s_topicPartitionKey =
        static key => new TopicPartition(KeyedResultMarshal.ReadStringKey(key.Topic), key.Partition);

    /// <summary>
    /// The <b>shape-4</b> <c>(topic, partition, broker)</c> key reader, reassembling the
    /// <see cref="TopicPartitionReplica"/> Java keys the map by.
    /// </summary>
    private static readonly Func<PartitionKeySource, TopicPartitionReplica> s_replicaKey =
        static key => new TopicPartitionReplica(
            KeyedResultMarshal.ReadStringKey(key.Topic), key.Partition, key.BrokerId);

    /// <summary>
    /// <c>deleteRecords</c>' <b>shape-4a</b> value reader: the low watermark is read off
    /// the owned per-key handle, so the <c>-1</c> overload the flattened result had
    /// ("that partition failed" / "index out of range") cannot arise — a failing key
    /// arrives with a NULL value and a non-null error instead.
    /// </summary>
    internal static readonly Func<IntPtr, DeletedRecords> DeletedRecordsPerKeyValue =
        static value => new DeletedRecords(NativeMethods.DeletedRecordsLowWatermark(value));

    /// <summary>
    /// <c>describeReplicaLogDirs</c>' <b>shape-4a</b> value reader — with the one per-key
    /// outcome no other RPC has: an <b>absent</b> replica.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Both <c>value</c> and <c>error</c> NULL means the replica is absent from the
    /// result</b> — Java's <c>values().get(replica) == null</c>, which only Java's
    /// <c>MockAdminClient</c> produces (it skips a replica of a topic it does not know).
    /// The shared per-key body calls this reader only when the error is NULL, so a NULL
    /// <c>value</c> here is exactly that case (header,
    /// <c>kafka_admin_AdminClient_describe_replica_log_dirs_callback_t</c>).
    /// </para>
    /// <para>
    /// .NET cannot drop the key the way Java's map does — <c>Values</c> is handed to the
    /// caller before any callback fires — and completing it with an empty
    /// <c>ReplicaLogDirInfo</c> would invent data. So the key faults with the code and
    /// message the core used for it before it reported the key as absent, exactly as the
    /// Python binding does (<see cref="AbsentKey"/>). The throw is caught by the per-key
    /// no-throw boundary and faults this one replica only.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>
        ReplicaLogDirInfoPerKeyValue = static value =>
            value == IntPtr.Zero ? throw AbsentKey() : LogDirMarshal.CopyOutReplicaInfo(value);

    /// <summary>
    /// The error a per-key admin result that is <b>absent</b> resolves with: code <c>-4</c>
    /// (<c>kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE</c>), not retriable, and the message
    /// the core used to synthesise for such a key.
    /// </summary>
    /// <remarks>
    /// ⚠ Built directly rather than through <c>kafka_common_Error_new</c>, which maps a
    /// non-protocol code such as <c>-4</c> to <c>UnknownServerError</c> (header,
    /// <c>kafka_common_Error_new</c>) and so cannot mint it. Python does the same
    /// (<c>_ABSENT_KEY_ERROR_CODE</c> / <c>_ABSENT_KEY_ERROR_MESSAGE</c> in <c>admin.py</c>).
    /// </remarks>
    /// <returns>A fresh exception per key.</returns>
    internal static KafkaException AbsentKey() =>
        new KafkaException(
            AbsentKeyErrorCode,
            "the requested key was not present in the admin RPC's response",
            isRetriable: false);

    /// <summary><c>kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE</c>.</summary>
    private const int AbsentKeyErrorCode = -4;

    /// <inheritdoc cref="TopicDescriptionPerKeyValue"/>
    internal static readonly Func<IntPtr, ListOffsetsResult.ListOffsetsResultInfo>
        ListOffsetsInfoPerKeyValue =
            static value => ListOffsetsResultInfoMarshal.CopyOut(value)
                ?? throw new KafkaException(
                    "The listOffsets result produced no offset information for a partition that "
                    + "reported no error.");

    /// <summary>
    /// The single rooted instance passed to <b>both</b> <c>delete_topics_async</c> and
    /// <c>delete_topics_by_ids_async</c>. One trampoline serves both because the ABI hands
    /// back the same result type; which key type the awaiters are keyed by travels in the
    /// <c>user_data</c> context, not in the delegate.
    /// </summary>
    internal static readonly DeleteTopicsCallback DeleteTopicsByName = OnDeleteTopicsByName;

    /// <summary>
    /// The by-<b>id</b> rooted instance. It is a separate delegate from
    /// <see cref="DeleteTopicsByName"/> only because the two recover a differently-typed
    /// context out of <c>user_data</c> (<c>Uuid</c> keys versus <c>string</c> keys); the
    /// ABI signature is identical.
    /// </summary>
    internal static readonly DeleteTopicsCallback DeleteTopicsById = OnDeleteTopicsById;

    /// <inheritdoc cref="DeleteTopicsByName"/>
    internal static readonly DescribeTopicsCallback DescribeTopicsByName = OnDescribeTopicsByName;

    /// <inheritdoc cref="DeleteTopicsById"/>
    internal static readonly DescribeTopicsCallback DescribeTopicsById = OnDescribeTopicsById;

    /// <summary>
    /// The single rooted instance passed to every <c>list_topics_async</c> submission.
    /// </summary>
    internal static readonly ListTopicsCallback ListTopics = OnListTopics;

    /// <summary>
    /// The single rooted instance passed to every <c>create_partitions_async</c>
    /// submission.
    /// </summary>
    internal static readonly CreatePartitionsCallback CreatePartitions = OnCreatePartitions;

    /// <summary>
    /// The single rooted instance passed to every <c>delete_records_async</c> submission.
    /// </summary>
    internal static readonly DeleteRecordsCallback DeleteRecords = OnDeleteRecords;

    /// <summary>
    /// <c>listTopics</c>' key reader (result shape 3). The map is keyed by topic name,
    /// exactly as Java's <c>Map&lt;String, TopicListing&gt;</c> is.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> ListTopicsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.ListTopicsResultGetKey(result, index));

    /// <summary>
    /// <c>listTopics</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>TopicListing_t</c>, copied out before the root dies.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicListing> TopicListingValue =
        static (result, index) =>
            TopicListingMarshal.CopyOut(NativeMethods.ListTopicsResultGetValue(result, index));

    /// <summary>
    /// <c>deleteRecords</c>' <b>composite</b> key reader — the sub-shape M15/P2a's
    /// <c>(result, index)</c> key seam exists for. This result declares no
    /// <c>get_key</c>; the key is <c>(get_topic(i), get_partition(i))</c>, reassembled
    /// into the shipped <see cref="TopicPartition"/> that Java keys the map by.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> DeleteRecordsKey =
        TopicPartitionKey(
            NativeMethods.DeleteRecordsResultGetTopic, NativeMethods.DeleteRecordsResultGetPartition);

    /// <summary>
    /// The rooted instance passed to every <c>describe_cluster_async</c> submission
    /// (result shape 5 — not a table walk, so it has no accessor set and no readers).
    /// </summary>
    internal static readonly DescribeClusterCallback DescribeCluster = OnDescribeCluster;

    /// <summary>
    /// The rooted instance passed to every <c>list_config_resources_async</c> submission
    /// (result sub-shape 3b).
    /// </summary>
    internal static readonly ListConfigResourcesCallback ListConfigResources = OnListConfigResources;

    /// <summary>
    /// The rooted instance passed to every <c>list_client_metrics_resources_async</c>
    /// submission (result sub-shape 3b).
    /// </summary>
    internal static readonly ListClientMetricsResourcesCallback ListClientMetricsResources =
        OnListClientMetricsResources;

    /// <summary>
    /// The rooted instance passed to every <c>describe_configs_async</c> submission
    /// (result shape 1, composite key).
    /// </summary>
    internal static readonly DescribeConfigsCallback DescribeConfigs = OnDescribeConfigs;

    /// <summary>
    /// The rooted instance passed to every <c>incremental_alter_configs_async</c>
    /// submission (result shape 2, composite key).
    /// </summary>
    internal static readonly IncrementalAlterConfigsCallback IncrementalAlterConfigs =
        OnIncrementalAlterConfigs;

    /// <summary>
    /// The rooted instance passed to every <c>describe_log_dirs_async</c> submission
    /// (result shape 1, scalar key).
    /// </summary>
    internal static readonly DescribeLogDirsCallback DescribeLogDirs = OnDescribeLogDirs;

    /// <summary>
    /// The rooted instance passed to every <c>alter_replica_log_dirs_async</c> submission
    /// (result shape 2, 3-part key).
    /// </summary>
    internal static readonly AlterReplicaLogDirsCallback AlterReplicaLogDirs = OnAlterReplicaLogDirs;

    /// <summary>
    /// The rooted instance passed to every <c>describe_replica_log_dirs_async</c>
    /// submission (result shape 1, 3-part key).
    /// </summary>
    internal static readonly DescribeReplicaLogDirsCallback DescribeReplicaLogDirs = OnDescribeReplicaLogDirs;

    /// <summary>
    /// <c>listConfigResources</c>' element reader — sub-shape 3b, so this is a <b>value</b>
    /// reader with no key beside it. The element is <b>composite</b>, assembled from
    /// <c>get_type(i)</c> and <c>get_name(i)</c>, which is why the walker's reader seam
    /// takes <c>(result, index)</c> rather than a single pointer.
    /// </summary>
    /// <remarks>
    /// The type id goes through <see cref="ConfigResourceMarshal.TypeFromId"/>, which owns
    /// the rule that separates the ABI's out-of-range <c>-1</c> from a non-negative id this
    /// client has no member for. The name goes through
    /// <see cref="KeyedResultMarshal.ReadStringKey"/>. Either can throw; the throw
    /// propagates to the trampoline's no-throw boundary, which faults the one awaiter this
    /// shape has.
    /// </remarks>
    internal static readonly Func<IntPtr, int, ConfigResource> ConfigResourceValue =
        static (result, index) => new ConfigResource(
            ConfigResourceMarshal.TypeFromId(NativeMethods.ListConfigResourcesResultGetType(result, index)),
            KeyedResultMarshal.ReadStringKey(NativeMethods.ListConfigResourcesResultGetName(result, index)));

#pragma warning disable CS0618 // Java deprecates the listing type itself; mirrored, not avoided.

    /// <summary>
    /// <c>listClientMetricsResources</c>' element reader — the whole listing <em>is</em>
    /// the name, so <c>get_name(i)</c> is the result's only per-index accessor.
    /// </summary>
    internal static readonly Func<IntPtr, int, ClientMetricsResourceListing> ClientMetricsResourceListingValue =
        static (result, index) => new ClientMetricsResourceListing(
            KeyedResultMarshal.ReadStringKey(
                NativeMethods.ListClientMetricsResourcesResultGetName(result, index)));

#pragma warning restore CS0618

    /// <summary>
    /// The rooted instance passed to every <c>elect_leaders_async</c> submission (result
    /// shape 3 — one aggregate future, so it has no accessor set).
    /// </summary>
    internal static readonly ElectLeadersCallback ElectLeaders = OnElectLeaders;

    /// <summary>
    /// The rooted instance passed to every <c>alter_partition_reassignments_async</c>
    /// submission (result shape 2, composite key).
    /// </summary>
    internal static readonly AlterPartitionReassignmentsCallback AlterPartitionReassignments =
        OnAlterPartitionReassignments;

    /// <summary>
    /// The rooted instance passed to every <c>alter_consumer_group_offsets_async</c>
    /// submission (one callback resolving one aggregate task, like
    /// <see cref="ElectLeaders"/>).
    /// </summary>
    internal static readonly AlterConsumerGroupOffsetsCallback AlterConsumerGroupOffsets =
        OnAlterConsumerGroupOffsets;

    /// <summary>
    /// The rooted instance passed to every <c>delete_consumer_group_offsets_async</c>
    /// submission (the same one-shot shape as <see cref="AlterConsumerGroupOffsets"/>).
    /// </summary>
    internal static readonly DeleteConsumerGroupOffsetsCallback DeleteConsumerGroupOffsets =
        OnDeleteConsumerGroupOffsets;

    /// <summary>
    /// The rooted instance passed to every <c>delete_consumer_groups_async</c> submission
    /// (result shape 2, like <see cref="DeleteTopicsByName"/>).
    /// </summary>
    internal static readonly DeleteConsumerGroupsCallback DeleteConsumerGroups = OnDeleteConsumerGroups;

    /// <summary>
    /// The rooted instance passed to every
    /// <c>remove_members_from_consumer_group_async</c> submission (the same one-shot shape as
    /// <see cref="AlterConsumerGroupOffsets"/>).
    /// </summary>
    internal static readonly RemoveMembersFromConsumerGroupCallback RemoveMembersFromConsumerGroup =
        OnRemoveMembersFromConsumerGroup;

    /// <summary>
    /// The rooted instance passed to every <c>create_acls_async</c> submission.
    /// </summary>
    internal static readonly CreateAclsCallback CreateAcls = OnCreateAcls;

    /// <summary>
    /// The rooted instance passed to every <c>delete_acls_async</c> submission.
    /// </summary>
    internal static readonly DeleteAclsCallback DeleteAcls = OnDeleteAcls;

    /// <summary>
    /// <c>deleteAcls</c>' per-key <b>key</b> reader: the OWNED
    /// <c>kafka_common_AclBindingFilter_t</c> the callback is handed, copied out before
    /// <see cref="CompletePerKeyOwnedKey{TKey, TValue}"/>'s <c>finally</c> destroys it.
    /// </summary>
    internal static readonly Func<IntPtr, AclBindingFilter> DeleteAclsPerKeyKey =
        AclRowMarshal.ReadFilter;

    /// <summary>
    /// <c>createAcls</c>' per-key <b>key</b> reader: the OWNED
    /// <c>kafka_common_AclBinding_t</c> the callback is handed, copied out before
    /// <see cref="CompletePerKeyVoidOwnedKey{TKey}"/>'s <c>finally</c> destroys it.
    /// </summary>
    internal static readonly Func<IntPtr, AclBinding> CreateAclsPerKeyKey =
        AclRowMarshal.ReadBinding;

    /// <summary>
    /// <c>alterClientQuotas</c>' per-key <b>key</b> reader: the OWNED
    /// <c>kafka_common_ClientQuotaEntity_t</c> the callback is handed, copied out before
    /// <see cref="CompletePerKeyVoidOwnedKey{TKey}"/>'s <c>finally</c> destroys it.
    /// </summary>
    internal static readonly Func<IntPtr, ClientQuotaEntity> AlterClientQuotasPerKeyKey =
        ClientQuotaMarshal.ReadEntity;

    /// <summary>
    /// <c>deleteAcls</c>' per-key value reader, over the owned
    /// <c>kafka_admin_DeleteAclsFilterResults_t</c> rather than the retired root's
    /// <c>(i, j)</c> pair.
    /// </summary>
    internal static readonly Func<IntPtr, DeleteAclsResult.FilterResults>
        DeleteAclsFilterResultsPerKeyValue =
            DeleteAclsResultMarshal.FilterResultsPerKeyReader(
                NativeMethods.DeleteAclsFilterResultsCount,
                NativeMethods.DeleteAclsFilterResultsGetBinding,
                NativeMethods.DeleteAclsFilterResultsGetError,
                AclRowMarshal.ReadBinding);

    /// <summary>
    /// The rooted instance passed to every <c>describe_acls_async</c> submission.
    /// </summary>
    internal static readonly DescribeAclsCallback DescribeAcls = OnDescribeAcls;

    /// <summary>
    /// <c>describeAcls</c>' element reader: the borrowed <c>get_binding(i)</c>, copied out
    /// into the nested managed <see cref="AclBinding"/> before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Built over <b><c>describeAcls</c>'</b> own <c>get_binding</c> — the ACL results
    /// expose byte-identical accessor sets, so a cross-wired reader returns a plausible
    /// answer rather than failing (<c>AdminP4ReaderWiringTests</c>).
    /// </remarks>
    internal static readonly Func<IntPtr, int, AclBinding> DescribeAclsValue =
        AclRowMarshal.BindingReader(NativeMethods.DescribeAclsResultGetBinding);

    /// <summary>
    /// The rooted instance passed to every <c>describe_client_quotas_async</c> submission.
    /// </summary>
    internal static readonly DescribeClientQuotasCallback DescribeClientQuotas =
        OnDescribeClientQuotas;

    /// <summary>
    /// <c>describeClientQuotas</c>' key reader: the borrowed <c>get_entity(i)</c>, copied out
    /// into the managed <see cref="ClientQuotaEntity"/> before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Built over <b><c>describeClientQuotas</c>'</b> own <c>get_entity</c> — the quota
    /// results expose byte-identical accessor sets (<c>AdminP4ReaderWiringTests</c>).
    /// </remarks>
    internal static readonly Func<IntPtr, int, ClientQuotaEntity> DescribeClientQuotasKey =
        ClientQuotaMarshal.EntityReader(NativeMethods.DescribeClientQuotasResultGetEntity);

    /// <summary>
    /// <c>describeClientQuotas</c>' value reader: the whole inner <c>(i, j)</c> quota axis,
    /// walked inside the reader so the aggregate walker needs no second index.
    /// </summary>
    internal static readonly Func<IntPtr, int, IReadOnlyDictionary<string, double>> DescribeClientQuotasValue =
        ClientQuotaMarshal.QuotaMapReader(
            NativeMethods.DescribeClientQuotasResultGetQuotaCount,
            NativeMethods.DescribeClientQuotasResultGetQuotaKey,
            NativeMethods.DescribeClientQuotasResultGetQuotaValue);

    /// <summary>
    /// The rooted instance passed to every <c>alter_client_quotas_async</c> submission.
    /// </summary>
    internal static readonly AlterClientQuotasCallback AlterClientQuotas = OnAlterClientQuotas;

    // ====================================================================================
    // M15/P7 — SCRAM credentials, delegation tokens, features.
    //
    // Four of the eight RPCs have no count and no index: their whole result is one value
    // read off the root, so they bypass KeyedResultMarshal entirely and resolve
    // SingleAdminOperation<T> directly through CompleteRootValueRpc below. No walker
    // callable is added or edited by this phase.
    // ====================================================================================

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_user_scram_credentials_callback_t</c>. ⚠ A
    /// <b>per-user</b> failure arrives inside <paramref name="result"/>, borrowed; a non-null
    /// <paramref name="error"/> means the whole call failed and is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeUserScramCredentialsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_alter_user_scram_credentials_callback_t</c>:
    /// <c>void (*)(const char* user, kafka_common_KafkaError_t* error, void* user_data)</c>
    /// — result shape <b>4b</b>, fired <b>once per distinct user</b> (several alterations
    /// may name one user); a null <paramref name="error"/> <em>is</em> the success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="user"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterUserScramCredentialsCallback(IntPtr user, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_create_delegation_token_callback_t</c>. ⚠ No per-key error
    /// channel exists, so <paramref name="error"/> is the only failure and is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreateDelegationTokenCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_renew_delegation_token_callback_t</c>. <paramref name="error"/>
    /// is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void RenewDelegationTokenCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_expire_delegation_token_callback_t</c>. <paramref name="error"/>
    /// is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ExpireDelegationTokenCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_delegation_token_callback_t</c>. <paramref name="error"/>
    /// is <b>owned</b>; the result declares no <c>get_error</c>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeDelegationTokenCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_features_callback_t</c>. <paramref name="error"/> is
    /// <b>owned</b>; the result declares no <c>get_error</c>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeFeaturesCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_update_features_callback_t</c>:
    /// <c>void (*)(const char* feature, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — result shape <b>4b</b>, fired <b>once per feature</b>; a null
    /// <paramref name="error"/> <em>is</em> the success value.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="feature"/> is borrowed for the call only;
    /// <paramref name="error"/> is <b>owned</b> by this callback. With <c>count == 0</c>
    /// the callback is <b>never</b> invoked, which the submit token covers.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void UpdateFeaturesCallback(IntPtr feature, IntPtr error, IntPtr userData);

    /// <summary>The rooted instance passed to every <c>describe_user_scram_credentials_async</c> submission.</summary>
    internal static readonly DescribeUserScramCredentialsCallback DescribeUserScramCredentials =
        OnDescribeUserScramCredentials;

    /// <summary>The rooted instance passed to every <c>alter_user_scram_credentials_async</c> submission.</summary>
    internal static readonly AlterUserScramCredentialsCallback AlterUserScramCredentials =
        OnAlterUserScramCredentials;

    /// <summary>The rooted instance passed to every <c>create_delegation_token_async</c> submission.</summary>
    internal static readonly CreateDelegationTokenCallback CreateDelegationToken = OnCreateDelegationToken;

    /// <summary>The rooted instance passed to every <c>renew_delegation_token_async</c> submission.</summary>
    internal static readonly RenewDelegationTokenCallback RenewDelegationToken = OnRenewDelegationToken;

    /// <summary>The rooted instance passed to every <c>expire_delegation_token_async</c> submission.</summary>
    internal static readonly ExpireDelegationTokenCallback ExpireDelegationToken = OnExpireDelegationToken;

    /// <summary>The rooted instance passed to every <c>describe_delegation_token_async</c> submission.</summary>
    internal static readonly DescribeDelegationTokenCallback DescribeDelegationToken =
        OnDescribeDelegationToken;

    /// <summary>The rooted instance passed to every <c>describe_features_async</c> submission.</summary>
    internal static readonly DescribeFeaturesCallback DescribeFeatures = OnDescribeFeatures;

    /// <summary>The rooted instance passed to every <c>update_features_async</c> submission.</summary>
    internal static readonly UpdateFeaturesCallback UpdateFeatures = OnUpdateFeatures;

    /// <summary>
    /// <c>describeDelegationToken</c>' element reader — sub-shape 3b, one collection, no key
    /// and no per-element error.
    /// </summary>
    internal static readonly Func<IntPtr, int, Confluent.Kafka.DelegationToken> DescribeDelegationTokenValue =
        DelegationTokenMarshal.TokenReader(NativeMethods.DescribeDelegationTokenResultGetToken);

    private static readonly Action<IntPtr> s_destroyDescribeUserScramCredentialsResult =
        NativeMethods.DescribeUserScramCredentialsResultDestroy;

    private static readonly Action<IntPtr> s_destroyCreateDelegationTokenResult =
        NativeMethods.CreateDelegationTokenResultDestroy;

    private static readonly Action<IntPtr> s_destroyRenewDelegationTokenResult =
        NativeMethods.RenewDelegationTokenResultDestroy;

    private static readonly Action<IntPtr> s_destroyExpireDelegationTokenResult =
        NativeMethods.ExpireDelegationTokenResultDestroy;

    private static readonly Action<IntPtr> s_destroyDescribeDelegationTokenResult =
        NativeMethods.DescribeDelegationTokenResultDestroy;

    private static readonly Action<IntPtr> s_destroyDescribeFeaturesResult =
        NativeMethods.DescribeFeaturesResultDestroy;

    private static readonly KeyedResultMarshal.CountAccessor s_describeDelegationTokenCount =
        NativeMethods.DescribeDelegationTokenResultCount;

    /// <summary>
    /// The completion body shared by every trampoline whose single awaiter resolves with
    /// <b>one value read off the result root</b>: <paramref name="readRoot"/> builds that value,
    /// and this shell owns everything else.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <paramref name="error"/> is <b>OWNED</b> — <see cref="KafkaException.FromHandle"/> is
    /// what frees it exactly once. Whatever <paramref name="readRoot"/> reads off the root is
    /// its own business to classify: a <b>borrowed</b> pointer must be copied out before it
    /// returns, and an <b>owned</b> one freed by it (the group-offsets outcome readers do both
    /// — see <see cref="KeyedOutcomes{TKey}"/>). Nothing it returns may point into the root.
    /// </para>
    /// <para>
    /// The <c>finally</c> discharges the usual three obligations, with the destroy strictly
    /// after the read because the read borrows from that root. The destroy is null-safe, so it
    /// also covers the <paramref name="error"/> path, where the root is null.
    /// </para>
    /// </remarks>
    private static void CompleteRootValueRpc<TValue>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        Func<IntPtr, TValue> readRoot,
        Action<IntPtr> destroyResult)
    {
        SingleAdminOperation<TValue>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<TValue>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                context.SetResult(readRoot(result));
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// <c>describeUserScramCredentials</c>' own trampoline: it copies out the <c>all()</c> and
    /// <c>users()</c> views and then <b>retains</b> the root for on-demand
    /// <c>description(user)</c>, so it no longer fits the shared flattening helper.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Two ownership inversions relative to the accessor set this replaced, neither of
    /// which has a managed symptom. <c>all_error</c> is <b>OWNED</b> —
    /// <see cref="KafkaException.FromHandle"/>, never <c>FromBorrowedHandle</c>, which would
    /// leak one error per faulting <c>All()</c>. The <c>all_get_description</c> pointers are
    /// <b>BORROWED</b> from the root — the plain
    /// <see cref="UserScramCredentialMarshal.Read(IntPtr)"/>, never the destroying twin.
    /// </para>
    /// <para>
    /// ⚠ <paramref name="result"/> is tracked by an ownership <b>baton</b> zeroed only once
    /// the <see cref="SafeDescribeUserScramCredentialsResultHandle"/> has adopted it, and the
    /// <c>finally</c> destroys whatever the baton still holds (ffi §B6). So a throw anywhere
    /// in the copy-out destroys the root exactly once, and a successful adoption never
    /// double-destroys — the destroy is null-safe, which also covers the submit-error path
    /// where the root may be null.
    /// </para>
    /// </remarks>
    /// <param name="result">The result root, owned by this callback until adopted.</param>
    /// <param name="error">The submit's own error, <b>owned</b>; null on success.</param>
    /// <param name="userData">The operation's <see cref="GCHandle"/>.</param>
    private static void OnDescribeUserScramCredentials(IntPtr result, IntPtr error, IntPtr userData)
    {
        SingleAdminOperation<DescribeUserScramCredentialsViews>? context = null;
        IntPtr rootBaton = result;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<DescribeUserScramCredentialsViews>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.SetException(KafkaException.FromHandle(error)!);
                return;
            }

            // ⚠ OWNED — FromHandle frees it exactly once.
            KafkaException? allError = KafkaException.FromHandle(
                NativeMethods.DescribeUserScramCredentialsResultAllError(result));

            Dictionary<string, UserScramCredentialsDescription> all;
            if (allError is null)
            {
                int allCount = NativeMethods.DescribeUserScramCredentialsResultAllCount(result);
                all = new Dictionary<string, UserScramCredentialsDescription>(
                    Math.Max(allCount, 0), StringComparer.Ordinal);
                for (int row = 0; row < allCount; row++)
                {
                    string user = KeyedResultMarshal.ReadStringKey(
                        NativeMethods.DescribeUserScramCredentialsResultAllGetUser(result, row));

                    // ⚠ BORROWED description — the plain read, never ReadAndDestroy.
                    all[user] = UserScramCredentialMarshal.Read(
                        NativeMethods.DescribeUserScramCredentialsResultAllGetDescription(result, row));
                }
            }
            else
            {
                // `all()` faulted, so its rows are unavailable (count is 0, getters null).
                all = new Dictionary<string, UserScramCredentialsDescription>(StringComparer.Ordinal);
            }

            int usersCount = NativeMethods.DescribeUserScramCredentialsResultUsersCount(result);
            List<string> users = new List<string>(Math.Max(usersCount, 0));
            for (int index = 0; index < usersCount; index++)
            {
                users.Add(KeyedResultMarshal.ReadStringKey(
                    NativeMethods.DescribeUserScramCredentialsResultUsersGet(result, index)));
            }

            SafeDescribeUserScramCredentialsResultHandle root =
                SafeDescribeUserScramCredentialsResultHandle.Adopt(result);
            rootBaton = IntPtr.Zero;

            context.SetResult(new DescribeUserScramCredentialsViews(allError, all, users, root));
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            s_destroyDescribeUserScramCredentialsResult(rootBaton);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    private static void OnAlterUserScramCredentials(IntPtr key, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(key, error, userData, s_stringKey);

    private static void OnCreateDelegationToken(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            root => DelegationTokenMarshal.Read(NativeMethods.CreateDelegationTokenResultGetToken(root)),
            s_destroyCreateDelegationTokenResult);

    private static void OnRenewDelegationToken(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            NativeMethods.RenewDelegationTokenResultExpiryTimestamp,
            s_destroyRenewDelegationTokenResult);

    private static void OnExpireDelegationToken(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            NativeMethods.ExpireDelegationTokenResultExpiryTimestamp,
            s_destroyExpireDelegationTokenResult);

    private static void OnDescribeDelegationToken(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteListRpc(
            result,
            error,
            userData,
            s_describeDelegationTokenCount,
            DescribeDelegationTokenValue,
            s_destroyDescribeDelegationTokenResult);

    private static void OnDescribeFeatures(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            FeatureMetadataMarshal.CopyOut,
            s_destroyDescribeFeaturesResult);

    private static void OnUpdateFeatures(IntPtr key, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(key, error, userData, s_stringKey);

    // ---- M15/P8: producers & transactions ----

    /// <summary>
    /// <c>kafka_admin_AdminClient_abort_transaction_callback_t</c> (header). ⚠ There is
    /// <b>no result handle</b> — Java's <c>AbortTransactionResult</c> carries nothing but the
    /// future's success — so the signature is <c>(error, user_data)</c> and
    /// <paramref name="error"/> is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AbortTransactionCallback(IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_force_terminate_transaction_callback_t</c> (header).
    /// Its own delegate type although the signature matches
    /// <see cref="AbortTransactionCallback"/>, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ForceTerminateTransactionCallback(IntPtr error, IntPtr userData);

    /// <summary>The rooted instance passed to every <c>abort_transaction_async</c> submission.</summary>
    internal static readonly AbortTransactionCallback AbortTransaction = OnAbortTransaction;

    /// <summary>The rooted instance passed to every <c>force_terminate_transaction_async</c> submission.</summary>
    internal static readonly ForceTerminateTransactionCallback ForceTerminateTransaction =
        OnForceTerminateTransaction;

    /// <summary>
    /// The completion body the two <b>result-handle-less</b> P8 trampolines share: there is
    /// nothing to read and nothing to destroy, so the callback only resolves or faults the
    /// single awaiter.
    /// </summary>
    /// <remarks>
    /// ⚠ This can run <b>synchronously on the submitting thread</b> — the ABI unwraps its
    /// marshalling result inside the submit closure, so a rejected input completes inline
    /// before the entry point returns (header,
    /// <c>kafka_admin_AdminClient_abort_transaction_async</c> and
    /// <c>kafka_admin_AdminClient_force_terminate_transaction_async</c>). The context is therefore
    /// already published by the submitter before the P/Invoke, and the source's
    /// <c>RunContinuationsAsynchronously</c> keeps the awaiter's continuation off that thread.
    /// </remarks>
    /// <param name="error">The operation's failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void CompleteVoidRpc(IntPtr error, IntPtr userData)
    {
        SingleAdminOperation<bool>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<bool>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                context.SetResult(true);
            }
        }
        catch (Exception exception)
        {
            context?.SetException(exception);
        }
        finally
        {
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    private static void OnAbortTransaction(IntPtr error, IntPtr userData) =>
        CompleteVoidRpc(error, userData);

    private static void OnForceTerminateTransaction(IntPtr error, IntPtr userData) =>
        CompleteVoidRpc(error, userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_fence_producers_callback_t</c>:
    /// <c>void (*)(const char* transactional_id, kafka_admin_FenceProducersResult_t* value,
    /// kafka_common_KafkaError_t* error, void* user_data)</c> — result shape <b>4a</b>,
    /// fired <b>once per distinct id</b>.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>This RPC has no standalone per-key value handle</b>, so the ABI reuses the
    /// flattened result type as the single-key value: <paramref name="value"/> is a
    /// <c>FenceProducersResult_t</c> carrying exactly this one id, readable at
    /// <b>index 0</b>, and it is <b>owned</b> — freed with
    /// <c>kafka_admin_FenceProducersResult_destroy</c>, the same function the synchronous
    /// path uses. It is <em>not</em> a result root to walk. <paramref name="key"/> is
    /// borrowed for the call only; <paramref name="error"/> is owned too.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void FenceProducersCallback(IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>The rooted instance passed to every <c>fence_producers_async</c> submission.</summary>
    internal static readonly FenceProducersCallback FenceProducers = OnFenceProducers;

    /// <summary>
    /// <c>fenceProducers</c>' value reader — the two inline scalars, read together so the pair
    /// cannot be transposed by a caller wiring them separately.
    /// </summary>
    /// <remarks>
    /// ⚠ Both accessors return <c>-1</c> for a failed id (Java's
    /// <c>ProducerIdAndEpoch.NONE</c>), which is a <b>value</b>, not a verdict — the per-id
    /// error is the authoritative signal and the walker reads it first.
    /// </remarks>
    internal static readonly Func<IntPtr, int, ProducerIdAndEpoch> FenceProducersValue =
        FenceProducersValueReader(
            NativeMethods.FenceProducersResultGetProducerId,
            NativeMethods.FenceProducersResultGetEpochId);

    private static readonly Action<IntPtr> s_destroyFenceProducersResult =
        NativeMethods.FenceProducersResultDestroy;

    /// <summary>
    /// <c>fenceProducers</c>' <b>shape-4a</b> value reader: the per-key value is a
    /// single-id <c>FenceProducersResult_t</c>, so the table reader is reused at
    /// <b>index 0</b>.
    /// </summary>
    internal static readonly Func<IntPtr, ProducerIdAndEpoch> FenceProducersPerKeyValue =
        static value => FenceProducersValue(value, 0);

    /// <summary>
    /// Builds <see cref="FenceProducersValue"/> from its two accessors so that the wiring guard
    /// can read them back off the closure (the <see cref="KeyedResultMarshal.StringKeyReader"/>
    /// rationale, applied to a two-accessor bundle).
    /// </summary>
    /// <param name="getProducerId">That result's <c>get_producer_id(i)</c>.</param>
    /// <param name="getEpochId">That result's <c>get_epoch_id(i)</c>.</param>
    /// <returns>A reader over <c>(result, index)</c>.</returns>
    internal static Func<IntPtr, int, ProducerIdAndEpoch> FenceProducersValueReader(
        Func<IntPtr, int, long> getProducerId,
        Func<IntPtr, int, short> getEpochId) =>
        (result, index) =>
            new ProducerIdAndEpoch(getProducerId(result, index), getEpochId(result, index));

    private static void OnFenceProducers(IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            FenceProducersPerKeyValue,
            s_destroyFenceProducersResult);

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_transactions_callback_t</c>:
    /// <c>void (*)(const char* transactional_id,
    /// kafka_admin_DescribeTransactionsResult_t* value, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — result shape <b>4a</b>, fired <b>once per distinct id</b>.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ Same shape as <see cref="FenceProducersCallback"/>: no standalone per-key value
    /// handle exists, so <paramref name="value"/> is a
    /// <c>DescribeTransactionsResult_t</c> carrying exactly this one id at <b>index 0</b>,
    /// <b>owned</b> and freed with <c>kafka_admin_DescribeTransactionsResult_destroy</c>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeTransactionsCallback(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>The rooted instance passed to every <c>describe_transactions_async</c> submission.</summary>
    internal static readonly DescribeTransactionsCallback DescribeTransactions = OnDescribeTransactions;

    /// <summary>
    /// <c>describeTransactions</c>' row reader: six inline scalars plus the nested
    /// <c>(i, j)</c> partition walk.
    /// </summary>
    /// <remarks>
    /// ⚠ The inner walk is bounded by <c>get_topic_partition_count(i)</c> — <b>never</b> by the
    /// outer <c>count</c>, which is <c>0</c> for a failed row (header,
    /// <c>kafka_admin_DescribeTransactionsResult_get_topic_partition_count</c>).
    /// </remarks>
    internal static readonly Func<IntPtr, int, TransactionDescription> DescribeTransactionsValue =
        TransactionDescriptionMarshal.ReadDescription;

    private static readonly Action<IntPtr> s_destroyDescribeTransactionsResult =
        NativeMethods.DescribeTransactionsResultDestroy;

    /// <inheritdoc cref="FenceProducersPerKeyValue"/>
    internal static readonly Func<IntPtr, TransactionDescription> DescribeTransactionsPerKeyValue =
        static value => DescribeTransactionsValue(value, 0);

    private static void OnDescribeTransactions(IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            DescribeTransactionsPerKeyValue,
            s_destroyDescribeTransactionsResult);

    /// <summary>
    /// <c>kafka_admin_AdminClient_describe_producers_callback_t</c>:
    /// <c>void (*)(const char* topic, int32_t partition,
    /// kafka_admin_DescribeProducersResult_t* value, kafka_common_KafkaError_t* error,
    /// void* user_data)</c> — result shape <b>4a</b>, fired <b>once per distinct
    /// partition</b>, with the key decomposed into two scalars.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>This RPC has no standalone per-key value handle</b> (plan §1.1 item 4, the
    /// third such case after <see cref="DescribeTransactionsCallback"/> and
    /// <see cref="FenceProducersCallback"/>), so the ABI reuses the flattened result type
    /// as the single-key value: <paramref name="value"/> is a
    /// <c>DescribeProducersResult_t</c> carrying exactly this one partition, readable at
    /// <b>index 0</b>, and it is <b>owned</b> — freed with
    /// <c>kafka_admin_DescribeProducersResult_destroy</c>, the same function the
    /// synchronous path uses. It is <em>not</em> a result root to walk.
    /// <paramref name="topic"/> is borrowed for the call only; <paramref name="error"/> is
    /// owned too.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeProducersCallback(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>The rooted instance passed to every <c>describe_producers_async</c> submission.</summary>
    internal static readonly DescribeProducersCallback DescribeProducers = OnDescribeProducers;

    /// <summary>
    /// <c>describeProducers</c>' row reader: the nested <c>(i, j)</c> walk over that
    /// partition's active producers.
    /// </summary>
    /// <remarks>
    /// ⚠ The inner walk is bounded by <c>get_producer_count(i)</c> — <b>never</b> by the outer
    /// <c>count</c>, which is <c>0</c> for a failed partition (header,
    /// <c>kafka_admin_DescribeProducersResult_get_producer_count</c>).
    /// </remarks>
    internal static readonly Func<IntPtr, int, DescribeProducersResult.PartitionProducerState>
        DescribeProducersValue = PartitionProducerStateMarshal.ReadPartitionProducerState;

    /// <inheritdoc cref="FenceProducersPerKeyValue"/>
    internal static readonly Func<IntPtr, DescribeProducersResult.PartitionProducerState>
        DescribeProducersPerKeyValue = static value => DescribeProducersValue(value, 0);

    private static readonly Action<IntPtr> s_destroyDescribeProducersResult =
        NativeMethods.DescribeProducersResultDestroy;

    private static void OnDescribeProducers(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            new PartitionKeySource(topic, partition),
            value,
            error,
            userData,
            s_topicPartitionKey,
            DescribeProducersPerKeyValue,
            s_destroyDescribeProducersResult);

    /// <summary>
    /// <c>kafka_admin_ListTransactionsResult_by_broker_id_callback_t</c> — the broker
    /// discovery, Java's <c>byBrokerId()</c> outer future. Fired <b>exactly once</b>.
    /// </summary>
    /// <remarks>
    /// On success <paramref name="brokerIds"/> holds <paramref name="count"/> distinct ids,
    /// <b>borrowed for the call only</b>, and <paramref name="error"/> is null; on failure
    /// <paramref name="brokerIds"/> is null, <paramref name="count"/> is <c>0</c>, and
    /// <paramref name="error"/> is <b>owned</b> — and no per-broker callback follows (header,
    /// <c>kafka_admin_ListTransactionsResult_by_broker_id_callback_t</c>).
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListTransactionsByBrokerIdCallback(
        IntPtr brokerIds, int count, IntPtr error, IntPtr userData);

    /// <summary>
    /// <c>kafka_admin_AdminClient_list_transactions_callback_t</c> — one discovered broker's
    /// own outcome, fired once per broker the discovery announced, as that broker finishes.
    /// </summary>
    /// <remarks>
    /// Exactly one of <paramref name="value"/> / <paramref name="error"/> is non-null, and
    /// <b>both are owned</b>: <paramref name="value"/> is a
    /// <c>kafka_admin_ListTransactionsResult_t</c> carrying this one broker at index <c>0</c>
    /// (freed with <c>kafka_admin_ListTransactionsResult_destroy</c>), and
    /// <paramref name="error"/> is that broker's failure (header,
    /// <c>kafka_admin_AdminClient_list_transactions_callback_t</c>).
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListTransactionsCallback(
        int brokerId, IntPtr value, IntPtr error, IntPtr userData);

    /// <summary>
    /// The rooted discovery instance passed to every <c>list_transactions_async</c> submission.
    /// </summary>
    internal static readonly ListTransactionsByBrokerIdCallback ListTransactionsByBrokerId =
        OnListTransactionsByBrokerId;

    /// <summary>
    /// The rooted per-broker instance passed to every <c>list_transactions_async</c> submission.
    /// </summary>
    internal static readonly ListTransactionsCallback ListTransactions = OnListTransactions;

    /// <summary>
    /// ⚠⚠ <c>listTransactions</c>' <b>discovery</b> trampoline — the first of the two stages
    /// (see <see cref="ListTransactionsAdminOperation"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>On success the per-broker callbacks are added to the countdown FIRST</b> — before
    /// anything that can throw, and necessarily before this callback releases its own count
    /// in the <c>finally</c>. Native fires <paramref name="count"/> per-broker callbacks
    /// whatever this body does, so the count taken from the ABI argument is what they will
    /// release; added after the release, the countdown could reach zero in between and every
    /// per-broker callback would then recover its context from a freed <c>GCHandle</c>.
    /// </para>
    /// <para>
    /// The ids are copied out with <see cref="Marshal.Copy(IntPtr, int[], int, int)"/>: they
    /// are borrowed for this call only. An error is <b>owned</b> and goes through
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>, which frees it exactly once; it faults
    /// the outer stage, and so every one of the three public views.
    /// </para>
    /// </remarks>
    private static void OnListTransactionsByBrokerId(
        IntPtr brokerIds, int count, IntPtr error, IntPtr userData)
    {
        ListTransactionsAdminOperation? context = null;
        IntPtr ownedError = error;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (ListTransactionsAdminOperation)handle.Target!;

            if (ownedError != IntPtr.Zero)
            {
                // Hand the error to FromHandle, which frees it in its own finally.
                IntPtr consumed = ownedError;
                ownedError = IntPtr.Zero;
                context.FailDiscovery(KafkaException.FromHandle(consumed)!);
            }
            else
            {
                context.AddPendingCallbacks(count);
                context.PublishBrokers(CopyBrokerIds(brokerIds, count));
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.FailDiscovery(exception);
        }
        finally
        {
            if (ownedError != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(ownedError);
            }

            context?.ReleaseOne();
        }
    }

    /// <summary>
    /// ⚠⚠ <c>listTransactions</c>' <b>per-broker</b> trampoline — the second stage. Resolves
    /// <b>that one broker's</b> awaitable and releases one countdown slot.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Both handles are owned and freed on <b>every</b> path: the value in the inner
    /// <c>finally</c> (null-safe <c>ListTransactionsResult_destroy</c>, after its listings are
    /// copied out), the error by <see cref="KafkaException.FromHandle(IntPtr)"/> — or by the
    /// outer <c>finally</c> when the context could not be recovered.
    /// </para>
    /// <para>
    /// ⚠ A broker the discovery did not announce "cannot happen" per the header. If it does,
    /// what arrived is destroyed and the slot released — never a throw across the boundary.
    /// A value and an error that are <b>both</b> null would also break the header's contract;
    /// that faults this broker with a message naming it rather than reading a NULL handle.
    /// </para>
    /// </remarks>
    private static void OnListTransactions(int brokerId, IntPtr value, IntPtr error, IntPtr userData)
    {
        ListTransactionsAdminOperation? context = null;
        IntPtr ownedError = error;
        try
        {
            try
            {
                GCHandle handle = GCHandle.FromIntPtr(userData);
                context = (ListTransactionsAdminOperation)handle.Target!;

                IntPtr consumed = ownedError;
                ownedError = IntPtr.Zero;
                KafkaException? failure = KafkaException.FromHandle(consumed);

                if (!context.IsAnnounced(brokerId))
                {
                    // Not a broker this operation is waiting on: nothing to resolve.
                }
                else if (failure is not null)
                {
                    context.SetBrokerException(brokerId, failure);
                }
                else if (value == IntPtr.Zero)
                {
                    context.SetBrokerException(
                        brokerId,
                        new KafkaException(
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "The listTransactions result carried neither listings nor an error for broker {0}.",
                                brokerId)));
                }
                else
                {
                    // The value carries exactly this one broker, at index 0 (header).
                    context.SetBrokerResult(brokerId, TransactionListingMarshal.ReadListings(value, 0));
                }
            }
            finally
            {
                NativeMethods.ListTransactionsResultDestroy(value);
            }
        }
        catch (Exception exception)
        {
            // Per-callback no-throw boundary: never unwind into native, and never fault the
            // other brokers, which have their own callbacks.
            context?.SetBrokerException(brokerId, exception);
        }
        finally
        {
            if (ownedError != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(ownedError);
            }

            context?.ReleaseOne();
        }
    }

    /// <summary>
    /// Copies the discovery's <b>borrowed</b> broker-id array out before the callback returns.
    /// </summary>
    /// <param name="brokerIds">The borrowed <c>const int32_t*</c>.</param>
    /// <param name="count">Its length, as the ABI reported it.</param>
    /// <returns>The owned ids.</returns>
    /// <exception cref="KafkaException">A positive count came with a NULL array.</exception>
    private static int[] CopyBrokerIds(IntPtr brokerIds, int count)
    {
        if (count <= 0)
        {
            return Array.Empty<int>();
        }

        if (brokerIds == IntPtr.Zero)
        {
            // Reading it would dereference NULL on the foreign thread — an uncatchable crash.
            throw new KafkaException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "The listTransactions broker discovery reported {0} broker(s) but no broker ids.",
                    count));
        }

        int[] ids = new int[count];
        Marshal.Copy(brokerIds, ids, 0, count);
        return ids;
    }

    /// <summary>
    /// ⚠⚠ <c>removeMembersFromConsumerGroup</c>' per-member <b>VALUE</b> reader — and it
    /// reads <c>get_error(i)</c>. Same shape as
    /// <see cref="AlterConsumerGroupOffsetsOptionalError"/>, for the same reason: Java's
    /// future resolves to <c>Map&lt;MemberIdentity, Errors&gt;</c>
    /// (<c>RemoveMembersFromConsumerGroupResult.java:35</c>) — one future over the whole
    /// map, so a per-member error is an ordinary map value, not a per-member fault. Only
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult.MemberResult"/> turns a non-null
    /// entry into a fault, mirroring Java's <c>memberResult</c>;
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult.All"/> does not consult the map at
    /// all: it carries the core's own <c>all()</c> outcome, read beside the map by
    /// <see cref="RemoveMembersFromConsumerGroupOutcome"/>. ⚠ The pointer is <b>BORROWED</b>
    /// from the result root, so it goes through
    /// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/> and is never destroyed.
    /// </summary>
    internal static readonly Func<IntPtr, int, KafkaException?> RemoveMembersFromConsumerGroupOptionalError =
        BorrowedOptionalError(NativeMethods.RemoveMembersFromConsumerGroupResultGetError);

    /// <summary>
    /// <c>electLeaders</c>' <b>composite</b> key reader — this result declares no
    /// <c>get_key</c>; the key is <c>(get_topic(i), get_partition(i))</c>, reassembled into
    /// the <see cref="TopicPartition"/> Java keys the map by. Same reader shape as
    /// <see cref="DeleteRecordsKey"/>, which is the precedent for a partition-composed key.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> ElectLeadersKey =
        TopicPartitionKey(
            NativeMethods.ElectLeadersResultGetTopic, NativeMethods.ElectLeadersResultGetPartition);

    /// <summary>
    /// ⚠⚠ <c>electLeaders</c>' per-partition <b>VALUE</b> reader — and it reads
    /// <c>get_error(i)</c>. This looks like a defect and is not.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java's future resolves to
    /// <c>Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;</c>
    /// (<c>ElectLeadersResult.java:36, :47</c>), and the javadoc on <c>partitions()</c>
    /// defines the value: "If the election succeeded then the value for a topic partition
    /// will be the empty Optional. Otherwise the election failed and the Optional will be
    /// set with the error" (<c>:43-46</c>). The per-partition error <em>is</em> the map's
    /// value, so it is read by the value reader and lands in the map — it does not fault
    /// anything. <c>Optional&lt;Throwable&gt;</c> maps to
    /// <see cref="KafkaException"/><c>?</c>, the same substitution M15/P3 made for
    /// <c>OptionalLong</c> → <c>long?</c>.
    /// </para>
    /// <para>
    /// ⚠ <b>The ABI accessor set cannot tell you this.</b>
    /// <c>kafka_admin_ElectLeadersResult_t</c> and
    /// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> declare byte-identical
    /// accessor sets — <c>count</c>, <c>get_topic</c>, <c>get_partition</c>,
    /// <c>get_error</c>, <c>destroy</c> — and
    /// <see cref="AlterPartitionReassignments"/> really does route its
    /// <c>get_error(i)</c> to the per-key failure channel
    /// (<see cref="AlterPartitionReassignmentsAccessors"/>). Only the Java return type
    /// separates the two, which is why routing this one through
    /// <c>Complete&lt;TKey&gt;</c> would compile, read plausibly, and be wrong.
    /// </para>
    /// <para>
    /// ⚠ The pointer is <b>BORROWED</b> from the result root, so it goes through
    /// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/> and is never destroyed —
    /// the same rule as every other per-key error site, unaffected by it being a value
    /// here.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, int, KafkaException?> ElectLeadersOptionalError =
        BorrowedOptionalError(NativeMethods.ElectLeadersResultGetError);

    /// <summary>
    /// ⚠⚠ <c>alterConsumerGroupOffsets</c>' per-partition <b>VALUE</b> reader — and it reads
    /// <c>get_error(i)</c>. Same shape as <see cref="ElectLeadersOptionalError"/>, for the
    /// same reason.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java's future resolves to <c>Map&lt;TopicPartition, Errors&gt;</c>
    /// (<c>AlterConsumerGroupOffsetsResult.java:33</c>) — <c>Errors.NONE</c> means that
    /// partition's offset was altered successfully, any other code is that partition's own
    /// outcome. The per-partition error <em>is</em> the map's value, so it is read by the
    /// value reader and lands in the map — it does not fault anything on its own; only
    /// <see cref="Admin.AlterConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/>
    /// turns a non-null entry into a fault, mirroring Java's <c>partitionResult</c>.
    /// <see cref="Admin.AlterConsumerGroupOffsetsResult.All"/> does not consult the map at
    /// all: it carries the core's own <c>all()</c> outcome, read beside the map by
    /// <see cref="AlterConsumerGroupOffsetsOutcome"/>.
    /// </para>
    /// <para>
    /// ⚠ <b>The ABI accessor set cannot tell you this.</b> This result's accessor set is
    /// byte-identical to <see cref="NativeMethods.ElectLeadersResultGetTopic"/>'s family and
    /// to <see cref="AlterPartitionReassignmentsAccessors"/>'s — accessor-set identity is
    /// evidence of nothing (<c>admin-client.md</c> §1.4). Only the Java return type
    /// separates them.
    /// </para>
    /// <para>
    /// ⚠ The pointer is <b>BORROWED</b> from the result root, so it goes through
    /// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/> and is never destroyed.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, int, KafkaException?> AlterConsumerGroupOffsetsOptionalError =
        BorrowedOptionalError(NativeMethods.AlterConsumerGroupOffsetsResultGetError);

    /// <summary>
    /// ⚠⚠ <c>deleteConsumerGroupOffsets</c>' per-partition <b>VALUE</b> reader — and it reads
    /// <c>get_error(i)</c>. Same shape as <see cref="AlterConsumerGroupOffsetsOptionalError"/>,
    /// for the same reason.
    /// </summary>
    /// <remarks>
    /// Java's future resolves to <c>Map&lt;TopicPartition, Errors&gt;</c>
    /// (<c>DeleteConsumerGroupOffsetsResult.java:31</c>) — <c>Errors.NONE</c> means that
    /// partition's offset was deleted successfully, any other code is that partition's own
    /// outcome. The per-partition error <em>is</em> the map's value, so it is read by the
    /// value reader and lands in the map — it does not fault anything on its own; only
    /// <see cref="Admin.DeleteConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/>
    /// turns a non-null entry into a fault, mirroring Java's <c>partitionResult</c>.
    /// <see cref="Admin.DeleteConsumerGroupOffsetsResult.All"/> carries the core's own
    /// <c>all()</c> outcome instead, read beside the map by
    /// <see cref="DeleteConsumerGroupOffsetsOutcome"/>.
    /// </remarks>
    internal static readonly Func<IntPtr, int, KafkaException?> DeleteConsumerGroupOffsetsOptionalError =
        BorrowedOptionalError(NativeMethods.DeleteConsumerGroupOffsetsResultGetError);

    /// <summary>
    /// <c>alterConsumerGroupOffsets</c>' <b>composite</b> key reader — the key is
    /// <c>(get_topic(i), get_partition(i))</c>, reassembled into the
    /// <see cref="TopicPartition"/> Java keys the map by. Same reader shape as
    /// <see cref="ElectLeadersKey"/>.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> AlterConsumerGroupOffsetsKey =
        TopicPartitionKey(
            NativeMethods.AlterConsumerGroupOffsetsResultGetTopic,
            NativeMethods.AlterConsumerGroupOffsetsResultGetPartition);

    /// <summary>
    /// <c>deleteConsumerGroupOffsets</c>' <b>composite</b> key reader — the same shape as
    /// <see cref="AlterConsumerGroupOffsetsKey"/>, over this result's own accessors.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> DeleteConsumerGroupOffsetsKey =
        TopicPartitionKey(
            NativeMethods.DeleteConsumerGroupOffsetsResultGetTopic,
            NativeMethods.DeleteConsumerGroupOffsetsResultGetPartition);

    /// <summary>
    /// ⚠⚠ <c>alterConsumerGroupOffsets</c>' <b>outcome</b> reader: the per-partition map
    /// (<see cref="AlterConsumerGroupOffsetsKey"/> →
    /// <see cref="AlterConsumerGroupOffsetsOptionalError"/>) plus the core's
    /// <c>kafka_admin_AlterConsumerGroupOffsetsResult_all</c>.
    /// </summary>
    /// <remarks>
    /// The <c>all()</c> fault is the <b>core's</b>, not rebuilt here — including Java's
    /// aggregate <c>Failed altering group offsets for the following partitions: [...]</c>
    /// message — so <see cref="Admin.AlterConsumerGroupOffsetsResult.All"/> rethrows it
    /// unchanged. See <see cref="KeyedOutcomes{TKey}"/> for the ownership of the two reads.
    /// </remarks>
    internal static readonly Func<IntPtr, (IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>
        AlterConsumerGroupOffsetsOutcome = KeyedOutcomes(
            NativeMethods.AlterConsumerGroupOffsetsResultCount,
            AlterConsumerGroupOffsetsKey,
            AlterConsumerGroupOffsetsOptionalError,
            NativeMethods.AlterConsumerGroupOffsetsResultAll,
            EqualityComparer<TopicPartition>.Default);

    /// <summary>
    /// ⚠⚠ <c>deleteConsumerGroupOffsets</c>' <b>outcome</b> reader: the per-partition map
    /// (<see cref="DeleteConsumerGroupOffsetsKey"/> →
    /// <see cref="DeleteConsumerGroupOffsetsOptionalError"/>) plus the core's
    /// <c>kafka_admin_DeleteConsumerGroupOffsetsResult_all</c>.
    /// </summary>
    /// <remarks>
    /// The <c>all()</c> fault is the <b>core's</b> choice of the first failing requested
    /// partition, not re-derived here, so
    /// <see cref="Admin.DeleteConsumerGroupOffsetsResult.All"/> rethrows it unchanged. See
    /// <see cref="KeyedOutcomes{TKey}"/> for the ownership of the two reads.
    /// </remarks>
    internal static readonly Func<IntPtr, (IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>
        DeleteConsumerGroupOffsetsOutcome = KeyedOutcomes(
            NativeMethods.DeleteConsumerGroupOffsetsResultCount,
            DeleteConsumerGroupOffsetsKey,
            DeleteConsumerGroupOffsetsOptionalError,
            NativeMethods.DeleteConsumerGroupOffsetsResultAll,
            EqualityComparer<TopicPartition>.Default);

    /// <summary>
    /// <c>removeMembersFromConsumerGroup</c>' per-member key reader:
    /// <c>get_group_instance_id(i)</c>, borrowed and NUL-terminated. Built by the shared
    /// <see cref="KeyedResultMarshal.StringKeyReader"/> factory so the wiring guard can see
    /// which accessor it closes over.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> RemoveMembersFromConsumerGroupKey =
        KeyedResultMarshal.StringKeyReader(NativeMethods.RemoveMembersFromConsumerGroupResultGetGroupInstanceId);

    /// <summary>
    /// ⚠⚠ <c>removeMembersFromConsumerGroup</c>' <b>outcome</b> reader: the per-member map
    /// (<see cref="RemoveMembersFromConsumerGroupKey"/> →
    /// <see cref="RemoveMembersFromConsumerGroupOptionalError"/>) plus the core's
    /// <c>kafka_admin_RemoveMembersFromConsumerGroupResult_all</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The <c>all()</c> fault is the <b>core's</b> — its choice of the first failing requested
    /// member, in group-instance-id order — not re-derived here, so
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult.All"/> rethrows it unchanged.
    /// </para>
    /// <para>
    /// ⚠ In <c>removeAll</c> mode <c>count</c> is 0, so the map is empty and <c>all()</c> is
    /// the <b>only</b> carrier of a member failure: a bare Kafka error with code -1 and
    /// Java's "Encounter error when trying to remove: MemberIdentity(...)" message, whose
    /// cause — the member's own error — <see cref="KafkaException.FromHandle"/> reads into its
    /// <see cref="Exception.InnerException"/>. See <see cref="KeyedOutcomes{TKey}"/> for the
    /// ownership of the two reads.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, (IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)>
        RemoveMembersFromConsumerGroupOutcome = KeyedOutcomes(
            NativeMethods.RemoveMembersFromConsumerGroupResultCount,
            RemoveMembersFromConsumerGroupKey,
            RemoveMembersFromConsumerGroupOptionalError,
            NativeMethods.RemoveMembersFromConsumerGroupResultAll,
            StringComparer.Ordinal);

    /// <summary>
    /// <c>alterPartitionReassignments</c>' universal accessors — result <b>shape 2</b>:
    /// the ABI declares no <c>_get_value</c>, because Java's per-partition future is
    /// <c>KafkaFuture&lt;Void&gt;</c>, so a null per-partition error <em>is</em> the
    /// success value. ⚠⚠ The accessor pair is byte-identical to <c>electLeaders</c>';
    /// see <see cref="ElectLeadersOptionalError"/> for why the two shapes still differ.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors AlterPartitionReassignmentsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.AlterPartitionReassignmentsResultCount,
            NativeMethods.AlterPartitionReassignmentsResultGetError);

    /// <summary>
    /// The rooted instance passed to every <c>list_partition_reassignments_async</c>
    /// submission (result shape 3 — one aggregate future, so no accessor set).
    /// </summary>
    internal static readonly ListPartitionReassignmentsCallback ListPartitionReassignments =
        OnListPartitionReassignments;

    /// <summary>
    /// The rooted instance passed to every <c>list_offsets_async</c> submission (result
    /// shape 1, composite key).
    /// </summary>
    internal static readonly ListOffsetsCallback ListOffsets = OnListOffsets;

    /// <summary>
    /// The rooted instance passed to every <c>list_groups_async</c> submission (sub-shape
    /// 3c — one aggregate future over two independent lists, so no accessor set).
    /// </summary>
    internal static readonly ListGroupsCallback ListGroups = OnListGroups;

    /// <summary>
    /// The rooted instance passed to every <c>list_consumer_groups_async</c> submission
    /// (sub-shape 3c, the same as <see cref="ListGroups"/> — one aggregate future over two
    /// independent lists, so no accessor set).
    /// </summary>
    internal static readonly ListConsumerGroupsCallback ListConsumerGroups = OnListConsumerGroups;

    /// <summary>
    /// The rooted instance passed to every <c>describe_consumer_groups_async</c> submission
    /// (shape 1 — a keyed map, one future per requested group id).
    /// </summary>
    internal static readonly DescribeConsumerGroupsCallback DescribeConsumerGroups =
        OnDescribeConsumerGroups;

    /// <summary>
    /// The rooted instance passed to every <c>describe_classic_groups_async</c> submission
    /// (shape 1 — a keyed map, one future per requested group id).
    /// </summary>
    internal static readonly DescribeClassicGroupsCallback DescribeClassicGroups =
        OnDescribeClassicGroups;

    /// <summary>
    /// The rooted instance passed to every <c>list_consumer_group_offsets_async</c>
    /// submission (shape 1 — a keyed map, one future per requested group id).
    /// </summary>
    internal static readonly ListConsumerGroupOffsetsCallback ListConsumerGroupOffsets =
        OnListConsumerGroupOffsets;

    /// <inheritdoc cref="ElectLeadersKey"/>
    internal static readonly Func<IntPtr, int, TopicPartition> ListPartitionReassignmentsKey =
        TopicPartitionKey(
            NativeMethods.ListPartitionReassignmentsResultGetTopic,
            NativeMethods.ListPartitionReassignmentsResultGetPartition);

    /// <summary>
    /// <c>listPartitionReassignments</c>' value reader: <c>get_value(i)</c> yields a
    /// borrowed <c>PartitionReassignment_t</c>, whose three broker lists are copied out
    /// before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Unlike <see cref="ElectLeadersOptionalError"/>, this one reads a genuine
    /// <c>get_value</c>. Shape 3 now has three members and they do not agree on which
    /// accessor carries the map's value: <c>listTopics</c> and
    /// <c>listPartitionReassignments</c> read <c>get_value</c>, while <c>electLeaders</c>
    /// reads <c>get_error</c> — because Java's map value differs
    /// (<c>TopicListing</c> / <c>PartitionReassignment</c> versus
    /// <c>Optional&lt;Throwable&gt;</c>). Which accessor to read is a per-RPC fact, not a
    /// property of the shape.
    /// </remarks>
    internal static readonly Func<IntPtr, int, PartitionReassignment> PartitionReassignmentValue =
        static (result, index) =>
            PartitionReassignmentMarshal.CopyOut(
                NativeMethods.ListPartitionReassignmentsResultGetValue(result, index))
            ?? throw new KafkaException(
                "The listPartitionReassignments result produced no reassignment for an index "
                + "within its own count.");

    /// <summary>
    /// <c>listGroups</c>' <b>first</b>-list reader: <c>get_valid(i)</c> yields a borrowed
    /// <c>GroupListing_t</c>, whose four fields are copied out before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>group_type</c> and <c>group_state</c> return <b>null for Java's
    /// <c>Optional.empty()</c></b> — an older broker that reported neither — and
    /// <see cref="GroupMarshal.TypeFromName"/> / <see cref="GroupMarshal.StateFromName"/>
    /// map that to <see langword="null"/>. Null is <em>absence</em>, not failure, and it is
    /// distinct from the <c>Unknown</c> an unrecognised name decodes to, so neither is
    /// rejected here. <c>group_id</c> and <c>protocol</c> are the opposite case: the header
    /// says both are non-null (<c>protocol</c> is the <em>empty string</em> for a classic
    /// group not using one), and <see cref="GroupListing"/>'s constructor rejects null for
    /// both, so a null from either is an ABI contract violation and faults the call.
    /// </remarks>
    internal static readonly Func<IntPtr, int, GroupListing> GroupListingValue =
        static (result, index) =>
        {
            IntPtr listing = NativeMethods.ListGroupsResultGetValid(result, index);
            if (listing == IntPtr.Zero)
            {
                throw new KafkaException(
                    "The listGroups result produced no listing for an index within its own valid count.");
            }

            return new GroupListing(
                Utf8Marshal.PtrToString(NativeMethods.GroupListingGroupId(listing))
                    ?? throw new KafkaException("The listGroups result produced a listing with no group id."),
                GroupMarshal.TypeFromName(NativeMethods.GroupListingGroupType(listing)),
                Utf8Marshal.PtrToString(NativeMethods.GroupListingProtocol(listing))
                    ?? throw new KafkaException("The listGroups result produced a listing with no protocol."),
                GroupMarshal.StateFromName(NativeMethods.GroupListingGroupState(listing)));
        };

    /// <summary>
    /// <c>listGroups</c>' <b>second</b>-list reader: one broker's failure, read from
    /// <c>get_error(i)</c> — a value on a <b>successful</b> call, never a fault.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>BORROWED.</b> The header returns <c>const kafka_common_Error_t *</c> and says
    /// "Do not destroy it", so this reads through
    /// <see cref="KafkaException.FromBorrowedHandle"/>; it dies with the result root, which
    /// the trampoline destroys exactly once. <see cref="KafkaException.FromHandle"/> here —
    /// the reflex, since the callback's own <c>error</c> parameter a few lines away takes
    /// exactly that — frees it a second time and aborts the host.
    /// </para>
    /// <para>
    /// ⚠ It is a <em>value</em> for the same reason <see cref="ElectLeadersOptionalError"/>
    /// is: Java publishes it through <c>ListGroupsResult.errors()</c> as an ordinary
    /// <c>Collection&lt;Throwable&gt;</c> (<c>ListGroupsResult.java:58</c>), and only
    /// <c>all()</c> rethrows the first of them (<c>:52-53</c>). A non-empty error list
    /// alongside a non-empty listing list is Java's normal partial-success outcome.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, int, KafkaException> ListGroupsBrokerError =
        static (result, index) =>
            KafkaException.FromBorrowedHandle(NativeMethods.ListGroupsResultGetError(result, index))
            ?? throw new KafkaException(
                "The listGroups result produced no error for an index within its own error count.");

#pragma warning disable CS0618 // Java deprecates the listing type itself; mirrored, not avoided.

    /// <summary>
    /// <c>listConsumerGroups</c>' <b>first</b>-list reader: <c>get_valid(i)</c> yields a
    /// borrowed <c>ConsumerGroupListing_t</c>, whose four fields are copied out before the
    /// root dies.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <c>group_state</c> and <c>group_type</c> return <b>null for Java's
    /// <c>Optional.empty()</c></b> and are mapped by the same two
    /// <see cref="GroupMarshal"/> entries <see cref="GroupListingValue"/> uses, with the
    /// same reading: null is <em>absence</em>, distinct from the <c>Unknown</c> an
    /// unrecognised name decodes to, and neither is rejected. <c>group_id</c> is the
    /// opposite case — the header says it is never null and the constructor rejects null —
    /// so a null there is an ABI contract violation and faults the call.
    /// </para>
    /// <para>
    /// ⚠⚠ <b>The two accessors this reads and skips are the <em>mirror image</em> of
    /// <see cref="GroupListingValue"/>'s.</b> <c>is_simple_consumer_group</c> is read here
    /// and deliberately not declared there; <c>state</c> is skipped here although the ABI
    /// exports it. Both follow from which value the managed class stores and which it
    /// derives, and the two classes differ: <c>ConsumerGroupListing</c> stores the simple
    /// flag (it has no protocol to compute one from) and derives <c>State</c> as a lossy
    /// projection of <c>GroupState</c>, exactly as Java does. Reading the projection back
    /// out of the ABI would give a native-built listing and a caller-built one two ways to
    /// disagree on one axis.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, int, ConsumerGroupListing> ConsumerGroupListingValue =
        static (result, index) =>
        {
            IntPtr listing = NativeMethods.ListConsumerGroupsResultGetValid(result, index);
            if (listing == IntPtr.Zero)
            {
                throw new KafkaException(
                    "The listConsumerGroups result produced no listing for an index within its own "
                    + "valid count.");
            }

            return new ConsumerGroupListing(
                Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupListingGroupId(listing))
                    ?? throw new KafkaException(
                        "The listConsumerGroups result produced a listing with no group id."),
                GroupMarshal.StateFromName(NativeMethods.ConsumerGroupListingGroupState(listing)),
                GroupMarshal.TypeFromName(NativeMethods.ConsumerGroupListingGroupType(listing)),
                NativeMethods.ConsumerGroupListingIsSimpleConsumerGroup(listing));
        };

#pragma warning restore CS0618

    /// <summary>
    /// <c>listConsumerGroups</c>' <b>second</b>-list reader: one broker's failure, read from
    /// <c>get_error(i)</c> — a value on a <b>successful</b> call, never a fault.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>BORROWED</b>, for the reasons stated at length on
    /// <see cref="ListGroupsBrokerError"/>: the header returns
    /// <c>const kafka_common_Error_t *</c> and says "Do not destroy it", so this reads
    /// through <see cref="KafkaException.FromBorrowedHandle"/> and never
    /// <see cref="KafkaException.FromHandle"/>, which would free a handle the result root
    /// still owns. It is a <em>value</em> for the same reason too — Java publishes it
    /// through <c>ListConsumerGroupsResult.errors()</c> and only <c>all()</c> rethrows the
    /// first of them, so errors alongside listings are a normal partial success.
    /// </remarks>
    internal static readonly Func<IntPtr, int, KafkaException> ListConsumerGroupsBrokerError =
        static (result, index) =>
            KafkaException.FromBorrowedHandle(NativeMethods.ListConsumerGroupsResultGetError(result, index))
            ?? throw new KafkaException(
                "The listConsumerGroups result produced no error for an index within its own error count.");

    /// <summary>
    /// <c>describeConsumerGroups</c>' key reader: the group id at one index.
    /// </summary>
    /// <remarks>
    /// ⚠ The bridge dictionary these keys resolve against must be built with
    /// <see cref="StringComparer.Ordinal"/>. <c>DescribeConsumerGroupsResult</c>'s
    /// aggregate hardcodes that comparer and — unlike <c>DescribeTopicsResult</c> — has a
    /// public constructor with no internal factory to thread a different one through, so a
    /// bridge keyed by the default comparer would silently disagree with the result it
    /// feeds.
    /// </remarks>
    internal static readonly Func<IntPtr, int, string> DescribeConsumerGroupsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(
                NativeMethods.DescribeConsumerGroupsResultGetGroupId(result, index));

    /// <summary>
    /// <c>describeConsumerGroups</c>' <b>shape-4a</b> value reader — the same copy-out,
    /// over the <b>owned</b> description the callback is handed directly. Nothing here
    /// borrows from a result root any more, so the copy-out is what must complete before
    /// the trampoline's <c>finally</c> destroys the description itself.
    /// </summary>
    internal static readonly Func<IntPtr, ConsumerGroupDescription> ConsumerGroupDescriptionPerKeyValue =
        CopyOutConsumerGroupDescription;

    /// <summary>
    /// <c>describeClassicGroups</c>' key reader: the group id at one index.
    /// </summary>
    /// <remarks>
    /// ⚠ The bridge dictionary these keys resolve against must be built with
    /// <see cref="StringComparer.Ordinal"/>, for the reason given on
    /// <see cref="DescribeConsumerGroupsKey"/>: <c>DescribeClassicGroupsResult</c>'s
    /// aggregate hardcodes that comparer and has a public constructor with no internal
    /// factory to thread a different one through.
    /// </remarks>
    internal static readonly Func<IntPtr, int, string> DescribeClassicGroupsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(
                NativeMethods.DescribeClassicGroupsResultGetGroupId(result, index));

    /// <inheritdoc cref="ConsumerGroupDescriptionPerKeyValue"/>
    internal static readonly Func<IntPtr, ClassicGroupDescription> ClassicGroupDescriptionPerKeyValue =
        CopyOutClassicGroupDescription;

    /// <summary>
    /// <c>listConsumerGroupOffsets</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>OffsetAndMetadataMap_t</c>, copied out entry by entry before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>Everything reachable from here is borrowed from the one result root</b>, two
    /// levels deep: the map from the result, every topic and metadata string from the map.
    /// None of it is owned, none of it is freed here, and all of it dangles the moment
    /// <see cref="NativeMethods.ListConsumerGroupOffsetsResultDestroy"/> runs — which is why
    /// the copy-out completes inside the walk and the destroy is in the trampoline's
    /// <c>finally</c>, strictly after. No native-backed string or pointer is retained.
    /// </remarks>
    internal static readonly Func<IntPtr, int, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>
        ListConsumerGroupOffsetsValue =
            static (result, index) =>
                CopyOutOffsetAndMetadataMap(
                    NativeMethods.ListConsumerGroupOffsetsResultGetValue(result, index));

    /// <inheritdoc cref="ConsumerGroupDescriptionPerKeyValue"/>
    internal static readonly Func<IntPtr, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>
        ListConsumerGroupOffsetsPerKeyValue = CopyOutOffsetAndMetadataMap;

    /// <summary>
    /// The result-root destroys, hoisted for the same reason as the accessor sets: a
    /// method group converted at the call site would allocate a delegate per completion.
    /// All are null-safe, so the trampoline's <c>finally</c> can call them
    /// unconditionally.
    /// </summary>
    /// <remarks>
    /// This one has no remaining call site — <c>createTopics</c> moved to the per-key
    /// shape below, which has no result root to free — and survives as the documentation
    /// anchor every sibling destroy inherits from. It goes with the shape-1/2 walkers.
    /// </remarks>
    private static readonly Action<IntPtr> s_destroyCreateTopicsResult = NativeMethods.CreateTopicsResultDestroy;

    /// <summary>
    /// <c>createTopics</c>' <b>per-key</b> value destroy (shape 4a). ⚠ <b>Not</b>
    /// <see cref="s_destroyCreateTopicsResult"/>, which frees a result root the per-key
    /// path does not have.
    /// </summary>
    private static readonly Action<IntPtr> s_destroyTopicMetadataAndConfig =
        NativeMethods.TopicMetadataAndConfigDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyTopicDescription =
        NativeMethods.TopicDescriptionDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyConfig = NativeMethods.ConfigDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyLogDirDescriptionMap =
        NativeMethods.LogDirDescriptionMapDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyConsumerGroupDescription =
        NativeMethods.ConsumerGroupDescriptionDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyClassicGroupDescription =
        NativeMethods.ClassicGroupDescriptionDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyOffsetAndMetadataMap =
        NativeMethods.OffsetAndMetadataMapDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyDeletedRecords =
        NativeMethods.DeletedRecordsDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyReplicaLogDirInfo =
        NativeMethods.ReplicaLogDirInfoDestroy;

    /// <inheritdoc cref="s_destroyTopicMetadataAndConfig"/>
    private static readonly Action<IntPtr> s_destroyListOffsetsResultInfo =
        NativeMethods.ListOffsetsResultInfoDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListConfigResourcesResult =
        NativeMethods.ListConfigResourcesResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListClientMetricsResourcesResult =
        NativeMethods.ListClientMetricsResourcesResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListTopicsResult = NativeMethods.ListTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyElectLeadersResult = NativeMethods.ElectLeadersResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult" path="/summary"/>
    private static readonly Action<IntPtr> s_destroyAlterConsumerGroupOffsetsResult =
        NativeMethods.AlterConsumerGroupOffsetsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult" path="/summary"/>
    private static readonly Action<IntPtr> s_destroyDeleteConsumerGroupOffsetsResult =
        NativeMethods.DeleteConsumerGroupOffsetsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult" path="/summary"/>
    private static readonly Action<IntPtr> s_destroyRemoveMembersFromConsumerGroupResult =
        NativeMethods.RemoveMembersFromConsumerGroupResultDestroy;

    /// <summary>
    /// The count accessors the two sub-shape-3b walks read, hoisted for the same reason as
    /// everything else here.
    /// </summary>
    private static readonly KeyedResultMarshal.CountAccessor s_listConfigResourcesCount =
        NativeMethods.ListConfigResourcesResultCount;

    /// <inheritdoc cref="s_listConfigResourcesCount"/>
    private static readonly KeyedResultMarshal.CountAccessor s_listClientMetricsResourcesCount =
        NativeMethods.ListClientMetricsResourcesResultCount;

    /// <summary>
    /// A count accessor a <b>shape-3</b> aggregate walk reads, hoisted for the same reason
    /// as everything else here.
    /// </summary>
    /// <remarks>
    /// ⚠ Phrased per-field rather than as a count, because every <c>&lt;inheritdoc&gt;</c>
    /// below copies this sentence verbatim — so a number here becomes a number in each of
    /// them, and goes stale the moment the shape gains a member. It did: this read "the two
    /// shape-3 aggregate walks" until <c>listPartitionReassignments</c> became the third
    /// (finding 70.11), and the new field inherited the wrong count on the way in.
    /// </remarks>
    private static readonly KeyedResultMarshal.CountAccessor s_listTopicsCount =
        NativeMethods.ListTopicsResultCount;

    /// <inheritdoc cref="s_listTopicsCount"/>
    private static readonly KeyedResultMarshal.CountAccessor s_electLeadersCount =
        NativeMethods.ElectLeadersResultCount;

    /// <inheritdoc cref="s_listTopicsCount"/>
    private static readonly KeyedResultMarshal.CountAccessor s_listPartitionReassignmentsCount =
        NativeMethods.ListPartitionReassignmentsResultCount;

    /// <summary>
    /// The count bounding <c>listGroups</c>' <b>listing</b> walk, hoisted for the same
    /// reason as everything else here.
    /// </summary>
    private static readonly KeyedResultMarshal.CountAccessor s_listGroupsValidCount =
        NativeMethods.ListGroupsResultValidCount;

    /// <summary>
    /// The count bounding <c>listGroups</c>' <b>error</b> walk — its own, separate count.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>Not <see cref="s_listGroupsValidCount"/>.</b> The header states the two lists
    /// are not parallel and are generally of different lengths, so each walk is bounded by
    /// the count belonging to the list it reads. They are two fields here, rather than one
    /// reused, so that the bug is not expressible at the call site.
    /// </remarks>
    private static readonly KeyedResultMarshal.CountAccessor s_listGroupsErrorCount =
        NativeMethods.ListGroupsResultErrorCount;

    /// <summary>
    /// The count bounding <c>listConsumerGroups</c>' <b>listing</b> walk, hoisted for the
    /// same reason as everything else here.
    /// </summary>
    private static readonly KeyedResultMarshal.CountAccessor s_listConsumerGroupsValidCount =
        NativeMethods.ListConsumerGroupsResultValidCount;

    /// <summary>
    /// The count bounding <c>listConsumerGroups</c>' <b>error</b> walk — its own, separate
    /// count.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>Not <see cref="s_listConsumerGroupsValidCount"/></b>, and not
    /// <see cref="s_listGroupsErrorCount"/> either — the neighbouring RPC's count reads a
    /// different result type through the same-shaped accessor, so a mis-wire there is a
    /// read against a foreign root. The header states this RPC's two lists are not parallel
    /// and are generally of different lengths, so each walk is bounded by the count
    /// belonging to the list it reads.
    /// </remarks>
    private static readonly KeyedResultMarshal.CountAccessor s_listConsumerGroupsErrorCount =
        NativeMethods.ListConsumerGroupsResultErrorCount;

    /// <summary>
    /// <c>ConsumerGroupDescription</c>'s three authorized-operation accessors, hoisted so a
    /// copy-out allocates no delegates. The <b>rule</b> they feed — that the boolean gate,
    /// not the count, separates Java's null from an empty set — lives once, in
    /// <see cref="AuthorizedOperationsMarshal"/>, so this result cannot re-derive it
    /// differently from <c>describeTopics</c> or <c>describeCluster</c>.
    /// </summary>
    private static readonly Func<IntPtr, bool> s_consumerGroupHasAuthorizedOperations =
        NativeMethods.ConsumerGroupDescriptionHasAuthorizedOperations;

    /// <inheritdoc cref="s_consumerGroupHasAuthorizedOperations"/>
    private static readonly Func<IntPtr, int> s_consumerGroupAuthorizedOperationCount =
        NativeMethods.ConsumerGroupDescriptionAuthorizedOperationCount;

    /// <inheritdoc cref="s_consumerGroupHasAuthorizedOperations"/>
    private static readonly Func<IntPtr, int, int> s_consumerGroupAuthorizedOperation =
        NativeMethods.ConsumerGroupDescriptionAuthorizedOperation;

    /// <summary>
    /// <c>ClassicGroupDescription</c>'s three authorized-operation accessors, hoisted so a
    /// copy-out allocates no delegates. They feed the same single
    /// <see cref="AuthorizedOperationsMarshal"/> rule
    /// <see cref="s_consumerGroupHasAuthorizedOperations"/> does, so this result cannot
    /// re-derive the absent-versus-empty discriminant differently.
    /// </summary>
    private static readonly Func<IntPtr, bool> s_classicGroupHasAuthorizedOperations =
        NativeMethods.ClassicGroupDescriptionHasAuthorizedOperations;

    /// <inheritdoc cref="s_classicGroupHasAuthorizedOperations"/>
    private static readonly Func<IntPtr, int> s_classicGroupAuthorizedOperationCount =
        NativeMethods.ClassicGroupDescriptionAuthorizedOperationCount;

    /// <inheritdoc cref="s_classicGroupHasAuthorizedOperations"/>
    private static readonly Func<IntPtr, int, int> s_classicGroupAuthorizedOperation =
        NativeMethods.ClassicGroupDescriptionAuthorizedOperation;

    /// <summary>
    /// The production binding of the six <c>kafka_admin_OffsetAndMetadataMap_*</c> entry
    /// points, hoisted so copying a group's offsets out allocates no delegates.
    /// </summary>
    private static readonly OffsetAndMetadataMapAccessors s_offsetAndMetadataMapAccessors =
        new OffsetAndMetadataMapAccessors(
            NativeMethods.OffsetAndMetadataMapCount,
            NativeMethods.OffsetAndMetadataMapGetTopic,
            NativeMethods.OffsetAndMetadataMapGetPartition,
            NativeMethods.OffsetAndMetadataMapHasOffset,
            NativeMethods.OffsetAndMetadataMapGetOffset,
            NativeMethods.OffsetAndMetadataMapGetMetadata,
            NativeMethods.OffsetAndMetadataMapGetLeaderEpoch);

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListPartitionReassignmentsResult =
        NativeMethods.ListPartitionReassignmentsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteAclsFilterResults =
        NativeMethods.DeleteAclsFilterResultsDestroy;

    /// <summary>The three per-key KEY destroys — the only RPCs whose key is owned.</summary>
    private static readonly Action<IntPtr> s_destroyAclBindingFilter =
        NativeMethods.AclBindingFilterDestroy;

    /// <inheritdoc cref="s_destroyAclBindingFilter"/>
    private static readonly Action<IntPtr> s_destroyAclBinding =
        NativeMethods.AclBindingDestroy;

    /// <inheritdoc cref="s_destroyAclBindingFilter"/>
    private static readonly Action<IntPtr> s_destroyClientQuotaEntity =
        NativeMethods.ClientQuotaEntityDestroy;

    private static readonly KeyedResultMarshal.CountAccessor s_describeAclsCount =
        NativeMethods.DescribeAclsResultCount;

    private static readonly Action<IntPtr> s_destroyDescribeAclsResult =
        NativeMethods.DescribeAclsResultDestroy;

    private static readonly KeyedResultMarshal.CountAccessor s_describeClientQuotasCount =
        NativeMethods.DescribeClientQuotasResultCount;

    private static readonly Action<IntPtr> s_destroyDescribeClientQuotasResult =
        NativeMethods.DescribeClientQuotasResultDestroy;

    private static void OnClose(IntPtr error, IntPtr userData)
    {
        OperationCompletionSource? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource)handle.Target!;
            context.Complete(error);
        }
        catch (Exception exception)
        {
            // No-throw boundary: never unwind into native. Surface via the Task if the
            // context was recovered; otherwise there is nothing to fault.
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of the GCHandle free and the span-the-op reference release, on
            // every path (ffi §B6 hookless one-shot).
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// <b>Shape 4a</b> — the one body every per-key value-carrying trampoline delegates
    /// to. Resolves <b>one</b> key and releases <b>one</b> countdown slot.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <see cref="AdminOperation.ReleaseOne"/> runs in the <c>finally</c> on
    /// <b>every</b> path. Skipping it on any path leaves the countdown short of zero
    /// forever: the rooting <c>GCHandle</c> is never freed and
    /// <c>AdminClient_destroy</c> is deferred for the process lifetime, with no managed
    /// symptom.
    /// </para>
    /// <para>
    /// ⚠ <paramref name="value"/> and <paramref name="error"/> are owned, and ownership
    /// passes to <see cref="KeyedResultMarshal.CompleteKey{TKey, TValue}"/> the instant
    /// it is called — which is what <c>resolved</c> tracks. Before that point (no
    /// context, or an unreadable key) this body owes both frees itself; an unnameable key
    /// has no source to resolve, and the countdown-zero <c>FailUncompleted</c> faults
    /// whichever requested key went unaccounted for.
    /// </para>
    /// </remarks>
    /// <param name="key">
    /// This callback's raw key argument(s) — a borrowed <c>const char*</c>, a scalar, or a
    /// struct bundling the parts of a composite key. Nothing native-backed in it outlives
    /// the call, so <paramref name="readKey"/> must copy out.
    /// </param>
    /// <param name="value">This key's owned value handle, or <c>IntPtr.Zero</c>.</param>
    /// <param name="error">This key's owned error, or <c>IntPtr.Zero</c> on success.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="readKey">That RPC's key reader over the raw key argument(s).</param>
    /// <param name="readValue">That RPC's value reader over the owned value handle.</param>
    /// <param name="destroyValue">That value type's own destroy.</param>
    private static void CompletePerKey<TKeySource, TKey, TValue>(
        TKeySource key,
        IntPtr value,
        IntPtr error,
        IntPtr userData,
        Func<TKeySource, TKey> readKey,
        Func<IntPtr, TValue> readValue,
        Action<IntPtr> destroyValue)
        where TKey : notnull
    {
        KeyedAdminOperation<TKey, TValue>? context = null;
        bool resolved = false;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (KeyedAdminOperation<TKey, TValue>)handle.Target!;

            TKey materialized = readKey(key);
            resolved = true;
            KeyedResultMarshal.CompleteKey(context, materialized, value, error, readValue, destroyValue);
        }
        catch (Exception)
        {
            // Per-callback no-throw boundary: never unwind into native, and never fault
            // the other N-1 keys, which have their own callbacks still to come.
        }
        finally
        {
            if (!resolved)
            {
                destroyValue(value);
                if (error != IntPtr.Zero)
                {
                    NativeMethods.ErrorDestroy(error);
                }
            }

            context?.ReleaseOne();
        }
    }

    /// <summary>
    /// <b>Shape 4a with an OWNED key</b> — <see cref="CompletePerKey{TKeySource, TKey, TValue}"/>
    /// plus the one obligation it cannot carry: destroying the key handle.
    /// </summary>
    /// <remarks>
    /// A wrapper rather than a fourth parameter on <c>CompletePerKey</c>, so the borrowed-key
    /// RPCs cannot accidentally acquire a key destroy and this one cannot silently lose it.
    /// <c>CompletePerKey</c> is already a total no-throw boundary, so the <c>finally</c> below
    /// is the entire difference; omitting it leaks one filter per key, with no managed symptom.
    /// </remarks>
    /// <param name="key">This key's <b>owned</b> handle.</param>
    /// <param name="value">This key's owned value, or <c>IntPtr.Zero</c> on failure.</param>
    /// <param name="error">This key's owned error, or <c>IntPtr.Zero</c> on success.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="readKey">That RPC's key reader, which must copy out before returning.</param>
    /// <param name="readValue">That RPC's value reader.</param>
    /// <param name="destroyValue">That value type's <c>_destroy</c>.</param>
    /// <param name="destroyKey">That key type's <c>_destroy</c>.</param>
    private static void CompletePerKeyOwnedKey<TKey, TValue>(
        IntPtr key,
        IntPtr value,
        IntPtr error,
        IntPtr userData,
        Func<IntPtr, TKey> readKey,
        Func<IntPtr, TValue> readValue,
        Action<IntPtr> destroyValue,
        Action<IntPtr> destroyKey)
        where TKey : notnull
    {
        try
        {
            CompletePerKey(key, value, error, userData, readKey, readValue, destroyValue);
        }
        finally
        {
            destroyKey(key);
        }
    }

    /// <summary>
    /// <b>Shape 4b</b> — the value-less twin of <see cref="CompletePerKey{TKeySource, TKey, TValue}"/>:
    /// a null <paramref name="error"/> <em>is</em> the success value.
    /// </summary>
    /// <remarks>
    /// Its own method rather than a null value reader, so a value-carrying operation
    /// cannot be routed here and have its value dropped — and it accepts only a
    /// <see cref="VoidKeyedAdminOperation{TKey}"/>. Every obligation in
    /// <see cref="CompletePerKey{TKeySource, TKey, TValue}"/>'s remarks applies verbatim.
    /// </remarks>
    /// <param name="key">
    /// This callback's raw key argument(s) — a borrowed <c>const char*</c>, an owned handle,
    /// or a struct bundling the parts of a composite key, exactly as in
    /// <see cref="CompletePerKey{TKeySource, TKey, TValue}"/>.
    /// </param>
    /// <param name="error">This key's owned error, or <c>IntPtr.Zero</c> on success.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="readKey">That RPC's key reader over the raw key argument(s).</param>
    private static void CompletePerKeyVoid<TKeySource, TKey>(
        TKeySource key,
        IntPtr error,
        IntPtr userData,
        Func<TKeySource, TKey> readKey)
        where TKey : notnull
    {
        VoidKeyedAdminOperation<TKey>? context = null;
        bool resolved = false;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (VoidKeyedAdminOperation<TKey>)handle.Target!;

            TKey materialized = readKey(key);
            resolved = true;
            KeyedResultMarshal.CompleteKey(context, materialized, error);
        }
        catch (Exception)
        {
            // Per-callback no-throw boundary. See CompletePerKey.
        }
        finally
        {
            if (!resolved && error != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(error);
            }

            context?.ReleaseOne();
        }
    }

    /// <summary>
    /// <b>Shape 4b with an OWNED key</b> — the void twin of
    /// <see cref="CompletePerKeyOwnedKey{TKey, TValue}"/>, a wrapper for the same reason.
    /// </summary>
    /// <param name="key">This key's <b>owned</b> handle.</param>
    /// <param name="error">This key's owned error, or <c>IntPtr.Zero</c> on success.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="readKey">That RPC's key reader, which must copy out before returning.</param>
    /// <param name="destroyKey">That key type's <c>_destroy</c>.</param>
    private static void CompletePerKeyVoidOwnedKey<TKey>(
        IntPtr key,
        IntPtr error,
        IntPtr userData,
        Func<IntPtr, TKey> readKey,
        Action<IntPtr> destroyKey)
        where TKey : notnull
    {
        try
        {
            CompletePerKeyVoid(key, error, userData, readKey);
        }
        finally
        {
            destroyKey(key);
        }
    }

    private static void OnCreateTopics(IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            TopicMetadataAndConfigPerKeyValue,
            s_destroyTopicMetadataAndConfig);

    private static void OnDeleteTopicsByName(IntPtr key, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(key, error, userData, s_stringKey);

    private static void OnDeleteTopicsById(IntPtr key, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(key, error, userData, s_uuidKey);

    private static void OnDescribeTopicsByName(IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            TopicDescriptionPerKeyValue,
            s_destroyTopicDescription);

    private static void OnDescribeTopicsById(IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_uuidKey,
            TopicDescriptionPerKeyValue,
            s_destroyTopicDescription);

    private static void OnCreatePartitions(IntPtr key, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(key, error, userData, s_stringKey);

    private static void OnDeleteRecords(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            new PartitionKeySource(topic, partition),
            value,
            error,
            userData,
            s_topicPartitionKey,
            DeletedRecordsPerKeyValue,
            s_destroyDeletedRecords);

    private static void OnDescribeConfigs(
        int resourceType, IntPtr resourceName, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            new KeyValuePair<int, IntPtr>(resourceType, resourceName),
            value,
            error,
            userData,
            s_configResourceKey,
            ConfigPerKeyValue,
            s_destroyConfig);

    private static void OnIncrementalAlterConfigs(
        int resourceType, IntPtr resourceName, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(
            new KeyValuePair<int, IntPtr>(resourceType, resourceName),
            error,
            userData,
            s_configResourceKey);

    private static void OnDescribeLogDirs(int broker, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            broker,
            value,
            error,
            userData,
            s_int32Key,
            LogDirDescriptionsPerKeyValue,
            s_destroyLogDirDescriptionMap);

    private static void OnAlterReplicaLogDirs(
        IntPtr topic, int partition, int brokerId, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(
            new PartitionKeySource(topic, partition, brokerId), error, userData, s_replicaKey);

    private static void OnDescribeReplicaLogDirs(
        IntPtr topic, int partition, int brokerId, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            new PartitionKeySource(topic, partition, brokerId),
            value,
            error,
            userData,
            s_replicaKey,
            ReplicaLogDirInfoPerKeyValue,
            s_destroyReplicaLogDirInfo);

    /// <summary>
    /// The one completion body every <b>shape-3</b> trampoline delegates to: one awaiter
    /// over a whole map, and no per-key error channel.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The differences from the keyed trampolines are exactly the two the shape implies.
    /// The callback's <c>error</c> is the <em>only</em> failure channel, so it faults the
    /// single awaiter rather than fanning out across keys; and any failure during the
    /// walk does the same, because there is no per-key <see cref="System.Threading.Tasks.Task"/>
    /// to attribute it to. The <c>finally</c>'s three obligations are unchanged.
    /// </para>
    /// <para>
    /// ⚠ "No per-key error channel" is about the <em>completion</em>, not about the
    /// accessor set: <c>electLeaders</c> routes through here while its result type does
    /// declare a <c>get_error</c>, because for that RPC the accessor carries the map's
    /// value (<see cref="ElectLeadersOptionalError"/>). What the shape asserts is that a
    /// per-key outcome never faults anything.
    /// </para>
    /// <para>
    /// This mirrors <see cref="CompleteListRpc{TValue}"/>: a shared body per shape, so the
    /// ownership rules are stated once instead of once per RPC.
    /// </para>
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="count">That RPC's <c>*Result_count</c>.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="readValue">That RPC's value reader.</param>
    /// <param name="keyComparer">The comparer the assembled map is keyed by.</param>
    /// <param name="destroyResult">That RPC's <c>*Result_destroy</c>.</param>
    private static void CompleteAggregateRpc<TKey, TValue>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        KeyedResultMarshal.CountAccessor count,
        Func<IntPtr, int, TKey> readKey,
        Func<IntPtr, int, TValue> readValue,
        IEqualityComparer<TKey> keyComparer,
        Action<IntPtr> destroyResult)
        where TKey : notnull
    {
        SingleAdminOperation<IReadOnlyDictionary<TKey, TValue>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<IReadOnlyDictionary<TKey, TValue>>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                // ⚠ OWNED — FromHandle frees it exactly once (the mirror image of the
                // per-key errors inside a keyed result, which are borrowed).
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.CompleteAggregate(result, count, context, readKey, readValue, keyComparer);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// <c>listTopics</c>' shape-3 trampoline: one awaiter over
    /// <c>Map&lt;String, TopicListing&gt;</c>, keyed by topic name.
    /// </summary>
    private static void OnListTopics(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_listTopicsCount,
            ListTopicsKey,
            TopicListingValue,
            StringComparer.Ordinal,
            s_destroyListTopicsResult);

    /// <summary>
    /// ⚠⚠ <c>electLeaders</c>' shape-3 trampoline — <b>the aggregate walker, with
    /// <c>get_error(i)</c> supplied as the VALUE reader.</b>
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java's <c>ElectLeadersResult</c> holds
    /// <c>KafkaFuture&lt;Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;&gt;</c>
    /// (<c>ElectLeadersResult.java:36, :47</c>) and its <c>partitions()</c> javadoc says
    /// "If the election succeeded then the value for a topic partition will be the empty
    /// Optional. Otherwise the election failed and the Optional will be set with the
    /// error" (<c>:43-46</c>). So a per-partition failure is a map <em>value</em> on a
    /// <b>successful</b> task, and <see cref="ElectLeadersOptionalError"/> is the reader
    /// that produces it.
    /// </para>
    /// <para>
    /// ⚠ <b>Reading the ABI accessor set alone gets this wrong.</b>
    /// <see cref="OnAlterPartitionReassignments"/> below sits on a byte-identical accessor
    /// set — <c>count</c>, <c>get_topic</c>, <c>get_partition</c>, <c>get_error</c>,
    /// <c>destroy</c> — and correctly routes through
    /// <see cref="CompletePerKeyVoid{TKeySource, TKey}"/>, where that partition's own error
    /// faults its own awaitable. Rewiring this
    /// trampoline to match it compiles and is wrong: it would fault a partition Java
    /// reports as an ordinary map entry, and would replace one future with N.
    /// </para>
    /// </remarks>
    private static void OnElectLeaders(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_electLeadersCount,
            ElectLeadersKey,
            ElectLeadersOptionalError,
            EqualityComparer<TopicPartition>.Default,
            s_destroyElectLeadersResult);

    /// <summary>
    /// ⚠⚠ <c>alterConsumerGroupOffsets</c>' trampoline: the <b>one</b> callback of Java's
    /// single <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c>
    /// (<c>AlterConsumerGroupOffsetsResult.java:33</c>), resolving the one awaiter with the
    /// per-partition map <b>and</b> the core's own <c>all()</c> outcome.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Routed through <see cref="CompleteRootValueRpc{TValue}"/>, the shared single-awaiter
    /// shell — so the no-throw boundary and the <c>finally</c>'s three obligations (destroy the
    /// root once, <see cref="SingleAdminOperation{TValue}.FailUncompleted"/>,
    /// <see cref="AdminOperation.FreeGcHandle"/>) are stated once, not re-derived here.
    /// <see cref="AlterConsumerGroupOffsetsOutcome"/> is the whole of what is specific to the
    /// RPC.
    /// </para>
    /// <para>
    /// ⚠ The <see cref="AdminOperation.FreeGcHandle"/> in that <c>finally</c> is the
    /// <b>only</b> release of the operation's handle and its span-the-op client reference.
    /// The ABI fires this callback exactly once — also for an empty request and, inline on the
    /// submitting thread, for a request that cannot be submitted — so the submit path must
    /// take no countdown and release no submit token: either would free the handle before
    /// this runs (PLAN R1).
    /// </para>
    /// </remarks>
    private static void OnAlterConsumerGroupOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            AlterConsumerGroupOffsetsOutcome,
            s_destroyAlterConsumerGroupOffsetsResult);

    /// <summary>
    /// <c>deleteConsumerGroupOffsets</c>' trampoline — the same one-shot shell as
    /// <see cref="OnAlterConsumerGroupOffsets"/>, for the same Java return type
    /// (<c>DeleteConsumerGroupOffsetsResult.java:31</c>), over this RPC's own
    /// <see cref="DeleteConsumerGroupOffsetsOutcome"/>.
    /// </summary>
    private static void OnDeleteConsumerGroupOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            DeleteConsumerGroupOffsetsOutcome,
            s_destroyDeleteConsumerGroupOffsetsResult);

    /// <summary>
    /// <c>deleteConsumerGroups</c>' shape-4b trampoline: one awaiter per requested group id,
    /// the same body as <see cref="OnCreatePartitions"/>.
    /// </summary>
    private static void OnDeleteConsumerGroups(IntPtr key, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(key, error, userData, s_stringKey);

    /// <summary>
    /// <c>removeMembersFromConsumerGroup</c>' trampoline — the same one-shot shell as
    /// <see cref="OnAlterConsumerGroupOffsets"/>, for the same kind of Java return type
    /// (<c>KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt;</c>,
    /// <c>RemoveMembersFromConsumerGroupResult.java:35</c>), over this RPC's own
    /// <see cref="RemoveMembersFromConsumerGroupOutcome"/>.
    /// </summary>
    /// <remarks>
    /// It fires once in both modes. In <c>removeAll</c> mode the walked map is empty and the
    /// core's <c>all()</c> is the whole outcome.
    /// </remarks>
    private static void OnRemoveMembersFromConsumerGroup(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteRootValueRpc(
            result,
            error,
            userData,
            RemoveMembersFromConsumerGroupOutcome,
            s_destroyRemoveMembersFromConsumerGroupResult);

    /// <summary>
    /// <c>createAcls</c>' shape-4b trampoline, whose <b>key</b> is owned.
    /// </summary>
    private static void OnCreateAcls(IntPtr binding, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoidOwnedKey(
            binding, error, userData, CreateAclsPerKeyKey, s_destroyAclBinding);

    /// <summary>
    /// <c>alterClientQuotas</c>' <b>shape-4b</b> trampoline: one awaitable per entity, each
    /// carrying only that entity's own outcome, and whose <b>key</b> is owned.
    /// </summary>
    private static void OnAlterClientQuotas(IntPtr entity, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoidOwnedKey(
            entity, error, userData, AlterClientQuotasPerKeyKey, s_destroyClientQuotaEntity);

    /// <summary>
    /// <c>deleteAcls</c>' shape-4a trampoline, and the only one whose <b>key</b> is owned.
    /// </summary>
    /// <remarks>
    /// ⚠ Three owned handles per callback — filter, value, error. The inner
    /// <c>get_error(j)</c> the value reader reads stays borrowed and stays a stored value on
    /// a successfully completed <c>FilterResults</c>; only the top-level error is a fault.
    /// </remarks>
    private static void OnDeleteAcls(IntPtr filter, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKeyOwnedKey(
            filter,
            value,
            error,
            userData,
            DeleteAclsPerKeyKey,
            DeleteAclsFilterResultsPerKeyValue,
            s_destroyDeleteAclsFilterResults,
            s_destroyAclBindingFilter);

    /// <summary>
    /// <c>listPartitionReassignments</c>' shape-3 trampoline: one awaiter over
    /// <c>Map&lt;TopicPartition, PartitionReassignment&gt;</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ It shares a walker with <see cref="OnElectLeaders"/> and <see cref="OnListTopics"/>
    /// for the reason the shape is defined by: Java stores <b>one</b> future
    /// (<c>ListPartitionReassignmentsResult.java:31</c>), so no per-partition outcome
    /// faults anything. Here the ABI agrees visibly — the result declares no
    /// <c>get_error</c> — whereas <c>electLeaders</c> declares one and still belongs. The
    /// Java stored field is what decides both.
    /// </remarks>
    private static void OnListPartitionReassignments(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_listPartitionReassignmentsCount,
            ListPartitionReassignmentsKey,
            PartitionReassignmentValue,
            EqualityComparer<TopicPartition>.Default,
            s_destroyListPartitionReassignmentsResult);

    /// <summary>
    /// <c>listOffsets</c>' shape-1 trampoline: one awaitable per partition, each carrying
    /// that partition's own value or its own borrowed error.
    /// </summary>
    private static void OnListOffsets(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            new PartitionKeySource(topic, partition),
            value,
            error,
            userData,
            s_topicPartitionKey,
            ListOffsetsInfoPerKeyValue,
            s_destroyListOffsetsResultInfo);

    /// <summary>
    /// <c>alterPartitionReassignments</c>' shape-4b trampoline: one awaitable per
    /// partition, faulted by that partition's own owned error.
    /// </summary>
    private static void OnAlterPartitionReassignments(
        IntPtr topic, int partition, IntPtr error, IntPtr userData) =>
        CompletePerKeyVoid(
            new PartitionKeySource(topic, partition), error, userData, s_topicPartitionKey);

    /// <summary>
    /// The <b>shape-5</b> trampoline: one awaiter over four cluster attributes, and no
    /// table to walk.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The callback's <c>error</c> parameter is the ONLY error this RPC can see, and
    /// it is OWNED.</b> There is no per-key <c>get_error</c> anywhere in
    /// <c>kafka_admin_DescribeClusterResult_t</c> — so unlike every keyed RPC bound so far,
    /// there is nothing here that is <em>borrowed</em>, and reaching for
    /// <see cref="KafkaException.FromBorrowedHandle"/> would leak this handle rather than
    /// protect it. <see cref="KafkaException.FromHandle"/> frees it exactly once.
    /// <para>
    /// The <c>finally</c> discharges the same three obligations as every other trampoline:
    /// destroy the owned root (null-safe, so the error branch is a no-op), fault an awaiter
    /// nothing completed, and release the <c>GCHandle</c> plus the span-the-op client
    /// reference. The destroy runs strictly after the copy-out, because every value the
    /// copy-out reads is borrowed from that root.
    /// </para>
    /// </remarks>
    private static void OnDescribeCluster(IntPtr result, IntPtr error, IntPtr userData)
    {
        SingleAdminOperation<DescribeClusterSnapshot>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<DescribeClusterSnapshot>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                context.SetResult(DescribeClusterMarshal.CopyOut(result));
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            NativeMethods.DescribeClusterResultDestroy(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The one completion body both <b>sub-shape-3b</b> trampolines delegate to: one
    /// awaiter over an ordered collection, with no key and no per-key error channel.
    /// </summary>
    /// <remarks>
    /// ⚠ Same ownership asymmetry as <see cref="OnDescribeCluster"/> and
    /// <see cref="OnListTopics"/>: <paramref name="error"/> is <b>OWNED</b> and freed by
    /// <see cref="KafkaException.FromHandle"/>. These result types declare no
    /// <c>get_error</c> at all, so there is no borrowed error anywhere on this path.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="count">That RPC's <c>*Result_count</c>.</param>
    /// <param name="readValue">That RPC's element reader.</param>
    /// <param name="destroyResult">That RPC's <c>*Result_destroy</c>.</param>
    private static void CompleteListRpc<TValue>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        KeyedResultMarshal.CountAccessor count,
        Func<IntPtr, int, TValue> readValue,
        Action<IntPtr> destroyResult)
    {
        SingleAdminOperation<IReadOnlyCollection<TValue>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<IReadOnlyCollection<TValue>>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.CompleteList(result, count, context, readValue);
            }
        }
        catch (Exception exception)
        {
            context?.SetException(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    private static void OnListConfigResources(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteListRpc(
            result,
            error,
            userData,
            s_listConfigResourcesCount,
            ConfigResourceValue,
            s_destroyListConfigResourcesResult);

    /// <summary>
    /// <c>describeAcls</c>' <b>sub-shape-3b</b> trampoline: one awaiter over the matching
    /// bindings, in the broker's own order.
    /// </summary>
    private static void OnDescribeAcls(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteListRpc(
            result,
            error,
            userData,
            s_describeAclsCount,
            DescribeAclsValue,
            s_destroyDescribeAclsResult);

    /// <summary>
    /// <c>describeClientQuotas</c>' <b>shape-3</b> trampoline: one awaiter over the whole
    /// entity-to-quota map.
    /// </summary>
    private static void OnDescribeClientQuotas(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_describeClientQuotasCount,
            DescribeClientQuotasKey,
            DescribeClientQuotasValue,
            EqualityComparer<ClientQuotaEntity>.Default,
            s_destroyDescribeClientQuotasResult);

#pragma warning disable CS0618 // Java deprecates the listing type itself; mirrored, not avoided.
    private static void OnListClientMetricsResources(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteListRpc(
            result,
            error,
            userData,
            s_listClientMetricsResourcesCount,
            ClientMetricsResourceListingValue,
            s_destroyListClientMetricsResourcesResult);
#pragma warning restore CS0618

    /// <summary>
    /// <c>listGroups</c>' <b>sub-shape-3c</b> trampoline: one awaiter over two independent
    /// lists — the listings the responding brokers returned, and the failures the others
    /// reported.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Two errors of opposite ownership meet in this one body.</b>
    /// <paramref name="error"/> — the callback's own parameter, non-const, meaning the RPC
    /// could not be submitted at all — is <b>OWNED</b> and freed by
    /// <see cref="KafkaException.FromHandle"/>. The per-broker errors reached through
    /// <see cref="ListGroupsBrokerError"/> are <b>BORROWED</b> from the result root and must
    /// never be freed. Swapping the two costs a leak in one direction and a double-free
    /// process abort in the other.
    /// </para>
    /// <para>
    /// It has its own body rather than a shared <c>Complete*Rpc</c> helper because it is so
    /// far the only member of its sub-shape — the same call
    /// <see cref="OnDescribeCluster"/> makes — and the direct
    /// <c>ListGroupsResultDestroy</c> in the <c>finally</c> needs no hoisted
    /// <c>Action&lt;IntPtr&gt;</c>. The <c>finally</c> discharges the usual three
    /// obligations: destroy the owned root (null-safe, so the submit-failure branch is a
    /// no-op) strictly after the copy-out, fault an awaiter nothing completed, and release
    /// the <c>GCHandle</c> plus the span-the-op client reference.
    /// </para>
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnListGroups(IntPtr result, IntPtr error, IntPtr userData)
    {
        SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>?
            context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context =
                (SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>)
                handle.Target!;

            if (error != IntPtr.Zero)
            {
                // ⚠ OWNED — FromHandle frees it exactly once (the mirror image of the
                // per-broker errors inside the result, which are borrowed).
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.CompleteTwoLists(
                    result,
                    s_listGroupsValidCount,
                    s_listGroupsErrorCount,
                    context,
                    GroupListingValue,
                    ListGroupsBrokerError);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            NativeMethods.ListGroupsResultDestroy(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

#pragma warning disable CS0618 // Java deprecates the listing type itself; mirrored, not avoided.

    /// <summary>
    /// <c>listConsumerGroups</c>' <b>sub-shape-3c</b> trampoline: one awaiter over two
    /// independent lists — the listings the responding brokers returned, and the failures
    /// the others reported.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Two errors of opposite ownership meet in this one body</b>, exactly as they do
    /// in <see cref="OnListGroups"/>. <paramref name="error"/> — the callback's own
    /// parameter, non-const, meaning the RPC could not be submitted at all — is
    /// <b>OWNED</b> and freed by <see cref="KafkaException.FromHandle"/>. The per-broker
    /// errors reached through <see cref="ListConsumerGroupsBrokerError"/> are
    /// <b>BORROWED</b> from the result root and must never be freed. Swapping the two costs
    /// a leak in one direction and a double-free process abort in the other.
    /// </para>
    /// <para>
    /// ⚠ The <c>finally</c> destroys <b>this</b> RPC's root — a <c>listGroups</c> result and
    /// a <c>listConsumerGroups</c> result are different native types, so the neighbouring
    /// <c>ListGroupsResultDestroy</c> is not interchangeable with this one although the two
    /// bodies are otherwise line-for-line. It discharges the usual three obligations:
    /// destroy the owned root (null-safe, so the submit-failure branch is a no-op) strictly
    /// after the copy-out, fault an awaiter nothing completed, and release the
    /// <c>GCHandle</c> plus the span-the-op client reference.
    /// </para>
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnListConsumerGroups(IntPtr result, IntPtr error, IntPtr userData)
    {
        SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing> Valid,
            IReadOnlyCollection<KafkaException> Errors)>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context =
                (SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing> Valid,
                    IReadOnlyCollection<KafkaException> Errors)>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                // ⚠ OWNED — FromHandle frees it exactly once (the mirror image of the
                // per-broker errors inside the result, which are borrowed).
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.CompleteTwoLists(
                    result,
                    s_listConsumerGroupsValidCount,
                    s_listConsumerGroupsErrorCount,
                    context,
                    ConsumerGroupListingValue,
                    ListConsumerGroupsBrokerError);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            NativeMethods.ListConsumerGroupsResultDestroy(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

#pragma warning restore CS0618

    /// <summary>
    /// <c>describeConsumerGroups</c>' <b>shape-4a</b> trampoline: one callback per group,
    /// resolving that group's own future with its description or faulting it with its own
    /// error.
    /// </summary>
    /// <remarks>
    /// ⚠ There is one error direction here, not two: <paramref name="error"/> is this
    /// <em>key's</em> outcome and is <b>OWNED</b>, so it goes through
    /// <c>KafkaException.FromHandle</c> — the inverse of the borrowed per-key errors the
    /// shape-1 walk read out of a result root.
    /// </remarks>
    /// <param name="key">The borrowed group id.</param>
    /// <param name="value">This group's owned description, or <c>IntPtr.Zero</c>.</param>
    /// <param name="error">This group's owned error, or <c>IntPtr.Zero</c>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnDescribeConsumerGroups(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            ConsumerGroupDescriptionPerKeyValue,
            s_destroyConsumerGroupDescription);

    /// <summary>
    /// Copies one borrowed <c>ConsumerGroupDescription_t</c> out into an owned
    /// <see cref="ConsumerGroupDescription"/>. Frees nothing: the caller's result root owns
    /// every pointer reached from here and destroys it after this returns.
    /// </summary>
    /// <param name="description">The borrowed <c>get_value(i)</c> pointer.</param>
    private static ConsumerGroupDescription CopyOutConsumerGroupDescription(IntPtr description)
    {
        if (description == IntPtr.Zero)
        {
            // Unreachable on this path: the walker reads get_error first and only calls the
            // value reader when that key reported no error. Faulting just this key is the
            // safe reading if the ABI ever disagrees.
            throw new KafkaException(
                "The describeConsumerGroups result produced no description for a group that "
                + "reported no error.");
        }

        string groupId =
            Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupDescriptionGroupId(description))
            ?? throw new KafkaException(
                "The describeConsumerGroups result produced a description with no group id.");

        int memberCount = NativeMethods.ConsumerGroupDescriptionMemberCount(description);
        List<MemberDescription> members = new List<MemberDescription>(Math.Max(memberCount, 0));
        for (int index = 0; index < memberCount; index++)
        {
            IntPtr member = NativeMethods.ConsumerGroupDescriptionGetMember(description, index);
            if (member == IntPtr.Zero)
            {
                // Guarded by `member_count`, so unreachable; skipping is the safe reading.
                continue;
            }

            members.Add(CopyOutMemberDescription(member));
        }

        // ⚠ Non-optional on this class — a null is an ABI contract violation, not an
        // absence, and reads as Unknown because the managed properties are non-nullable.
        GroupType type =
            GroupMarshal.TypeFromName(NativeMethods.ConsumerGroupDescriptionGroupType(description))
            ?? GroupType.Unknown;
        GroupState groupState =
            GroupMarshal.StateFromName(NativeMethods.ConsumerGroupDescriptionGroupState(description))
            ?? GroupState.Unknown;

        // Java's authorizedOperations(): null when the broker reported no set at all, a
        // (possibly empty) owned collection when it did — the gate, not the count.
        IReadOnlyCollection<AclOperation>? authorizedOperations = AuthorizedOperationsMarshal.CopyOut(
            description,
            s_consumerGroupHasAuthorizedOperations,
            s_consumerGroupAuthorizedOperationCount,
            s_consumerGroupAuthorizedOperation);

        // ⚠ The RETURN is the presence signal; a negative epoch written to the out-param is
        // a present value, so absence cannot be read off the value.
        int? groupEpoch =
            NativeMethods.ConsumerGroupDescriptionGroupEpoch(description, out int epoch)
                ? epoch
                : (int?)null;
        int? targetAssignmentEpoch =
            NativeMethods.ConsumerGroupDescriptionTargetAssignmentEpoch(description, out int targetEpoch)
                ? targetEpoch
                : (int?)null;

        return new ConsumerGroupDescription(
            groupId,
            NativeMethods.ConsumerGroupDescriptionIsSimpleConsumerGroup(description),
            members,
            Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupDescriptionPartitionAssignor(description)),
            type,
            groupState,
            NodeMarshal.CopyOut(NativeMethods.ConsumerGroupDescriptionCoordinator(description)),
            authorizedOperations,
            groupEpoch,
            targetAssignmentEpoch);
    }

    /// <summary>
    /// Copies one borrowed <c>MemberDescription_t</c> out. Frees nothing — see
    /// <see cref="CopyOutConsumerGroupDescription"/>.
    /// </summary>
    /// <param name="member">The borrowed <c>get_member(i)</c> pointer.</param>
    private static MemberDescription CopyOutMemberDescription(IntPtr member)
    {
        // ⚠ groupInstanceId and rackId stay null when absent — Java's accessors are
        // nullable there, unlike consumerId / clientId / host, which the constructor
        // coalesces to the empty string.
        // ⚠ upgraded has two booleans of different meaning: the RETURN is presence, the
        // out-param is the value, so Optional.of(false) and Optional.empty() are distinct.
        return new MemberDescription(
            Utf8Marshal.PtrToString(NativeMethods.MemberDescriptionConsumerId(member)),
            Utf8Marshal.PtrToString(NativeMethods.MemberDescriptionGroupInstanceId(member)),
            Utf8Marshal.PtrToString(NativeMethods.MemberDescriptionRackId(member)),
            Utf8Marshal.PtrToString(NativeMethods.MemberDescriptionClientId(member)),
            Utf8Marshal.PtrToString(NativeMethods.MemberDescriptionHost(member)),
            CopyOutMemberAssignment(NativeMethods.MemberDescriptionAssignment(member)),
            CopyOutMemberAssignment(NativeMethods.MemberDescriptionTargetAssignment(member)),
            NativeMethods.MemberDescriptionMemberEpoch(member, out int epoch) ? epoch : (int?)null,
            NativeMethods.MemberDescriptionUpgraded(member, out bool upgraded) ? upgraded : (bool?)null);
    }

    /// <summary>
    /// Copies one borrowed <c>MemberAssignment_t</c> out, or returns <see langword="null"/>
    /// for a null pointer — Java's <c>Optional.empty()</c> on <c>targetAssignment()</c>.
    /// The constructor coalesces a null <c>assignment()</c> to an empty one, so a defensive
    /// null there stays faithful.
    /// </summary>
    /// <param name="assignment">The borrowed assignment pointer.</param>
    private static MemberAssignment? CopyOutMemberAssignment(IntPtr assignment)
    {
        if (assignment == IntPtr.Zero)
        {
            return null;
        }

        int count = NativeMethods.MemberAssignmentCount(assignment);
        List<TopicPartition> topicPartitions = new List<TopicPartition>(Math.Max(count, 0));
        for (int index = 0; index < count; index++)
        {
            // NUL-terminated, borrowed (ffi §B3 row 2) — copied out here, never NUL-scanned
            // past its own terminator.
            string? topic = Utf8Marshal.PtrToString(NativeMethods.MemberAssignmentGetTopic(assignment, index));
            if (topic is null)
            {
                // Guarded by `count`, so unreachable; skipping is the safe reading.
                continue;
            }

            topicPartitions.Add(
                new TopicPartition(topic, NativeMethods.MemberAssignmentGetPartition(assignment, index)));
        }

        return new MemberAssignment(topicPartitions);
    }

    /// <summary>
    /// <c>describeClassicGroups</c>' <b>shape-4a</b> trampoline: one callback per group.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="value"/> is destroyed with
    /// <c>kafka_admin_ClassicGroupDescription_destroy</c> — not
    /// <c>describeConsumerGroups</c>' namesake, which would free the wrong native type.
    /// See <see cref="OnDescribeConsumerGroups"/> on the owned per-key error.
    /// </remarks>
    /// <param name="key">The borrowed group id.</param>
    /// <param name="value">This group's owned description, or <c>IntPtr.Zero</c>.</param>
    /// <param name="error">This group's owned error, or <c>IntPtr.Zero</c>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnDescribeClassicGroups(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            ClassicGroupDescriptionPerKeyValue,
            s_destroyClassicGroupDescription);

    /// <summary>
    /// Copies one borrowed <c>ClassicGroupDescription_t</c> out into an owned
    /// <see cref="ClassicGroupDescription"/>. Frees nothing: the caller's result root owns
    /// every pointer reached from here and destroys it after this returns.
    /// </summary>
    /// <param name="description">The borrowed <c>get_value(i)</c> pointer.</param>
    private static ClassicGroupDescription CopyOutClassicGroupDescription(IntPtr description)
    {
        if (description == IntPtr.Zero)
        {
            // Unreachable on this path: the walker reads get_error first and only calls the
            // value reader when that key reported no error. Faulting just this key is the
            // safe reading if the ABI ever disagrees.
            throw new KafkaException(
                "The describeClassicGroups result produced no description for a group that "
                + "reported no error.");
        }

        int memberCount = NativeMethods.ClassicGroupDescriptionMemberCount(description);
        List<MemberDescription> members = new List<MemberDescription>(Math.Max(memberCount, 0));
        for (int index = 0; index < memberCount; index++)
        {
            IntPtr member = NativeMethods.ClassicGroupDescriptionGetMember(description, index);
            if (member == IntPtr.Zero)
            {
                // Guarded by `member_count`, so unreachable; skipping is the safe reading.
                continue;
            }

            // The same kafka_admin_MemberDescription_t describeConsumerGroups walks, read by
            // that RPC's copy-out rather than a second one: a member this class holds and a
            // member that one holds are one native type, and two readers could drift.
            members.Add(CopyOutMemberDescription(member));
        }

        // ⚠ One state accessor on this class, and it is a ClassicGroupState — not the
        // GroupState / deprecated-projection pair ConsumerGroupDescription carries. An
        // unrecognised or null name reads as Unknown, because the managed property is a
        // non-nullable enum with no null to store.
        ClassicGroupState state =
            GroupMarshal.ClassicStateFromName(NativeMethods.ClassicGroupDescriptionState(description))
            ?? ClassicGroupState.Unknown;

        // Java's authorizedOperations(): null when the broker reported no set at all, a
        // (possibly empty) owned collection when it did — the gate, not the count. Passed to
        // the SEVEN-argument constructor precisely so absence survives: the six-argument
        // overload forwards Set.of() (ClassicGroupDescription.java:48), which would render
        // "never asked" as "asked, none authorized".
        IReadOnlyCollection<AclOperation>? authorizedOperations = AuthorizedOperationsMarshal.CopyOut(
            description,
            s_classicGroupHasAuthorizedOperations,
            s_classicGroupAuthorizedOperationCount,
            s_classicGroupAuthorizedOperation);

        // ⚠ No is_simple_consumer_group read: Java derives isSimpleConsumerGroup() from
        // protocol (:113) and so does the managed class, so protocol is the only input and
        // there is no second axis to disagree on. ⚠ protocol stays null when absent — it is
        // the one string Java does not coalesce (:59) — while groupId and protocolData are
        // coalesced by the constructor exactly as Java coalesces them (:58, :60).
        return new ClassicGroupDescription(
            Utf8Marshal.PtrToString(NativeMethods.ClassicGroupDescriptionGroupId(description)),
            Utf8Marshal.PtrToString(NativeMethods.ClassicGroupDescriptionProtocol(description)),
            Utf8Marshal.PtrToString(NativeMethods.ClassicGroupDescriptionProtocolData(description)),
            members,
            state,
            NodeMarshal.CopyOut(NativeMethods.ClassicGroupDescriptionCoordinator(description)),
            authorizedOperations);
    }

    /// <summary>
    /// <c>listConsumerGroupOffsets</c>' <b>shape-4a</b> trampoline: one callback per group,
    /// carrying that group's committed offsets or its own error.
    /// </summary>
    /// <remarks>
    /// See <see cref="OnDescribeConsumerGroups"/> on the owned per-key error. The offsets
    /// map is owned too and is destroyed after the copy-out completes.
    /// </remarks>
    /// <param name="key">The borrowed group id.</param>
    /// <param name="value">This group's owned offsets map, or <c>IntPtr.Zero</c>.</param>
    /// <param name="error">This group's owned error, or <c>IntPtr.Zero</c>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnListConsumerGroupOffsets(
        IntPtr key, IntPtr value, IntPtr error, IntPtr userData) =>
        CompletePerKey(
            key,
            value,
            error,
            userData,
            s_stringKey,
            ListConsumerGroupOffsetsPerKeyValue,
            s_destroyOffsetAndMetadataMap);

    /// <summary>
    /// Copies one group's <b>borrowed</b> <c>kafka_admin_OffsetAndMetadataMap_t</c> into an
    /// owned managed dictionary, entry by entry. Nothing native-backed is retained, and the
    /// borrowed map is never freed here — the result root that owns it is destroyed by the
    /// trampoline, strictly after this returns.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Not <see cref="OffsetMapMarshal"/>.</b> That marshaller reads the consumer's
    /// <c>kafka_consumer_OffsetMap_t</c>, whose entries are borrowed child handles and whose
    /// value cannot be null; this map is flat and index-addressed, and its value <em>is</em>
    /// nullable. The two ABIs are unrelated and neither reader can decode the other. The one
    /// piece deliberately shared is
    /// <see cref="OffsetMapMarshalShared.ReadLeaderEpoch"/> — the presence-pair rule every
    /// offset map on this surface obeys, stated once so no map can decode it differently.
    /// </remarks>
    /// <param name="map">The borrowed map, or <c>IntPtr.Zero</c>.</param>
    /// <returns>The group's committed offsets keyed by partition; empty when the map is
    /// null or carries no entries.</returns>
    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> CopyOutOffsetAndMetadataMap(
        IntPtr map) =>
        CopyOutOffsetAndMetadataMap(map, s_offsetAndMetadataMapAccessors);

    /// <summary>
    /// The walk itself, over an injectable accessor set — production passes
    /// <see cref="s_offsetAndMetadataMapAccessors"/>, which binds the six
    /// <c>kafka_admin_OffsetAndMetadataMap_*</c> entry points.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Absent is not zero, and absent is not missing.</b> Java's map value is nullable:
    /// a requested partition the group has never committed for is reported <em>present with
    /// a null <c>OffsetAndMetadata</c></em>. So there are three states — not in the map, in
    /// the map with a null value, in the map with an offset — and this walk must preserve
    /// all three. An entry whose <c>has_offset</c> is <see langword="false"/> is
    /// <b>still added</b>, with a <see langword="null"/> value; dropping it would silently
    /// merge the first two.
    /// </para>
    /// <para>
    /// ⚠ <c>has_offset</c> is the gate and is read <b>first</b>. When it is
    /// <see langword="false"/> the other three accessors return fillers — <c>-1</c>, null,
    /// <see langword="false"/> — and are <b>not consulted at all</b>: <c>-1</c> is a
    /// sentinel, committed offsets are never negative, and
    /// <see cref="OffsetAndMetadata"/>'s constructor rejects a negative offset, so letting
    /// the filler through would fault the whole group's future over one uncommitted
    /// partition.
    /// </para>
    /// <para>
    /// The accessor set is injectable for the same reason
    /// <see cref="AuthorizedOperationsMarshal.CopyOut"/>'s is: the rule under test is
    /// "which accessor is consulted, and when", and a test cannot fabricate a native map to
    /// ask it of.
    /// </para>
    /// </remarks>
    /// <param name="map">The borrowed map, or <c>IntPtr.Zero</c>.</param>
    /// <param name="accessors">The six map accessors.</param>
    /// <returns>The group's committed offsets keyed by partition.</returns>
    internal static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> CopyOutOffsetAndMetadataMap(
        IntPtr map,
        OffsetAndMetadataMapAccessors accessors)
    {
        int count = map == IntPtr.Zero ? 0 : accessors.Count(map);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>.Instance;
        }

        Dictionary<TopicPartition, OffsetAndMetadata?> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata?>(count);
        for (int index = 0; index < count; index++)
        {
            TopicPartition partition = new TopicPartition(
                Utf8Marshal.PtrToString(accessors.GetTopic(map, index)) ?? string.Empty,
                accessors.GetPartition(map, index));

            // ⚠ The gate, and nothing else, decides — see the remarks. An uncommitted
            // partition is listed with a null value, never dropped and never given -1.
            offsets[partition] = accessors.HasOffset(map, index)
                ? new OffsetAndMetadata(
                    accessors.GetOffset(map, index),
                    // Java normalises an absent metadata string to "", and so does the
                    // constructor — copied out here, before the root destroy (§B3).
                    Utf8Marshal.PtrToString(accessors.GetMetadata(map, index)),
                    // ⚠ A presence pair, so it becomes nullable — never a sentinel.
                    OffsetMapMarshalShared.ReadLeaderEpoch(
                        accessors.GetLeaderEpoch(map, index, out int epoch), epoch))
                : null;
        }

        return offsets;
    }

    /// <summary>
    /// The <c>kafka_admin_OffsetAndMetadataMap_get_leader_epoch</c> shape: a presence pair,
    /// which no <see cref="Func{T1, T2, TResult}"/> can express because of the
    /// <see langword="out"/> parameter.
    /// </summary>
    /// <param name="map">The borrowed map.</param>
    /// <param name="index">The entry to read.</param>
    /// <param name="epoch">The epoch, written only when this returns <see langword="true"/>.</param>
    /// <returns>Whether the entry has a leader epoch at all.</returns>
    internal delegate bool LeaderEpochAccessor(IntPtr map, int index, out int epoch);

    /// <summary>
    /// The six <c>kafka_admin_OffsetAndMetadataMap_t</c> accessors, bundled so the walk can
    /// be driven over a synthetic map. Built once as a <c>static readonly</c> field (method
    /// groups bind straight to the delegate types), so a completion allocates none of them.
    /// </summary>
    internal sealed class OffsetAndMetadataMapAccessors
    {
        /// <summary>Creates an accessor set for one offset map.</summary>
        /// <param name="count"><c>_count</c>.</param>
        /// <param name="getTopic"><c>_get_topic</c>.</param>
        /// <param name="getPartition"><c>_get_partition</c>.</param>
        /// <param name="hasOffset"><c>_has_offset</c> — the gate.</param>
        /// <param name="getOffset"><c>_get_offset</c>.</param>
        /// <param name="getMetadata"><c>_get_metadata</c>.</param>
        /// <param name="getLeaderEpoch"><c>_get_leader_epoch</c>.</param>
        internal OffsetAndMetadataMapAccessors(
            Func<IntPtr, int> count,
            Func<IntPtr, int, IntPtr> getTopic,
            Func<IntPtr, int, int> getPartition,
            Func<IntPtr, int, bool> hasOffset,
            Func<IntPtr, int, long> getOffset,
            Func<IntPtr, int, IntPtr> getMetadata,
            LeaderEpochAccessor getLeaderEpoch)
        {
            Count = count;
            GetTopic = getTopic;
            GetPartition = getPartition;
            HasOffset = hasOffset;
            GetOffset = getOffset;
            GetMetadata = getMetadata;
            GetLeaderEpoch = getLeaderEpoch;
        }

        /// <summary>How many partition entries the map carries.</summary>
        internal Func<IntPtr, int> Count { get; }

        /// <summary>The entry's topic — borrowed NUL-terminated UTF-8.</summary>
        internal Func<IntPtr, int, IntPtr> GetTopic { get; }

        /// <summary>The entry's partition.</summary>
        internal Func<IntPtr, int, int> GetPartition { get; }

        /// <summary>
        /// ⚠ <b>The gate.</b> <see langword="false"/> means the partition is listed with no
        /// committed offset, and the three accessors below must not be consulted.
        /// </summary>
        internal Func<IntPtr, int, bool> HasOffset { get; }

        /// <summary>The entry's committed offset — only once <see cref="HasOffset"/> holds.</summary>
        internal Func<IntPtr, int, long> GetOffset { get; }

        /// <summary>The entry's metadata — only once <see cref="HasOffset"/> holds.</summary>
        internal Func<IntPtr, int, IntPtr> GetMetadata { get; }

        /// <summary>The entry's leader epoch, as a presence pair.</summary>
        internal LeaderEpochAccessor GetLeaderEpoch { get; }
    }
}
