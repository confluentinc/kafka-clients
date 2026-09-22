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
    /// <c>void (*)(kafka_admin_CreateTopicsResult_t* result,
    /// kafka_common_KafkaError_t* error, void* user_data)</c>. Exactly one of the two is
    /// non-null and the callback owns it. ⚠ A <b>per-topic</b> failure arrives inside
    /// <paramref name="result"/>, not as <paramref name="error"/>: a non-null
    /// <paramref name="error"/> means the request could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreateTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_topics_callback_t</c>:
    /// <c>void (*)(kafka_admin_DeleteTopicsResult_t* result,
    /// kafka_common_KafkaError_t* error, void* user_data)</c>. Shared by <b>both</b>
    /// delete entry points — the by-name and the by-id one — because they produce the same
    /// result type. ⚠ A <b>per-topic</b> failure arrives inside
    /// <paramref name="result"/>; a non-null <paramref name="error"/> means the request
    /// could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_topics_callback_t</c>,
    /// shared by both describe entry points for the same reason as
    /// <see cref="DeleteTopicsCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_list_topics_callback_t</c>.
    /// ⚠ Unlike every other admin callback here, <paramref name="error"/> is the
    /// <b>only</b> failure channel: the result type has no <c>get_error</c>, so there is
    /// no per-topic failure to carry (result shape 3).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_partitions_callback_t</c>.
    /// ⚠ A <b>per-topic</b> failure arrives inside <paramref name="result"/>; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreatePartitionsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_records_callback_t</c>.
    /// ⚠ A <b>per-partition</b> failure arrives inside <paramref name="result"/>; a
    /// non-null <paramref name="error"/> means the request could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteRecordsCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// The C signature for <c>kafka_admin_AdminClient_describe_configs_callback_t</c>.
    /// ⚠ A <b>per-resource</b> failure arrives inside <paramref name="result"/>, borrowed;
    /// a non-null <paramref name="error"/> means the request could not be submitted at all
    /// and is owned.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeConfigsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_incremental_alter_configs_callback_t</c>.
    /// ⚠ A <b>per-resource</b> failure arrives inside <paramref name="result"/>; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all — which for
    /// this RPC includes an <b>unknown op-type code</b>, delivered on the inline path.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void IncrementalAlterConfigsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_log_dirs_callback_t</c>.
    /// ⚠ A <b>per-broker</b> failure arrives inside <paramref name="result"/>, borrowed —
    /// and so does a <b>per-log-directory</b> one, which is a different thing that does not
    /// fault anything. A non-null <paramref name="error"/> is owned.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeLogDirsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_alter_replica_log_dirs_callback_t</c>. ⚠ A
    /// <b>per-replica</b> failure arrives inside <paramref name="result"/>, borrowed.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterReplicaLogDirsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <inheritdoc cref="AlterReplicaLogDirsCallback"/>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeReplicaLogDirsCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// <c>kafka_admin_AdminClient_alter_consumer_group_offsets_callback_t</c> (result
    /// shape 3 — one aggregate future over the map, the same shape as
    /// <see cref="ElectLeadersCallback"/>).
    /// ⚠⚠ <paramref name="error"/> is the <b>only</b> failure channel here, even though
    /// <c>kafka_admin_AlterConsumerGroupOffsetsResult_t</c> does declare a <c>get_error</c>:
    /// that accessor carries the map's per-partition <em>value</em>
    /// (<see cref="AlterConsumerGroupOffsetsOptionalError"/>), not a failure. A non-null
    /// <paramref name="error"/> means the request could not be submitted at all, and is
    /// <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterConsumerGroupOffsetsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_delete_consumer_group_offsets_callback_t</c> (result
    /// shape 3 — one aggregate future over the map, the same shape as
    /// <see cref="AlterConsumerGroupOffsetsCallback"/>).
    /// ⚠⚠ <paramref name="error"/> is the <b>only</b> failure channel here, even though
    /// <c>kafka_admin_DeleteConsumerGroupOffsetsResult_t</c> does declare a <c>get_error</c>:
    /// that accessor carries the map's per-partition <em>value</em>
    /// (<see cref="DeleteConsumerGroupOffsetsOptionalError"/>), not a failure. A non-null
    /// <paramref name="error"/> means the request could not be submitted at all, and is
    /// <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteConsumerGroupOffsetsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_alter_partition_reassignments_callback_t</c> (result
    /// shape 2). ⚠ A <b>per-partition</b> failure arrives inside
    /// <paramref name="result"/>, borrowed; a non-null <paramref name="error"/> means the
    /// request could not be submitted at all — which for this RPC includes a
    /// <b>non-cancelled entry with no target replicas</b>, delivered on the inline path —
    /// and is owned.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterPartitionReassignmentsCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// The C signature for <c>kafka_admin_AdminClient_list_offsets_callback_t</c> (result
    /// shape 1). ⚠ A <b>per-partition</b> failure arrives inside
    /// <paramref name="result"/>, borrowed; a non-null <paramref name="error"/> means the
    /// request could not be submitted at all — which for this RPC includes an unknown
    /// isolation level or an unrecognised offset sentinel, both delivered on the inline
    /// path — and is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListOffsetsCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// <c>kafka_admin_AdminClient_describe_consumer_groups_callback_t</c> (result shape 1).
    /// ⚠ A <b>per-group</b> failure arrives inside <paramref name="result"/>
    /// (<c>kafka_admin_DescribeConsumerGroupsResult_get_error</c>), borrowed; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all and is
    /// <b>owned</b>.
    /// </summary>
    /// <remarks>
    /// Its own delegate type, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>: each is the managed spelling of one C
    /// typedef, and the result roots they carry are different native types destroyed by
    /// different functions. Sharing one would let a <c>describeConsumerGroups</c> root
    /// reach another RPC's destroy.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeConsumerGroupsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_describe_classic_groups_callback_t</c> (result shape 1).
    /// ⚠ A <b>per-group</b> failure arrives inside <paramref name="result"/>
    /// (<c>kafka_admin_DescribeClassicGroupsResult_get_error</c>), borrowed; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all and is
    /// <b>owned</b>.
    /// </summary>
    /// <remarks>
    /// Its own delegate type, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>: each is the managed spelling of one C
    /// typedef, and the result roots they carry are different native types destroyed by
    /// different functions. Sharing the structurally identical
    /// <see cref="DescribeConsumerGroupsCallback"/> would let a <c>describeClassicGroups</c>
    /// root reach <c>kafka_admin_DescribeConsumerGroupsResult_destroy</c>.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeClassicGroupsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_list_consumer_group_offsets_callback_t</c> (result shape 1).
    /// ⚠ A <b>per-group</b> failure arrives inside <paramref name="result"/>
    /// (<c>kafka_admin_ListConsumerGroupOffsetsResult_get_error</c>), borrowed; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all and is
    /// <b>owned</b>.
    /// </summary>
    /// <remarks>
    /// Its own delegate type, for the reason stated on
    /// <see cref="ListConsumerGroupsCallback"/>: each is the managed spelling of one C
    /// typedef, and the result roots they carry are different native types destroyed by
    /// different functions. Sharing a structurally identical sibling would let a
    /// <c>listConsumerGroupOffsets</c> root reach some other RPC's destroy.
    /// </remarks>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListConsumerGroupOffsetsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_consumer_groups_callback_t</c>
    /// (result shape 2, like <see cref="DeleteTopicsCallback"/>). ⚠ A <b>per-group</b>
    /// failure arrives inside <paramref name="result"/>, borrowed; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all and is
    /// <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteConsumerGroupsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for
    /// <c>kafka_admin_AdminClient_remove_members_from_consumer_group_callback_t</c> (result
    /// shape 3 — one aggregate future over the whole map, the same shape as
    /// <see cref="AlterConsumerGroupOffsetsCallback"/>).
    /// ⚠⚠ <paramref name="error"/> is the <b>only</b> failure channel here, even though
    /// <c>kafka_admin_RemoveMembersFromConsumerGroupResult_t</c> does declare a
    /// <c>get_error</c>: that accessor carries the map's per-member <em>value</em>
    /// (<see cref="RemoveMembersFromConsumerGroupOptionalError"/>), not a failure. A
    /// non-null <paramref name="error"/> means the request could not be submitted at all,
    /// and is <b>owned</b>. In <b>removeAll</b> mode the result handle always has zero
    /// rows — see <see cref="NativeMethods.AdminClientRemoveMembersFromConsumerGroupAsync"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void RemoveMembersFromConsumerGroupCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_acls_callback_t</c>.
    /// ⚠ A <b>per-binding</b> failure arrives inside <paramref name="result"/>, borrowed; a
    /// non-null <paramref name="error"/> means the request could not be submitted at all and
    /// is <b>owned</b>. This callback reaches the inline path for ordinary bad input — see
    /// <see cref="NativeMethods.AdminClientCreateAclsAsync"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreateAclsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_acls_callback_t</c>.
    /// ⚠ A <b>per-filter</b> failure arrives inside <paramref name="result"/>, borrowed; a
    /// non-null <paramref name="error"/> means the request could not be submitted at all and
    /// is <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteAclsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_acls_callback_t</c>.
    /// ⚠ <c>describeAcls</c> has a <b>single</b> future for the whole call, so <em>any</em>
    /// failure arrives as <paramref name="error"/>, <b>owned</b>; the result declares no
    /// <c>get_error</c> at all (<c>confluent_kafka.h:1085-1092</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeAclsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_client_quotas_callback_t</c>.
    /// ⚠ <c>describeClientQuotas</c> has a <b>single</b> future for the whole call, so
    /// <em>any</em> failure arrives as <paramref name="error"/>, <b>owned</b>; the result
    /// declares no <c>get_error</c> at all (<c>confluent_kafka.h:1110-1117</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeClientQuotasCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_alter_client_quotas_callback_t</c>.
    /// ⚠ A <b>per-entity</b> failure arrives inside <paramref name="result"/>, borrowed; a
    /// non-null <paramref name="error"/> means the request could not be submitted at all and
    /// is <b>owned</b> (<c>confluent_kafka.h:1123-1131</c>).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterClientQuotasCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// <c>createTopics</c>' universal accessors — <c>count</c> and the borrowed per-key
    /// <c>get_error</c>. Built once, so walking a result allocates no delegates.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so a test can walk a real result with the <b>same</b>
    /// accessor set production uses (<c>definition-of-done.md</c> §12): a test that
    /// assembled its own could keep passing after production started pointing at a
    /// different function.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors CreateTopicsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.CreateTopicsResultCount,
            NativeMethods.CreateTopicsResultGetError);

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
    /// key seam exists for, and which three RPCs now share.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>One body instead of three copies, for the same testability reason as
    /// <see cref="BorrowedOptionalError"/>.</b> <c>deleteRecords</c> and
    /// <c>alterPartitionReassignments</c> both drive this body against a real result root,
    /// so a defect in the composition — a missing
    /// <see cref="KeyedResultMarshal.ReadStringKey"/>, a transposed pair — is caught there
    /// and therefore for <c>electLeaders</c> too. What that still does not reach is
    /// <em>which accessors</em> <c>electLeaders</c>' instance is built on; that is pinned
    /// structurally by <c>AdminP4ReaderWiringTests</c>.
    /// </remarks>
    /// <param name="getTopic">That RPC's <c>*Result_get_topic</c>, borrowed and NUL-terminated.</param>
    /// <param name="getPartition">That RPC's <c>*Result_get_partition</c>.</param>
    /// <returns>The reader.</returns>
    internal static Func<IntPtr, int, TopicPartition> TopicPartitionKey(
        KeyedResultMarshal.IndexedAccessor getTopic, Func<IntPtr, int, int> getPartition) =>
        (result, index) => new TopicPartition(
            KeyedResultMarshal.ReadStringKey(getTopic(result, index)), getPartition(result, index));

    /// <summary>
    /// <c>createTopics</c>' key reader — Java keys this result by topic <b>name</b>
    /// (<c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt; values()</c>), so the borrowed
    /// <c>get_key(i)</c> string is the key with no parsing. Hoisted for the same reason
    /// as <see cref="CreateTopicsAccessors"/>: no delegate is allocated per walk.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> CreateTopicsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.CreateTopicsResultGetKey(result, index));

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
    /// <c>deleteTopics</c>' universal accessors. Result <b>shape 2</b>: the ABI declares
    /// no <c>DeleteTopicsResult_get_value</c>, because Java's per-key future is
    /// <c>KafkaFuture&lt;Void&gt;</c> and a null error <em>is</em> the success value —
    /// which is stated by routing through the value-less <c>Complete</c> overload, not by
    /// nulling anything here.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DeleteTopicsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DeleteTopicsResultCount,
            NativeMethods.DeleteTopicsResultGetError);

    /// <summary>
    /// <c>describeTopics</c>' universal accessors — result shape 1 (a value per key).
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DescribeTopicsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeTopicsResultCount,
            NativeMethods.DescribeTopicsResultGetError);

    /// <summary>
    /// <c>deleteTopics</c>' by-<b>name</b> key reader: the borrowed <c>get_key(i)</c>
    /// string is the key, as it is for <c>createTopics</c>.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> DeleteTopicsNameKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.DeleteTopicsResultGetKey(result, index));

    /// <summary>
    /// <c>deleteTopics</c>' by-<b>id</b> key reader — the other half of the base64
    /// topic-id round trip. The header is explicit that "result keys are the same base64"
    /// strings the request supplied, so the key is <c>get_key(i)</c> parsed back through
    /// <see cref="Uuid.Parse"/>; a caller who passed <c>Uuid</c>s gets <c>Uuid</c>s back.
    /// This is what <c>KeyedAdminOperation</c>'s generic key exists for.
    /// </summary>
    internal static readonly Func<IntPtr, int, Uuid> DeleteTopicsIdKey =
        static (result, index) =>
            Uuid.Parse(KeyedResultMarshal.ReadStringKey(NativeMethods.DeleteTopicsResultGetKey(result, index)));

    /// <inheritdoc cref="DeleteTopicsNameKey"/>
    internal static readonly Func<IntPtr, int, string> DescribeTopicsNameKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeTopicsResultGetKey(result, index));

    /// <inheritdoc cref="DeleteTopicsIdKey"/>
    internal static readonly Func<IntPtr, int, Uuid> DescribeTopicsIdKey =
        static (result, index) =>
            Uuid.Parse(KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeTopicsResultGetKey(result, index)));

    /// <summary>
    /// The per-key value reader for <c>describeTopics</c>, hoisted for the same reason
    /// as <see cref="TopicMetadataAndConfigValue"/>.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicDescription> TopicDescriptionValue =
        static (result, index) =>
            TopicDescriptionMarshal.CopyOut(NativeMethods.DescribeTopicsResultGetValue(result, index));

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
    /// <c>createPartitions</c>' universal accessors — result <b>shape 2</b>, like
    /// <c>deleteTopics</c>: Java's per-topic future is <c>KafkaFuture&lt;Void&gt;</c> and
    /// the ABI declares no <c>_get_value</c>.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors CreatePartitionsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.CreatePartitionsResultCount,
            NativeMethods.CreatePartitionsResultGetError);

    /// <summary><c>createPartitions</c>' key reader — the topic name.</summary>
    internal static readonly Func<IntPtr, int, string> CreatePartitionsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.CreatePartitionsResultGetKey(result, index));

    /// <summary>
    /// <c>deleteRecords</c>' universal accessors. The <c>get_error</c> here is the
    /// <b>authoritative</b> success/failure signal for the whole RPC — see
    /// <see cref="DeletedRecordsValue"/>.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DeleteRecordsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DeleteRecordsResultCount,
            NativeMethods.DeleteRecordsResultGetError);

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
    /// <c>deleteRecords</c>' <b>inline-scalar</b> value reader — the sub-shape M15/P2b's
    /// value seam exists for. There is no borrowed child handle to copy out of:
    /// <c>get_low_watermark(i)</c> <em>is</em> the value, an <c>int64_t</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This reader is reached only when the entry's <c>get_error</c> was null</b> —
    /// the walker checks the error first, so the <c>-1</c> the header mentions is never
    /// interpreted here. That matters because <c>-1</c> is overloaded three ways
    /// ("that partition failed", "index out of range", and a genuine watermark of
    /// <c>-1</c>), so it cannot serve as a verdict. A <c>-1</c> reaching this reader is a
    /// <b>success</b> carrying <c>-1</c>.
    /// </remarks>
    internal static readonly Func<IntPtr, int, DeletedRecords> DeletedRecordsValue =
        static (result, index) =>
            new DeletedRecords(NativeMethods.DeleteRecordsResultGetLowWatermark(result, index));

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
    /// <c>describeConfigs</c>' universal accessors — result <b>shape 1</b>, with a
    /// <b>borrowed</b> per-resource <c>get_error</c>.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DescribeConfigsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeConfigsResultCount,
            NativeMethods.DescribeConfigsResultGetError);

    /// <summary>
    /// <c>incrementalAlterConfigs</c>' universal accessors — result <b>shape 2</b>: the
    /// ABI declares no <c>_get_value</c>, so a null per-resource error <em>is</em> the
    /// success value.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors AlterConfigsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.AlterConfigsResultCount,
            NativeMethods.AlterConfigsResultGetError);

    /// <summary>
    /// <c>describeConfigs</c>' <b>composite</b> key reader — neither result declares a
    /// <c>get_key</c>; the key is <c>(get_key_type(i), get_key_name(i))</c>, reassembled
    /// into the <see cref="ConfigResource"/> Java keys the map by. The type id goes through
    /// <see cref="ConfigResourceMarshal.TypeFromId"/> rather than a raw cast, so an id this
    /// client has no member for degrades to <see cref="ConfigResourceType.Unknown"/> as
    /// Java's <c>Type.forId</c> does.
    /// </summary>
    internal static readonly Func<IntPtr, int, ConfigResource> DescribeConfigsKey =
        static (result, index) => new ConfigResource(
            ConfigResourceMarshal.TypeFromId(NativeMethods.DescribeConfigsResultGetKeyType(result, index)),
            KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeConfigsResultGetKeyName(result, index)));

    /// <inheritdoc cref="DescribeConfigsKey"/>
    internal static readonly Func<IntPtr, int, ConfigResource> AlterConfigsKey =
        static (result, index) => new ConfigResource(
            ConfigResourceMarshal.TypeFromId(NativeMethods.AlterConfigsResultGetKeyType(result, index)),
            KeyedResultMarshal.ReadStringKey(NativeMethods.AlterConfigsResultGetKeyName(result, index)));

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
    /// <c>describeLogDirs</c>' universal accessors — result <b>shape 1</b>, with a
    /// <b>borrowed</b> per-broker <c>get_error</c>. ⚠ That is the FIRST of this RPC's two
    /// borrowed errors; the second is nested in the value tree
    /// (<see cref="LogDirMarshal"/>).
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DescribeLogDirsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeLogDirsResultCount,
            NativeMethods.DescribeLogDirsResultGetError);

    /// <summary>
    /// <c>alterReplicaLogDirs</c>' universal accessors — result <b>shape 2</b>: the ABI
    /// declares no <c>_get_value</c>, so a null per-replica error <em>is</em> the success
    /// value.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors AlterReplicaLogDirsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.AlterReplicaLogDirsResultCount,
            NativeMethods.AlterReplicaLogDirsResultGetError);

    /// <inheritdoc cref="DescribeLogDirsAccessors"/>
    internal static readonly KeyedResultMarshal.Accessors DescribeReplicaLogDirsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeReplicaLogDirsResultCount,
            NativeMethods.DescribeReplicaLogDirsResultGetError);

    /// <summary>
    /// <c>describeLogDirs</c>' key reader — a <b>bare scalar</b> broker id, the first such
    /// key in M15. The <c>(result, index)</c> seam takes it unchanged, with no parsing and
    /// no composition.
    /// </summary>
    internal static readonly Func<IntPtr, int, int> DescribeLogDirsKey =
        static (result, index) => NativeMethods.DescribeLogDirsResultGetBroker(result, index);

    /// <summary>
    /// <c>describeLogDirs</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>LogDirDescriptionMap_t</c>, whose whole description-and-replica tree is copied out
    /// before the root dies.
    /// </summary>
    internal static readonly Func<IntPtr, int, IReadOnlyDictionary<string, LogDirDescription>> LogDirDescriptionsValue =
        static (result, index) =>
            LogDirMarshal.CopyOutMap(NativeMethods.DescribeLogDirsResultGetValue(result, index));

    /// <summary>
    /// <c>alterReplicaLogDirs</c>' <b>3-part composite</b> key reader — this result declares
    /// no <c>get_key</c>; the key is
    /// <c>(get_topic(i), get_partition(i), get_broker_id(i))</c>, reassembled into the
    /// <see cref="TopicPartitionReplica"/> Java keys the map by.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartitionReplica> AlterReplicaLogDirsKey =
        static (result, index) => new TopicPartitionReplica(
            KeyedResultMarshal.ReadStringKey(NativeMethods.AlterReplicaLogDirsResultGetTopic(result, index)),
            NativeMethods.AlterReplicaLogDirsResultGetPartition(result, index),
            NativeMethods.AlterReplicaLogDirsResultGetBrokerId(result, index));

    /// <inheritdoc cref="AlterReplicaLogDirsKey"/>
    internal static readonly Func<IntPtr, int, TopicPartitionReplica> DescribeReplicaLogDirsKey =
        static (result, index) => new TopicPartitionReplica(
            KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeReplicaLogDirsResultGetTopic(result, index)),
            NativeMethods.DescribeReplicaLogDirsResultGetPartition(result, index),
            NativeMethods.DescribeReplicaLogDirsResultGetBrokerId(result, index));

    /// <summary>
    /// <c>describeReplicaLogDirs</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>ReplicaLogDirInfo_t</c>, copied out before the root dies.
    /// </summary>
    internal static readonly Func<IntPtr, int, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>
        ReplicaLogDirInfoValue =
            static (result, index) =>
                LogDirMarshal.CopyOutReplicaInfo(
                    NativeMethods.DescribeReplicaLogDirsResultGetValue(result, index));

    /// <summary>
    /// <c>describeConfigs</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>Config_t</c>, whose whole entry-and-synonym tree is copied out before the root
    /// dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Reached only when the entry's <c>get_error</c> was null — the walker checks the
    /// error first — which is why the header's "null value if that resource failed" case
    /// cannot arrive here.
    /// </remarks>
    internal static readonly Func<IntPtr, int, Config> ConfigValue =
        static (result, index) =>
            ConfigMarshal.CopyOut(NativeMethods.DescribeConfigsResultGetValue(result, index));

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
    /// submission (result shape 3 — one aggregate future, the same shape as
    /// <see cref="ElectLeaders"/>).
    /// </summary>
    internal static readonly AlterConsumerGroupOffsetsCallback AlterConsumerGroupOffsets =
        OnAlterConsumerGroupOffsets;

    /// <summary>
    /// The rooted instance passed to every <c>delete_consumer_group_offsets_async</c>
    /// submission (result shape 3 — one aggregate future, the same shape as
    /// <see cref="AlterConsumerGroupOffsets"/>).
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
    /// <c>remove_members_from_consumer_group_async</c> submission (result shape 3 — one
    /// aggregate future, the same shape as <see cref="AlterConsumerGroupOffsets"/>).
    /// </summary>
    internal static readonly RemoveMembersFromConsumerGroupCallback RemoveMembersFromConsumerGroup =
        OnRemoveMembersFromConsumerGroup;

    /// <summary>
    /// The rooted instance passed to every <c>create_acls_async</c> submission.
    /// </summary>
    internal static readonly CreateAclsCallback CreateAcls = OnCreateAcls;

    /// <summary>
    /// <c>createAcls</c>' universal accessors — result <b>shape 2</b>: Java stores
    /// <c>Map&lt;AclBinding, KafkaFuture&lt;Void&gt;&gt;</c>
    /// (<c>CreateAclsResult.java:30</c>), so the ABI declares no <c>_get_value</c> and a
    /// null per-binding error <em>is</em> the success value.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors CreateAclsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.CreateAclsResultCount,
            NativeMethods.CreateAclsResultGetError);

    /// <summary>
    /// <c>createAcls</c>' key reader: the borrowed <c>get_binding(i)</c>, copied out into the
    /// nested managed <see cref="AclBinding"/> before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Built over <b><c>createAcls</c>'</b> own <c>get_binding</c>. The ACL result types
    /// expose byte-identical accessor sets, so a cross-wired reader returns a plausible
    /// answer rather than failing — <c>AdminP4ReaderWiringTests</c> reads the captured symbol
    /// back off this closure for exactly that reason.
    /// </remarks>
    internal static readonly Func<IntPtr, int, AclBinding> CreateAclsKey =
        AclRowMarshal.BindingReader(NativeMethods.CreateAclsResultGetBinding);

    /// <summary>
    /// The rooted instance passed to every <c>delete_acls_async</c> submission.
    /// </summary>
    internal static readonly DeleteAclsCallback DeleteAcls = OnDeleteAcls;

    /// <summary>
    /// <c>deleteAcls</c>' universal accessors — result <b>shape 1</b>: Java stores
    /// <c>Map&lt;AclBindingFilter, KafkaFuture&lt;FilterResults&gt;&gt;</c>
    /// (<c>DeleteAclsResult.java:91</c>), so each filter carries a value <em>and</em> a fault
    /// channel.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>GetError</c> here is <c>get_error(i)</c> — the <b>filter's</b> future failing. The
    /// inner <c>get_result_error(i, j)</c> is a stored value and is read by
    /// <see cref="DeleteAclsFilterResults"/> instead; the two are independent.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors DeleteAclsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DeleteAclsResultCount,
            NativeMethods.DeleteAclsResultGetError);

    /// <summary>
    /// <c>deleteAcls</c>' key reader: the borrowed <c>get_filter(i)</c>, copied out into the
    /// nested managed <see cref="AclBindingFilter"/> before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Built over <b><c>deleteAcls</c>'</b> own <c>get_filter</c> — the ACL results expose
    /// byte-identical accessor sets, so a cross-wired reader returns a plausible answer rather
    /// than failing (<c>AdminP4ReaderWiringTests</c>).
    /// </remarks>
    internal static readonly Func<IntPtr, int, AclBindingFilter> DeleteAclsKey =
        AclRowMarshal.FilterReader(NativeMethods.DeleteAclsResultGetFilter);

    /// <summary>
    /// <c>deleteAcls</c>' value reader: the whole inner <c>(i, j)</c> axis, walked inside the
    /// reader so the keyed walker needs no second index.
    /// </summary>
    internal static readonly Func<IntPtr, int, DeleteAclsResult.FilterResults> DeleteAclsFilterResults =
        DeleteAclsResultMarshal.FilterResultsReader(
            NativeMethods.DeleteAclsResultGetResultCount,
            NativeMethods.DeleteAclsResultGetBinding,
            NativeMethods.DeleteAclsResultGetResultError,
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

    /// <summary>
    /// <c>alterClientQuotas</c>' universal accessors — result <b>shape 2</b>: Java stores
    /// <c>Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt;</c>
    /// (<c>AlterClientQuotasResult.java:31</c>), so the ABI declares no <c>_get_value</c> and
    /// a null per-entity error <em>is</em> the success value.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors AlterClientQuotasAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.AlterClientQuotasResultCount,
            NativeMethods.AlterClientQuotasResultGetError);

    /// <summary>
    /// <c>alterClientQuotas</c>' key reader: the borrowed <c>get_entity(i)</c>, copied out
    /// into the managed <see cref="ClientQuotaEntity"/> before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Built over <b><c>alterClientQuotas</c>'</b> own <c>get_entity</c>.
    /// <c>kafka_admin_CreateAclsResult_t</c> declares a byte-identical accessor set
    /// (<c>count</c> / <c>get_X</c> / <c>get_error</c> / <c>destroy</c>), so a cross-wired
    /// reader returns a plausible answer rather than failing — <c>AdminP4ReaderWiringTests</c>
    /// reads the captured symbol back off this closure for exactly that reason.
    /// </remarks>
    internal static readonly Func<IntPtr, int, ClientQuotaEntity> AlterClientQuotasKey =
        ClientQuotaMarshal.EntityReader(NativeMethods.AlterClientQuotasResultGetEntity);

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
    /// <c>kafka_admin_AdminClient_alter_user_scram_credentials_callback_t</c>. Same ownership
    /// split as above.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AlterUserScramCredentialsCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// <c>kafka_admin_AdminClient_update_features_callback_t</c>. ⚠ A <b>per-feature</b> failure
    /// arrives inside <paramref name="result"/>, borrowed; <paramref name="error"/> is
    /// <b>owned</b>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void UpdateFeaturesCallback(IntPtr result, IntPtr error, IntPtr userData);

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
    /// <c>describeUserScramCredentials</c>' row reader: the user, that user's <b>borrowed</b>
    /// error, and the user's credential infos.
    /// </summary>
    /// <remarks>
    /// ⚠ The inner walk is bounded by <c>get_credential_count(i)</c> — <b>never</b> by the
    /// outer <c>count</c>; the two are unrelated, and the inner count is <c>0</c> for a failed
    /// user (<c>confluent_kafka.h:9424-9425</c>).
    /// </remarks>
    internal static readonly Func<IntPtr, int, UserScramCredentialEntry> DescribeUserScramCredentialsEntry =
        UserScramCredentialMarshal.ReadEntry;

    /// <summary>
    /// <c>alterUserScramCredentials</c>' universal accessors — result <b>shape 2</b>: Java's
    /// per-user future is <c>KafkaFuture&lt;Void&gt;</c>
    /// (<c>AlterUserScramCredentialsResult.java:31</c>), so a null per-user error <em>is</em>
    /// the success value.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors AlterUserScramCredentialsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.AlterUserScramCredentialsResultCount,
            NativeMethods.AlterUserScramCredentialsResultGetError);

    /// <summary>
    /// <c>alterUserScramCredentials</c>' key reader, built over <b>its own</b>
    /// <c>get_user(i)</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>kafka_admin_UpdateFeaturesResult_t</c> (and P6's <c>CreateAclsResult</c>) declare a
    /// byte-identical accessor set, so a cross-wired reader returns a plausible answer rather
    /// than failing. Built through <see cref="KeyedResultMarshal.StringKeyReader"/> so it
    /// <b>captures</b> that symbol and the wiring guard can read it back — as an inline lambda
    /// it was invisible to the guard's discovery, and a swap went undetected (M15/P7, 77.3).
    /// </remarks>
    internal static readonly Func<IntPtr, int, string> AlterUserScramCredentialsKey =
        KeyedResultMarshal.StringKeyReader(NativeMethods.AlterUserScramCredentialsResultGetUser);

    /// <summary>
    /// <c>describeDelegationToken</c>' element reader — sub-shape 3b, one collection, no key
    /// and no per-element error.
    /// </summary>
    internal static readonly Func<IntPtr, int, Confluent.Kafka.DelegationToken> DescribeDelegationTokenValue =
        DelegationTokenMarshal.TokenReader(NativeMethods.DescribeDelegationTokenResultGetToken);

    /// <summary>
    /// <c>updateFeatures</c>' universal accessors — result <b>shape 2</b>
    /// (<c>UpdateFeaturesResult.java:29</c>).
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors UpdateFeaturesAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.UpdateFeaturesResultCount,
            NativeMethods.UpdateFeaturesResultGetError);

    /// <summary>
    /// <c>updateFeatures</c>' key reader, built over <b>its own</b> <c>get_feature(i)</c>. ⚠ See
    /// <see cref="AlterUserScramCredentialsKey"/> on the byte-identical twin.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> UpdateFeaturesKey =
        KeyedResultMarshal.StringKeyReader(NativeMethods.UpdateFeaturesResultGetFeature);

    private static readonly Action<IntPtr> s_destroyDescribeUserScramCredentialsResult =
        NativeMethods.DescribeUserScramCredentialsResultDestroy;

    private static readonly Action<IntPtr> s_destroyAlterUserScramCredentialsResult =
        NativeMethods.AlterUserScramCredentialsResultDestroy;

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

    private static readonly Action<IntPtr> s_destroyUpdateFeaturesResult =
        NativeMethods.UpdateFeaturesResultDestroy;

    private static readonly KeyedResultMarshal.CountAccessor s_describeUserScramCredentialsCount =
        NativeMethods.DescribeUserScramCredentialsResultCount;

    private static readonly KeyedResultMarshal.CountAccessor s_describeDelegationTokenCount =
        NativeMethods.DescribeDelegationTokenResultCount;

    /// <summary>
    /// The completion body the four <b>count-less</b> P7 trampolines share: read one value off
    /// the result root and resolve the single awaiter with it.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="error"/> is <b>OWNED</b> — none of these results declares a
    /// <c>get_error</c>, so nothing on this path is borrowed and
    /// <see cref="KafkaException.FromHandle"/> is what frees it exactly once. The
    /// <c>finally</c> discharges the usual three obligations, with the destroy strictly after
    /// the read because every value read is borrowed from that root.
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

    private static void OnDescribeUserScramCredentials(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteListRpc(
            result,
            error,
            userData,
            s_describeUserScramCredentialsCount,
            DescribeUserScramCredentialsEntry,
            s_destroyDescribeUserScramCredentialsResult);

    private static void OnAlterUserScramCredentials(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            AlterUserScramCredentialsAccessors,
            AlterUserScramCredentialsKey,
            s_destroyAlterUserScramCredentialsResult);

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

    private static void OnUpdateFeatures(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            UpdateFeaturesAccessors,
            UpdateFeaturesKey,
            s_destroyUpdateFeaturesResult);

    /// <summary>
    /// <c>deleteConsumerGroups</c>' universal accessors — result <b>shape 2</b>: Java's
    /// per-group future is <c>KafkaFuture&lt;Void&gt;</c>
    /// (<c>DeleteConsumerGroupsResult.java:30</c>), so the ABI declares no
    /// <c>_get_value</c> and a null per-group error <em>is</em> the success value.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DeleteConsumerGroupsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DeleteConsumerGroupsResultCount,
            NativeMethods.DeleteConsumerGroupsResultGetError);

    /// <summary><c>deleteConsumerGroups</c>' key reader — the group id.</summary>
    internal static readonly Func<IntPtr, int, string> DeleteConsumerGroupsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.DeleteConsumerGroupsResultGetGroupId(result, index));

    /// <summary>
    /// <c>removeMembersFromConsumerGroup</c>' key reader — the member's group instance id.
    /// Java keys the resolved map by <c>MemberIdentity</c>
    /// (<c>RemoveMembersFromConsumerGroupResult.java:35</c>), whose only distinguishing
    /// field the ABI carries back is the group instance id
    /// (<c>MemberToRemove.java</c> has no <c>toString()</c> override, so there is no
    /// Java-mandated string form to preserve here).
    /// </summary>
    internal static readonly Func<IntPtr, int, string> RemoveMembersFromConsumerGroupKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(
                NativeMethods.RemoveMembersFromConsumerGroupResultGetGroupInstanceId(result, index));

    /// <summary>
    /// ⚠⚠ <c>removeMembersFromConsumerGroup</c>' per-member <b>VALUE</b> reader — and it
    /// reads <c>get_error(i)</c>. Same shape as
    /// <see cref="AlterConsumerGroupOffsetsOptionalError"/>, for the same reason: Java's
    /// future resolves to <c>Map&lt;MemberIdentity, Errors&gt;</c>
    /// (<c>RemoveMembersFromConsumerGroupResult.java:35</c>) — one future over the whole
    /// map, so a per-member error is an ordinary map value, not a per-member fault. Only
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult.MemberResult"/> and
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult.All"/> turn a non-null entry
    /// into a fault, mirroring Java's <c>maybeCompleteExceptionally</c> / <c>all()</c>.
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
    /// <c>alterConsumerGroupOffsets</c>' <b>composite</b> key reader — this result declares
    /// no <c>get_key</c>; the key is <c>(get_topic(i), get_partition(i))</c>, reassembled
    /// into the <see cref="TopicPartition"/> Java's map is keyed by. Same reader shape as
    /// <see cref="ElectLeadersKey"/>.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> AlterConsumerGroupOffsetsKey =
        TopicPartitionKey(
            NativeMethods.AlterConsumerGroupOffsetsResultGetTopic,
            NativeMethods.AlterConsumerGroupOffsetsResultGetPartition);

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
    /// <see cref="Admin.AlterConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/> and
    /// <see cref="Admin.AlterConsumerGroupOffsetsResult.All"/> turn a non-null entry into a
    /// fault, mirroring Java's <c>partitionResult</c> / <c>all()</c>.
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
    /// <c>deleteConsumerGroupOffsets</c>' <b>composite</b> key reader — this result declares
    /// no <c>get_key</c>; the key is <c>(get_topic(i), get_partition(i))</c>, reassembled
    /// into the <see cref="TopicPartition"/> Java's map is keyed by. Same reader shape as
    /// <see cref="AlterConsumerGroupOffsetsKey"/>.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> DeleteConsumerGroupOffsetsKey =
        TopicPartitionKey(
            NativeMethods.DeleteConsumerGroupOffsetsResultGetTopic,
            NativeMethods.DeleteConsumerGroupOffsetsResultGetPartition);

    /// <summary>
    /// ⚠⚠ <c>deleteConsumerGroupOffsets</c>' per-partition <b>VALUE</b> reader — and it reads
    /// <c>get_error(i)</c>. Same shape as <see cref="AlterConsumerGroupOffsetsOptionalError"/>,
    /// for the same reason.
    /// </summary>
    /// <remarks>
    /// Java's future resolves to <c>Map&lt;TopicPartition, Errors&gt;</c>
    /// (<c>DeleteConsumerGroupOffsetsResult.java:33</c>) — <c>Errors.NONE</c> means that
    /// partition's offset was deleted successfully, any other code is that partition's own
    /// outcome. The per-partition error <em>is</em> the map's value, so it is read by the
    /// value reader and lands in the map — it does not fault anything on its own; only
    /// <see cref="Admin.DeleteConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/> and
    /// <see cref="Admin.DeleteConsumerGroupOffsetsResult.All"/> turn a non-null entry into a
    /// fault, mirroring Java's <c>partitionResult</c> / <c>all()</c>.
    /// </remarks>
    internal static readonly Func<IntPtr, int, KafkaException?> DeleteConsumerGroupOffsetsOptionalError =
        BorrowedOptionalError(NativeMethods.DeleteConsumerGroupOffsetsResultGetError);

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

    /// <inheritdoc cref="ElectLeadersKey"/>
    internal static readonly Func<IntPtr, int, TopicPartition> AlterPartitionReassignmentsKey =
        TopicPartitionKey(
            NativeMethods.AlterPartitionReassignmentsResultGetTopic,
            NativeMethods.AlterPartitionReassignmentsResultGetPartition);

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
    /// <c>listOffsets</c>' universal accessors — result <b>shape 1</b>, with a
    /// <b>borrowed</b> per-partition <c>get_error</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ This RPC declares <b>both</b> a <c>get_error</c> and a <c>get_value</c>, which is
    /// what separates it from its similarly-named Stage-2 sibling
    /// (<c>listPartitionReassignments</c>, which declares only a <c>get_value</c>) and from
    /// its Stage-1 near-namesake (<c>alterPartitionReassignments</c>, which declares only a
    /// <c>get_error</c>). Three names in one family, three shapes.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors ListOffsetsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.ListOffsetsResultCount,
            NativeMethods.ListOffsetsResultGetError);

    /// <inheritdoc cref="ElectLeadersKey"/>
    internal static readonly Func<IntPtr, int, TopicPartition> ListOffsetsKey =
        TopicPartitionKey(
            NativeMethods.ListOffsetsResultGetTopic, NativeMethods.ListOffsetsResultGetPartition);

    /// <summary>
    /// <c>listOffsets</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>ListOffsetsResultInfo_t</c>, copied out before the root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ Reached only when the entry's <c>get_error</c> was null — the walker checks the
    /// error first — which is why the header's "null value if that partition failed" case
    /// cannot arrive here.
    /// </remarks>
    internal static readonly Func<IntPtr, int, ListOffsetsResult.ListOffsetsResultInfo> ListOffsetsInfoValue =
        static (result, index) =>
            ListOffsetsResultInfoMarshal.CopyOut(NativeMethods.ListOffsetsResultGetValue(result, index))
            ?? throw new KafkaException(
                "The listOffsets result produced no offset information for a partition that "
                + "reported no error.");

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
    /// <c>describeConsumerGroups</c>' universal accessors — the count and the per-key
    /// <c>get_error</c>, the two every <b>keyed</b> shape has.
    /// </summary>
    /// <remarks>
    /// ⚠ A keyed map, <em>not</em> the two-independent-lists sub-shape its
    /// <c>listConsumerGroups</c> neighbour uses: there is one count here, and index
    /// <c>i</c> of the key, value and error walks all name the same group. So this goes
    /// through <see cref="KeyedResultMarshal.Complete{TKey, TValue}"/> unchanged, and no
    /// new walker is needed.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors DescribeConsumerGroupsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeConsumerGroupsResultCount,
            NativeMethods.DescribeConsumerGroupsResultGetError);

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
    /// <c>describeConsumerGroups</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>ConsumerGroupDescription_t</c>, copied out in full — members, their assignments
    /// and all — before the root dies.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Everything reachable from here is borrowed from the one result root</b>, three
    /// levels deep: the description from the result, each member from the description, each
    /// assignment from its member. None of it is owned, none of it is freed here, and all
    /// of it dangles the moment
    /// <see cref="NativeMethods.DescribeConsumerGroupsResultDestroy"/> runs — which is why
    /// the copy-out completes inside the walk and the destroy is in the trampoline's
    /// <c>finally</c>, strictly after.
    /// </para>
    /// <para>
    /// ⚠ <c>group_type</c> and <c>group_state</c> are <b>non-optional on this class</b>
    /// (the header says so of <c>type()</c>), unlike their <c>ConsumerGroupListing</c>
    /// namesakes where null is Java's <c>Optional.empty()</c>. A null here is an ABI
    /// contract violation, and it reads as <c>Unknown</c> — the same answer an unrecognised
    /// name gets — because the managed properties are non-nullable.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, int, ConsumerGroupDescription> ConsumerGroupDescriptionValue =
        static (result, index) =>
            CopyOutConsumerGroupDescription(
                NativeMethods.DescribeConsumerGroupsResultGetValue(result, index));

    /// <summary>
    /// <c>describeClassicGroups</c>' universal accessors — the count and the per-key
    /// <c>get_error</c>, the two every <b>keyed</b> shape has.
    /// </summary>
    /// <remarks>
    /// ⚠ A keyed map, the same shape <see cref="DescribeConsumerGroupsAccessors"/> has: one
    /// count, and index <c>i</c> of the key, value and error walks all name the same group.
    /// So this goes through <see cref="KeyedResultMarshal.Complete{TKey, TValue}"/>
    /// unchanged, and no new walker callable is needed.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors DescribeClassicGroupsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeClassicGroupsResultCount,
            NativeMethods.DescribeClassicGroupsResultGetError);

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

    /// <summary>
    /// <c>describeClassicGroups</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>ClassicGroupDescription_t</c>, copied out in full — members, their assignments and
    /// all — before the root dies.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Everything reachable from here is borrowed from the one result root</b>, three
    /// levels deep: the description from the result, each member from the description, each
    /// assignment from its member. None of it is owned, none of it is freed here, and all of
    /// it dangles the moment
    /// <see cref="NativeMethods.DescribeClassicGroupsResultDestroy"/> runs — which is why
    /// the copy-out completes inside the walk and the destroy is in the trampoline's
    /// <c>finally</c>, strictly after.
    /// </para>
    /// <para>
    /// ⚠ <c>is_simple_consumer_group</c> is <b>not read</b>, although the ABI exports it:
    /// Java derives it from <c>protocol</c> and so does the managed class. See the comment
    /// beside <see cref="NativeMethods.ClassicGroupDescriptionProtocol"/>.
    /// </para>
    /// </remarks>
    internal static readonly Func<IntPtr, int, ClassicGroupDescription> ClassicGroupDescriptionValue =
        static (result, index) =>
            CopyOutClassicGroupDescription(
                NativeMethods.DescribeClassicGroupsResultGetValue(result, index));

    /// <summary>
    /// <c>listConsumerGroupOffsets</c>' universal accessors — the count and the per-key
    /// <c>get_error</c>, the two every <b>keyed</b> shape has.
    /// </summary>
    /// <remarks>
    /// ⚠ A keyed map, the same shape <see cref="DescribeClassicGroupsAccessors"/> has: one
    /// count, and index <c>i</c> of the key, value and error walks all name the same group.
    /// So this goes through <see cref="KeyedResultMarshal.Complete{TKey, TValue}"/>
    /// unchanged, and no new walker callable is needed.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors ListConsumerGroupOffsetsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.ListConsumerGroupOffsetsResultCount,
            NativeMethods.ListConsumerGroupOffsetsResultGetError);

    /// <summary>
    /// <c>listConsumerGroupOffsets</c>' key reader: the group id at one index.
    /// </summary>
    /// <remarks>
    /// ⚠ The bridge dictionary these keys resolve against must be built with
    /// <see cref="StringComparer.Ordinal"/>, for the reason given on
    /// <see cref="DescribeConsumerGroupsKey"/>: <c>ListConsumerGroupOffsetsResult</c>'s
    /// aggregate hardcodes that comparer, and its <c>PartitionsToOffsetAndMetadata(groupId)</c>
    /// lookup rejects a group id the map does not contain — so a comparer mismatch would
    /// turn a returned group into an <see cref="ArgumentException"/>.
    /// </remarks>
    internal static readonly Func<IntPtr, int, string> ListConsumerGroupOffsetsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(
                NativeMethods.ListConsumerGroupOffsetsResultGetGroupId(result, index));

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

    /// <summary>
    /// The result-root destroys, hoisted for the same reason as the accessor sets: a
    /// method group converted at the call site would allocate a delegate per completion.
    /// All are null-safe, so the trampoline's <c>finally</c> can call them
    /// unconditionally.
    /// </summary>
    private static readonly Action<IntPtr> s_destroyCreateTopicsResult = NativeMethods.CreateTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteTopicsResult = NativeMethods.DeleteTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeTopicsResult = NativeMethods.DescribeTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyCreatePartitionsResult =
        NativeMethods.CreatePartitionsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteRecordsResult = NativeMethods.DeleteRecordsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListConfigResourcesResult =
        NativeMethods.ListConfigResourcesResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListClientMetricsResourcesResult =
        NativeMethods.ListClientMetricsResourcesResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeConfigsResult =
        NativeMethods.DescribeConfigsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyAlterConfigsResult =
        NativeMethods.AlterConfigsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeLogDirsResult =
        NativeMethods.DescribeLogDirsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyAlterReplicaLogDirsResult =
        NativeMethods.AlterReplicaLogDirsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeReplicaLogDirsResult =
        NativeMethods.DescribeReplicaLogDirsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListTopicsResult = NativeMethods.ListTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyElectLeadersResult = NativeMethods.ElectLeadersResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyAlterPartitionReassignmentsResult =
        NativeMethods.AlterPartitionReassignmentsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyAlterConsumerGroupOffsetsResult =
        NativeMethods.AlterConsumerGroupOffsetsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteConsumerGroupOffsetsResult =
        NativeMethods.DeleteConsumerGroupOffsetsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteConsumerGroupsResult =
        NativeMethods.DeleteConsumerGroupsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
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

    /// <inheritdoc cref="s_listTopicsCount"/>
    private static readonly KeyedResultMarshal.CountAccessor s_alterConsumerGroupOffsetsCount =
        NativeMethods.AlterConsumerGroupOffsetsResultCount;

    /// <inheritdoc cref="s_listTopicsCount"/>
    private static readonly KeyedResultMarshal.CountAccessor s_deleteConsumerGroupOffsetsCount =
        NativeMethods.DeleteConsumerGroupOffsetsResultCount;

    /// <inheritdoc cref="s_listTopicsCount"/>
    private static readonly KeyedResultMarshal.CountAccessor s_removeMembersFromConsumerGroupCount =
        NativeMethods.RemoveMembersFromConsumerGroupResultCount;

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

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeConsumerGroupsResult =
        NativeMethods.DescribeConsumerGroupsResultDestroy;

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

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeClassicGroupsResult =
        NativeMethods.DescribeClassicGroupsResultDestroy;

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

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyListConsumerGroupOffsetsResult =
        NativeMethods.ListConsumerGroupOffsetsResultDestroy;

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
    private static readonly Action<IntPtr> s_destroyListOffsetsResult =
        NativeMethods.ListOffsetsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyCreateAclsResult =
        NativeMethods.CreateAclsResultDestroy;

    private static readonly Action<IntPtr> s_destroyDeleteAclsResult =
        NativeMethods.DeleteAclsResultDestroy;

    private static readonly KeyedResultMarshal.CountAccessor s_describeAclsCount =
        NativeMethods.DescribeAclsResultCount;

    private static readonly Action<IntPtr> s_destroyDescribeAclsResult =
        NativeMethods.DescribeAclsResultDestroy;

    private static readonly KeyedResultMarshal.CountAccessor s_describeClientQuotasCount =
        NativeMethods.DescribeClientQuotasResultCount;

    private static readonly Action<IntPtr> s_destroyDescribeClientQuotasResult =
        NativeMethods.DescribeClientQuotasResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyAlterClientQuotasResult =
        NativeMethods.AlterClientQuotasResultDestroy;

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
    /// The one completion body every keyed admin trampoline delegates to, so the
    /// ownership rules are stated once instead of once per RPC.
    /// </summary>
    /// <remarks>
    /// The <c>finally</c> discharges three obligations on <b>every</b> path — including
    /// the inline ones and the no-throw path: the owned result root is destroyed exactly
    /// once (null-safe, so the top-level-error branch is a no-op); any awaiter the result
    /// failed to account for is faulted, so no caller can be left holding a <c>Task</c>
    /// that never completes; and the rooting <c>GCHandle</c> plus the span-the-op client
    /// reference are released. The destroy runs strictly <em>after</em> the walk, because
    /// every value the walk reads is borrowed from that root.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">
    /// The submit failure, or <c>IntPtr.Zero</c>. ⚠ <b>OWNED</b> — freed here with
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>, the mirror image of the per-key
    /// errors inside a result, which are borrowed and must never be freed.
    /// </param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="accessors">That RPC's universal accessors.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="readValue">That RPC's value reader.</param>
    /// <param name="destroyResult">That RPC's <c>*Result_destroy</c>.</param>
    private static void CompleteKeyed<TKey, TValue>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        KeyedResultMarshal.Accessors accessors,
        Func<IntPtr, int, TKey> readKey,
        Func<IntPtr, int, TValue> readValue,
        Action<IntPtr> destroyResult)
        where TKey : notnull
    {
        KeyedAdminOperation<TKey, TValue>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (KeyedAdminOperation<TKey, TValue>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                // The request could not be submitted at all: there is no result table, so
                // every requested key fails with this one error.
                context.FailAll(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.Complete(result, accessors, context, readKey, readValue);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Tasks.
            context?.FailAll(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The <b>shape-2</b> twin of <see cref="CompleteKeyed{TKey, TValue}"/>: identical in
    /// every respect except that the walk carries no per-key value, because the RPC's
    /// result type has no <c>_get_value</c> function.
    /// </summary>
    /// <remarks>
    /// It exists as its own method rather than as a null argument so the shape is stated
    /// by the type system: it accepts only a <see cref="VoidKeyedAdminOperation{TKey}"/>,
    /// so a value-carrying operation cannot be routed here and have its value dropped.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="accessors">That RPC's universal accessors.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="destroyResult">That RPC's <c>*Result_destroy</c>.</param>
    private static void CompleteKeyedVoid<TKey>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        KeyedResultMarshal.Accessors accessors,
        Func<IntPtr, int, TKey> readKey,
        Action<IntPtr> destroyResult)
        where TKey : notnull
    {
        VoidKeyedAdminOperation<TKey>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (VoidKeyedAdminOperation<TKey>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.FailAll(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.Complete(result, accessors, context, readKey);

                // Keys the ABI request could not carry are resolved here, after the walk and
                // before FailUncompleted sees them. A no-op for every RPC that has none —
                // see VoidKeyedAdminOperation.SetKeysWithNoRequest for the one shape that
                // does, and why it is deliberately not reached on the failure branch above.
                context.CompleteKeysWithNoRequest();
            }
        }
        catch (Exception exception)
        {
            context?.FailAll(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    private static void OnCreateTopics(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            CreateTopicsAccessors,
            CreateTopicsKey,
            TopicMetadataAndConfigValue,
            s_destroyCreateTopicsResult);

    private static void OnDeleteTopicsByName(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            DeleteTopicsAccessors,
            DeleteTopicsNameKey,
            s_destroyDeleteTopicsResult);

    private static void OnDeleteTopicsById(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            DeleteTopicsAccessors,
            DeleteTopicsIdKey,
            s_destroyDeleteTopicsResult);

    private static void OnDescribeTopicsByName(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeTopicsAccessors,
            DescribeTopicsNameKey,
            TopicDescriptionValue,
            s_destroyDescribeTopicsResult);

    private static void OnDescribeTopicsById(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeTopicsAccessors,
            DescribeTopicsIdKey,
            TopicDescriptionValue,
            s_destroyDescribeTopicsResult);

    private static void OnCreatePartitions(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            CreatePartitionsAccessors,
            CreatePartitionsKey,
            s_destroyCreatePartitionsResult);

    private static void OnDeleteRecords(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DeleteRecordsAccessors,
            DeleteRecordsKey,
            DeletedRecordsValue,
            s_destroyDeleteRecordsResult);

    private static void OnDescribeConfigs(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeConfigsAccessors,
            DescribeConfigsKey,
            ConfigValue,
            s_destroyDescribeConfigsResult);

    private static void OnIncrementalAlterConfigs(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            AlterConfigsAccessors,
            AlterConfigsKey,
            s_destroyAlterConfigsResult);

    private static void OnDescribeLogDirs(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeLogDirsAccessors,
            DescribeLogDirsKey,
            LogDirDescriptionsValue,
            s_destroyDescribeLogDirsResult);

    private static void OnAlterReplicaLogDirs(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            AlterReplicaLogDirsAccessors,
            AlterReplicaLogDirsKey,
            s_destroyAlterReplicaLogDirsResult);

    private static void OnDescribeReplicaLogDirs(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeReplicaLogDirsAccessors,
            DescribeReplicaLogDirsKey,
            ReplicaLogDirInfoValue,
            s_destroyDescribeReplicaLogDirsResult);

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
    /// <c>destroy</c> — and correctly routes through <see cref="CompleteKeyedVoid"/>,
    /// where <c>get_error(i)</c> faults that partition's own awaitable. Rewiring this
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
    /// ⚠⚠ <c>alterConsumerGroupOffsets</c>' shape-3 trampoline — <b>the aggregate walker,
    /// with <c>get_error(i)</c> supplied as the VALUE reader.</b> Same walker as
    /// <see cref="OnElectLeaders"/>, for the same reason.
    /// </summary>
    /// <remarks>
    /// Java's <c>AlterConsumerGroupOffsetsResult</c> holds
    /// <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c>
    /// (<c>AlterConsumerGroupOffsetsResult.java:33</c>) — one future over the whole map, so a
    /// per-partition <c>Errors</c> code is an ordinary map value, not a per-partition fault.
    /// ⚠ Its accessor set is byte-identical to <see cref="OnElectLeaders"/>'s and to
    /// <see cref="OnAlterPartitionReassignments"/>'s, which routes through
    /// <see cref="CompleteKeyedVoid"/> instead — the Java return type, not the ABI shape,
    /// is what decides the walker.
    /// </remarks>
    private static void OnAlterConsumerGroupOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_alterConsumerGroupOffsetsCount,
            AlterConsumerGroupOffsetsKey,
            AlterConsumerGroupOffsetsOptionalError,
            EqualityComparer<TopicPartition>.Default,
            s_destroyAlterConsumerGroupOffsetsResult);

    /// <summary>
    /// ⚠⚠ <c>deleteConsumerGroupOffsets</c>' shape-3 trampoline — <b>the aggregate walker,
    /// with <c>get_error(i)</c> supplied as the VALUE reader.</b> Same walker as
    /// <see cref="OnAlterConsumerGroupOffsets"/>, for the same reason.
    /// </summary>
    /// <remarks>
    /// Java's <c>DeleteConsumerGroupOffsetsResult</c> holds
    /// <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c>
    /// (<c>DeleteConsumerGroupOffsetsResult.java:33</c>) — one future over the whole map, so a
    /// per-partition <c>Errors</c> code is an ordinary map value, not a per-partition fault.
    /// ⚠ Its accessor set is byte-identical to <see cref="OnAlterConsumerGroupOffsets"/>'s —
    /// the Java return type, not the ABI shape, is what decides the walker.
    /// </remarks>
    private static void OnDeleteConsumerGroupOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_deleteConsumerGroupOffsetsCount,
            DeleteConsumerGroupOffsetsKey,
            DeleteConsumerGroupOffsetsOptionalError,
            EqualityComparer<TopicPartition>.Default,
            s_destroyDeleteConsumerGroupOffsetsResult);

    /// <summary>
    /// <c>deleteConsumerGroups</c>' shape-2 trampoline: one awaiter per requested group id,
    /// the same walker as <see cref="OnCreatePartitions"/>.
    /// </summary>
    private static void OnDeleteConsumerGroups(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            DeleteConsumerGroupsAccessors,
            DeleteConsumerGroupsKey,
            s_destroyDeleteConsumerGroupsResult);

    /// <summary>
    /// ⚠⚠ <c>removeMembersFromConsumerGroup</c>' shape-3 trampoline — <b>the aggregate
    /// walker, with <c>get_error(i)</c> supplied as the VALUE reader.</b> Same walker as
    /// <see cref="OnAlterConsumerGroupOffsets"/>, for the same reason.
    /// </summary>
    /// <remarks>
    /// Java's <c>RemoveMembersFromConsumerGroupResult</c> holds
    /// <c>KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt; future</c>
    /// (<c>RemoveMembersFromConsumerGroupResult.java:35</c>) — one future over the whole
    /// map, so a per-member <c>Errors</c> code is an ordinary map value, not a per-member
    /// fault. In <b>removeAll</b> mode
    /// (<c>NativeMethods.AdminClientRemoveMembersFromConsumerGroupAsync</c>) the result
    /// handle always carries zero rows, so this walker resolves to an empty map and any
    /// failure must have arrived through <paramref name="error"/> instead — matching
    /// Java's <c>all()</c>, which in that mode reports only a call-level fault (see
    /// <see cref="Admin.RemoveMembersFromConsumerGroupResult"/>'s remarks for the
    /// documented deviation this forces on <c>All()</c>).
    /// </remarks>
    private static void OnRemoveMembersFromConsumerGroup(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteAggregateRpc(
            result,
            error,
            userData,
            s_removeMembersFromConsumerGroupCount,
            RemoveMembersFromConsumerGroupKey,
            RemoveMembersFromConsumerGroupOptionalError,
            StringComparer.Ordinal,
            s_destroyRemoveMembersFromConsumerGroupResult);

    private static void OnCreateAcls(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            CreateAclsAccessors,
            CreateAclsKey,
            s_destroyCreateAclsResult);

    /// <summary>
    /// <c>alterClientQuotas</c>' <b>shape-2</b> trampoline: one awaitable per entity, each
    /// carrying only that entity's own outcome.
    /// </summary>
    private static void OnAlterClientQuotas(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            AlterClientQuotasAccessors,
            AlterClientQuotasKey,
            s_destroyAlterClientQuotasResult);

    /// <summary>
    /// <c>deleteAcls</c>' shape-1 trampoline: one awaitable per filter, each carrying that
    /// filter's own <c>FilterResults</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ The accessor set's <c>get_error(i)</c> faults the filter's <c>Task</c>; the inner
    /// <c>get_result_error(i, j)</c> the value reader reads is a stored value on a
    /// successfully completed one. Both are borrowed; neither is destroyed.
    /// </remarks>
    private static void OnDeleteAcls(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DeleteAclsAccessors,
            DeleteAclsKey,
            DeleteAclsFilterResults,
            s_destroyDeleteAclsResult);

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
    private static void OnListOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            ListOffsetsAccessors,
            ListOffsetsKey,
            ListOffsetsInfoValue,
            s_destroyListOffsetsResult);

    /// <summary>
    /// <c>alterPartitionReassignments</c>' shape-2 trampoline: one awaitable per
    /// partition, faulted by that partition's own borrowed <c>get_error(i)</c>. ⚠⚠ Its
    /// accessor set is byte-identical to <see cref="OnElectLeaders"/>'; the Java return
    /// type is what separates them.
    /// </summary>
    private static void OnAlterPartitionReassignments(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            AlterPartitionReassignmentsAccessors,
            AlterPartitionReassignmentsKey,
            s_destroyAlterPartitionReassignmentsResult);

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
    /// <c>describeConsumerGroups</c>' <b>shape-1</b> trampoline: one future per requested
    /// group id, each resolved with that group's description or faulted with that group's
    /// own error.
    /// </summary>
    /// <remarks>
    /// The two error directions meet here as in every keyed RPC:
    /// <paramref name="error"/> is the callback's own parameter, non-const, meaning the
    /// request could not be submitted at all — it is <b>OWNED</b> and fails every requested
    /// key; the per-group errors <see cref="DescribeConsumerGroupsAccessors"/> reaches
    /// through <c>get_error</c> are <b>BORROWED</b> from the result root and are read, never
    /// freed. Both live in <see cref="CompleteKeyed{TKey, TValue}"/>, which also destroys
    /// this RPC's own root in its <c>finally</c>, strictly after the copy-out.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnDescribeConsumerGroups(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeConsumerGroupsAccessors,
            DescribeConsumerGroupsKey,
            ConsumerGroupDescriptionValue,
            s_destroyDescribeConsumerGroupsResult);

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
    /// <c>describeClassicGroups</c>' <b>shape-1</b> trampoline: one future per requested
    /// group id, each resolved with that group's description or faulted with that group's
    /// own error.
    /// </summary>
    /// <remarks>
    /// The two error directions meet here as in every keyed RPC:
    /// <paramref name="error"/> is the callback's own parameter, non-const, meaning the
    /// request could not be submitted at all — it is <b>OWNED</b> and fails every requested
    /// key; the per-group errors <see cref="DescribeClassicGroupsAccessors"/> reaches
    /// through <c>get_error</c> are <b>BORROWED</b> from the result root and are read, never
    /// freed. Both live in <see cref="CompleteKeyed{TKey, TValue}"/>, which also destroys
    /// this RPC's own root — not <c>describeConsumerGroups</c>' — in its <c>finally</c>,
    /// strictly after the copy-out.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnDescribeClassicGroups(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeClassicGroupsAccessors,
            DescribeClassicGroupsKey,
            ClassicGroupDescriptionValue,
            s_destroyDescribeClassicGroupsResult);

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
    /// <c>listConsumerGroupOffsets</c>' <b>shape-1</b> trampoline: one future per requested
    /// group id, each resolved with that group's committed offsets or faulted with that
    /// group's own error.
    /// </summary>
    /// <remarks>
    /// The two error directions meet here as in every keyed RPC:
    /// <paramref name="error"/> is the callback's own parameter, non-const, meaning the
    /// request could not be submitted at all — it is <b>OWNED</b> and fails every requested
    /// key; the per-group errors <see cref="ListConsumerGroupOffsetsAccessors"/> reaches
    /// through <c>get_error</c> are <b>BORROWED</b> from the result root and are read, never
    /// freed. Both live in <see cref="CompleteKeyed{TKey, TValue}"/>, which also destroys
    /// this RPC's own root in its <c>finally</c>, strictly after the copy-out.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    private static void OnListConsumerGroupOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            ListConsumerGroupOffsetsAccessors,
            ListConsumerGroupOffsetsKey,
            ListConsumerGroupOffsetsValue,
            s_destroyListConsumerGroupOffsetsResult);

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
