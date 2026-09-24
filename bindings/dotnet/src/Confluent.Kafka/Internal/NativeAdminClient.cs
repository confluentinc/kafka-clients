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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The shared native surface behind <see cref="KafkaAdminClient"/> and
/// <see cref="MockAdminClient"/>: it owns the <see cref="SafeAdminHandle"/>, submits
/// every RPC, and orchestrates teardown. The two public clients differ only in how the
/// handle is constructed — the ABI hands back the <em>same</em>
/// <c>kafka_admin_AdminClient_t*</c> for both — so keeping the RPC surface here is what
/// stops it being written twice, exactly as <c>NativeConsumer</c> / <c>NativeProducer</c>
/// do for their pairs.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every RPC drives the <c>_async</c> ABI entry point, even though the C# method is
/// synchronous.</b> That reads backwards at first glance and is deliberate: "synchronous"
/// describes the C# method's own return behaviour — it hands back a <c>*Result</c>
/// without waiting, as Java's non-blocking <c>Admin</c> methods do — and the
/// <c>_async</c> ABI is what makes that possible. The sync ABI twin blocks until every
/// per-key future resolves, so calling it would invent the blocking Java does not have,
/// and wrapping it in <c>Task.Run</c> would be sync-over-async (ffi §B7). The 46
/// synchronous ABI entry points are simply unused by this binding.
/// </para>
/// <para>
/// <b>No access guard, and none is wanted.</b> Unlike the consumer, the admin ABI
/// permits concurrent operations, so there is no single-op-in-flight assumption here and
/// no managed mirror of one. What each in-flight operation <em>does</em> hold is a
/// span-the-op reference on the client handle — see <see cref="AdminOperation"/>.
/// </para>
/// </remarks>
internal sealed class NativeAdminClient : IDisposable
{
    /// <summary>
    /// A negative <c>timeout_ms</c> means <b>unset</b> — the client default applies —
    /// and for <c>close</c> it means Java's no-argument <c>close()</c> (wait
    /// indefinitely). It does <b>not</b> mean a zero timeout, which is why a null
    /// <c>TimeSpan</c>/<c>int</c> maps here rather than to 0.
    /// </summary>
    private const int UnsetTimeoutMs = -1;

    /// <summary>
    /// Java's own <c>DescribeTopicsOptions.partitionSizeLimitPerResponse</c> default
    /// (<c>DescribeTopicsOptions.java:28</c>), used when the caller passes no options at
    /// all so that <c>options: null</c> behaves exactly like a fresh instance.
    /// </summary>
    private const int DefaultPartitionSizeLimitPerResponse = 2000;

    /// <summary>
    /// The comparer every <see cref="ConfigResource"/>-keyed bridge and result view is
    /// built with, so a lookup in a per-key map and a lookup in an aggregate can never
    /// disagree about a key.
    /// </summary>
    /// <remarks>
    /// <see cref="EqualityComparer{T}.Default"/> dispatches to
    /// <see cref="ConfigResource.Equals(object)"/> — type plus an ordinal name — which is
    /// exactly Java's <c>equals</c>. Naming it once here is what lets the submit, the
    /// bridge and the public result all be handed the <em>same</em> instance rather than
    /// each reaching for a default that could later diverge.
    /// </remarks>
    private static readonly IEqualityComparer<ConfigResource> s_configResourceComparer =
        EqualityComparer<ConfigResource>.Default;

    /// <summary>
    /// The comparer every <see cref="TopicPartitionReplica"/>-keyed bridge and result view
    /// is built with, for the same reason as <see cref="s_configResourceComparer"/>: it is a
    /// reference type with custom value equality, so a per-key map and an aggregate built
    /// with different comparers could disagree about whether a key is present.
    /// </summary>
    private static readonly IEqualityComparer<TopicPartitionReplica> s_replicaComparer =
        EqualityComparer<TopicPartitionReplica>.Default;

    private readonly SafeAdminHandle _handle;
    private int _closed;

    private NativeAdminClient(SafeAdminHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// The ABI shape of <c>create_topics_async</c>. A method-group reference to
    /// <see cref="NativeMethods.AdminClientCreateTopicsAsync"/> binds to it directly, so
    /// production passes the real P/Invoke while a test can pass a stand-in that
    /// captures <c>user_data</c> and drives the <em>production</em> trampoline at a
    /// moment of its choosing — the only way to make "an operation is in flight"
    /// deterministic without a broker or a sleep.
    /// </summary>
    internal delegate void NativeCreateTopicsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int count,
        int timeoutMs,
        bool validateOnly,
        bool retryOnQuotaViolation,
        AdminCallbacks.CreateTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The owned client handle. Exposed for the interop tests, which read
    /// <see cref="SafeHandle.IsClosed"/> to observe when the native release actually
    /// happened; the public clients never expose it.
    /// </summary>
    /// <summary>
    /// The <c>delete_topics[_by_ids]_async</c> submit shape, injectable for the same
    /// reason as <see cref="NativeCreateTopicsSubmit"/>: "an operation is in flight" has
    /// to be a fact a test controls, not a race it hopes to win.
    /// </summary>
    internal delegate void NativeDeleteTopicsSubmit(
        IntPtr admin,
        IntPtr[] keys,
        int count,
        int timeoutMs,
        bool retryOnQuotaViolation,
        AdminCallbacks.DeleteTopicsCallback callback,
        IntPtr userData);

    /// <inheritdoc cref="NativeDeleteTopicsSubmit"/>
    internal delegate void NativeDescribeTopicsSubmit(
        IntPtr admin,
        IntPtr[] keys,
        int count,
        int timeoutMs,
        bool includeAuthorizedOperations,
        int partitionSizeLimitPerResponse,
        AdminCallbacks.DescribeTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_topics_async</c> submit shape, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>. ⚠ Note the absence of a key array — the
    /// keys are discovered from the response, which is why this RPC uses
    /// <see cref="SingleAdminOperation{TValue}"/> rather than the per-key bridge.
    /// </summary>
    internal delegate void NativeListTopicsSubmit(
        IntPtr admin,
        int timeoutMs,
        bool listInternal,
        AdminCallbacks.ListTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>create_partitions_async</c> submit shape — <b>two</b> parallel arrays,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeCreatePartitionsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        IntPtr[] newPartitions,
        int count,
        int timeoutMs,
        bool validateOnly,
        bool retryOnQuotaViolation,
        AdminCallbacks.CreatePartitionsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>delete_records_async</c> submit shape — <b>three</b> parallel arrays,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDeleteRecordsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        long[] beforeOffsets,
        int count,
        int timeoutMs,
        AdminCallbacks.DeleteRecordsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_cluster_async</c> submit shape, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>. ⚠ No key array and no key type at all — the
    /// result is four attributes of one cluster (result shape 5), so this RPC uses
    /// <see cref="SingleAdminOperation{TValue}"/>.
    /// </summary>
    internal delegate void NativeDescribeClusterSubmit(
        IntPtr admin,
        int timeoutMs,
        bool includeAuthorizedOperations,
        bool includeFencedBrokers,
        AdminCallbacks.DescribeClusterCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_config_resources_async</c> submit shape, injectable for the same reason
    /// as <see cref="NativeCreateTopicsSubmit"/>. The <c>int[]</c> carries
    /// <c>ConfigResource.Type.id()</c> codes; a <c>count</c> of 0 is the legitimate
    /// "every supported type" request, not an error.
    /// </summary>
    internal delegate void NativeListConfigResourcesSubmit(
        IntPtr admin,
        int[] resourceTypes,
        int count,
        int timeoutMs,
        AdminCallbacks.ListConfigResourcesCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_client_metrics_resources_async</c> submit shape, injectable for the same
    /// reason as <see cref="NativeCreateTopicsSubmit"/>. No arrays at all.
    /// </summary>
    internal delegate void NativeListClientMetricsResourcesSubmit(
        IntPtr admin,
        int timeoutMs,
        AdminCallbacks.ListClientMetricsResourcesCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_log_dirs_async</c> submit shape — <b>one</b> array of broker ids,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDescribeLogDirsSubmit(
        IntPtr admin,
        int[] brokers,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeLogDirsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>alter_replica_log_dirs_async</c> submit shape — <b>four</b> parallel arrays,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeAlterReplicaLogDirsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int[] brokerIds,
        IntPtr[] logDirs,
        int count,
        int timeoutMs,
        AdminCallbacks.AlterReplicaLogDirsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_replica_log_dirs_async</c> submit shape — <b>three</b> parallel
    /// arrays, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDescribeReplicaLogDirsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int[] brokerIds,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeReplicaLogDirsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_configs_async</c> submit shape — <b>two</b> parallel arrays plus two
    /// booleans, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDescribeConfigsSubmit(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        int count,
        int timeoutMs,
        bool includeSynonyms,
        bool includeDocumentation,
        AdminCallbacks.DescribeConfigsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>incremental_alter_configs_async</c> submit shape — <b>five</b> parallel
    /// arrays, one row per operation, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeIncrementalAlterConfigsSubmit(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        IntPtr[] configNames,
        IntPtr[] configValues,
        int[] opTypes,
        int count,
        int timeoutMs,
        bool validateOnly,
        AdminCallbacks.IncrementalAlterConfigsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>elect_leaders_async</c> submit shape — <b>two</b> parallel arrays plus the
    /// <c>all_partitions</c> discriminant, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>allPartitions</c> is Java's <b>null</b> partition set — "every partition in
    /// the cluster" — and is <em>not</em> an empty selection. The two are different
    /// requests and the ABI keeps them apart deliberately.
    /// </remarks>
    internal delegate void NativeElectLeadersSubmit(
        IntPtr admin,
        int electionType,
        bool allPartitions,
        IntPtr[] topics,
        int[] partitions,
        int count,
        int timeoutMs,
        AdminCallbacks.ElectLeadersCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>alter_partition_reassignments_async</c> submit shape — <b>five</b> parallel
    /// arrays, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>cancel[i]</c> is Java's <c>Optional.empty()</c> — revert that partition's
    /// reassignment — and <c>targetReplicas[i]</c> is then not read. It is a dedicated
    /// channel precisely so cancelling cannot be confused with a present-but-empty replica
    /// list, which Java rejects.
    /// </remarks>
    internal delegate void NativeAlterPartitionReassignmentsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        bool[] cancel,
        IntPtr[] targetReplicas,
        int[] targetReplicaCounts,
        int count,
        int timeoutMs,
        bool allowReplicationFactorChange,
        AdminCallbacks.AlterPartitionReassignmentsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>alter_consumer_group_offsets_async</c> submit shape — <b>six</b> parallel
    /// arrays plus the group id, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <paramref name="hasLeaderEpoch"/>[i] is Java's <c>OffsetAndMetadata.leaderEpoch()</c>
    /// <c>Optional&lt;Integer&gt;</c> presence flag; when false, <paramref name="leaderEpochs"/>[i]
    /// is not read (<c>NativeMethods.AdminClientAlterConsumerGroupOffsetsAsync</c>).
    /// </remarks>
    internal delegate void NativeAlterConsumerGroupOffsetsSubmit(
        IntPtr admin,
        IntPtr groupId,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        IntPtr[] metadata,
        int[] leaderEpochs,
        bool[] hasLeaderEpoch,
        int count,
        int timeoutMs,
        AdminCallbacks.AlterConsumerGroupOffsetsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>delete_consumer_group_offsets_async</c> submit shape — <b>two</b> parallel
    /// arrays plus the group id, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>. Simpler than
    /// <see cref="NativeAlterConsumerGroupOffsetsSubmit"/>: deleting a committed offset
    /// carries no per-partition value, so there is no offset/metadata/leader-epoch array.
    /// </summary>
    internal delegate void NativeDeleteConsumerGroupOffsetsSubmit(
        IntPtr admin,
        IntPtr groupId,
        IntPtr[] topics,
        int[] partitions,
        int count,
        int timeoutMs,
        AdminCallbacks.DeleteConsumerGroupOffsetsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_partition_reassignments_async</c> submit shape, injectable for the same
    /// reason as <see cref="NativeCreateTopicsSubmit"/>. ⚠ <c>allPartitions</c> is Java's
    /// <c>Optional.empty()</c> — every ongoing reassignment in the cluster — and is not an
    /// empty selection.
    /// </summary>
    internal delegate void NativeListPartitionReassignmentsSubmit(
        IntPtr admin,
        bool allPartitions,
        IntPtr[] topics,
        int[] partitions,
        int count,
        int timeoutMs,
        AdminCallbacks.ListPartitionReassignmentsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_offsets_async</c> submit shape — <b>four</b> parallel arrays plus the
    /// isolation level, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>isTimestamp[i]</c> travels beside <c>specTimestamps[i]</c> because the
    /// six-sentinel projection is not injective — <c>ForTimestamp(-2)</c> and
    /// <c>Earliest()</c> would otherwise be the same call.
    /// </remarks>
    internal delegate void NativeListOffsetsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        bool[] isTimestamp,
        long[] specTimestamps,
        int count,
        int timeoutMs,
        int isolationLevel,
        AdminCallbacks.ListOffsetsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_groups_async</c> submit shape — <b>three</b> name arrays, each with its
    /// own count, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>The three arrays are NOT parallel.</b> They are three independent filter axes,
    /// so their counts are unrelated and every axis is read against its own — never against
    /// the first. An axis whose count is <c>0</c> is Java's empty <c>Set</c>, "do not filter
    /// on this one", not "match nothing".
    /// </remarks>
    internal delegate void NativeListGroupsSubmit(
        IntPtr admin,
        IntPtr[] groupStates,
        int groupStateCount,
        IntPtr[] protocolTypes,
        int protocolTypeCount,
        IntPtr[] types,
        int typeCount,
        int timeoutMs,
        AdminCallbacks.ListGroupsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_consumer_groups_async</c> submit shape — <b>two</b> name arrays, each with
    /// its own count, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>Two axes, not three.</b> The deprecated predecessor of <c>listGroups</c> filters on
    /// group state and group type only — there is no protocol-type axis
    /// (<c>ListConsumerGroupsOptions.java</c> carries no such filter). Copying
    /// <see cref="NativeListGroupsSubmit"/>'s argument list here would feed <c>types</c> into the
    /// ABI's second array and shift every argument after it, including the callback pointer.
    /// <br/>
    /// ⚠ <b>The two arrays are NOT parallel</b>, exactly as in the three-axis case: their counts
    /// are unrelated and each axis is read against its own. An axis whose count is <c>0</c> is
    /// Java's empty <c>Set</c>, "do not filter on this one", not "match nothing".
    /// </remarks>
    internal delegate void NativeListConsumerGroupsSubmit(
        IntPtr admin,
        IntPtr[] groupStates,
        int groupStateCount,
        IntPtr[] types,
        int typeCount,
        int timeoutMs,
        AdminCallbacks.ListConsumerGroupsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_consumer_groups_async</c> submit shape — one name array and
    /// <b>one</b> flag, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>One flag, not the two of <see cref="NativeDescribeTopicsSubmit"/>.</b> There is
    /// no <c>partitionSizeLimitPerResponse</c> axis here — Java's
    /// <c>DescribeConsumerGroupsOptions</c> declares none — so the flag is immediately
    /// followed by the callback pointer. Reusing the topic shape would shift every argument
    /// after the flag, including the callback, which is why this gets its own declaration
    /// rather than borrowing one that merely looks close enough.
    /// </remarks>
    internal delegate void NativeDescribeConsumerGroupsSubmit(
        IntPtr admin,
        IntPtr[] groupIds,
        int count,
        int timeoutMs,
        bool includeAuthorizedOperations,
        AdminCallbacks.DescribeConsumerGroupsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_classic_groups_async</c> submit shape — argument-for-argument
    /// <see cref="NativeDescribeConsumerGroupsSubmit"/>, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ Its own declaration despite being identical in layout, because the callback
    /// parameter is not: an <see cref="AdminCallbacks.DescribeClassicGroupsCallback"/>
    /// carries a <c>DescribeClassicGroupsResult_t</c> root, and sharing the consumer
    /// delegate would make it a compile-time option to hand that root to the wrong RPC's
    /// destroy.
    /// </remarks>
    internal delegate void NativeDescribeClassicGroupsSubmit(
        IntPtr admin,
        IntPtr[] groupIds,
        int count,
        int timeoutMs,
        bool includeAuthorizedOperations,
        AdminCallbacks.DescribeClassicGroupsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The native <c>listConsumerGroupOffsets</c> submit, injectable for tests.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The only jagged submit on this surface.</b> Per group id it carries a
    /// topic-partition selection as two parallel inner arrays — <paramref name="topics"/>
    /// (UTF-8 <c>char*</c> per pair) and <paramref name="partitions"/> — sized by
    /// <paramref name="partitionCounts"/>. <paramref name="allPartitions"/> is the
    /// discriminant: <see langword="true"/> means "every partition the group has committed",
    /// and the inner pair for that index is never read. An <em>empty explicit</em> selection
    /// is the opposite request and must therefore cross with the flag
    /// <see langword="false"/> and a count of 0 — which is why the flag is a separate array
    /// and not inferable from the count.
    /// </remarks>
    internal delegate void NativeListConsumerGroupOffsetsSubmit(
        IntPtr admin,
        IntPtr[] groupIds,
        bool[] allPartitions,
        IntPtr[] topics,
        IntPtr[] partitions,
        int[] partitionCounts,
        int groupCount,
        int timeoutMs,
        bool requireStable,
        AdminCallbacks.ListConsumerGroupOffsetsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>delete_consumer_groups_async</c> submit shape — a flat group-id array, no
    /// options fields at all (<see cref="DeleteConsumerGroupsOptions"/> carries only the
    /// timeout), injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDeleteConsumerGroupsSubmit(
        IntPtr admin,
        IntPtr[] groupIds,
        int count,
        int timeoutMs,
        AdminCallbacks.DeleteConsumerGroupsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>remove_members_from_consumer_group_async</c> submit shape — one group id, a
    /// <c>removeAll</c> discriminant, and a group-instance-id array read only when that flag
    /// is <see langword="false"/>, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The discriminant, not the array, decides removeAll mode.</b> When
    /// <paramref name="removeAll"/> is <see langword="true"/>, <paramref name="groupInstanceIds"/>
    /// is <see langword="null"/> and <paramref name="memberCount"/> is 0 — Java's
    /// <c>RemoveMembersFromConsumerGroupOptions()</c> no-arg constructor
    /// (<c>removeAll = members.isEmpty()</c>). The ABI's result carries zero rows in that mode
    /// (there is no per-member outcome model at the wire level for "remove everyone"), so
    /// success/failure travels entirely through the callback's own <c>error</c> parameter — see
    /// <see cref="RemoveMembersFromConsumerGroupResult"/>'s remarks for the resulting
    /// <c>All()</c> deviation.
    /// </remarks>
    internal delegate void NativeRemoveMembersFromConsumerGroupSubmit(
        IntPtr admin,
        IntPtr groupId,
        [MarshalAs(UnmanagedType.I1)] bool removeAll,
        IntPtr[]? groupInstanceIds,
        int memberCount,
        IntPtr reason,
        int timeoutMs,
        AdminCallbacks.RemoveMembersFromConsumerGroupCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>create_acls_async</c> submit shape — seven parallel arrays sharing one count,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ The three string arrays follow <c>AclRowMarshal</c>'s null-versus-empty rule, which
    /// is invisible end to end because the Rust mock does not implement this RPC — it is
    /// therefore asserted here, at the seam.
    /// </remarks>
    internal delegate void NativeCreateAclsSubmit(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        int[] patternTypes,
        IntPtr[] principals,
        IntPtr[] hosts,
        int[] operations,
        int[] permissionTypes,
        int count,
        int timeoutMs,
        AdminCallbacks.CreateAclsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>delete_acls_async</c> submit shape — byte-identical to
    /// <see cref="NativeCreateAclsSubmit"/>, injectable for the same reason.
    /// </summary>
    /// <remarks>
    /// ⚠ Here a NULL string entry is meaningful ("match any") rather than rejected, so
    /// <c>AclRowMarshal</c>'s null-versus-empty rule is load-bearing on this path — and, as
    /// there, invisible end to end because the Rust mock does not implement this RPC.
    /// </remarks>
    internal delegate void NativeDeleteAclsSubmit(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        int[] patternTypes,
        IntPtr[] principals,
        IntPtr[] hosts,
        int[] operations,
        int[] permissionTypes,
        int count,
        int timeoutMs,
        AdminCallbacks.DeleteAclsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_acls_async</c> submit shape, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>. Java takes a <b>single</b> filter, so the
    /// seven fields cross as scalars rather than arrays.
    /// </summary>
    internal delegate void NativeDescribeAclsSubmit(
        IntPtr admin,
        int resourceType,
        IntPtr resourceName,
        int patternType,
        IntPtr principal,
        IntPtr host,
        int operation,
        int permissionType,
        int timeoutMs,
        AdminCallbacks.DescribeAclsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_client_quotas_async</c> submit shape, injectable for the same reason
    /// as <see cref="NativeCreateTopicsSubmit"/>. A <c>count</c> of 0 with
    /// <paramref name="strict"/> false is Java's <c>ClientQuotaFilter.all()</c>, not an error.
    /// </summary>
    internal delegate void NativeDescribeClientQuotasSubmit(
        IntPtr admin,
        IntPtr[] entityTypes,
        int[] matchTypes,
        IntPtr[] matchNames,
        int count,
        bool strict,
        int timeoutMs,
        AdminCallbacks.DescribeClientQuotasCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>alter_client_quotas_async</c> submit shape — the phase's only <b>ragged
    /// 2-level</b> input, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Four of the seven arrays are arrays-of-arrays: row <c>i</c>'s entity is
    /// <c>entityCounts[i]</c> <c>(type, name)</c> pairs and its ops are <c>opCounts[i]</c>
    /// triples, with the two lengths independent of each other
    /// (<c>confluent_kafka.h:8321-8332</c>).
    /// </para>
    /// <para>
    /// ⚠ <paramref name="opHasValues"/> is a <c>bool**</c> whose inner buffers are pinned
    /// <c>byte[]</c>s of 0/1 — a cleared flag is Java's <c>Op(key, null)</c>, i.e. REMOVE the
    /// quota, not "set it to 0".
    /// </para>
    /// </remarks>
    internal delegate void NativeAlterClientQuotasSubmit(
        IntPtr admin,
        IntPtr[] entityTypes,
        IntPtr[] entityNames,
        int[] entityCounts,
        IntPtr[] opKeys,
        IntPtr[] opValues,
        IntPtr[] opHasValues,
        int[] opCounts,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool validateOnly,
        AdminCallbacks.AlterClientQuotasCallback callback,
        IntPtr userData);

    // ---- M15/P7 submit shapes, injectable so the tests can assert on the marshalled rows ----

    /// <summary>The <c>describe_user_scram_credentials_async</c> submit shape.</summary>
    internal delegate void NativeDescribeUserScramCredentialsSubmit(
        IntPtr admin,
        IntPtr[] users,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeUserScramCredentialsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>alter_user_scram_credentials_async</c> submit shape — the phase's largest
    /// marshalling job at ten parallel arrays, and the one with no mock happy path, so the
    /// tests assert its rows here rather than end to end.
    /// </summary>
    internal delegate void NativeAlterUserScramCredentialsSubmit(
        IntPtr admin,
        IntPtr[] users,
        IntPtr isDeletions,
        int[] mechanisms,
        int[] iterations,
        IntPtr[] passwords,
        int[] passwordLens,
        IntPtr[] salts,
        int[] saltLens,
        IntPtr hasSalts,
        int count,
        int timeoutMs,
        AdminCallbacks.AlterUserScramCredentialsCallback callback,
        IntPtr userData);

    /// <summary>The <c>create_delegation_token_async</c> submit shape.</summary>
    internal delegate void NativeCreateDelegationTokenSubmit(
        IntPtr admin,
        IntPtr[] renewerPrincipalTypes,
        IntPtr[] renewerNames,
        int renewerCount,
        IntPtr ownerPrincipalType,
        IntPtr ownerName,
        long maxLifetimeMs,
        int timeoutMs,
        AdminCallbacks.CreateDelegationTokenCallback callback,
        IntPtr userData);

    /// <summary>The <c>renew_delegation_token_async</c> submit shape.</summary>
    internal delegate void NativeRenewDelegationTokenSubmit(
        IntPtr admin,
        IntPtr hmac,
        int hmacLength,
        long renewTimePeriodMs,
        int timeoutMs,
        AdminCallbacks.RenewDelegationTokenCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>expire_delegation_token_async</c> submit shape — byte-identical to
    /// <see cref="NativeRenewDelegationTokenSubmit"/> save for the callback type, which is what
    /// makes the two cross-wirable.
    /// </summary>
    internal delegate void NativeExpireDelegationTokenSubmit(
        IntPtr admin,
        IntPtr hmac,
        int hmacLength,
        long expiryTimePeriodMs,
        int timeoutMs,
        AdminCallbacks.ExpireDelegationTokenCallback callback,
        IntPtr userData);

    /// <summary>The <c>describe_delegation_token_async</c> submit shape.</summary>
    internal delegate void NativeDescribeDelegationTokenSubmit(
        IntPtr admin,
        [MarshalAs(UnmanagedType.I1)] bool hasOwnersFilter,
        IntPtr[] ownerPrincipalTypes,
        IntPtr[] ownerNames,
        int ownerCount,
        int timeoutMs,
        AdminCallbacks.DescribeDelegationTokenCallback callback,
        IntPtr userData);

    /// <summary>The <c>describe_features_async</c> submit shape.</summary>
    internal delegate void NativeDescribeFeaturesSubmit(
        IntPtr admin,
        [MarshalAs(UnmanagedType.I1)] bool hasNodeId,
        int nodeId,
        int timeoutMs,
        AdminCallbacks.DescribeFeaturesCallback callback,
        IntPtr userData);

    /// <summary>The <c>update_features_async</c> submit shape — <c>short</c> version levels.</summary>
    internal delegate void NativeUpdateFeaturesSubmit(
        IntPtr admin,
        IntPtr[] features,
        short[] maxVersionLevels,
        int[] upgradeTypes,
        int count,
        int timeoutMs,
        [MarshalAs(UnmanagedType.I1)] bool validateOnly,
        AdminCallbacks.UpdateFeaturesCallback callback,
        IntPtr userData);

    // ---- M15/P8 submit shapes, injectable so the tests can assert on the marshalled scalars ----

    /// <summary>
    /// The <c>abort_transaction_async</c> submit shape — no arrays, and ⚠ no result handle,
    /// so the callback is <c>(error, user_data)</c>.
    /// </summary>
    internal delegate void NativeAbortTransactionSubmit(
        IntPtr admin,
        IntPtr topic,
        int partition,
        long producerId,
        int producerEpoch,
        int coordinatorEpoch,
        int timeoutMs,
        AdminCallbacks.AbortTransactionCallback callback,
        IntPtr userData);

    /// <summary>The <c>force_terminate_transaction_async</c> submit shape.</summary>
    internal delegate void NativeForceTerminateTransactionSubmit(
        IntPtr admin,
        IntPtr transactionalId,
        int timeoutMs,
        AdminCallbacks.ForceTerminateTransactionCallback callback,
        IntPtr userData);

    /// <summary>The <c>fence_producers_async</c> submit shape.</summary>
    internal delegate void NativeFenceProducersSubmit(
        IntPtr admin,
        IntPtr[] transactionalIds,
        int count,
        int timeoutMs,
        AdminCallbacks.FenceProducersCallback callback,
        IntPtr userData);

    /// <summary>The <c>describe_transactions_async</c> submit shape.</summary>
    internal delegate void NativeDescribeTransactionsSubmit(
        IntPtr admin,
        IntPtr[] transactionalIds,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeTransactionsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_transactions_async</c> submit shape — ⚠ <b>two independent filters, each
    /// with its own count</b>.
    /// </summary>
    internal delegate void NativeListTransactionsSubmit(
        IntPtr admin,
        IntPtr[] states,
        int stateCount,
        long[] producerIds,
        int producerIdCount,
        long durationMs,
        IntPtr transactionalIdPattern,
        int timeoutMs,
        AdminCallbacks.ListTransactionsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_producers_async</c> submit shape — parallel topic/partition arrays plus
    /// the broker-id <c>OptionalInt</c>'s explicit discriminant.
    /// </summary>
    internal delegate void NativeDescribeProducersSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int count,
        bool hasBrokerId,
        int brokerId,
        int timeoutMs,
        AdminCallbacks.DescribeProducersCallback callback,
        IntPtr userData);

    internal SafeAdminHandle Handle => _handle;

    /// <summary>
    /// Creates a real admin client from a config map: each entry becomes an
    /// <c>AdminClientProperties_put</c> (keys are the Java dotted names), then
    /// <c>AdminClient_new</c> reads the properties. A construction failure surfaces as a
    /// <see cref="KafkaException"/>.
    /// </summary>
    /// <param name="config">Config keyed by Java dotted names; values are strings.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    internal static NativeAdminClient Create(IReadOnlyDictionary<string, string> config)
    {
        // Preconditions BEFORE any pin/marshal/P-Invoke (ffi §B5): the ABI does not
        // validate them and panics on violation (UB across FFI).
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        foreach (KeyValuePair<string, string> entry in config)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"Configuration value for key '{entry.Key}' must not be null.",
                    nameof(config));
            }
        }

        SafeAdminHandle handle;
        IntPtr error;

        SafeAdminPropertiesHandle props = SafeAdminPropertiesHandle.Create();
        try
        {
            foreach (KeyValuePair<string, string> entry in config)
            {
                using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                NativeMethods.AdminClientPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
            }

            // props is passed as the SafeHandle so the marshaller keeps it alive across
            // the call; the ABI does not consume it (freed below). The client handle
            // arrives ALREADY WRAPPED — the marshaller invokes SafeAdminHandle's private
            // ctor and sets the pointer atomically, closing the allocation-gap window.
            handle = NativeMethods.AdminClientNew(props, out error);
        }
        finally
        {
            // Header: the caller retains props ownership → free it after the call.
            props.Dispose();
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            // On the null native return the marshaller handed back an IsInvalid handle;
            // disposing it skips ReleaseHandle, so there is no spurious destroy.
            handle.Dispose();
            throw failure;
        }

        if (handle.IsInvalid)
        {
            // (null handle, null error) would be a core contract violation. Never store an
            // IsInvalid handle — every later call would hand native a null pointer.
            handle.Dispose();
            throw new KafkaException("kafka_admin_AdminClient_new returned a null handle without an error.");
        }

        return new NativeAdminClient(handle);
    }

    /// <summary>
    /// Creates a broker-less mock admin client. The ABI returns the same handle type as
    /// the real constructor, so the whole RPC surface works against it unchanged.
    /// </summary>
    /// <param name="numBrokers">The number of brokers to simulate; at least 1.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="numBrokers"/> is less than 1.</exception>
    /// <exception cref="KafkaException">The core could not create the mock.</exception>
    internal static NativeAdminClient CreateMock(int numBrokers)
    {
        // Validate BEFORE the call (ffi §B5). The ABI returns null for num_brokers < 1 —
        // Java's MockAdminClient.Builder.build() throw expressed in the FFI idiom — and a
        // null must be mapped, never dereferenced. Rejecting it here gives the caller the
        // .NET exception the mistake deserves instead of an opaque core error.
        if (numBrokers < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(numBrokers), numBrokers, "A mock admin client requires at least one broker.");
        }

        SafeAdminHandle handle = NativeMethods.MockAdminClientNew(numBrokers);
        if (handle.IsInvalid)
        {
            // Reachable only if the core could not create its tokio runtime — the guard
            // above already excluded the num_brokers case. There is no out_error on this
            // entry point, so the null return is all the ABI gives us.
            handle.Dispose();
            throw new KafkaException("kafka_admin_MockAdminClient_new returned a null handle.");
        }

        return new NativeAdminClient(handle);
    }

    /// <summary>
    /// Submits <c>createTopics</c> and returns immediately with one awaitable per topic
    /// (Java's non-blocking <c>createTopics</c>).
    /// </summary>
    /// <param name="newTopics">The topics to create.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    internal CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options) =>
        CreateTopics(newTopics, options, NativeMethods.AdminClientCreateTopicsAsync);

    /// <summary>
    /// The <c>createTopics</c> submit, with the native call injectable. Production calls
    /// the overload above, which supplies the real P/Invoke; the interop tests supply a
    /// stand-in so the in-flight window is deterministic. Everything else — validation,
    /// the per-key sources, the <c>GCHandle</c>, the span-the-op reference, the input
    /// handles' lifetime — is the one production path either way, so a test cannot
    /// accidentally prove a property of its own fixture.
    /// </summary>
    internal CreateTopicsResult CreateTopics(
        IEnumerable<NewTopic> newTopics,
        CreateTopicsOptions? options,
        NativeCreateTopicsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (newTopics is null)
        {
            throw new ArgumentNullException(nameof(newTopics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(CreateTopicsOptions));
            validateOnly = options.ValidateOnly;
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        // Java keys its result on a Map and skips a repeated name (KafkaAdminClient
        // populates `topicFutures` only `if (!topicFutures.containsKey(...))`), so a
        // duplicate is one entry here too — and the request array is de-duplicated with
        // it, so the two sides cannot disagree about how many topics were asked for.
        List<NewTopic> requested = new List<NewTopic>();
        List<string> keys = new List<string>();
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (NewTopic topic in newTopics)
        {
            if (topic is null)
            {
                throw new ArgumentException("The topics to create must not contain a null element.", nameof(newTopics));
            }

            if (topic.Configs is not null)
            {
                foreach (KeyValuePair<string, string> entry in topic.Configs)
                {
                    if (entry.Value is null)
                    {
                        throw new ArgumentException(
                            $"Configuration value for key '{entry.Key}' on topic '{topic.Name}' must not be null.",
                            nameof(newTopics));
                    }
                }
            }

            if (seen.Add(topic.Name))
            {
                requested.Add(topic);
                keys.Add(topic.Name);
            }
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        // The header requires it, and the inline-callback path makes it real: the callback
        // can run on this very thread before the entry point returns.
        KeyedAdminOperation<string, TopicMetadataAndConfig> operation =
            new KeyedAdminOperation<string, TopicMetadataAndConfig>(
                "createTopics", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        // Deliberately EMPTY here and allocated inside the try. Everything between the
        // GCHandle allocation above and the try is a window in which a throw would root
        // the operation for the process lifetime, because neither the catch nor the
        // finally covers it — so the window is kept to nothing at all.
        // Array.Empty allocates nothing.
        IntPtr[] handles = Array.Empty<IntPtr>();
        try
        {
            handles = new IntPtr[requested.Count];

            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever. It
            // keeps the native client alive from here until the completion callback
            // releases it — the binding's whole defence against the ABI's unguarded
            // AdminClient_destroy (see AdminOperation / SafeAdminHandle).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            for (int i = 0; i < requested.Count; i++)
            {
                handles[i] = NewTopicMarshal.Build(requested[i]);
            }

            // Result shape 4a: one callback per topic. The ABI fires "exactly `count`
            // times (once per requested topic, minus NULL entries in `topics`)"; no entry
            // is null here (a null NewTopic was rejected above), so `count` is the number.
            // Armed before the submit — every key can fire inline on this thread.
            operation.SetPendingCallbacks(handles.Length);

            submit(
                _handle.DangerousGetHandle(),
                handles,
                handles.Length,
                timeoutMs,
                validateOnly,
                retryOnQuotaViolation,
                AdminCallbacks.CreateTopics,
                GCHandle.ToIntPtr(gcHandle));

            // The submit's own countdown slot. Releasing it here is what makes an EMPTY
            // topic collection — zero callbacks — release rather than leak.
            operation.ReleaseSubmitToken();
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            // Idempotent, so it is harmless even in the (unreachable) case where an
            // inline callback already ran before the throw.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The ABI copies out during the submit and "the caller retains ownership" of
            // the input entries, so they are destroyed here — after the call, on every
            // path, including a partially built array. Null-safe.
            foreach (IntPtr handle in handles)
            {
                NativeMethods.NewTopicDestroy(handle);
            }
        }

        return new CreateTopicsResult(operation.Tasks);
    }

    internal DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options) =>
        DeleteTopics(
            topics,
            options,
            NativeMethods.AdminClientDeleteTopicsAsync,
            NativeMethods.AdminClientDeleteTopicsByIdsAsync);

    /// <summary>
    /// <b>Entry-point selection is the whole point of <see cref="TopicCollection"/>.</b>
    /// The ABI gives the two forms separate functions rather than a tagged input struct,
    /// so "names xor ids" cannot be violated; this switch is where the C# type's two
    /// inhabitants are mapped onto them.
    /// </summary>
    internal DeleteTopicsResult DeleteTopics(
        TopicCollection topics,
        DeleteTopicsOptions? options,
        NativeDeleteTopicsSubmit submitByName,
        NativeDeleteTopicsSubmit submitByIds)
    {
        ThrowIfClosed();

        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteTopicsOptions));
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        switch (topics)
        {
            case TopicCollection.TopicNameCollection names:
                {
                    List<string> keys = DistinctNames(names.TopicNames(), "topic names", nameof(topics));
                    VoidKeyedAdminOperation<string> operation = new VoidKeyedAdminOperation<string>(
                        "deleteTopics", keys, StringComparer.Ordinal);

                    Submit(
                        operation,
                        keys,
                        (admin, pinned, count, callbackUserData) =>
                        {
                            // Result shape 4b: one callback per topic. The ABI fires
                            // "exactly `count` times (minus NULL entries in `names`)", and
                            // a pinned key is never NULL — so `count` is the number. Armed
                            // before the submit: every key can fire inline on this thread.
                            operation.SetPendingCallbacks(count);
                            submitByName(
                                admin,
                                pinned,
                                count,
                                timeoutMs,
                                retryOnQuotaViolation,
                                AdminCallbacks.DeleteTopicsByName,
                                callbackUserData);

                            // The submit's own slot — what makes an EMPTY name collection
                            // (zero callbacks) release instead of leaking.
                            operation.ReleaseSubmitToken();
                        });

                    return DeleteTopicsResult.OfTopicNames(operation.Tasks, operation.KeyComparer);
                }

            case TopicCollection.TopicIdCollection ids:
                {
                    List<Uuid> keys = DistinctIds(ids.TopicIds());
                    VoidKeyedAdminOperation<Uuid> operation = new VoidKeyedAdminOperation<Uuid>(
                        "deleteTopics", keys, EqualityComparer<Uuid>.Default);

                    Submit(
                        operation,
                        ToBase64(keys),
                        (admin, pinned, count, callbackUserData) =>
                        {
                            // Shape 4b: "exactly `count` times"; `keys` is already
                            // DistinctIds, so `count` is the number.
                            operation.SetPendingCallbacks(count);
                            submitByIds(
                                admin,
                                pinned,
                                count,
                                timeoutMs,
                                retryOnQuotaViolation,
                                AdminCallbacks.DeleteTopicsById,
                                callbackUserData);

                            operation.ReleaseSubmitToken();
                        });

                    return DeleteTopicsResult.OfTopicIds(operation.Tasks, operation.KeyComparer);
                }

            default:
                throw UnreachableCollection(nameof(topics));
        }
    }

    internal DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options) =>
        DescribeTopics(
            topics,
            options,
            NativeMethods.AdminClientDescribeTopicsAsync,
            NativeMethods.AdminClientDescribeTopicsByIdsAsync);

    /// <inheritdoc cref="DeleteTopics(TopicCollection, DeleteTopicsOptions, NativeDeleteTopicsSubmit, NativeDeleteTopicsSubmit)"/>
    internal DescribeTopicsResult DescribeTopics(
        TopicCollection topics,
        DescribeTopicsOptions? options,
        NativeDescribeTopicsSubmit submitByName,
        NativeDescribeTopicsSubmit submitByIds)
    {
        ThrowIfClosed();

        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool includeAuthorizedOperations = false;
        int partitionSizeLimitPerResponse = DefaultPartitionSizeLimitPerResponse;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeTopicsOptions));

            if (options.PartitionSizeLimitPerResponse < 0)
            {
                // The ABI reads a negative as "keep Java's 2000 default"
                // (src/ffi/admin.rs:3712), so a negative would be silently reinterpreted
                // rather than honoured — the same reasoning as the timeout guard.
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options.PartitionSizeLimitPerResponse,
                    "DescribeTopicsOptions.PartitionSizeLimitPerResponse must not be negative.");
            }

            includeAuthorizedOperations = options.IncludeAuthorizedOperations;
            partitionSizeLimitPerResponse = options.PartitionSizeLimitPerResponse;
        }

        switch (topics)
        {
            case TopicCollection.TopicNameCollection names:
                {
                    List<string> keys = DistinctNames(names.TopicNames(), "topic names", nameof(topics));
                    KeyedAdminOperation<string, TopicDescription> operation =
                        new KeyedAdminOperation<string, TopicDescription>(
                            "describeTopics", keys, StringComparer.Ordinal);

                    Submit(
                        operation,
                        keys,
                        (admin, pinned, count, callbackUserData) =>
                        {
                            // Shape 4a: the ABI fires "exactly `count` times (minus NULL
                            // entries in `names`)" and a pinned key is never NULL, so
                            // `count` is the number. Armed before the submit — every key
                            // can fire inline on this thread; the submit's own token is
                            // released after, which is what makes an EMPTY collection
                            // (zero callbacks) release instead of leaking.
                            operation.SetPendingCallbacks(count);
                            submitByName(
                                admin,
                                pinned,
                                count,
                                timeoutMs,
                                includeAuthorizedOperations,
                                partitionSizeLimitPerResponse,
                                AdminCallbacks.DescribeTopicsByName,
                                callbackUserData);
                            operation.ReleaseSubmitToken();
                        });

                    return DescribeTopicsResult.OfTopicNames(operation.Tasks, operation.KeyComparer);
                }

            case TopicCollection.TopicIdCollection ids:
                {
                    List<Uuid> keys = DistinctIds(ids.TopicIds());
                    KeyedAdminOperation<Uuid, TopicDescription> operation =
                        new KeyedAdminOperation<Uuid, TopicDescription>(
                            "describeTopics", keys, EqualityComparer<Uuid>.Default);

                    Submit(
                        operation,
                        ToBase64(keys),
                        (admin, pinned, count, callbackUserData) =>
                        {
                            // Shape 4a: the ABI fires "exactly `count` times", including
                            // when a base64 id fails to parse (the whole call then fails
                            // and every key gets that error). See the by-name arm.
                            operation.SetPendingCallbacks(count);
                            submitByIds(
                                admin,
                                pinned,
                                count,
                                timeoutMs,
                                includeAuthorizedOperations,
                                partitionSizeLimitPerResponse,
                                AdminCallbacks.DescribeTopicsById,
                                callbackUserData);
                            operation.ReleaseSubmitToken();
                        });

                    return DescribeTopicsResult.OfTopicIds(operation.Tasks, operation.KeyComparer);
                }

            default:
                throw UnreachableCollection(nameof(topics));
        }
    }

    internal ListTopicsResult ListTopics(ListTopicsOptions? options) =>
        ListTopics(options, NativeMethods.AdminClientListTopicsAsync);

    /// <summary>
    /// Submits <c>listTopics</c> and returns immediately with the <b>single</b> awaitable
    /// Java's <c>ListTopicsResult</c> wraps (result shape 3).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>No keys are pre-registered, because there are none to register.</b> The ABI
    /// entry point takes no key array — the topics are discovered from the response — so
    /// the per-key bridge, which builds one source per requested key before the submit,
    /// cannot express this RPC. <see cref="SingleAdminOperation{TValue}"/> reuses every
    /// rooting invariant and differs only in the completion payload.
    /// </remarks>
    internal ListTopicsResult ListTopics(ListTopicsOptions? options, NativeListTopicsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        bool listInternal = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListTopicsOptions));
            listInternal = options.ListInternal;
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation =
            new SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>("listTopics");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                timeoutMs,
                listInternal,
                AdminCallbacks.ListTopics,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new ListTopicsResult(operation.Task);
    }

    internal CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options) =>
        CreatePartitions(newPartitions, options, NativeMethods.AdminClientCreatePartitionsAsync);

    /// <summary>
    /// Submits <c>createPartitions</c> and returns immediately with one awaitable per
    /// topic. Java's <c>Map&lt;String, NewPartitions&gt;</c> becomes the ABI's two
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// No de-duplication step: the input <em>is</em> a map, so its keys are already
    /// distinct — under the caller's own comparer, and therefore under the finer
    /// <see cref="StringComparer.Ordinal"/> the bridge keys by.
    /// </remarks>
    internal CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions,
        CreatePartitionsOptions? options,
        NativeCreatePartitionsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (newPartitions is null)
        {
            throw new ArgumentNullException(nameof(newPartitions));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(CreatePartitionsOptions));
            validateOnly = options.ValidateOnly;
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        List<string> keys = new List<string>(newPartitions.Count);
        List<NewPartitions> requested = new List<NewPartitions>(newPartitions.Count);
        foreach (KeyValuePair<string, NewPartitions> entry in newPartitions)
        {
            // The header requires `count` valid C strings and `count` valid entries, and
            // the ABI does not validate its own preconditions (ffi §B5). A null on either
            // side makes the ABI *skip that pair*, which would silently drop a topic the
            // caller asked for and leave its awaiter to FailUncompleted.
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The new-partitions map must not contain a null topic name.", nameof(newPartitions));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The new-partitions entry for topic '{entry.Key}' must not be null.", nameof(newPartitions));
            }

            keys.Add(entry.Key);
            requested.Add(entry.Value);
        }

        VoidKeyedAdminOperation<string> operation =
            new VoidKeyedAdminOperation<string>("createPartitions", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        // Both deliberately EMPTY/null here and allocated inside the try, as CreateTopics
        // does: everything between the GCHandle allocation above and the try is a window
        // in which a throw would root the operation for the process lifetime, because
        // neither the catch nor the finally covers it.
        IntPtr[] handles = Array.Empty<IntPtr>();
        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            handles = new IntPtr[requested.Count];
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(keys[i]);
                pinned.Add(key);
                topics[i] = key.Pointer;
            }

            for (int i = 0; i < requested.Count; i++)
            {
                handles[i] = NewPartitionsMarshal.Build(requested[i]);
            }

            // Shape 4b: one callback per distinct topic, skipping NULL-paired entries —
            // and neither side can be null here (both are rejected above), so the row
            // count is the number. `newPartitions` is a map, so its keys are distinct.
            operation.SetPendingCallbacks(handles.Length);

            submit(
                _handle.DangerousGetHandle(),
                topics,
                handles,
                handles.Length,
                timeoutMs,
                validateOnly,
                retryOnQuotaViolation,
                AdminCallbacks.CreatePartitions,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The ABI copies out during the submit and "the caller retains ownership" of
            // the input entries, so they are destroyed here — after the call, on every
            // path, including a partially built array. Null-safe.
            foreach (IntPtr handle in handles)
            {
                NativeMethods.NewPartitionsDestroy(handle);
            }

            // The key strings are pinned only for the call (ffi §A4's call-scoped rule):
            // the ABI copies them out during the submit.
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String key in pinned)
                {
                    key.Dispose();
                }
            }
        }

        return new CreatePartitionsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options) =>
        DeleteRecords(recordsToDelete, options, NativeMethods.AdminClientDeleteRecordsAsync);

    /// <summary>
    /// Submits <c>deleteRecords</c> and returns immediately with one awaitable per topic
    /// partition. Java's <c>Map&lt;TopicPartition, RecordsToDelete&gt;</c> becomes the
    /// ABI's three parallel arrays — <c>topics</c>, <c>partitions</c>,
    /// <c>before_offsets</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>RecordsToDelete</c> carries only an offset, so — unlike <c>NewPartitions</c> —
    /// it needs no input handle and nothing here has to be destroyed afterwards.
    /// </para>
    /// <para>
    /// An offset of <c>-1</c> is passed through unchanged: it is Java's documented
    /// "truncate to the high watermark", a <em>value</em> rather than an unset sentinel,
    /// so the negative-value guard that applies to timeouts deliberately does not apply
    /// here.
    /// </para>
    /// </remarks>
    internal DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options,
        NativeDeleteRecordsSubmit submit)
    {
        ThrowIfClosed();

        if (recordsToDelete is null)
        {
            throw new ArgumentNullException(nameof(recordsToDelete));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteRecordsOptions));
        }

        List<TopicPartition> keys = new List<TopicPartition>(recordsToDelete.Count);
        int[] partitions = new int[recordsToDelete.Count];
        long[] beforeOffsets = new long[recordsToDelete.Count];
        int next = 0;
        foreach (KeyValuePair<TopicPartition, RecordsToDelete> entry in recordsToDelete)
        {
            // A `default(TopicPartition)` has a null Topic, and the header's "an entry
            // with a NULL topic is skipped" would silently drop it (ffi §B5).
            if (entry.Key.Topic is null)
            {
                throw new ArgumentException(
                    "The records-to-delete map must not contain a topic partition with a null topic.",
                    nameof(recordsToDelete));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The records-to-delete entry for '{entry.Key}' must not be null.",
                    nameof(recordsToDelete));
            }

            keys.Add(entry.Key);
            partitions[next] = entry.Key.Partition;
            beforeOffsets[next] = entry.Value.BeforeOffset();
            next++;
        }

        // EqualityComparer<TopicPartition>.Default dispatches to the struct's own
        // IEquatable implementation (ordinal on the topic), so it neither boxes nor
        // disagrees with the public DeleteRecordsResult view — the same reasoning as the
        // Uuid-keyed deleteTopics path.
        KeyedAdminOperation<TopicPartition, DeletedRecords> operation =
            new KeyedAdminOperation<TopicPartition, DeletedRecords>(
                "deleteRecords", keys, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
            }

            // Shape 4a: the ABI fires once per distinct (topic, partition) pair, skipping
            // NULL topics — and `keys` is already distinct (a map's key set) with no null
            // topic (rejected above), so that is keys.Count. Armed before the submit —
            // every key can fire inline on this thread; the submit's own token is released
            // after, which is what makes an EMPTY map (zero callbacks) release instead of
            // leaking.
            operation.SetPendingCallbacks(keys.Count);

            // The blittable int[] / long[] are pinned by the interop marshaller for the
            // duration of the call; the ABI copies out during it (ffi §A4 call-scoped).
            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                beforeOffsets,
                keys.Count,
                timeoutMs,
                AdminCallbacks.DeleteRecords,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new DeleteRecordsResult(operation.Tasks);
    }

    internal DescribeClusterResult DescribeCluster(DescribeClusterOptions? options) =>
        DescribeCluster(options, NativeMethods.AdminClientDescribeClusterAsync);

    /// <summary>
    /// Submits <c>describeCluster</c> and returns immediately with the four awaitables
    /// Java's <c>DescribeClusterResult</c> exposes (result shape 5).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>One completion, four projections.</b> Java holds four independent
    /// <c>KafkaFuture</c> fields; the ABI settles the whole result together and has no
    /// <c>KafkaFuture</c> type with which to express independent timing, so the four public
    /// tasks derive from this single <see cref="SingleAdminOperation{TValue}"/> over an
    /// internal snapshot. The deviation is recorded on
    /// <see cref="DescribeClusterResult"/> (M15/P3 decision D12 — there is deliberately no
    /// public aggregate type).
    /// </remarks>
    internal DescribeClusterResult DescribeCluster(
        DescribeClusterOptions? options, NativeDescribeClusterSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        bool includeAuthorizedOperations = false;
        bool includeFencedBrokers = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeClusterOptions));
            includeAuthorizedOperations = options.IncludeAuthorizedOperations;
            includeFencedBrokers = options.IncludeFencedBrokers;
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<DescribeClusterSnapshot> operation =
            new SingleAdminOperation<DescribeClusterSnapshot>("describeCluster");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                timeoutMs,
                includeAuthorizedOperations,
                includeFencedBrokers,
                AdminCallbacks.DescribeCluster,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new DescribeClusterResult(operation.Task);
    }

    internal ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes, ListConfigResourcesOptions? options) =>
        ListConfigResources(configResourceTypes, options, NativeMethods.AdminClientListConfigResourcesAsync);

    /// <summary>
    /// Submits <c>listConfigResources</c> and returns immediately with the <b>single</b>
    /// awaitable Java's <c>ListConfigResourcesResult</c> wraps (result sub-shape 3b).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>An empty or absent type filter is the "every supported type" request and must
    /// NOT be rejected.</b> The header: "pass NULL or <c>count == 0</c> for Java's empty
    /// set, which means 'every supported type'", matching Java's no-argument
    /// <c>listConfigResources()</c>, which delegates with <c>Set.of()</c>
    /// (<c>Admin.java:1812</c>). So there is deliberately no emptiness guard and no
    /// <c>?? throw</c> here — either would turn Java's most common call into an error.
    /// </para>
    /// <para>
    /// The types are de-duplicated because Java's parameter is a <c>Set</c>. Request order
    /// is preserved among the survivors; it does not reach the result, whose entries the
    /// ABI sorts by <c>(type id, name)</c>.
    /// </para>
    /// </remarks>
    internal ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes,
        ListConfigResourcesOptions? options,
        NativeListConfigResourcesSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListConfigResourcesOptions));
        }

        int[] resourceTypes = DistinctTypeIds(configResourceTypes);

        SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ConfigResource>>("listConfigResources");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // The blittable int[] is pinned by the interop marshaller for the duration of
            // the call; the ABI copies out during it (ffi §A4 call-scoped).
            submit(
                _handle.DangerousGetHandle(),
                resourceTypes,
                resourceTypes.Length,
                timeoutMs,
                AdminCallbacks.ListConfigResources,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new ListConfigResourcesResult(operation.Task);
    }

#pragma warning disable CS0618 // Java deprecates this RPC and its three types; mirrored, not avoided.

    internal ListClientMetricsResourcesResult ListClientMetricsResources(
        ListClientMetricsResourcesOptions? options) =>
        ListClientMetricsResources(options, NativeMethods.AdminClientListClientMetricsResourcesAsync);

    /// <summary>
    /// Submits <c>listClientMetricsResources</c> and returns immediately with the
    /// <b>single</b> awaitable Java's <c>ListClientMetricsResourcesResult</c> wraps (result
    /// sub-shape 3b).
    /// </summary>
    /// <remarks>
    /// Java deprecates this RPC in favour of <c>listConfigResources</c> filtered to
    /// <c>CLIENT_METRICS</c> (<c>Admin.java:1821-1824</c>); it is bound for parity, and the
    /// deprecation is carried onto the public surface rather than dropped.
    /// </remarks>
    internal ListClientMetricsResourcesResult ListClientMetricsResources(
        ListClientMetricsResourcesOptions? options,
        NativeListClientMetricsResourcesSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListClientMetricsResourcesOptions));
        }

        SingleAdminOperation<IReadOnlyCollection<ClientMetricsResourceListing>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ClientMetricsResourceListing>>(
                "listClientMetricsResources");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                timeoutMs,
                AdminCallbacks.ListClientMetricsResources,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new ListClientMetricsResourcesResult(operation.Task);
    }

#pragma warning restore CS0618

    internal DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options) =>
        DescribeConfigs(resources, options, NativeMethods.AdminClientDescribeConfigsAsync);

    /// <summary>
    /// Submits <c>describeConfigs</c> and returns immediately with one awaitable per
    /// resource. Java's <c>Collection&lt;ConfigResource&gt;</c> becomes the ABI's two
    /// parallel arrays — <c>resource_types</c> (Java's <c>Type.id()</c> codes) and
    /// <c>resource_names</c>.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>, so a repeated resource is
    /// one entry — the same reasoning as <c>createTopics</c>. The null-element check is
    /// mandatory rather than defensive: the header says "an entry with a NULL name is
    /// skipped", silently, which would drop a resource whose <see cref="Task"/> the caller
    /// is holding (ffi §B5).
    /// </remarks>
    internal DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources,
        DescribeConfigsOptions? options,
        NativeDescribeConfigsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (resources is null)
        {
            throw new ArgumentNullException(nameof(resources));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool includeSynonyms = false;
        bool includeDocumentation = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeConfigsOptions));
            includeSynonyms = options.IncludeSynonyms;
            includeDocumentation = options.IncludeDocumentation;
        }

        List<ConfigResource> keys = DistinctResources(resources, nameof(resources));

        int[] resourceTypes = new int[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            resourceTypes[i] = (int)keys[i].Type;
        }

        KeyedAdminOperation<ConfigResource, Config> operation =
            new KeyedAdminOperation<ConfigResource, Config>(
                "describeConfigs", keys, s_configResourceComparer);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] resourceNames = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(keys[i].Name);
                pinned.Add(name);
                resourceNames[i] = name.Pointer;
            }

            // Shape 4a: the ABI fires "exactly once per DISTINCT requested resource",
            // and `keys` is already distinct (DistinctResources above), so that is
            // keys.Count. Armed before the submit; the submit's own token is released
            // after, which is what makes an empty collection release instead of leaking.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                resourceTypes,
                resourceNames,
                keys.Count,
                timeoutMs,
                includeSynonyms,
                includeDocumentation,
                AdminCallbacks.DescribeConfigs,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String name in pinned)
                {
                    name.Dispose();
                }
            }
        }

        return new DescribeConfigsResult(operation.Tasks, operation.KeyComparer);
    }

    internal AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options) =>
        IncrementalAlterConfigs(configs, options, NativeMethods.AdminClientIncrementalAlterConfigsAsync);

    /// <summary>
    /// Submits <c>incrementalAlterConfigs</c> and returns immediately with one awaitable
    /// per resource. Java's <c>Map&lt;ConfigResource, Collection&lt;AlterConfigOp&gt;&gt;</c>
    /// becomes the ABI's <b>five parallel arrays, one row per operation</b>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Rows for one resource are emitted contiguously, in the caller's op order</b> —
    /// the header requires it ("rows naming the same resource are grouped in order"), and
    /// the flattening below walks the map resource-by-resource so two resources can never
    /// interleave.
    /// </para>
    /// <para>
    /// ⚠ <b>A null config value is passed through as a null pointer.</b> It is the value
    /// <see cref="AlterConfigOpType.Delete"/> uses, and the header names it. There is
    /// deliberately no <c>?? string.Empty</c> anywhere on this path: an empty value and an
    /// absent value are different requests.
    /// </para>
    /// <para>
    /// ⚠ <b>A null resource name or config name is rejected here</b>, because the ABI
    /// <em>silently skips</em> such a row — the caller would be left holding a
    /// <see cref="Task"/> for a resource the broker was never asked about
    /// (<c>FailUncompleted</c> would fault it, but with a far less useful message).
    /// </para>
    /// <para>
    /// ⚠⚠ <b>A resource mapped to an EMPTY operation collection completes successfully
    /// LOCALLY, and that is a recorded divergence — not a full fix</b>
    /// (<c>definition-of-done.md</c> §7; M15/P3 round 3, finding 69.6). Java keys its
    /// futures on the <em>resource collection</em>, which it sends alongside the ops map
    /// (<c>KafkaAdminClient.java:2889-2896</c>, <c>:2902</c>), so the broker <b>does</b> hear
    /// about a zero-op resource and answers for it. The ABI request is
    /// <b>row-flattened</b> — one row per operation — so a zero-op resource contributes no
    /// row and is <b>absent from the request entirely</b>
    /// (<c>src/ffi/admin.rs:4233-4260</c> builds the resource map from rows alone). There is
    /// no encoding for it: a row with a null config name is <em>skipped</em> by the ABI, and
    /// any non-null config name would be a real operation.
    /// </para>
    /// <para>
    /// <b>The root of the divergence is single and stated once: the resource is never
    /// sent, so any answer the broker would have given for it is lost.</b> Local completion
    /// therefore reproduces Java's outcome for a resource that exists and is authorized.
    /// Four instances where it does not, each independently checkable — this is a list of
    /// what was found, not a claim that nothing else follows from the root:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>The resource does not exist.</b> Java's future fails; here it succeeds. Evidence
    /// that this is a real answer rather than a hypothetical: the Rust core checks the
    /// resource <em>before</em> applying any operation — <c>mock_admin_client.rs:630-636</c>
    /// resolves the topic and returns <c>UnknownTopicOrPartition</c> "No such topic as {name}"
    /// on the way to a no-op <c>apply_alter_ops</c> — so with a zero-op list the core would
    /// still fail it, exactly as Java does. Only the FFI encoding loses it.
    /// </item>
    /// <item>
    /// <b>Authorization fails for the resource.</b> Java sends it and surfaces the broker's
    /// per-resource authorization error; here nothing is asked, so it succeeds.
    /// </item>
    /// <item>
    /// <b><see cref="AlterConfigsOptions.ValidateOnly"/> is set.</b> This is the case a
    /// caller most plausibly reaches with an empty collection — "validate this resource,
    /// change nothing" — and it is the case local completion answers without validating
    /// anything.
    /// </item>
    /// <item>
    /// <b>The call itself cannot be submitted</b> (M15/P9 CP6). The per-key ABI fans a
    /// submit failure out over the resources it named, which a zero-op resource is not
    /// among — so it completes successfully while every other key faults. Under the
    /// aggregate callback this key faulted with the rest.
    /// </item>
    /// </list>
    /// <para>
    /// Closing the root needs a way to express a zero-operation resource in the request,
    /// which is a <b>Rust-core (Mode-B) dependency</b> and is escalated as such rather than worked around further. The core
    /// already behaves correctly; only the row encoding cannot carry it. Faulting the
    /// awaitable instead was the shipped behaviour and was worse — it reported a defect for
    /// a call Java accepts — and rejecting the input outright is not open, because Java
    /// accepts it too.
    /// </para>
    /// </remarks>
    internal AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options,
        NativeIncrementalAlterConfigsSubmit submit)
    {
        ThrowIfClosed();

        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterConfigsOptions));
            validateOnly = options.ValidateOnly;
        }

        // ---- Flatten the map to one row per operation, grouped by resource ----
        List<ConfigResource> keys = new List<ConfigResource>(configs.Count);
        List<ConfigResource> rowResources = new List<ConfigResource>();
        List<AlterConfigOp> rowOps = new List<AlterConfigOp>();

        // ⚠ Resources the ABI request cannot carry. `configs` is a map, so each key appears
        // once, and every operation becomes exactly one row — so "the collection is empty"
        // IS "contributes no row". See the divergence note on this method.
        List<ConfigResource> keysWithNoRequest = new List<ConfigResource>();
        foreach (KeyValuePair<ConfigResource, IReadOnlyCollection<AlterConfigOp>> entry in configs)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The configs map must not contain a null resource.", nameof(configs));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The operations for '{entry.Key}' must not be null.", nameof(configs));
            }

            // The header skips a row whose resource name or config name is NULL. Neither
            // can be null here: ConfigResource's and ConfigEntry's constructors both reject
            // a null name, so the guard lives there rather than being restated per row —
            // a second check would only shadow the one that actually runs.

            keys.Add(entry.Key);
            if (entry.Value.Count == 0)
            {
                keysWithNoRequest.Add(entry.Key);
            }

            foreach (AlterConfigOp op in entry.Value)
            {
                if (op is null)
                {
                    throw new ArgumentException(
                        $"The operations for '{entry.Key}' must not contain a null element.", nameof(configs));
                }

                rowResources.Add(entry.Key);
                rowOps.Add(op);
            }
        }

        int rowCount = rowOps.Count;
        int[] resourceTypes = new int[rowCount];
        int[] opTypes = new int[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            resourceTypes[i] = (int)rowResources[i].Type;
            opTypes[i] = (int)rowOps[i].OpType;
        }

        VoidKeyedAdminOperation<ConfigResource> operation =
            new VoidKeyedAdminOperation<ConfigResource>(
                "incrementalAlterConfigs", keys, s_configResourceComparer);

        // Registering these makes the completion resolve them successfully instead of
        // letting FailUncompleted fault them — see the divergence note above this method.
        operation.SetKeysWithNoRequest(keysWithNoRequest);

        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(rowCount * 3);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] resourceNames = new IntPtr[rowCount];
            IntPtr[] configNames = new IntPtr[rowCount];
            IntPtr[] configValues = new IntPtr[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                Utf8Marshal.PinnedUtf8String resourceName = Utf8Marshal.Pin(rowResources[i].Name);
                pinned.Add(resourceName);
                resourceNames[i] = resourceName.Pointer;

                Utf8Marshal.PinnedUtf8String configName = Utf8Marshal.Pin(rowOps[i].ConfigEntry.Name);
                pinned.Add(configName);
                configNames[i] = configName.Pointer;

                // ⚠ A null value stays a NULL POINTER — it is DELETE's null value, and the
                // ABI documents it as such. No `?? string.Empty` here, ever.
                string? value = rowOps[i].ConfigEntry.Value;
                if (value is null)
                {
                    configValues[i] = IntPtr.Zero;
                }
                else
                {
                    Utf8Marshal.PinnedUtf8String configValue = Utf8Marshal.Pin(value);
                    pinned.Add(configValue);
                    configValues[i] = configValue.Pointer;
                }
            }

            // ⚠ Shape 4b, and the ONE RPC in the phase whose callback count is neither the
            // key count nor the row count: the ABI fires once per DISTINCT RESOURCE NAMED
            // ACROSS THE ROWS (`distinct_config_resources`). Every resource is named by at
            // least one row except the zero-op ones, which contribute none — so the count
            // is exactly the keys minus those, and they are resolved locally at countdown
            // zero (VoidKeyedAdminOperation.OnAllCallbacksComplete) instead.
            operation.SetPendingCallbacks(keys.Count - keysWithNoRequest.Count);

            submit(
                _handle.DangerousGetHandle(),
                resourceTypes,
                resourceNames,
                configNames,
                configValues,
                opTypes,
                rowCount,
                timeoutMs,
                validateOnly,
                AdminCallbacks.IncrementalAlterConfigs,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String value in pinned)
                {
                    value.Dispose();
                }
            }
        }

        return new AlterConfigsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options) =>
        DescribeLogDirs(brokers, options, NativeMethods.AdminClientDescribeLogDirsAsync);

    /// <summary>
    /// Submits <c>describeLogDirs</c> and returns immediately with one awaitable per broker.
    /// Java's <c>Collection&lt;Integer&gt;</c> becomes the ABI's single broker-id array.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c> keyed by broker id, so a
    /// repeated broker is one entry. There is no null-element check because the element type
    /// is <see cref="int"/> — there is no null to reject, which is also why this is the one
    /// Stage-3 input with no silent-skip hazard.
    /// </remarks>
    internal DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers,
        DescribeLogDirsOptions? options,
        NativeDescribeLogDirsSubmit submit)
    {
        ThrowIfClosed();

        if (brokers is null)
        {
            throw new ArgumentNullException(nameof(brokers));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeLogDirsOptions));
        }

        List<int> keys = new List<int>(brokers.Count);
        HashSet<int> seen = new HashSet<int>();
        foreach (int broker in brokers)
        {
            if (seen.Add(broker))
            {
                keys.Add(broker);
            }
        }

        KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>> operation =
            new KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>>(
                "describeLogDirs", keys, EqualityComparer<int>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // Shape 4a: the ABI fires once per DISTINCT broker id, and `keys` is already
            // distinct (the HashSet above), so that is keys.Count. The submit's own token
            // is released after, so an empty collection releases instead of leaking.
            operation.SetPendingCallbacks(keys.Count);

            // The blittable int[] is pinned by the interop marshaller for the duration of
            // the call; the ABI copies out during it (ffi §A4 call-scoped).
            submit(
                _handle.DangerousGetHandle(),
                keys.ToArray(),
                keys.Count,
                timeoutMs,
                AdminCallbacks.DescribeLogDirs,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new DescribeLogDirsResult(operation.Tasks);
    }

    internal AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options) =>
        AlterReplicaLogDirs(
            replicaAssignment, options, NativeMethods.AdminClientAlterReplicaLogDirsAsync);

    /// <summary>
    /// Submits <c>alterReplicaLogDirs</c> and returns immediately with one awaitable per
    /// replica. Java's <c>Map&lt;TopicPartitionReplica, String&gt;</c> becomes the ABI's four
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>§9.1 item 9 (KEY-SET vs ROW-SET) was checked here BEFORE the RPC was written,
    /// and the zero-row shape CANNOT arise.</b> The finding it exists for (69.6) needed a
    /// <c>Map&lt;K, Collection&lt;V&gt;&gt;</c>, where a key can map to an empty collection
    /// and so flatten to no rows. This map is <c>K → V</c>: <b>every key carries exactly one
    /// value and therefore produces exactly one row</b>, so <c>count</c> always equals the
    /// key count and no key can vanish from the request. Nothing is completed locally here,
    /// and no divergence arises.
    /// </para>
    /// <para>
    /// ⚠ <b>A null topic or null log directory is rejected here</b>, because the ABI
    /// <em>silently skips</em> such a row — the caller would be left holding a
    /// <see cref="Task"/> for a replica the broker was never asked about. That is the one
    /// way a key could still lose its row, and it is turned into an
    /// <see cref="ArgumentException"/> naming the entry rather than a late
    /// <c>FailUncompleted</c> message (ffi §B5).
    /// </para>
    /// </remarks>
    internal AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options,
        NativeAlterReplicaLogDirsSubmit submit)
    {
        ThrowIfClosed();

        if (replicaAssignment is null)
        {
            throw new ArgumentNullException(nameof(replicaAssignment));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterReplicaLogDirsOptions));
        }

        List<TopicPartitionReplica> keys = new List<TopicPartitionReplica>(replicaAssignment.Count);
        List<string> logDirs = new List<string>(replicaAssignment.Count);
        foreach (KeyValuePair<TopicPartitionReplica, string> entry in replicaAssignment)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The replica assignment must not contain a null replica.", nameof(replicaAssignment));
            }

            // The ABI skips a row whose log dir is NULL, which would silently drop this
            // replica. The topic cannot be null — TopicPartitionReplica's constructor
            // rejects that — so the guard lives there and is not restated here.
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The log directory for '{entry.Key}' must not be null.", nameof(replicaAssignment));
            }

            keys.Add(entry.Key);
            logDirs.Add(entry.Value);
        }

        VoidKeyedAdminOperation<TopicPartitionReplica> operation =
            new VoidKeyedAdminOperation<TopicPartitionReplica>(
                "alterReplicaLogDirs", keys, s_replicaComparer);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count * 2);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            int[] partitions = new int[keys.Count];
            int[] brokerIds = new int[keys.Count];
            IntPtr[] directories = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitions[i] = keys[i].Partition;
                brokerIds[i] = keys[i].BrokerId;

                Utf8Marshal.PinnedUtf8String directory = Utf8Marshal.Pin(logDirs[i]);
                pinned.Add(directory);
                directories[i] = directory.Pointer;
            }

            // Shape 4b: one callback per distinct (topic, partition, broker) triple,
            // skipping entries with a NULL topic or log dir — neither is possible here, and
            // `replicaAssignment` is a map, so the key count is the number.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                brokerIds,
                directories,
                keys.Count,
                timeoutMs,
                AdminCallbacks.AlterReplicaLogDirs,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String value in pinned)
                {
                    value.Dispose();
                }
            }
        }

        return new AlterReplicaLogDirsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions? options) =>
        DescribeReplicaLogDirs(
            replicas, options, NativeMethods.AdminClientDescribeReplicaLogDirsAsync);

    /// <summary>
    /// Submits <c>describeReplicaLogDirs</c> and returns immediately with one awaitable per
    /// replica. Java's <c>Collection&lt;TopicPartitionReplica&gt;</c> becomes the ABI's three
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>, so a repeated replica is
    /// one entry. The topic cannot be null (<see cref="TopicPartitionReplica"/>'s
    /// constructor rejects it), so the ABI's "an entry with a NULL topic is skipped" cannot
    /// be reached through this surface.
    /// </para>
    /// <para>
    /// ⚠ <b>The result count is NOT guaranteed to equal the request count, and the honest
    /// outcome for a missing key is a FAULT.</b> Java's real client pre-registers a future
    /// per requested replica and completes each one — defaulting to an empty
    /// <c>ReplicaLogDirInfo</c> when the broker said nothing about it
    /// (<c>KafkaAdminClient.java:3103-3106</c> seeds <c>replicaDirInfoByPartition</c>, and
    /// <c>:3155-3160</c> completes every entry) — and the <b>Rust core does the same</b>
    /// (<c>src/admin/kafka_admin_client.rs:3705-3708</c> inserts a future for every
    /// requested replica). So against a real client every requested key gets an entry and
    /// <c>FailUncompleted</c> never fires.
    /// </para>
    /// <para>
    /// The <b>mock</b> is the exception: it omits replicas of unknown topics outright
    /// (<c>src/admin/mock_admin_client.rs:1352-1355</c>), so a broker-less test can reach
    /// the missing-key path. There <c>FailUncompleted</c> faults that key with a message
    /// naming it. That is deliberately <b>not</b> smoothed over by completing locally with a
    /// default: unlike the Stage-2 zero-op case, the key here is genuinely sent and the
    /// answer genuinely absent, so fabricating an "empty" description would report data the
    /// binding does not have — the same reasoning that keeps
    /// <see cref="LogDirDescription"/> free of a faked <c>IsCordoned</c>.
    /// </para>
    /// </remarks>
    internal DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options,
        NativeDescribeReplicaLogDirsSubmit submit)
    {
        ThrowIfClosed();

        if (replicas is null)
        {
            throw new ArgumentNullException(nameof(replicas));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeReplicaLogDirsOptions));
        }

        List<TopicPartitionReplica> keys = new List<TopicPartitionReplica>(replicas.Count);
        HashSet<TopicPartitionReplica> seen = new HashSet<TopicPartitionReplica>(s_replicaComparer);
        foreach (TopicPartitionReplica replica in replicas)
        {
            if (replica is null)
            {
                throw new ArgumentException(
                    "The replicas must not contain a null element.", nameof(replicas));
            }

            if (seen.Add(replica))
            {
                keys.Add(replica);
            }
        }

        KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> operation =
            new KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                "describeReplicaLogDirs", keys, s_replicaComparer);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            int[] partitions = new int[keys.Count];
            int[] brokerIds = new int[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitions[i] = keys[i].Partition;
                brokerIds[i] = keys[i].BrokerId;
            }

            // Shape 4a: the ABI fires once per DISTINCT replica, and `keys` is already
            // distinct (the HashSet above), so that is keys.Count. The submit's own token
            // is released after, so an empty collection releases instead of leaking.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                brokerIds,
                keys.Count,
                timeoutMs,
                AdminCallbacks.DescribeReplicaLogDirs,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new DescribeReplicaLogDirsResult(operation.Tasks, operation.KeyComparer);
    }

    internal ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options) =>
        ElectLeaders(electionType, partitions, options, NativeMethods.AdminClientElectLeadersAsync);

    /// <summary>
    /// Submits <c>electLeaders</c> and returns immediately with the <b>single</b> awaitable
    /// Java's <c>ElectLeadersResult</c> wraps (result shape 3).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>One awaitable, and the per-partition outcomes are its map's VALUES.</b> Java's
    /// future resolves to <c>Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;</c>
    /// (<c>ElectLeadersResult.java:47</c>), so this uses
    /// <see cref="SingleAdminOperation{TValue}"/> rather than the per-key bridge even
    /// though the ABI result declares a <c>get_error</c>. See
    /// <see cref="AdminCallbacks.ElectLeadersOptionalError"/>.
    /// </para>
    /// <para>
    /// ⚠ <b>No keys are pre-registered, and here that is forced twice over.</b> The keys
    /// come from the response — and with <paramref name="partitions"/>
    /// <see langword="null"/> the request names no partitions at all, so there is nothing
    /// to register even in principle.
    /// </para>
    /// <para>
    /// ⚠ <b><see langword="null"/> and empty are different requests.</b>
    /// <see langword="null"/> is Java's null <c>Set</c> — elect leaders for every partition
    /// in the cluster (<c>Admin.java:1099-1100</c>) — and sets <c>all_partitions</c>. An
    /// empty collection asks for an election over no partitions and leaves the flag false.
    /// Collapsing the two would silently turn a no-op into a cluster-wide election.
    /// </para>
    /// </remarks>
    internal ElectLeadersResult ElectLeaders(
        ElectionType electionType,
        IReadOnlyCollection<TopicPartition>? partitions,
        ElectLeadersOptions? options,
        NativeElectLeadersSubmit submit)
    {
        ThrowIfClosed();

        // Java's parameter is the ElectionType enum, so a value outside its two members is
        // not expressible there at all; in C# it is, by a cast. Rejected here as the
        // programmer error it is (ffi §B5) rather than left to the ABI, which would fire
        // its completion callback inline with an IllegalArgument error instead.
        if (electionType != ElectionType.Preferred && electionType != ElectionType.Unclean)
        {
            throw new ArgumentOutOfRangeException(
                nameof(electionType),
                electionType,
                "The election type must be ElectionType.Preferred or ElectionType.Unclean.");
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ElectLeadersOptions));
        }

        // NOT `partitions?.Count == 0` folded in: null is "every partition", empty is "no
        // partitions", and the ABI keeps a dedicated flag so the two stay apart.
        bool allPartitions = partitions is null;

        // De-duplicated because Java's parameter is a Set, and null-topic-checked because
        // the ABI would skip such an entry silently. Shared with
        // listPartitionReassignments, which takes the identical selection shape.
        List<TopicPartition> selection = DistinctPartitions(partitions, nameof(partitions));

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>> operation =
            new SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>>("electLeaders");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(selection.Count);

            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[selection.Count];
            int[] partitionIds = new int[selection.Count];
            for (int i = 0; i < selection.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(selection[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitionIds[i] = selection[i].Partition;
            }

            submit(
                _handle.DangerousGetHandle(),
                (int)electionType,
                allPartitions,
                topics,
                partitionIds,
                selection.Count,
                timeoutMs,
                AdminCallbacks.ElectLeaders,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The topic strings are pinned only for the call (ffi §A4's call-scoped rule):
            // the ABI copies them out during the submit.
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new ElectLeadersResult(operation.Task);
    }

    internal AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options) =>
        AlterPartitionReassignments(
            reassignments, options, NativeMethods.AdminClientAlterPartitionReassignmentsAsync);

    /// <summary>
    /// Submits <c>alterPartitionReassignments</c> and returns immediately with one
    /// awaitable per topic partition. Java's
    /// <c>Map&lt;TopicPartition, Optional&lt;NewPartitionReassignment&gt;&gt;</c> becomes
    /// the ABI's five parallel arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>A <see langword="null"/> entry is Java's <c>Optional.empty()</c> — cancel —
    /// and it travels in the dedicated <c>cancel</c> array, never as an empty replica
    /// list.</b> The header states the intent verbatim: "A separate flag rather than a NULL
    /// replica pointer, so cancelling stays distinct from 'present but empty', which Java
    /// rejects." A <c>?? Array.Empty&lt;int&gt;()</c> anywhere on this path would collapse
    /// the two, turning a cancellation into a request the ABI rejects outright.
    /// </para>
    /// <para>
    /// ⚠ <b>Null-as-a-shape here is the input's meaning, not a walker discriminant.</b>
    /// M15/P2b's rule — a result shape is never encoded in whether a field is null —
    /// governs the <em>completion</em> seam, which P4 leaves untouched. On the request
    /// side Java's own carrier is an <c>Optional</c>, so <c>NewPartitionReassignment?</c>
    /// is the faithful mapping, and the value it selects is carried on its own wire, which
    /// is exactly what that rule asks for.
    /// </para>
    /// <para>
    /// Java's empty target-replica list is rejected by
    /// <see cref="NewPartitionReassignment"/>'s constructor, so it cannot reach here; the
    /// ABI would reject it too, on the inline callback path.
    /// </para>
    /// </remarks>
    internal AlterPartitionReassignmentsResult AlterPartitionReassignments(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options,
        NativeAlterPartitionReassignmentsSubmit submit)
    {
        ThrowIfClosed();

        if (reassignments is null)
        {
            throw new ArgumentNullException(nameof(reassignments));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool allowReplicationFactorChange = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterPartitionReassignmentsOptions));
            allowReplicationFactorChange = options.AllowReplicationFactorChange;
        }

        List<TopicPartition> keys = new List<TopicPartition>(reassignments.Count);
        bool[] cancel = new bool[reassignments.Count];
        int[] partitions = new int[reassignments.Count];
        int[] targetReplicaCounts = new int[reassignments.Count];
        int[]?[] targetReplicas = new int[reassignments.Count][];
        int next = 0;
        foreach (KeyValuePair<TopicPartition, NewPartitionReassignment?> entry in reassignments)
        {
            // A `default(TopicPartition)` has a null Topic, and the header's "an entry with
            // a NULL topic is skipped" would silently drop it (ffi §B5).
            if (entry.Key.Topic is null)
            {
                throw new ArgumentException(
                    "The reassignments map must not contain a topic partition with a null topic.",
                    nameof(reassignments));
            }

            keys.Add(entry.Key);
            partitions[next] = entry.Key.Partition;

            // The whole cancel-versus-empty distinction lives on these three lines: a null
            // entry sets the flag and contributes NO replica array, a present one
            // contributes its own (never empty — the constructor rejects that).
            cancel[next] = entry.Value is null;
            if (entry.Value is not null)
            {
                IReadOnlyList<int> replicas = entry.Value.TargetReplicas;
                int[] ids = new int[replicas.Count];
                for (int i = 0; i < replicas.Count; i++)
                {
                    ids[i] = replicas[i];
                }

                targetReplicas[next] = ids;
                targetReplicaCounts[next] = ids.Length;
            }

            next++;
        }

        // EqualityComparer<TopicPartition>.Default dispatches to the struct's own
        // IEquatable implementation (ordinal on the topic), so it neither boxes nor
        // disagrees with the public AlterPartitionReassignmentsResult view — the same
        // reasoning as the deleteRecords path.
        VoidKeyedAdminOperation<TopicPartition> operation = new VoidKeyedAdminOperation<TopicPartition>(
            "alterPartitionReassignments", keys, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        List<GCHandle>? pinnedReplicas = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
            pinnedReplicas = new List<GCHandle>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            IntPtr[] replicaPointers = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;

                // A cancelled entry contributes a NULL pointer and a count of 0, which the
                // ABI never reads — `cancel[i]` alone decides. Pinned only for the call
                // (ffi §A4): the core copies the ids out during the submit.
                int[]? ids = targetReplicas[i];
                if (ids is null)
                {
                    replicaPointers[i] = IntPtr.Zero;
                    continue;
                }

                GCHandle pin = GCHandle.Alloc(ids, GCHandleType.Pinned);
                pinnedReplicas.Add(pin);
                replicaPointers[i] = pin.AddrOfPinnedObject();
            }

            // Shape 4b: one callback per distinct (topic, partition) pair, skipping entries
            // with a NULL topic — rejected above — and `reassignments` is a map.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                cancel,
                replicaPointers,
                targetReplicaCounts,
                keys.Count,
                timeoutMs,
                allowReplicationFactorChange,
                AdminCallbacks.AlterPartitionReassignments,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }

            if (pinnedReplicas is not null)
            {
                foreach (GCHandle pin in pinnedReplicas)
                {
                    pin.Free();
                }
            }
        }

        return new AlterPartitionReassignmentsResult(operation.Tasks, operation.KeyComparer);
    }

    internal AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options) =>
        AlterConsumerGroupOffsets(
            groupId, offsets, options, NativeMethods.AdminClientAlterConsumerGroupOffsetsAsync);

    /// <summary>
    /// Submits <c>alterConsumerGroupOffsets</c> and returns immediately with the
    /// <b>single</b> awaitable Java's <c>AlterConsumerGroupOffsetsResult</c> wraps (result
    /// shape 3).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>One awaitable, and the per-partition outcomes are its map's VALUES.</b> Java's
    /// future resolves to <c>Map&lt;TopicPartition, Errors&gt;</c>
    /// (<c>AlterConsumerGroupOffsetsResult.java:33</c>) while the ABI fires one callback per
    /// partition, so this uses <see cref="FanInAdminOperation{TKey, TValue}"/> — result
    /// shape 4c.
    /// </para>
    /// </remarks>
    internal AlterConsumerGroupOffsetsResult AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options,
        NativeAlterConsumerGroupOffsetsSubmit submit)
    {
        ThrowIfClosed();

        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        if (offsets is null)
        {
            throw new ArgumentNullException(nameof(offsets));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterConsumerGroupOffsetsOptions));
        }

        List<TopicPartition> keys = new List<TopicPartition>(offsets.Count);
        int[] partitions = new int[offsets.Count];
        long[] offsetValues = new long[offsets.Count];
        int[] leaderEpochs = new int[offsets.Count];
        bool[] hasLeaderEpoch = new bool[offsets.Count];
        string[] metadataValues = new string[offsets.Count];
        int next = 0;
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
            if (entry.Key.Topic is null)
            {
                throw new ArgumentException(
                    "The offsets map must not contain a topic partition with a null topic.",
                    nameof(offsets));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException("Offset value must not be null.", nameof(offsets));
            }

            keys.Add(entry.Key);
            partitions[next] = entry.Key.Partition;
            offsetValues[next] = entry.Value.Offset;
            hasLeaderEpoch[next] = entry.Value.LeaderEpoch.HasValue;
            leaderEpochs[next] = entry.Value.LeaderEpoch ?? -1;
            metadataValues[next] = entry.Value.Metadata ?? string.Empty;
            next++;
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        FanInAdminOperation<TopicPartition, KafkaException?> operation =
            new FanInAdminOperation<TopicPartition, KafkaException?>(
                keys.Count, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        Utf8Marshal.PinnedUtf8String? pinnedGroupId = null;
        List<Utf8Marshal.PinnedUtf8String>? pinnedTopics = null;
        List<Utf8Marshal.PinnedUtf8String>? pinnedMetadata = null;
        try
        {
            pinnedTopics = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
            pinnedMetadata = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinnedGroupId = Utf8Marshal.Pin(groupId);

            IntPtr[] topics = new IntPtr[keys.Count];
            IntPtr[] metadataPointers = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinnedTopics.Add(topic);
                topics[i] = topic.Pointer;

                Utf8Marshal.PinnedUtf8String metadata = Utf8Marshal.Pin(metadataValues[i]);
                pinnedMetadata.Add(metadata);
                metadataPointers[i] = metadata.Pointer;
            }

            // Shape 4c: the ABI fires once per (topic, partition) key it was handed, and
            // `offsets` is a dictionary, so `keys` is already distinct.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                pinnedGroupId.Pointer,
                topics,
                partitions,
                offsetValues,
                metadataPointers,
                leaderEpochs,
                hasLeaderEpoch,
                keys.Count,
                timeoutMs,
                AdminCallbacks.AlterConsumerGroupOffsets,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The group id / topic / metadata strings are pinned only for the call
            // (ffi §A4's call-scoped rule): the ABI copies them out during the submit.
            pinnedGroupId?.Dispose();

            if (pinnedTopics is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinnedTopics)
                {
                    topic.Dispose();
                }
            }

            if (pinnedMetadata is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String metadata in pinnedMetadata)
                {
                    metadata.Dispose();
                }
            }
        }

        return new AlterConsumerGroupOffsetsResult(operation.Task);
    }

    internal DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options) =>
        DeleteConsumerGroupOffsets(
            groupId, partitions, options, NativeMethods.AdminClientDeleteConsumerGroupOffsetsAsync);

    /// <summary>
    /// Submits <c>deleteConsumerGroupOffsets</c> and returns immediately with the
    /// <b>single</b> awaitable Java's <c>DeleteConsumerGroupOffsetsResult</c> wraps (result
    /// shape 3) — plus the original request's partition set, the second stored field Java
    /// carries (<c>DeleteConsumerGroupOffsetsResult.java:34</c>) that
    /// <c>AlterConsumerGroupOffsetsResult</c> does not.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>One awaitable, and the per-partition outcomes are its map's VALUES.</b> Java's
    /// future resolves to <c>Map&lt;TopicPartition, Errors&gt;</c>
    /// (<c>DeleteConsumerGroupOffsetsResult.java:33</c>) while the ABI fires one callback per
    /// partition, so this uses <see cref="FanInAdminOperation{TKey, TValue}"/> — result
    /// shape 4c, the same as
    /// <see cref="AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, AlterConsumerGroupOffsetsOptions?, NativeAlterConsumerGroupOffsetsSubmit)"/>.
    /// </remarks>
    internal DeleteConsumerGroupOffsetsResult DeleteConsumerGroupOffsets(
        string groupId,
        IReadOnlyCollection<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions? options,
        NativeDeleteConsumerGroupOffsetsSubmit submit)
    {
        ThrowIfClosed();

        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteConsumerGroupOffsetsOptions));
        }

        // De-duplicated (Java's parameter is a Set): the ABI fires once per key it was
        // handed, so a repeat would draw a second callback the accumulator cannot key.
        List<TopicPartition> keys = new List<TopicPartition>(partitions.Count);
        HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
        foreach (TopicPartition partition in partitions)
        {
            if (partition.Topic is null)
            {
                throw new ArgumentException(
                    "The partitions collection must not contain a topic partition with a null topic.",
                    nameof(partitions));
            }

            if (seen.Add(partition))
            {
                keys.Add(partition);
            }
        }

        int[] partitionValues = new int[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            partitionValues[i] = keys[i].Partition;
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        FanInAdminOperation<TopicPartition, KafkaException?> operation =
            new FanInAdminOperation<TopicPartition, KafkaException?>(
                keys.Count, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        Utf8Marshal.PinnedUtf8String? pinnedGroupId = null;
        List<Utf8Marshal.PinnedUtf8String>? pinnedTopics = null;
        try
        {
            pinnedTopics = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinnedGroupId = Utf8Marshal.Pin(groupId);

            IntPtr[] topics = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinnedTopics.Add(topic);
                topics[i] = topic.Pointer;
            }

            // Shape 4c: one callback per key handed over, and `keys` is distinct above.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                pinnedGroupId.Pointer,
                topics,
                partitionValues,
                keys.Count,
                timeoutMs,
                AdminCallbacks.DeleteConsumerGroupOffsets,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The group id / topic strings are pinned only for the call (ffi §A4's
            // call-scoped rule): the ABI copies them out during the submit.
            pinnedGroupId?.Dispose();

            if (pinnedTopics is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinnedTopics)
                {
                    topic.Dispose();
                }
            }
        }

        return new DeleteConsumerGroupOffsetsResult(operation.Task, keys);
    }

    internal DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options) =>
        DeleteConsumerGroups(groupIds, options, NativeMethods.AdminClientDeleteConsumerGroupsAsync);

    /// <summary>
    /// Submits <c>deleteConsumerGroups</c> and returns immediately with one awaitable per
    /// requested group id — Java's <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>
    /// (<c>DeleteConsumerGroupsResult.java:30</c>, result shape 2).
    /// </summary>
    /// <remarks>
    /// Argument-for-argument <see cref="DeleteTopics(TopicCollection, DeleteTopicsOptions?, NativeDeleteTopicsSubmit, NativeDeleteTopicsSubmit)"/>'s
    /// by-name branch — a flat group-id array, no per-key options flag — so this reuses the
    /// same <see cref="Submit"/> helper and <see cref="VoidKeyedAdminOperation{TKey}"/> bridge
    /// rather than hand-rolling a pin/<c>GCHandle</c> sequence. The group ids cross as a pinned
    /// <c>IntPtr[]</c> of UTF-8 plus a separate count, never a <c>string[]</c> (ffi §A2); the
    /// pins are call-scoped (§A4) — the core copies every id out during the submit.
    /// </remarks>
    /// <param name="groupIds">The consumer group ids to delete. Duplicates collapse.</param>
    /// <param name="options">The options, or <see langword="null"/> for the defaults.</param>
    /// <param name="submit">The native submit, injectable for tests.</param>
    internal DeleteConsumerGroupsResult DeleteConsumerGroups(
        IReadOnlyCollection<string> groupIds,
        DeleteConsumerGroupsOptions? options,
        NativeDeleteConsumerGroupsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (groupIds is null)
        {
            throw new ArgumentNullException(nameof(groupIds));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteConsumerGroupsOptions));
        }

        List<string> keys = DistinctNames(groupIds, "group ids", nameof(groupIds));

        VoidKeyedAdminOperation<string> operation = new VoidKeyedAdminOperation<string>(
            "deleteConsumerGroups", keys, StringComparer.Ordinal);

        Submit(
            operation,
            keys,
            (admin, pinned, count, callbackUserData) =>
            {
                // Shape 4b: one callback per distinct group id, skipping NULL entries —
                // a pinned key is never NULL and `keys` is already DistinctNames.
                operation.SetPendingCallbacks(count);
                submit(
                    admin,
                    pinned,
                    count,
                    timeoutMs,
                    AdminCallbacks.DeleteConsumerGroups,
                    callbackUserData);

                operation.ReleaseSubmitToken();
            });

        return new DeleteConsumerGroupsResult(operation.Tasks, operation.KeyComparer);
    }

    internal RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options) =>
        RemoveMembersFromConsumerGroup(
            groupId, options, NativeMethods.AdminClientRemoveMembersFromConsumerGroupAsync);

    /// <summary>
    /// Submits <c>removeMembersFromConsumerGroup</c> and returns immediately with the
    /// <b>single</b> awaitable Java's <c>RemoveMembersFromConsumerGroupResult</c> wraps
    /// (<c>future</c>, <c>RemoveMembersFromConsumerGroupResult.java:35</c>, result shape 4c) —
    /// plus the original request's member collection, the second stored field Java carries
    /// (<c>memberInfos</c>, <c>:36</c>), the same two-field pattern as
    /// <see cref="DeleteConsumerGroupOffsets(string, IReadOnlyCollection{TopicPartition}, DeleteConsumerGroupOffsetsOptions?, NativeDeleteConsumerGroupOffsetsSubmit)"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>One awaitable, and the per-member outcomes are its map's VALUES</b> — Java's future
    /// resolves to <c>Map&lt;MemberIdentity, Errors&gt;</c> while the ABI fires one callback per
    /// member, so this uses <see cref="FanInAdminOperation{TKey, TValue}"/> — result shape 4c,
    /// the same as <see cref="AlterConsumerGroupOffsets(string, IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, AlterConsumerGroupOffsetsOptions?, NativeAlterConsumerGroupOffsetsSubmit)"/>.
    /// </para>
    /// <para>
    /// ⚠⚠ <b>removeAll mode passes no member array at all, and its callback count is 1.</b>
    /// When <see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/> is
    /// <see langword="true"/>, Java has no per-member request to send, so
    /// <paramref name="submit"/> gets a <see langword="null"/> group-instance-id array and a
    /// count of 0 — and the ABI answers with <b>one</b> NULL-keyed whole-operation callback
    /// instead of one per member. This is the only RPC in the phase whose <c>n</c> is
    /// mode-dependent.
    /// </para>
    /// </remarks>
    internal RemoveMembersFromConsumerGroupResult RemoveMembersFromConsumerGroup(
        string groupId,
        RemoveMembersFromConsumerGroupOptions options,
        NativeRemoveMembersFromConsumerGroupSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        int timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(RemoveMembersFromConsumerGroupOptions));
        bool removeAll = options.RemoveAll;
        IReadOnlyCollection<MemberToRemove> members = options.Members;

        List<MemberToRemove>? keys = null;
        if (!removeAll)
        {
            keys = new List<MemberToRemove>(members.Count);
            foreach (MemberToRemove member in members)
            {
                keys.Add(member);
            }
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        // Shape 4c, MODE-DEPENDENT n: one NULL-keyed whole-operation callback in removeAll
        // mode, else one per distinct group.instance.id — and `options.Members` is a set of
        // non-null ids, so `keys` carries no duplicate and no NULL the core would skip.
        int pendingCallbacks = removeAll ? 1 : keys!.Count;
        FanInAdminOperation<string, KafkaException?> operation =
            new FanInAdminOperation<string, KafkaException?>(
                keys?.Count ?? 0, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        Utf8Marshal.PinnedUtf8String? pinnedGroupId = null;
        Utf8Marshal.PinnedUtf8String? pinnedReason = null;
        List<Utf8Marshal.PinnedUtf8String>? pinnedGroupInstanceIds = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinnedGroupId = Utf8Marshal.Pin(groupId);
            pinnedReason = options.Reason is null ? null : Utf8Marshal.Pin(options.Reason);

            IntPtr[]? groupInstanceIds = null;
            int memberCount = 0;
            if (keys is not null)
            {
                pinnedGroupInstanceIds = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
                groupInstanceIds = new IntPtr[keys.Count];
                for (int i = 0; i < keys.Count; i++)
                {
                    Utf8Marshal.PinnedUtf8String groupInstanceId = Utf8Marshal.Pin(keys[i].GroupInstanceId);
                    pinnedGroupInstanceIds.Add(groupInstanceId);
                    groupInstanceIds[i] = groupInstanceId.Pointer;
                }

                memberCount = keys.Count;
            }

            operation.SetPendingCallbacks(pendingCallbacks);

            submit(
                _handle.DangerousGetHandle(),
                pinnedGroupId.Pointer,
                removeAll,
                groupInstanceIds,
                memberCount,
                pinnedReason?.Pointer ?? IntPtr.Zero,
                timeoutMs,
                AdminCallbacks.RemoveMembersFromConsumerGroup,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The group id / reason / group-instance-id strings are pinned only for the call
            // (ffi §A4's call-scoped rule): the ABI copies them out during the submit.
            pinnedGroupId?.Dispose();
            pinnedReason?.Dispose();

            if (pinnedGroupInstanceIds is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String groupInstanceId in pinnedGroupInstanceIds)
                {
                    groupInstanceId.Dispose();
                }
            }
        }

        return new RemoveMembersFromConsumerGroupResult(operation.Task, members);
    }

    internal CreateAclsResult CreateAcls(IEnumerable<AclBinding> acls, CreateAclsOptions? options) =>
        CreateAcls(acls, options, NativeMethods.AdminClientCreateAclsAsync);

    /// <summary>
    /// Submits <c>createAcls</c> and returns immediately with one awaitable per binding
    /// (result shape 2).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ The binding is the <b>key</b>, so <see cref="AclBinding"/>'s value equality is
    /// load-bearing here rather than decorative: the bridge's dictionary and every
    /// <c>Values[binding]</c> lookup a caller makes are keyed by it (PLAN D39).
    /// </para>
    /// <para>
    /// No ANY/NULL screening happens here. Java's <c>ResourcePattern</c> and
    /// <c>AccessControlEntry</c> constructors reject exactly what the ABI rejects, and this
    /// binding's do too, so an <see cref="AclBinding"/> that exists is already valid
    /// (PLAN D38). A second check would only shadow the one that runs.
    /// </para>
    /// </remarks>
    internal CreateAclsResult CreateAcls(
        IEnumerable<AclBinding> acls, CreateAclsOptions? options, NativeCreateAclsSubmit submit)
    {
        ThrowIfClosed();

        if (acls is null)
        {
            throw new ArgumentNullException(nameof(acls));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(CreateAclsOptions));
        }

        List<AclBinding> keys = DistinctBindings(acls, nameof(acls));

        VoidKeyedAdminOperation<AclBinding> operation = new VoidKeyedAdminOperation<AclBinding>(
            "createAcls", keys, EqualityComparer<AclBinding>.Default);

        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        AclRowMarshal.Rows? rows = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            rows = AclRowMarshal.Pin(keys);

            // Shape 4b, owned key: "exactly `count` times", and `keys` is already
            // DistinctBindings, so the row count is the number. A binding the core rejects
            // locally still arrives as its own callback carrying an INVALID_REQUEST error.
            operation.SetPendingCallbacks(rows.Count);

            submit(
                _handle.DangerousGetHandle(),
                rows.ResourceTypes,
                rows.ResourceNames,
                rows.PatternTypes,
                rows.Principals,
                rows.Hosts,
                rows.Operations,
                rows.PermissionTypes,
                rows.Count,
                timeoutMs,
                AdminCallbacks.CreateAcls,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies every row out during the submit,
            // and the inline callback path has already run by the time it returns.
            rows?.Dispose();
        }

        return new CreateAclsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options) =>
        DeleteAcls(filters, options, NativeMethods.AdminClientDeleteAclsAsync);

    /// <summary>
    /// Submits <c>deleteAcls</c> and returns immediately with one awaitable per filter
    /// (result shape 1, whose value is itself an indexed list).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ The filter is the <b>key</b>, so <see cref="AclBindingFilter"/>'s value equality is
    /// load-bearing here rather than decorative (PLAN D39).
    /// </para>
    /// <para>
    /// No ANY/NULL screening happens here, and unlike <c>createAcls</c> none is warranted:
    /// the ABI rejects no enum combination and reads a NULL name as "match any"
    /// (<c>confluent_kafka.h:8121-8125</c>), exactly as Java's filter constructors do.
    /// </para>
    /// </remarks>
    internal DeleteAclsResult DeleteAcls(
        IEnumerable<AclBindingFilter> filters,
        DeleteAclsOptions? options,
        NativeDeleteAclsSubmit submit)
    {
        ThrowIfClosed();

        if (filters is null)
        {
            throw new ArgumentNullException(nameof(filters));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteAclsOptions));
        }

        List<AclBindingFilter> keys = DistinctFilters(filters, nameof(filters));

        KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation =
            new KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults>(
                "deleteAcls", keys, EqualityComparer<AclBindingFilter>.Default);

        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        AclRowMarshal.Rows? rows = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            rows = AclRowMarshal.Pin(keys);

            // Shape 4a: one callback per DISTINCT filter. ⚠ delete_acls_async's own doc says
            // "exactly `count` times", but its body de-duplicates before the fan-out — and
            // `keys` is already DistinctFilters, so the two agree here. Armed before the
            // submit (every key can fire inline on this thread); the submit's own token is
            // released after, which is what makes an EMPTY filter list release rather than
            // leak.
            operation.SetPendingCallbacks(rows.Count);

            submit(
                _handle.DangerousGetHandle(),
                rows.ResourceTypes,
                rows.ResourceNames,
                rows.PatternTypes,
                rows.Principals,
                rows.Hosts,
                rows.Operations,
                rows.PermissionTypes,
                rows.Count,
                timeoutMs,
                AdminCallbacks.DeleteAcls,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies every row out during the submit,
            // and the inline callback path has already run by the time it returns.
            rows?.Dispose();
        }

        return new DeleteAclsResult(operation.Tasks);
    }

    internal DescribeAclsResult DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options) =>
        DescribeAcls(filter, options, NativeMethods.AdminClientDescribeAclsAsync);

    /// <summary>
    /// Submits <c>describeAcls</c> and returns immediately with the <b>single</b> awaitable
    /// Java's <c>DescribeAclsResult</c> wraps (result sub-shape 3b).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ The filter's seven fields cross as <b>scalars</b>, not arrays — Java takes one
    /// filter, not a collection (<c>confluent_kafka.h:8040-8041</c>).
    /// </para>
    /// <para>
    /// No ANY/NULL screening happens here, as on <c>deleteAcls</c>: a NULL string means
    /// "match any", distinct from a pointer to <c>""</c>, and no enum combination is
    /// rejected (<c>confluent_kafka.h:8052-8063</c>).
    /// </para>
    /// </remarks>
    internal DescribeAclsResult DescribeAcls(
        AclBindingFilter filter,
        DescribeAclsOptions? options,
        NativeDescribeAclsSubmit submit)
    {
        ThrowIfClosed();

        if (filter is null)
        {
            throw new ArgumentNullException(nameof(filter));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeAclsOptions));
        }

        ResourcePatternFilter pattern = filter.PatternFilter;
        AccessControlEntryFilter entry = filter.EntryFilter;

        SingleAdminOperation<IReadOnlyCollection<AclBinding>> operation =
            new SingleAdminOperation<IReadOnlyCollection<AclBinding>>("describeAcls");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(3);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                (int)pattern.ResourceType,
                AclRowMarshal.PinName(pattern.Name, pinned),
                (int)pattern.PatternType,
                AclRowMarshal.PinName(entry.Principal, pinned),
                AclRowMarshal.PinName(entry.Host, pinned),
                (int)entry.Operation,
                (int)entry.PermissionType,
                timeoutMs,
                AdminCallbacks.DescribeAcls,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the filter out during the submit,
            // and the inline callback path has already run by the time it returns.
            foreach (Utf8Marshal.PinnedUtf8String name in pinned)
            {
                name.Dispose();
            }
        }

        return new DescribeAclsResult(operation.Task);
    }

    internal DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options) =>
        DescribeClientQuotas(filter, options, NativeMethods.AdminClientDescribeClientQuotasAsync);

    /// <summary>
    /// Submits <c>describeClientQuotas</c> and returns immediately with the <b>single</b>
    /// awaitable Java's <c>DescribeClientQuotasResult</c> wraps (result shape 3).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ The component's match type is the ABI's own discriminant — 0 EXACT, 1 DEFAULT,
    /// 2 SPECIFIED — and DEFAULT and SPECIFIED both carry no name, so a <c>string?</c> model
    /// would collapse them (<c>confluent_kafka.h:8204-8216</c>, PLAN D37).
    /// </para>
    /// <para>
    /// ⚠ An empty component list with <c>Strict</c> false is Java's
    /// <c>ClientQuotaFilter.all()</c> — the most common call — so there is deliberately no
    /// emptiness guard here.
    /// </para>
    /// </remarks>
    internal DescribeClientQuotasResult DescribeClientQuotas(
        ClientQuotaFilter filter,
        DescribeClientQuotasOptions? options,
        NativeDescribeClientQuotasSubmit submit)
    {
        ThrowIfClosed();

        if (filter is null)
        {
            throw new ArgumentNullException(nameof(filter));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeClientQuotasOptions));
        }

        SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> operation =
            new SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                "describeClientQuotas");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        ClientQuotaMarshal.FilterRows? rows = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            rows = ClientQuotaMarshal.Pin(filter);

            submit(
                _handle.DangerousGetHandle(),
                rows.EntityTypes,
                rows.MatchTypes,
                rows.MatchNames,
                rows.Count,
                rows.Strict,
                timeoutMs,
                AdminCallbacks.DescribeClientQuotas,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the filter out during the submit,
            // and the inline callback path has already run by the time it returns.
            rows?.Dispose();
        }

        return new DescribeClientQuotasResult(operation.Task);
    }

    internal AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options) =>
        AlterClientQuotas(entries, options, NativeMethods.AdminClientAlterClientQuotasAsync);

    /// <summary>
    /// Submits <c>alterClientQuotas</c> and returns immediately with one awaitable per entity
    /// (result shape 2).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ The entity is the <b>key</b>, so <see cref="ClientQuotaEntity"/>'s value equality is
    /// load-bearing here rather than decorative: the bridge's dictionary and every
    /// <c>Values[entity]</c> lookup a caller makes are keyed by it (PLAN D39).
    /// </para>
    /// <para>
    /// ⚠ Three ABI rejections are surfaced here, before any pin (PLAN D38), because the ABI
    /// reports them by firing the callback synchronously on this thread
    /// (<c>confluent_kafka.h:8341-8342</c>). Two of them are reachable and are checked below:
    /// an alteration with <b>no entity types</b>, and a <b>repeated entity</b> across
    /// alterations — which is rejected rather than collapsed, since the ABI refuses it and
    /// silently dropping one would lose an alteration the caller wrote
    /// (<c>h:8300-8302</c>). ⚠ <b>Java accepts a repeated entity</b>
    /// (<c>KafkaAdminClient.java:4314-4318</c> puts the futures unconditionally, collapsing
    /// the map, and still sends every alteration), so this is a recorded divergence
    /// (<c>definition-of-done.md</c> §7), not parity — the one P6 RPC whose Java-faithful
    /// <c>Collection</c> shape is not accepted verbatim. The rest are already unreachable: a repeated entity type
    /// <em>within</em> one alteration and a null entity type cannot survive
    /// <see cref="ClientQuotaEntity"/>'s dictionary, and a null op key cannot survive
    /// <see cref="ClientQuotaAlteration.Op"/>'s constructor.
    /// </para>
    /// </remarks>
    internal AlterClientQuotasResult AlterClientQuotas(
        IEnumerable<ClientQuotaAlteration> entries,
        AlterClientQuotasOptions? options,
        NativeAlterClientQuotasSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (entries is null)
        {
            throw new ArgumentNullException(nameof(entries));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterClientQuotasOptions));
            validateOnly = options.ValidateOnly;
        }

        List<ClientQuotaAlteration> alterations = new List<ClientQuotaAlteration>();
        List<ClientQuotaEntity> keys = new List<ClientQuotaEntity>();
        HashSet<ClientQuotaEntity> seen = new HashSet<ClientQuotaEntity>();
        foreach (ClientQuotaAlteration alteration in entries)
        {
            if (alteration is null)
            {
                throw new ArgumentException(
                    "The client quota alterations must not contain a null element.", nameof(entries));
            }

            if (alteration.Entity.Entries.Count == 0)
            {
                throw new ArgumentException(
                    "The client quota alterations must not contain an alteration with no entity types.",
                    nameof(entries));
            }

            if (!seen.Add(alteration.Entity))
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "The client quota alterations must not alter the entity {0} more than once.",
                        alteration.Entity),
                    nameof(entries));
            }

            alterations.Add(alteration);
            keys.Add(alteration.Entity);
        }

        VoidKeyedAdminOperation<ClientQuotaEntity> operation =
            new VoidKeyedAdminOperation<ClientQuotaEntity>(
                "alterClientQuotas", keys, EqualityComparer<ClientQuotaEntity>.Default);

        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        ClientQuotaMarshal.AlterationRows? rows = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            rows = ClientQuotaMarshal.PinAlterations(alterations);

            // Shape 4b, owned key: one callback per distinct entity, and a repeated entity
            // is rejected above, so the row count is the number.
            operation.SetPendingCallbacks(rows.Count);

            submit(
                _handle.DangerousGetHandle(),
                rows.EntityTypes,
                rows.EntityNames,
                rows.EntityCounts,
                rows.OpKeys,
                rows.OpValues,
                rows.OpHasValues,
                rows.OpCounts,
                rows.Count,
                timeoutMs,
                validateOnly,
                AdminCallbacks.AlterClientQuotas,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies every row out during the submit,
            // and the inline callback path has already run by the time it returns.
            rows?.Dispose();
        }

        return new AlterClientQuotasResult(operation.Tasks, operation.KeyComparer);
    }

    // ====================================================================================
    // M15/P7 — SCRAM credentials, delegation tokens, features.
    // ====================================================================================

    internal DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users, DescribeUserScramCredentialsOptions? options) =>
        DescribeUserScramCredentials(
            users, options, NativeMethods.AdminClientDescribeUserScramCredentialsAsync);

    /// <summary>
    /// Submits <c>describeUserScramCredentials</c> and returns immediately with the single
    /// awaitable over the flattened result table.
    /// </summary>
    /// <remarks>
    /// ⚠ A <see langword="null"/> or empty <paramref name="users"/> describes <b>every</b> user
    /// (<c>confluent_kafka.h:8784-8785</c>), which is why this cannot use a per-key bridge: the
    /// keys are discovered from the response. That matches Java, whose stored field is one
    /// future over raw response data — see <see cref="UserScramCredentialEntry"/>.
    /// </remarks>
    internal DescribeUserScramCredentialsResult DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users,
        DescribeUserScramCredentialsOptions? options,
        NativeDescribeUserScramCredentialsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = options is null
            ? UnsetTimeoutMs
            : ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeUserScramCredentialsOptions));

        List<string> requested = users is null
            ? new List<string>()
            : DistinctNames(users, "users", nameof(users));

        SingleAdminOperation<IReadOnlyCollection<UserScramCredentialEntry>> operation =
            new SingleAdminOperation<IReadOnlyCollection<UserScramCredentialEntry>>(
                "describeUserScramCredentials");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(requested.Count);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                PinNames(requested, pinned),
                requested.Count,
                timeoutMs,
                AdminCallbacks.DescribeUserScramCredentials,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the names out during the submit.
            foreach (Utf8Marshal.PinnedUtf8String pin in pinned)
            {
                pin.Dispose();
            }
        }

        return new DescribeUserScramCredentialsResult(operation.Task);
    }

    internal AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions? options) =>
        AlterUserScramCredentials(
            alterations, options, NativeMethods.AdminClientAlterUserScramCredentialsAsync);

    /// <summary>
    /// Submits <c>alterUserScramCredentials</c> and returns immediately with one awaitable per
    /// user (result shape 2).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>A repeated user is passed through, not rejected</b> — two rows naming the same user
    /// (a <c>SCRAM_SHA_256</c> deletion plus a <c>SCRAM_SHA_512</c> upsertion, say) both reach
    /// the broker, and Java keys one future per user so they collapse to one outcome row
    /// (<c>confluent_kafka.h:8875-8886</c>). Hence <b>every</b> alteration is marshalled while
    /// the bridge's key set is de-duplicated. This is the deliberate inverse of
    /// <see cref="AlterClientQuotas(IEnumerable{ClientQuotaAlteration}, AlterClientQuotasOptions?)"/>,
    /// whose compound key a caller could not re-derive.
    /// </remarks>
    internal AlterUserScramCredentialsResult AlterUserScramCredentials(
        IEnumerable<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions? options,
        NativeAlterUserScramCredentialsSubmit submit)
    {
        ThrowIfClosed();

        if (alterations is null)
        {
            throw new ArgumentNullException(nameof(alterations));
        }

        int timeoutMs = options is null
            ? UnsetTimeoutMs
            : ValidateTimeoutMs(options.TimeoutMs, nameof(AlterUserScramCredentialsOptions));

        List<UserScramCredentialAlteration> rows = new List<UserScramCredentialAlteration>();
        List<string> keys = new List<string>();
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (UserScramCredentialAlteration alteration in alterations)
        {
            if (alteration is null)
            {
                throw new ArgumentException(
                    "The SCRAM credential alterations must not contain a null element.",
                    nameof(alterations));
            }

            rows.Add(alteration);
            if (seen.Add(alteration.User))
            {
                keys.Add(alteration.User);
            }
        }

        VoidKeyedAdminOperation<string> operation = new VoidKeyedAdminOperation<string>(
            "alterUserScramCredentials", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        AlterUserScramCredentialsMarshal.Rows? pinned = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinned = AlterUserScramCredentialsMarshal.Pin(rows);

            // ⚠ Shape 4b: the ABI fires once per DISTINCT USER, not once per row — two
            // alterations naming one user collapse to one outcome, which is why `keys` is
            // already the distinct-user list while `rows` may be longer.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                pinned.Users,
                pinned.IsDeletions,
                pinned.Mechanisms,
                pinned.Iterations,
                pinned.Passwords,
                pinned.PasswordLens,
                pinned.Salts,
                pinned.SaltLens,
                pinned.HasSalts,
                pinned.Count,
                timeoutMs,
                AdminCallbacks.AlterUserScramCredentials,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies every row out during the submit.
            pinned?.Dispose();
        }

        return new AlterUserScramCredentialsResult(operation.Tasks, operation.KeyComparer);
    }

    internal CreateDelegationTokenResult CreateDelegationToken(CreateDelegationTokenOptions? options) =>
        CreateDelegationToken(options, NativeMethods.AdminClientCreateDelegationTokenAsync);

    /// <summary>
    /// Submits <c>createDelegationToken</c> and returns immediately with the single awaitable
    /// over the issued token. The result carries no table, so the completion resolves the
    /// awaiter straight off the root.
    /// </summary>
    internal CreateDelegationTokenResult CreateDelegationToken(
        CreateDelegationTokenOptions? options, NativeCreateDelegationTokenSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        IReadOnlyList<KafkaPrincipal> renewers = Array.Empty<KafkaPrincipal>();
        KafkaPrincipal? owner = null;
        long maxLifetimeMs = -1L;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(CreateDelegationTokenOptions));
            renewers = options.Renewers
                ?? throw new ArgumentException(
                    "CreateDelegationTokenOptions.Renewers must not be null; use an empty list.",
                    nameof(options));
            owner = options.Owner;
            maxLifetimeMs = options.MaxLifetimeMs;
        }

        for (int i = 0; i < renewers.Count; i++)
        {
            if (renewers[i] is null)
            {
                throw new ArgumentException(
                    "CreateDelegationTokenOptions.Renewers must not contain a null element.",
                    nameof(options));
            }
        }

        SingleAdminOperation<DelegationToken> operation =
            new SingleAdminOperation<DelegationToken>("createDelegationToken");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        DelegationTokenMarshal.PrincipalRows? pinnedRenewers = null;
        Utf8Marshal.PinnedUtf8String? ownerType = null;
        Utf8Marshal.PinnedUtf8String? ownerName = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinnedRenewers = DelegationTokenMarshal.PinPrincipals(renewers);
            if (owner is not null)
            {
                ownerType = Utf8Marshal.Pin(owner.PrincipalType);
                ownerName = Utf8Marshal.Pin(owner.Name);
            }

            submit(
                _handle.DangerousGetHandle(),
                pinnedRenewers.PrincipalTypes,
                pinnedRenewers.Names,
                pinnedRenewers.Count,
                ownerType?.Pointer ?? IntPtr.Zero,
                ownerName?.Pointer ?? IntPtr.Zero,
                maxLifetimeMs,
                timeoutMs,
                AdminCallbacks.CreateDelegationToken,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            pinnedRenewers?.Dispose();
            ownerType?.Dispose();
            ownerName?.Dispose();
        }

        return new CreateDelegationTokenResult(operation.Task);
    }

    internal RenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options) =>
        RenewDelegationToken(hmac, options, NativeMethods.AdminClientRenewDelegationTokenAsync);

    /// <summary>
    /// Submits <c>renewDelegationToken</c> and returns immediately with the single awaitable
    /// over the token's new expiry timestamp.
    /// </summary>
    internal RenewDelegationTokenResult RenewDelegationToken(
        byte[] hmac, RenewDelegationTokenOptions? options, NativeRenewDelegationTokenSubmit submit)
    {
        ThrowIfClosed();

        if (hmac is null)
        {
            throw new ArgumentNullException(nameof(hmac));
        }

        long renewTimePeriodMs = -1L;
        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(RenewDelegationTokenOptions));
            renewTimePeriodMs = options.RenewTimePeriodMs;
        }

        SingleAdminOperation<long> operation = new SingleAdminOperation<long>("renewDelegationToken");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        GCHandle hmacPin = default;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            hmacPin = GCHandle.Alloc(hmac, GCHandleType.Pinned);

            submit(
                _handle.DangerousGetHandle(),
                hmac.Length == 0 ? IntPtr.Zero : hmacPin.AddrOfPinnedObject(),
                hmac.Length,
                renewTimePeriodMs,
                timeoutMs,
                AdminCallbacks.RenewDelegationToken,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (hmacPin.IsAllocated)
            {
                hmacPin.Free();
            }
        }

        return new RenewDelegationTokenResult(operation.Task);
    }

    internal ExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options) =>
        ExpireDelegationToken(hmac, options, NativeMethods.AdminClientExpireDelegationTokenAsync);

    /// <summary>
    /// Submits <c>expireDelegationToken</c> and returns immediately with the single awaitable
    /// over the token's expiry timestamp.
    /// </summary>
    internal ExpireDelegationTokenResult ExpireDelegationToken(
        byte[] hmac, ExpireDelegationTokenOptions? options, NativeExpireDelegationTokenSubmit submit)
    {
        ThrowIfClosed();

        if (hmac is null)
        {
            throw new ArgumentNullException(nameof(hmac));
        }

        long expiryTimePeriodMs = -1L;
        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ExpireDelegationTokenOptions));
            expiryTimePeriodMs = options.ExpiryTimePeriodMs;
        }

        SingleAdminOperation<long> operation = new SingleAdminOperation<long>("expireDelegationToken");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        GCHandle hmacPin = default;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            hmacPin = GCHandle.Alloc(hmac, GCHandleType.Pinned);

            submit(
                _handle.DangerousGetHandle(),
                hmac.Length == 0 ? IntPtr.Zero : hmacPin.AddrOfPinnedObject(),
                hmac.Length,
                expiryTimePeriodMs,
                timeoutMs,
                AdminCallbacks.ExpireDelegationToken,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (hmacPin.IsAllocated)
            {
                hmacPin.Free();
            }
        }

        return new ExpireDelegationTokenResult(operation.Task);
    }

    internal DescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options) =>
        DescribeDelegationToken(options, NativeMethods.AdminClientDescribeDelegationTokenAsync);

    /// <summary>
    /// Submits <c>describeDelegationToken</c> and returns immediately with the single awaitable
    /// over the token list (sub-shape 3b).
    /// </summary>
    /// <remarks>
    /// ⚠ A <see langword="null"/> <c>Owners</c> and an <b>empty</b> one are different requests,
    /// and the difference travels in <c>has_owners_filter</c>, never in the count.
    /// </remarks>
    internal DescribeDelegationTokenResult DescribeDelegationToken(
        DescribeDelegationTokenOptions? options, NativeDescribeDelegationTokenSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        IReadOnlyList<KafkaPrincipal>? owners = null;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeDelegationTokenOptions));
            owners = options.Owners;
        }

        for (int i = 0; owners is not null && i < owners.Count; i++)
        {
            if (owners[i] is null)
            {
                throw new ArgumentException(
                    "DescribeDelegationTokenOptions.Owners must not contain a null element.",
                    nameof(options));
            }
        }

        SingleAdminOperation<IReadOnlyCollection<DelegationToken>> operation =
            new SingleAdminOperation<IReadOnlyCollection<DelegationToken>>("describeDelegationToken");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        DelegationTokenMarshal.PrincipalRows? pinned = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinned = DelegationTokenMarshal.PinPrincipals(
                owners ?? (IReadOnlyList<KafkaPrincipal>)Array.Empty<KafkaPrincipal>());

            submit(
                _handle.DangerousGetHandle(),
                owners is not null,
                pinned.PrincipalTypes,
                pinned.Names,
                pinned.Count,
                timeoutMs,
                AdminCallbacks.DescribeDelegationToken,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            pinned?.Dispose();
        }

        return new DescribeDelegationTokenResult(operation.Task);
    }

    internal DescribeFeaturesResult DescribeFeatures(DescribeFeaturesOptions? options) =>
        DescribeFeatures(options, NativeMethods.AdminClientDescribeFeaturesAsync);

    /// <summary>
    /// Submits <c>describeFeatures</c> and returns immediately with the <b>single</b> awaitable
    /// Java publishes, over the whole composite.
    /// </summary>
    internal DescribeFeaturesResult DescribeFeatures(
        DescribeFeaturesOptions? options, NativeDescribeFeaturesSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        int? nodeId = null;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeFeaturesOptions));
            nodeId = options.NodeId;
        }

        SingleAdminOperation<FeatureMetadata> operation =
            new SingleAdminOperation<FeatureMetadata>("describeFeatures");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // ⚠ Presence travels in has_node_id, never in the value: 0 is a legal broker id.
            submit(
                _handle.DangerousGetHandle(),
                nodeId.HasValue,
                nodeId ?? 0,
                timeoutMs,
                AdminCallbacks.DescribeFeatures,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new DescribeFeaturesResult(operation.Task);
    }

    internal UpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates, UpdateFeaturesOptions? options) =>
        UpdateFeatures(featureUpdates, options, NativeMethods.AdminClientUpdateFeaturesAsync);

    /// <summary>
    /// Submits <c>updateFeatures</c> and returns immediately with one awaitable per feature
    /// (result shape 2).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <c>max_version_levels</c> is <c>short</c>, not <c>int</c>, all the way through the
    /// features family.
    /// </para>
    /// <para>
    /// ⚠⚠ <b>The empty-map and blank-name guards are load-bearing, not defensive.</b> With zero
    /// keys the bridge mints zero awaitables, so <c>All()</c> is <c>WhenAll(&lt;empty&gt;)</c> and
    /// reports <b>success</b> — the whole-call error the ABI delivers has nowhere to go. Java
    /// rejects both inputs before it enqueues anything
    /// (<c>KafkaAdminClient.java:4590-4592</c>, <c>:4597-4599</c>).
    /// </para>
    /// </remarks>
    internal UpdateFeaturesResult UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions? options,
        NativeUpdateFeaturesSubmit submit)
    {
        ThrowIfClosed();

        if (featureUpdates is null)
        {
            throw new ArgumentNullException(nameof(featureUpdates));
        }

        if (featureUpdates.Count == 0)
        {
            throw new ArgumentException(
                "Feature updates can not be null or empty.", nameof(featureUpdates));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(UpdateFeaturesOptions));
            validateOnly = options.ValidateOnly;
        }

        List<string> keys = new List<string>(featureUpdates.Count);
        short[] maxVersionLevels = new short[featureUpdates.Count];
        int[] upgradeTypes = new int[featureUpdates.Count];
        int next = 0;
        foreach (KeyValuePair<string, FeatureUpdate> entry in featureUpdates)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The feature updates must not contain a null feature name.", nameof(featureUpdates));
            }

            if (IsBlank(entry.Key))
            {
                throw new ArgumentException(
                    "Provided feature can not be empty.", nameof(featureUpdates));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    "The feature updates must not contain a null update.", nameof(featureUpdates));
            }

            keys.Add(entry.Key);
            maxVersionLevels[next] = entry.Value.MaxVersionLevel;
            upgradeTypes[next] = (int)entry.Value.Type;
            next++;
        }

        VoidKeyedAdminOperation<string> operation = new VoidKeyedAdminOperation<string>(
            "updateFeatures", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // Shape 4b: one callback per feature. `featureUpdates` is a map, and an empty
            // one is rejected above, so the key count is the number and the ABI's
            // never-invoked `count == 0` case is unreachable through this surface.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                PinNames(keys, pinned),
                maxVersionLevels,
                upgradeTypes,
                keys.Count,
                timeoutMs,
                validateOnly,
                AdminCallbacks.UpdateFeatures,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            foreach (Utf8Marshal.PinnedUtf8String pin in pinned)
            {
                pin.Dispose();
            }
        }

        return new UpdateFeaturesResult(operation.Tasks, operation.KeyComparer);
    }

    internal FenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options) =>
        FenceProducers(transactionalIds, options, NativeMethods.AdminClientFenceProducersAsync);

    /// <summary>
    /// Submits <c>fenceProducers</c> and returns immediately with one awaitable per
    /// transactional id (result shape 1, value <c>ProducerIdAndEpoch</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>No empty-input guard.</b> Java builds a future from the collection and calls
    /// <c>invokeDriver</c> with no validation (<c>KafkaAdminClient.java:4889-4895</c>), so an
    /// empty request succeeds with an empty result; adding a Java-less throw would be the
    /// defect.
    /// </para>
    /// <para>
    /// The ids are de-duplicated because Java keys its result on a <c>Map</c>, and a null
    /// element is rejected because the header <em>skips</em> one silently.
    /// </para>
    /// </remarks>
    internal FenceProducersResult FenceProducers(
        IReadOnlyCollection<string> transactionalIds,
        FenceProducersOptions? options,
        NativeFenceProducersSubmit submit)
    {
        ThrowIfClosed();

        if (transactionalIds is null)
        {
            throw new ArgumentNullException(nameof(transactionalIds));
        }

        int timeoutMs = options is null
            ? UnsetTimeoutMs
            : ValidateTimeoutMs(options.TimeoutMs, nameof(FenceProducersOptions));

        List<string> keys = DistinctNames(transactionalIds, "transactional ids", nameof(transactionalIds));

        KeyedAdminOperation<string, ProducerIdAndEpoch> operation =
            new KeyedAdminOperation<string, ProducerIdAndEpoch>(
                "fenceProducers", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // Shape 4a: the ABI fires "exactly once per DISTINCT requested id", never at
            // all for zero distinct ids, and `keys` is already distinct (DistinctNames
            // above) — so that is keys.Count, with the submit's own token released after.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                PinNames(keys, pinned),
                keys.Count,
                timeoutMs,
                AdminCallbacks.FenceProducers,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the ids out during the submit.
            foreach (Utf8Marshal.PinnedUtf8String pin in pinned)
            {
                pin.Dispose();
            }
        }

        return new FenceProducersResult(operation.Tasks, operation.KeyComparer);
    }

    internal DescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options) =>
        DescribeTransactions(
            transactionalIds, options, NativeMethods.AdminClientDescribeTransactionsAsync);

    /// <summary>
    /// Submits <c>describeTransactions</c> and returns immediately with one awaitable per
    /// transactional id (result shape 1, value <c>TransactionDescription</c>).
    /// </summary>
    /// <remarks>
    /// Like <c>fenceProducers</c>: Java adds no empty-input guard
    /// (<c>KafkaAdminClient.java:4833-4839</c>), the ids are de-duplicated because Java keys
    /// its result on a map, and a null element is rejected because the header skips one
    /// silently.
    /// </remarks>
    internal DescribeTransactionsResult DescribeTransactions(
        IReadOnlyCollection<string> transactionalIds,
        DescribeTransactionsOptions? options,
        NativeDescribeTransactionsSubmit submit)
    {
        ThrowIfClosed();

        if (transactionalIds is null)
        {
            throw new ArgumentNullException(nameof(transactionalIds));
        }

        int timeoutMs = options is null
            ? UnsetTimeoutMs
            : ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeTransactionsOptions));

        List<string> keys = DistinctNames(transactionalIds, "transactional ids", nameof(transactionalIds));

        KeyedAdminOperation<string, TransactionDescription> operation =
            new KeyedAdminOperation<string, TransactionDescription>(
                "describeTransactions", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // Shape 4a, same countdown as fenceProducers: once per distinct id, and
            // `keys` is already distinct.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                PinNames(keys, pinned),
                keys.Count,
                timeoutMs,
                AdminCallbacks.DescribeTransactions,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the ids out during the submit.
            foreach (Utf8Marshal.PinnedUtf8String pin in pinned)
            {
                pin.Dispose();
            }
        }

        return new DescribeTransactionsResult(operation.Tasks);
    }

    internal DescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options) =>
        DescribeProducers(partitions, options, NativeMethods.AdminClientDescribeProducersAsync);

    /// <summary>
    /// Submits <c>describeProducers</c> and returns immediately with one awaitable per topic
    /// partition (result shape 1, value <c>PartitionProducerState</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <c>options.BrokerId</c> is Java's <c>OptionalInt</c> and crosses as an explicit
    /// discriminant plus a value: Java's <c>brokerId(int)</c> setter accepts any <c>int</c>
    /// (<c>DescribeProducersOptions.java:44</c>), so no sentinel is free and a negative broker
    /// id is a request the ABI must be able to carry — the timeout's negative guard
    /// deliberately does not apply to it.
    /// </remarks>
    internal DescribeProducersResult DescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions,
        DescribeProducersOptions? options,
        NativeDescribeProducersSubmit submit)
    {
        ThrowIfClosed();

        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        int timeoutMs = UnsetTimeoutMs;
        int brokerId = 0;
        bool hasBrokerId = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeProducersOptions));
            hasBrokerId = options.BrokerId.HasValue;
            brokerId = options.BrokerId ?? 0;
        }

        // De-duplicated because Java keys its result on a Map, and null-topic-checked because
        // the ABI would skip such an entry silently and desynchronize the parallel arrays.
        List<TopicPartition> keys = DistinctPartitions(partitions, nameof(partitions));

        KeyedAdminOperation<TopicPartition, DescribeProducersResult.PartitionProducerState> operation =
            new KeyedAdminOperation<TopicPartition, DescribeProducersResult.PartitionProducerState>(
                "describeProducers", keys, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            int[] partitionIds = new int[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitionIds[i] = keys[i].Partition;
            }

            // Shape 4a: the ABI fires once per DISTINCT (topic, partition) pair, and
            // `keys` is already distinct (DistinctPartitions above), so that is keys.Count.
            // The submit's own token is released after, so an empty collection releases.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitionIds,
                keys.Count,
                hasBrokerId,
                brokerId,
                timeoutMs,
                AdminCallbacks.DescribeProducers,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the topics out during the submit.
            foreach (Utf8Marshal.PinnedUtf8String pin in pinned)
            {
                pin.Dispose();
            }
        }

        return new DescribeProducersResult(operation.Tasks);
    }

    internal ListTransactionsResult ListTransactions(ListTransactionsOptions? options) =>
        ListTransactions(options, NativeMethods.AdminClientListTransactionsAsync);

    /// <summary>
    /// Submits <c>listTransactions</c> and returns immediately with the one broker-keyed
    /// awaitable Java's result holds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Two independent filters, each with its own count.</b> Each array and its count
    /// come from one helper call, so there is no free-standing count at the call site to
    /// transpose — the two filters have different lengths in the submit-seam test for exactly
    /// that reason.
    /// </para>
    /// <para>
    /// ⚠ Three different neutral encodings survive to the wire: an empty filter array means
    /// "every value"; a <b>negative</b> duration means no duration filter, so a <c>0</c> is a
    /// real one; and a null pattern is distinct from an empty pattern, which the broker
    /// evaluates (<c>h:10645-10652</c>).
    /// </para>
    /// </remarks>
    internal ListTransactionsResult ListTransactions(
        ListTransactionsOptions? options, NativeListTransactionsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = options is null
            ? UnsetTimeoutMs
            : ValidateTimeoutMs(options.TimeoutMs, nameof(ListTransactionsOptions));

        // Java's own -1 default. Any negative value is "no filter", so this is a pass-through
        // value rather than a sentinel the binding may normalize.
        long durationMs = options?.FilteredDuration ?? -1L;

        (long[] Values, int Count) producerIds = ProducerIdFilter(options);

        SingleAdminOperation<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>
            operation =
                new SingleAdminOperation<
                    IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>(
                    "listTransactions");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>();
        Utf8Marshal.PinnedUtf8String? pattern = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            (IntPtr[] Values, int Count) states = StateFilter(options, pinned);

            // ⚠ null stays NULL: an empty pattern is a legal value the broker evaluates.
            if (options?.FilteredTransactionalIdPattern is not null)
            {
                pattern = Utf8Marshal.Pin(options.FilteredTransactionalIdPattern);
            }

            submit(
                _handle.DangerousGetHandle(),
                states.Values,
                states.Count,
                producerIds.Values,
                producerIds.Count,
                durationMs,
                pattern?.Pointer ?? IntPtr.Zero,
                timeoutMs,
                AdminCallbacks.ListTransactions,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Call-scoped pins (ffi §A4): the ABI copies the names out during the submit.
            pattern?.Dispose();
            foreach (Utf8Marshal.PinnedUtf8String pin in pinned)
            {
                pin.Dispose();
            }
        }

        return new ListTransactionsResult(operation.Task);
    }

    /// <summary>
    /// The state filter and its count, produced together — see the transposition note on
    /// <see cref="ListTransactions(ListTransactionsOptions, NativeListTransactionsSubmit)"/>.
    /// </summary>
    /// <param name="options">The request options, or null.</param>
    /// <param name="pinned">Receives the call-scoped pins the returned pointers borrow from.</param>
    /// <returns>The pinned <c>TransactionState.toString()</c> names and their count.</returns>
    private static (IntPtr[] Values, int Count) StateFilter(
        ListTransactionsOptions? options, List<Utf8Marshal.PinnedUtf8String> pinned)
    {
        IReadOnlyCollection<TransactionState> states =
            options?.FilteredStates ?? (IReadOnlyCollection<TransactionState>)Array.Empty<TransactionState>();

        IntPtr[] names = new IntPtr[states.Count];
        int next = 0;
        foreach (TransactionState state in states)
        {
            // ⚠ Java's toString(), never name() — TransactionMarshal owns the spelling.
            Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(TransactionMarshal.WireName(state));
            pinned.Add(name);
            names[next] = name.Pointer;
            next++;
        }

        return (names, names.Length);
    }

    /// <summary>
    /// The producer-id filter and its count, produced together — see the transposition note on
    /// <see cref="ListTransactions(ListTransactionsOptions, NativeListTransactionsSubmit)"/>.
    /// </summary>
    /// <param name="options">The request options, or null.</param>
    /// <returns>The producer ids and their count.</returns>
    private static (long[] Values, int Count) ProducerIdFilter(ListTransactionsOptions? options)
    {
        IReadOnlyCollection<long> producerIds =
            options?.FilteredProducerIds ?? (IReadOnlyCollection<long>)Array.Empty<long>();

        long[] values = new long[producerIds.Count];
        int next = 0;
        foreach (long producerId in producerIds)
        {
            values[next] = producerId;
            next++;
        }

        return (values, values.Length);
    }

    internal AbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options) =>
        AbortTransaction(spec, options, NativeMethods.AdminClientAbortTransactionAsync);

    /// <summary>
    /// Submits <c>abortTransaction</c> and returns immediately with the single awaitable
    /// Java's <c>AbortTransactionResult</c> publishes (result shape 6 — no result handle).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>The ABI fires the completion callback synchronously on this thread for a rejected
    /// input</b> (a NULL topic, or an epoch outside 16 bits — <c>h:10474-10486</c>), so the
    /// <c>GCHandle</c> and the operation are published before the P/Invoke, exactly as for
    /// every other admin submit.
    /// </para>
    /// <para>
    /// Of those two ABI rejections only the NULL topic is reachable from C#, and it is
    /// rejected here first: <see cref="AbortTransactionSpec.ProducerEpoch"/> is a
    /// <see langword="short"/>, so an out-of-16-bit epoch cannot be expressed.
    /// </para>
    /// </remarks>
    internal AbortTransactionResult AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options, NativeAbortTransactionSubmit submit)
    {
        ThrowIfClosed();

        if (spec is null)
        {
            throw new ArgumentNullException(nameof(spec));
        }

        if (spec.TopicPartition.Topic is null)
        {
            throw new ArgumentException(
                "The abort transaction spec must not carry a topic partition with a null topic.",
                nameof(spec));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AbortTransactionOptions));
        }

        SingleAdminOperation<bool> operation = new SingleAdminOperation<bool>("abortTransaction");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        Utf8Marshal.PinnedUtf8String? topic = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            topic = Utf8Marshal.Pin(spec.TopicPartition.Topic);
            submit(
                _handle.DangerousGetHandle(),
                topic.Pointer,
                spec.TopicPartition.Partition,
                spec.ProducerId,
                spec.ProducerEpoch,
                spec.CoordinatorEpoch,
                timeoutMs,
                AdminCallbacks.AbortTransaction,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            topic?.Dispose();
        }

        return new AbortTransactionResult(operation.Task);
    }

    internal TerminateTransactionResult ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions? options) =>
        ForceTerminateTransaction(
            transactionalId, options, NativeMethods.AdminClientForceTerminateTransactionAsync);

    /// <summary>
    /// Submits <c>forceTerminateTransaction</c> and returns immediately with the single
    /// awaitable Java's <c>TerminateTransactionResult</c> publishes as <c>result()</c>
    /// (result shape 6 — no result handle).
    /// </summary>
    /// <remarks>
    /// ⚠ A NULL transactional id fires the callback synchronously on this thread
    /// (<c>h:10535-10546</c>); it is rejected here first, before any pin.
    /// </remarks>
    internal TerminateTransactionResult ForceTerminateTransaction(
        string transactionalId,
        TerminateTransactionOptions? options,
        NativeForceTerminateTransactionSubmit submit)
    {
        ThrowIfClosed();

        if (transactionalId is null)
        {
            throw new ArgumentNullException(nameof(transactionalId));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(TerminateTransactionOptions));
        }

        SingleAdminOperation<bool> operation =
            new SingleAdminOperation<bool>("forceTerminateTransaction");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        Utf8Marshal.PinnedUtf8String? pinnedId = null;
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            pinnedId = Utf8Marshal.Pin(transactionalId);
            submit(
                _handle.DangerousGetHandle(),
                pinnedId.Pointer,
                timeoutMs,
                AdminCallbacks.ForceTerminateTransaction,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            pinnedId?.Dispose();
        }

        return new TerminateTransactionResult(operation.Task);
    }

    internal ListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions, ListPartitionReassignmentsOptions? options) =>
        ListPartitionReassignments(
            partitions, options, NativeMethods.AdminClientListPartitionReassignmentsAsync);

    /// <summary>
    /// Submits <c>listPartitionReassignments</c> and returns immediately with the
    /// <b>single</b> awaitable Java's <c>ListPartitionReassignmentsResult</c> wraps
    /// (result shape 3).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>One awaitable, despite the name's parallel with
    /// <c>alterPartitionReassignments</c>.</b> Java stores a single
    /// <c>KafkaFuture&lt;Map&lt;TopicPartition, PartitionReassignment&gt;&gt;</c>
    /// (<c>ListPartitionReassignmentsResult.java:31</c>), and the ABI's result declares no
    /// <c>get_error</c> at all — so this uses <see cref="SingleAdminOperation{TValue}"/>,
    /// not the per-key bridge.
    /// </para>
    /// <para>
    /// ⚠ <b>Keys cannot be pre-registered even in principle</b>: the header says "only
    /// partitions with an ongoing reassignment appear in the result, so it can be shorter
    /// than the request". A per-key bridge would fault every quiet partition.
    /// </para>
    /// <para>
    /// ⚠ <b><see langword="null"/> and empty are different requests.</b>
    /// <see langword="null"/> is Java's <c>Optional.empty()</c> — list every ongoing
    /// reassignment in the cluster (<c>Admin.java:1246-1247</c>) — and sets
    /// <c>all_partitions</c>. An empty collection asks about no partitions at all.
    /// </para>
    /// </remarks>
    internal ListPartitionReassignmentsResult ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions,
        ListPartitionReassignmentsOptions? options,
        NativeListPartitionReassignmentsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListPartitionReassignmentsOptions));
        }

        // NOT `partitions?.Count == 0` folded in — see the null-versus-empty note above.
        bool allPartitions = partitions is null;
        List<TopicPartition> selection = DistinctPartitions(partitions, nameof(partitions));

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> operation =
            new SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                "listPartitionReassignments");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(selection.Count);

            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[selection.Count];
            int[] partitionIds = new int[selection.Count];
            for (int i = 0; i < selection.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(selection[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitionIds[i] = selection[i].Partition;
            }

            submit(
                _handle.DangerousGetHandle(),
                allPartitions,
                topics,
                partitionIds,
                selection.Count,
                timeoutMs,
                AdminCallbacks.ListPartitionReassignments,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Pinned only for the call (ffi §A4): the ABI copies out during the submit.
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new ListPartitionReassignmentsResult(operation.Task);
    }

    internal ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions? options) =>
        ListOffsets(topicPartitionOffsets, options, NativeMethods.AdminClientListOffsetsAsync);

    /// <summary>
    /// Submits <c>listOffsets</c> and returns immediately with one awaitable per topic
    /// partition. Java's <c>Map&lt;TopicPartition, OffsetSpec&gt;</c> becomes the ABI's four
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>Each spec is encoded as a (flag, value) PAIR, and the flag is not
    /// redundant.</b> The six no-argument kinds map to <c>ListOffsets</c> wire sentinels;
    /// <see cref="OffsetSpec.ForTimestamp"/> maps to the timestamp itself — and those two
    /// ranges overlap, so <c>ForTimestamp(-2)</c> and <see cref="OffsetSpec.Earliest"/>
    /// would be indistinguishable without <c>is_timestamp</c>. The header states it: they
    /// "both yield <c>-2</c>, yet Java treats them differently up to that point". Collapsing
    /// the flag is a silent wrong-answer defect.
    /// </remarks>
    internal ListOffsetsResult ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions? options,
        NativeListOffsetsSubmit submit)
    {
        ThrowIfClosed();

        if (topicPartitionOffsets is null)
        {
            throw new ArgumentNullException(nameof(topicPartitionOffsets));
        }

        int timeoutMs = UnsetTimeoutMs;
        IsolationLevel isolationLevel = IsolationLevel.ReadUncommitted;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListOffsetsOptions));
            isolationLevel = options.IsolationLevel;
        }

        // Java's parameter is the IsolationLevel enum, so a value outside its two members
        // is not expressible there; in C# it is, by a cast. Rejected here as the programmer
        // error it is (ffi §B5) rather than left to the ABI, which documents an inline
        // callback carrying an IllegalArgument instead — the same call this binding already
        // makes for ElectionType.
        if (isolationLevel != IsolationLevel.ReadUncommitted && isolationLevel != IsolationLevel.ReadCommitted)
        {
            throw new ArgumentOutOfRangeException(
                "options",
                isolationLevel,
                "ListOffsetsOptions.IsolationLevel must be IsolationLevel.ReadUncommitted or "
                + "IsolationLevel.ReadCommitted.");
        }

        List<TopicPartition> keys = new List<TopicPartition>(topicPartitionOffsets.Count);
        int[] partitionIds = new int[topicPartitionOffsets.Count];
        bool[] isTimestamp = new bool[topicPartitionOffsets.Count];
        long[] specTimestamps = new long[topicPartitionOffsets.Count];
        int next = 0;
        foreach (KeyValuePair<TopicPartition, OffsetSpec> entry in topicPartitionOffsets)
        {
            // A `default(TopicPartition)` has a null Topic, and the header's "an entry with
            // a NULL topic is skipped" would silently drop it (ffi §B5).
            if (entry.Key.Topic is null)
            {
                throw new ArgumentException(
                    "The offsets map must not contain a topic partition with a null topic.",
                    nameof(topicPartitionOffsets));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The offset spec for '{entry.Key}' must not be null.",
                    nameof(topicPartitionOffsets));
            }

            keys.Add(entry.Key);
            partitionIds[next] = entry.Key.Partition;

            // ⚠ The pair, never the value alone. A TimestampSpec sets the flag and carries
            // its own number whatever that number is; every other kind clears the flag and
            // carries its wire sentinel.
            if (entry.Value is OffsetSpec.TimestampSpec timestamp)
            {
                isTimestamp[next] = true;
                specTimestamps[next] = timestamp.Timestamp;
            }
            else
            {
                isTimestamp[next] = false;
                specTimestamps[next] = SentinelFor(entry.Value, nameof(topicPartitionOffsets));
            }

            next++;
        }

        // EqualityComparer<TopicPartition>.Default dispatches to the struct's own
        // IEquatable implementation, so it neither boxes nor disagrees with the public
        // ListOffsetsResult view — the same reasoning as the deleteRecords path.
        KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> operation =
            new KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>(
                "listOffsets", keys, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
            }

            // Shape 4a: the ABI fires once per (topic, partition) pair in the input —
            // including on the inline whole-call failure (an unknown isolation level or an
            // unrecognised sentinel), which fires for every key — and `keys` is already
            // distinct (a map's key set) with no null topic, so that is keys.Count. The
            // submit's own token is released after, so an empty map releases.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitionIds,
                isTimestamp,
                specTimestamps,
                keys.Count,
                timeoutMs,
                (int)isolationLevel,
                AdminCallbacks.ListOffsets,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new ListOffsetsResult(operation.Tasks);
    }

    internal ListGroupsResult ListGroups(ListGroupsOptions? options) =>
        ListGroups(options, NativeMethods.AdminClientListGroupsAsync);

    /// <summary>
    /// Submits <c>listGroups</c> and returns immediately with the <b>single</b> awaitable
    /// Java's <c>ListGroupsResult</c> wraps over its two independent collections (result
    /// sub-shape 3c).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Three independent filter axes, never parallel arrays.</b> Each travels with its
    /// own count, and an axis left empty is Java's empty <c>Set</c> — "do not filter on this
    /// one" — not "match nothing". So the no-options call submits three empty arrays and
    /// three zero counts, which the ABI reads as an unfiltered listing; sizing any axis from
    /// another's count would silently narrow or widen the request.
    /// </para>
    /// <para>
    /// ⚠ <b>The enums cross as Java's <c>toString()</c> names, not as ordinals</b> — neither
    /// <see cref="GroupState"/> nor <see cref="GroupType"/> has a numeric id in Java, which
    /// the header restates at every accessor. A value no member defines is reachable in C#
    /// only by a cast; it is rejected here, before anything is pinned, rather than encoded,
    /// because <see cref="GroupMarshal"/>'s encode direction is deliberately partial and the
    /// caller's parameter is what names the blame (ffi §B5).
    /// </para>
    /// <para>
    /// Every name is pinned only for the call (ffi §A4): the core copies each one out during
    /// the submit, so nothing native holds them afterwards and the <c>finally</c> unpins on
    /// every path — including the inline-callback one, which has already run to completion by
    /// the time the P/Invoke returns.
    /// </para>
    /// </remarks>
    internal ListGroupsResult ListGroups(ListGroupsOptions? options, NativeListGroupsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        IReadOnlyCollection<GroupState> groupStates = Array.Empty<GroupState>();
        IReadOnlyCollection<string> protocolTypes = Array.Empty<string>();
        IReadOnlyCollection<GroupType> types = Array.Empty<GroupType>();
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListGroupsOptions));

            // Each getter already hands back a de-duplicated, immutable, null-element-free
            // copy, so there is nothing left to validate on the protocol-type axis.
            groupStates = options.GroupStates;
            protocolTypes = options.ProtocolTypes;
            types = options.Types;
        }

        // Encoded BEFORE the operation is rooted and before anything is pinned (ffi §B5), so
        // an undefined cast value throws with nothing to unwind.
        List<string> stateNames = FilterNames(
            groupStates,
            GroupMarshal.NameFromState,
            nameof(ListGroupsOptions),
            nameof(ListGroupsOptions.GroupStates));
        List<string> typeNames = FilterNames(
            types,
            GroupMarshal.NameFromType,
            nameof(ListGroupsOptions),
            nameof(ListGroupsOptions.Types));

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>
            operation =
                new SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>(
                    "listGroups");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(
                stateNames.Count + protocolTypes.Count + typeNames.Count);

            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] statePointers = PinNames(stateNames, pinned);
            IntPtr[] protocolPointers = PinNames(protocolTypes, pinned);
            IntPtr[] typePointers = PinNames(typeNames, pinned);

            // ⚠ Each axis carries ITS OWN length. Reusing one count for another axis is the
            // defect this shape invites.
            submit(
                _handle.DangerousGetHandle(),
                statePointers,
                statePointers.Length,
                protocolPointers,
                protocolPointers.Length,
                typePointers,
                typePointers.Length,
                timeoutMs,
                AdminCallbacks.ListGroups,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Pinned only for the call (ffi §A4's call-scoped rule): the ABI copies every
            // name out during the submit.
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String name in pinned)
                {
                    name.Dispose();
                }
            }
        }

        return new ListGroupsResult(operation.Task);
    }

#pragma warning disable CS0618 // Java deprecates this RPC and its three types; mirrored, not avoided.

    internal ListConsumerGroupsResult ListConsumerGroups(ListConsumerGroupsOptions? options) =>
        ListConsumerGroups(options, NativeMethods.AdminClientListConsumerGroupsAsync);

    /// <summary>
    /// Submits <c>listConsumerGroups</c> and returns immediately with the <b>single</b> awaitable
    /// Java's <c>ListConsumerGroupsResult</c> wraps over its two independent collections (result
    /// sub-shape 3c).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>Two filter axes, not three.</b> This is the generation-older sibling of
    /// <c>ListGroups</c>, and <c>ListConsumerGroupsOptions.java</c> carries no protocol-type
    /// filter — so the ABI takes group state and group type only. Copying the three-axis argument
    /// list across would feed the type axis into the ABI's <em>second</em> array and shift every
    /// argument after it, including the callback pointer.
    /// </para>
    /// <para>
    /// ⚠ <b><see cref="ListConsumerGroupsOptions.States"/> is not a second state axis.</b> It is
    /// the same set of states projected into the older <see cref="ConsumerGroupState"/> spelling —
    /// Java defines the deprecated <c>inStates(Set&lt;ConsumerGroupState&gt;)</c> in terms of
    /// <c>inGroupStates(...)</c> — so there is nothing extra to submit, and submitting it as well
    /// would filter one axis twice.
    /// </para>
    /// <para>
    /// ⚠ <b>The two arrays are never parallel.</b> Each travels with its own count, and an axis
    /// left empty is Java's empty <c>Set</c> — "do not filter on this one" — not "match nothing".
    /// So the no-options call submits two empty arrays and two zero counts, which the ABI reads as
    /// an unfiltered listing; sizing either axis from the other's count would silently narrow or
    /// widen the request.
    /// </para>
    /// <para>
    /// ⚠ <b>The enums cross as Java's <c>toString()</c> names, not as ordinals</b> — neither
    /// <see cref="GroupState"/> nor <see cref="GroupType"/> has a numeric id in Java. A value no
    /// member defines is reachable in C# only by a cast; it is rejected here, before the operation
    /// is rooted and before anything is pinned, because <see cref="GroupMarshal"/>'s encode
    /// direction is deliberately partial and the caller's parameter is what names the blame
    /// (ffi §B5).
    /// </para>
    /// <para>
    /// Every name is pinned only for the call (ffi §A4): the core copies each one out during the
    /// submit, so nothing native holds them afterwards and the <c>finally</c> unpins on every path
    /// — including the inline-callback one, which has already run to completion by the time the
    /// P/Invoke returns.
    /// </para>
    /// </remarks>
    internal ListConsumerGroupsResult ListConsumerGroups(
        ListConsumerGroupsOptions? options, NativeListConsumerGroupsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        IReadOnlyCollection<GroupState> groupStates = Array.Empty<GroupState>();
        IReadOnlyCollection<GroupType> types = Array.Empty<GroupType>();
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListConsumerGroupsOptions));

            // Each getter already hands back a de-duplicated, immutable copy. States is
            // deliberately not read — see the remarks above.
            groupStates = options.GroupStates;
            types = options.Types;
        }

        // Encoded BEFORE the operation is rooted and before anything is pinned (ffi §B5), so
        // an undefined cast value throws with nothing to unwind.
        List<string> stateNames = FilterNames(
            groupStates,
            GroupMarshal.NameFromState,
            nameof(ListConsumerGroupsOptions),
            nameof(ListConsumerGroupsOptions.GroupStates));
        List<string> typeNames = FilterNames(
            types,
            GroupMarshal.NameFromType,
            nameof(ListConsumerGroupsOptions),
            nameof(ListConsumerGroupsOptions.Types));

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>
            operation =
                new SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>(
                    "listConsumerGroups");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(stateNames.Count + typeNames.Count);

            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] statePointers = PinNames(stateNames, pinned);
            IntPtr[] typePointers = PinNames(typeNames, pinned);

            // ⚠ Each axis carries ITS OWN length. Reusing one count for the other axis is the
            // defect this shape invites.
            submit(
                _handle.DangerousGetHandle(),
                statePointers,
                statePointers.Length,
                typePointers,
                typePointers.Length,
                timeoutMs,
                AdminCallbacks.ListConsumerGroups,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // Pinned only for the call (ffi §A4's call-scoped rule): the ABI copies every
            // name out during the submit.
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String name in pinned)
                {
                    name.Dispose();
                }
            }
        }

        return new ListConsumerGroupsResult(operation.Task);
    }

#pragma warning restore CS0618

    internal DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options) =>
        DescribeConsumerGroups(groupIds, options, NativeMethods.AdminClientDescribeConsumerGroupsAsync);

    /// <summary>
    /// Submits <c>describeConsumerGroups</c> and returns immediately with one awaitable per
    /// requested group id — Java's <c>Map&lt;String, KafkaFuture&lt;ConsumerGroupDescription&gt;&gt;</c>
    /// (result shape 1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>The bridge must be keyed by <see cref="StringComparer.Ordinal"/>.</b>
    /// <see cref="DescribeConsumerGroupsResult"/>'s aggregate hardcodes that comparer, and —
    /// unlike <c>DescribeTopicsResult</c> — Java declares its constructor <b>public</b>, so
    /// there is no internal factory through which a different comparer could be threaded.
    /// A bridge built with the default comparer would therefore not fail loudly; it would
    /// silently disagree with the result object it feeds, for group ids differing only by
    /// culture-sensitive equivalence. The same warning is recorded on
    /// <c>AdminCallbacks.DescribeConsumerGroupsKey</c>, the reader at the other end.
    /// </para>
    /// <para>
    /// ⚠ <b>One flag, not two.</b> Java's <c>DescribeConsumerGroupsOptions</c> has no
    /// partition-size limit, so <c>includeAuthorizedOperations</c> is the last value before
    /// the callback pointer — see <see cref="NativeDescribeConsumerGroupsSubmit"/>.
    /// </para>
    /// <para>
    /// The group ids cross as a pinned <c>IntPtr[]</c> of UTF-8 plus a separate count, never
    /// as a <c>string[]</c> whose default marshaller would emit ANSI (ffi §A2). The pins are
    /// call-scoped (§A4) — the core copies every id out during the submit — and
    /// <see cref="Submit"/> owns that sequence, together with the rooting of the operation
    /// and the span-the-op client reference that keeps <c>AdminClient_destroy</c> from
    /// running under an in-flight call.
    /// </para>
    /// </remarks>
    internal DescribeConsumerGroupsResult DescribeConsumerGroups(
        IReadOnlyCollection<string> groupIds,
        DescribeConsumerGroupsOptions? options,
        NativeDescribeConsumerGroupsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (groupIds is null)
        {
            throw new ArgumentNullException(nameof(groupIds));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool includeAuthorizedOperations = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeConsumerGroupsOptions));
            includeAuthorizedOperations = options.IncludeAuthorizedOperations;
        }

        List<string> keys = DistinctNames(groupIds, "group ids", nameof(groupIds));

        KeyedAdminOperation<string, ConsumerGroupDescription> operation =
            new KeyedAdminOperation<string, ConsumerGroupDescription>(
                "describeConsumerGroups", keys, StringComparer.Ordinal);

        Submit(
            operation,
            keys,
            (admin, pinned, count, callbackUserData) =>
            {
                // Shape 4a: once per distinct group id, and `keys` is already distinct
                // (DistinctNames above). The submit's own token is released after, so an
                // empty collection (zero callbacks) releases instead of leaking.
                operation.SetPendingCallbacks(count);
                submit(
                    admin,
                    pinned,
                    count,
                    timeoutMs,
                    includeAuthorizedOperations,
                    AdminCallbacks.DescribeConsumerGroups,
                    callbackUserData);
                operation.ReleaseSubmitToken();
            });

        return new DescribeConsumerGroupsResult(operation.Tasks);
    }

    internal DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options) =>
        DescribeClassicGroups(groupIds, options, NativeMethods.AdminClientDescribeClassicGroupsAsync);

    /// <summary>
    /// Submits <c>describeClassicGroups</c> and returns immediately with one awaitable per
    /// requested group id — Java's <c>Map&lt;String, KafkaFuture&lt;ClassicGroupDescription&gt;&gt;</c>
    /// (result shape 1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>The bridge must be keyed by <see cref="StringComparer.Ordinal"/></b>, for the
    /// reason spelled out on <see cref="DescribeConsumerGroups(IReadOnlyCollection{string}, DescribeConsumerGroupsOptions?, NativeDescribeConsumerGroupsSubmit)"/>:
    /// <see cref="DescribeClassicGroupsResult"/>'s aggregate hardcodes that comparer and its
    /// constructor is public, so a bridge built with the default comparer would not fail
    /// loudly — it would silently disagree with the result object it feeds, for group ids
    /// differing only by culture-sensitive equivalence. The same warning is recorded on
    /// <c>AdminCallbacks.DescribeClassicGroupsKey</c>, the reader at the other end.
    /// </para>
    /// <para>
    /// ⚠ <b>One flag, and it is the last value before the callback.</b> Java's
    /// <c>DescribeClassicGroupsOptions</c> declares no partition-size limit — see
    /// <see cref="NativeDescribeClassicGroupsSubmit"/>.
    /// </para>
    /// <para>
    /// The group ids cross as a pinned <c>IntPtr[]</c> of UTF-8 plus a separate count, never
    /// as a <c>string[]</c> whose default marshaller would emit ANSI (ffi §A2). The pins are
    /// call-scoped (§A4) — the core copies every id out during the submit — and
    /// <see cref="Submit"/> owns that sequence, together with the rooting of the operation
    /// and the span-the-op client reference that keeps <c>AdminClient_destroy</c> from
    /// running under an in-flight call.
    /// </para>
    /// </remarks>
    /// <param name="groupIds">The classic group ids to describe. Duplicates collapse.</param>
    /// <param name="options">The options, or <see langword="null"/> for the defaults.</param>
    /// <param name="submit">The native submit, injectable for tests.</param>
    internal DescribeClassicGroupsResult DescribeClassicGroups(
        IReadOnlyCollection<string> groupIds,
        DescribeClassicGroupsOptions? options,
        NativeDescribeClassicGroupsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (groupIds is null)
        {
            throw new ArgumentNullException(nameof(groupIds));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool includeAuthorizedOperations = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeClassicGroupsOptions));
            includeAuthorizedOperations = options.IncludeAuthorizedOperations;
        }

        List<string> keys = DistinctNames(groupIds, "group ids", nameof(groupIds));

        KeyedAdminOperation<string, ClassicGroupDescription> operation =
            new KeyedAdminOperation<string, ClassicGroupDescription>(
                "describeClassicGroups", keys, StringComparer.Ordinal);

        Submit(
            operation,
            keys,
            (admin, pinned, count, callbackUserData) =>
            {
                // Shape 4a, same countdown as describeConsumerGroups.
                operation.SetPendingCallbacks(count);
                submit(
                    admin,
                    pinned,
                    count,
                    timeoutMs,
                    includeAuthorizedOperations,
                    AdminCallbacks.DescribeClassicGroups,
                    callbackUserData);
                operation.ReleaseSubmitToken();
            });

        return new DescribeClassicGroupsResult(operation.Tasks);
    }

    internal ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options) =>
        ListConsumerGroupOffsets(
            groupSpecs, options, NativeMethods.AdminClientListConsumerGroupOffsetsAsync);

    /// <summary>
    /// Submits <c>listConsumerGroupOffsets</c> and returns immediately with one awaitable per
    /// requested group id — Java's
    /// <c>Map&lt;String, KafkaFuture&lt;Map&lt;TopicPartition, OffsetAndMetadata&gt;&gt;&gt;</c>
    /// (result shape 1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>The bridge must be keyed by <see cref="StringComparer.Ordinal"/></b>, for the
    /// reason spelled out on <see cref="DescribeConsumerGroups(IReadOnlyCollection{string}, DescribeConsumerGroupsOptions?, NativeDescribeConsumerGroupsSubmit)"/>:
    /// <see cref="ListConsumerGroupOffsetsResult"/> hardcodes that comparer, so a bridge built
    /// with the default comparer would silently disagree with the result object it feeds.
    /// </para>
    /// <para>
    /// ⚠ <b>The all-versus-empty distinction is the whole point of this argument shape.</b> A
    /// spec whose <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions"/> is
    /// <see langword="null"/> asks for every committed partition and crosses with
    /// <c>allPartitions[i] == true</c>; a spec carrying an <em>empty</em> collection asks for
    /// nothing and crosses with <c>false</c> and a count of 0. Collapsing the two — for
    /// instance by deriving the flag from the count — would turn "no partitions" into "all
    /// partitions", which is a data-returning difference, not a cosmetic one.
    /// </para>
    /// <para>
    /// A group whose selection contributes no pairs (either of the two cases above) passes a
    /// <see cref="IntPtr.Zero"/> inner pointer with a count of 0. The core null-checks both
    /// inner pointers before reading, so that is well defined rather than a zero-length pin.
    /// </para>
    /// <para>
    /// Every string crosses as pinned UTF-8, never as a <c>string[]</c> whose default
    /// marshaller would emit ANSI (ffi §A2), and every pin — the group ids, the per-group
    /// topic names, and the inner <c>IntPtr[]</c> / <c>int[]</c> arrays — is call-scoped
    /// (§A4): the core copies the whole request out during the submit. The operation is
    /// rooted and the span-the-op client reference is taken before the P/Invoke, because the
    /// callback may fire inline.
    /// </para>
    /// </remarks>
    /// <param name="groupSpecs">The per-group partition selections, keyed by group id.</param>
    /// <param name="options">The options, or <see langword="null"/> for the defaults.</param>
    /// <param name="submit">The native submit, injectable for tests.</param>
    /// <exception cref="ArgumentNullException"><paramref name="groupSpecs"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A group id, a spec, or a selected topic is null, or a group id repeats.
    /// </exception>
    internal ListConsumerGroupOffsetsResult ListConsumerGroupOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options,
        NativeListConsumerGroupOffsetsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (groupSpecs is null)
        {
            throw new ArgumentNullException(nameof(groupSpecs));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool requireStable = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListConsumerGroupOffsetsOptions));
            requireStable = options.RequireStable;
        }

        List<string> keys = new List<string>(groupSpecs.Count);
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        bool[] allPartitions = new bool[groupSpecs.Count];
        int[] partitionCounts = new int[groupSpecs.Count];
        List<TopicPartition>?[] selections = new List<TopicPartition>?[groupSpecs.Count];
        int next = 0;
        foreach (KeyValuePair<string, ListConsumerGroupOffsetsSpec> entry in groupSpecs)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The group specs must not contain a null group id.", nameof(groupSpecs));
            }

            // A dictionary already guarantees this, but the parameter is an interface a caller
            // may implement; a repeat would make the per-key bridge ambiguous about which
            // spec won, and the core rejects it too.
            if (!seen.Add(entry.Key))
            {
                throw new ArgumentException(
                    $"The group specs must not contain the group id '{entry.Key}' more than once.",
                    nameof(groupSpecs));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The spec for group id '{entry.Key}' must not be null.", nameof(groupSpecs));
            }

            keys.Add(entry.Key);

            IReadOnlyCollection<TopicPartition>? selected = entry.Value.TopicPartitions;
            if (selected is null)
            {
                // "All partitions" — the inner pair at this index is never read.
                allPartitions[next] = true;
                next++;
                continue;
            }

            List<TopicPartition> pairs = new List<TopicPartition>(selected.Count);
            foreach (TopicPartition partition in selected)
            {
                // A `default(TopicPartition)` has a null Topic, which the ABI would read as
                // an absent name rather than a request (ffi §B5).
                if (partition.Topic is null)
                {
                    throw new ArgumentException(
                        $"The spec for group id '{entry.Key}' must not select a topic partition "
                        + "with a null topic.",
                        nameof(groupSpecs));
                }

                pairs.Add(partition);
            }

            selections[next] = pairs;
            partitionCounts[next] = pairs.Count;
            next++;
        }

        KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> operation =
            new KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                "listConsumerGroupOffsets", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        List<GCHandle>? pinnedArrays = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
            pinnedArrays = new List<GCHandle>(keys.Count * 2);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] groupIds = new IntPtr[keys.Count];
            IntPtr[] topics = new IntPtr[keys.Count];
            IntPtr[] partitions = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String groupId = Utf8Marshal.Pin(keys[i]);
                pinned.Add(groupId);
                groupIds[i] = groupId.Pointer;

                List<TopicPartition>? pairs = selections[i];
                if (pairs is null || pairs.Count == 0)
                {
                    // Both the all-partitions case and an empty explicit selection: a NULL
                    // inner pointer the core never dereferences. `allPartitions[i]` alone
                    // tells the two apart.
                    topics[i] = IntPtr.Zero;
                    partitions[i] = IntPtr.Zero;
                    continue;
                }

                IntPtr[] topicPointers = new IntPtr[pairs.Count];
                int[] partitionIds = new int[pairs.Count];
                for (int j = 0; j < pairs.Count; j++)
                {
                    Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(pairs[j].Topic);
                    pinned.Add(topic);
                    topicPointers[j] = topic.Pointer;
                    partitionIds[j] = pairs[j].Partition;
                }

                GCHandle topicPin = GCHandle.Alloc(topicPointers, GCHandleType.Pinned);
                pinnedArrays.Add(topicPin);
                topics[i] = topicPin.AddrOfPinnedObject();

                GCHandle partitionPin = GCHandle.Alloc(partitionIds, GCHandleType.Pinned);
                pinnedArrays.Add(partitionPin);
                partitions[i] = partitionPin.AddrOfPinnedObject();
            }

            // Shape 4a: the ABI fires once per group id in the input — including on the
            // inline whole-call failure, which fires for every key — and the loop above
            // rejects both a null and a repeated group id, so that is keys.Count. The
            // submit's own token is released after, so an empty map releases.
            operation.SetPendingCallbacks(keys.Count);

            submit(
                _handle.DangerousGetHandle(),
                groupIds,
                allPartitions,
                topics,
                partitions,
                partitionCounts,
                keys.Count,
                timeoutMs,
                requireStable,
                AdminCallbacks.ListConsumerGroupOffsets,
                GCHandle.ToIntPtr(gcHandle));

            operation.ReleaseSubmitToken();
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String value in pinned)
                {
                    value.Dispose();
                }
            }

            if (pinnedArrays is not null)
            {
                foreach (GCHandle pin in pinnedArrays)
                {
                    pin.Free();
                }
            }
        }

        return new ListConsumerGroupOffsetsResult(operation.Tasks);
    }

    /// <summary>
    /// Encodes one enum-valued group-listing filter axis as the Java <c>toString()</c> names the
    /// ABI reads, rejecting a value no member defines.
    /// </summary>
    /// <typeparam name="T">The filter's enum type.</typeparam>
    /// <param name="values">The axis, already de-duplicated by the options object.</param>
    /// <param name="name">
    /// <see cref="GroupMarshal"/>'s encoder for <typeparamref name="T"/>, which returns
    /// <see langword="null"/> for an undefined value rather than throwing — it does not know
    /// which property to blame.
    /// </param>
    /// <param name="optionsName">
    /// The options type to name in the error. Passed rather than fixed because two generations of
    /// the same RPC share this helper — <c>listGroups</c> and its deprecated predecessor
    /// <c>listConsumerGroups</c> — and naming the wrong one sends the caller to a property that
    /// does not exist on the type they passed.
    /// </param>
    /// <param name="propertyName">The options property to name in the error.</param>
    private static List<string> FilterNames<T>(
        IReadOnlyCollection<T> values, Func<T, string?> name, string optionsName, string propertyName)
        where T : struct
    {
        List<string> names = new List<string>(values.Count);
        foreach (T value in values)
        {
            string? text = name(value);
            if (text is null)
            {
                throw new ArgumentOutOfRangeException(
                    "options",
                    value,
                    $"{optionsName}.{propertyName} must contain only defined {typeof(T).Name} members.");
            }

            names.Add(text);
        }

        return names;
    }

    /// <summary>
    /// Pins one filter axis's names for the duration of the submit, recording every pin in
    /// <paramref name="pinned"/> so the caller's <c>finally</c> unpins on every path.
    /// </summary>
    /// <param name="names">The axis's names, possibly none.</param>
    /// <param name="pinned">The call's pin ledger, shared by all three axes.</param>
    /// <returns>
    /// The pointer array for this axis. An empty axis yields an empty array, which the ABI
    /// reads together with a count of <c>0</c> as "no filter on this axis".
    /// </returns>
    private static IntPtr[] PinNames(
        IReadOnlyCollection<string> names, List<Utf8Marshal.PinnedUtf8String> pinned)
    {
        if (names.Count == 0)
        {
            return Array.Empty<IntPtr>();
        }

        IntPtr[] pointers = new IntPtr[names.Count];
        int next = 0;
        foreach (string name in names)
        {
            Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(name);
            pinned.Add(pin);
            pointers[next++] = pin.Pointer;
        }

        return pointers;
    }

    /// <summary>
    /// The <c>ListOffsets</c> wire sentinel for one of the six no-argument
    /// <see cref="OffsetSpec"/> kinds — Java's <c>KafkaAdminClient.getOffsetFromSpec</c>
    /// (<c>KafkaAdminClient.java:5176-5191</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Java has no <c>LatestSpec</c> branch</b> — it falls out of the <c>if/else</c>
    /// chain to <c>return LATEST_TIMESTAMP</c> at <c>:5190</c>, which also silently swallows
    /// any unrecognised subclass. <see cref="OffsetSpec"/>'s hierarchy is closed here
    /// precisely so that fall-through cannot be reached by a wrong kind, so
    /// <see cref="OffsetSpec.LatestSpec"/> is matched explicitly and lands on the same
    /// <c>-1</c>. The final throw is therefore unreachable through the public API and
    /// exists so a future eighth kind fails loudly rather than being queried as "latest".
    /// </remarks>
    private static long SentinelFor(OffsetSpec spec, string parameterName) => spec switch
    {
        OffsetSpec.LatestSpec => -1L,
        OffsetSpec.EarliestSpec => -2L,
        OffsetSpec.MaxTimestampSpec => -3L,
        OffsetSpec.EarliestLocalSpec => -4L,
        OffsetSpec.LatestTieredSpec => -5L,
        OffsetSpec.EarliestPendingUploadSpec => -6L,
        _ => throw new ArgumentException(
            $"Unsupported offset spec '{spec.GetType().Name}'.", parameterName),
    };

    /// <summary>
    /// De-duplicates a partition selection, preserving request order, and rejects a null
    /// topic before it can reach the ABI.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose parameter is a <c>Set</c>. The null-topic check is
    /// mandatory: the header skips such an entry silently, which would leave the caller
    /// believing a partition was queried.
    /// </remarks>
    private static List<TopicPartition> DistinctPartitions(
        IReadOnlyCollection<TopicPartition>? partitions, string parameterName)
    {
        List<TopicPartition> selection = new List<TopicPartition>(partitions?.Count ?? 0);
        if (partitions is null)
        {
            return selection;
        }

        HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
        foreach (TopicPartition partition in partitions)
        {
            if (partition.Topic is null)
            {
                throw new ArgumentException(
                    "The partitions must not contain a topic partition with a null topic.", parameterName);
            }

            if (seen.Add(partition))
            {
                selection.Add(partition);
            }
        }

        return selection;
    }

    /// <summary>
    /// De-duplicates the requested config resources, preserving request order, and rejects
    /// a null element before it can reach the ABI.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>. The null check is
    /// mandatory: the header skips an entry with a NULL name silently.
    /// </remarks>
    private static List<ConfigResource> DistinctResources(
        IReadOnlyCollection<ConfigResource> resources, string parameterName)
    {
        List<ConfigResource> keys = new List<ConfigResource>(resources.Count);
        HashSet<ConfigResource> seen = new HashSet<ConfigResource>(s_configResourceComparer);
        foreach (ConfigResource resource in resources)
        {
            if (resource is null)
            {
                throw new ArgumentException("The resources must not contain a null element.", parameterName);
            }

            if (seen.Add(resource))
            {
                keys.Add(resource);
            }
        }

        return keys;
    }

    /// <summary>
    /// De-duplicates the requested config-resource types into the ABI's
    /// <c>ConfigResource.Type.id()</c> array, preserving request order.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>A null or empty input yields an empty array, and that is a valid request</b> —
    /// Java's <c>Set.of()</c>, which the header maps to "every supported type". De-dup
    /// mirrors Java's <c>Set</c> parameter. There is no null-element check because the
    /// element type is an enum, which has no null.
    /// </remarks>
    private static int[] DistinctTypeIds(IReadOnlyCollection<ConfigResourceType>? types)
    {
        if (types is null || types.Count == 0)
        {
            return Array.Empty<int>();
        }

        List<int> ids = new List<int>(types.Count);
        HashSet<ConfigResourceType> seen = new HashSet<ConfigResourceType>();
        foreach (ConfigResourceType type in types)
        {
            if (seen.Add(type))
            {
                ids.Add((int)type);
            }
        }

        return ids.ToArray();
    }

    /// <summary>
    /// Validates and converts Java's <c>close(Duration)</c> timeout, then closes. Shared
    /// by both public clients so the guard and the millisecond conversion exist once.
    /// </summary>
    /// <param name="timeout">
    /// How long to wait for the background task. <see cref="TimeSpan.Zero"/> is valid.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    internal Task Close(TimeSpan timeout)
    {
        // Validate BEFORE the native call (ffi §B5). A negative timeout must not simply
        // be forwarded: the ABI reads a negative timeout_ms as "wait indefinitely", so
        // passing one through would turn a caller mistake into an unbounded wait.
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must not be negative.");
        }

        return Close((long)timeout.TotalMilliseconds);
    }

    /// <summary>
    /// Closes the client, awaiting the background task for up to
    /// <paramref name="timeoutMs"/> (negative = wait indefinitely, Java's no-argument
    /// <c>close()</c>), then releases the handle. Idempotent: a second call is a no-op.
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    internal async Task Close(long timeoutMs)
    {
        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — closing again would double-close.
            return;
        }

        try
        {
            await CloseInternal(timeoutMs).ConfigureAwait(false);
        }
        finally
        {
            // Requests ReleaseHandle → AdminClient_destroy. It runs when the reference
            // count reaches zero, which is immediately when nothing is in flight and
            // deferred to the last in-flight operation's callback otherwise.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// The graceful asynchronous teardown: <c>close_async</c> (which joins the
    /// background task) then the handle release. Unlike <see cref="Close(long)"/> a close
    /// failure is swallowed — <c>DisposeAsync</c> must not throw out of a
    /// <c>using</c> block during unwinding.
    /// </summary>
    internal async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            await CloseInternal(UnsetTimeoutMs).ConfigureAwait(false);
        }
        catch (KafkaException)
        {
            // Teardown: surfacing a close failure from DisposeAsync would replace whatever
            // exception is already unwinding. Close(TimeSpan) is the surface that reports it.
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// The blocking teardown fallback: the <b>synchronous</b> <c>AdminClient_close</c>
    /// with Java's no-argument <c>close()</c> semantics, then the handle release.
    /// </summary>
    /// <remarks>
    /// The sync ABI is called directly — no <c>Task.Run</c>, no
    /// <c>GetAwaiter().GetResult()</c>. The wait happens inside the core's own
    /// multi-thread runtime, so the calling thread simply parks; that is the shipped
    /// sync-op precedent, not the sync-over-async this binding forbids. The handle is
    /// passed as the <see cref="SafeHandle"/> so the marshaller holds a call-scoped
    /// reference for the whole blocking call.
    /// </remarks>
    public void Dispose()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            NativeMethods.AdminClientClose(_handle, UnsetTimeoutMs);
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Bridges <c>close_async</c> to a <see cref="Task"/> via the shared void completion
    /// bridge (admin's one genuinely single-awaiter operation, so it reuses
    /// <see cref="OperationCompletionSource"/> rather than the per-key
    /// <see cref="KeyedAdminOperation{TKey, TValue}"/>).
    /// </summary>
    private Task CloseInternal(long timeoutMs)
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, inside the try for the same reason as in
            // CreateTopics: an AddRef throw must route through AbandonBeforeSubmit.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            NativeMethods.AdminClientCloseAsync(
                _handle.DangerousGetHandle(), timeoutMs, AdminCallbacks.Close, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Wins the one-shot teardown latch, so exactly one of
    /// <see cref="Dispose"/> / <see cref="DisposeAsync"/> / <see cref="Close(long)"/>
    /// performs the close and the handle release.
    /// </summary>
    /// <summary>
    /// Rejects a negative <c>TimeoutMs</c> before any native call (ffi §B5) and maps
    /// <see langword="null"/> onto the ABI's "unset" sentinel.
    /// </summary>
    /// <remarks>
    /// The ABI reads a negative <c>timeout_ms</c> as <em>unset</em>, so a negative would
    /// silently mean "use the client default" rather than the timeout asked for — the
    /// caller would never learn their value was discarded. Shared by every RPC so the
    /// three options types cannot drift apart on the rule or on its message.
    /// </remarks>
    private static int ValidateTimeoutMs(int? timeoutMs, string optionsTypeName)
    {
        if (timeoutMs is < 0)
        {
            throw new ArgumentOutOfRangeException(
                "options",
                timeoutMs,
                string.Format(
                    CultureInfo.InvariantCulture,
                    "{0}.TimeoutMs must not be negative; leave it null to use the client default.",
                    optionsTypeName));
        }

        return timeoutMs ?? UnsetTimeoutMs;
    }

    /// <summary>
    /// De-duplicates a requested string key axis, preserving request order, and rejects a
    /// null element before it can reach the ABI.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>, so a repeated key is one
    /// entry — the same reasoning as <c>CreateTopics</c>. The null-element check is
    /// mandatory rather than defensive: the header requires "<c>count</c> valid C
    /// strings", and the ABI does not validate its own preconditions (ffi §B5). Java's
    /// <c>TopicCollection.ofTopicNames</c> accepts a null element and fails later, so the
    /// check lives at the submit rather than in the collection's factory.
    /// </remarks>
    /// <param name="names">The requested keys, in request order.</param>
    /// <param name="elementNoun">
    /// What the elements are, for the error text. Passed rather than fixed because more
    /// than one RPC family keys on plain strings — topic names and consumer group ids —
    /// and naming the wrong kind points the caller at the wrong argument.
    /// </param>
    /// <param name="parameterName">The caller's parameter to blame.</param>
    private static List<string> DistinctNames(
        IReadOnlyCollection<string> names, string elementNoun, string parameterName)
    {
        List<string> keys = new List<string>(names.Count);
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (string name in names)
        {
            if (name is null)
            {
                throw new ArgumentException(
                    $"The {elementNoun} must not contain a null element.", parameterName);
            }

            if (seen.Add(name))
            {
                keys.Add(name);
            }
        }

        return keys;
    }

    /// <summary>
    /// Java's <c>Utils.isBlank</c> (<c>Utils.java:1569-1571</c>) — <c>str == null ||
    /// str.trim().isEmpty()</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ Not <see cref="string.IsNullOrWhiteSpace"/>: Java's <c>trim</c> strips every char
    /// <c>&lt;= ' '</c>, so it treats a control character as blank where
    /// <c>char.IsWhiteSpace</c> does not.
    /// </remarks>
    /// <param name="value">The value to test.</param>
    /// <returns><c>true</c> when Java would call it blank.</returns>
    private static bool IsBlank(string? value)
    {
        if (value is null)
        {
            return true;
        }

        foreach (char character in value)
        {
            if (character > ' ')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// De-duplicates the requested ACL bindings by <b>value</b>, preserving request order,
    /// and rejects a null element before it can reach the ABI.
    /// </summary>
    /// <remarks>
    /// The de-duplication is by value because Java's result is a
    /// <c>Map&lt;AclBinding, …&gt;</c>, so two equal bindings are one entry — which is also
    /// what stops the per-key bridge seeing a duplicate key. Takes an
    /// <see cref="IEnumerable{T}"/> rather than a collection because Java's signature is
    /// <c>Collection&lt;AclBinding&gt;</c> and the public overload mirrors it.
    /// </remarks>
    /// <param name="acls">The requested bindings, in request order.</param>
    /// <param name="parameterName">The caller's parameter to blame.</param>
    private static List<AclBinding> DistinctBindings(IEnumerable<AclBinding> acls, string parameterName)
    {
        List<AclBinding> keys = new List<AclBinding>();
        HashSet<AclBinding> seen = new HashSet<AclBinding>();
        foreach (AclBinding acl in acls)
        {
            if (acl is null)
            {
                throw new ArgumentException(
                    "The ACL bindings must not contain a null element.", parameterName);
            }

            if (seen.Add(acl))
            {
                keys.Add(acl);
            }
        }

        return keys;
    }

    /// <summary>
    /// The filter twin of <see cref="DistinctBindings"/>, for the same reason: Java's result
    /// is a <c>Map&lt;AclBindingFilter, …&gt;</c>, so two value-equal filters are one entry.
    /// </summary>
    /// <param name="filters">The requested filters, in request order.</param>
    /// <param name="parameterName">The caller's parameter to blame.</param>
    private static List<AclBindingFilter> DistinctFilters(
        IEnumerable<AclBindingFilter> filters, string parameterName)
    {
        List<AclBindingFilter> keys = new List<AclBindingFilter>();
        HashSet<AclBindingFilter> seen = new HashSet<AclBindingFilter>();
        foreach (AclBindingFilter filter in filters)
        {
            if (filter is null)
            {
                throw new ArgumentException(
                    "The ACL filters must not contain a null element.", parameterName);
            }

            if (seen.Add(filter))
            {
                keys.Add(filter);
            }
        }

        return keys;
    }

    /// <inheritdoc cref="DistinctNames"/>
    private static List<Uuid> DistinctIds(IReadOnlyCollection<Uuid> ids)
    {
        List<Uuid> keys = new List<Uuid>(ids.Count);
        HashSet<Uuid> seen = new HashSet<Uuid>();
        foreach (Uuid id in ids)
        {
            // No null check: Uuid is a value type, so there is no null element to reject.
            if (seen.Add(id))
            {
                keys.Add(id);
            }
        }

        return keys;
    }

    /// <summary>
    /// The out-half of the base64 topic-id round trip: the by-id entry points take
    /// <c>const char *const *</c> base64 strings (Java's <c>Uuid.toString()</c> form), not
    /// binary UUIDs, and the header states "result keys are the same base64" — which is
    /// what lets the completion parse them straight back into <see cref="Uuid"/> keys.
    /// </summary>
    private static List<string> ToBase64(List<Uuid> ids)
    {
        List<string> text = new List<string>(ids.Count);
        foreach (Uuid id in ids)
        {
            text.Add(id.ToString());
        }

        return text;
    }

    /// <summary>
    /// The shared submit sequence for a keyed admin RPC: root the operation, take the
    /// span-the-op client reference, pin the keys, call native, unpin.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The order is load-bearing. The <c>GCHandle</c> and the
    /// <see cref="System.Runtime.InteropServices.SafeHandle.DangerousAddRef(ref bool)"/>
    /// are both published <b>before</b> the P/Invoke because the callback can fire
    /// <em>inside</em> it — the header requires everything the callback needs to be
    /// published before the call, not after. The reference is released by the completion
    /// (<c>AdminOperation.FreeGcHandle</c>), which is what makes a <c>Dispose</c> racing
    /// an in-flight operation defer <c>AdminClient_destroy</c> instead of freeing the
    /// client under it.
    /// </para>
    /// <para>
    /// The key strings are pinned only for the call (ffi §A4's call-scoped rule): the ABI
    /// copies them out during the submit, so nothing native holds them afterwards. The
    /// <c>finally</c> unpins on every path, including the inline-callback one — which has
    /// already run to completion by the time the P/Invoke returns.
    /// </para>
    /// </remarks>
    private void Submit(
        AdminOperation operation,
        List<string> keys,
        Action<IntPtr, IntPtr[], int, IntPtr> submit)
    {
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        // Allocated INSIDE the try, for the same reason CreateTopics allocates its handle
        // array there: everything between the GCHandle allocation above and the try is a
        // window in which a throw would root the operation for the process lifetime,
        // because neither the catch nor the finally covers it — so the window is kept to
        // nothing at all. Declaring the local null allocates nothing; the finally
        // null-checks precisely because the allocation itself is now inside the try.
        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] pointers = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(keys[i]);
                pinned.Add(key);
                pointers[i] = key.Pointer;
            }

            submit(_handle.DangerousGetHandle(), pointers, pointers.Length, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String key in pinned)
                {
                    key.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// The exhaustiveness arm for a <see cref="TopicCollection"/> switch. Unreachable by
    /// construction — the outer constructor is private and both subclasses are sealed, so
    /// there is no third inhabitant — but C# cannot see that, so the arm names the
    /// invariant rather than being a bare <c>default</c>.
    /// </summary>
    private static ArgumentException UnreachableCollection(string parameterName) =>
        new ArgumentException(
            "The topic collection must come from TopicCollection.OfTopicNames or TopicCollection.OfTopicIds.",
            parameterName);

    private bool TryBeginClose() => Interlocked.Exchange(ref _closed, 1) == 0;

    /// <summary>The use-after-dispose guard for every RPC.</summary>
    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeAdminClient));
        }
    }
}
