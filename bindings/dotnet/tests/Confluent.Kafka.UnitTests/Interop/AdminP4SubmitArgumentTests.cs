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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What M15/P4 Stage 1's inputs actually become at the P/Invoke, and — the phase's
/// headline — <b>which completion bridge each RPC registers with native</b>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The wiring half of the swap detection lives here.</b>
/// <c>kafka_admin_ElectLeadersResult_t</c> and
/// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> expose byte-identical accessor
/// sets while their Java shapes differ, so the realistic mistake is routing one through the
/// other's walker. The context object a submit publishes as <c>user_data</c> <em>is</em>
/// that routing decision, and it is readable at the seam:
/// <see cref="ElectLeaders_RegistersTheAggregateBridge_AndItsTrampolineAgrees"/> and
/// <see cref="AlterPartitionReassignments_RegistersThePerPartitionBridge_AndItsTrampolineAgrees"/>
/// read it, then drive the <em>production</em> trampoline against it so a body rewired to
/// the other shape cannot pass either.
/// </para>
/// <para>
/// ⚠ <b>The timeout and the two option flags are invisible to a behavioural test.</b> The
/// Rust <c>MockAdminClient</c> ignores the <em>options</em> it is handed —
/// <c>_options: ElectLeadersOptions</c> and <c>_options:
/// AlterPartitionReassignmentsOptions</c> — so their values are read here, at the seam,
/// where they are facts rather than inferences.
/// </para>
/// <para>
/// ⚠ <b>No <c>MarshalAs</c> claim is made here.</b> An injected submit never crosses the
/// P/Invoke, so this file cannot observe marshalling at all; the attributes are pinned
/// structurally by <see cref="AdminNativeMethodsMarshallingTests"/>, and the
/// <c>cancel</c> flag's real crossing is driven end to end by
/// <see cref="AdminP4ResultMarshalTests"/>.
/// </para>
/// </remarks>
public sealed class AdminP4SubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// ⚠⚠ <c>electLeaders</c> publishes the <b>aggregate</b> bridge —
    /// <see cref="SingleAdminOperation{TValue}"/> over a map of nullable errors — and its
    /// production trampoline agrees with it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The context type is the routing decision made visible. Rewiring this RPC to the
    /// per-key shape the identical ABI accessor set invites would publish a
    /// <see cref="VoidKeyedAdminOperation{TKey}"/> here instead.
    /// </para>
    /// <para>
    /// ⚠ <b>The second half — driving the production trampoline — is what catches a
    /// trampoline rewired on its own.</b> The two trampolines cast <c>user_data</c> at
    /// runtime, so pointing <c>OnElectLeaders</c> at the shape-2 walker still compiles: the
    /// cast then throws, the no-throw boundary absorbs it with no context recovered, and
    /// nothing completes the awaiter. That is why the assertion below awaits rather than
    /// inspecting — a mismatched body leaves this task pending forever and the deadline
    /// reports it.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task ElectLeaders_RegistersTheAggregateBridge_AndItsTrampolineAgrees()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        object? context = null;
        IntPtr captured = IntPtr.Zero;
        ElectLeadersResult result = admin.ElectLeaders(
            ElectionType.Preferred,
            new[] { new TopicPartition("p4-routing", 0) },
            options: null,
            (nativeHandle, electionType, allPartitions, topics, partitions, count, timeoutMs, callback,
                userData) =>
            {
                captured = userData;
                context = GCHandle.FromIntPtr(userData).Target;
            });

        Assert.IsType<SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException>>>(context);

        // The production trampoline must be able to complete THAT context.
        AdminCallbacks.ElectLeaders(IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(3, "routing"), captured);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.Partitions), s_deadline);
        Assert.Equal(3, failure.Code);
        Assert.Equal("routing", failure.Message);
    }

    /// <summary>
    /// ⚠⚠ <c>alterPartitionReassignments</c> publishes the <b>per-partition</b> bridge —
    /// <see cref="VoidKeyedAdminOperation{TKey}"/>, one source per requested partition — and
    /// its production trampoline agrees with it.
    /// </summary>
    /// <remarks>
    /// The mirror of
    /// <see cref="ElectLeaders_RegistersTheAggregateBridge_AndItsTrampolineAgrees"/>: this
    /// RPC's <c>get_error(i)</c> really is a per-partition failure, so the per-key bridge is
    /// the right one here even though the accessor set is the same.
    /// </remarks>
    [Fact]
    public async Task AlterPartitionReassignments_RegistersThePerPartitionBridge_AndItsTrampolineAgrees()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        object? context = null;
        IntPtr captured = IntPtr.Zero;
        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-routing", 0)] = new NewPartitionReassignment(new[] { 0 }),
                [new TopicPartition("p4-routing", 1)] = null,
            },
            options: null,
            (nativeHandle, topics, partitions, cancel, targetReplicas, targetReplicaCounts, count, timeoutMs,
                allowReplicationFactorChange, callback, userData) =>
            {
                captured = userData;
                context = GCHandle.FromIntPtr(userData).Target;
            });

        Assert.IsType<VoidKeyedAdminOperation<TopicPartition>>(context);

        // One awaitable per requested partition, keyed by the caller's own keys — the shape
        // the aggregate bridge cannot produce.
        Assert.Equal(2, result.Values.Count);
        Assert.True(result.Values.ContainsKey(new TopicPartition("p4-routing", 0)));
        Assert.True(result.Values.ContainsKey(new TopicPartition("p4-routing", 1)));

        AdminCallbacks.AlterPartitionReassignments(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(4, "routing"), captured);

        foreach (Task task in result.Values.Values)
        {
            KafkaException failure =
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => task), s_deadline);
            Assert.Equal(4, failure.Code);
            Assert.Equal("routing", failure.Message);
        }
    }

    /// <summary>
    /// ⚠ <c>electLeaders</c>' <see langword="null"/> selection is Java's null <c>Set</c> —
    /// "every partition in the cluster" — and an <b>empty</b> collection is not. The two
    /// produce different calls.
    /// </summary>
    /// <remarks>
    /// Java's own javadoc draws the line (<c>Admin.java:1099-1100</c>: "or for all
    /// partitions if the argument to <c>partitions</c> is null"), and the ABI carries it in
    /// a dedicated flag "so 'all partitions' and 'an empty selection' stay
    /// distinguishable". A binding that mapped <c>Count == 0</c> onto the flag would turn a
    /// request for nothing into a cluster-wide election — which no result inspection could
    /// then distinguish from the caller having asked for it.
    /// </remarks>
    [Fact]
    public void ElectLeaders_NullSelectionIsAllPartitions_AndAnEmptyOneIsNot()
    {
        Captured all = CaptureElect(ElectionType.Preferred, partitions: null);
        Assert.True(all.AllPartitions, "a null selection is Java's null Set — every partition");
        Assert.Equal(0, all.Count);

        Captured none = CaptureElect(ElectionType.Preferred, Array.Empty<TopicPartition>());
        Assert.False(none.AllPartitions, "an empty selection asks for an election over no partitions");
        Assert.Equal(0, none.Count);

        Captured some = CaptureElect(
            ElectionType.Preferred, new[] { new TopicPartition("t", 0) });
        Assert.False(some.AllPartitions);
        Assert.Equal(1, some.Count);
    }

    /// <summary>
    /// The election type crosses as Java's <c>ElectionType.value</c> byte, in its own
    /// argument slot.
    /// </summary>
    [Theory]
    [InlineData(ElectionType.Preferred, 0)]
    [InlineData(ElectionType.Unclean, 1)]
    public void ElectLeaders_TheElectionTypeCode_ReachesTheSubmit(ElectionType type, int code) =>
        Assert.Equal(code, CaptureElect(type, Array.Empty<TopicPartition>()).ElectionType);

    /// <summary>
    /// A value cast into <see cref="ElectionType"/> from outside its two members is
    /// rejected <b>before</b> the native call.
    /// </summary>
    /// <remarks>
    /// Java's parameter is the enum itself, so an undefined value is not expressible there
    /// at all; in C# a cast makes it expressible, and ffi §B5 requires the binding to
    /// validate rather than hand the ABI a value it documents as rejected. The ABI would
    /// otherwise fire its completion callback inline with an <c>IllegalArgument</c> error —
    /// a faulted task where .NET callers expect an argument exception.
    /// </remarks>
    [Theory]
    [InlineData(-1)]
    [InlineData(2)]
    [InlineData(int.MaxValue)]
    public void ElectLeaders_RejectsAnUndefinedElectionType(int value)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ArgumentOutOfRangeException rejected = Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.ElectLeaders(
                (ElectionType)value,
                Array.Empty<TopicPartition>(),
                options: null,
                (nativeHandle, electionType, allPartitions, topics, partitions, count, timeoutMs, callback,
                    userData) => submitted = true));

        Assert.Equal("electionType", rejected.ParamName);
        Assert.False(submitted, "the value must be rejected before the native call");
    }

    /// <summary>
    /// The selection's two parallel arrays carry the caller's partitions, de-duplicated as
    /// Java's <c>Set</c> parameter is.
    /// </summary>
    [Fact]
    public void ElectLeaders_TheSelectionArrays_ReachTheSubmit_Deduplicated()
    {
        Captured captured = CaptureElect(
            ElectionType.Unclean,
            new[]
            {
                new TopicPartition("p4-elect-a", 7),
                new TopicPartition("p4-elect-b", 0),
                new TopicPartition("p4-elect-a", 7),
            });

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "p4-elect-a", "p4-elect-b" }, captured.Topics);
        Assert.Equal(new[] { 7, 0 }, captured.Partitions);
    }

    /// <summary>
    /// A topic partition with a <see langword="null"/> topic is rejected before the native
    /// call — the ABI would <em>silently skip</em> that entry, leaving the caller believing
    /// an election was attempted for it.
    /// </summary>
    [Fact]
    public void ElectLeaders_RejectsANullTopic_BeforeTheNativeCall()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ArgumentException rejected = Assert.Throws<ArgumentException>(() =>
            admin.ElectLeaders(
                ElectionType.Preferred,
                new[] { default(TopicPartition) },
                options: null,
                (nativeHandle, electionType, allPartitions, topics, partitions, count, timeoutMs, callback,
                    userData) => submitted = true));

        Assert.Equal("partitions", rejected.ParamName);
        Assert.False(submitted, "a null topic must be rejected before the native call");
    }

    /// <summary>
    /// ⚠⚠ <b>Cancelling and reassigning produce different calls, on different wires.</b> A
    /// <see langword="null"/> entry sets <c>cancel[i]</c> and contributes <b>no</b> replica
    /// array; a present one clears the flag and contributes its own ids.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the null-versus-empty discipline the ABI header asks for verbatim: "A
    /// separate flag rather than a NULL replica pointer, so cancelling stays distinct from
    /// 'present but empty', which Java rejects." A <c>?? Array.Empty&lt;int&gt;()</c>
    /// anywhere on this path would clear the flag and send an empty list, which the ABI
    /// rejects outright — so a cancellation would become a failed call.
    /// </para>
    /// <para>
    /// The third arm of the distinction, a present-but-empty list, is not expressible at
    /// all: <see cref="NewPartitionReassignment"/>'s constructor rejects it, as Java's does.
    /// That is asserted in
    /// <c>PublicAdminP4ShapeParityTests.NewPartitionReassignment_MirrorsJavasShape_AndRejectsAnEmptyList</c>.
    /// </para>
    /// </remarks>
    [Fact]
    public void AlterPartitionReassignments_CancelAndReassign_ProduceDifferentCalls()
    {
        Captured captured = CaptureReassign(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-cancel", 0)] = null,
                [new TopicPartition("p4-cancel", 1)] = new NewPartitionReassignment(new[] { 5, 6, 7 }),
            },
            options: null);

        Assert.Equal(2, captured.Count);

        int cancelled = Array.IndexOf(captured.Partitions!, 0);
        int reassigned = Array.IndexOf(captured.Partitions!, 1);
        Assert.InRange(cancelled, 0, 1);
        Assert.InRange(reassigned, 0, 1);

        // The cancelled entry: the flag is set, and NOTHING is offered as replicas.
        Assert.True(captured.Cancel![cancelled]);
        Assert.Equal(IntPtr.Zero, captured.TargetReplicas![cancelled]);
        Assert.Equal(0, captured.TargetReplicaCounts![cancelled]);

        // The reassigned entry: the flag is clear, and its ids are offered.
        Assert.False(captured.Cancel[reassigned]);
        Assert.NotEqual(IntPtr.Zero, captured.TargetReplicas[reassigned]);
        Assert.Equal(3, captured.TargetReplicaCounts![reassigned]);
        Assert.Equal(new[] { 5, 6, 7 }, captured.ReplicaIds![reassigned]);
    }

    /// <summary>
    /// <see cref="AlterPartitionReassignmentsOptions.AllowReplicationFactorChange"/> reaches
    /// the submit in its own slot, both ways, and <see langword="null"/> options send Java's
    /// <see langword="true"/> default.
    /// </summary>
    [Fact]
    public void AlterPartitionReassignments_AllowReplicationFactorChange_ReachesTheSubmit()
    {
        Dictionary<TopicPartition, NewPartitionReassignment?> request =
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-rf", 0)] = new NewPartitionReassignment(new[] { 0 }),
            };

        Assert.True(CaptureReassign(request, options: null).AllowReplicationFactorChange);
        Assert.True(
            CaptureReassign(request, new AlterPartitionReassignmentsOptions()).AllowReplicationFactorChange);
        Assert.False(
            CaptureReassign(
                    request, new AlterPartitionReassignmentsOptions { AllowReplicationFactorChange = false })
                .AllowReplicationFactorChange);
    }

    /// <summary>
    /// A reassignment keyed by a topic partition with a <see langword="null"/> topic is
    /// rejected before the native call — the ABI would silently skip it.
    /// </summary>
    [Fact]
    public void AlterPartitionReassignments_RejectsANullTopic_BeforeTheNativeCall()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ArgumentException rejected = Assert.Throws<ArgumentException>(() =>
            admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?> { [default] = null },
                options: null,
                (nativeHandle, topics, partitions, cancel, targetReplicas, targetReplicaCounts, count,
                    timeoutMs, allowReplicationFactorChange, callback, userData) => submitted = true));

        Assert.Equal("reassignments", rejected.ParamName);
        Assert.False(submitted, "a null topic must be rejected before the native call");
    }

    /// <summary>A null request map is rejected, as every other admin RPC rejects one.</summary>
    [Fact]
    public void AlterPartitionReassignments_RejectsANullMap()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() =>
            admin.AlterPartitionReassignments(null!, options: null));
        Assert.Equal("reassignments", rejected.ParamName);
    }

    /// <summary>
    /// A <see langword="null"/> timeout must become a <b>negative</b> <c>timeout_ms</c>,
    /// which the ABI reads as "unset, use the client default" — <b>not</b> <c>0</c>, which
    /// would mean "time out immediately". An explicit timeout is forwarded verbatim, and
    /// <c>0</c> stays <c>0</c>.
    /// </summary>
    [Fact]
    public void Timeouts_MapNullToANegative_AndForwardTheRestVerbatim()
    {
        Assert.True(CaptureElect(ElectionType.Preferred, null, options: null).TimeoutMs < 0);
        Assert.True(CaptureElect(ElectionType.Preferred, null, new ElectLeadersOptions()).TimeoutMs < 0);
        Assert.Equal(0, CaptureElect(ElectionType.Preferred, null, new ElectLeadersOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            12_345,
            CaptureElect(ElectionType.Preferred, null, new ElectLeadersOptions { TimeoutMs = 12_345 }).TimeoutMs);

        Dictionary<TopicPartition, NewPartitionReassignment?> request =
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-timeout", 0)] = null,
            };

        Assert.True(CaptureReassign(request, options: null).TimeoutMs < 0);
        Assert.True(CaptureReassign(request, new AlterPartitionReassignmentsOptions()).TimeoutMs < 0);
        Assert.Equal(0, CaptureReassign(request, new AlterPartitionReassignmentsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            23_456,
            CaptureReassign(request, new AlterPartitionReassignmentsOptions { TimeoutMs = 23_456 }).TimeoutMs);
    }

    /// <summary>A negative timeout is rejected rather than silently meaning "unset".</summary>
    [Fact]
    public void NegativeTimeout_IsRejected()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.ElectLeaders(ElectionType.Preferred, null, new ElectLeadersOptions { TimeoutMs = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>(),
                new AlterPartitionReassignmentsOptions { TimeoutMs = -1 }));
    }

    private static Captured CaptureElect(
        ElectionType type, IReadOnlyCollection<TopicPartition>? partitions, ElectLeadersOptions? options = null)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        IntPtr userDataToRelease = IntPtr.Zero;
        admin.ElectLeaders(
            type,
            partitions,
            options,
            (nativeHandle, electionType, allPartitions, topics, partitionIds, count, timeoutMs, callback,
                userData) =>
            {
                captured.ElectionType = electionType;
                captured.AllPartitions = allPartitions;
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;

                // The pinned UTF-8 topics are alive for the duration of this call, exactly
                // as they are for the real P/Invoke (ffi §A4's call-scoped rule).
                string[] names = new string[count];
                int[] ids = new int[count];
                for (int i = 0; i < count; i++)
                {
                    names[i] = Utf8Marshal.PtrToString(topics[i])!;
                    ids[i] = partitionIds[i];
                }

                captured.Topics = names;
                captured.Partitions = ids;
                userDataToRelease = userData;
            });

        // Release the operation rather than leaking its GCHandle and the client reference.
        AdminCallbacks.ElectLeaders(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(1, "captured"), userDataToRelease);
        return captured;
    }

    private static Captured CaptureReassign(
        IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
        AlterPartitionReassignmentsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        IntPtr userDataToRelease = IntPtr.Zero;
        admin.AlterPartitionReassignments(
            reassignments,
            options,
            (nativeHandle, topics, partitionIds, cancel, targetReplicas, targetReplicaCounts, count, timeoutMs,
                allowReplicationFactorChange, callback, userData) =>
            {
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;
                captured.AllowReplicationFactorChange = allowReplicationFactorChange;
                captured.Cancel = (bool[])cancel.Clone();
                captured.TargetReplicas = (IntPtr[])targetReplicas.Clone();
                captured.TargetReplicaCounts = (int[])targetReplicaCounts.Clone();

                string[] names = new string[count];
                int[] ids = new int[count];
                int[]?[] replicaIds = new int[count][];
                for (int i = 0; i < count; i++)
                {
                    names[i] = Utf8Marshal.PtrToString(topics[i])!;
                    ids[i] = partitionIds[i];

                    // The int[] replica arrays are pinned for the duration of this call, so
                    // reading them back through the pointer is what the core would do.
                    if (targetReplicas[i] != IntPtr.Zero)
                    {
                        int[] read = new int[targetReplicaCounts[i]];
                        Marshal.Copy(targetReplicas[i], read, 0, read.Length);
                        replicaIds[i] = read;
                    }
                }

                captured.Topics = names;
                captured.Partitions = ids;
                captured.ReplicaIds = replicaIds;
                userDataToRelease = userData;
            });

        AdminCallbacks.AlterPartitionReassignments(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(1, "captured"), userDataToRelease);
        return captured;
    }

    private sealed class Captured
    {
        internal int ElectionType { get; set; }

        internal bool AllPartitions { get; set; }

        internal int Count { get; set; }

        internal int TimeoutMs { get; set; }

        internal bool AllowReplicationFactorChange { get; set; }

        internal string[]? Topics { get; set; }

        internal int[]? Partitions { get; set; }

        internal bool[]? Cancel { get; set; }

        internal IntPtr[]? TargetReplicas { get; set; }

        internal int[]? TargetReplicaCounts { get; set; }

        internal int[]?[]? ReplicaIds { get; set; }
    }
}
