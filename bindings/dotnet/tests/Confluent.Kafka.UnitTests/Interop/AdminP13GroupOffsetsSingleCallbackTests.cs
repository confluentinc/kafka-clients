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
/// M15/P13.1 — <c>alterConsumerGroupOffsets</c> and <c>deleteConsumerGroupOffsets</c> (CP1)
/// and <c>removeMembersFromConsumerGroup</c> (CP2) on PR #201's single-callback ABI:
/// <b>one</b> callback per call, carrying the result root or the whole-request error, never
/// both, and firing "also when <c>count</c> is 0" (for <c>removeMembersFromConsumerGroup</c>,
/// "in <c>removeAll</c> mode too").
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The lifetime these tests pin (PLAN R1, R2).</b> The operation's <c>GCHandle</c> and
/// its span-the-op client reference are released in <b>one</b> place — the callback's
/// <c>finally</c>. There is no countdown and no submit token, so nothing on the submit side
/// may release either: a leftover submit-side release frees them <em>before</em> the callback
/// runs, which is a use-after-free when the callback then recovers <c>user_data</c>.
/// <see cref="System.Runtime.InteropServices.SafeHandle.IsClosed"/> after
/// <see cref="IDisposable.Dispose"/> is the direct read of "was the client reference
/// released", and it is checked on <b>both</b> sides: still open after the client's own
/// <c>Dispose</c> while the callback is outstanding (an early release would close it), and
/// closed once the callback ran (a missing release would not).
/// </para>
/// <para>
/// ⚠ <b>The inline path is real native.</b> The header documents the callback running
/// "<b>synchronously on the calling thread, before this function returns</b>" when the RPC
/// cannot be submitted. The inline tests below reach it through the production submit seam
/// with the <b>real</b> P/Invoke, so the callback that fires is the core's, not a stand-in's.
/// The asserted messages are the core's own text (<c>src/ffi/admin.rs</c>); the header names
/// the triggers but not their messages, and their codes are not documented, so only the
/// message is asserted.
/// </para>
/// <para>
/// ⚠ <b>A real root through the async trampoline (PLAN D7).</b> The async path never hands
/// the trampoline a populated root in-process — the mock fails the whole future, which
/// arrives as the callback's <c>error</c>. The <b>sync</b> entry points do produce a real root
/// against the mock, one whose every requested partition reports the whole-request error
/// (header: "so is a failure of the whole request, which every requested partition then
/// reports"). Feeding that root to the rooted async trampoline exercises the per-key walk,
/// the <b>borrowed</b> <c>_get_error</c>, the <b>owned</b> <c>_all</c> and the single destroy
/// on native memory — a borrowed error read as owned would double-free at the destroy and
/// take the test host down, which is the loud half of the ownership contract.
/// </para>
/// <para>
/// ⚠ <c>removeMembersFromConsumerGroup</c>'s sync entry point produces a root only in
/// member-list mode. In <c>removeAll</c> mode it returns the error instead ("A non-null return
/// means the request could not be submitted at all — or that <c>remove_all</c> was true and
/// <c>all()</c> failed"), so no in-process root carries a <c>removeAll</c> result: its
/// <c>_all</c> read is pinned by <c>AdminP4ReaderWiringTests</c>, not driven on native memory.
/// </para>
/// </remarks>
public sealed class AdminP13GroupOffsetsSingleCallbackTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code the mock reports for both refused RPCs.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary><c>deleteConsumerGroupOffsets</c>' mock refusal.</summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary><c>alterConsumerGroupOffsets</c>' mock refusal, with Java's own typo.</summary>
    private const string NotImplementedTypo = "Not implement yet";

    /// <summary>The core's message for a NULL admin handle (<c>src/ffi/admin.rs:782</c>).</summary>
    private const string NullAdmin = "admin handle must not be null";

    /// <summary>
    /// The core's message for a negative offset at entry 0 — Java's <c>OffsetAndMetadata</c>
    /// constructor text behind an index prefix (<c>src/ffi/admin.rs:11062</c>, asserted by the
    /// core's own test at <c>:23604</c>).
    /// </summary>
    private const string NegativeOffsetAtZero = "offset at index 0: Invalid negative offset";

    /// <summary>
    /// The core's message for an empty member list without <c>remove_all</c> — Java's
    /// <c>IllegalArgumentException("Invalid empty members has been provided")</c>, as the
    /// header's <c>kafka_admin_AdminClient_remove_members_from_consumer_group</c> documents it.
    /// </summary>
    private const string EmptyMembers = "Invalid empty members has been provided";

    private const string Group = "p13-group";

    // ------------------------------------------------------------------------------------
    // Inline submit failure — the callback runs inside the P/Invoke (PLAN §5 row 5).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// A NULL admin handle: the real <c>alter_consumer_group_offsets_async</c> fires the
    /// callback before it returns, so the result is already faulted — no <c>await</c> — with
    /// the core's message, from both accessors, and the client reference is released exactly
    /// once.
    /// </summary>
    [Fact]
    public void AlterConsumerGroupOffsets_ANullAdmin_FaultsInlineWithTheCoresMessage()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        TopicPartition partition = new TopicPartition("p13-inline-acgo", 0);

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            Group,
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(10) },
            options: null,
            (nativeHandle, groupId, topics, partitions, offsets, metadata, leaderEpochs,
             hasLeaderEpoch, count, timeoutMs, callback, userData) =>
                NativeMethods.AdminClientAlterConsumerGroupOffsetsAsync(
                    IntPtr.Zero, groupId, topics, partitions, offsets, metadata, leaderEpochs,
                    hasLeaderEpoch, count, timeoutMs, callback, userData));

        AssertFaultedInline(result.All(), NullAdmin, result.PartitionResult(partition));
        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// A negative offset — the managed <see cref="OffsetAndMetadata"/> rejects one, so the
    /// seam writes it into the array the core reads, then forwards with the <b>real</b> handle.
    /// The core refuses the entry on the calling thread (a trigger the header lists for this
    /// RPC only).
    /// </summary>
    [Fact]
    public void AlterConsumerGroupOffsets_ANegativeOffset_FaultsInlineWithTheCoresMessage()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        TopicPartition partition = new TopicPartition("p13-inline-acgo", 1);

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            Group,
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(10) },
            options: null,
            (nativeHandle, groupId, topics, partitions, offsets, metadata, leaderEpochs,
             hasLeaderEpoch, count, timeoutMs, callback, userData) =>
            {
                Assert.Equal(1, count);
                offsets[0] = -1;
                NativeMethods.AdminClientAlterConsumerGroupOffsetsAsync(
                    nativeHandle, groupId, topics, partitions, offsets, metadata, leaderEpochs,
                    hasLeaderEpoch, count, timeoutMs, callback, userData);
            });

        AssertFaultedInline(result.All(), NegativeOffsetAtZero, result.PartitionResult(partition));
        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// A NULL admin handle for <c>delete_consumer_group_offsets_async</c>: the same inline
    /// fault, through the same single release.
    /// </summary>
    [Fact]
    public void DeleteConsumerGroupOffsets_ANullAdmin_FaultsInlineWithTheCoresMessage()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        TopicPartition partition = new TopicPartition("p13-inline-dcgo", 0);

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            Group,
            new[] { partition },
            options: null,
            (nativeHandle, groupId, topics, partitions, count, timeoutMs, callback, userData) =>
                NativeMethods.AdminClientDeleteConsumerGroupOffsetsAsync(
                    IntPtr.Zero, groupId, topics, partitions, count, timeoutMs, callback, userData));

        AssertFaultedInline(result.All(), NullAdmin, result.PartitionResult(partition));
        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// A NULL admin handle for <c>remove_members_from_consumer_group_async</c>, in both modes:
    /// the same inline fault, through the same single release. In <c>removeAll</c> mode only
    /// <c>All()</c> is reachable — <c>MemberResult</c> is rejected synchronously there, before
    /// the task is consulted.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RemoveMembers_ANullAdmin_FaultsInlineWithTheCoresMessage(bool removeAll)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        MemberToRemove member = new MemberToRemove("p13-inline-rm");

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            Group,
            RemoveMembersOptions(removeAll, member),
            (nativeHandle, groupId, removeAllFlag, groupInstanceIds, memberCount, reason, timeoutMs,
             callback, userData) =>
                NativeMethods.AdminClientRemoveMembersFromConsumerGroupAsync(
                    IntPtr.Zero, groupId, removeAllFlag, groupInstanceIds, memberCount, reason,
                    timeoutMs, callback, userData));

        Assert.Equal(removeAll, result.RemoveAll);
        if (removeAll)
        {
            AssertFaultedInline(result.All(), NullAdmin);
        }
        else
        {
            AssertFaultedInline(result.All(), NullAdmin, result.MemberResult(member));
        }

        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// An empty member list without <c>remove_all</c> — the managed
    /// <see cref="RemoveMembersFromConsumerGroupOptions"/> rejects one, so the seam forwards a
    /// zero <c>member_count</c> with the <b>real</b> handle. The core refuses it on the calling
    /// thread (a trigger the header lists for this RPC only).
    /// </summary>
    [Fact]
    public void RemoveMembers_AnEmptyMemberListWithoutRemoveAll_FaultsInlineWithTheCoresMessage()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        MemberToRemove member = new MemberToRemove("p13-inline-rm-empty");

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            Group,
            RemoveMembersOptions(removeAll: false, member),
            (nativeHandle, groupId, removeAllFlag, groupInstanceIds, memberCount, reason, timeoutMs,
             callback, userData) =>
            {
                Assert.False(removeAllFlag);
                Assert.Equal(1, memberCount);
                NativeMethods.AdminClientRemoveMembersFromConsumerGroupAsync(
                    nativeHandle, groupId, removeAllFlag, groupInstanceIds, 0, reason, timeoutMs,
                    callback, userData);
            });

        AssertFaultedInline(result.All(), EmptyMembers, result.MemberResult(member));
        AssertReleasedExactlyOnce(admin, handle);
    }

    // ------------------------------------------------------------------------------------
    // Lifetime — nothing but the one callback releases the operation (PLAN §5 row 6).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ With a seam that does not fire, the operation stays <b>pending</b> and
    /// <b>rooted</b> after the submit returns — past the client's own <c>Dispose</c> — and the
    /// one callback, fired later through the production trampoline, is what releases it.
    /// </summary>
    /// <remarks>
    /// The empty row is the discriminating one for PLAN R1: a leftover submit-side
    /// <c>SetPendingCallbacks(0)</c> + <c>ReleaseSubmitToken()</c> reaches zero at the submit
    /// boundary, so <c>IsClosed</c> reads <see langword="true"/> after <c>Dispose</c> below and
    /// the <c>user_data</c> the callback later recovers is already freed. The non-empty row
    /// pins that a request with keys takes the same one-callback path.
    /// </remarks>
    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task AlterConsumerGroupOffsets_UntilTheCallbackFires_TheOperationStaysPendingAndRooted(
        int partitionCount)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>();
        for (int i = 0; i < partitionCount; i++)
        {
            offsets[new TopicPartition("p13-pending-acgo", i)] = new OffsetAndMetadata(i);
        }

        AdminCallbacks.AlterConsumerGroupOffsetsCallback? capturedCallback = null;
        IntPtr capturedUserData = IntPtr.Zero;
        int capturedCount = -1;

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            Group,
            offsets,
            options: null,
            (nativeHandle, groupId, topics, partitions, offsetValues, metadata, leaderEpochs,
             hasLeaderEpoch, count, timeoutMs, callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
                capturedCount = count;
            });

        Assert.Equal(partitionCount, capturedCount);
        Assert.Same(AdminCallbacks.AlterConsumerGroupOffsets, capturedCallback);

        Task all = result.All();
        AssertPendingAndRooted<TopicPartition>(admin, handle, all, capturedUserData);

        capturedCallback!(IntPtr.Zero, MintError(7, "p13-late-acgo"), capturedUserData);

        Assert.True(handle.IsClosed, "the one callback's finally must be the release");
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => all, s_deadline));
        Assert.Equal(7, failure.Code);
        Assert.Equal("p13-late-acgo", failure.Message);
    }

    /// <summary>
    /// The same lifetime for <c>deleteConsumerGroupOffsets</c>.
    /// </summary>
    /// <remarks>
    /// See <see cref="AlterConsumerGroupOffsets_UntilTheCallbackFires_TheOperationStaysPendingAndRooted"/>
    /// for why the empty row discriminates.
    /// </remarks>
    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task DeleteConsumerGroupOffsets_UntilTheCallbackFires_TheOperationStaysPendingAndRooted(
        int partitionCount)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition[] partitions = new TopicPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            partitions[i] = new TopicPartition("p13-pending-dcgo", i);
        }

        AdminCallbacks.DeleteConsumerGroupOffsetsCallback? capturedCallback = null;
        IntPtr capturedUserData = IntPtr.Zero;
        int capturedCount = -1;

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            Group,
            partitions,
            options: null,
            (nativeHandle, groupId, topics, partitionIds, count, timeoutMs, callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
                capturedCount = count;
            });

        Assert.Equal(partitionCount, capturedCount);
        Assert.Same(AdminCallbacks.DeleteConsumerGroupOffsets, capturedCallback);

        Task all = result.All();
        AssertPendingAndRooted<TopicPartition>(admin, handle, all, capturedUserData);

        capturedCallback!(IntPtr.Zero, MintError(11, "p13-late-dcgo"), capturedUserData);

        Assert.True(handle.IsClosed, "the one callback's finally must be the release");
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => all, s_deadline));
        Assert.Equal(11, failure.Code);
        Assert.Equal("p13-late-dcgo", failure.Message);
    }

    /// <summary>
    /// The same lifetime for <c>removeMembersFromConsumerGroup</c>, in both modes: ONE
    /// callback is owed whatever the member count, and nothing on the submit side may resolve
    /// or release the operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The removeAll row also pins the submit shape: no member array at all and a zero count,
    /// with the <c>remove_all</c> flag carrying the mode. The member-list row pins that
    /// <c>N &gt; 1</c> members still take the one-callback path — the pre-CP2 countdown was
    /// armed for one callback per member there.
    /// </para>
    /// <para>
    /// ⚠ Unlike the empty group-offsets row, <b>neither</b> row here is reachable by a
    /// replay of the pre-CP2 <c>SetPendingCallbacks(pendingCallbacks)</c> +
    /// <c>ReleaseSubmitToken()</c> lines: <c>pendingCallbacks</c> was 1 in removeAll mode and
    /// the member count otherwise, and the options reject an empty member set, so that
    /// countdown is armed at two or more and one submit release never reaches zero. What
    /// these rows discriminate is any submit-side release that <em>does</em> reach zero —
    /// <c>SetPendingCallbacks(0)</c> + <c>ReleaseSubmitToken()</c>, or a direct
    /// <c>FreeGcHandle</c>.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData(true, 0)]
    [InlineData(false, 2)]
    public async Task RemoveMembers_UntilTheCallbackFires_TheOperationStaysPendingAndRooted(
        bool removeAll, int memberCount)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        MemberToRemove[] members = new MemberToRemove[memberCount];
        for (int i = 0; i < memberCount; i++)
        {
            members[i] = new MemberToRemove("p13-pending-rm-" + i);
        }

        AdminCallbacks.RemoveMembersFromConsumerGroupCallback? capturedCallback = null;
        IntPtr capturedUserData = IntPtr.Zero;
        int capturedCount = -1;
        bool? capturedRemoveAll = null;
        bool capturedNullArray = false;

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            Group,
            RemoveMembersOptions(removeAll, members),
            (nativeHandle, groupId, removeAllFlag, groupInstanceIds, count, reason, timeoutMs,
             callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
                capturedCount = count;
                capturedRemoveAll = removeAllFlag;
                capturedNullArray = groupInstanceIds is null;
            });

        Assert.Equal(memberCount, capturedCount);
        Assert.Equal(removeAll, capturedRemoveAll);
        Assert.Equal(removeAll, capturedNullArray);
        Assert.Same(AdminCallbacks.RemoveMembersFromConsumerGroup, capturedCallback);

        Task all = result.All();
        AssertPendingAndRooted<string>(admin, handle, all, capturedUserData);

        capturedCallback!(IntPtr.Zero, MintError(13, "p13-late-rm"), capturedUserData);

        Assert.True(handle.IsClosed, "the one callback's finally must be the release");
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => all, s_deadline));
        Assert.Equal(13, failure.Code);
        Assert.Equal("p13-late-rm", failure.Message);
    }

    // ------------------------------------------------------------------------------------
    // A real result root through the async trampoline (PLAN §5 row 12, D7).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ The rooted <c>alterConsumerGroupOffsets</c> trampoline, fed a <b>real</b> root from
    /// the sync entry point, resolves the operation with every requested partition keyed by
    /// its reconstructed <c>(topic, partition)</c> and carrying the whole-request error, plus
    /// the <c>_all</c> outcome carrying the same — and the one callback releases the client.
    /// </summary>
    [Fact]
    public void AlterTrampoline_OnARealSyncRoot_ReadsEveryPartitionAndTheOwnedAll()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition first = new TopicPartition("p13-d7-acgo", 0);
        TopicPartition second = new TopicPartition("p13-d7-acgo", 3);

        IntPtr root;
        using (Utf8Marshal.PinnedUtf8String groupId = Utf8Marshal.Pin(Group))
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(first.Topic))
        {
            KafkaException? submitError = KafkaException.FromHandle(
                SyncNativeMethods.AlterConsumerGroupOffsets(
                    handle,
                    groupId.Pointer,
                    new[] { topic.Pointer, topic.Pointer },
                    new[] { first.Partition, second.Partition },
                    new long[] { 5, 6 },
                    metadata: null,
                    leaderEpochs: null,
                    hasLeaderEpoch: null,
                    count: 2,
                    timeoutMs: -1,
                    out root));
            Assert.Null(submitError);
        }

        Assert.NotEqual(IntPtr.Zero, root);

        SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation =
            RootedOperation<TopicPartition>("alterConsumerGroupOffsets", handle, out IntPtr userData);

        // ⚠ Ownership of `root` transfers to the trampoline: it destroys it after the reads,
        // so nothing below may touch `root` again.
        AdminCallbacks.AlterConsumerGroupOffsets(root, IntPtr.Zero, userData);

        AssertWholeErrorOnEveryKey(
            operation, new[] { first, second }, NotImplementedTypo);
        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// The same for <c>deleteConsumerGroupOffsets</c>, whose sync entry point takes only the
    /// partition arrays.
    /// </summary>
    [Fact]
    public void DeleteTrampoline_OnARealSyncRoot_ReadsEveryPartitionAndTheOwnedAll()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition first = new TopicPartition("p13-d7-dcgo", 1);
        TopicPartition second = new TopicPartition("p13-d7-dcgo", 4);

        IntPtr root;
        using (Utf8Marshal.PinnedUtf8String groupId = Utf8Marshal.Pin(Group))
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(first.Topic))
        {
            KafkaException? submitError = KafkaException.FromHandle(
                SyncNativeMethods.DeleteConsumerGroupOffsets(
                    handle,
                    groupId.Pointer,
                    new[] { topic.Pointer, topic.Pointer },
                    new[] { first.Partition, second.Partition },
                    count: 2,
                    timeoutMs: -1,
                    out root));
            Assert.Null(submitError);
        }

        Assert.NotEqual(IntPtr.Zero, root);

        SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation =
            RootedOperation<TopicPartition>("deleteConsumerGroupOffsets", handle, out IntPtr userData);

        // ⚠ Ownership of `root` transfers to the trampoline — see the alter twin.
        AdminCallbacks.DeleteConsumerGroupOffsets(root, IntPtr.Zero, userData);

        AssertWholeErrorOnEveryKey(
            operation, new[] { first, second }, NotImplemented);
        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// ⚠⚠ The rooted <c>removeMembersFromConsumerGroup</c> trampoline, fed a <b>real</b>
    /// member-list-mode root from the sync entry point, resolves the operation with every
    /// requested member keyed by its <c>group.instance.id</c> and carrying the whole-request
    /// error (header: "so is a failure of the whole request, which every requested member then
    /// reports"), plus the <c>_all</c> outcome carrying the same — and the one callback
    /// releases the client.
    /// </summary>
    [Fact]
    public void RemoveMembersTrampoline_OnARealSyncRoot_ReadsEveryMemberAndTheOwnedAll()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        string firstMember = "p13-d7-rm-b";
        string secondMember = "p13-d7-rm-a";

        IntPtr root = IntPtr.Zero;
        using (Utf8Marshal.PinnedUtf8String groupId = Utf8Marshal.Pin(Group))
        using (Utf8Marshal.PinnedUtf8String firstPin = Utf8Marshal.Pin(firstMember))
        using (Utf8Marshal.PinnedUtf8String secondPin = Utf8Marshal.Pin(secondMember))
        {
            KafkaException? submitError = KafkaException.FromHandle(
                SyncNativeMethods.RemoveMembersFromConsumerGroup(
                    handle,
                    groupId.Pointer,
                    removeAll: false,
                    new[] { firstPin.Pointer, secondPin.Pointer },
                    memberCount: 2,
                    reason: IntPtr.Zero,
                    timeoutMs: -1,
                    ref root));
            Assert.Null(submitError);
        }

        Assert.NotEqual(IntPtr.Zero, root);

        SingleAdminOperation<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> operation =
            RootedOperation<string>("removeMembersFromConsumerGroup", handle, out IntPtr userData);

        // ⚠ Ownership of `root` transfers to the trampoline — see the alter twin.
        AdminCallbacks.RemoveMembersFromConsumerGroup(root, IntPtr.Zero, userData);

        AssertWholeErrorOnEveryKey(operation, new[] { firstMember, secondMember }, NotImplemented);
        AssertReleasedExactlyOnce(admin, handle);
    }

    /// <summary>
    /// ⚠ The removeAll half of D7 has <b>no</b> root to feed: the sync entry point returns the
    /// failed <c>all()</c> directly (header: "A non-null return means … or that
    /// <c>remove_all</c> was true and <c>all()</c> failed") and writes nothing to
    /// <c>out_result</c>. Pinned here so the reachability claim in this class's remarks rests
    /// on an observation, not on a reading of the header alone.
    /// </summary>
    [Fact]
    public void RemoveMembersSync_RemoveAllMode_ReturnsTheErrorAndWritesNoRoot()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr root = IntPtr.Zero;
        KafkaException? error;
        using (Utf8Marshal.PinnedUtf8String groupId = Utf8Marshal.Pin(Group))
        {
            error = KafkaException.FromHandle(
                SyncNativeMethods.RemoveMembersFromConsumerGroup(
                    handle,
                    groupId.Pointer,
                    removeAll: true,
                    groupInstanceIds: null,
                    memberCount: 0,
                    reason: IntPtr.Zero,
                    timeoutMs: -1,
                    ref root));
        }

        Assert.NotNull(error);
        Assert.Equal(UnsupportedVersionCode, error!.Code);
        Assert.Equal(NotImplemented, error.Message);
        Assert.Equal(IntPtr.Zero, root);

        AssertReleasedExactlyOnce(admin, handle);
    }

    // ------------------------------------------------------------------------------------
    // Helpers.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// Every accessor is faulted at submit return — so the callback ran inside the P/Invoke —
    /// with the one error instance carrying <paramref name="message"/>.
    /// </summary>
    private static void AssertFaultedInline(Task all, string message, params Task[] keyResults)
    {
        Assert.True(all.IsFaulted, "the inline callback must have faulted the task before the submit returned");
        KafkaException error = Assert.IsType<KafkaException>(all.Exception!.InnerException);
        Assert.Equal(message, error.Message);

        foreach (Task keyResult in keyResults)
        {
            Assert.True(keyResult.IsFaulted, "a per-key accessor awaits the same, already-faulted task");
            Assert.Same(error, keyResult.Exception!.InnerException);
        }
    }

    /// <summary>
    /// The client's own reference is intact before <c>Dispose</c> — an over-release by the
    /// operation would already have closed the handle — and gone after it — an
    /// under-release would keep it open.
    /// </summary>
    private static void AssertReleasedExactlyOnce(NativeAdminClient admin, SafeAdminHandle handle)
    {
        Assert.False(handle.IsClosed, "the operation must not release more than its own reference");

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(handle.IsClosed, "the operation must have released its reference exactly once");
    }

    /// <summary>
    /// After the submit returned without firing: the task is pending, the client stays open
    /// past its own <c>Dispose</c>, and <c>user_data</c> still resolves to the operation.
    /// </summary>
    private static void AssertPendingAndRooted<TKey>(
        NativeAdminClient admin, SafeAdminHandle handle, Task all, IntPtr userData)
        where TKey : notnull
    {
        Assert.NotEqual(IntPtr.Zero, userData);
        Assert.False(all.IsCompleted, "nothing but the callback may resolve the operation");

        TestTimeout.Run(admin.Dispose, s_deadline);

        // Checked BEFORE the GCHandle is recovered: if a submit-side release had run, this is
        // where it shows, and recovering a freed GCHandle below is not something to attempt.
        Assert.False(
            handle.IsClosed,
            "the outstanding operation must still hold its client reference (PLAN R1)");

        object? target = GCHandle.FromIntPtr(userData).Target;
        Assert.IsType<SingleAdminOperation<(IReadOnlyDictionary<TKey, KafkaException?> PerKey, KafkaException? All)>>(
            target);
        Assert.False(all.IsCompleted);
    }

    /// <summary>
    /// An operation wired the way the production submit wires it: rooted by a
    /// <see cref="GCHandle"/> and holding a span-the-op reference on the client, both of which
    /// the trampoline's <c>finally</c> releases.
    /// </summary>
    private static SingleAdminOperation<(IReadOnlyDictionary<TKey, KafkaException?> PerKey, KafkaException? All)>
        RootedOperation<TKey>(string operationName, SafeAdminHandle handle, out IntPtr userData)
        where TKey : notnull
    {
        SingleAdminOperation<(IReadOnlyDictionary<TKey, KafkaException?> PerKey, KafkaException? All)> operation =
            new SingleAdminOperation<(IReadOnlyDictionary<TKey, KafkaException?> PerKey, KafkaException? All)>(
                operationName);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        bool refAdded = false;
        handle.DangerousAddRef(ref refAdded);
        Assert.True(refAdded);
        operation.SetHandleRef(handle);

        userData = GCHandle.ToIntPtr(gcHandle);
        return operation;
    }

    /// <summary>
    /// The operation resolved (not faulted — the root arrived with a null error), keyed by
    /// exactly the requested keys, each carrying the whole-request error, with the
    /// <c>_all</c> outcome carrying it too.
    /// </summary>
    private static void AssertWholeErrorOnEveryKey<TKey>(
        SingleAdminOperation<(IReadOnlyDictionary<TKey, KafkaException?> PerKey, KafkaException? All)> operation,
        TKey[] requested,
        string message)
        where TKey : notnull
    {
        Assert.Equal(TaskStatus.RanToCompletion, operation.Task.Status);
        (IReadOnlyDictionary<TKey, KafkaException?> perKey, KafkaException? all) = operation.Task.Result;

        Assert.Equal(requested.Length, perKey.Count);
        foreach (TKey key in requested)
        {
            Assert.True(perKey.TryGetValue(key, out KafkaException? error), key + " must be keyed");
            Assert.NotNull(error);
            Assert.Equal(UnsupportedVersionCode, error!.Code);
            Assert.Equal(message, error.Message);
        }

        Assert.NotNull(all);
        Assert.Equal(UnsupportedVersionCode, all!.Code);
        Assert.Equal(message, all.Message);
    }

    /// <summary>Mints an owned error handle the way the core would.</summary>
    private static IntPtr MintError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    /// <summary>
    /// The options for either mode: Java's no-argument constructor for <c>removeAll</c>, the
    /// member-list constructor otherwise.
    /// </summary>
    private static RemoveMembersFromConsumerGroupOptions RemoveMembersOptions(
        bool removeAll, params MemberToRemove[] members) =>
        removeAll
            ? new RemoveMembersFromConsumerGroupOptions()
            : new RemoveMembersFromConsumerGroupOptions(members);

    /// <summary>
    /// ⚠ <b>Test-only</b> P/Invokes of the three <b>synchronous</b> entry points (PLAN D7): the
    /// binding itself ships only the <c>_async</c> forms, so these live here and nowhere in
    /// the library. Declared exactly as the header's prototypes, with the admin handle passed
    /// as the <see cref="SafeAdminHandle"/> so the marshaller holds a call-scoped reference
    /// (ffi §A2's sync convention).
    /// </summary>
    private static class SyncNativeMethods
    {
        private const string DllName = "confluent_kafka";

        /// <summary>
        /// <c>kafka_admin_AdminClient_alter_consumer_group_offsets</c> — returns the owned
        /// error (null on success) and writes the owned result root to
        /// <paramref name="outResult"/>.
        /// </summary>
        [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_alter_consumer_group_offsets", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AlterConsumerGroupOffsets(
            SafeAdminHandle admin,
            IntPtr groupId,
            IntPtr[] topics,
            int[] partitions,
            long[] offsets,
            IntPtr[]? metadata,
            int[]? leaderEpochs,
            [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1)] bool[]? hasLeaderEpoch,
            int count,
            int timeoutMs,
            out IntPtr outResult);

        /// <summary>
        /// <c>kafka_admin_AdminClient_delete_consumer_group_offsets</c> — returns the owned
        /// error (null on success) and writes the owned result root to
        /// <paramref name="outResult"/>.
        /// </summary>
        [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_delete_consumer_group_offsets", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DeleteConsumerGroupOffsets(
            SafeAdminHandle admin,
            IntPtr groupId,
            IntPtr[] topics,
            int[] partitions,
            int count,
            int timeoutMs,
            out IntPtr outResult);

        /// <summary>
        /// <c>kafka_admin_AdminClient_remove_members_from_consumer_group</c> — returns the owned
        /// error (null on success) and, in member-list mode, writes the owned result root to
        /// <paramref name="outResult"/>. In <c>removeAll</c> mode a failed <c>all()</c> is the
        /// return value instead, and nothing is written. <c>ref</c>, not <c>out</c>, so a caller
        /// can pre-zero the slot and observe that.
        /// </summary>
        [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_remove_members_from_consumer_group", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr RemoveMembersFromConsumerGroup(
            SafeAdminHandle admin,
            IntPtr groupId,
            [MarshalAs(UnmanagedType.I1)] bool removeAll,
            IntPtr[]? groupInstanceIds,
            int memberCount,
            IntPtr reason,
            int timeoutMs,
            ref IntPtr outResult);
    }
}
