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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P9 CP7's group RPCs — the two group-offsets RPCs and
/// <c>removeMembersFromConsumerGroup</c>, all of which Java answers with ONE aggregate future
/// whose map VALUE is the per-key error, so only the public result type's projection turns a
/// per-key error into a throw.
/// </summary>
/// <remarks>
/// <para>
/// Since M15/P13.1 all three are on PR #201's <b>single-callback</b> ABI: one callback
/// carrying the result root or the whole-request error, never both — the
/// <c>electLeaders</c> shape — whatever the key count, and for
/// <c>removeMembersFromConsumerGroup</c> whatever the mode. Their per-key walk and the
/// lifetime of that one callback are pinned in <c>AdminP13GroupOffsetsSingleCallbackTests</c>;
/// the rows here are the end-to-end ones against the mock.
/// </para>
/// </remarks>
public sealed class AdminP9PerKeyStage6Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>How long a fire count is left to settle before it is read.</summary>
    private static readonly TimeSpan s_settleWindow = TimeSpan.FromMilliseconds(250);

    /// <summary>The code the mock reports for every unimplemented RPC.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    /// <summary>The <c>alterConsumerGroupOffsets</c> mock message carries the core's typo.</summary>
    private const string NotImplementedTypo = "Not implement yet";

    // ------------------------------------------------------------------------------------
    // alterConsumerGroupOffsets / deleteConsumerGroupOffsets — one callback, one outcome.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠ Against the real ABI the mock fails the RPC's one future, which the callback
    /// delivers as the whole-request error: <c>All()</c> rethrows it <b>verbatim</b> (no
    /// "Failed altering group offsets" wording, which Java attaches only to a per-partition
    /// failure), and <c>PartitionResult</c> rethrows the <b>same instance</b> — for an
    /// unrequested partition too, because Java tests the <c>throwable</c> before the map
    /// (<c>AlterConsumerGroupOffsetsResult.java:46-47</c>, G5-4). The single callback's
    /// <c>finally</c> is what lets the client release.
    /// </summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_AgainstTheMock_TheWholeRequestErrorArrivesUnchanged()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition first = new TopicPartition("cp7-mock-acgo", 0);
        TopicPartition second = new TopicPartition("cp7-mock-acgo", 1);
        TopicPartition unrequested = new TopicPartition("cp7-mock-acgo", 9);

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "cp7-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [first] = new OffsetAndMetadata(5),
                [second] = new OffsetAndMetadata(6),
            },
            options: null);

        KafkaException fromAll = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplementedTypo, fromAll.Message);

        foreach (TopicPartition partition in new[] { first, second, unrequested })
        {
            KafkaException fromPartition = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.PartitionResult(partition), s_deadline));
            Assert.Same(fromAll, fromPartition);
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "the one callback's finally must have released the client");
    }

    /// <summary>
    /// The same for <c>deleteConsumerGroupOffsets</c>: every requested partition rethrows the
    /// one whole-request error instance, verbatim (Java's
    /// <c>DeleteConsumerGroupOffsetsResult.java:50-51</c> / <c>:67-68</c>).
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_AgainstTheMock_TheWholeRequestErrorArrivesUnchanged()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition[] requested =
        {
            new TopicPartition("cp7-mock-dcgo", 0), new TopicPartition("cp7-mock-dcgo", 3),
        };

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "cp7-group", requested, options: null);

        KafkaException fromAll = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);

        foreach (TopicPartition partition in requested)
        {
            KafkaException fromPartition = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.PartitionResult(partition), s_deadline));
            Assert.Same(fromAll, fromPartition);
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "the one callback's finally must have released the client");
    }

    /// <summary>
    /// ⚠ A repeated partition collapses to ONE request entry before the submit, because Java's
    /// parameter is a <c>Set</c>. Asserted on the <c>count</c> the core is handed, through a
    /// seam that forwards to the real P/Invoke — so the refusal and the release below are
    /// the core's, not a stand-in's.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_ADuplicatePartition_ReachesTheCoreOnce()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition partition = new TopicPartition("cp7-dup-dcgo", 2);
        int sawCount = -1;

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "cp7-group",
            new[] { partition, partition },
            options: null,
            (nativeHandle, groupId, topics, partitions, count, timeoutMs, callback, userData) =>
            {
                sawCount = count;
                NativeMethods.AdminClientDeleteConsumerGroupOffsetsAsync(
                    nativeHandle, groupId, topics, partitions, count, timeoutMs, callback, userData);
            });

        Assert.Equal(1, sawCount);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.PartitionResult(partition), s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "the one callback's finally must have released the client");
    }

    // ------------------------------------------------------------------------------------
    // removeMembersFromConsumerGroup — one callback in either mode, against the real ABI.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠ removeAll mode against the real ABI: the core fires its ONE callback "in
    /// <c>remove_all</c> mode too" — against the mock, with the whole-request refusal, which
    /// <c>All()</c> rethrows verbatim — and that callback's <c>finally</c> is what releases the
    /// client. The seam forwards to the real P/Invoke through a counting wrapper, so the fire
    /// count is the core's, and it pins the removeAll submit shape: the flag set, no member
    /// array at all, a zero count.
    /// </summary>
    [Fact]
    public async Task RemoveMembers_RemoveAllMode_AgainstTheMock_FaultsAndReleases()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        CountingRemoveMembersSeam seam = new CountingRemoveMembersSeam();

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "cp7-group", new RemoveMembersFromConsumerGroupOptions(), seam.Submit);

        Assert.True(seam.RemoveAll);
        Assert.Equal(0, seam.MemberCount);
        Assert.Null(seam.GroupInstanceIds);
        Assert.True(result.RemoveAll);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        await seam.AssertFiredExactlyOnceAfterSettling();

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "the one callback's finally must have released the client");
    }

    /// <summary>
    /// ⚠⚠ Member-list mode against the real ABI with <b>N &gt; 1</b> members: the core fires
    /// <b>one</b> callback, not one per member, and that one callback both resolves every
    /// accessor — <c>All()</c> and each requested member's <c>MemberResult</c> rethrow the
    /// <b>same</b> whole-request refusal instance — and releases the operation, so the client's
    /// <see cref="IDisposable.Dispose"/> returns and closes the handle.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A duplicate id in the request collapses in the options' member set, so the core is
    /// handed exactly the three distinct ids. Before M15/P13.1 this row armed a countdown for
    /// that distinct count and needed one callback per member to reach zero; on the
    /// single-callback ABI there is nothing to count down, and a leftover per-member arming
    /// would leave the handle open below (no second callback ever arrives).
    /// </para>
    /// <para>
    /// <c>IsClosed</c> is the witness for "the <c>GCHandle</c> was freed" as well as for the
    /// client reference: <c>AdminOperation.FreeGcHandle</c> frees the one and releases the
    /// other inside a single once-only block, so the reference cannot be released without the
    /// <c>GCHandle</c> free having run first.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task RemoveMembers_NonRemoveAllMode_AgainstTheMock_ManyMembersFireOneCallbackAndRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        CountingRemoveMembersSeam seam = new CountingRemoveMembersSeam();

        MemberToRemove[] members =
        {
            new MemberToRemove("cp7-many-a"),
            new MemberToRemove("cp7-many-b"),
            new MemberToRemove("cp7-many-c"),
        };
        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(
            new[] { members[0], members[1], members[2], new MemberToRemove("cp7-many-b") });
        Assert.Equal(3, options.Members.Count);

        RemoveMembersFromConsumerGroupResult result =
            admin.RemoveMembersFromConsumerGroup("cp7-group", options, seam.Submit);

        Assert.False(seam.RemoveAll);
        Assert.Equal(3, seam.MemberCount);
        Assert.Equal(
            new[] { "cp7-many-a", "cp7-many-b", "cp7-many-c" },
            SortedOrdinal(seam.GroupInstanceIds!));

        KafkaException fromAll = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);

        foreach (MemberToRemove member in members)
        {
            KafkaException fromMember = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.MemberResult(member), s_deadline));
            Assert.Same(fromAll, fromMember);
        }

        await seam.AssertFiredExactlyOnceAfterSettling();

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "the one callback's finally must have released the client");
    }

    private static string[] SortedOrdinal(IEnumerable<string> values)
    {
        List<string> sorted = new List<string>(values);
        sorted.Sort(StringComparer.Ordinal);
        return sorted.ToArray();
    }

    /// <summary>
    /// A <c>remove_members_from_consumer_group_async</c> submit seam that forwards to the
    /// <b>real</b> P/Invoke through a wrapper counting the core's fires, recording what the
    /// core was handed.
    /// </summary>
    /// <remarks>
    /// ⚠ The wrapper is a fresh delegate the core holds a thunk for until it fires, so it is
    /// rooted in a field of this seam, and the test keeps the seam reachable past the settle
    /// window with <see cref="GC.KeepAlive(object)"/> (DoD §12: the wrapper forwards to the
    /// production trampoline it was handed, never a stand-in).
    /// </remarks>
    private sealed class CountingRemoveMembersSeam
    {
        private int _fires;
        private AdminCallbacks.RemoveMembersFromConsumerGroupCallback? _wrapper;

        internal bool RemoveAll { get; private set; }

        internal int MemberCount { get; private set; } = -1;

        internal string[]? GroupInstanceIds { get; private set; }

        internal void Submit(
            IntPtr admin,
            IntPtr groupId,
            bool removeAll,
            IntPtr[]? groupInstanceIds,
            int memberCount,
            IntPtr reason,
            int timeoutMs,
            AdminCallbacks.RemoveMembersFromConsumerGroupCallback callback,
            IntPtr userData)
        {
            Assert.Same(AdminCallbacks.RemoveMembersFromConsumerGroup, callback);

            RemoveAll = removeAll;
            MemberCount = memberCount;
            if (groupInstanceIds is not null)
            {
                string[] ids = new string[memberCount];
                for (int i = 0; i < memberCount; i++)
                {
                    ids[i] = Utf8Marshal.PtrToString(groupInstanceIds[i])!;
                }

                GroupInstanceIds = ids;
            }

            _wrapper = (result, error, operationUserData) =>
            {
                Interlocked.Increment(ref _fires);
                callback(result, error, operationUserData);
            };

            NativeMethods.AdminClientRemoveMembersFromConsumerGroupAsync(
                admin, groupId, removeAll, groupInstanceIds, memberCount, reason, timeoutMs,
                _wrapper, userData);
        }

        /// <summary>
        /// Exactly one fire, read <b>after</b> a settle window: a first observation of 1
        /// cannot tell one invocation from two.
        /// </summary>
        internal async Task AssertFiredExactlyOnceAfterSettling()
        {
            await Task.Delay(s_settleWindow);
            Assert.Equal(1, Volatile.Read(ref _fires));
            GC.KeepAlive(this);
        }
    }

    // ------------------------------------------------------------------------------------
    // Empty group-offsets requests — the core's one callback still decides the outcome.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>Inverted in M15/P13.1 (F4).</b> An empty request no longer resolves at the
    /// submit boundary: the ABI fires its one callback "also when <c>count</c> is 0", so
    /// <c>All()</c> carries the core's outcome — against the mock, its refusal, verbatim (Java's
    /// <c>MockAdminClient</c> refuses both calls whatever their input). The client still
    /// releases once that callback ran.
    /// </summary>
    /// <remarks>
    /// The status check is the deterministic half: whether the task is still pending or
    /// already faulted when <c>All()</c> is first called races the dispatcher, but it is
    /// never <see cref="TaskStatus.RanToCompletion"/>, which is exactly what the pre-P13.1
    /// submit-boundary resolution produced. The "pending at submit return" half needs a seam
    /// that does not fire, and lives in
    /// <c>AdminP13GroupOffsetsSingleCallbackTests.UntilTheCallbackFires_TheOperationStaysPendingAndRooted</c>.
    /// </remarks>
    [Fact]
    public async Task EmptyConsumerGroupOffsetRequests_CarryTheCoresOutcome()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        AlterConsumerGroupOffsetsResult alter = admin.AlterConsumerGroupOffsets(
            "cp7-group", new Dictionary<TopicPartition, OffsetAndMetadata>(), options: null);
        DeleteConsumerGroupOffsetsResult delete = admin.DeleteConsumerGroupOffsets(
            "cp7-group", Array.Empty<TopicPartition>(), options: null);

        Task alterAll = alter.All();
        Task deleteAll = delete.All();
        Assert.NotEqual(TaskStatus.RanToCompletion, alterAll.Status);
        Assert.NotEqual(TaskStatus.RanToCompletion, deleteAll.Status);

        KafkaException alterFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => alterAll, s_deadline));
        Assert.Equal(UnsupportedVersionCode, alterFailure.Code);
        Assert.Equal(NotImplementedTypo, alterFailure.Message);

        KafkaException deleteFailure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => deleteAll, s_deadline));
        Assert.Equal(UnsupportedVersionCode, deleteFailure.Code);
        Assert.Equal(NotImplemented, deleteFailure.Message);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "an empty request must still release the client");
    }
}
