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
/// Since M15/P13.1 the two group-offsets RPCs are on PR #201's <b>single-callback</b> ABI:
/// one callback carrying the result root or the whole-request error, never both — the
/// <c>electLeaders</c> shape. Their per-key walk and the lifetime of that one callback are
/// pinned in <c>AdminP13GroupOffsetsSingleCallbackTests</c>; the rows here are the
/// end-to-end ones against the mock.
/// </para>
/// <para>
/// <c>removeMembersFromConsumerGroup</c> is still <b>shape 4c</b> — one callback per key —
/// and is the one RPC whose callback count depends on the request mode (1 NULL-keyed
/// callback in removeAll mode, else one per member).
/// </para>
/// </remarks>
public sealed class AdminP9PerKeyStage6Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

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
    // removeMembersFromConsumerGroup — the string key, and the phase's only mode-dependent n.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// Non-removeAll mode: one callback per <c>group.instance.id</c>, and each member's error
    /// is a map value.
    /// </summary>
    [Fact]
    public async Task RemoveMembers_NonRemoveAllMode_PerMemberErrorIsAMapValue()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        MemberToRemove good = new MemberToRemove("cp7-member-ok");
        MemberToRemove bad = new MemberToRemove("cp7-member-bad");

        bool sawRemoveAll = true;
        int sawCount = -1;

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "cp7-group",
            new RemoveMembersFromConsumerGroupOptions(new[] { good, bad }),
            (handle, groupId, removeAll, groupInstanceIds, memberCount, reason, timeoutMs,
             callback, userData) =>
            {
                sawRemoveAll = removeAll;
                sawCount = memberCount;

                for (int i = 0; i < memberCount; i++)
                {
                    string id = Utf8Marshal.PtrToString(groupInstanceIds![i])!;
                    callback(
                        groupInstanceIds[i],
                        id == bad.GroupInstanceId ? MintError(25, "cp7-member-failure") : IntPtr.Zero,
                        userData);
                }
            });

        Assert.False(sawRemoveAll);
        Assert.Equal(2, sawCount);

        await TestTimeout.Run(() => result.MemberResult(good), s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.MemberResult(bad), s_deadline));
        Assert.Equal(25, failure.Code);
        Assert.Equal("cp7-member-failure", failure.Message);

        KafkaException fromAll = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal("cp7-member-failure", fromAll.Message);
    }

    /// <summary>
    /// ⚠⚠ removeAll mode, success: the ABI fires ONE callback with a <b>NULL</b> key and a
    /// null error, which must resolve the aggregate with an <b>empty</b> map — no sentinel key
    /// enters the dictionary. <c>MemberResult</c> keeps throwing synchronously.
    /// </summary>
    [Fact]
    public async Task RemoveMembers_RemoveAllMode_ANullKeyWithNoError_ResolvesEmpty()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool sawRemoveAll = false;
        int sawCount = -1;
        bool sawNullArray = false;

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "cp7-group",
            new RemoveMembersFromConsumerGroupOptions(),
            (handle, groupId, removeAll, groupInstanceIds, memberCount, reason, timeoutMs,
             callback, userData) =>
            {
                sawRemoveAll = removeAll;
                sawCount = memberCount;
                sawNullArray = groupInstanceIds is null;

                callback(IntPtr.Zero, IntPtr.Zero, userData);
            });

        Assert.True(sawRemoveAll);
        Assert.Equal(0, sawCount);
        Assert.True(sawNullArray, "removeAll mode passes no member array at all");

        Assert.True(result.RemoveAll);
        await TestTimeout.Run(result.All, s_deadline);

        Assert.Throws<ArgumentException>(() =>
        {
            // Synchronously, before any await — so the discard is the assertion's subject.
            _ = result.MemberResult(new MemberToRemove("cp7-anyone"));
        });
    }

    /// <summary>
    /// ⚠⚠ removeAll mode, failure: the NULL-keyed callback's error <b>faults</b> the aggregate
    /// verbatim. The absence of <c>All</c>'s per-key wording is what proves no sentinel key was
    /// mapped — a sentinel would have surfaced as a re-wrapped per-member failure instead.
    /// </summary>
    [Fact]
    public async Task RemoveMembers_RemoveAllMode_ANullKeyWithAnError_FaultsTheAggregate()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "cp7-group",
            new RemoveMembersFromConsumerGroupOptions(),
            (handle, groupId, removeAll, groupInstanceIds, memberCount, reason, timeoutMs,
             callback, userData) =>
                callback(IntPtr.Zero, MintError(37, "cp7-remove-all-failure"), userData));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(37, failure.Code);
        Assert.Equal("cp7-remove-all-failure", failure.Message);
        Assert.DoesNotContain(
            "Encounter exception when trying to remove",
            failure.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠ Against the real ABI, removeAll mode must arm the countdown for exactly ONE callback:
    /// armed for zero, the aggregate would resolve empty at the submit boundary and
    /// <c>All()</c> would complete instead of faulting.
    /// </summary>
    [Fact]
    public async Task RemoveMembers_RemoveAllMode_AgainstTheMock_FaultsAndReleases()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "cp7-group", new RemoveMembersFromConsumerGroupOptions());

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        admin.Dispose();
        Assert.True(handle.IsClosed, "the removeAll countdown must have reached zero");
    }

    /// <summary>
    /// Non-removeAll mode against the real ABI, with a duplicate id in the request that the
    /// options' member set collapses — so the countdown is armed for the DISTINCT count and
    /// reaches zero. An over-armed countdown leaks the <c>GCHandle</c> forever, leaving
    /// <c>IsClosed</c> false after <see cref="IDisposable.Dispose"/>.
    /// </summary>
    [Fact]
    public async Task RemoveMembers_NonRemoveAllMode_AgainstTheMock_ArmsTheDistinctCount()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        MemberToRemove member = new MemberToRemove("cp7-mock-member");
        RemoveMembersFromConsumerGroupOptions options =
            new RemoveMembersFromConsumerGroupOptions(new[] { member, new MemberToRemove("cp7-mock-member") });
        Assert.Single(options.Members);

        RemoveMembersFromConsumerGroupResult result =
            admin.RemoveMembersFromConsumerGroup("cp7-group", options);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.MemberResult(member), s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        admin.Dispose();
        Assert.True(handle.IsClosed, "the per-member countdown must have reached zero");
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

    /// <summary>Mints an owned error handle the way the core would.</summary>
    private static IntPtr MintError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
