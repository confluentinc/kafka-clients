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
/// M15/P9 CP7 — the three <b>shape 4c</b> RPCs, where the ABI fans out one callback per key
/// but Java holds ONE aggregate future, so a per-key error is that map's VALUE and only the
/// public result type's projection turns it into a throw. Includes
/// <c>removeMembersFromConsumerGroup</c>, the one RPC whose callback count depends on the
/// request mode (1 NULL-keyed callback in removeAll mode, else one per member).
/// </summary>
public sealed class AdminP9PerKeyStage6Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code the mock reports for every unimplemented RPC.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    /// <summary>The <c>alterConsumerGroupOffsets</c> mock message carries the core's typo.</summary>
    private const string NotImplementedTypo = "Not implement yet";

    // ------------------------------------------------------------------------------------
    // alterConsumerGroupOffsets / deleteConsumerGroupOffsets — the (topic, partition) key.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ A per-partition error resolves as a map VALUE: the task is <b>not</b> faulted, the
    /// null-error partition completes, and the failing one throws its own code and message
    /// under a correctly reconstructed <c>(topic, partition)</c> key.
    /// </summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_PerPartitionErrorIsAMapValue()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition good = new TopicPartition("cp7-acgo", 4);
        TopicPartition bad = new TopicPartition("cp7-acgo", 9);

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "cp7-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [good] = new OffsetAndMetadata(1),
                [bad] = new OffsetAndMetadata(2),
            },
            options: null,
            (handle, groupId, topics, partitions, offsets, metadata, leaderEpochs,
             hasLeaderEpoch, count, timeoutMs, callback, userData) =>
            {
                Assert.Equal(2, count);
                for (int i = 0; i < count; i++)
                {
                    callback(
                        topics[i],
                        partitions[i],
                        partitions[i] == bad.Partition ? MintError(7, "cp7-acgo-9") : IntPtr.Zero,
                        userData);
                }
            });

        await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.PartitionResult(bad), s_deadline));
        Assert.Equal(7, failure.Code);
        Assert.Equal("cp7-acgo-9", failure.Message);
    }

    /// <summary>
    /// The same fan-in for <c>deleteConsumerGroupOffsets</c>, whose submit carries no
    /// per-partition value arrays.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_PerPartitionErrorIsAMapValue()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition good = new TopicPartition("cp7-dcgo", 0);
        TopicPartition bad = new TopicPartition("cp7-dcgo", 3);

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "cp7-group",
            new[] { good, bad },
            options: null,
            (handle, groupId, topics, partitions, count, timeoutMs, callback, userData) =>
            {
                Assert.Equal(2, count);
                for (int i = 0; i < count; i++)
                {
                    callback(
                        topics[i],
                        partitions[i],
                        partitions[i] == bad.Partition ? MintError(11, "cp7-dcgo-3") : IntPtr.Zero,
                        userData);
                }
            });

        await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.PartitionResult(bad), s_deadline));
        Assert.Equal(11, failure.Code);
        Assert.Equal("cp7-dcgo-3", failure.Message);
    }

    /// <summary>
    /// ⚠ Against the real ABI, <c>All()</c>'s aggregate message names <b>both</b> requested
    /// partitions — which is only reachable if the map RESOLVED with per-key values. Had the
    /// binding faulted the aggregate task instead, the mock's own message would surface
    /// verbatim and neither partition string would appear.
    /// </summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_AgainstTheMock_ResolvesWithEveryRequestedPartition()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition first = new TopicPartition("cp7-mock-acgo", 0);
        TopicPartition second = new TopicPartition("cp7-mock-acgo", 1);

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "cp7-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [first] = new OffsetAndMetadata(5),
                [second] = new OffsetAndMetadata(6),
            },
            options: null);

        KafkaException aggregate = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(UnsupportedVersionCode, aggregate.Code);
        Assert.Contains(first.ToString(), aggregate.Message, StringComparison.Ordinal);
        Assert.Contains(second.ToString(), aggregate.Message, StringComparison.Ordinal);

        KafkaException perPartition = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.PartitionResult(second), s_deadline));
        Assert.Equal(NotImplementedTypo, perPartition.Message);
    }

    /// <summary>
    /// Both partitions carry the mock's refusal under their own key — a collapsed or crossed
    /// key would leave one of them missing and fault with the distinct "was not attempted"
    /// <see cref="ArgumentException"/> instead.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_AgainstTheMock_ResolvesWithEveryRequestedPartition()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition[] requested =
        {
            new TopicPartition("cp7-mock-dcgo", 0), new TopicPartition("cp7-mock-dcgo", 3),
        };

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "cp7-group", requested, options: null);

        foreach (TopicPartition partition in requested)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.PartitionResult(partition), s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// ⚠ A repeated partition must collapse to ONE key before the submit: the core fires once
    /// per key it was handed, so a duplicate would draw a second callback the accumulator
    /// cannot add — faulting the aggregate with a dictionary <see cref="ArgumentException"/>
    /// rather than the mock's <see cref="KafkaException"/>.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_ADuplicatePartition_CollapsesToOneKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition partition = new TopicPartition("cp7-dup-dcgo", 2);

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "cp7-group", new[] { partition, partition }, options: null);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.PartitionResult(partition), s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        admin.Dispose();
        Assert.True(handle.IsClosed, "the countdown must have reached zero exactly once");
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
    // n == 0 — the submit token alone resolves the aggregate.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// An empty request resolves at the submit boundary (the <c>n + 1</c> countdown's lone
    /// token) and releases the client.
    /// </summary>
    [Fact]
    public async Task EmptyConsumerGroupOffsetRequests_ResolveAtTheSubmitBoundary()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        AlterConsumerGroupOffsetsResult alter = admin.AlterConsumerGroupOffsets(
            "cp7-group", new Dictionary<TopicPartition, OffsetAndMetadata>(), options: null);
        DeleteConsumerGroupOffsetsResult delete = admin.DeleteConsumerGroupOffsets(
            "cp7-group", Array.Empty<TopicPartition>(), options: null);

        await TestTimeout.Run(alter.All, s_deadline);
        await TestTimeout.Run(delete.All, s_deadline);

        admin.Dispose();
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
