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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 F4 — since PR #201 round 70 the core calls back <b>once per distinct key</b>
/// (first occurrence wins), not once per occurrence. The five RPCs whose keys are a caller
/// map's key set must therefore count the keys the way the <b>core</b> compares them, not the
/// way the caller's comparer does.
/// </summary>
/// <remarks>
/// <para>
/// A caller map with a non-value comparer holds two value-equal keys as two entries. The
/// bridge keeps one <see cref="Task"/> for them (it is keyed by value equality), the core
/// answers once, and a countdown armed with the map's key count never reaches zero: the
/// caller's <see cref="Task"/> completes, so nothing visibly hangs, but the <c>GCHandle</c>
/// and the span-the-op client reference stay held and <c>AdminClient_destroy</c> is deferred
/// for the process lifetime. <see cref="SafeHandle.IsClosed"/> after <c>Dispose</c> is what
/// observes that (the method Critic 85's finding 85.1 used for <c>incrementalAlterConfigs</c>).
/// </para>
/// <para>
/// Each test drives <b>production's own submit</b> through its seam: the stand-in records what
/// the submit handed native and then forwards to the real P/Invoke, so the answer and the
/// release both come from the real core. The recorded arguments prove that only the first
/// occurrence was sent; the core's answer proves how many callbacks it made.
/// </para>
/// <para>
/// The comparers are hand-written because this project also compiles for net462, where
/// <c>System.Collections.Generic.ReferenceEqualityComparer</c> (.NET 5+) does not exist.
/// <see cref="TopicPartition"/> is a struct, so reference equality cannot apply to it; its
/// stand-in compares the <em>topic string's</em> identity instead, which is the same failure:
/// two value-equal keys kept apart.
/// </para>
/// </remarks>
public sealed class AdminDistinctKeyArmingTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    /// <summary>The code the mock reports for an RPC Java's mock does not implement.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>The code Kafka assigns to <c>KAFKA_STORAGE_ERROR</c>.</summary>
    private const int KafkaStorageErrorCode = 56;

    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// <c>createPartitions</c>: two equal topic names are one topic. The mock refuses the RPC
    /// (Java's own <c>UnsupportedOperationException</c>), which is enough — the answer arrives
    /// once and the operation must release.
    /// </summary>
    [Fact]
    public async Task CreatePartitions_TwoEqualTopicsUnderTheCallersComparer_AreSentOnce_AndRelease()
    {
        string first = Copy("f4-parts");
        string second = Copy("f4-parts");
        Dictionary<string, NewPartitions> request =
            new Dictionary<string, NewPartitions>(ReferenceComparer<string>.Instance)
            {
                [first] = NewPartitions.IncreaseTo(3),
                [second] = NewPartitions.IncreaseTo(4),
            };
        Assert.Equal(2, request.Count);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        int count = -1;
        IReadOnlyList<string?> topics = Array.Empty<string?>();
        CreatePartitionsResult result = admin.CreatePartitions(
            request,
            options: null,
            (nativeHandle, topicArray, newPartitions, n, timeoutMs, validateOnly, retry, callback, userData) =>
            {
                count = n;
                topics = Decode(topicArray);
                Assert.Equal(n, newPartitions.Length);
                NativeMethods.AdminClientCreatePartitionsAsync(
                    nativeHandle, topicArray, newPartitions, n, timeoutMs, validateOnly, retry, callback, userData);
            });

        Assert.Equal(1, count);
        Assert.Equal(new[] { "f4-parts" }, topics);
        Assert.Single(result.Values);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[second], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the topics the core answers, not the caller map's keys");
    }

    /// <summary>
    /// <c>deleteRecords</c> (STATUS open item (a)): two equal partitions are one partition, and
    /// the first one's offset is the one sent.
    /// </summary>
    [Fact]
    public async Task DeleteRecords_TwoEqualPartitionsUnderTheCallersComparer_SendTheFirst_AndRelease()
    {
        TopicPartition first = new TopicPartition(Copy("f4-records"), 0);
        TopicPartition second = new TopicPartition(Copy("f4-records"), 0);
        Assert.Equal(first, second);
        Dictionary<TopicPartition, RecordsToDelete> request =
            new Dictionary<TopicPartition, RecordsToDelete>(TopicIdentityComparer.Instance)
            {
                [first] = RecordsToDelete.BeforeOffset(10),
                [second] = RecordsToDelete.BeforeOffset(20),
            };
        Assert.Equal(2, request.Count);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        int count = -1;
        IReadOnlyList<string?> topics = Array.Empty<string?>();
        int[] partitions = Array.Empty<int>();
        long[] offsets = Array.Empty<long>();
        DeleteRecordsResult result = admin.DeleteRecords(
            request,
            options: null,
            (nativeHandle, topicArray, partitionArray, beforeOffsets, n, timeoutMs, callback, userData) =>
            {
                count = n;
                topics = Decode(topicArray);
                partitions = partitionArray.ToArray();
                offsets = beforeOffsets.ToArray();
                NativeMethods.AdminClientDeleteRecordsAsync(
                    nativeHandle, topicArray, partitionArray, beforeOffsets, n, timeoutMs, callback, userData);
            });

        Assert.Equal(1, count);
        Assert.Equal(new[] { "f4-records" }, topics);
        Assert.Equal(new[] { 0 }, partitions);
        Assert.Equal(new[] { 10L }, offsets);
        Assert.Single(result.LowWatermarks);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.LowWatermarks[second], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the partitions the core answers, not the caller map's keys");
    }

    /// <summary>
    /// <c>alterReplicaLogDirs</c> (STATUS open item (a)): two equal replicas are one replica.
    /// The mock names the log directory it was asked for in its answer, so the message itself
    /// proves the <b>first</b> occurrence's directory reached the core.
    /// </summary>
    [Fact]
    public async Task AlterReplicaLogDirs_TwoEqualReplicasUnderTheCallersComparer_SendTheFirst_AndRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("f4-replicas", 1, 1) }, options: null).All(),
            s_deadline);

        TopicPartitionReplica first = new TopicPartitionReplica("f4-replicas", 0, 0);
        TopicPartitionReplica second = new TopicPartitionReplica("f4-replicas", 0, 0);
        Assert.Equal(first, second);
        Dictionary<TopicPartitionReplica, string> request =
            new Dictionary<TopicPartitionReplica, string>(ReferenceComparer<TopicPartitionReplica>.Instance)
            {
                [first] = "/f4-first",
                [second] = "/f4-second",
            };
        Assert.Equal(2, request.Count);

        int count = -1;
        IReadOnlyList<string?> directories = Array.Empty<string?>();
        AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            request,
            options: null,
            (nativeHandle, topics, partitions, brokerIds, logDirs, n, timeoutMs, callback, userData) =>
            {
                count = n;
                directories = Decode(logDirs);
                NativeMethods.AdminClientAlterReplicaLogDirsAsync(
                    nativeHandle, topics, partitions, brokerIds, logDirs, n, timeoutMs, callback, userData);
            });

        Assert.Equal(1, count);
        Assert.Equal(new[] { "/f4-first" }, directories);
        Assert.Single(result.Values);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[second], s_deadline));
        Assert.Equal(KafkaStorageErrorCode, failure.Code);
        Assert.Equal("Log directory /f4-first is offline", failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the replicas the core answers, not the caller map's keys");
    }

    /// <summary>
    /// <c>alterPartitionReassignments</c>: two equal partitions are one partition. The first
    /// is a reassignment and the second a cancellation, so the recorded flag tells which was
    /// sent, and the mock answers the reassignment with success.
    /// </summary>
    [Fact]
    public async Task AlterPartitionReassignments_TwoEqualPartitionsUnderTheCallersComparer_SendTheFirst_AndRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("f4-reassign", 1, 1) }, options: null).All(),
            s_deadline);

        TopicPartition first = new TopicPartition(Copy("f4-reassign"), 0);
        TopicPartition second = new TopicPartition(Copy("f4-reassign"), 0);
        Dictionary<TopicPartition, NewPartitionReassignment?> request =
            new Dictionary<TopicPartition, NewPartitionReassignment?>(TopicIdentityComparer.Instance)
            {
                [first] = new NewPartitionReassignment(new[] { 0 }),
                [second] = null,
            };
        Assert.Equal(2, request.Count);

        int count = -1;
        bool[] cancel = Array.Empty<bool>();
        int[] replicaCounts = Array.Empty<int>();
        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            request,
            options: null,
            (nativeHandle, topics, partitions, cancelArray, replicas, counts, n, timeoutMs, allowChange, callback,
                userData) =>
            {
                count = n;
                Assert.Equal(n, topics.Length);
                Assert.Equal(n, partitions.Length);
                Assert.Equal(n, replicas.Length);
                cancel = cancelArray.ToArray();
                replicaCounts = counts.ToArray();
                NativeMethods.AdminClientAlterPartitionReassignmentsAsync(
                    nativeHandle, topics, partitions, cancelArray, replicas, counts, n, timeoutMs, allowChange,
                    callback, userData);
            });

        Assert.Equal(1, count);
        Assert.Equal(new[] { false }, cancel);
        Assert.Equal(new[] { 1 }, replicaCounts);
        Assert.Single(result.Values);

        await TestTimeout.Run(() => result.Values[second], s_deadline);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the partitions the core answers, not the caller map's keys");
    }

    /// <summary>
    /// <c>listOffsets</c>: two equal partitions are one partition. The first asks for the
    /// latest offset, which the mock answers; the second asks for a timestamp, which it
    /// refuses — so a successful answer proves the first occurrence's spec reached the core.
    /// </summary>
    [Fact]
    public async Task ListOffsets_TwoEqualPartitionsUnderTheCallersComparer_SendTheFirst_AndRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("f4-offsets", 1, 1) }, options: null).All(),
            s_deadline);

        TopicPartition first = new TopicPartition(Copy("f4-offsets"), 0);
        TopicPartition second = new TopicPartition(Copy("f4-offsets"), 0);
        Dictionary<TopicPartition, OffsetSpec> request =
            new Dictionary<TopicPartition, OffsetSpec>(TopicIdentityComparer.Instance)
            {
                [first] = OffsetSpec.Latest(),
                [second] = OffsetSpec.ForTimestamp(1),
            };
        Assert.Equal(2, request.Count);

        int count = -1;
        bool[] isTimestamp = Array.Empty<bool>();
        long[] specs = Array.Empty<long>();
        ListOffsetsResult result = admin.ListOffsets(
            request,
            options: null,
            (nativeHandle, topics, partitions, timestampFlags, specTimestamps, n, timeoutMs, isolation, callback,
                userData) =>
            {
                count = n;
                Assert.Equal(n, topics.Length);
                Assert.Equal(n, partitions.Length);
                isTimestamp = timestampFlags.ToArray();
                specs = specTimestamps.ToArray();
                NativeMethods.AdminClientListOffsetsAsync(
                    nativeHandle, topics, partitions, timestampFlags, specTimestamps, n, timeoutMs, isolation,
                    callback, userData);
            });

        Assert.Equal(1, count);
        Assert.Equal(new[] { false }, isTimestamp);
        Assert.Equal(new[] { -1L }, specs);

        ListOffsetsResult.ListOffsetsResultInfo info =
            await TestTimeout.Run(() => result.PartitionResult(second), s_deadline);
        Assert.Equal(-1L, info.Offset);
        IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> all =
            await TestTimeout.Run(result.All, s_deadline);
        Assert.Single(all);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the partitions the core answers, not the caller map's keys");
    }

    /// <summary>
    /// The list-input regression: a name repeated in a plain list is one topic, completes once
    /// and releases. <c>DistinctNames</c> already de-duplicates list inputs ordinally, as the
    /// core compares them; this pins it now that a second occurrence gets no callback at all.
    /// </summary>
    [Fact]
    public async Task DescribeTopics_ANameRepeatedInAList_CompletesOnce_AndReleases()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("a", 1, 1) }, options: null).All(),
            s_deadline);

        DescribeTopicsResult result =
            admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { "a", "a" }), options: null);

        KeyValuePair<string, Task<TopicDescription>> only = Assert.Single(result.TopicNameValues!);
        Assert.Equal("a", only.Key);
        TopicDescription description = await TestTimeout.Run(() => only.Value, s_deadline);
        Assert.Equal("a", description.Name);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "a repeated name must be counted once, as the core answers it once");
    }

    /// <summary>A fresh string instance equal to <paramref name="value"/> but never the same reference.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string Copy(string value) => new string(value.ToCharArray());

    private static IReadOnlyList<string?> Decode(IntPtr[] values) =>
        values.Select(value => Utf8Marshal.PtrToString(value)).ToArray();

    /// <summary>
    /// Disposes the client, then waits — bounded — for the native release: the callbacks come
    /// from the real ABI on its dispatcher thread, and a per-key trampoline resolves its key
    /// before it releases the operation, so an awaiter can reach <c>Dispose</c> a moment early.
    /// A leak never releases, so the bound only decides how long a red takes to report.
    /// </summary>
    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
    }

    /// <summary>Reference equality — two equal instances stay two keys.</summary>
    private sealed class ReferenceComparer<T> : IEqualityComparer<T>
        where T : class
    {
        public static readonly ReferenceComparer<T> Instance = new ReferenceComparer<T>();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>
    /// The struct's stand-in for reference equality: the partition by value, the topic by
    /// string <em>identity</em>, so two value-equal <see cref="TopicPartition"/>s over two
    /// string instances stay two keys.
    /// </summary>
    private sealed class TopicIdentityComparer : IEqualityComparer<TopicPartition>
    {
        public static readonly TopicIdentityComparer Instance = new TopicIdentityComparer();

        public bool Equals(TopicPartition x, TopicPartition y) =>
            ReferenceEquals(x.Topic, y.Topic) && x.Partition == y.Partition;

        public int GetHashCode(TopicPartition obj) => RuntimeHelpers.GetHashCode(obj.Topic) ^ obj.Partition;
    }
}
