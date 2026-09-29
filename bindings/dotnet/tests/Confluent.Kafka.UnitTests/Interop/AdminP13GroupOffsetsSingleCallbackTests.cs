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
/// M15/P13.1 CP1 — <c>alterConsumerGroupOffsets</c> and <c>deleteConsumerGroupOffsets</c> on
/// PR #201's single-callback ABI: <b>one</b> callback per call, carrying the result root or
/// the whole-request error, never both, and firing "also when <c>count</c> is 0".
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

        AssertFaultedInline(result.All(), result.PartitionResult(partition), NullAdmin);
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

        AssertFaultedInline(result.All(), result.PartitionResult(partition), NegativeOffsetAtZero);
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

        AssertFaultedInline(result.All(), result.PartitionResult(partition), NullAdmin);
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
        AssertPendingAndRooted(admin, handle, all, capturedUserData);

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
        AssertPendingAndRooted(admin, handle, all, capturedUserData);

        capturedCallback!(IntPtr.Zero, MintError(11, "p13-late-dcgo"), capturedUserData);

        Assert.True(handle.IsClosed, "the one callback's finally must be the release");
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => all, s_deadline));
        Assert.Equal(11, failure.Code);
        Assert.Equal("p13-late-dcgo", failure.Message);
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
            RootedOperation("alterConsumerGroupOffsets", handle, out IntPtr userData);

        // ⚠ Ownership of `root` transfers to the trampoline: it destroys it after the reads,
        // so nothing below may touch `root` again.
        AdminCallbacks.AlterConsumerGroupOffsets(root, IntPtr.Zero, userData);

        AssertWholeErrorOnEveryPartition(
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
            RootedOperation("deleteConsumerGroupOffsets", handle, out IntPtr userData);

        // ⚠ Ownership of `root` transfers to the trampoline — see the alter twin.
        AdminCallbacks.DeleteConsumerGroupOffsets(root, IntPtr.Zero, userData);

        AssertWholeErrorOnEveryPartition(
            operation, new[] { first, second }, NotImplemented);
        AssertReleasedExactlyOnce(admin, handle);
    }

    // ------------------------------------------------------------------------------------
    // Helpers.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// Both accessors are faulted at submit return — so the callback ran inside the P/Invoke —
    /// with the one error instance carrying <paramref name="message"/>.
    /// </summary>
    private static void AssertFaultedInline(Task all, Task partitionResult, string message)
    {
        Assert.True(all.IsFaulted, "the inline callback must have faulted the task before the submit returned");
        KafkaException error = Assert.IsType<KafkaException>(all.Exception!.InnerException);
        Assert.Equal(message, error.Message);

        Assert.True(partitionResult.IsFaulted, "PartitionResult awaits the same, already-faulted task");
        Assert.Same(error, partitionResult.Exception!.InnerException);
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
    private static void AssertPendingAndRooted(
        NativeAdminClient admin, SafeAdminHandle handle, Task all, IntPtr userData)
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
        Assert.IsType<SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>>(
            target);
        Assert.False(all.IsCompleted);
    }

    /// <summary>
    /// An operation wired the way the production submit wires it: rooted by a
    /// <see cref="GCHandle"/> and holding a span-the-op reference on the client, both of which
    /// the trampoline's <c>finally</c> releases.
    /// </summary>
    private static SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>
        RootedOperation(string operationName, SafeAdminHandle handle, out IntPtr userData)
    {
        SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation =
            new SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
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
    /// exactly the requested partitions, each carrying the whole-request error, with the
    /// <c>_all</c> outcome carrying it too.
    /// </summary>
    private static void AssertWholeErrorOnEveryPartition(
        SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation,
        TopicPartition[] requested,
        string message)
    {
        Assert.Equal(TaskStatus.RanToCompletion, operation.Task.Status);
        (IReadOnlyDictionary<TopicPartition, KafkaException?> perKey, KafkaException? all) = operation.Task.Result;

        Assert.Equal(requested.Length, perKey.Count);
        foreach (TopicPartition partition in requested)
        {
            Assert.True(perKey.TryGetValue(partition, out KafkaException? error), partition + " must be keyed");
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
    /// ⚠ <b>Test-only</b> P/Invokes of the two <b>synchronous</b> entry points (PLAN D7): the
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
    }
}
