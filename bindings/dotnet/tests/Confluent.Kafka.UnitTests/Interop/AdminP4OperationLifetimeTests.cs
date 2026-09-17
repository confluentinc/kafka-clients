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
/// The span-the-op reference contract for M15/P4 Stage 1's two RPCs — the P4 twin of
/// <see cref="AdminP3OperationLifetimeTests"/>. <c>kafka_admin_AdminClient_destroy</c> is
/// <b>not</b> ref-counted and does <b>not</b> drain, so every new submit needs its own
/// proof rather than inheriting an earlier one's.
/// </summary>
/// <remarks>
/// The native call is injected for the same reason as in every earlier twin: "an operation
/// is in flight" has to be a fact the test controls, not a race it hopes to win. Everything
/// under test — the <c>DangerousAddRef</c>, the <c>GCHandle</c>, the trampoline, the
/// release in <c>FreeGcHandle</c> — is production code; only the clock is the test's.
/// </remarks>
public sealed class AdminP4OperationLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The three-way differential for both new submits. A single-case assertion cannot
    /// tell a working reference count from a permanently unbalanced one — both read "not
    /// released" — so each RPC gets all three cases: nothing in flight → <c>Dispose</c>
    /// releases; one in flight → it does <b>not</b>; the operation completes → it then
    /// does.
    /// </summary>
    [Theory]
    [InlineData(Rpc.ElectLeaders)]
    [InlineData(Rpc.AlterPartitionReassignments)]
    public async Task DisposeRacingAnInFlightOperation_DefersTheNativeDestroy(Rpc rpc)
    {
        // ---- (1) Nothing in flight: Dispose releases immediately. ----
        NativeAdminClient baseline = NativeAdminClient.CreateMock(1);
        SafeAdminHandle baselineHandle = baseline.Handle;
        TestTimeout.Run(baseline.Dispose, s_deadline);
        Assert.True(
            baselineHandle.IsClosed,
            "with no operation in flight the native release must be immediate");

        // ---- (2) One in flight: Dispose must NOT release. ----
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        Func<Task> outcome = SubmitCapturing(admin, rpc, userData => capturedUserData = userData);

        Assert.NotEqual(IntPtr.Zero, capturedUserData);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(
            handle.IsClosed,
            "an in-flight operation must defer AdminClient_destroy — the ABI does not protect this itself");

        // ---- (3) Completing the operation releases it. ----
        Complete(rpc, capturedUserData, MakeError(42, "submit failed"));

        Assert.True(handle.IsClosed, "completing the in-flight operation must run the deferred release");

        // …and the awaiter carries that failure rather than hanging.
        //
        // ⚠ AWAITED, not read synchronously. Every completion source here is built with
        // RunContinuationsAsynchronously (mandatory — an admin callback can fire inline on
        // the submitting thread), so an awaiter is only *scheduled* to fault when its
        // source faults. Reading IsFaulted immediately would be a race.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
    }

    /// <summary>
    /// Many operations of both kinds leave the reference count <b>balanced</b>: each
    /// completion releases exactly the one reference its submit took. An over-release would
    /// have thrown out of the <see cref="SafeHandle"/>; an under-release would leave
    /// <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public async Task ManyOperationsOfBothKinds_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int i = 0; i < 25; i++)
        {
            foreach (Rpc rpc in new[] { Rpc.ElectLeaders, Rpc.AlterPartitionReassignments })
            {
                IntPtr userData = IntPtr.Zero;
                Func<Task> outcome = SubmitCapturing(admin, rpc, captured => userData = captured);
                Complete(rpc, userData, MakeError(9, "balance probe"));
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            }

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "50 operations must leave the reference count balanced");
    }

    /// <summary>
    /// A submit that throws before native ran must not root the operation forever: the
    /// abandon path frees the <c>GCHandle</c> and releases the reference, so the very next
    /// <c>Dispose</c> still releases the handle. Both RPCs, since each has its own submit
    /// body — and <c>alterPartitionReassignments</c> additionally pins its own
    /// <c>int[]</c> replica arrays, which the same <c>finally</c> must unpin.
    /// </summary>
    [Theory]
    [InlineData(Rpc.ElectLeaders)]
    [InlineData(Rpc.AlterPartitionReassignments)]
    public void SubmitThatThrows_AbandonsTheOperationAndLeavesTheHandleReleasable(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Throws<InvalidOperationException>(() => SubmitThrowing(admin, rpc));

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "a submit that never reached native must not leave the operation's reference held");
    }

    /// <summary>
    /// The <c>GCHandle</c> is freed <b>exactly once</b>, including on the inline-callback
    /// path where the callback runs on the submitting thread before the entry point
    /// returns.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Both RPCs reach that path on ordinary bad input, not only on a NULL handle.</b>
    /// The header names an <c>election_type</c> that is neither 0 nor 1, and a non-cancelled
    /// entry with no target replicas, as inline triggers. The managed surface rejects both
    /// before the P/Invoke — the first by an
    /// <see cref="ArgumentOutOfRangeException"/>, the second because
    /// <see cref="NewPartitionReassignment"/>'s constructor will not build one — so the
    /// injected submit calls the production trampoline <em>synchronously</em>, which is
    /// exactly what the ABI does there. The assertions afterwards are made with <b>no await
    /// and no sleep</b>: the awaiter is already faulted and the client handle already
    /// releasable, which can only be true if the callback ran to completion inside the
    /// submit.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.ElectLeaders)]
    [InlineData(Rpc.AlterPartitionReassignments)]
    public void InlineCallback_FreesTheGcHandleExactlyOnce(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Func<Task> outcome = SubmitCapturing(
            admin, rpc, userData => Complete(rpc, userData, MakeError(11, "inline failure")));

        // No await: the callback has already run, on this thread, inside the submit.
        Assert.True(outcome().IsFaulted, "the callback must have fired inline, before the submit returned");

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "the inline callback must have freed the GCHandle and released the reference exactly once");
    }

    /// <summary>
    /// The callback's <c>error</c> parameter is <b>OWNED</b> — the trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/> and it must never be freed again.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Both P4 result types also carry a BORROWED per-partition error, so the two
    /// directions live side by side in this phase.</b> The borrowed one is covered by
    /// <see cref="AdminP4ResultMarshalTests"/>, over a real result root. What the loop here
    /// catches is a <em>second</em> free of the owned one, which aborts the run; the value
    /// asserted is the message surviving intact after the trampoline consumed the handle,
    /// across enough iterations that a freed-then-reused allocation would show up as a
    /// corrupted message.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.ElectLeaders)]
    [InlineData(Rpc.AlterPartitionReassignments)]
    public async Task TheCallbacksErrorParameter_IsOwned_AndConsumedExactlyOnce(Rpc rpc)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        for (int i = 0; i < 50; i++)
        {
            string message = $"owned error {i} — não ascii";
            Func<Task> outcome = SubmitCapturing(
                admin, rpc, userData => Complete(rpc, userData, MakeError(70 + i, message)));

            KafkaException failure =
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            Assert.Equal(70 + i, failure.Code);
            Assert.Equal(message, failure.Message);
        }
    }

    /// <summary>
    /// Each new trampoline is a <b>total no-throw boundary</b>: a managed exception raised
    /// inside it is absorbed rather than unwinding into Rust, which on the inline path has
    /// no caller frame willing to catch it (undefined behaviour).
    /// </summary>
    /// <remarks>
    /// The throw is induced where a test can reach it safely — recovering the
    /// per-operation context out of <c>user_data</c>, here a <c>GCHandle</c> over an object
    /// of the wrong type, so the cast throws <see cref="InvalidCastException"/> before
    /// anything else in the body runs. Inducing it later would mean handing the ABI a
    /// non-null pointer that is not a valid result root, which is undefined behaviour
    /// rather than a test.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.ElectLeaders)]
    [InlineData(Rpc.AlterPartitionReassignments)]
    public void EachTrampoline_IsATotalNoThrowBoundary(Rpc rpc)
    {
        // Deliberately the wrong type for both trampolines, so the context cast throws.
        GCHandle wrongType = GCHandle.Alloc("not an admin operation", GCHandleType.Normal);
        try
        {
            // The assertion is the absence of a throw: an escaping exception here is the
            // defect, and on the inline path it would be UB rather than a failed test.
            Complete(rpc, GCHandle.ToIntPtr(wrongType), MakeError(5, "absorbed"));
        }
        finally
        {
            wrongType.Free();
        }
    }

    /// <summary>Which RPC a parameterised case drives.</summary>
    public enum Rpc
    {
        /// <summary>
        /// Result shape 3 — ONE aggregate awaiter, whose map values are the per-partition
        /// outcomes.
        /// </summary>
        ElectLeaders,

        /// <summary>
        /// Result shape 2 — one awaiter PER partition, faulted by that partition's own
        /// borrowed error. Byte-identical ABI accessor set to
        /// <see cref="ElectLeaders"/>'.
        /// </summary>
        AlterPartitionReassignments,
    }

    /// <summary>
    /// Submits one RPC with the native call replaced by <paramref name="onSubmit"/>, and
    /// returns a closure over an awaiter that RPC's result exposes — so the two shapes can
    /// be asserted uniformly.
    /// </summary>
    internal static Func<Task> SubmitCapturing(NativeAdminClient admin, Rpc rpc, Action<IntPtr> onSubmit)
    {
        if (rpc == Rpc.ElectLeaders)
        {
            ElectLeadersResult result = admin.ElectLeaders(
                ElectionType.Preferred,
                new[] { new TopicPartition("p4-lifetime", 0) },
                options: null,
                (nativeHandle, electionType, allPartitions, topics, partitions, count, timeoutMs, callback,
                    userData) => onSubmit(userData));

            return result.Partitions;
        }

        AlterPartitionReassignmentsResult reassigned = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-lifetime", 0)] = new NewPartitionReassignment(new[] { 0 }),
            },
            options: null,
            (nativeHandle, topics, partitions, cancel, targetReplicas, targetReplicaCounts, count, timeoutMs,
                allowReplicationFactorChange, callback, userData) => onSubmit(userData));

        return reassigned.All;
    }

    private static void SubmitThrowing(NativeAdminClient admin, Rpc rpc)
    {
        if (rpc == Rpc.ElectLeaders)
        {
            admin.ElectLeaders(
                ElectionType.Unclean,
                new[] { new TopicPartition("p4-throwing", 0) },
                options: null,
                (nativeHandle, electionType, allPartitions, topics, partitions, count, timeoutMs, callback,
                    userData) => throw new InvalidOperationException("submit failed"));
            return;
        }

        admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-throwing", 0)] = new NewPartitionReassignment(new[] { 0, 1 }),
                [new TopicPartition("p4-throwing", 1)] = null,
            },
            options: null,
            (nativeHandle, topics, partitions, cancel, targetReplicas, targetReplicaCounts, count, timeoutMs,
                allowReplicationFactorChange, callback, userData) =>
                throw new InvalidOperationException("submit failed"));
    }

    /// <summary>
    /// Drives the <b>production</b> trampoline for one RPC with a top-level submit failure
    /// — the channel that means "the request could not be issued at all", as opposed to the
    /// per-partition outcomes inside a result.
    /// </summary>
    internal static void Complete(Rpc rpc, IntPtr userData, IntPtr error)
    {
        if (rpc == Rpc.ElectLeaders)
        {
            AdminCallbacks.ElectLeaders(IntPtr.Zero, error, userData);
            return;
        }

        AdminCallbacks.AlterPartitionReassignments(IntPtr.Zero, error, userData);
    }

    /// <summary>
    /// Builds an <b>owned</b> <c>kafka_common_KafkaError_t</c> to stand in for the one
    /// native would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    internal static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
