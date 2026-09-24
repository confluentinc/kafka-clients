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
/// The span-the-op reference contract for M15/P4 Stage 2's two RPCs — the Stage 2 twin of
/// <see cref="AdminP4OperationLifetimeTests"/>. <c>kafka_admin_AdminClient_destroy</c> is
/// <b>not</b> ref-counted and does <b>not</b> drain, so every new submit needs its own
/// proof rather than inheriting an earlier one's.
/// </summary>
public sealed class AdminP4Stage2OperationLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The topic <see cref="SubmitCapturing"/> submits and <see cref="Complete"/> settles.</summary>
    private const string LifetimeTopic = "p4s2-lifetime";

    /// <summary>
    /// The three-way differential for both new submits: nothing in flight →
    /// <c>Dispose</c> releases; one in flight → it does <b>not</b>; the operation completes
    /// → it then does.
    /// </summary>
    [Theory]
    [InlineData(Rpc.ListPartitionReassignments)]
    [InlineData(Rpc.ListOffsets)]
    public async Task DisposeRacingAnInFlightOperation_DefersTheNativeDestroy(Rpc rpc)
    {
        NativeAdminClient baseline = NativeAdminClient.CreateMock(1);
        SafeAdminHandle baselineHandle = baseline.Handle;
        TestTimeout.Run(baseline.Dispose, s_deadline);
        Assert.True(baselineHandle.IsClosed, "with nothing in flight the release must be immediate");

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        Func<Task> outcome = SubmitCapturing(admin, rpc, userData => capturedUserData = userData);
        Assert.NotEqual(IntPtr.Zero, capturedUserData);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(
            handle.IsClosed,
            "an in-flight operation must defer AdminClient_destroy — the ABI does not protect this itself");

        Complete(rpc, capturedUserData, 42, "submit failed");
        Assert.True(handle.IsClosed, "completing the in-flight operation must run the deferred release");

        // AWAITED, not read synchronously: every source uses RunContinuationsAsynchronously.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
    }

    /// <summary>
    /// Many operations of both kinds leave the reference count <b>balanced</b>.
    /// </summary>
    [Fact]
    public async Task ManyOperationsOfBothKinds_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int i = 0; i < 25; i++)
        {
            foreach (Rpc rpc in new[] { Rpc.ListPartitionReassignments, Rpc.ListOffsets })
            {
                IntPtr userData = IntPtr.Zero;
                Func<Task> outcome = SubmitCapturing(admin, rpc, captured => userData = captured);
                Complete(rpc, userData, 9, "balance probe");
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            }

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "50 operations must leave the reference count balanced");
    }

    /// <summary>
    /// A submit that throws before native ran must not root the operation forever.
    /// </summary>
    [Theory]
    [InlineData(Rpc.ListPartitionReassignments)]
    [InlineData(Rpc.ListOffsets)]
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
    /// The <c>GCHandle</c> is freed <b>exactly once</b> on the inline-callback path.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>listOffsets</c> reaches that path on ordinary bad input at the ABI — an unknown
    /// isolation level or an unrecognised sentinel — though the managed guards mean a C#
    /// caller cannot produce either (they are driven directly in
    /// <see cref="AdminP4Stage2ResultMarshalTests"/>). Here the production trampoline is
    /// called synchronously, which is exactly what the ABI does there; the assertions are
    /// made with <b>no await and no sleep</b>.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.ListPartitionReassignments)]
    [InlineData(Rpc.ListOffsets)]
    public void InlineCallback_FreesTheGcHandleExactlyOnce(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Func<Task> outcome = SubmitCapturing(
            admin,
            rpc,
            userData => Complete(rpc, userData, 11, "inline failure"));

        Assert.True(outcome().IsFaulted, "the callback must have fired inline, before the submit returned");

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "the inline callback must have freed the GCHandle and released the reference exactly once");
    }

    /// <summary>
    /// The callback's <c>error</c> parameter is <b>OWNED</b> and consumed exactly once.
    /// </summary>
    /// <remarks>
    /// ⚠ For <c>listPartitionReassignments</c> this is the <em>only</em> error channel that
    /// exists — its result declares no <c>get_error</c> — whereas <c>listOffsets</c> carries
    /// a second, <b>borrowed</b> one inside the result. Both directions live in this stage,
    /// which is why neither is assumed from the other.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.ListPartitionReassignments)]
    [InlineData(Rpc.ListOffsets)]
    public async Task TheCallbacksErrorParameter_IsOwned_AndConsumedExactlyOnce(Rpc rpc)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        for (int i = 0; i < 50; i++)
        {
            string message = $"owned error {i} — não ascii";
            Func<Task> outcome = SubmitCapturing(
                admin, rpc, userData => Complete(rpc, userData, 70 + i, message));

            KafkaException failure =
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            Assert.Equal(70 + i, failure.Code);
            Assert.Equal(message, failure.Message);
        }
    }

    /// <summary>
    /// Each new trampoline is a <b>total no-throw boundary</b>.
    /// </summary>
    [Theory]
    [InlineData(Rpc.ListPartitionReassignments)]
    [InlineData(Rpc.ListOffsets)]
    public void EachTrampoline_IsATotalNoThrowBoundary(Rpc rpc)
    {
        GCHandle wrongType = GCHandle.Alloc("not an admin operation", GCHandleType.Normal);
        try
        {
            // The assertion is the absence of a throw: an escaping exception here would be
            // UB on the inline path rather than a failed test.
            Complete(rpc, GCHandle.ToIntPtr(wrongType), 5, "absorbed");
        }
        finally
        {
            wrongType.Free();
        }
    }

    /// <summary>Which RPC a parameterised case drives.</summary>
    public enum Rpc
    {
        /// <summary>Result shape 3 — one aggregate awaiter, and no per-key error at all.</summary>
        ListPartitionReassignments,

        /// <summary>Result shape 4a — one awaiter per partition, each with its own callback.</summary>
        ListOffsets,
    }

    private static Func<Task> SubmitCapturing(NativeAdminClient admin, Rpc rpc, Action<IntPtr> onSubmit)
    {
        if (rpc == Rpc.ListPartitionReassignments)
        {
            ListPartitionReassignmentsResult result = admin.ListPartitionReassignments(
                new[] { new TopicPartition(LifetimeTopic, 0) },
                options: null,
                (nativeHandle, allPartitions, topics, partitions, count, timeoutMs, callback, userData) =>
                    onSubmit(userData));

            return result.Reassignments;
        }

        TopicPartition partition = new TopicPartition(LifetimeTopic, 0);
        ListOffsetsResult offsets = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [partition] = OffsetSpec.Latest() },
            options: null,
            (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs, isolationLevel,
                callback, userData) => onSubmit(userData));

        return () => offsets.PartitionResult(partition);
    }

    private static void SubmitThrowing(NativeAdminClient admin, Rpc rpc)
    {
        if (rpc == Rpc.ListPartitionReassignments)
        {
            admin.ListPartitionReassignments(
                new[] { new TopicPartition("p4s2-throwing", 0) },
                options: null,
                (nativeHandle, allPartitions, topics, partitions, count, timeoutMs, callback, userData) =>
                    throw new InvalidOperationException("submit failed"));
            return;
        }

        admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [new TopicPartition("p4s2-throwing", 0)] = OffsetSpec.Earliest(),
                [new TopicPartition("p4s2-throwing", 1)] = OffsetSpec.ForTimestamp(5),
            },
            options: null,
            (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs, isolationLevel,
                callback, userData) => throw new InvalidOperationException("submit failed"));
    }

    /// <summary>
    /// Settles one operation through the <b>production</b> trampoline. <c>listOffsets</c> is
    /// result shape 4a, so the failure arrives as one callback per key with a NULL value
    /// slot — hence a freshly minted owned error per key rather than one shared handle.
    /// </summary>
    private static void Complete(Rpc rpc, IntPtr userData, int code, string message)
    {
        if (rpc == Rpc.ListPartitionReassignments)
        {
            AdminCallbacks.ListPartitionReassignments(
                IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(code, message), userData);
            return;
        }

        // The one key SubmitCapturing submits; a shape-4a operation is settled only when
        // every key has fired, so this must match it.
        using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(LifetimeTopic);
        AdminCallbacks.ListOffsets(
            topic.Pointer, 0, IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(code, message), userData);
    }
}
