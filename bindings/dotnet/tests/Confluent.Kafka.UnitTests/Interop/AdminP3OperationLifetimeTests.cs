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
/// The span-the-op reference contract for M15/P3 Stage 1's three RPCs — the P3 twin of
/// <see cref="AdminP2bOperationLifetimeTests"/>. <c>kafka_admin_AdminClient_destroy</c> is
/// <b>not</b> ref-counted and does <b>not</b> drain, so every new submit needs its own
/// proof rather than inheriting an earlier one's.
/// </summary>
/// <remarks>
/// The native call is injected for the same reason as in the P1, P2a and P2b twins: "an
/// operation is in flight" has to be a fact the test controls, not a race it hopes to win.
/// Everything under test — the <c>DangerousAddRef</c>, the <c>GCHandle</c>, the
/// trampoline, the release in <c>FreeGcHandle</c> — is production code; only the clock is
/// the test's.
/// </remarks>
public sealed class AdminP3OperationLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The three-way differential for all three new submits. A single-case assertion
    /// cannot tell a working reference count from a permanently unbalanced one — both read
    /// "not released" — so each RPC gets all three cases: nothing in flight →
    /// <c>Dispose</c> releases; one in flight → it does <b>not</b>; the operation
    /// completes → it then does.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeCluster)]
    [InlineData(Rpc.ListConfigResources)]
    [InlineData(Rpc.ListClientMetricsResources)]
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
        // the submitting thread), so a projection derived from one is only *scheduled* to
        // fault when the source faults. Reading IsFaulted immediately would be a race the
        // test would sometimes win and sometimes lose.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
    }

    /// <summary>
    /// Many operations of all three kinds leave the reference count <b>balanced</b>: each
    /// completion releases exactly the one reference its submit took. An over-release would
    /// have thrown out of the <see cref="System.Runtime.InteropServices.SafeHandle"/>; an
    /// under-release would leave <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public async Task ManyOperationsOfAllThreeKinds_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int i = 0; i < 25; i++)
        {
            foreach (Rpc rpc in new[] { Rpc.DescribeCluster, Rpc.ListConfigResources, Rpc.ListClientMetricsResources })
            {
                IntPtr userData = IntPtr.Zero;
                Func<Task> outcome = SubmitCapturing(admin, rpc, captured => userData = captured);
                Complete(rpc, userData, MakeError(9, "balance probe"));
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            }

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "75 operations must leave the reference count balanced");
    }

    /// <summary>
    /// A submit that throws before native ran must not root the operation forever: the
    /// abandon path frees the <c>GCHandle</c> and releases the reference, so the very next
    /// <c>Dispose</c> still releases the handle. All three RPCs, since each has its own
    /// submit body.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeCluster)]
    [InlineData(Rpc.ListConfigResources)]
    [InlineData(Rpc.ListClientMetricsResources)]
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
    /// The injected submit calls the production trampoline <em>synchronously</em>, which is
    /// exactly what the ABI does when an RPC cannot be submitted at all. The assertions
    /// afterwards are made with <b>no await and no sleep</b>: the awaiter is already
    /// faulted and the client handle already releasable, which can only be true if the
    /// callback ran to completion inside the submit. A leaked <c>GCHandle</c> or an
    /// unreleased reference would leave <c>IsClosed</c> false forever; a double free would
    /// abort the run.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.DescribeCluster)]
    [InlineData(Rpc.ListConfigResources)]
    [InlineData(Rpc.ListClientMetricsResources)]
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
    /// The callback's <c>error</c> parameter is <b>OWNED</b>, and for these three RPCs it is
    /// the <em>only</em> error channel that exists — so the trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/> and it must never be freed again.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Stage 1 is the inverse of every earlier admin phase.</b> The keyed RPCs carry a
    /// <em>borrowed</em> per-key error inside the result, where
    /// <see cref="KafkaException.FromBorrowedHandle"/> is mandatory; none of these three
    /// result types declares a <c>get_error</c> at all, so the reflex to reach for the
    /// borrowed form here would leak this handle on every failed call. What the loop catches
    /// is the opposite mistake, a <em>second</em> free, which aborts the run; the value
    /// asserted is the message surviving intact after the trampoline consumed the handle,
    /// across enough iterations that a freed-then-reused allocation would show up as a
    /// corrupted message.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.DescribeCluster)]
    [InlineData(Rpc.ListConfigResources)]
    [InlineData(Rpc.ListClientMetricsResources)]
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
    /// Every new trampoline is a <b>total no-throw boundary</b>: a managed exception raised
    /// inside it is absorbed rather than unwinding into Rust, which on the inline path has
    /// no caller frame willing to catch it (undefined behaviour).
    /// </summary>
    /// <remarks>
    /// The throw is induced at the one place a test can reach safely — recovering the
    /// per-operation context out of <c>user_data</c>, here a <c>GCHandle</c> over an object
    /// of the wrong type, so the cast throws <see cref="System.InvalidCastException"/>
    /// before anything else in the body runs. Inducing it later would mean handing the ABI
    /// a non-null pointer that is not a valid result root, which is undefined behaviour
    /// rather than a test.
    /// </remarks>
    [Theory]
    [InlineData(Rpc.DescribeCluster)]
    [InlineData(Rpc.ListConfigResources)]
    [InlineData(Rpc.ListClientMetricsResources)]
    public void EveryTrampoline_IsATotalNoThrowBoundary(Rpc rpc)
    {
        // Deliberately the wrong type for every trampoline, so the context cast throws.
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
        /// <summary>Result shape 5 — one completion over four cluster attributes.</summary>
        DescribeCluster,

        /// <summary>Result sub-shape 3b — one aggregate awaiter over a collection.</summary>
        ListConfigResources,

        /// <summary>Result sub-shape 3b, with no input array at all.</summary>
        ListClientMetricsResources,
    }

    /// <summary>
    /// Submits one RPC with the native call replaced by <paramref name="onSubmit"/>, and
    /// returns a closure over an awaiter that RPC's result exposes — so the three shapes can
    /// be asserted uniformly.
    /// </summary>
    private static Func<Task> SubmitCapturing(NativeAdminClient admin, Rpc rpc, Action<IntPtr> onSubmit)
    {
        switch (rpc)
        {
            case Rpc.DescribeCluster:
                {
                    DescribeClusterResult result = admin.DescribeCluster(
                        options: null,
                        (nativeHandle, timeoutMs, includeAuthorizedOperations, includeFencedBrokers, callback,
                            userData) => onSubmit(userData));

                    // Any one of the four projections carries the call's failure; Nodes is
                    // arbitrary. The "all four fault together" claim is asserted in
                    // AdminP3ResultMarshalTests.
                    return result.Nodes;
                }

            case Rpc.ListConfigResources:
                {
                    ListConfigResourcesResult result = admin.ListConfigResources(
                        new[] { ConfigResourceType.Topic },
                        options: null,
                        (nativeHandle, resourceTypes, count, timeoutMs, callback, userData) => onSubmit(userData));
                    return result.All;
                }

            default:
                {
#pragma warning disable CS0618 // Java deprecates this RPC; mirrored, not avoided.
                    ListClientMetricsResourcesResult result = admin.ListClientMetricsResources(
                        options: null,
                        (nativeHandle, timeoutMs, callback, userData) => onSubmit(userData));
                    return result.All;
#pragma warning restore CS0618
                }
        }
    }

    private static void SubmitThrowing(NativeAdminClient admin, Rpc rpc)
    {
        switch (rpc)
        {
            case Rpc.DescribeCluster:
                admin.DescribeCluster(
                    options: null,
                    (nativeHandle, timeoutMs, includeAuthorizedOperations, includeFencedBrokers, callback,
                        userData) => throw new InvalidOperationException("submit failed"));
                break;

            case Rpc.ListConfigResources:
                admin.ListConfigResources(
                    new[] { ConfigResourceType.Topic },
                    options: null,
                    (nativeHandle, resourceTypes, count, timeoutMs, callback, userData) =>
                        throw new InvalidOperationException("submit failed"));
                break;

            default:
#pragma warning disable CS0618 // Java deprecates this RPC; mirrored, not avoided.
                admin.ListClientMetricsResources(
                    options: null,
                    (nativeHandle, timeoutMs, callback, userData) =>
                        throw new InvalidOperationException("submit failed"));
#pragma warning restore CS0618
                break;
        }
    }

    /// <summary>
    /// Drives the <b>production</b> trampoline for one RPC with a top-level submit failure
    /// — for these three shapes that is the only failure channel there is.
    /// </summary>
    private static void Complete(Rpc rpc, IntPtr userData, IntPtr error)
    {
        switch (rpc)
        {
            case Rpc.DescribeCluster:
                AdminCallbacks.DescribeCluster(IntPtr.Zero, error, userData);
                break;

            case Rpc.ListConfigResources:
                AdminCallbacks.ListConfigResources(IntPtr.Zero, error, userData);
                break;

            default:
                AdminCallbacks.ListClientMetricsResources(IntPtr.Zero, error, userData);
                break;
        }
    }

    /// <summary>
    /// Builds an <b>owned</b> <c>kafka_common_KafkaError_t</c> to stand in for the one
    /// native would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
