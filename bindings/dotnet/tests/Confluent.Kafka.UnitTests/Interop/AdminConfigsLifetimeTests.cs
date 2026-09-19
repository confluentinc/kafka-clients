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
/// The span-the-op reference contract and the inline-callback path for M15/P3 Stage 2's
/// two RPCs — the config twin of <see cref="AdminP3OperationLifetimeTests"/>.
/// <c>kafka_admin_AdminClient_destroy</c> is <b>not</b> ref-counted and does <b>not</b>
/// drain, so every new submit needs its own proof rather than inheriting an earlier one's.
/// </summary>
public sealed class AdminConfigsLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly ConfigResource s_resource =
        new ConfigResource(ConfigResourceType.Topic, "cfg-lifetime-topic");

    /// <summary>
    /// The three-way differential for both new submits. A single-case assertion cannot tell
    /// a working reference count from a permanently unbalanced one — both read "not
    /// released" — so each RPC gets all three cases.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeConfigs)]
    [InlineData(Rpc.IncrementalAlterConfigs)]
    public async Task DisposeRacingAnInFlightOperation_DefersTheNativeDestroy(Rpc rpc)
    {
        // ---- (1) Nothing in flight: Dispose releases immediately. ----
        NativeAdminClient baseline = NativeAdminClient.CreateMock(1);
        SafeAdminHandle baselineHandle = baseline.Handle;
        TestTimeout.Run(baseline.Dispose, s_deadline);
        Assert.True(baselineHandle.IsClosed, "with no operation in flight the native release must be immediate");

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
            foreach (Rpc rpc in new[] { Rpc.DescribeConfigs, Rpc.IncrementalAlterConfigs })
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
    /// A submit that throws before native ran must not root the operation forever.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeConfigs)]
    [InlineData(Rpc.IncrementalAlterConfigs)]
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
    /// ⚠⚠ <b>The REAL inline-callback path, reached by ordinary bad input.</b> An unknown
    /// <c>AlterConfigOp.OpType</c> code makes the ABI fire the completion <b>synchronously
    /// on the calling thread, before the entry point returns</b> — this entry point's own
    /// async doc extends the inline trigger set beyond a NULL handle to exactly this case.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>No injected submit here.</b> Every other inline-path test in this phase drives
    /// the production trampoline by hand; this one goes through the <em>real</em>
    /// <c>incremental_alter_configs_async</c>, which is the only way to prove the ABI
    /// really does take that path and that the binding survives it. An integration test
    /// against a broker would never reach it.
    /// </para>
    /// <para>
    /// Two consequences are asserted together: every key faults (the call failed as a
    /// whole, so there is no result table), and the <c>GCHandle</c> plus the span-the-op
    /// reference are released exactly once — which is what lets the subsequent
    /// <c>Dispose</c> close the handle. A leak would leave <c>IsClosed</c> false forever; a
    /// double free would abort the run.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task UnknownOpTypeCode_DrivesTheRealInlineCallbackPath()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                // 99 is not one of Java's four OpType ids (0/1/2/3).
                [s_resource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), (AlterConfigOpType)99) },
            },
            options: null);

        // The awaiter is already faulted with NO await and NO sleep, which can only be true
        // if the callback ran to completion inside the entry point.
        Assert.NotNull(result.Values[s_resource].Exception);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[s_resource]), s_deadline);
        Assert.Contains("op type", failure.Message, StringComparison.OrdinalIgnoreCase);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "the inline callback must have freed the GCHandle and released the reference exactly once");
    }

    /// <summary>
    /// ⚠⚠ <b>A submit failure faults EVERY awaitable — including a zero-op resource's.</b>
    /// The local completion of zero-op keys
    /// (<c>VoidKeyedAdminOperation.CompleteKeysWithNoRequest</c>) must not override that,
    /// which a comment alone cannot enforce (M15/P3 round 4, finding 69.7).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A resource mapped to an empty operation collection is completed <em>locally</em>,
    /// because the row-flattened request cannot carry it. It is placed on the successful
    /// completion path, and what matters is that it does not run <b>before</b> the failure
    /// branch. Java's failure path is explicit:
    /// <c>KafkaAdminClient.java:2922-2924</c>'s <c>handleFailure</c> calls
    /// <c>completeAllExceptionally(futures.values(), throwable)</c> — <em>all</em> of them,
    /// and the zero-op resource's future is among them because Java keys the map on the
    /// resource collection it sends (<c>:2893-2895</c>, <c>:2902</c>).
    /// </para>
    /// <para>
    /// ⚠ <b>Without this test the invariant is only a comment.</b> Completing those keys
    /// <em>before</em> the failure branch makes <c>TrySetResult</c> win and
    /// <c>FailAll</c>'s <c>TrySetException</c> a no-op — so a call that failed at submit
    /// reports <b>success</b> for the zero-op resource. That is a wrong answer, and the
    /// suite stays green everywhere else.
    /// </para>
    /// <para>
    /// ⚠ <b>What is pinned is the ORDER relative to <c>FailAll</c>, not the lexical
    /// position of the call — measured, not assumed.</b> Three placements were driven
    /// against this test, each on a build reporting <c>0 Error(s)</c>: moving the call into
    /// the <c>finally</c> beside <c>FailUncompleted</c> → <b>still green</b>; hoisting it
    /// out of the <c>if</c>/<c>else</c> but leaving it <em>after</em> → <b>still green</b>;
    /// hoisting it to <em>before</em> the failure branch → <b>RED</b>. The first two are
    /// unobservable because <c>FailAll</c> has already completed those keys by then, so
    /// <c>TrySetResult</c> cannot take effect. Of the three, the one that changes an answer
    /// is the one this catches.
    /// </para>
    /// <para>
    /// The non-zero-op resource is asserted alongside it as the control: if it did not fault
    /// too, the test would be pinning a broken failure path rather than this invariant.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task ASubmitFailure_FaultsEveryAwaitable_IncludingAZeroOpResource()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        ConfigResource withOps = new ConfigResource(ConfigResourceType.Topic, "cfg-fail-with-ops");
        ConfigResource zeroOps = new ConfigResource(ConfigResourceType.Topic, "cfg-fail-zero-ops");

        IntPtr userData = IntPtr.Zero;
        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [withOps] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
                [zeroOps] = Array.Empty<AlterConfigOp>(),
            },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, configNames, configValues, opTypes, count, timeoutMs,
                validateOnly, callback, captured) => userData = captured);

        // Only the one resource contributed a row; the other is the zero-op case.
        Assert.NotEqual(IntPtr.Zero, userData);

        // The submit-failure path: no result table, so the production trampoline faults
        // every requested key with this one owned error.
        AdminCallbacks.IncrementalAlterConfigs(IntPtr.Zero, MakeError(61, "submit failed"), userData);

        KafkaException carrying = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[withOps]), s_deadline);
        Assert.Equal(61, carrying.Code);

        // ⚠ THE ASSERTION THIS TEST EXISTS FOR.
        KafkaException empty = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[zeroOps]), s_deadline);
        Assert.Equal(61, empty.Code);
        Assert.Equal("submit failed", empty.Message);

        // …and All() therefore faults as well.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// The callback's <c>error</c> parameter is <b>OWNED</b> — the mirror image of the
    /// borrowed per-resource errors inside a result — so the trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/> and it must never be freed again.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeConfigs)]
    [InlineData(Rpc.IncrementalAlterConfigs)]
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
    /// Both new trampolines are <b>total no-throw boundaries</b>: a managed exception raised
    /// inside one is absorbed rather than unwinding into Rust.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeConfigs)]
    [InlineData(Rpc.IncrementalAlterConfigs)]
    public void EveryTrampoline_IsATotalNoThrowBoundary(Rpc rpc)
    {
        GCHandle wrongType = GCHandle.Alloc("not an admin operation", GCHandleType.Normal);
        try
        {
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
        /// <summary>Result shape 1 — a composite key with a per-resource value.</summary>
        DescribeConfigs,

        /// <summary>Result shape 2 — a composite key with a void per-resource future.</summary>
        IncrementalAlterConfigs,
    }

    private static Func<Task> SubmitCapturing(NativeAdminClient admin, Rpc rpc, Action<IntPtr> onSubmit)
    {
        if (rpc == Rpc.DescribeConfigs)
        {
            DescribeConfigsResult result = admin.DescribeConfigs(
                new[] { s_resource },
                options: null,
                (nativeHandle, resourceTypes, resourceNames, count, timeoutMs, synonyms, documentation, callback,
                    userData) => onSubmit(userData));
            return () => result.Values[s_resource];
        }

        AlterConfigsResult altered = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [s_resource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, configNames, configValues, opTypes, count, timeoutMs,
                validateOnly, callback, userData) => onSubmit(userData));
        return () => altered.Values[s_resource];
    }

    private static void SubmitThrowing(NativeAdminClient admin, Rpc rpc)
    {
        if (rpc == Rpc.DescribeConfigs)
        {
            admin.DescribeConfigs(
                new[] { s_resource },
                options: null,
                (nativeHandle, resourceTypes, resourceNames, count, timeoutMs, synonyms, documentation, callback,
                    userData) => throw new InvalidOperationException("submit failed"));
            return;
        }

        admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [s_resource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, configNames, configValues, opTypes, count, timeoutMs,
                validateOnly, callback, userData) => throw new InvalidOperationException("submit failed"));
    }

    /// <summary>
    /// Drives the <b>production</b> trampoline with a top-level submit failure — the path
    /// where there is no result table, so every requested key fails with that one error.
    /// </summary>
    private static void Complete(Rpc rpc, IntPtr userData, IntPtr error)
    {
        if (rpc == Rpc.DescribeConfigs)
        {
            AdminCallbacks.DescribeConfigs(IntPtr.Zero, error, userData);
        }
        else
        {
            AdminCallbacks.IncrementalAlterConfigs(IntPtr.Zero, error, userData);
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
