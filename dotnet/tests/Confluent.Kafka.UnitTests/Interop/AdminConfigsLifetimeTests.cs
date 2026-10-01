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
/// The span-the-op reference contract and the inline-callback path for M15/P3 Stage 2's
/// two RPCs — the config twin of <see cref="AdminP3OperationLifetimeTests"/>.
/// <c>kafka_admin_AdminClient_destroy</c> is <b>not</b> ref-counted and does <b>not</b>
/// drain, so every new submit needs its own proof rather than inheriting an earlier one's.
/// </summary>
public sealed class AdminConfigsLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>How long <see cref="DisposeAndAwaitRelease"/> waits for a dispatcher-side release.</summary>
    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    /// <summary><c>Errors::UnsupportedVersion</c>'s wire code, which the core's <c>unsupported_version</c> carries.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The core's rejection of an unknown op-type code at row 0 (<c>read_alter_config_ops</c>,
    /// <c>rust/src/ffi/admin.rs</c>), copied from an actual run rather than composed from the
    /// format string.
    /// </summary>
    private const string UnknownOpTypeMessage = "unknown AlterConfigOp op type id 99 at index 0";

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
    /// ⚠⚠ <b>The REAL inline-callback path, reached by an unknown op-type code.</b> An
    /// unknown <c>AlterConfigOp.OpType</c> code makes the ABI fire the completion
    /// <b>synchronously on the calling thread, before the entry point returns</b> — this
    /// entry point's own async doc extends the inline trigger set beyond a NULL handle to
    /// exactly this case.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>The code is planted at the submit seam, and the submit is still the REAL
    /// one.</b> Since M15/P13.4 G2-4 the public <see cref="AlterConfigOp"/> constructor
    /// rejects an undefined <see cref="AlterConfigOpType"/>, so ordinary input can no
    /// longer produce the code. The test therefore goes through the internal seam
    /// <c>IncrementalAlterConfigs(configs, options, NativeIncrementalAlterConfigsSubmit)</c>
    /// with a <em>valid</em> <see cref="AlterConfigOpType.Set"/> in the dictionary, and its
    /// lambda overwrites row 0's op type with <c>99</c> — not one of Java's four
    /// <c>OpType</c> ids (0/1/2/3) — and then forwards <b>every</b> argument, unchanged
    /// otherwise, to the real <c>incremental_alter_configs_async</c>
    /// (<see cref="NativeMethods.AdminClientIncrementalAlterConfigsAsync"/>). That is still
    /// the only way to prove the ABI really takes the inline path and that the binding
    /// survives it: the production trampoline, <c>GCHandle</c> and span-the-op reference are
    /// all the real ones, and an integration test against a broker would never reach it.
    /// </para>
    /// <para>
    /// Two consequences are asserted together: every key faults (the call failed as a
    /// whole, so there is no result table), and the <c>GCHandle</c> plus the span-the-op
    /// reference are released exactly once — which is what lets the subsequent
    /// <c>Dispose</c> close the handle. A leak would leave <c>IsClosed</c> false forever; a
    /// double free would abort the run.
    /// </para>
    /// <para>
    /// Vacuity guard, measured: with the lambda forwarding the op type unchanged (a valid
    /// <c>Set</c>), the "already faulted with no await" assertion fails — the mock answers
    /// asynchronously, so only the planted code makes the completion inline.
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
                [s_resource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, configNames, configValues, opTypes, count, timeoutMs,
                validateOnly, callback, userData) =>
            {
                // 99 is not one of Java's four OpType ids (0/1/2/3).
                opTypes[0] = 99;
                NativeMethods.AdminClientIncrementalAlterConfigsAsync(
                    nativeHandle, resourceTypes, resourceNames, configNames, configValues, opTypes, count, timeoutMs,
                    validateOnly, callback, userData);
            });

        // The awaiter is already faulted with NO await and NO sleep, which can only be true
        // if the callback ran to completion inside the entry point.
        Assert.NotNull(result.Values[s_resource].Exception);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[s_resource]), s_deadline);
        Assert.Equal(UnknownOpTypeMessage, failure.Message);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "the inline callback must have freed the GCHandle and released the reference exactly once");
    }

    /// <summary>
    /// ⚠⚠ <b>A submit failure faults EVERY resource, a zero-op one included</b> (M15/P13.2,
    /// finding F1) — Java's outcome, restored. This test used to pin the opposite: M15/P9
    /// CP6 completed a zero-op resource locally, so it resolved successfully while the named
    /// resources faulted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java keys its futures on the resource collection it sends and fails them all on a
    /// whole-call failure: <c>handleFailure</c> calls
    /// <c>completeAllExceptionally(futures.values(), throwable)</c>
    /// (<c>KafkaAdminClient.java:2889-2896</c>, <c>:2902</c>, <c>:2922-2924</c>). A zero-op
    /// resource is now <em>sent</em> — as one sentinel row, a NULL config name — so the ABI
    /// names it and answers it with its own callback, and the submit-failure fan-out reaches
    /// it like any other.
    /// </para>
    /// <para>
    /// Both callbacks are fired, with the same code and message, and both keys are asserted
    /// with both — the zero-op key is not special in any way the caller can see.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task ASubmitFailure_FaultsEveryResource_IncludingAZeroOpOne()
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

        Assert.NotEqual(IntPtr.Zero, userData);

        // The submit-failure path, fanned out per resource: two resources, two callbacks.
        foreach (ConfigResource resource in new[] { withOps, zeroOps })
        {
            using Utf8Marshal.PinnedUtf8String named = Utf8Marshal.Pin(resource.Name);
            AdminCallbacks.IncrementalAlterConfigs(
                (int)resource.Type, named.Pointer, MakeError(61, "submit failed"), userData);
        }

        foreach (ConfigResource resource in new[] { withOps, zeroOps })
        {
            KafkaException carrying = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => result.Values[resource]), s_deadline);
            Assert.Equal(61, carrying.Code);
            Assert.Equal("submit failed", carrying.Message);
        }

        KafkaException all = await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Equal(61, all.Code);
        Assert.Equal("submit failed", all.Message);
    }

    /// <summary>
    /// ⚠ <b>An undefined resource type and <see cref="ConfigResourceType.Unknown"/> are one
    /// resource end to end</b> (M15/P13.2, finding G2-1): through the <em>real</em> ABI,
    /// both config RPCs settle, fault with the mock's own answer for an <c>UNKNOWN</c>
    /// resource, and release the operation and the client exactly once.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Before G2-1 the <c>describeConfigs</c> half hung: the binding kept the undefined type
    /// as a second key, the ABI folded both to <c>UNKNOWN</c>, collapsed them and answered
    /// once, and the countdown waited for an answer that never came — rooting the
    /// <c>GCHandle</c> and the client reference forever. <c>IsClosed</c> after
    /// <c>Dispose</c> is what proves both were released.
    /// </para>
    /// <para>
    /// The <c>incrementalAlterConfigs</c> half goes through a map, where the two keys are
    /// one entry as soon as they are equal; it is sent with no ops, so it also exercises
    /// F1's sentinel row against the real ABI. Both answers are the core's
    /// <c>unsupported_version("Not implemented yet")</c> for a type it has no arm for
    /// (<c>mock_admin_client.rs:599</c>, <c>:656</c>).
    /// </para>
    /// </remarks>
    [Fact]
    public async Task AnUndefinedTypeCollidingWithUnknown_SettlesAndReleases_ThroughTheRealAbi()
    {
        ConfigResource undefined = new ConfigResource((ConfigResourceType)64, "x");
        ConfigResource unknown = new ConfigResource(ConfigResourceType.Unknown, "x");

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        DescribeConfigsResult described = admin.DescribeConfigs(new[] { undefined, unknown }, options: null);
        Assert.Single(described.Values);

        KafkaException describeFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => described.Values[unknown]), s_deadline);
        Assert.Equal(UnsupportedVersionCode, describeFailure.Code);
        Assert.Equal("Not implemented yet", describeFailure.Message);

        AlterConfigsResult altered = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [undefined] = Array.Empty<AlterConfigOp>(),
                [unknown] = Array.Empty<AlterConfigOp>(),
            },
            options: null);
        Assert.Single(altered.Values);

        KafkaException alterFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => altered.Values[undefined]), s_deadline);
        Assert.Equal(UnsupportedVersionCode, alterFailure.Code);
        Assert.Equal("Not implemented yet", alterFailure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "both operations must have released their GCHandle and client reference");
    }

    /// <summary>
    /// ⚠ <b>Two equal resources in a map that holds them as two keys are ONE resource, and
    /// the operation is released</b> (M15/P13.2, Critic 85 finding 85.1) — through the
    /// <em>real</em> ABI, for every mix of zero-op and non-empty op collections.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The <c>configs</c> parameter is a public <see cref="IReadOnlyDictionary{TKey, TValue}"/>,
    /// so its comparer is the caller's, not <see cref="ConfigResource"/>'s value equality. A
    /// reference comparer keeps two equal resources as two entries. The ABI then answers
    /// <b>once</b>: it fires "once per distinct resource named across the input rows", which
    /// <c>distinct_config_resources</c> computes on <c>(for_id(type), name)</c>. The bridge
    /// holds <b>one</b> <see cref="Task"/>, keyed by value equality. A countdown armed with the
    /// caller map's key count (2) never reaches zero, so the <c>GCHandle</c> and the
    /// span-the-op client reference stay held for the process lifetime. The caller's
    /// <see cref="Task"/> still completes, so nothing hangs; the only symptom is the leak, and
    /// <see cref="SafeHandle.IsClosed"/> after <c>Dispose</c> is what observes it.
    /// </para>
    /// <para>
    /// Java cannot leak here: it keys its futures on a value-equality <c>HashMap</c>
    /// (<c>KafkaAdminClient.java:2893-2895</c>) and returns one future per distinct resource
    /// (<c>:2886</c>). A <see cref="ConfigResourceType.Group"/> resource is used because the
    /// mock applies ops to a group it has never seen and answers success
    /// (<c>handle_incremental_resource_alteration</c>'s <c>Group</c> arm), so the entry settles
    /// successfully and there is no error message to assert.
    /// </para>
    /// </remarks>
    /// <param name="row">Which op collections the two equal keys carry.</param>
    [Theory]
    [InlineData(CollisionRow.ZeroOpsAndZeroOps)]
    [InlineData(CollisionRow.OpsAndZeroOps)]
    [InlineData(CollisionRow.OpsAndOps)]
    public async Task EqualResourcesInANonValueEqualityMap_AreOneResource_AndReleaseTheOperation(
        CollisionRow row)
    {
        ConfigResource first = new ConfigResource(ConfigResourceType.Group, "cfg-collision-group");
        ConfigResource second = new ConfigResource(ConfigResourceType.Group, "cfg-collision-group");
        Assert.Equal(first, second);
        Assert.NotSame(first, second);

        IReadOnlyCollection<AlterConfigOp> ops =
            new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) };
        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs =
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(ReferenceComparer.Instance)
            {
                [first] = row == CollisionRow.ZeroOpsAndZeroOps ? Array.Empty<AlterConfigOp>() : ops,
                [second] = row == CollisionRow.OpsAndOps ? ops : Array.Empty<AlterConfigOp>(),
            };
        Assert.Equal(2, configs.Count);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        AlterConfigsResult result = admin.IncrementalAlterConfigs(configs, options: null);
        Assert.Single(result.Values);

        await TestTimeout.Run(() => result.Values[first], s_deadline);
        await TestTimeout.Run(result.All, s_deadline);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must be armed with the distinct resources the ABI answers, not the caller map's key count");
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

    /// <summary>Which op collections the two equal keys of a collision case carry.</summary>
    public enum CollisionRow
    {
        /// <summary>Both keys map to an empty collection — two sentinel rows.</summary>
        ZeroOpsAndZeroOps,

        /// <summary>The first key has one op; the second maps to an empty collection.</summary>
        OpsAndZeroOps,

        /// <summary>Both keys have one op.</summary>
        OpsAndOps,
    }

    /// <summary>
    /// A comparer that is <b>not</b> value equality: two equal <see cref="ConfigResource"/>
    /// instances stay two keys. <c>System.Collections.Generic.ReferenceEqualityComparer</c> is
    /// .NET 5+, and this project also targets net462, so it is spelled out here.
    /// </summary>
    private sealed class ReferenceComparer : IEqualityComparer<ConfigResource>
    {
        internal static readonly ReferenceComparer Instance = new ReferenceComparer();

        public bool Equals(ConfigResource? x, ConfigResource? y) => ReferenceEquals(x, y);

        public int GetHashCode(ConfigResource obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>
    /// Disposes the client, then waits — bounded — for the native release, for a test whose
    /// callbacks the <em>real</em> ABI fired on its dispatcher thread.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Not an immediate <see cref="SafeHandle.IsClosed"/> check.</b> A per-key
    /// trampoline resolves its key's <see cref="Task"/> first and releases the operation in
    /// its <c>finally</c> after that, and the awaiter resumes on the pool
    /// (<see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>). So a test can
    /// observe the outcome and reach <c>Dispose</c> while the dispatcher still holds the
    /// operation's reference; the deferred destroy then runs on the dispatcher a moment
    /// later. A temporary probe measured exactly that for a correctly released operation
    /// (closed only after the wait). A leak never releases, so the bound only decides how
    /// long a red takes to report. Tests that fire the trampoline on the test thread, or
    /// whose callback runs inline inside the submit, need no wait and do not use this.
    /// </remarks>
    /// <returns>Whether the handle closed within the bound.</returns>
    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
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
    /// Drives the <b>production</b> trampoline with a submit failure. Both RPCs are keyed
    /// by the resource — a composite <c>(type id, name)</c> scalar pair — with
    /// <c>describeConfigs</c> (shape 4a) also carrying a NULL <c>value</c> slot and
    /// <c>incrementalAlterConfigs</c> (shape 4b) carrying none.
    /// </summary>
    private static void Complete(Rpc rpc, IntPtr userData, IntPtr error)
    {
        using Utf8Marshal.PinnedUtf8String pinnedName = Utf8Marshal.Pin(s_resource.Name);
        if (rpc == Rpc.DescribeConfigs)
        {
            AdminCallbacks.DescribeConfigs(
                (int)s_resource.Type, pinnedName.Pointer, IntPtr.Zero, error, userData);
        }
        else
        {
            AdminCallbacks.IncrementalAlterConfigs(
                (int)s_resource.Type, pinnedName.Pointer, error, userData);
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
