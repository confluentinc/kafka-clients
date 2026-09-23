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
/// The span-the-op reference contract and the entry-point selection for M15/P2a's two
/// RPCs — the P2a twin of <see cref="AdminOperationLifetimeTests"/>.
/// <c>kafka_admin_AdminClient_destroy</c> is <b>not</b> ref-counted and does <b>not</b>
/// drain, so every new submit needs its own proof rather than inheriting
/// <c>createTopics</c>'.
/// </summary>
/// <remarks>
/// The native call is injected for the same reason as in
/// <see cref="AdminOperationLifetimeTests"/>: "an operation is in flight" has to be a fact
/// the test controls, not a race it hopes to win. Everything under test — the
/// <c>DangerousAddRef</c>, the <c>GCHandle</c>, the trampoline, the release in
/// <c>FreeGcHandle</c> — is production code; only the clock is the test's.
/// </remarks>
public sealed class AdminP2aOperationLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "p2a-lifetime-topic";

    /// <summary>
    /// The three-way differential for <c>deleteTopics</c>. A single-case assertion cannot
    /// tell a working reference count from a permanently unbalanced one — both read "not
    /// released" — so all three cases are required.
    /// </summary>
    [Fact]
    public void DeleteTopics_DisposeRacingAnInFlightOperation_DefersTheNativeDestroy()
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
        DeleteTopicsResult result = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retryOnQuotaViolation, callback, userData) =>
                capturedUserData = userData,
            UnusedDeleteSubmit);

        Assert.NotEqual(IntPtr.Zero, capturedUserData);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(
            handle.IsClosed,
            "an in-flight operation must defer AdminClient_destroy — the ABI does not protect this itself");

        // ---- (3) Completing the operation releases it. ----
        AdminCallbacks.DeleteTopicsByName(IntPtr.Zero, MakeError(42, "submit failed"), capturedUserData);

        Assert.True(handle.IsClosed, "completing the in-flight operation must run the deferred release");
        Assert.NotNull(result.TopicNameValues![Topic].Exception);
    }

    /// <summary>
    /// The same three-way differential for <c>describeTopics</c>, whose submit is separate
    /// code and therefore needs its own proof.
    /// </summary>
    [Fact]
    public void DescribeTopics_DisposeRacingAnInFlightOperation_DefersTheNativeDestroy()
    {
        NativeAdminClient baseline = NativeAdminClient.CreateMock(1);
        SafeAdminHandle baselineHandle = baseline.Handle;
        TestTimeout.Run(baseline.Dispose, s_deadline);
        Assert.True(baselineHandle.IsClosed);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        DescribeTopicsResult result = admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, includeAuthorized, limit, callback, userData) =>
                capturedUserData = userData,
            UnusedDescribeSubmit);

        Assert.NotEqual(IntPtr.Zero, capturedUserData);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed);

        AdminCallbacks.DescribeTopicsByName(IntPtr.Zero, MakeError(42, "submit failed"), capturedUserData);

        Assert.True(handle.IsClosed);
        Assert.NotNull(result.TopicNameValues![Topic].Exception);
    }

    /// <summary>
    /// ⚠ <b>Prove the selection, not just the outcome.</b> The two forms of each RPC share
    /// one result type, so a test that only checks the returned keys passes whichever
    /// entry point ran. These assert which of the two submits was actually invoked — the
    /// only thing that distinguishes <c>delete_topics</c> from
    /// <c>delete_topics_by_ids</c> — and that the key travels in the form that entry point
    /// expects: a raw name, or Java's <c>Uuid.toString()</c> base64.
    /// </summary>
    [Fact]
    public void EntryPointSelection_FollowsTheCollectionsRuntimeType()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Uuid id = new Uuid(1L, 2L);

        // ---- deleteTopics, by name ----
        string? byNameKey = null;
        bool byIdsCalled = false;
        admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
            {
                byNameKey = Utf8Marshal.PtrToString(keys[0]);
                AdminCallbacks.DeleteTopicsByName(IntPtr.Zero, MakeError(1, "done"), userData);
            },
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => byIdsCalled = true);

        Assert.Equal(Topic, byNameKey);
        Assert.False(byIdsCalled, "a name collection must not drive the by-id entry point");

        // ---- deleteTopics, by id ----
        string? byIdKey = null;
        bool byNameCalled = false;
        admin.DeleteTopics(
            TopicCollection.OfTopicIds(new[] { id }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => byNameCalled = true,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
            {
                byIdKey = Utf8Marshal.PtrToString(keys[0]);
                AdminCallbacks.DeleteTopicsById(IntPtr.Zero, MakeError(1, "done"), userData);
            });

        Assert.False(byNameCalled, "an id collection must not drive the by-name entry point");

        // The key crossed as Java's base64 text, not as raw bytes or a decimal rendering.
        Assert.Equal(id.ToString(), byIdKey);
        Assert.Equal(id, Uuid.Parse(byIdKey!));

        // ---- describeTopics, both forms ----
        string? describeByNameKey = null;
        bool describeByIdsCalled = false;
        admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, includeAuthorized, limit, callback, userData) =>
            {
                describeByNameKey = Utf8Marshal.PtrToString(keys[0]);
                AdminCallbacks.DescribeTopicsByName(IntPtr.Zero, MakeError(1, "done"), userData);
            },
            (nativeHandle, keys, count, timeoutMs, includeAuthorized, limit, callback, userData) =>
                describeByIdsCalled = true);

        Assert.Equal(Topic, describeByNameKey);
        Assert.False(describeByIdsCalled);

        string? describeByIdKey = null;
        bool describeByNameCalled = false;
        admin.DescribeTopics(
            TopicCollection.OfTopicIds(new[] { id }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, includeAuthorized, limit, callback, userData) =>
                describeByNameCalled = true,
            (nativeHandle, keys, count, timeoutMs, includeAuthorized, limit, callback, userData) =>
            {
                describeByIdKey = Utf8Marshal.PtrToString(keys[0]);
                AdminCallbacks.DescribeTopicsById(IntPtr.Zero, MakeError(1, "done"), userData);
            });

        Assert.False(describeByNameCalled);
        Assert.Equal(id.ToString(), describeByIdKey);
    }

    /// <summary>
    /// The options are destructured onto the ABI's scalar parameters faithfully — the ABI
    /// has no options handle, so a swapped or dropped flag is invisible above this layer.
    /// </summary>
    [Fact]
    public void Options_AreDestructuredOntoTheAbiParameters()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        int deleteTimeout = 0;
        bool deleteRetry = true;
        admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            new DeleteTopicsOptions { TimeoutMs = 1234, RetryOnQuotaViolation = false },
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
            {
                deleteTimeout = timeoutMs;
                deleteRetry = retry;
                AdminCallbacks.DeleteTopicsByName(IntPtr.Zero, MakeError(1, "done"), userData);
            },
            UnusedDeleteSubmit);

        Assert.Equal(1234, deleteTimeout);
        Assert.False(deleteRetry);

        // Null options must reproduce Java's defaults exactly: unset timeout (a NEGATIVE,
        // which the ABI reads as "use the client default" — not 0, which would mean a
        // zero timeout) and retryOnQuotaViolation = true.
        int defaultedTimeout = 0;
        bool defaultedRetry = false;
        admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
            {
                defaultedTimeout = timeoutMs;
                defaultedRetry = retry;
                AdminCallbacks.DeleteTopicsByName(IntPtr.Zero, MakeError(1, "done"), userData);
            },
            UnusedDeleteSubmit);

        Assert.True(defaultedTimeout < 0, "an unset timeout must cross as negative, not as zero");
        Assert.True(defaultedRetry);

        int describeTimeout = 0;
        bool includeAuthorized = false;
        int limit = 0;
        admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            new DescribeTopicsOptions
            {
                TimeoutMs = 4321,
                IncludeAuthorizedOperations = true,
                PartitionSizeLimitPerResponse = 7,
            },
            (nativeHandle, keys, count, timeoutMs, include, partitionLimit, callback, userData) =>
            {
                describeTimeout = timeoutMs;
                includeAuthorized = include;
                limit = partitionLimit;
                AdminCallbacks.DescribeTopicsByName(IntPtr.Zero, MakeError(1, "done"), userData);
            },
            UnusedDescribeSubmit);

        Assert.Equal(4321, describeTimeout);
        Assert.True(includeAuthorized);
        Assert.Equal(7, limit);

        // Null options => Java's own 2000 default, not 0 and not the ABI's "unset".
        int defaultedLimit = 0;
        admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, include, partitionLimit, callback, userData) =>
            {
                defaultedLimit = partitionLimit;
                AdminCallbacks.DescribeTopicsByName(IntPtr.Zero, MakeError(1, "done"), userData);
            },
            UnusedDescribeSubmit);

        Assert.Equal(2000, defaultedLimit);
    }

    /// <summary>
    /// ⚠ <b>The INLINE callback path, reachable for the first time in M15/P2a.</b> The
    /// header says a by-id entry point fires its callback <em>"synchronously on the calling
    /// thread, before this function returns"</em> when the base64 topic id cannot be
    /// parsed. M15/P1 built for that path but had no entry point that could reach it —
    /// <c>createTopics</c> parses nothing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A malformed id cannot be produced through the public API — <see cref="Uuid"/> always
    /// renders valid base64 — which is itself the right outcome. So the test injects a
    /// submit that calls the <b>real</b> ABI with a deliberately unparseable key, leaving
    /// the whole managed path (rooting, span-the-op reference, production trampoline,
    /// <c>FreeGcHandle</c>) exactly as production runs it.
    /// </para>
    /// <para>
    /// The three assertions after the call are made with <b>no await and no sleep</b>, and
    /// that is the point: the awaiter is already faulted and the client handle already
    /// releasable, which can only be true if the callback ran to completion before the
    /// entry point returned. A leaked <c>GCHandle</c> or an unreleased reference would
    /// leave <c>IsClosed</c> false forever; a double free would abort the run.
    /// </para>
    /// </remarks>
    [Fact]
    public void MalformedTopicId_FaultsCleanlyThroughTheInlineCallbackPath()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Uuid placeholder = new Uuid(9L, 9L);

        DeleteTopicsResult result = admin.DeleteTopics(
            TopicCollection.OfTopicIds(new[] { placeholder }),
            options: null,
            UnusedDeleteSubmit,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
            {
                // Replace the well-formed base64 with something Uuid.fromString rejects,
                // then call the REAL entry point.
                using Utf8Marshal.PinnedUtf8String malformed = Utf8Marshal.Pin("!!! not base64 !!!");
                NativeMethods.AdminClientDeleteTopicsByIdsAsync(
                    nativeHandle,
                    new[] { malformed.Pointer },
                    1,
                    timeoutMs,
                    retry,
                    callback,
                    userData);
            });

        // No await: the callback has already run, on this thread, inside the call above.
        Assert.True(
            result.TopicIdValues![placeholder].IsFaulted,
            "the callback must have fired inline, before the entry point returned");

        KafkaException failure = Assert.IsType<KafkaException>(
            result.TopicIdValues![placeholder].Exception!.InnerException);
        Assert.Contains("!!! not base64 !!!", failure.Message, StringComparison.Ordinal);

        // The GCHandle was freed and the span-the-op reference released by that same
        // inline callback — so the very next Dispose releases the handle.
        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "the inline callback must have released the span-the-op reference exactly once");
    }

    /// <summary>
    /// Many operations of both kinds leave the reference count <b>balanced</b>: each
    /// completion releases exactly the one reference its submit took. An over-release would
    /// have thrown out of the <see cref="SafeHandle"/>; an under-release would leave
    /// <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public void ManyOperationsOfBothKinds_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int i = 0; i < 25; i++)
        {
            IntPtr deleteUserData = IntPtr.Zero;
            DeleteTopicsResult deleted = admin.DeleteTopics(
                TopicCollection.OfTopicNames(new[] { $"{Topic}-{i}" }),
                options: null,
                (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
                    deleteUserData = userData,
                UnusedDeleteSubmit);
            AdminCallbacks.DeleteTopicsByName(IntPtr.Zero, MakeError(9, "balance probe"), deleteUserData);
            Assert.NotNull(deleted.TopicNameValues![$"{Topic}-{i}"].Exception);

            IntPtr describeUserData = IntPtr.Zero;
            DescribeTopicsResult described = admin.DescribeTopics(
                TopicCollection.OfTopicIds(new[] { new Uuid(i, i) }),
                options: null,
                UnusedDescribeSubmit,
                (nativeHandle, keys, count, timeoutMs, include, limit, callback, userData) =>
                    describeUserData = userData);
            AdminCallbacks.DescribeTopicsById(IntPtr.Zero, MakeError(9, "balance probe"), describeUserData);
            Assert.NotNull(described.TopicIdValues![new Uuid(i, i)].Exception);

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "50 operations must leave the reference count balanced");
    }

    /// <summary>
    /// A submit that throws before native ran must not root the operation forever: the
    /// abandon path frees the <c>GCHandle</c> and releases the reference, so the very next
    /// <c>Dispose</c> still releases the handle. Both RPCs, since each has its own submit.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SubmitThatThrows_AbandonsTheOperationAndLeavesTheHandleReleasable(bool delete)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Throws<InvalidOperationException>(() =>
        {
            if (delete)
            {
                admin.DeleteTopics(
                    TopicCollection.OfTopicNames(new[] { Topic }),
                    options: null,
                    (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
                        throw new InvalidOperationException("submit failed"),
                    UnusedDeleteSubmit);
            }
            else
            {
                admin.DescribeTopics(
                    TopicCollection.OfTopicNames(new[] { Topic }),
                    options: null,
                    (nativeHandle, keys, count, timeoutMs, include, limit, callback, userData) =>
                        throw new InvalidOperationException("submit failed"),
                    UnusedDescribeSubmit);
            }
        });

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "a submit that never reached native must not leave the operation's reference held");
    }

    /// <summary>
    /// A top-level submit failure — a non-null callback <c>error</c>, meaning the request
    /// never reached the broker — faults <b>every</b> per-key awaiter and leaves none
    /// hanging, for both key types. The error parameter is <b>owned</b>, so it is freed by
    /// the trampoline (the mirror image of the borrowed per-key errors inside a result).
    /// </summary>
    [Fact]
    public async Task TopLevelSubmitFailure_FaultsEveryPerKeyTask_ForBothKeyTypes()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr nameUserData = IntPtr.Zero;
        DeleteTopicsResult byName = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { "alpha", "beta", "gamma" }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => nameUserData = userData,
            UnusedDeleteSubmit);

        AdminCallbacks.DeleteTopicsByName(IntPtr.Zero, MakeError(7, "could not submit"), nameUserData);

        Assert.Equal(3, byName.TopicNameValues!.Count);
        foreach (KeyValuePair<string, Task> entry in byName.TopicNameValues!)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => entry.Value, s_deadline));
            Assert.Equal(7, failure.Code);
            Assert.Equal("could not submit", failure.Message);
        }

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(byName.All, s_deadline));

        IntPtr idUserData = IntPtr.Zero;
        DescribeTopicsResult byId = admin.DescribeTopics(
            TopicCollection.OfTopicIds(new[] { new Uuid(1L, 1L), new Uuid(2L, 2L) }),
            options: null,
            UnusedDescribeSubmit,
            (nativeHandle, keys, count, timeoutMs, include, limit, callback, userData) => idUserData = userData);

        AdminCallbacks.DescribeTopicsById(IntPtr.Zero, MakeError(8, "could not submit either"), idUserData);

        Assert.Equal(2, byId.TopicIdValues!.Count);
        foreach (KeyValuePair<Uuid, Task<TopicDescription>> entry in byId.TopicIdValues!)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => entry.Value, s_deadline));
            Assert.Equal(8, failure.Code);
        }

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => byId.AllTopicIds()!, s_deadline));
    }

    /// <summary>
    /// An aggressive GC while an operation is in flight must not collect the per-operation
    /// context or a callback thunk — the <c>GCHandle</c> roots the first, the
    /// <c>static readonly</c> delegate fields the second (ffi §B6 keep-alive).
    /// </summary>
    [Fact]
    public void AggressiveGcDuringAnInFlightOperation_CollectsNothingNativeStillHolds()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr capturedUserData = IntPtr.Zero;
        DescribeTopicsResult result = admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Topic }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, include, limit, callback, userData) =>
                capturedUserData = userData,
            UnusedDescribeSubmit);

        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        GC.Collect();

        AdminCallbacks.DescribeTopicsByName(IntPtr.Zero, MakeError(3, "after gc"), capturedUserData);

        Assert.NotNull(result.TopicNameValues![Topic].Exception);
    }

    /// <summary>
    /// ⚠ <b>The key strings must stay pinned for the whole native call</b> (ffi §A4's
    /// call-scoped rule). The ABI copies the names out <em>during</em> the submit, so
    /// unpinning even a moment early hands it a pointer the GC is free to move or reclaim
    /// — and nothing else in the suite notices, because the window is normally too short
    /// for a collection to land inside it.
    /// </summary>
    /// <remarks>
    /// So the collection is <b>forced</b> into that window: the injected submit runs a
    /// compacting <see cref="GC.Collect()"/> between the pinning and the real ABI call,
    /// then calls the genuine entry point with the pointers production handed it. With the
    /// pin held those pointers are stable by construction; released, the small
    /// gen-0 arrays behind them are exactly what a gen-0 collection relocates. The
    /// assertion is that the name arrived intact — the topic really was deleted, and the
    /// result key is the name and not garbage.
    /// <para>
    /// ⚠ <b>This probe is deliberately one-sided, and that is what makes it safe to
    /// keep.</b> Whether a released pin's buffer actually moves is up to the collector, so
    /// the injection that removes the pin was measured going red on 2 of 3 runs, not 3 of
    /// 3 — a green run does not prove the pin is held. What it <em>cannot</em> do is fail
    /// against correct code: a held pin makes relocation impossible, so red is always a
    /// real defect. The alternative was no coverage at all, since every other assertion in
    /// this file passes with the pin removed.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task KeyStringsStayPinned_AcrossACollectionInsideTheNativeCall()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        const string Doomed = "p2a-pin-lifetime";

        CreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic(Doomed, 1, 1) }, options: null);
        await TestTimeout.Run(() => created.Values[Doomed], s_deadline);

        DeleteTopicsResult result = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { Doomed }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) =>
            {
                // Force a compacting collection into the window between the pin and the
                // native read. A released pin makes the pointers stale here.
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                NativeMethods.AdminClientDeleteTopicsAsync(
                    nativeHandle, keys, count, timeoutMs, retry, callback, userData);
            },
            UnusedDeleteSubmit);

        // The key survived: the delete found the topic rather than a relocated buffer, so
        // it neither faulted nor came back keyed by garbage.
        Assert.True(result.TopicNameValues!.ContainsKey(Doomed), "the result key is not the name that was sent");
        await TestTimeout.Run(() => result.TopicNameValues![Doomed], s_deadline);

        // …and it is really gone, so the name the ABI read was the name that was sent.
        DescribeTopicsResult gone = admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { Doomed }), options: null);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => gone.TopicNameValues![Doomed], s_deadline));
        Assert.Equal($"Topic {Doomed} not found.", failure.Message);
    }

    /// <summary>
    /// The submit that must not be reached, so a wrong-arm dispatch fails loudly rather
    /// than silently doing nothing.
    /// </summary>
    private static void UnusedDeleteSubmit(
        IntPtr admin,
        IntPtr[] keys,
        int count,
        int timeoutMs,
        bool retryOnQuotaViolation,
        AdminCallbacks.DeleteTopicsCallback callback,
        IntPtr userData) =>
        throw new InvalidOperationException("the wrong deleteTopics entry point was selected");

    /// <inheritdoc cref="UnusedDeleteSubmit"/>
    private static void UnusedDescribeSubmit(
        IntPtr admin,
        IntPtr[] keys,
        int count,
        int timeoutMs,
        bool includeAuthorizedOperations,
        int partitionSizeLimitPerResponse,
        AdminCallbacks.DescribeTopicsCallback callback,
        IntPtr userData) =>
        throw new InvalidOperationException("the wrong describeTopics entry point was selected");

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
