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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The internal flavour seam (M15/P14 S2): every RPC core builds its operation for either
/// <see cref="AdminFlavour"/>, and the <see cref="AdminFlavour.Sync"/> flavour's per-key
/// latches (<see cref="KeyedAdminOperation{TKey, TValue}.SyncSources"/>,
/// <see cref="SingleAdminOperation{TValue}.SyncSource"/>) are completed by the same
/// production trampolines, countdown and release as the async flavour's tasks.
/// </summary>
/// <remarks>
/// <para>
/// Every row drives a <b>production</b> core through its own submit seam (DoD #12): either
/// the real P/Invoke against the native mock, or a stand-in for it that fires the
/// <b>production</b> trampoline it was handed, at a moment the test chooses. Each admin
/// callback can land on one of three threads (<c>rust/src/ffi/admin.rs</c>, "Callback
/// thread"), and each has its rows here: <b>inline</b>, on the submitting thread before the
/// entry point returns; the <b>dispatcher</b>, after it returned; and a <b>foreign</b>
/// thread, standing in for the tokio-worker path.
/// </para>
/// <para>
/// Every wait is bounded (<see cref="BlockingCall"/>, <see cref="TestTimeout"/>). The async
/// flavour's continuation-placement guarantee is not restated here: it is
/// <see cref="AdminOperationLifetimeTests.CallbackOnTheSubmittingThread_DoesNotRunTheContinuationInline"/>,
/// which drives <see cref="TaskAdminCompletion{T}"/> through the same core.
/// </para>
/// </remarks>
public sealed class AdminP14FlavourSeamTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Errors.INVALID_REPLICATION_FACTOR, as PublicAdminCreateTopicsTests asserts it.
    private const int InvalidReplicationFactorCode = 38;

    // ---------------------------------------------------------------- (a) inline

    /// <summary>
    /// A keyed callback fired <b>inside</b> the submit has completed the key's latch by the
    /// time the RPC returns, and <c>Get()</c> rethrows the delivered error — the same
    /// instance on every call. This is what makes creating the latches <em>before</em> the
    /// submit load-bearing: a latch created afterwards would miss this callback.
    /// </summary>
    [Fact]
    public void SyncKeyed_CallbackFiredInsideTheSubmit_IsDoneOnReturn_AndGetRethrowsTheDeliveredError()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        KeyedAdminOperation<string, TopicMetadataAndConfig> operation = admin.CreateTopics(
            new[] { new NewTopic("inline-alpha", 1, 1), new NewTopic("inline-beta", 1, 1) },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                // `topics` holds NewTopic_t handles, not names, so each key is pinned here.
                using Utf8Marshal.PinnedUtf8String alphaKey = Utf8Marshal.Pin("inline-alpha");
                using Utf8Marshal.PinnedUtf8String betaKey = Utf8Marshal.Pin("inline-beta");
                callback(alphaKey.Pointer, IntPtr.Zero, MakeError(41, "alpha failed inline"), userData);
                callback(betaKey.Pointer, IntPtr.Zero, MakeError(42, "beta failed inline"), userData);
            });

        Assert.True(operation.SyncSources["inline-alpha"].IsDone, "the inline callback ran before the RPC returned");
        Assert.True(operation.SyncSources["inline-beta"].IsDone, "the inline callback ran before the RPC returned");

        KafkaException alpha = AssertThrew(BlockingCall.RunGet(() => operation.SyncSources["inline-alpha"].Get()), 41, "alpha failed inline");
        KafkaException again = AssertThrew(BlockingCall.RunGet(() => operation.SyncSources["inline-alpha"].Get()), 41, "alpha failed inline");
        Assert.Same(alpha, again);
        AssertThrew(BlockingCall.RunWait(() => operation.SyncSources["inline-beta"].Wait()), 42, "beta failed inline");

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>
    /// The void-keyed twin: a success and a failure, both fired inside the submit. The
    /// success key's <c>Wait()</c> returns and its <c>Get()</c> is <see langword="true"/>
    /// (a null per-key error <em>is</em> the success value); the failure key's rethrows.
    /// </summary>
    [Fact]
    public void SyncVoidKeyed_CallbackFiredInsideTheSubmit_IsDoneOnReturn_AndWaitRethrowsTheDeliveredError()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        (VoidKeyedAdminOperation<string>? byName, VoidKeyedAdminOperation<Uuid>? byId) = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { "inline-gone", "inline-kept" }),
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, keys, count, timeoutMs, retryOnQuotaViolation, callback, userData) =>
            {
                callback(keys[0], IntPtr.Zero, userData);
                callback(keys[1], MakeError(3, "kept inline"), userData);
            },
            (nativeHandle, keys, count, timeoutMs, retryOnQuotaViolation, callback, userData) =>
                throw new InvalidOperationException("a by-name delete must not reach the by-id submit"));

        Assert.Null(byId);
        Assert.NotNull(byName);
        Assert.True(byName!.SyncSources["inline-gone"].IsDone, "the inline callback ran before the RPC returned");
        Assert.True(byName.SyncSources["inline-kept"].IsDone, "the inline callback ran before the RPC returned");

        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => byName.SyncSources["inline-gone"].Wait()));
        Assert.True(Assert.IsType<bool>(BlockingCall.RunGet(() => byName.SyncSources["inline-gone"].Get())));
        AssertThrew(BlockingCall.RunWait(() => byName.SyncSources["inline-kept"].Wait()), 3, "kept inline");

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>The single-completion twin: one callback fired inside the submit.</summary>
    [Fact]
    public void SyncSingle_CallbackFiredInsideTheSubmit_IsDoneOnReturn_AndGetRethrowsTheDeliveredError()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation = admin.ListTopics(
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, timeoutMs, listInternal, callback, userData) =>
                callback(IntPtr.Zero, MakeError(7, "list failed inline"), userData));

        Assert.True(operation.SyncSource.IsDone, "the inline callback ran before the RPC returned");
        AssertThrew(BlockingCall.RunGet(() => operation.SyncSource.Get()), 7, "list failed inline");
        AssertThrew(BlockingCall.RunWait(() => operation.SyncSource.Wait()), 7, "list failed inline");

        AssertReleasedOnDispose(admin, handle);
    }

    // ---------------------------------------------------------------- (b) dispatcher

    /// <summary>
    /// The real native submit against the mock, whose callbacks land on the handle's
    /// dispatcher thread after the entry point returned: a bounded <c>Get()</c> on a
    /// dedicated thread returns each key's <b>own</b> outcome — a value for one, the core's
    /// error for the other.
    /// </summary>
    [Fact]
    public void SyncKeyed_RealNativeSubmit_GetReturnsEachKeysOwnOutcome()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        // Replication factor 5 against a 1-broker mock is rejected; the other is fine.
        KeyedAdminOperation<string, TopicMetadataAndConfig> operation = admin.CreateTopics(
            new[] { new NewTopic("dispatched-ok", 2, 1), new NewTopic("dispatched-bad", 1, 5) },
            options: null,
            AdminFlavour.Sync,
            NativeMethods.AdminClientCreateTopicsAsync);

        TopicMetadataAndConfig created = Assert.IsType<TopicMetadataAndConfig>(
            BlockingCall.RunGet(() => operation.SyncSources["dispatched-ok"].Get()));
        Assert.Equal(2, created.NumPartitions());
        Assert.Equal(1, created.ReplicationFactor());

        AssertThrew(
            BlockingCall.RunGet(() => operation.SyncSources["dispatched-bad"].Get()),
            InvalidReplicationFactorCode,
            "Replication factor: 5 is larger than brokers: 1");

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>
    /// The real native submit for a void-keyed and a single-completion RPC: deleting a topic
    /// the mock holds returns, and listing returns the topics it holds.
    /// </summary>
    [Fact]
    public async Task SyncVoidKeyedAndSingle_RealNativeSubmit_GetReturnsTheValue()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        // Seeded through the async flavour, so the sync rows below read a topic the mock has.
        AsyncCreateTopicsResult seeded = AsyncCreateTopicsResult.From(admin.CreateTopics(
            new[] { new NewTopic("dispatched-listed", 1, 1), new NewTopic("dispatched-deleted", 1, 1) },
            options: null,
            AdminFlavour.Async));
        await TestTimeout.Run(() => seeded.All().Get(), s_deadline);

        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> listed = admin.ListTopics(
            options: null, AdminFlavour.Sync, NativeMethods.AdminClientListTopicsAsync);
        IReadOnlyDictionary<string, TopicListing> topics = Assert.IsAssignableFrom<IReadOnlyDictionary<string, TopicListing>>(
            BlockingCall.RunGet(() => listed.SyncSource.Get()));
        Assert.Contains("dispatched-deleted", topics.Keys);
        Assert.Equal("dispatched-listed", topics["dispatched-listed"].Name);

        (VoidKeyedAdminOperation<string>? deleted, _) = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { "dispatched-deleted" }),
            options: null,
            AdminFlavour.Sync,
            NativeMethods.AdminClientDeleteTopicsAsync,
            NativeMethods.AdminClientDeleteTopicsByIdsAsync);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => deleted!.SyncSources["dispatched-deleted"].Wait()));

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>
    /// The dispatcher shape driven by a stand-in, so "after the submit returned" is a fact
    /// rather than a likelihood: the seam hands the production trampoline to another thread,
    /// which fires only once the RPC has returned. A bounded <c>Get()</c> on a dedicated
    /// thread then returns the value.
    /// </summary>
    [Fact]
    public void SyncVoidKeyed_SeamFiresFromAnotherThreadAfterTheSubmitReturned_GetReturnsTheValue()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        using ManualResetEventSlim returned = new ManualResetEventSlim();
        Thread? dispatcher = null;
        VoidKeyedAdminOperation<string> operation = admin.CreatePartitions(
            new Dictionary<string, NewPartitions> { ["dispatched-grown"] = NewPartitions.IncreaseTo(3) },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, newPartitions, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                dispatcher = new Thread(() =>
                {
                    returned.Wait();
                    FireKey(callback, "dispatched-grown", IntPtr.Zero, userData);
                })
                {
                    IsBackground = true,
                };
                dispatcher.Start();
            });

        Assert.False(operation.SyncSources["dispatched-grown"].IsDone, "nothing has fired yet");
        returned.Set();

        Assert.True(Assert.IsType<bool>(BlockingCall.RunGet(() => operation.SyncSources["dispatched-grown"].Get())));
        Assert.True(dispatcher!.Join(s_deadline), "the dispatcher stand-in did not finish");

        AssertReleasedOnDispose(admin, handle);
    }

    // ---------------------------------------------------------------- (c) foreign thread

    /// <summary>
    /// A <c>Get()</c> already parked on its own thread is released by a completion from a
    /// <b>third</b> thread — neither the caller's nor the waiter's, standing in for the
    /// tokio-worker path.
    /// </summary>
    [Fact]
    public void SyncKeyed_ParkedGet_IsReleasedByACompletionFromAForeignThread()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        AdminCallbacks.CreateTopicsCallback? capturedCallback = null;
        KeyedAdminOperation<string, TopicMetadataAndConfig> operation = admin.CreateTopics(
            new[] { new NewTopic("foreign-topic", 1, 1) },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
            });

        BlockingCall waiter = BlockingCall.StartGet(() => operation.SyncSources["foreign-topic"].Get());
        waiter.AssertBlocked();

        int completingThread = RunOnForeignThread(() =>
        {
            using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("foreign-topic");
            capturedCallback!(key.Pointer, IntPtr.Zero, MakeError(29, "failed on a foreign thread"), capturedUserData);
        });

        AssertThrew(waiter.Join(), 29, "failed on a foreign thread");
        AssertThreeDistinctThreads(completingThread, waiter.CallingThreadId);

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>The void-keyed twin: a parked <c>Wait()</c>, released by a foreign success.</summary>
    [Fact]
    public void SyncVoidKeyed_ParkedWait_IsReleasedByACompletionFromAForeignThread()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        AdminCallbacks.CreatePartitionsCallback? capturedCallback = null;
        VoidKeyedAdminOperation<string> operation = admin.CreatePartitions(
            new Dictionary<string, NewPartitions> { ["foreign-grown"] = NewPartitions.IncreaseTo(2) },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, newPartitions, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
            });

        BlockingCall waiter = BlockingCall.StartWait(() => operation.SyncSources["foreign-grown"].Wait());
        waiter.AssertBlocked();

        int completingThread = RunOnForeignThread(
            () => FireKey(capturedCallback!, "foreign-grown", IntPtr.Zero, capturedUserData));

        Assert.Same(BlockingCall.Returned, waiter.Join());
        AssertThreeDistinctThreads(completingThread, waiter.CallingThreadId);

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>The single-completion twin: a parked <c>Get()</c>, released by a foreign failure.</summary>
    [Fact]
    public void SyncSingle_ParkedGet_IsReleasedByACompletionFromAForeignThread()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        AdminCallbacks.ListTopicsCallback? capturedCallback = null;
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation = admin.ListTopics(
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, timeoutMs, listInternal, callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
            });

        BlockingCall waiter = BlockingCall.StartGet(() => operation.SyncSource.Get());
        waiter.AssertBlocked();

        int completingThread = RunOnForeignThread(
            () => capturedCallback!(IntPtr.Zero, MakeError(11, "list failed on a foreign thread"), capturedUserData));

        AssertThrew(waiter.Join(), 11, "list failed on a foreign thread");
        AssertThreeDistinctThreads(completingThread, waiter.CallingThreadId);

        AssertReleasedOnDispose(admin, handle);
    }

    // ---------------------------------------------------------------- per-key independence

    /// <summary>
    /// Each key's latch carries <b>its own</b> outcome and completes on its own callback: a
    /// success, a failure and a key not answered yet, whose <c>Wait()</c> stays parked until
    /// its callback lands. A latch shared across keys, or one completed by a sibling's
    /// callback, fails here.
    /// </summary>
    [Fact]
    public void SyncKeyed_EachKeyCompletesOnItsOwnCallback()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        AdminCallbacks.CreatePartitionsCallback? capturedCallback = null;
        VoidKeyedAdminOperation<string> operation = admin.CreatePartitions(
            new Dictionary<string, NewPartitions>
            {
                ["independent-ok"] = NewPartitions.IncreaseTo(2),
                ["independent-bad"] = NewPartitions.IncreaseTo(2),
                ["independent-late"] = NewPartitions.IncreaseTo(2),
            },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, newPartitions, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                capturedCallback = callback;
                capturedUserData = userData;
            });

        FireKey(capturedCallback!, "independent-ok", IntPtr.Zero, capturedUserData);
        FireKey(capturedCallback!, "independent-bad", MakeError(37, "only this key failed"), capturedUserData);

        Assert.True(Assert.IsType<bool>(BlockingCall.RunGet(() => operation.SyncSources["independent-ok"].Get())));
        AssertThrew(BlockingCall.RunGet(() => operation.SyncSources["independent-bad"].Get()), 37, "only this key failed");
        Assert.False(operation.SyncSources["independent-late"].IsDone, "a key with no callback yet must stay pending");

        BlockingCall late = BlockingCall.StartWait(() => operation.SyncSources["independent-late"].Wait());
        late.AssertBlocked();

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the late key's callback is still owed, so the release is deferred");

        FireKey(capturedCallback!, "independent-late", IntPtr.Zero, capturedUserData);

        Assert.Same(BlockingCall.Returned, late.Join());
        Assert.True(handle.IsClosed, "the last callback runs the deferred release");
    }

    // ---------------------------------------------------------------- FailUncompleted

    /// <summary>
    /// A requested key no callback answered is faulted at countdown zero with today's
    /// message, on the sync flavour as on the async one. Here the one callback the submit
    /// owes answers a key the request never named, so by the time the RPC returns the
    /// countdown has reached zero.
    /// </summary>
    [Fact]
    public void SyncKeyed_AKeyNoCallbackAnswered_IsFaultedAtCountdownZero()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        KeyedAdminOperation<string, TopicMetadataAndConfig> operation = admin.CreateTopics(
            new[] { new NewTopic("unanswered-topic", 1, 1) },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                using Utf8Marshal.PinnedUtf8String stray = Utf8Marshal.Pin("never-requested");
                callback(stray.Pointer, IntPtr.Zero, MakeError(5, "a key nobody asked for"), userData);
            });

        Assert.True(operation.SyncSources["unanswered-topic"].IsDone, "countdown zero faults the unanswered key");
        KafkaException failure = Assert.IsType<KafkaException>(
            BlockingCall.RunGet(() => operation.SyncSources["unanswered-topic"].Get()));
        Assert.Equal("The createTopics result contained no entry for 'unanswered-topic'.", failure.Message);

        AssertReleasedOnDispose(admin, handle);
    }

    /// <summary>
    /// The single-completion bridge's sweep, on the sync flavour, with today's message, and
    /// idempotent once settled. Driven on the bridge directly for the reason
    /// <see cref="AdminP2bResultMarshalTests.ListTopics_AnUnresolvedAwaiter_IsFaulted_NotLeftHanging"/>
    /// records: no ABI-legal callback reaches the trampoline with nothing resolved.
    /// </summary>
    [Fact]
    public void SyncSingle_FailUncompleted_FaultsTheLatch_AndLeavesASettledOneAlone()
    {
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> unresolved =
            new SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>("listTopics", AdminFlavour.Sync);
        Assert.False(unresolved.SyncSource.IsDone);

        unresolved.FailUncompleted();

        KafkaException failure = Assert.IsType<KafkaException>(BlockingCall.RunGet(() => unresolved.SyncSource.Get()));
        Assert.Equal("The listTopics call completed without delivering a result.", failure.Message);

        SingleAdminOperation<int> settled = new SingleAdminOperation<int>("describeCluster", AdminFlavour.Sync);
        settled.SetResult(17);
        settled.FailUncompleted();
        Assert.Equal(17, Assert.IsType<int>(BlockingCall.RunGet(() => settled.SyncSource.Get())));
    }

    // ---------------------------------------------------------------- views

    /// <summary>
    /// Each flavour has exactly one view; asking a keyed operation for the other throws
    /// <see cref="InvalidOperationException"/> naming the operation, its flavour and the view.
    /// </summary>
    [Fact]
    public void KeyedOperation_TheOtherFlavoursView_Throws()
    {
        KeyedAdminOperation<string, int> sync = new KeyedAdminOperation<string, int>(
            "createTopics", new[] { "alpha" }, StringComparer.Ordinal, AdminFlavour.Sync);
        InvalidOperationException noTasks = Assert.Throws<InvalidOperationException>(() => sync.Tasks);
        Assert.Equal("The createTopics admin operation was built for the Sync flavour, so it has no Tasks view.", noTasks.Message);

        VoidKeyedAdminOperation<string> async = new VoidKeyedAdminOperation<string>(
            "deleteTopics", new[] { "alpha" }, StringComparer.Ordinal, AdminFlavour.Async);
        InvalidOperationException noSources = Assert.Throws<InvalidOperationException>(() => async.SyncSources);
        Assert.Equal(
            "The deleteTopics admin operation was built for the Async flavour, so it has no SyncSources view.",
            noSources.Message);
    }

    /// <summary>The single-completion twin.</summary>
    [Fact]
    public void SingleOperation_TheOtherFlavoursView_Throws()
    {
        SingleAdminOperation<int> sync = new SingleAdminOperation<int>("listTopics", AdminFlavour.Sync);
        // A statement lambda: a Task-returning one would bind Assert.Throws's async overload.
        InvalidOperationException noTask = Assert.Throws<InvalidOperationException>(() => { _ = sync.Task; });
        Assert.Equal("The listTopics admin operation was built for the Sync flavour, so it has no Task view.", noTask.Message);

        SingleAdminOperation<int> async = new SingleAdminOperation<int>("listTopics", AdminFlavour.Async);
        InvalidOperationException noSource = Assert.Throws<InvalidOperationException>(() => async.SyncSource);
        Assert.Equal("The listTopics admin operation was built for the Async flavour, so it has no SyncSource view.", noSource.Message);
    }

    /// <summary>A value outside the enum is rejected, not silently read as one of the two.</summary>
    [Fact]
    public void AnUnknownFlavour_IsRejected()
    {
        ArgumentOutOfRangeException keyed = Assert.Throws<ArgumentOutOfRangeException>(
            () => new KeyedAdminOperation<string, int>("createTopics", new[] { "alpha" }, StringComparer.Ordinal, (AdminFlavour)7));
        Assert.Equal("flavour", keyed.ParamName);

        ArgumentOutOfRangeException single = Assert.Throws<ArgumentOutOfRangeException>(
            () => new SingleAdminOperation<int>("listTopics", (AdminFlavour)7));
        Assert.Equal("flavour", single.ParamName);
    }

    /// <summary>
    /// Both views are keyed in the request's order — the order the async <c>*Result</c>
    /// exposes today, and the one a sync result built from the other view must share.
    /// </summary>
    [Fact]
    public void BothViews_ListTheKeysInRequestOrder()
    {
        string[] keys = { "gamma", "alpha", "beta" };

        KeyedAdminOperation<string, int> sync = new KeyedAdminOperation<string, int>(
            "createTopics", keys, StringComparer.Ordinal, AdminFlavour.Sync);
        KeyedAdminOperation<string, int> async = new KeyedAdminOperation<string, int>(
            "createTopics", keys, StringComparer.Ordinal, AdminFlavour.Async);

        Assert.Equal(keys, sync.SyncSources.Keys);
        Assert.Equal(keys, async.Tasks.Keys);
    }

    // ---------------------------------------------------------------- helpers

    /// <summary>
    /// Fires a void per-key callback for <paramref name="key"/> through the production
    /// trampoline the seam was handed. The key is a borrowed <c>const char*</c>, pinned only
    /// for the call; <paramref name="error"/> is owned and the trampoline frees it.
    /// </summary>
    private static void FireKey(AdminCallbacks.CreatePartitionsCallback callback, string key, IntPtr error, IntPtr userData)
    {
        using Utf8Marshal.PinnedUtf8String pinnedKey = Utf8Marshal.Pin(key);
        callback(pinnedKey.Pointer, error, userData);
    }

    /// <summary>
    /// Runs <paramref name="complete"/> on a fresh thread, joins it within the deadline and
    /// returns its managed id.
    /// </summary>
    private static int RunOnForeignThread(Action complete)
    {
        int threadId = 0;
        Thread thread = new Thread(() =>
        {
            threadId = Environment.CurrentManagedThreadId;
            complete();
        })
        {
            IsBackground = true,
        };
        thread.Start();
        Assert.True(thread.Join(s_deadline), "the completing thread did not finish");
        return threadId;
    }

    private static void AssertThreeDistinctThreads(int completingThread, int waitingThread)
    {
        int testThread = Environment.CurrentManagedThreadId;
        Assert.NotEqual(testThread, completingThread);
        Assert.NotEqual(testThread, waitingThread);
        Assert.NotEqual(waitingThread, completingThread);
    }

    private static KafkaException AssertThrew(object? outcome, int code, string message)
    {
        KafkaException failure = Assert.IsType<KafkaException>(outcome);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
        return failure;
    }

    /// <summary>
    /// Disposes the client and asserts the native release ran: the sync flavour's operation
    /// is released by the same countdown as the async one, so nothing is left holding the
    /// span-the-op reference. Polled within the deadline because, on the real-native rows,
    /// the dispatcher's own release can trail the latch it just completed.
    /// </summary>
    private static void AssertReleasedOnDispose(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            SpinWait.SpinUntil(() => handle.IsClosed, s_deadline),
            "every operation must have released its reference to the client");
    }

    /// <summary>
    /// Builds an <b>owned</b> <c>kafka_common_KafkaError_t</c> standing in for the one native
    /// would hand the callback; the trampoline frees it via <see cref="KafkaException.FromHandle"/>.
    /// </summary>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
