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
/// The span-the-op reference contract on <see cref="SafeAdminHandle"/> — the binding's
/// only defence against the admin ABI's unguarded destroy.
/// <c>kafka_admin_AdminClient_destroy</c> is <b>not</b> ref-counted and does <b>not</b>
/// drain: the header makes "do not destroy concurrently with an in-flight <c>_async</c>
/// operation" a precondition <em>the caller</em> must uphold, unlike the consumer ABI
/// which ref-counts internally.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the native call is injected rather than really made.</b> "An operation is in
/// flight" has to be a fact the test controls, not a race it hopes to win — against the
/// mock a real <c>create_topics_async</c> completes in microseconds. So these tests run
/// the <b>production</b> submit (<see cref="NativeAdminClient.CreateTopics(IEnumerable{NewTopic},
/// CreateTopicsOptions, NativeAdminClient.NativeCreateTopicsSubmit)"/>) with a stand-in
/// that captures <c>user_data</c> instead of calling native, and then invoke the
/// <b>production</b> trampoline (<see cref="AdminCallbacks.CreateTopics"/>) at the exact
/// moment they choose. Everything under test — the <c>DangerousAddRef</c>, the
/// <c>GCHandle</c>, the release in <c>FreeGcHandle</c> — is production code; only the
/// clock is the test's.
/// </para>
/// <para>
/// <see cref="System.Runtime.InteropServices.SafeHandle.IsClosed"/> is the direct read of
/// "did <c>ReleaseHandle</c> run": the runtime sets that bit exactly when the reference
/// count reaches zero and the release fires, not merely when <c>Dispose</c> is called.
/// </para>
/// </remarks>
public sealed class AdminOperationLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "lifetime-topic";

    /// <summary>
    /// Set only while this thread is inside the completion trampoline. Thread-static on
    /// purpose: it is observable from the completing thread's own stack and from nowhere
    /// else, which is exactly the question "did the continuation run inline?" asks.
    /// </summary>
    [ThreadStatic]
    private static bool s_insideTrampoline;

    /// <summary>
    /// The three-way differential. A single-case assertion cannot tell a working
    /// reference count from a permanently unbalanced one — both would read "not
    /// released" — so all three cases are required: with nothing in flight the release
    /// is immediate, with an operation in flight it is deferred, and completing that
    /// operation is what lets it happen.
    /// </summary>
    [Fact]
    public void DisposeRacingAnInFlightOperation_DefersTheNativeDestroy()
    {
        // ---- (1) Nothing in flight: Dispose releases immediately. ----
        NativeAdminClient baseline = NativeAdminClient.CreateMock(1);
        SafeAdminHandle baselineHandle = baseline.Handle;
        Assert.False(baselineHandle.IsClosed);

        TestTimeout.Run(baseline.Dispose, s_deadline);

        Assert.True(
            baselineHandle.IsClosed,
            "with no operation in flight the native release must be immediate");

        // ---- (2) One in flight: Dispose must NOT release. ----
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic(Topic, 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
                capturedUserData = userData);

        Assert.NotEqual(IntPtr.Zero, capturedUserData);

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.False(
            handle.IsClosed,
            "an in-flight operation must defer AdminClient_destroy — the ABI does not protect this itself");

        // ---- (3) Completing the operation releases it. ----
        // Through the production trampoline, so the release under test is production's.
        AdminCallbacks.CreateTopics(IntPtr.Zero, MakeError(42, "submit failed"), capturedUserData);

        Assert.True(
            handle.IsClosed,
            "completing the in-flight operation must run the deferred release");

        // Observe the faulted awaiter so it is not left unobserved.
        Assert.NotNull(result.Values[Topic].Exception);
    }

    /// <summary>
    /// A top-level submit failure — a non-null callback <c>error</c>, which means the
    /// request never reached the broker at all — faults <b>every</b> per-key awaiter and
    /// leaves none hanging. The error parameter is <b>owned</b>, so it is freed here (the
    /// mirror image of the borrowed per-key errors inside a result).
    /// </summary>
    [Fact]
    public async Task TopLevelSubmitFailure_FaultsEveryPerKeyTask()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr capturedUserData = IntPtr.Zero;
        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic("alpha", 1, 1), new NewTopic("beta", 1, 1), new NewTopic("gamma", 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
                capturedUserData = userData);

        AdminCallbacks.CreateTopics(IntPtr.Zero, MakeError(7, "could not submit"), capturedUserData);

        Assert.Equal(3, result.Values.Count);
        foreach (KeyValuePair<string, Task<TopicMetadataAndConfig>> entry in result.Values)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => entry.Value, s_deadline));
            Assert.Equal(7, failure.Code);
            Assert.Equal("could not submit", failure.Message);
        }

        // all() must fault too, not hang.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// The awaiter's continuation must <b>not</b> run inside the callback. That matters
    /// more for admin than for the consumer: the header says an admin callback can fire
    /// synchronously on the submitting thread, before the entry point returns, on
    /// <em>ordinary bad input</em> — so without
    /// <c>RunContinuationsAsynchronously</c> a caller's own P/Invoke would execute
    /// arbitrary continuation code.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The probe is a continuation registered <c>ExecuteSynchronously</c> — which
    /// normally runs on whichever thread completes the task, i.e. inline inside the
    /// trampoline — reading a <b>thread-static</b> flag that is set only for the
    /// duration of the trampoline call. Seeing that flag is possible <em>only</em> from
    /// the completing thread's own stack, so it detects exactly "ran inline" and nothing
    /// else.
    /// </para>
    /// <para>
    /// ⚠ Comparing thread <em>ids</em> instead does not work, and looked like it did:
    /// it passed alone and failed once the whole suite ran, because
    /// <c>RunContinuationsAsynchronously</c> promises only that the continuation is
    /// <em>queued</em> — the pool is free to hand it back to the same thread afterwards.
    /// The thread-static flag has no such race: this thread cannot run the continuation
    /// while it is still executing the assignment that clears the flag.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task CallbackOnTheSubmittingThread_DoesNotRunTheContinuationInline()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr capturedUserData = IntPtr.Zero;
        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic(Topic, 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
                capturedUserData = userData);

        int ranInline = 0;
        Task probe = result.Values[Topic].ContinueWith(
            _ =>
            {
                if (s_insideTrampoline)
                {
                    Volatile.Write(ref ranInline, 1);
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        s_insideTrampoline = true;
        try
        {
            // Fire the completion right here — the inline case, on this very thread.
            AdminCallbacks.CreateTopics(IntPtr.Zero, MakeError(1, "inline"), capturedUserData);
        }
        finally
        {
            s_insideTrampoline = false;
        }

        // It also must not deadlock: TestTimeout turns a hang into a failure.
        await TestTimeout.Run(() => probe, s_deadline);

        Assert.Equal(0, Volatile.Read(ref ranInline));
    }

    /// <summary>
    /// An aggressive GC while an operation is in flight must not collect the per-operation
    /// context or the callback thunk. The <c>GCHandle</c> roots the first; the
    /// <c>static readonly</c> delegate field roots the second (ffi §B6 keep-alive).
    /// </summary>
    [Fact]
    public void AggressiveGcDuringAnInFlightOperation_CollectsNothingNativeStillHolds()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr capturedUserData = IntPtr.Zero;
        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic(Topic, 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
                capturedUserData = userData);

        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        GC.Collect();

        // If the context had been collected the trampoline's GCHandle.Target would be
        // null and this would throw rather than complete the awaiter.
        AdminCallbacks.CreateTopics(IntPtr.Zero, MakeError(3, "after gc"), capturedUserData);

        Assert.NotNull(result.Values[Topic].Exception);
    }

    /// <summary>
    /// Many operations on one client leave the reference count <b>balanced</b>: each
    /// completion releases exactly the one reference its submit took, so the final
    /// <c>Dispose</c> still releases the handle. An over-release would have thrown out of
    /// the <see cref="System.Runtime.InteropServices.SafeHandle"/> long before; an
    /// under-release would leave <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public void ManyOperations_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int i = 0; i < 50; i++)
        {
            IntPtr capturedUserData = IntPtr.Zero;
            CreateTopicsResult result = admin.CreateTopics(
                new[] { new NewTopic($"{Topic}-{i}", 1, 1) },
                options: null,
                (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
                    capturedUserData = userData);

            AdminCallbacks.CreateTopics(IntPtr.Zero, MakeError(9, "balance probe"), capturedUserData);

            // Reading Exception observes the fault, so no awaiter is left unobserved.
            Assert.NotNull(result.Values[$"{Topic}-{i}"].Exception);
            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "50 operations must leave the reference count balanced");
    }

    /// <summary>
    /// A submit that throws before native ran must not root the operation forever: the
    /// abandon path frees the <c>GCHandle</c> and releases the reference, so the very
    /// next <c>Dispose</c> still releases the handle.
    /// </summary>
    [Fact]
    public void SubmitThatThrows_AbandonsTheOperationAndLeavesTheHandleReleasable()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Throws<InvalidOperationException>(() => admin.CreateTopics(
            new[] { new NewTopic(Topic, 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
                throw new InvalidOperationException("submit failed")));

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(
            handle.IsClosed,
            "a submit that never reached native must not leave the operation's reference held");
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
