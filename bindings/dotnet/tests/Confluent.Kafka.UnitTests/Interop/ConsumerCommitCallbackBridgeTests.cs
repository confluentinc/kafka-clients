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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The <b>offset-commit callback registration</b> bridge (ffi-marshalling.md §B6) — a
/// one-shot completion whose <c>GCHandle</c> is freed by a <b>release hook</b>, not by the
/// callback. Covers what the public surface cannot reach: the free site on the path where the
/// callback never fires at all, the delivered-error channel, the discard trampoline, the
/// no-throw boundary, and free-exactly-once.
/// </summary>
/// <remarks>
/// <para>
/// <b>DoD #10 (hot-path allocation audit) is N/A for this phase</b> and is stated rather than
/// skipped: a commit callback fires per <em>commit</em>, not per record, so there is no
/// per-message hot path to budget.
/// </para>
/// <para>
/// <b>Why some tests invoke a trampoline directly.</b> On a <c>MockConsumer</c> the core always
/// delivers a <b>null</b> error (<c>confluent_kafka.h:2482-2485</c>), so a broker-originated
/// commit failure has no broker-free vehicle. The only way to exercise the delivered-error
/// channel and the no-throw boundary is to call the trampoline the way the core would, with a
/// genuine owned <c>OffsetMap_t</c> (obtained from <c>Consumer_committed</c>) and a genuine
/// owned <c>Error_t</c> (from <c>Error_new</c>) — so the callback-owns-both-handles
/// contract is exercised for real rather than simulated. P6's listener bridge tests do the
/// same, as does the Rust side in its own <c>#[cfg(test)]</c> coverage.
/// </para>
/// <para>
/// <b>What this suite provably catches, and what it cannot (measured, not asserted).</b> Each
/// claim below was checked by injecting the corresponding defect and re-running:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Caught</b> — the free site moved to the trampoline
/// (<c>CommitTrampoline_*_DoesNotReleaseTheRegistration</c> go red), and production dropping
/// the release hook (4 tests go red, including the marshal-failure one, because it takes its
/// ABI triple from <c>NativeConsumer.CommitRegistrationArguments</c> rather than naming the
/// hook itself — DoD #12).
/// </description></item>
/// <item><description>
/// <b>Caught</b> — a <b>double</b> destroy of the delivered <c>OffsetMap_t</c>: the test host
/// aborts on a Rust allocator panic.
/// </description></item>
/// <item><description>
/// <b>NOT caught</b> — <b>omitting</b> the delivered <c>OffsetMap_t</c> destroy (the
/// <c>OffsetMapMarshal.CopyOut</c>-does-not-destroy trap). Removing it leaves all 37 tests
/// green: a native leak is not observable from managed code, exactly as P6 recorded for the
/// delivered <c>TopicPartitionList_t</c>. The guard is correct; this suite cannot tell it from
/// a broken one, so it is stated rather than implied.
/// </description></item>
/// </list>
/// </remarks>
public sealed class ConsumerCommitCallbackBridgeTests
{
    private const string Topic = "commit-callback-bridge-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Drives a mock to a known committed offset and hands back the owned
    /// <c>OffsetMap_t</c> the core would deliver to a commit callback. Ownership transfers to
    /// the caller, exactly as it does to a real callback.
    /// </summary>
    private static IntPtr OwnedCommittedMap(NativeConsumer consumer, TopicPartition partition, long offset)
    {
        consumer.Assign(new[] { partition });
        consumer.CommitSyncOffsets(new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [partition] = new OffsetAndMetadata(offset, "bridge-meta", 3),
        });

        using Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(partition.Topic);
        IntPtr error = NativeMethods.ConsumerCommitted(
            consumer.Handle, new[] { pin.Pointer }, new[] { partition.Partition }, 1, out IntPtr map);
        Assert.Equal(IntPtr.Zero, error);
        Assert.NotEqual(IntPtr.Zero, map);
        return map;
    }

    // ---- THE free-site test: the path where the callback never fires ----

    [Fact]
    public void MarshalFailure_CallbackNeverFires_ButTheRegistrationIsStillReleasedExactlyOnce()
    {
        // THE test that discriminates the correct free site from the (wrong) one-shot default.
        //
        // src/ffi/consumer.rs:4001-4009 builds the callback adapter — which takes ownership of
        // `user_data` — at :4004, BEFORE the fallible read_offset_map at :4005, so the early
        // return at :4007 drops the adapter and fires the release hook WITHOUT ever invoking
        // the callback. A negative offset is exactly that failure
        // (OffsetAndMetadata::with_leader_epoch → "Invalid negative offset").
        //
        // If the GCHandle were freed in the trampoline (the shipped one-shot rule), this path
        // would leak it: IsReleased would stay false. VERIFIED by mutation — dropping the
        // destroy hook from the submit and freeing in ConsumerCallbacks.OnCommit instead turns
        // this test red (and only this one, plus the success-path hook test below).
        //
        // The call is driven directly rather than through NativeConsumer.CommitAsync(offsets,
        // callback), because the managed OffsetAndMetadata constructor rejects a negative
        // offset before it can ever reach the wire — so this ABI path is unreachable from the
        // public surface by construction.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback);

        // The ABI triple comes from PRODUCTION's own builder, never re-decided here (DoD #12):
        // if NativeConsumer stopped passing the release hook, this test would go red instead of
        // passing on a hook only the test supplied.
        (ConsumerCallbacks.CommitCallback trampoline, IntPtr userData,
            ConsumerCallbacks.CommitUserDataDestroyCallback? destroy) =
            NativeConsumer.CommitRegistrationArguments(registration);

        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(Topic))
        using (Utf8Marshal.PinnedUtf8String metadataPin = Utf8Marshal.Pin(string.Empty))
        {
            error = NativeMethods.ConsumerCommitAsyncOffsetsWithCallback(
                consumer.Handle,
                new[] { topicPin.Pointer },
                new[] { 0 },
                new[] { -1L },                       // the marshal failure
                new[] { -1 },
                new[] { metadataPin.Pointer },
                1,
                trampoline,
                userData,
                destroy);
        }

        // 1. The call reports the initiation failure (error channel #1).
        Assert.NotEqual(IntPtr.Zero, error);
        KafkaException failure = Assert.IsType<KafkaException>(KafkaException.FromHandle(error));
        Assert.Contains("Invalid negative offset", failure.Message, StringComparison.Ordinal);

        // 2. The callback NEVER fired.
        Assert.Empty(callback.Completions);

        // 3. ...and the registration was still released — by the hook, the only site that runs
        //    on this path.
        Assert.True(registration.IsReleased);

        // 4. Exactly once: a second Release() frees nothing and does not throw (GCHandle.Free
        //    on an already-freed handle is a hard InvalidOperationException).
        registration.Release();
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void SuccessfulCommit_FiresTheCallbackAndThenReleasesTheRegistrationViaTheHook()
    {
        // The other half of the free-site proof: on the success path the hook ALSO fires (the
        // core drops the registration once the commit completed and the callback returned),
        // so a trampoline that freed as well would be freeing a GCHandle the core is still
        // about to hand to the hook. Driven directly so the registration is observable —
        // NativeConsumer.CommitAsync owns its own and does not expose it.
        //
        // On a mock the callback runs INLINE during the call and the registration is dropped
        // before it returns, so this is fully deterministic: no polling, no wait
        // (confluent_kafka.h:2482-2485).
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback);

        // Production's own builder again (DoD #12) — see the marshal-failure test.
        (ConsumerCallbacks.CommitCallback trampoline, IntPtr userData,
            ConsumerCallbacks.CommitUserDataDestroyCallback? destroy) =
            NativeConsumer.CommitRegistrationArguments(registration);

        IntPtr error = NativeMethods.ConsumerCommitAsyncWithCallback(
            consumer.Handle, trampoline, userData, destroy);

        Assert.Equal(IntPtr.Zero, error);
        Assert.Single(callback.Completions);
        Assert.Null(Assert.Single(callback.Completions).Exception);
        Assert.True(registration.IsReleased);
    }

    // ---- The PRODUCTION wiring of the free site (DoD #12) ----

    [Fact]
    public void ProductionCommitAsync_WithCallback_RegistersTheHook_WhichReleasesTheRegistration()
    {
        // The tests above drive the P/Invoke directly and therefore pass the release hook
        // themselves — which would keep passing if NativeConsumer silently stopped passing
        // one. This is the test that pins the PRODUCTION wiring: go through
        // NativeConsumer.CommitAsync and observe that the registration it created was released
        // by the hook. VERIFIED by mutation — passing `null` for the hook in either production
        // overload turns this test (and its offsets twin below) red.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(callback);

        Assert.Single(callback.Completions);
        Assert.True(consumer.CurrentCommitCallbackRegistration!.IsReleased);
    }

    [Fact]
    public void ProductionCommitAsync_WithOffsetsAndCallback_RegistersTheHook()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        TopicPartition partition = new TopicPartition(Topic, 8);
        consumer.Assign(new[] { partition });
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(2) },
            callback);

        Assert.Single(callback.Completions);
        Assert.True(consumer.CurrentCommitCallbackRegistration!.IsReleased);
    }

    [Fact]
    public void ProductionCommitAsync_WithOffsetsAndNoCallback_AllocatesNoRegistration()
    {
        // The discard path has nothing managed to root, so it must allocate no GCHandle and
        // register no hook — the one place C's `user_data_destroy = nullptr` is right for .NET
        // too. A registration here would be a leak with no releaser.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        TopicPartition partition = new TopicPartition(Topic, 9);
        consumer.Assign(new[] { partition });

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(1) },
            null);

        Assert.Null(consumer.CurrentCommitCallbackRegistration);
    }

    // ---- The delivered-error channel (no broker-free vehicle; driven directly) ----

    [Fact]
    public void CommitTrampoline_NonNullError_DeliversCodeAndVerbatimMessage_AndFreesTheHandle()
    {
        // Error channel #2: the COMMIT'S OWN outcome, distinct from the initiation failure
        // above. Unreachable on a mock (the core always delivers a null error there), so the
        // trampoline is driven the way the core would drive it, with a genuine owned
        // Error_t. Asserting the code AND the exact message is definition-of-done.md §3.
        const int Code = 27;                       // REBALANCE_IN_PROGRESS
        const string Message = "commit failed — 例外";
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback);
        try
        {
            IntPtr map = OwnedCommittedMap(consumer, new TopicPartition(Topic, 0), 11);
            IntPtr error;
            using (Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(Message))
            {
                error = NativeMethods.KafkaErrorNew(Code, pin.Pointer);
            }

            ConsumerCallbacks.Commit(map, error, registration.UserData);

            (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception) completion =
                Assert.Single(callback.Completions);
            KafkaException delivered = Assert.IsType<KafkaException>(completion.Exception);
            Assert.Equal(Code, delivered.Code);
            Assert.Equal(Message, delivered.Message);

            // The offsets are delivered alongside the error, exactly as Java's onComplete does.
            Assert.Equal(11, completion.Offsets[new TopicPartition(Topic, 0)].Offset);

            // The trampoline did NOT free the registration (unlike the one-shot completions):
            // that is the hook's job, and here no hook ran.
            Assert.False(registration.IsReleased);
        }
        finally
        {
            registration.Release();
        }
    }

    [Fact]
    public void CommitTrampoline_NullError_DeliversNullException()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback);
        try
        {
            IntPtr map = OwnedCommittedMap(consumer, new TopicPartition(Topic, 4), 77);

            ConsumerCallbacks.Commit(map, IntPtr.Zero, registration.UserData);

            (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception) completion =
                Assert.Single(callback.Completions);
            Assert.Null(completion.Exception);
            OffsetAndMetadata value = completion.Offsets[new TopicPartition(Topic, 4)];
            Assert.Equal(77, value.Offset);
            Assert.Equal("bridge-meta", value.Metadata);
            Assert.Equal(3, value.LeaderEpoch);
        }
        finally
        {
            registration.Release();
        }
    }

    // ---- The no-throw boundary (there is no Task and no error return to surface through) ----

    [Fact]
    public void CommitTrampoline_ThrowingCallback_IsSwallowed_AndDoesNotReleaseTheRegistration()
    {
        // A managed exception must never unwind into native (there is no caller frame — it
        // would be UB). Unlike the rebalance listener there is NO error return channel here
        // (the ABI typedef returns void), so it is swallowed + traced. "Does not throw" is the
        // observable proof; the trace is asserted in the public-surface tests.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        ThrowingCommitCallback callback = new ThrowingCommitCallback("commit callback blew up");
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback);
        try
        {
            IntPtr map = OwnedCommittedMap(consumer, new TopicPartition(Topic, 1), 5);

            ConsumerCallbacks.Commit(map, IntPtr.Zero, registration.UserData);

            Assert.True(callback.WasInvoked);
            // Still not the free site, even on the throwing path.
            Assert.False(registration.IsReleased);
        }
        finally
        {
            registration.Release();
        }
    }

    [Fact]
    public void CommitTrampoline_WithUnexpectedContext_DoesNotUnwindIntoNative()
    {
        // Mirrors ConsumerCompletionBridgeTests.Callback_WithUnexpectedContext_DoesNotUnwind:
        // a user_data pointing at the wrong managed type makes the cast throw INSIDE the
        // trampoline. It must still be swallowed, and the delivered map must still be
        // destroyed (the finally runs regardless).
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        System.Runtime.InteropServices.GCHandle wrong =
            System.Runtime.InteropServices.GCHandle.Alloc(new object());
        CapturingTraceListener listener = new CapturingTraceListener();
        System.Diagnostics.Trace.Listeners.Add(listener);
        try
        {
            IntPtr map = OwnedCommittedMap(consumer, new TopicPartition(Topic, 2), 9);

            ConsumerCallbacks.Commit(map, IntPtr.Zero, System.Runtime.InteropServices.GCHandle.ToIntPtr(wrong));
        }
        finally
        {
            System.Diagnostics.Trace.Listeners.Remove(listener);
            wrong.Free();
        }

        // M9/P7 review, finding 2: this is a TRAMPOLINE fault (an InvalidCastException out of
        // the GCHandle recovery), not a user-callback fault. Before the fix it was logged as
        // "an IOffsetCommitCallback threw" — false, and it points a debugger at user code that
        // never ran.
        string line = Assert.Single(
            listener.Lines, l => l.Contains("InvalidCastException", StringComparison.Ordinal));
        Assert.Contains("before reaching the callback", line, StringComparison.Ordinal);
        Assert.DoesNotContain("IOffsetCommitCallback (OnComplete) threw", line, StringComparison.Ordinal);
    }

    /// <summary>
    /// Captures what the binding writes to <see cref="System.Diagnostics.Trace"/>. Registered
    /// only for one test and matched on content unique to that test, so assembly-wide parallel
    /// execution cannot make it observe another test's output.
    /// </summary>
    private sealed class CapturingTraceListener : System.Diagnostics.TraceListener
    {
        private readonly System.Text.StringBuilder _pending = new System.Text.StringBuilder();

        internal List<string> Lines { get; } = new List<string>();

        public override void Write(string? message) => _pending.Append(message);

        public override void WriteLine(string? message)
        {
            _pending.Append(message);
            lock (Lines)
            {
                Lines.Add(_pending.ToString());
            }

            _pending.Clear();
        }
    }

    // ---- The discard trampoline (Java's commitAsync(Map, null)) ----

    [Fact]
    public void CommitDiscard_FreesBothDeliveredHandles_AndInvokesNothing()
    {
        // The .NET equivalent of C's discard_commit_complete (server.cc:376-380). It exists
        // because the ABI's `callback` parameter is NOT nullable and there is no plain
        // Consumer_commit_async_offsets, so Java's legal commitAsync(Map, null) can only be
        // expressed with a no-op that still frees what it is given.
        //
        // Honest limit (the P6 churn-test precedent): managed code cannot observe a native
        // LEAK, so this asserts what it can — the call completes, dereferences no user_data
        // (IntPtr.Zero is what the production path passes), and does not double-free (a second
        // destroy of either handle would abort the test runner). The end-to-end coverage of
        // this path is PublicConsumerCommitCallbackTests' callback-less commit + churn loop.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        IntPtr map = OwnedCommittedMap(consumer, new TopicPartition(Topic, 3), 21);
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin("discarded"))
        {
            error = NativeMethods.KafkaErrorNew(42, pin.Pointer);
        }

        // Production's builder decides the discard triple too, so this cannot drift from it.
        (ConsumerCallbacks.CommitCallback trampoline, IntPtr userData,
            ConsumerCallbacks.CommitUserDataDestroyCallback? destroy) =
            NativeConsumer.CommitRegistrationArguments(null);
        Assert.Equal(IntPtr.Zero, userData);
        Assert.Null(destroy);

        trampoline(map, error, userData);
    }

    [Fact]
    public void CommitDiscard_NullError_IsANoOpBeyondDestroyingTheMap()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        IntPtr map = OwnedCommittedMap(consumer, new TopicPartition(Topic, 5), 1);

        ConsumerCallbacks.CommitDiscard(map, IntPtr.Zero, IntPtr.Zero);
    }

    // ---- The delivered map's copy-out-and-destroy helper (M9/P7 review, finding 4) ----

    [Fact]
    public void OffsetMapCopyOutAndDestroy_CopiesEveryEntry_AndReleasesTheRootOnce()
    {
        // The twin added so "copy the listener trampoline's shape" stops being a leak on the
        // commit path. Exercised directly: the copy is complete and owned (nothing
        // native-backed escapes), and the root is destroyed exactly once — a second destroy
        // would abort the host, which is what makes "exactly once" observable here at all.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        TopicPartition partition = new TopicPartition(Topic, 10);
        IntPtr map = OwnedCommittedMap(consumer, partition, 64);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> copied =
            OffsetMapMarshal.CopyOutAndDestroy(map);

        OffsetAndMetadata value = Assert.Single(copied).Value;
        Assert.Equal(64, value.Offset);
        Assert.Equal("bridge-meta", value.Metadata);
        Assert.Equal(3, value.LeaderEpoch);
    }

    // NOTE: there is deliberately no "releases the root even when the copy throws" test.
    // Making CopyOut throw needs an invalid map pointer, and the OffsetMap_t container
    // accessors are NOT null-safe — such a test would segfault the host rather than assert
    // anything. That property is structural (the destroy is in a `finally`) and is shared
    // verbatim with the shipped TopicPartitionListMarshal twin.

    // ---- Free exactly once ----

    [Fact]
    public void CommitCallbackRegistration_ReleasedTwice_FreesExactlyOnce()
    {
        // GCHandle.Free() on an already-freed handle throws InvalidOperationException, so
        // "does not throw" is the observable proof that the single free site is
        // idempotent-safe. It has to be: the release hook and the "native never ran" abandon
        // path can both reach it.
        CommitCallbackRegistration registration =
            CommitCallbackRegistration.Root(new RecordingCommitCallback());

        registration.Release();
        registration.Release();

        Assert.True(registration.IsReleased);
    }

    // ---- Keep-alive under GC + churn ----

    [Fact]
    public void CommitCallbacks_ChurnedUnderAggressiveGc_StayLive()
    {
        // The trampolines and the release hook are static readonly (process-rooted) and each
        // registration is rooted by its own GCHandle until the hook fires, so aggressive GC
        // across a submit→fire window must not collect either. Each iteration allocates a
        // fresh registration, so a missing free site would also show up here as growth.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();

        for (int i = 0; i < 50; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            consumer.CommitAsync(callback);
        }

        Assert.Equal(50, callback.Completions.Count);
    }

    [Fact]
    public void CommitAsyncWithOffsets_Churned_NoCorruption()
    {
        // Exercises BOTH offsets branches (with and without a callback) repeatedly. A per-fire
        // GCHandle mistake, a missing map destroy, or a double free would surface here as a
        // crash rather than a silent pass.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        TopicPartition partition = new TopicPartition(Topic, 6);
        consumer.Assign(new[] { partition });
        RecordingCommitCallback callback = new RecordingCommitCallback();

        for (int i = 0; i < 200; i++)
        {
            Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [partition] = new OffsetAndMetadata(i),
            };
            consumer.CommitAsync(offsets, i % 2 == 0 ? callback : null);
        }

        Assert.Equal(100, callback.Completions.Count);
    }

    // ---- Teardown ----

    [Fact]
    public void Dispose_AfterACommitCallback_ReturnsWithoutHanging()
    {
        // On a mock the callback fires inline and the core drops the registration before the
        // call returns, so by teardown there is nothing outstanding — but the teardown-returns
        // regression is asserted anyway (ffi §B6 "Tests required").
        NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();
        consumer.CommitAsync(callback);

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        Assert.Single(callback.Completions);
    }

    [Fact]
    public async System.Threading.Tasks.Task DisposeAsync_AfterACommitCallback_ReturnsWithoutHanging()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();
        consumer.CommitAsync(callback);

        await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);

        Assert.Single(callback.Completions);
    }

    // ---- Fixtures ----

    private sealed class RecordingCommitCallback : IOffsetCommitCallback
    {
        internal List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception)>
            Completions
        { get; } =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>, KafkaException?)>();

        public void OnComplete(
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception) =>
            Completions.Add((offsets, exception));
    }

    private sealed class ThrowingCommitCallback : IOffsetCommitCallback
    {
        private readonly string _message;

        internal ThrowingCommitCallback(string message)
        {
            _message = message;
        }

        internal bool WasInvoked { get; private set; }

        public void OnComplete(
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception)
        {
            WasInvoked = true;
            throw new InvalidOperationException(_message);
        }
    }
}
