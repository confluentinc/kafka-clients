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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The per-key countdown (M15/P9 CP1): with N independent callbacks per operation, the
/// rooting <c>GCHandle</c> and the span-the-op client reference may be released only by
/// the <b>last</b> of them, and an unaccounted key may be faulted only then.
/// </summary>
/// <remarks>
/// No RPC is wired to the countdown yet (CP2 onwards), so these tests arm an operation the
/// way a submit block will — <c>GCHandle</c>, <c>DangerousAddRef</c>,
/// <see cref="AdminOperation.SetPendingCallbacks"/> — and then drive the releases directly.
/// <see cref="SafeHandle.IsClosed"/> is the direct read of "did <c>ReleaseHandle</c> run":
/// the runtime sets that bit exactly when the reference count reaches zero.
/// </remarks>
public sealed class AdminP9CountdownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The differential: not released while any release is outstanding, released on the
    /// last one. A single "not released" assertion cannot tell a working countdown from a
    /// permanently unbalanced one, so both halves are asserted here.
    /// </summary>
    [Fact]
    public async Task Countdown_ReleasesOnlyOnTheLastRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        KeyedAdminOperation<string, long> operation = new KeyedAdminOperation<string, long>(
            "createTopics", new[] { "alpha", "beta", "gamma" }, StringComparer.Ordinal);
        Arm(operation, handle, callbacks: 3);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "three callbacks are outstanding");

        operation.SetResult("alpha", 1);
        operation.ReleaseOne();
        Assert.False(handle.IsClosed, "one of three callbacks has landed");

        operation.SetResult("beta", 2);
        operation.ReleaseOne();
        Assert.False(handle.IsClosed, "two of three callbacks have landed");

        // "gamma" deliberately gets no outcome: the point of firing FailUncompleted at
        // countdown zero is that a merely-late key is not faulted before its turn.
        Assert.False(operation.Tasks["gamma"].IsCompleted, "a key with no callback yet must stay pending");

        operation.ReleaseOne();
        Assert.False(handle.IsClosed, "the submit token is still outstanding");

        operation.ReleaseSubmitToken();
        Assert.True(handle.IsClosed, "the last release must run the deferred destroy");

        Assert.Equal(1, await operation.Tasks["alpha"]);
        Assert.Equal(2, await operation.Tasks["beta"]);
        KafkaException unaccounted = await Assert.ThrowsAsync<KafkaException>(
            () => operation.Tasks["gamma"]);
        Assert.Equal("The createTopics result contained no entry for 'gamma'.", unaccounted.Message);
    }

    /// <summary>
    /// The <c>n == 0</c> rule: an empty key collection means native never calls back, so
    /// without the submit token the countdown would never reach zero and
    /// <c>AdminClient_destroy</c> would be deferred for the process lifetime.
    /// </summary>
    [Fact]
    public void ZeroCallbacks_ReleaseAtTheSubmitBoundary()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        KeyedAdminOperation<string, long> operation = new KeyedAdminOperation<string, long>(
            "createTopics", Array.Empty<string>(), StringComparer.Ordinal);
        Arm(operation, handle, callbacks: 0);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the submit has not returned yet");

        operation.ReleaseSubmitToken();

        Assert.True(handle.IsClosed, "zero callbacks must release when the submit returns");
    }

    /// <summary>
    /// The abandon path and the countdown path share one release latch, so whichever runs
    /// first wins and the other is inert. Both orders are asserted: a double
    /// <c>DangerousRelease</c> would throw here, and a double <c>GCHandle.Free</c> aborts
    /// the process.
    /// </summary>
    [Fact]
    public void AbandonAndCountdown_CannotDoubleFree()
    {
        // ---- The countdown reached zero first; a later abandon is inert. ----
        NativeAdminClient first = NativeAdminClient.CreateMock(1);
        SafeAdminHandle firstHandle = first.Handle;
        KeyedAdminOperation<string, long> completed = new KeyedAdminOperation<string, long>(
            "createTopics", new[] { "alpha" }, StringComparer.Ordinal);
        Arm(completed, firstHandle, callbacks: 1);

        completed.ReleaseOne();
        completed.ReleaseSubmitToken();
        TestTimeout.Run(first.Dispose, s_deadline);
        Assert.True(firstHandle.IsClosed);

        completed.AbandonBeforeSubmit();
        completed.FreeGcHandle();
        Assert.True(firstHandle.IsClosed, "a second release must be inert, not a double free");

        // FailUncompleted faulted "alpha" at countdown zero; observe it.
        Assert.NotNull(completed.Tasks["alpha"].Exception);

        // ---- The abandon ran first; the countdown is then inert. ----
        NativeAdminClient second = NativeAdminClient.CreateMock(1);
        SafeAdminHandle secondHandle = second.Handle;
        KeyedAdminOperation<string, long> abandoned = new KeyedAdminOperation<string, long>(
            "createTopics", new[] { "alpha", "beta" }, StringComparer.Ordinal);
        Arm(abandoned, secondHandle, callbacks: 2);

        abandoned.AbandonBeforeSubmit();
        TestTimeout.Run(second.Dispose, s_deadline);
        Assert.True(secondHandle.IsClosed, "the abandon path releases the reference itself");

        abandoned.ReleaseOne();
        abandoned.ReleaseOne();
        abandoned.ReleaseSubmitToken();
        Assert.True(secondHandle.IsClosed, "the countdown must not release a second time");

        // The latch guards the release, not the finalization — so a countdown that runs
        // anyway still faults the unaccounted keys rather than leaving them hanging.
        // Observing them also keeps no awaiter unobserved.
        Assert.NotNull(abandoned.Tasks["alpha"].Exception);
        Assert.NotNull(abandoned.Tasks["beta"].Exception);
    }

    /// <summary>
    /// The void specialization resolves its no-request keys at countdown zero too — before
    /// <c>FailUncompleted</c> sees them, so a key that contributed no request row succeeds
    /// rather than reporting a defect Java never reports.
    /// </summary>
    [Fact]
    public async Task VoidCountdown_ResolvesNoRequestKeysAtZero()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        VoidKeyedAdminOperation<string> operation = new VoidKeyedAdminOperation<string>(
            "incrementalAlterConfigs", new[] { "with-rows", "no-rows" }, StringComparer.Ordinal);
        operation.SetKeysWithNoRequest(new[] { "no-rows" });
        Arm(operation, handle, callbacks: 1);

        KeyedResultMarshal.CompleteKey(operation, "with-rows", IntPtr.Zero);
        operation.ReleaseOne();
        Assert.False(operation.Tasks["no-rows"].IsCompleted, "still before countdown zero");

        operation.ReleaseSubmitToken();

        await operation.Tasks["with-rows"];
        await operation.Tasks["no-rows"];
    }

    /// <summary>
    /// Shape 4a resolves one key per callback, and a marshalling failure on one key faults
    /// <b>only</b> that key — the per-callback no-throw boundary.
    /// </summary>
    [Fact]
    public async Task PerKeyCompletion_IsolatesOneKeysMarshallingFailure()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        KeyedAdminOperation<string, long> operation = new KeyedAdminOperation<string, long>(
            "listOffsets", new[] { "good", "bad", "failed" }, StringComparer.Ordinal);
        Arm(operation, admin.Handle, callbacks: 3);

        int destroyed = 0;
        Action<IntPtr> destroy = _ => destroyed++;

        KeyedResultMarshal.CompleteKey(
            operation, "good", new IntPtr(7), IntPtr.Zero, value => value.ToInt64(), destroy);
        KeyedResultMarshal.CompleteKey(
            operation,
            "bad",
            new IntPtr(9),
            IntPtr.Zero,
            _ => throw new InvalidOperationException("unreadable value"),
            destroy);
        KeyedResultMarshal.CompleteKey(
            operation, "failed", IntPtr.Zero, MakeError(42, "that key failed"), value => value.ToInt64(), destroy);

        Assert.Equal(3, destroyed);
        Assert.Equal(7, await operation.Tasks["good"]);
        await Assert.ThrowsAsync<InvalidOperationException>(() => operation.Tasks["bad"]);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => operation.Tasks["failed"]);
        Assert.Equal(42, failure.Code);
        Assert.Equal("that key failed", failure.Message);
    }

    /// <summary>
    /// Shape 4c: N per-key arrivals resolve <b>one</b> aggregate task, once, and a per-key
    /// error is carried as the map's <b>value</b> — not as a fault (the <c>ElectLeaders</c>
    /// lesson).
    /// </summary>
    [Fact]
    public async Task FanIn_CompletesOnceWithPerKeyErrorsAsValues()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        FanInAdminOperation<TopicPartition, KafkaException?> operation =
            new FanInAdminOperation<TopicPartition, KafkaException?>(
                2, EqualityComparer<TopicPartition>.Default);
        Arm(operation, admin.Handle, callbacks: 2);

        TopicPartition ok = new TopicPartition("t", 0);
        TopicPartition bad = new TopicPartition("t", 1);

        operation.Add(ok, null);
        operation.ReleaseOne();
        Assert.False(operation.Task.IsCompleted, "the aggregate task must wait for the last key");

        operation.Add(bad, new KafkaException(37, "partition failed", false));
        operation.ReleaseOne();
        operation.ReleaseSubmitToken();

        IReadOnlyDictionary<TopicPartition, KafkaException?> entries =
            await operation.Task;

        Assert.Equal(TaskStatus.RanToCompletion, operation.Task.Status);
        Assert.Equal(2, entries.Count);
        Assert.Null(entries[ok]);
        Assert.Equal(37, entries[bad]!.Code);

        // Completing once: a stray extra release must not replace or re-resolve the result.
        operation.ReleaseOne();
        Assert.Same(entries, await operation.Task);
    }

    /// <summary>
    /// Shape 4c with no keys at all: Java's map is empty and the ABI never calls back, so
    /// the submit token both resolves the task and releases the operation.
    /// </summary>
    [Fact]
    public async Task FanIn_ZeroCallbacks_ResolvesEmptyAtTheSubmitBoundary()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        FanInAdminOperation<TopicPartition, KafkaException?> operation =
            new FanInAdminOperation<TopicPartition, KafkaException?>(
                0, EqualityComparer<TopicPartition>.Default);
        Arm(operation, handle, callbacks: 0);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(operation.Task.IsCompleted);
        Assert.False(handle.IsClosed);

        operation.ReleaseSubmitToken();

        Assert.Empty(await operation.Task);
        Assert.True(handle.IsClosed);
    }

    /// <summary>
    /// The submit-side arming a per-key submit block will do: root the context, take the
    /// span-the-op reference, then arm the countdown — all before the P/Invoke.
    /// </summary>
    private static void Arm(AdminOperation operation, SafeAdminHandle handle, int callbacks)
    {
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        bool added = false;
        handle.DangerousAddRef(ref added);
        Assert.True(added);
        operation.SetHandleRef(handle);

        operation.SetPendingCallbacks(callbacks);
    }

    /// <inheritdoc cref="AdminOperationLifetimeTests"/>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
