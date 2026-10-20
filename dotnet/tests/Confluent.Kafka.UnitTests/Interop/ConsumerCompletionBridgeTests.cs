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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The foreign-thread completion callback → <see cref="Task"/> bridge
/// (ffi-marshalling.md §B6/§B7), exercised through <c>subscribe_async</c> on a
/// <c>MockConsumer</c> (the void bridge's SUCCESS path — null error), plus the
/// RunContinuationsAsynchronously, chained-op, GC-survival, and no-throw properties of
/// that bridge. The bridge's error-path <em>mechanism</em> (a faulted <see cref="Task"/>
/// from a non-null <see cref="KafkaException"/>) is shared with the owned-handle / scalar
/// bridges and stays proven by the poll (<c>SetPollError</c>) / position (unassigned)
/// failure tests. M5/P7 made <c>seek</c> SYNC, so seeking an unassigned partition now
/// throws a <b>synchronous</b> <see cref="KafkaException"/> (asserted here) rather than
/// faulting the void bridge. Every awaited op is wrapped in a <see cref="TestTimeout"/>
/// hang guard so a bridge that never completes fails the run fast.
/// </summary>
public sealed class ConsumerCompletionBridgeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Seek on an unassigned partition builds its failure via
    // subscription_state::assigned_state_mut's Error::local_illegal_state (a8205c5c), which
    // is a specific, meaningful code (LOCAL_ILLEGAL_STATE == -4) — not one of the indistinct
    // broker-free codes this comment originally warned about.
    private const int LocalIllegalStateErrorCode = -4;

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task SubscribeWithCallback_OnMock_ResolvesSuccessfully()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(() => consumer.SubscribeWithCallback(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task SubscribeWithCallback_Churned_ResolvesEachTime_NoCorruption()
    {
        // The loop is the corruption detector: a double-free of the GCHandle /
        // error handle, or a leak, would corrupt the allocator over many ops.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        for (int i = 0; i < 200; i++)
        {
            await TestTimeout.Run(() => consumer.SubscribeWithCallback(ProofTopic()), s_deadline);
        }
    }

    [Fact]
    public void Seek_UnassignedPartition_ThrowsKafkaException()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        // Seek is SYNC (M5/P7): seeking an unassigned partition throws a SYNCHRONOUS
        // KafkaException from the sync ABI's returned error handle (freed exactly once by
        // FromHandle) — no longer a faulted void-bridge Task.
        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.Seek("proof-topic", 0, 0L));

        // Assert TYPE + Code (LOCAL_ILLEGAL_STATE == -4) + the retriable flag + a
        // non-empty Message.
        Assert.Equal(LocalIllegalStateErrorCode, ex.Code);
        Assert.False(ex.IsRetriable);
        Assert.False(string.IsNullOrEmpty(ex.Message));
    }

    [Fact]
    public void Seek_Churned_ThrowsEachTime_NoCorruption()
    {
        // The sync error path frees an owned Error handle each time (FromHandle);
        // churn to catch a double-free / leak on the failure branch.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        for (int i = 0; i < 50; i++)
        {
            Assert.Throws<KafkaException>(() => consumer.Seek("proof-topic", 0, 0L));
        }
    }

    [Fact]
    public async Task Completion_RunsContinuationsAsynchronously_OffTheCompletingThread()
    {
        // Drive the bridge context directly so the continuation is attached BEFORE
        // completion (attaching after completion would run inline on the test thread,
        // measuring nothing). Complete from a dedicated foreign thread standing in
        // for the core's dispatcher. An ExecuteSynchronously continuation would run
        // INLINE on that completing thread if the TCS were NOT built with
        // RunContinuationsAsynchronously; the flag forces it onto the thread pool
        // instead — so a different thread id AND IsThreadPoolThread prove it.
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            int completingThreadId = 0;
            int continuationThreadId = -1;
            bool continuationOnPool = false;

            Task check = context.Task.ContinueWith(
                _ =>
                {
                    continuationThreadId = Environment.CurrentManagedThreadId;
                    continuationOnPool = Thread.CurrentThread.IsThreadPoolThread;
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            Thread completer = new Thread(() =>
            {
                completingThreadId = Environment.CurrentManagedThreadId;
                context.Complete(IntPtr.Zero); // null error = success
            });
            completer.Start();
            completer.Join();

            await TestTimeout.Run(() => check, s_deadline);

            Assert.NotEqual(completingThreadId, continuationThreadId);
            Assert.True(continuationOnPool);
        }
        finally
        {
            context.FreeGcHandle();
        }
    }

    [Fact]
    public async Task ChainedOps_FromContinuation_Succeed_GuardReleasedBeforeCompletion()
    {
        // The guard is released just before the Task completes (ffi §B7), so a
        // continuation that immediately resubmits does NOT hit the one-op
        // (ConcurrentModification) rejection. Chained awaits must all succeed.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(
            async () =>
            {
                await consumer.SubscribeWithCallback(ProofTopic());
                await consumer.SubscribeWithCallback(ProofTopic());
                await consumer.SubscribeWithCallback(ProofTopic());
            },
            s_deadline);
    }

    [Fact]
    public async Task InFlightOperation_SurvivesAggressiveGc()
    {
        // The delegate is rooted (static readonly) and the per-op context is rooted
        // by its GCHandle (Normal), so aggressive GC during an op must not collect
        // either. Churn under GC pressure; a collected thunk/context would crash.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        for (int i = 0; i < 50; i++)
        {
            Task op = consumer.SubscribeWithCallback(ProofTopic());
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            await TestTimeout.Run(() => op, s_deadline);
        }
    }

    [Fact]
    public void FreeGcHandle_CalledTwice_FreesAndReleasesExactlyOnce()
    {
        // M9/P4 M3 moved the submit helpers' DangerousAddRef INSIDE the try, so every
        // "native never ran" path (including an AddRef throw) now reaches FreeGcHandle via
        // AbandonBeforeSubmit. That widens FreeGcHandle's reachability, and its Interlocked
        // guard is what keeps the widening safe: a second call must free NOTHING. Two
        // observable proofs, one per resource:
        //   * GCHandle.Free() on an already-freed handle throws InvalidOperationException,
        //     so "does not throw" proves the GCHandle is freed exactly once;
        //   * the SafeHandle ref is proven by tearing down afterwards — see below.
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;

        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);

        bool handleRefAdded = false;
        handle.DangerousAddRef(ref handleRefAdded);
        Assert.True(handleRefAdded);
        context.SetHandleRef(handle);

        context.FreeGcHandle();
        context.FreeGcHandle();

        // Discriminating check for the ref release: exactly ONE DangerousRelease happened, so
        // the ref count is back at its base and teardown still works. The load-bearing
        // assertion is that this Dispose does NOT throw — with one release too many the count
        // is already zero, and SafeHandle.Dispose then throws
        // ObjectDisposedException("Safe handle has been closed") with Consumer_destroy never
        // running (verified against a deliberately double-released handle). IsClosed is a
        // weaker corollary (it is set on the failing path too), asserted only to pin that
        // teardown actually reached the release rather than short-circuiting.
        consumer.Dispose();
        Assert.True(handle.IsClosed);
    }

    [Fact]
    public void AbandonBeforeSubmit_ThenFreeGcHandle_FreesAndReleasesExactlyOnce()
    {
        // AbandonBeforeSubmit is the ONE non-callback free path (native never ran), and after
        // M9/P4 M3 it also covers the AddRef-threw case. It routes through the same
        // Interlocked-guarded FreeGcHandle, so mixing the two entry points is still exactly
        // one free plus exactly one release — invariant I1 is preserved because no new free
        // site was introduced, only wider reachability of the existing one.
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;

        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);

        bool handleRefAdded = false;
        handle.DangerousAddRef(ref handleRefAdded);
        Assert.True(handleRefAdded);
        context.SetHandleRef(handle);

        context.AbandonBeforeSubmit();
        context.FreeGcHandle();

        consumer.Dispose();
        Assert.True(handle.IsClosed);
    }

    [Fact]
    public void AbandonBeforeSubmit_WithNoHandleRefTaken_ReleasesNothing()
    {
        // The exact shape M9/P4 M3 introduces: DangerousAddRef itself threw, so SetHandleRef
        // was never called. AbandonBeforeSubmit must free the GCHandle (otherwise the context
        // is rooted for the process lifetime) and release NO handle reference — releasing one
        // it never took would drop a count belonging to the owner, i.e. a use-after-free.
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;

        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);

        context.AbandonBeforeSubmit();

        // No ref was taken, so the count is untouched and teardown must still succeed.
        consumer.Dispose();
        Assert.True(handle.IsClosed);
    }

    [Fact]
    public void Callback_WithUnexpectedContext_DoesNotUnwindIntoNative()
    {
        // No-throw boundary: an exception INSIDE the callback body (here an
        // InvalidCastException from a wrong-typed user_data) must be caught, never
        // propagated — an unwind into the native dispatcher frame is UB. The delegate
        // must return normally. (The "faults the Task" half of the no-throw contract
        // is covered by the seek FAILURE path above.)
        GCHandle badHandle = GCHandle.Alloc("not a completion source", GCHandleType.Normal);
        try
        {
            // Must not throw.
            ConsumerCallbacks.Operation(IntPtr.Zero, GCHandle.ToIntPtr(badHandle));
        }
        finally
        {
            badHandle.Free();
        }
    }
}
