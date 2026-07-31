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
/// (ffi-marshalling.md §B6/§B7), exercised through the two proof ops on a
/// <c>MockConsumer</c>: <c>subscribe_async</c> (SUCCESS — null error) and
/// <c>seek_async</c> on an unassigned partition (FAILURE — a genuine broker-free
/// <c>IllegalState</c> error). Both share ONE void-result bridge. Every awaited op
/// is wrapped in a <see cref="TestTimeout"/> hang guard so a bridge that never
/// completes fails the run fast.
/// </summary>
public sealed class ConsumerCompletionBridgeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Broker-free error codes are all indistinct (UnknownServerError == -1); tests
    // assert on TYPE + Message + reusability, never on a distinctive Code.
    private const int UnknownServerErrorCode = -1;

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task SubscribeAsync_OnMock_ResolvesSuccessfully()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(() => consumer.SubscribeAsync(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task SubscribeAsync_Churned_ResolvesEachTime_NoCorruption()
    {
        // The loop is the corruption detector: a double-free of the GCHandle /
        // error handle, or a leak, would corrupt the allocator over many ops.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        for (int i = 0; i < 200; i++)
        {
            await TestTimeout.Run(() => consumer.SubscribeAsync(ProofTopic()), s_deadline);
        }
    }

    [Fact]
    public async Task SeekAsync_UnassignedPartition_FaultsWithKafkaException()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.SeekAsync("proof-topic", 0, 0L), s_deadline));

        // Assert TYPE + Code (indistinct -1) + flags + a non-empty Message — never a
        // distinctive code (PLAN finding #4).
        Assert.Equal(UnknownServerErrorCode, ex.Code);
        Assert.False(ex.IsRetriable);
        Assert.False(ex.IsFatal);
        Assert.False(string.IsNullOrEmpty(ex.Message));
    }

    [Fact]
    public async Task SeekAsync_Churned_FaultsEachTime_NoCorruption()
    {
        // The error path frees an owned KafkaError handle + the GCHandle each time;
        // churn to catch a double-free / leak on the failure branch.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        for (int i = 0; i < 50; i++)
        {
            await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.SeekAsync("proof-topic", 0, 0L), s_deadline));
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
        OperationCompletionSource context = new OperationCompletionSource(guard: null);
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
                await consumer.SubscribeAsync(ProofTopic());
                await consumer.SubscribeAsync(ProofTopic());
                await consumer.SubscribeAsync(ProofTopic());
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
            Task op = consumer.SubscribeAsync(ProofTopic());
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            await TestTimeout.Run(() => op, s_deadline);
        }
    }

    [Fact]
    public async Task FaultTaskOnly_FaultsTheTask()
    {
        // The sync-Dispose primitive (ffi §B7 / Critic N=5 Finding 3): when a
        // guard-rejected close cannot drain and destroy cancels the op's callback,
        // Dispose faults the awaiter here so the Task cannot strand (the Finding-1
        // strand fix). FaultTaskOnly does NOT free the rooting GCHandle — the callback
        // is the sole owner (proved by the weak-ref test below); the callback never
        // fired here, so this test frees the still-rooted handle itself.
        OperationCompletionSource context = new OperationCompletionSource(guard: null);
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        try
        {
            context.SetGcHandle(gcHandle);

            var expected = new ObjectDisposedException("consumer");
            context.FaultTaskOnly(expected);

            ObjectDisposedException actual = await Assert.ThrowsAsync<ObjectDisposedException>(
                () => TestTimeout.Run(() => context.Task, s_deadline));
            Assert.Same(expected, actual);
        }
        finally
        {
            // FaultTaskOnly did NOT free the handle (callback is the sole owner, and no
            // callback fired here), so the test owns the free.
            context.FreeGcHandle();
        }
    }

    [Fact]
    public async Task FaultTaskOnly_AfterCompletion_IsNoOp()
    {
        // Race-safety: if the callback already completed the op before Dispose ran,
        // FaultTaskOnly must NOT re-complete. TrySetException no-ops on a completed
        // Task. The callback (driven through the real FromIntPtr path) is the sole
        // owner of the GCHandle free, so no free happens here.
        OperationCompletionSource context = new OperationCompletionSource(guard: null);
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);

        // Callback fired first through the real dispatcher path: success + frees the
        // handle (sole owner).
        ConsumerCallbacks.Operation(IntPtr.Zero, GCHandle.ToIntPtr(gcHandle));

        // Dispose runs FaultTaskOnly afterward: harmless no-op — the result stands.
        context.FaultTaskOnly(new ObjectDisposedException("consumer"));

        await TestTimeout.Run(() => context.Task, s_deadline); // still RanToCompletion
        Assert.Equal(TaskStatus.RanToCompletion, context.Task.Status);
    }

    [Fact]
    public async Task FaultTaskOnly_ThenCallbackFires_CaseB_IsSafe()
    {
        // Case B (Critic N=5 Finding 3): the completion job was queued before destroy,
        // so the dispatcher runs it AFTER the sync Dispose faulted the Task. Because
        // FaultTaskOnly does NOT free the GCHandle, the straggler callback recovers a
        // LIVE context via GCHandle.FromIntPtr(userData).Target — the exact path
        // Finding 3 flagged — so there is no use-after-free. It completes safely
        // (TrySetResult no-ops on the faulted Task) and frees the handle exactly once.
        OperationCompletionSource context = new OperationCompletionSource(guard: null);
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);

        var expected = new ObjectDisposedException("consumer");
        context.FaultTaskOnly(expected); // sync Dispose faults the awaiter, keeps the handle

        // The straggler callback fires through the REAL FromIntPtr recovery path: it
        // must not crash, must not double-complete, and frees the handle (sole owner).
        ConsumerCallbacks.Operation(IntPtr.Zero, GCHandle.ToIntPtr(gcHandle));

        ObjectDisposedException actual = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => TestTimeout.Run(() => context.Task, s_deadline));
        Assert.Same(expected, actual); // the fault stands

        // The callback already freed the handle; a redundant free is the idempotent
        // no-op (proves no double-free / throw).
        context.FreeGcHandle();
    }

    [Fact]
    public void FaultTaskOnly_DoesNotUnrootContext_TheCallbackDoes()
    {
        // The inverse of the old reclaim proof: FaultTaskOnly (sync Dispose) must NOT
        // free the rooting GCHandle — the completion callback is the sole owner of the
        // free. So after FaultTaskOnly the context stays rooted (alive through GC);
        // only the straggler callback (case B) unroots it, via the real FromIntPtr
        // path (proving that path recovers a live handle, not a freed one).
        (WeakReference weak, IntPtr userData) = FaultTaskOnlyAndReturnWeakRef();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Still rooted by the un-freed GCHandle: FaultTaskOnly did not free it.
        Assert.True(weak.IsAlive);

        // The straggler callback fires (case B): recovers the live context via
        // FromIntPtr (no use-after-free), completes as a no-op on the faulted Task,
        // and frees the GCHandle exactly once.
        ConsumerCallbacks.Operation(IntPtr.Zero, userData);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // The callback freed the handle → the context is now unrooted and collected.
        Assert.False(weak.IsAlive);
    }

    // Kept out-of-line + non-inlined so the strong reference to the context genuinely
    // leaves scope before the caller forces GC (an inlined body could keep it rooted).
    // Returns the user_data IntPtr so the caller can drive the real callback path.
    [System.Runtime.CompilerServices.MethodImpl(
        System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static (WeakReference, IntPtr) FaultTaskOnlyAndReturnWeakRef()
    {
        OperationCompletionSource context = new OperationCompletionSource(guard: null);
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);

        // FaultTaskOnly faults the Task but does NOT free the GCHandle (the callback
        // is the sole owner). Observe the faulted Task so its exception is not left
        // unobserved, then drop every managed strong reference — only the GCHandle
        // keeps the context alive.
        context.FaultTaskOnly(new ObjectDisposedException("consumer"));
        _ = context.Task.Exception;

        return (new WeakReference(context), GCHandle.ToIntPtr(gcHandle));
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
