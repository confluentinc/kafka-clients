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
/// The <b>generic</b> owned-handle bridge (<see cref="OperationCompletionSource{TResult}"/>)
/// behavior: <c>RunContinuationsAsynchronously</c> off the completing thread, and the
/// poll trampoline's no-throw boundary (ffi-marshalling.md §B6/§B7). Complements the
/// void-bridge coverage in <c>ConsumerCompletionBridgeTests</c> — the generic migration
/// must not regress either.
/// </summary>
public sealed class ConsumerPollBridgeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread()
    {
        // Drive the result bridge context directly so the continuation is attached
        // BEFORE completion. An ExecuteSynchronously continuation would run INLINE on
        // the completing thread if the TCS were NOT built with
        // RunContinuationsAsynchronously; the flag forces it onto the thread pool — so a
        // different thread id AND IsThreadPoolThread prove it. This is the result-
        // returning analog of the void-bridge continuation test.
        OperationCompletionSource<ConsumerRecords> context = new OperationCompletionSource<ConsumerRecords>();
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
                context.CompleteWithResult(new ConsumerRecords(Array.Empty<ConsumerRecord>()));
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
    public void PollCallback_WithUnexpectedContext_DoesNotUnwindIntoNative()
    {
        // No-throw boundary: an exception INSIDE the poll callback body (here an
        // InvalidCastException from a wrong-typed user_data) must be caught, never
        // propagated — an unwind into the native dispatcher frame is UB. The delegate
        // must return normally. records == IntPtr.Zero so the null-safe destroy in the
        // finally is a no-op.
        GCHandle badHandle = GCHandle.Alloc("not a completion source", GCHandleType.Normal);
        try
        {
            // Must not throw.
            ConsumerCallbacks.Poll(IntPtr.Zero, IntPtr.Zero, GCHandle.ToIntPtr(badHandle));
        }
        finally
        {
            badHandle.Free();
        }
    }

    [Fact]
    public async Task ResultBridge_VoidPathUnchanged_SubscribeStillResolves()
    {
        // Regression guard for the generic migration (PLAN decision 4): the void bridge
        // is now OperationCompletionSource : OperationCompletionSource<bool>, and its
        // observable success (null error → Task completes) must be byte-for-byte
        // unchanged. A direct completion of the void context with a null error resolves
        // the non-generic Task.
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            context.Complete(IntPtr.Zero); // null error = success
            await TestTimeout.Run(() => context.Task, s_deadline);
            Assert.True(context.Task.IsCompletedSuccessfully);
        }
        finally
        {
            context.FreeGcHandle();
        }
    }
}
