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
/// <see cref="OperationCompletionSource{TResult}"/> runs its continuations off the completing thread, and the void
/// <see cref="OperationCompletionSource"/> completes its Task on a null error.
/// </summary>
public sealed class ConsumerPollBridgeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread()
    {
        // The continuation is attached before completion, so without RunContinuationsAsynchronously it would run
        // inline on the completing thread.
        OperationCompletionSource<ConsumerRecords<byte[], byte[]>> context = new OperationCompletionSource<ConsumerRecords<byte[], byte[]>>();
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
                context.CompleteWithResult(new ConsumerRecords<byte[], byte[]>(Array.Empty<ConsumerRecord<byte[], byte[]>>()));
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
    public async Task ResultBridge_VoidPathUnchanged_SubscribeStillResolves()
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            context.Complete(IntPtr.Zero);
            await TestTimeout.Run(() => context.Task, s_deadline);

            // TaskStatus rather than IsCompletedSuccessfully, which net462 lacks.
            Assert.Equal(System.Threading.Tasks.TaskStatus.RanToCompletion, context.Task.Status);
        }
        finally
        {
            context.FreeGcHandle();
        }
    }
}
