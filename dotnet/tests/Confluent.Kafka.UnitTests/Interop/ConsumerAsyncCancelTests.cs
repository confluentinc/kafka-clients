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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The async consumer's drain thread, trampolines and in-flight uses, and what a cancelled token does to each
/// operation.
/// </summary>
public sealed class ConsumerAsyncCancelTests
{
    private const string Topic = "test";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_tp1 = new TopicPartition(Topic, 1);

    [Fact]
    public async Task ACompletion_RunsOnThePumpThread()
    {
        NativeConsumer native = NativeConsumer.CreateMock();
        using IDisposable nativeDisposal = TestTimeout.Disposal(native);
        ConsumerCompletion<string?> completion = new ConsumerCompletion<string?>((_, _) => Thread.CurrentThread.Name);

        NativeMethods.ConsumerUnsubscribeCb(native.Handle, ConsumerCompletion.Void, completion.Root());

        Assert.Equal("confluent-kafka-consumer-pump", await WithDeadline(completion.Result));
    }

    [Fact]
    public async Task ACompletion_RunsItsContinuationsAsynchronously()
    {
        ConsumerCompletion<bool> completion = new ConsumerCompletion<bool>((_, _) => true);
        IntPtr opaque = completion.Root();
        Thread deliverer = new Thread(() => ConsumerCompletion.Void(IntPtr.Zero, opaque));
        Task<int> continuation = completion.Result.ContinueWith(
            _ => Environment.CurrentManagedThreadId,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        deliverer.Start();
        deliverer.Join();

        Assert.NotEqual(deliverer.ManagedThreadId, await WithDeadline(continuation));
    }

    [Fact]
    public void TheTrampolines_DoNotThrow_OnAnUnusableOpaque()
    {
        NativeMethods.ConsumerVoidCallback closed = (NativeMethods.ConsumerVoidCallback)typeof(NativeConsumer)
            .GetField("s_closed", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

        ConsumerCompletion.Void(IntPtr.Zero, IntPtr.Zero);
        ConsumerCompletion.Scalar(1, IntPtr.Zero, IntPtr.Zero);
        ConsumerCompletion.Pointer(IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
        closed(IntPtr.Zero, IntPtr.Zero);
    }

    [Fact]
    public async Task AThrowingConversion_FaultsTheResult()
    {
        InvalidCastException thrown = new InvalidCastException("conversion");
        ConsumerCompletion<long> completion = new ConsumerCompletion<long>((_, _) => throw thrown);

        ConsumerCompletion.Scalar(7, IntPtr.Zero, completion.Root());

        Assert.Same(thrown, await Assert.ThrowsAsync<InvalidCastException>(() => WithDeadline(completion.Result)));
    }

    [Fact]
    public void OnlyTheOperationsJavasWakeupInterrupts_AreWakeable()
    {
        string[] wakeable = Enum.GetValues(typeof(NativeConsumer.AsyncOperation))
            .Cast<NativeConsumer.AsyncOperation>()
            .Where(NativeConsumer.IsWakeable)
            .Select(operation => operation.ToString())
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[] { "Commit", "Committed", "ListTopics", "PartitionsFor", "Poll", "Position" },
            wakeable);
    }

    [Fact]
    public async Task ACancelledSubscribe_SendsNoWakeup()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await AssertCancelSendsNoWakeup(consumer, token => consumer.Subscribe(new[] { Topic }, token));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task ACancelledUnsubscribe_SendsNoWakeup()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Subscribe(new[] { Topic }));
            await AssertCancelSendsNoWakeup(consumer, token => consumer.Unsubscribe(token));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task ACancelledWakeableOperation_PostsItsWakeDecisionToThePump()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Assign(new[] { s_tp0 }));
            await WithDeadline(consumer.Seek(s_tp0, 0));
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task<long> position;
            using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
            {
                position = consumer.Position(s_tp0, cancel.Token);
                cancel.Cancel();
                Assert.Equal(1, stall.Posted);
            }

            await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(position));
            Assert.Empty(await WithDeadline(consumer.Poll(TimeSpan.Zero)));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public Task ACancelledOffsetsForTimes_SendsNoWakeup() =>
        AssertPendingCancelSendsNoWakeup((consumer, token) =>
            consumer.OffsetsForTimes(new Dictionary<TopicPartition, long> { [s_tp0] = 0L }, token));

    [Fact]
    public Task ACancelledBeginningOffsets_SendsNoWakeup() =>
        AssertPendingCancelSendsNoWakeup((consumer, token) => consumer.BeginningOffsets(new[] { s_tp0 }, token));

    [Fact]
    public Task ACancelledEndOffsets_SendsNoWakeup() =>
        AssertPendingCancelSendsNoWakeup((consumer, token) => consumer.EndOffsets(new[] { s_tp0 }, token));

    [Fact]
    public Task ACancelledCommitAsync_SendsNoWakeup() =>
        AssertAssignedCancelSendsNoWakeup((consumer, token) => consumer.CommitAsync(token));

    [Fact]
    public Task ACancelledCurrentLag_SendsNoWakeup() =>
        AssertAssignedCancelSendsNoWakeup((consumer, token) => consumer.CurrentLag(s_tp0, token));

    [Fact]
    public Task ACancelledSeek_SendsNoWakeup() =>
        AssertAssignedCancelSendsNoWakeup((consumer, token) => consumer.Seek(s_tp0, 3L, token));

    [Fact]
    public Task ACancelledSeekWithMetadata_SendsNoWakeup() =>
        AssertAssignedCancelSendsNoWakeup((consumer, token) => consumer.Seek(s_tp0, new OffsetAndMetadata(3L), token));

    [Fact]
    public async Task CancellingAnAsyncPoll_WakesTheConsumerAndLetsItEnd()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Subscribe(new[] { Topic }));
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task<ConsumerRecords<byte[], byte[]>> poll = consumer.Poll(TimeSpan.FromSeconds(30), cancel.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            Stopwatch started = Stopwatch.StartNew();

            cancel.Cancel();

            OperationCanceledException canceled =
                await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(poll));
            Assert.True(started.Elapsed < TimeSpan.FromSeconds(5), started.Elapsed.ToString());
            Assert.Equal(cancel.Token, canceled.CancellationToken);
            Assert.Equal(new[] { Topic }, consumer.Subscription());
            Assert.Empty(await WithDeadline(consumer.Poll(TimeSpan.Zero)));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task CancellingACallWhoseCompletionIsQueued_LeavesNoWakeupArmed()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Assign(new[] { s_tp0 }));
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task commit;
            using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
            {
                commit = consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>(), cancel.Token);
                stall.WaitUntilSignaled();
                cancel.Cancel();
            }

            await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(commit));
            Assert.Empty(await WithDeadline(consumer.Poll(TimeSpan.Zero)));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task CancellingAnAsyncPollReturningRecords_SeeksThemBack()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Assign(new[] { s_tp0, s_tp1 }));
            await WithDeadline(consumer.Seek(s_tp0, 0));
            await WithDeadline(consumer.Seek(s_tp1, 0));
            AddFetched(consumer);
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task<ConsumerRecords<byte[], byte[]>> poll;
            using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
            {
                poll = consumer.Poll(TimeSpan.FromSeconds(30), cancel.Token);
                stall.WaitUntilSignaled();
                cancel.Cancel();
            }

            await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(poll));
            Assert.Equal(3L, await WithDeadline(consumer.Position(s_tp0)));
            Assert.Equal(7L, await WithDeadline(consumer.Position(s_tp1)));
            AddFetched(consumer);
            Assert.Equal(
                new[] { (0, 3L), (0, 4L), (1, 7L) },
                (await WithDeadline(consumer.Poll(TimeSpan.FromSeconds(30))))
                    .Select(record => (record.Partition, record.Offset))
                    .OrderBy(fetched => fetched)
                    .ToArray());
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task DisposeDuringAnOperationWhoseCompletionIsQueued_ClosesOnlyAfterTheCompletionRan()
    {
        Thread warmUp = new Thread(new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray).Dispose);
        warmUp.IsBackground = true;
        warmUp.Start();
        Assert.True(warmUp.Join(s_deadline));

        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        SafeConsumerHandle handle = HandleOf(consumer);
        Task unsubscribe;
        Thread disposing = new Thread(consumer.Dispose);
        disposing.IsBackground = true;
        using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
        {
            unsubscribe = consumer.Unsubscribe();
            stall.WaitUntilSignaled();
            disposing.Start();
            WaitUntilParked(disposing);

            KafkaException woken = Assert.IsType<KafkaException>(PollTheCore(handle));
            Assert.Equal(-18, woken.Code);
            Assert.False(unsubscribe.IsCompleted);
        }

        Assert.True(disposing.Join(s_deadline));
        await WithDeadline(unsubscribe);
    }

    [Fact]
    public async Task DisposeDuringAPendingOperation_Returns_AndTheOperationEndsWithItsOwnError()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        await WithDeadline(consumer.Assign(new[] { s_tp0 }));
        Task<IReadOnlyDictionary<TopicPartition, long>> pending = consumer.BeginningOffsets(new[] { s_tp0 });
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        Assert.False(pending.IsCompleted);

        await WithDeadline(Task.Run(consumer.Dispose));

        Assert.True(pending.IsCompleted);
        KafkaException ended = await Assert.ThrowsAsync<KafkaException>(() => pending);
        Assert.Equal(7, ended.Code);
        Assert.Equal("Failed to get offsets by times in 1500ms", ended.Message);
    }

    private static Dictionary<string, string> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "async-cancel",
        ["default.api.timeout.ms"] = "1500",
    };

    private static void AddFetched(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        consumer.AddRecord(Topic, 0, 3, new byte[] { 1 }, new byte[] { 3 });
        consumer.AddRecord(Topic, 0, 4, new byte[] { 1 }, new byte[] { 4 });
        consumer.AddRecord(Topic, 1, 7, new byte[] { 1 }, new byte[] { 7 });
    }

    private static async Task AssertCancelSendsNoWakeup(
        AsyncMockConsumer<byte[], byte[]> consumer, Func<CancellationToken, Task> operation)
    {
        using CancellationTokenSource cancel = new CancellationTokenSource();
        Task running;
        using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
        {
            running = operation(cancel.Token);
            cancel.Cancel();
            Assert.Equal(0, stall.Posted);
        }

        await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(running));
        Assert.Empty(await WithDeadline(consumer.Poll(TimeSpan.Zero)));
    }

    private static async Task AssertAssignedCancelSendsNoWakeup(
        Func<AsyncMockConsumer<byte[], byte[]>, CancellationToken, Task> operation)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Assign(new[] { s_tp0 }));
            await WithDeadline(consumer.Seek(s_tp0, 0));
            await AssertCancelSendsNoWakeup(consumer, token => operation(consumer, token));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static async Task AssertPendingCancelSendsNoWakeup(
        Func<AsyncKafkaConsumer<byte[], byte[]>, CancellationToken, Task> operation)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await WithDeadline(consumer.Assign(new[] { s_tp0 }));
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task running = operation(consumer, cancel.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            Assert.False(running.IsCompleted);

            cancel.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(running));
            Assert.Empty(await WithDeadline(consumer.Poll(TimeSpan.Zero)));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static void WaitUntilParked(Thread thread)
    {
        Stopwatch waited = Stopwatch.StartNew();
        while ((thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) == 0)
        {
            Assert.True(waited.Elapsed < s_deadline, "The thread did not park within " + s_deadline + ".");
            Thread.Yield();
        }
    }

    private static SafeConsumerHandle HandleOf(object consumer) =>
        ((NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!).Handle;

    private static Exception? PollTheCore(SafeConsumerHandle handle)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr records = NativeMethods.ConsumerPoll(handle, 0, ref outErr);
        Exception? error = KafkaException.FromFfiError(outErr, owned: true);
        IntPtr destroyErr = IntPtr.Zero;
        NativeMethods.ConsumerRecordsDestroy(records, ref destroyErr);
        Assert.Equal(IntPtr.Zero, destroyErr);
        return error;
    }

    private static async Task<T> WithDeadline<T>(Task<T> task)
    {
        await WithDeadline((Task)task);
        return await task;
    }

    private static async Task WithDeadline(Task task)
    {
        if (await Task.WhenAny(task, Task.Delay(s_deadline)) != task)
        {
            throw new TimeoutException("The operation did not end within " + s_deadline + ".");
        }

        await task;
    }
}
