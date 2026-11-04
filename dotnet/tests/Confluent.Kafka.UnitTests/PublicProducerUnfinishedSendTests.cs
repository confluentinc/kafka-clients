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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

using Confluent.Kafka.Producer;
using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The future of a sync <see cref="KafkaProducer{TKey, TValue}"/> send that has returned but not completed: its
/// delivery waits behind an earlier record's callback, which holds the completion thread. The record's outcome on an
/// unreachable broker is the metadata timeout.
/// </summary>
public sealed class PublicProducerUnfinishedSendTests
{
    private const int MaxBlockMs = 500;

    [Fact]
    public void Cancel_OnAnUnfinishedSend_ReturnsFalse_AndTheRecordStillCompletes()
    {
        WithUnfinishedSend((future, callback, release) =>
        {
            Assert.False(future.Cancel(true));
            Assert.False(future.IsCancelled);
            Assert.False(future.IsDone);

            release();
            KafkaException failure = Assert.Throws<KafkaException>(() => future.Get(Deadline));
            Assert.Equal(7, failure.Code);
            Assert.False(future.IsCancelled);
            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Same(failure, callback.Delivered);
        });
    }

    [Fact]
    public void GetWithAZeroTimeout_OnAnUnfinishedSend_ThrowsTimeout_AndALaterGetStillAnswers()
    {
        WithUnfinishedSend((future, callback, release) =>
        {
            Assert.IsType<TimeoutException>(Record(() => future.Get(TimeSpan.Zero)));
            Assert.False(future.IsDone);

            release();
            KafkaException failure = Assert.Throws<KafkaException>(() => future.Get(Deadline));
            Assert.Equal(7, failure.Code);
            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Equal(1, callback.Calls);
        });
    }

    [Fact]
    public void ThenApply_OnASend_CompletesOnTheCompletionThread()
    {
        WithUnfinishedSend((future, callback, release) =>
        {
            string? appliedOn = null;
            using ManualResetEventSlim applied = new ManualResetEventSlim();
            KafkaFuture<long> offset = future.ThenApply(metadata => metadata.Offset);
            _ = offset.WhenComplete((_, _) =>
            {
                appliedOn = Thread.CurrentThread.Name;
                applied.Set();
            });
            Assert.False(applied.IsSet);

            release();
            Assert.True(applied.Wait(Deadline), "the derived future never completed");
            Assert.Equal(NativeProducer.CompletionThreadName, appliedOn);
            Assert.Equal(7, Assert.Throws<KafkaException>(() => offset.Get(Deadline)).Code);
        });
    }

    [Fact]
    public void GetWithAToken_EndsOnlyThatWait_ASecondGetAndTheCallbackStillComplete()
    {
        WithUnfinishedSend((future, callback, release) =>
        {
            using CancellationTokenSource cts = new CancellationTokenSource();
            Task<Exception?> wait = Task.Factory.StartNew(
                () => Record(() => future.Get(cts.Token)),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            Thread.Sleep(Settle);
            Assert.False(wait.IsCompleted, "Get(token) did not wait");

            cts.Cancel();
            Assert.True(wait.Wait(Deadline), "Get(token) did not end when its token fired");
            OperationCanceledException canceled = Assert.IsAssignableFrom<OperationCanceledException>(wait.Result);
            Assert.Equal(cts.Token, canceled.CancellationToken);
            Assert.False(future.IsDone);
            Assert.False(future.IsCancelled);

            release();
            Assert.Equal(7, Assert.Throws<KafkaException>(() => future.Get(Deadline)).Code);
            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Equal(1, callback.Calls);
        });
    }

    [Fact]
    public void DeliveryCallback_RunsBeforeItsFutureCompletes()
    {
        bool doneInCallback = true;
        bool getReturnedInCallback = true;
        Task? blockedGet = null;
        KafkaFuture<RecordMetadata> probed = default;
        WithUnfinishedSend(
            (future, callback, release) =>
            {
                probed = future;
                blockedGet = Task.Factory.StartNew(
                    () => Record(() => future.Get(Deadline)),
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
                Thread.Sleep(Settle);
                Assert.False(blockedGet.IsCompleted, "Get did not wait for the delivery");

                release();
                Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
                Assert.True(blockedGet.Wait(Deadline), "Get never returned");
            },
            () =>
            {
                doneInCallback = probed.IsDone;
                getReturnedInCallback = blockedGet!.IsCompleted;
            });

        Assert.False(doneInCallback, "the future completed before the delivery callback ran");
        Assert.False(getReturnedInCallback, "a blocked Get returned before the delivery callback ran");
    }

    [Fact]
    public async Task Cancel_OnAnUnfinishedAsyncSend_ReturnsFalse_AndTheRecordStillCompletes()
    {
        await WithUnfinishedAsyncSend(async (producer, future, callback) =>
        {
            Assert.False(future.Cancel(true));
            Assert.False(future.IsCancelled);
            Assert.False(future.IsDone);

            Assert.True(producer.CompleteNext());
            RecordMetadata metadata = await TestTimeout.Run(() => future.Get(), Deadline);
            Assert.Equal(0L, metadata.Offset);
            Assert.False(future.IsCancelled);
            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Equal(1, callback.Calls);
            Assert.Null(callback.Delivered);
        });
    }

    [Fact]
    public async Task GetWithAZeroTimeout_OnAnUnfinishedAsyncSend_ThrowsTimeout_AndALaterGetStillAnswers()
    {
        await WithUnfinishedAsyncSend(async (producer, future, callback) =>
        {
            _ = await Assert.ThrowsAsync<TimeoutException>(() => future.Get(TimeSpan.Zero));
            Assert.False(future.IsDone);

            Assert.True(producer.CompleteNext());
            RecordMetadata metadata = await TestTimeout.Run(() => future.Get(), Deadline);
            Assert.Equal(0L, metadata.Offset);
            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Equal(1, callback.Calls);
        });
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task ThenApply_OnASuccessfulMockSend_AppliesOnTheProducersCompletionThread(string flavor)
    {
        TaskCompletionSource<string?> appliedOn =
            new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        string topic = Topic("then-apply-success");
        if (flavor == SyncFlavor)
        {
            MockProducer<byte[], byte[]> producer =
                new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
            using IDisposable producerDisposal = TestTimeout.Disposal(producer);
            KafkaFuture<long> offset = producer.Send(UnreachableProducers.Record(topic)).ThenApply(metadata => metadata.Offset);
            _ = offset.WhenComplete((_, _) => appliedOn.TrySetResult(Thread.CurrentThread.Name));
            Assert.False(appliedOn.Task.IsCompleted);

            Assert.True(producer.CompleteNext());
            Assert.Equal(NativeProducer.CompletionThreadName, await TestTimeout.Run(() => appliedOn.Task, Deadline));
            Assert.Equal(0L, offset.Get(Deadline));
        }
        else
        {
            AsyncMockProducer<byte[], byte[]> producer =
                new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
            await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
            AsyncKafkaFuture<RecordMetadata> future =
                await TestTimeout.Run(() => producer.Send(UnreachableProducers.Record(topic)).AsTask(), Deadline);
            AsyncKafkaFuture<long> offset = future.ThenApply(metadata => metadata.Offset);
            _ = offset.WhenComplete((_, _) => appliedOn.TrySetResult(Thread.CurrentThread.Name));
            Assert.False(appliedOn.Task.IsCompleted);

            Assert.True(producer.CompleteNext());
            Assert.Equal(NativeProducer.PumpThreadName, await TestTimeout.Run(() => appliedOn.Task, Deadline));
            Assert.Equal(0L, await TestTimeout.Run(() => offset.Get(), Deadline));
        }
    }

    [Fact]
    public async Task DeliveryCallback_OnAnAsyncSend_RunsBeforeItsFutureCompletes()
    {
        bool doneInCallback = true;
        bool getReturnedInCallback = true;
        AsyncKafkaFuture<RecordMetadata> probed = default;
        Task<RecordMetadata>? pendingGet = null;
        await WithUnfinishedAsyncSend(
            async (producer, future, callback) =>
            {
                probed = future;
                pendingGet = future.Get();
                await Task.Delay(Settle);
                Assert.False(pendingGet.IsCompleted, "Get did not wait for the delivery");

                Assert.True(producer.CompleteNext());
                Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
                _ = await TestTimeout.Run(() => pendingGet, Deadline);
            },
            () =>
            {
                doneInCallback = probed.IsDone;
                getReturnedInCallback = pendingGet!.IsCompleted;
            });

        Assert.False(doneInCallback, "the future completed before the delivery callback ran");
        Assert.False(getReturnedInCallback, "a pending Get returned before the delivery callback ran");
    }

    private static Exception? Record(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
    }

    private static async Task WithUnfinishedAsyncSend(
        Func<AsyncMockProducer<byte[], byte[]>, AsyncKafkaFuture<RecordMetadata>, ProbeCallback, Task> test,
        Action? inCallback = null)
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProbeCallback callback = new ProbeCallback((_, _) => inCallback?.Invoke());
        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(
            () => producer.Send(UnreachableProducers.Record(Topic("unfinished-async-send")), callback).AsTask(), Deadline);
        Assert.False(future.IsDone, "the manual mock completed the send before CompleteNext");
        await test(producer, future, callback);
        Assert.Null(callback.BodyFailure);
    }

    private static void WithUnfinishedSend(
        Action<KafkaFuture<RecordMetadata>, ProbeCallback, Action> test,
        Action? inCallback = null)
    {
        KafkaProducer<byte[], byte[]> producer = Sync(MaxBlockMs);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        HoldingCallback hold = new HoldingCallback();
        ProbeCallback callback = new ProbeCallback((_, _) => inCallback?.Invoke());
        try
        {
            _ = producer.Send(UnreachableProducers.Record(Topic("unfinished-send-hold")), hold);
            Assert.True(hold.WaitEntered(Deadline), "the holding callback never ran");

            KafkaFuture<RecordMetadata> future = producer.Send(UnreachableProducers.Record(Topic("unfinished-send")), callback);
            Assert.False(future.IsDone, "the send's future completed while the completion thread was held");
            test(future, callback, hold.Release);
            Assert.Null(callback.BodyFailure);
        }
        finally
        {
            hold.Release();
        }
    }
}
