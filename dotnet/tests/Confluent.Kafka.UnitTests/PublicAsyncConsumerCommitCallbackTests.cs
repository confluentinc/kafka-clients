// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async consumers' commit callbacks, <see cref="IAsyncOffsetCommitCallback"/> above all, against an unreachable
/// broker and the mock: the thread they run on, the call that waits for them, their order, how their exception
/// leaves that call, and a cancel or a <c>Dispose</c> while one runs.
/// </summary>
public sealed class PublicAsyncConsumerCommitCallbackTests
{
    private const string Topic = "t";

    private const string PumpThreadName = "confluent-kafka-consumer-pump";

    private const int InvalidGroupIdCode = 24;

    private const int WakeupCode = -18;

    private const string NoGroupId =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_notYetWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Theory]
    [InlineData("plain")]
    [InlineData("coroutine")]
    public async Task CommitCallback_RunsOnThePumpThread(string kind)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            List<string?> seen = new List<string?>();
            if (kind == "plain")
            {
                await consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => seen.Add(Thread.CurrentThread.Name)));
            }
            else
            {
                await consumer.CommitAsync(Empty(), new AsyncDelegateCallback((_, _) =>
                {
                    seen.Add(Thread.CurrentThread.Name);
                    return Task.CompletedTask;
                }));
            }

            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            Assert.Equal(PumpThreadName, Assert.Single(seen));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task CommitAsync_WithoutAGroupId_FaultsItsOwnTask_AndCloseDoesNotThrow()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer(groupId: null);
        try
        {
            Task plain = consumer.CommitAsync();
            KafkaException e = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => plain, s_deadline));
            Assert.Equal(InvalidGroupIdCode, e.Code);
            Assert.Equal(NoGroupId, e.Message);

            Task withAsyncCallback = consumer.CommitAsync(Offsets(1), new AsyncDelegateCallback((_, _) => Task.CompletedTask));
            e = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => withAsyncCallback, s_deadline));
            Assert.Equal(InvalidGroupIdCode, e.Code);
            Assert.Equal(NoGroupId, e.Message);

            Task withCallback = consumer.CommitAsync(Offsets(1), new DelegateCallback((_, _) => { }));
            e = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => withCallback, s_deadline));
            Assert.Equal(InvalidGroupIdCode, e.Code);
            Assert.Equal(NoGroupId, e.Message);

            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("coroutine")]
    public async Task ARaisingCommitCallback_PropagatesFromCommitAsync_AndCloseDoesNotRaiseItAgain(string kind)
    {
        CommitCallbackException raised = new CommitCallbackException("callback boom");
        List<string> events = new List<string>();
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            if (kind == "plain")
            {
                await consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    Add(events, "boom");
                    throw raised;
                }));
            }
            else
            {
                await consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
                {
                    await Task.Yield();
                    Add(events, "boom");
                    throw raised;
                }));
            }

            Assert.Empty(Snapshot(events));

            CommitCallbackException propagated = await Assert.ThrowsAsync<CommitCallbackException>(
                () => TestTimeout.Run(() => consumer.CommitAsync(Empty()), s_deadline));
            Assert.Same(raised, propagated);
            Assert.Equal(new[] { "boom" }, Snapshot(events));

            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            Assert.Equal(new[] { "boom" }, Snapshot(events));
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task CancellingACommitAsync_WhileItAwaitsTheAsyncCallbackItDelivered_LeavesNoWakeupArmed()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            TaskCompletionSource<bool> started = NewSignal();
            TaskCompletionSource<bool> release = NewSignal();
            int ended = 0;
            await consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
            {
                started.TrySetResult(true);
                await release.Task;
                Volatile.Write(ref ended, 1);
            }));

            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task running = consumer.CommitAsync(Empty(), cancellationToken: cancel.Token);
            await TestTimeout.Run(() => started.Task, s_deadline);
            cancel.Cancel();
            ReleaseAfter(release, TimeSpan.FromMilliseconds(50));

            OperationCanceledException canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => TestTimeout.Run(() => running, s_deadline));

            Assert.Equal(cancel.Token, canceled.CancellationToken);
            Assert.Equal(1, Volatile.Read(ref ended));
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("expired_poll", 0, 50)]
    [InlineData("deadline_passes_in_callback", 300, 600)]
    [InlineData("poll", 30000, 50)]
    [InlineData("commit", -1, 50)]
    public async Task CancellingACall_WhileItAwaitsTheAsyncCallbackItDelivered_EndsIt_AndTheNextPollReturns(
        string call,
        int timeoutMs,
        int releaseAfterMs)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            TaskCompletionSource<bool> started = NewSignal();
            TaskCompletionSource<bool> release = NewSignal();
            await consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
            {
                started.TrySetResult(true);
                await release.Task;
            }));

            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task running = timeoutMs < 0
                ? consumer.Commit(Empty(), cancel.Token)
                : consumer.Poll(TimeSpan.FromMilliseconds(timeoutMs), cancel.Token);
            await TestTimeout.Run(() => started.Task, s_deadline);
            cancel.Cancel();
            ReleaseAfter(release, TimeSpan.FromMilliseconds(releaseAfterMs));
            Stopwatch watch = Stopwatch.StartNew();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => running, s_deadline));

            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"{call}: the cancelled call ended after {watch.Elapsed}");
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AUserWakeup_AfterACancelledCommitLeftTheConsumersOwnWakeArmed_IsRaisedByTheNextPoll()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            await CancelACommitAwaitingTheAsyncCallbackItDelivered(consumer, afterTheOwnWake: null);

            consumer.Wakeup();

            KafkaException woken = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Equal(WakeupCode, woken.Code);
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AHandleWakeup_AfterACancelledCommitLeftTheConsumersOwnWakeArmed_IsRaisedByTheNextPoll()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            await CancelACommitAwaitingTheAsyncCallbackItDelivered(consumer, afterTheOwnWake: null);

            using ConsumerHandle handle = consumer.Handle();
            handle.Wakeup();

            KafkaException woken = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Equal(WakeupCode, woken.Code);
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("committed")]
    [InlineData("position")]
    [InlineData("commit")]
    [InlineData("commit_consumed")]
    public async Task AWakeableHandleCallInsideACommitCallback_AfterACancelledCommitLeftTheConsumersOwnWakeArmed_IsSubmittedAgain(
        string call)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer(defaultApiTimeoutMs: 500);
        try
        {
            await CancelACommitAwaitingTheAsyncCallbackItDelivered(consumer, afterTheOwnWake: null);

            using ConsumerHandle handle = consumer.Handle();
            TaskCompletionSource<Exception?> raised =
                new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
            await TestTimeout.Run(
                () => consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    try
                    {
                        CallTheHandle(handle, call);
                        raised.TrySetResult(null);
                    }
                    catch (Exception e)
                    {
                        raised.TrySetResult(e);
                    }
                })),
                s_deadline);
            await TestTimeout.Run(() => consumer.CommitAsync(Empty()), s_deadline);

            KafkaException timeout = Assert.IsType<KafkaException>(await TestTimeout.Run(() => raised.Task, s_deadline));
            Assert.Equal(7, timeout.Code);
            Assert.True(timeout.IsRetriable);
            Assert.Equal(HandleTimeoutMessage(call), timeout.Message);

            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AUserWakeup_SentAfterTheConsumersOwnWake_WhileTheCancelledCommitRuns_IsRaisedByTheNextPoll()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            await CancelACommitAwaitingTheAsyncCallbackItDelivered(consumer, afterTheOwnWake: consumer.Wakeup);

            KafkaException woken = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Equal(WakeupCode, woken.Code);
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AWakeupBeforeACommitWithEmptyOffsets_IsRaisedByTheNextPoll()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            consumer.Wakeup();
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            KafkaException woken = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Equal(WakeupCode, woken.Code);
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("kafka", "poll")]
    [InlineData("kafka", "commit")]
    [InlineData("kafka", "commit_nowait")]
    [InlineData("kafka", "close")]
    [InlineData("mock", "poll")]
    [InlineData("mock", "commit")]
    [InlineData("mock", "commit_nowait")]
    [InlineData("mock", "close")]
    public async Task AsyncCallback_IsAwaitedBeforeTheDeliveringCallReturns(string kind, string call)
    {
        IAsyncConsumer<byte[], byte[]> consumer = await NewConsumer(kind);
        try
        {
            List<(string Event, string? Thread, Exception? Exception)> seen =
                new List<(string, string?, Exception?)>();
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? delivered = Empty();
            AsyncDelegateCallback callback = new AsyncDelegateCallback(async (offsets, exception) =>
            {
                delivered = offsets;
                Add(seen, ("start", Thread.CurrentThread.Name, null));
                await Task.Delay(50);
                Add(seen, ("end", null, exception));
            });
            (string, string?, Exception?)[] expected =
            {
                ("start", PumpThreadName, null),
                ("end", null, null),
            };

            await TestTimeout.Run(() => consumer.CommitAsync(kind == "mock" ? Offsets(1) : Empty(), callback), s_deadline);
            if (kind == "mock")
            {
                Assert.Equal(expected, Snapshot(seen));
            }
            else
            {
                Assert.Empty(Snapshot(seen));
            }

            await TestTimeout.Run(() => Deliver(consumer, call), s_deadline);

            Assert.Equal(expected, Snapshot(seen));
            if (kind == "mock")
            {
                Assert.Equal(1, Assert.Single(delivered!).Value.Offset);
            }
            else
            {
                Assert.Null(delivered);
            }
        }
        finally
        {
            await CloseAndDispose(consumer, closed: call == "close");
        }
    }

    [Theory]
    [InlineData("kafka")]
    [InlineData("mock")]
    public async Task AsyncCallbacks_RunOneAfterAnother(string kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = await NewConsumer(kind);
        try
        {
            List<string> events = new List<string>();
            IAsyncOffsetCommitCallback Callback(string name, TimeSpan delay) =>
                new AsyncDelegateCallback(async (_, _) =>
                {
                    Add(events, $"start {name}");
                    await Task.Delay(delay);
                    Add(events, $"end {name}");
                });
            Dictionary<TopicPartition, OffsetAndMetadata> offsets = kind == "mock" ? Offsets(1) : Empty();

            await TestTimeout.Run(() => consumer.CommitAsync(offsets, Callback("first", TimeSpan.FromMilliseconds(100))), s_deadline);
            await TestTimeout.Run(() => consumer.CommitAsync(offsets, Callback("second", TimeSpan.Zero)), s_deadline);
            await TestTimeout.Run(
                () => consumer.CommitAsync(offsets, new DelegateCallback((_, _) => Add(events, "plain"))), s_deadline);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            Assert.Equal(new[] { "start first", "end first", "start second", "end second", "plain" }, Snapshot(events));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task TheCoreDeliversTheNextCallback_OnlyAfterTheAsyncCallbackReported()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer();
        try
        {
            using ManualResetEventSlim started = new ManualResetEventSlim();
            TaskCompletionSource<int> queuedMeanwhile = new TaskCompletionSource<int>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            int plainRuns = 0;
            await consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
            {
                started.Set();
                await Task.Delay(100);
                queuedMeanwhile.TrySetResult(ExecuteCallbacks(consumer));
            }));

            Task second;
            using (new ConsumerPumpStall(consumer))
            {
                second = consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => Interlocked.Increment(ref plainRuns)));
                Stopwatch watch = Stopwatch.StartNew();
                while (!started.IsSet)
                {
                    Assert.True(watch.Elapsed < s_deadline, "the async callback was never delivered");
                    ExecuteCallbacks(consumer);
                    Thread.Sleep(1);
                }

                Assert.Equal(0, await TestTimeout.Run(() => queuedMeanwhile.Task, s_deadline));
            }

            await TestTimeout.Run(() => second, s_deadline);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            Assert.Equal(1, Volatile.Read(ref plainRuns));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("kafka", "error")]
    [InlineData("kafka", "cancelled")]
    [InlineData("kafka", "plain")]
    [InlineData("mock", "error")]
    [InlineData("mock", "cancelled")]
    [InlineData("mock", "plain")]
    public async Task ARaisingCallback_PropagatesTheSameInstance_Once(string kind, string raising)
    {
        Exception raised = raising == "cancelled"
            ? new OperationCanceledException("cancelled in the callback")
            : new CommitCallbackException("bad");
        IAsyncConsumer<byte[], byte[]> consumer = await NewConsumer(kind);
        try
        {
            Exception? propagated;
            if (kind == "mock")
            {
                propagated = await Record.ExceptionAsync(
                    () => TestTimeout.Run(() => CommitRaising(consumer, Offsets(1), raising, raised), s_deadline));
            }
            else
            {
                await TestTimeout.Run(() => CommitRaising(consumer, Empty(), raising, raised), s_deadline);
                propagated = await Record.ExceptionAsync(() => TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline));
            }

            Assert.Same(raised, propagated);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AsyncMock_CommitAsync_AwaitsTheAsyncCallbackInsideTheCall()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await NewMockConsumer();
        try
        {
            List<string> events = new List<string>();
            await TestTimeout.Run(
                () => consumer.CommitAsync(Offsets(1), new AsyncDelegateCallback(async (_, _) =>
                {
                    Add(events, "start");
                    await Task.Delay(50);
                    Add(events, "end");
                })),
                s_deadline);
            Add(events, "returned");

            Assert.Equal(new[] { "start", "end", "returned" }, Snapshot(events));
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))), s_deadline);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData("plain", "value-error")]
    [InlineData("plain", "kafka-error")]
    [InlineData("coroutine", "value-error")]
    [InlineData("coroutine", "kafka-error")]
    public async Task Close_WrapsARaisingCommitCallback(string kind, string error)
    {
        Exception raised = error == "value-error"
            ? new CommitCallbackException("bad")
            : new KafkaException("commit failed");
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewKafkaConsumer(enableAutoCommit: false);
        try
        {
            await TestTimeout.Run(
                () => CommitRaising(consumer, Empty(), kind == "plain" ? "plain" : "error", raised), s_deadline);

            KafkaException wrapped = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))), s_deadline));

            Assert.Equal(typeof(KafkaException), wrapped.GetType());
            Assert.Equal("Failed to close kafka consumer", wrapped.Message);
            Assert.Same(raised, wrapped.InnerException);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [InlineData("kafka", false)]
    [InlineData("kafka", true)]
    [InlineData("mock", false)]
    [InlineData("mock", true)]
    public async Task Dispose_WhileAnAsyncCallbackRuns_WaitsForItsTask_AndDoesNotThrow(string kind, bool disposeAsync)
    {
        IAsyncConsumer<byte[], byte[]> consumer = await NewConsumer(kind);
        TaskCompletionSource<bool> started = NewSignal();
        TaskCompletionSource<bool> release = NewSignal();
        AsyncDelegateCallback callback = new AsyncDelegateCallback(async (_, _) =>
        {
            started.TrySetResult(true);
            await release.Task;
        });
        Task delivering;
        if (kind == "mock")
        {
            delivering = consumer.CommitAsync(Offsets(1), callback);
        }
        else
        {
            await TestTimeout.Run(() => consumer.CommitAsync(Empty(), callback), s_deadline);
            delivering = consumer.Commit(Empty());
        }

        await TestTimeout.Run(() => started.Task, s_deadline);
        Task dispose = disposeAsync
            ? Task.Run(async () => await consumer.DisposeAsync())
            : Task.Run(() => consumer.Dispose());

        await Task.WhenAny(dispose, Task.Delay(s_notYetWindow));
        Assert.False(dispose.IsCompleted, "Dispose returned while the async commit callback still ran");

        release.SetResult(true);
        await TestTimeout.Run(() => dispose, s_deadline);
        Assert.Same(delivering, await Task.WhenAny(delivering, Task.Delay(s_deadline)));
        _ = delivering.Exception;
    }

    [Theory]
    [InlineData("kafka")]
    [InlineData("mock")]
    public async Task CommitAsync_ANullAsyncCallbackOrOffsets_ThrowsArgumentNullExceptionSynchronously(string kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = await NewConsumer(kind);
        try
        {
            ArgumentNullException plain = Assert.Throws<ArgumentNullException>(
                () => { _ = consumer.CommitAsync((IAsyncOffsetCommitCallback)null!); });
            Assert.Equal("callback", plain.ParamName);

            ArgumentNullException withOffsets = Assert.Throws<ArgumentNullException>(
                () => { _ = consumer.CommitAsync(Offsets(1), (IAsyncOffsetCommitCallback)null!); });
            Assert.Equal("callback", withOffsets.ParamName);

            IAsyncOffsetCommitCallback callback = new AsyncDelegateCallback((_, _) => Task.CompletedTask);
            ArgumentNullException nullOffsets = Assert.Throws<ArgumentNullException>(
                () => { _ = consumer.CommitAsync(null!, callback); });
            Assert.Equal("offsets", nullOffsets.ParamName);

            ArgumentNullException both = Assert.Throws<ArgumentNullException>(
                () => { _ = consumer.CommitAsync(null!, (IAsyncOffsetCommitCallback)null!); });
            Assert.Equal("offsets", both.ParamName);
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public void TheAsyncCallbackOverloads_AreOnIAsyncConsumerOnly()
    {
        MethodInfo callback = Single(
            typeof(IAsyncConsumer<byte[], byte[]>), typeof(IAsyncOffsetCommitCallback), typeof(CancellationToken));
        MethodInfo withOffsets = Single(
            typeof(IAsyncConsumer<byte[], byte[]>),
            typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>),
            typeof(IAsyncOffsetCommitCallback),
            typeof(CancellationToken));
        Assert.Equal(typeof(Task), callback.ReturnType);
        Assert.Equal(typeof(Task), withOffsets.ReturnType);
        Assert.False(callback.GetParameters()[0].HasDefaultValue);
        Assert.True(callback.GetParameters()[1].HasDefaultValue);
        Assert.False(withOffsets.GetParameters()[1].HasDefaultValue);
        Assert.True(withOffsets.GetParameters()[2].HasDefaultValue);

        foreach (Type surface in new[] { typeof(IConsumer<byte[], byte[]>), typeof(IConsumerCommon), typeof(ConsumerHandle) })
        {
            Assert.DoesNotContain(
                surface.GetMethods(),
                m => m.GetParameters().Any(p => p.ParameterType == typeof(IAsyncOffsetCommitCallback)));
        }

        MethodInfo onComplete = Assert.Single(typeof(IAsyncOffsetCommitCallback).GetMethods());
        Assert.Equal(nameof(IAsyncOffsetCommitCallback.OnComplete), onComplete.Name);
        Assert.Equal(typeof(Task), onComplete.ReturnType);
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>), typeof(Exception) },
            onComplete.GetParameters().Select(p => p.ParameterType).ToArray());
    }

    private static Task CommitRaising(
        IAsyncConsumer<byte[], byte[]> consumer,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        string raising,
        Exception raised) =>
        raising == "plain"
            ? consumer.CommitAsync(offsets, new DelegateCallback((_, _) => throw raised))
            : consumer.CommitAsync(offsets, new AsyncDelegateCallback(async (_, _) =>
            {
                await Task.Yield();
                throw raised;
            }));

    private static Task Deliver(IAsyncConsumer<byte[], byte[]> consumer, string call) =>
        call switch
        {
            "poll" => consumer.Poll(TimeSpan.Zero),
            "commit" => consumer.Commit(Empty()),
            "commit_nowait" => consumer.CommitAsync(Empty()),
            _ => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))),
        };

    private static void CallTheHandle(ConsumerHandle handle, string call)
    {
        switch (call)
        {
            case "committed":
                handle.Committed(new[] { s_tp0 });
                break;
            case "position":
                handle.Position(s_tp0);
                break;
            case "commit":
                handle.Commit(Offsets(1));
                break;
            default:
                handle.Seek(s_tp0, 1);
                handle.Commit();
                break;
        }
    }

    private static string HandleTimeoutMessage(string call) =>
        call switch
        {
            "committed" => "Timeout of 500ms expired before the last committed offset for partitions [" + Topic
                + "-0] could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.",
            "position" => "Timeout of 500ms expired before the position for partition " + Topic + "-0 could be determined",
            _ => "Timeout of 500ms expired before successfully committing offsets",
        };

    private static async Task CancelACommitAwaitingTheAsyncCallbackItDelivered(
        AsyncKafkaConsumer<byte[], byte[]> consumer,
        Action? afterTheOwnWake)
    {
        TaskCompletionSource<bool> started = NewSignal();
        TaskCompletionSource<bool> release = NewSignal();
        await consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
        {
            started.TrySetResult(true);
            await release.Task;
        }));

        using CancellationTokenSource cancel = new CancellationTokenSource();
        Task running = consumer.Commit(Empty(), cancel.Token);
        await TestTimeout.Run(() => started.Task, s_deadline);
        cancel.Cancel();
        if (afterTheOwnWake is not null)
        {
            ConsumerPumpStall.WaitForThePostedActions(consumer);
            afterTheOwnWake();
        }

        release.TrySetResult(true);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => running, s_deadline));
    }

    private static int ExecuteCallbacks(object consumer)
    {
        NativeConsumer native = (NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;
        IntPtr outErr = IntPtr.Zero;
        int ran = NativeMethods.ConsumerExecuteCallbacks(native.Handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return ran;
    }

    private static void ReleaseAfter(TaskCompletionSource<bool> release, TimeSpan delay) =>
        _ = Task.Delay(delay).ContinueWith(_ => release.TrySetResult(true), TaskScheduler.Default);

    private static TaskCompletionSource<bool> NewSignal() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static void Add<T>(List<T> list, T item)
    {
        lock (list)
        {
            list.Add(item);
        }
    }

    private static T[] Snapshot<T>(List<T> list)
    {
        lock (list)
        {
            return list.ToArray();
        }
    }

    private static MethodInfo Single(Type surface, params Type[] parameters) =>
        Assert.Single(
            surface.GetMethods(),
            m => m.Name == "CommitAsync" && m.GetParameters().Select(p => p.ParameterType).SequenceEqual(parameters));

    private static async Task CloseAndDispose(IAsyncConsumer<byte[], byte[]> consumer, bool closed = false)
    {
        try
        {
            if (!closed)
            {
                await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            }
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static async Task<IAsyncConsumer<byte[], byte[]>> NewConsumer(string kind) =>
        kind == "mock" ? await NewMockConsumer() : await NewKafkaConsumer();

    private static async Task<AsyncKafkaConsumer<byte[], byte[]>> NewKafkaConsumer(
        string? groupId = "mock-group",
        bool? enableAutoCommit = null,
        int? defaultApiTimeoutMs = null)
    {
        Dictionary<string, string> config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
        };
        if (groupId is not null)
        {
            config["group.id"] = groupId;
        }

        if (enableAutoCommit is bool autoCommit)
        {
            config["enable.auto.commit"] = autoCommit ? "true" : "false";
        }

        if (defaultApiTimeoutMs is int timeoutMs)
        {
            config["default.api.timeout.ms"] = timeoutMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        return consumer;
    }

    private static async Task<AsyncMockConsumer<byte[], byte[]>> NewMockConsumer()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        consumer.UpdateBeginningOffset(Topic, 0, 0);
        return consumer;
    }

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> _onComplete;

        internal DelegateCallback(Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> onComplete) =>
            _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }

    /// <summary>An async commit callback that runs the function it is given.</summary>
    private sealed class AsyncDelegateCallback : IAsyncOffsetCommitCallback
    {
        private readonly Func<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Task> _onComplete;

        internal AsyncDelegateCallback(
            Func<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Task> onComplete) =>
            _onComplete = onComplete;

        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }

    /// <summary>The exception a commit callback throws, of a type no other code throws.</summary>
    private sealed class CommitCallbackException : ArgumentException
    {
        internal CommitCallbackException(string message)
            : base(message)
        {
        }
    }
}
