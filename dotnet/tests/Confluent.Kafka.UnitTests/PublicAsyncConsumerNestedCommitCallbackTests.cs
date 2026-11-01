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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// An async commit callback that a blocking commit made inside another callback runs: it is reported at once, the
/// call that delivered the outer callback waits for its <see cref="Task"/> and throws its exception, and a
/// <c>Poll</c> or a cancelled call leaves the exception to the consumer's next call.
/// </summary>
public sealed class PublicAsyncConsumerNestedCommitCallbackTests
{
    private const string Topic = "t";

    private const int UnknownServerErrorCode = -1;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_notYetWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Theory]
    [InlineData("kafka", "Commit")]
    [InlineData("kafka", "Poll")]
    [InlineData("kafka", "Close")]
    [InlineData("mock", "CommitAsync")]
    public async Task AnAsyncCallbackANestedCommitRuns_IsReportedAtOnce_AndTheDeliveringCallThrowsItsException(
        string kind,
        string delivering)
    {
        bool mock = kind == "mock";
        IAsyncConsumer<byte[], byte[]> consumer = mock ? await NewAsyncMockConsumer() : await NewAsyncKafkaConsumer();
        CallbackException raised = new CallbackException("bad");
        List<string> events = new List<string>();
        TaskCompletionSource<bool> gate = NewSignal();
        TaskCompletionSource<bool> release = NewSignal();
        TaskCompletionSource<bool> outerEnded = NewSignal();
        try
        {
            AsyncDelegateCallback inner = new AsyncDelegateCallback(async (_, _) =>
            {
                await gate.Task;
                await release.Task;
                Add(events, "inner end");
                throw raised;
            });
            DelegateCallback outer = new DelegateCallback((_, _) =>
            {
                Add(events, "nested CommitAsync: " + Returned(() => consumer.CommitAsync(mock ? Offsets(2) : Empty(), inner)));
                if (!mock)
                {
                    Add(events, "nested Commit: " + Returned(() => consumer.Commit(Empty())));
                }

                gate.TrySetResult(true);
                Add(events, "outer end");
                outerEnded.TrySetResult(true);
            });

            Task call;
            if (mock)
            {
                call = Task.Run(() => consumer.CommitAsync(Offsets(1), outer));
            }
            else
            {
                await TestTimeout.RunOffThisThread(() => consumer.CommitAsync(Empty(), outer), s_deadline);
                call = delivering == "Poll"
                    ? Task.Run(() => consumer.Poll(TimeSpan.Zero))
                    : Task.Run(() => Deliver(consumer, delivering));
            }

            await TestTimeout.Run(() => outerEnded.Task, s_deadline);
            List<string> nested = new List<string> { "nested CommitAsync: ok" };
            if (!mock)
            {
                nested.Add("nested Commit: ok");
            }

            nested.Add("outer end");
            Assert.Equal(nested, Snapshot(events));
            Assert.NotSame(call, await Task.WhenAny(call, Task.Delay(s_notYetWindow)));

            release.TrySetResult(true);
            nested.Add("inner end");
            switch (delivering)
            {
                case "Poll":
                    Assert.Empty(await TestTimeout.Run(() => (Task<ConsumerRecords<byte[], byte[]>>)call, s_deadline));
                    Assert.Equal(nested, Snapshot(events));
                    Assert.Same(raised, await Assert.ThrowsAsync<CallbackException>(
                        () => TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline)));
                    break;
                case "Close":
                    KafkaException wrapped = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => call, s_deadline));
                    AssertCloseWrapped(raised, wrapped);
                    Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
                    break;
                default:
                    Assert.Same(raised, await Assert.ThrowsAsync<CallbackException>(() => TestTimeout.Run(() => call, s_deadline)));
                    break;
            }

            Assert.Equal(nested, Snapshot(events));
        }
        finally
        {
            gate.TrySetResult(true);
            release.TrySetResult(true);
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("CommitAsync")]
    [InlineData("Commit")]
    public async Task AsyncKafkaConsumer_ACommitInsideAnAsyncCallback_ReportsAQueuedAsyncCallbackAtOnce_AndAwaitsItAfter(
        string nested)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        TaskCompletionSource<bool> firstEnded = NewSignal();
        using ManualResetEventSlim queuedEnded = new ManualResetEventSlim();
        try
        {
            List<string> events = new List<string>();
            AsyncDelegateCallback queued = new AsyncDelegateCallback(async (_, _) =>
            {
                await firstEnded.Task;
                Add(events, "queued");
                queuedEnded.Set();
            });
            await TestTimeout.Run(
                () => consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
                {
                    await Task.Yield();
                    Add(events, "first");
                    await consumer.CommitAsync(Empty(), queued);
                    Add(events, "queued it");
                    if (nested == "CommitAsync")
                    {
                        await consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                        {
                            _ = queuedEnded.Wait(s_deadline);
                            Add(events, "nested");
                        }));
                    }
                    else
                    {
                        await consumer.Commit(Empty());
                    }

                    Add(events, "first end");
                    firstEnded.TrySetResult(true);
                })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            List<string> expected = new List<string> { "first", "queued it", "first end", "queued" };
            if (nested == "CommitAsync")
            {
                expected.Add("nested");
            }

            Assert.Equal(expected, Snapshot(events));
        }
        finally
        {
            firstEnded.TrySetResult(true);
            queuedEnded.Set();
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("Commit()")]
    [InlineData("CommitAsync()")]
    [InlineData("CommitAsync(callback)")]
    [InlineData("CommitAsync(asyncCallback)")]
    public async Task AsyncKafkaConsumer_ACommitWithoutOffsetsInsideAnAsyncCallback_ReportsAQueuedAsyncCallbackAtOnce_AndAwaitsItAfter(
        string nested)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        TaskCompletionSource<bool> firstEnded = NewSignal();
        try
        {
            List<string> events = new List<string>();
            AsyncDelegateCallback queued = new AsyncDelegateCallback(async (_, _) =>
            {
                await firstEnded.Task;
                Add(events, "queued");
            });
            await TestTimeout.RunOffThisThread(
                () => consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
                {
                    await Task.Yield();
                    Add(events, "first");
                    await consumer.CommitAsync(Empty(), queued);
                    Add(events, "queued it");
                    switch (nested)
                    {
                        case "Commit()":
                            await consumer.Commit();
                            break;
                        case "CommitAsync()":
                            await consumer.CommitAsync();
                            break;
                        case "CommitAsync(callback)":
                            await consumer.CommitAsync(new DelegateCallback((_, _) => Add(events, "nested")));
                            break;
                        default:
                            await consumer.CommitAsync(new AsyncDelegateCallback((_, _) =>
                            {
                                Add(events, "nested");
                                return Task.CompletedTask;
                            }));
                            break;
                    }

                    Add(events, "first end");
                    firstEnded.TrySetResult(true);
                })),
                s_deadline);

            await TestTimeout.RunOffThisThread(() => consumer.Commit(Empty()), s_deadline);
            await TestTimeout.RunOffThisThread(() => consumer.Commit(Empty()), s_deadline);

            string[] seen = Snapshot(events);
            Assert.Equal(
                new[] { "first", "queued it", "first end", "queued" },
                Array.FindAll(seen, e => e != "nested"));
            int nestedCallbacks = nested is "CommitAsync(callback)" or "CommitAsync(asyncCallback)" ? 1 : 0;
            Assert.Equal(nestedCallbacks, Array.FindAll(seen, e => e == "nested").Length);
        }
        finally
        {
            firstEnded.TrySetResult(true);
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_ARaisingAsyncCallbackInsideANestedCommit_IsThrownByTheDeliveringCall()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        Nesting nesting = new Nesting(consumer);
        try
        {
            await nesting.Queue();

            CallbackException propagated = await Assert.ThrowsAsync<CallbackException>(
                () => TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline));

            Assert.Same(nesting.Raised, propagated);
            Assert.Equal(new[] { "first end", "raising" }, nesting.Events);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
        }
        finally
        {
            nesting.Release();
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("Poll")]
    [InlineData("Commit")]
    [InlineData("Close")]
    public async Task AsyncKafkaConsumer_PollReturnsItsRecordsBeforeAKeptCallbackError_AndTheNextCallThrowsIt(
        string following)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        Nesting nesting = new Nesting(consumer);
        try
        {
            await nesting.Queue();

            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Equal(new[] { "first end", "raising" }, nesting.Events);

            switch (following)
            {
                case "Poll":
                    KafkaException polled = await Assert.ThrowsAsync<KafkaException>(
                        () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
                    AssertPollWrapped(nesting.Raised, polled);
                    break;
                case "Commit":
                    Assert.Same(nesting.Raised, await Assert.ThrowsAsync<CallbackException>(
                        () => TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline)));
                    break;
                default:
                    KafkaException closed = await Assert.ThrowsAsync<KafkaException>(
                        () => TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))), s_deadline));
                    AssertCloseWrapped(nesting.Raised, closed);
                    Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
                    return;
            }

            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            nesting.Release();
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_ACancelledPoll_KeepsAKeptCallbackErrorForTheNextCall()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        Nesting nesting = new Nesting(consumer);
        try
        {
            await nesting.Queue();

            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task<ConsumerRecords<byte[], byte[]>> poll = consumer.Poll(TimeSpan.FromSeconds(30), cancel.Token);
            await TestTimeout.Run(() => nesting.RaisingEnded, s_deadline);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => poll, s_deadline));
            Assert.Equal(new[] { "first end", "raising" }, nesting.Events);

            KafkaException wrapped = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            AssertPollWrapped(nesting.Raised, wrapped);
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            nesting.Release();
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("Commit")]
    [InlineData("CommitAsync")]
    public async Task AsyncKafkaConsumer_ACancelledCallWaitingForAKeptCallback_KeepsItsExceptionForTheNextCall(string delivering)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        CallbackException raised = new CallbackException("bad");
        TaskCompletionSource<bool> gate = NewSignal();
        TaskCompletionSource<bool> release = NewSignal();
        TaskCompletionSource<bool> outerEnded = NewSignal();
        using CancellationTokenSource cancel = new CancellationTokenSource();
        try
        {
            AsyncDelegateCallback inner = new AsyncDelegateCallback(async (_, _) =>
            {
                await gate.Task;
                await release.Task;
                throw raised;
            });
            DelegateCallback outer = new DelegateCallback((_, _) =>
            {
                consumer.CommitAsync(Empty(), inner).GetAwaiter().GetResult();
                consumer.Commit(Empty()).GetAwaiter().GetResult();
                gate.TrySetResult(true);
                outerEnded.TrySetResult(true);
            });
            await TestTimeout.RunOffThisThread(() => consumer.CommitAsync(Empty(), outer), s_deadline);

            Task call = delivering == "Commit"
                ? Task.Run(() => consumer.Commit(Empty(), cancel.Token))
                : Task.Run(() => consumer.CommitAsync(Empty(), cancellationToken: cancel.Token));
            await TestTimeout.Run(() => outerEnded.Task, s_deadline);
            Assert.NotSame(call, await Task.WhenAny(call, Task.Delay(s_notYetWindow)));

            cancel.Cancel();
            release.TrySetResult(true);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => call, s_deadline));

            Assert.Same(raised, await Assert.ThrowsAsync<CallbackException>(
                () => TestTimeout.RunOffThisThread(() => consumer.Commit(Empty()), s_deadline)));
            await TestTimeout.RunOffThisThread(() => consumer.Commit(Empty()), s_deadline);
        }
        finally
        {
            gate.TrySetResult(true);
            release.TrySetResult(true);
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("Poll")]
    [InlineData("Close")]
    public async Task AsyncKafkaConsumer_TheCoreWrapsADeliveredCallbacksException_AsTheKeptOneIsWrapped(string delivering)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        try
        {
            CallbackException raised = new CallbackException("bad");
            await TestTimeout.Run(() => consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => throw raised)), s_deadline);

            KafkaException wrapped = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => Deliver(consumer, delivering), s_deadline));

            if (delivering == "Poll")
            {
                AssertPollWrapped(raised, wrapped);
            }
            else
            {
                AssertCloseWrapped(raised, wrapped);
            }
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    private static void AssertPollWrapped(CallbackException raised, KafkaException wrapped)
    {
        Assert.Equal(typeof(KafkaException), wrapped.GetType());
        Assert.Equal(typeof(CallbackException).FullName + ": bad", wrapped.Message);
        Assert.Same(raised, wrapped.InnerException);
        Assert.Equal(UnknownServerErrorCode, wrapped.Code);
        Assert.False(wrapped.IsRetriable);
    }

    private static void AssertCloseWrapped(CallbackException raised, KafkaException wrapped)
    {
        Assert.Equal(typeof(KafkaException), wrapped.GetType());
        Assert.Equal("Failed to close kafka consumer", wrapped.Message);
        Assert.Same(raised, wrapped.InnerException);
        Assert.Equal(UnknownServerErrorCode, wrapped.Code);
        Assert.False(wrapped.IsRetriable);
    }

    private static Task Deliver(IAsyncConsumer<byte[], byte[]> consumer, string call) =>
        call switch
        {
            "Poll" => consumer.Poll(TimeSpan.Zero),
            "Commit" => consumer.Commit(Empty()),
            _ => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))),
        };

    private static string Returned(Func<Task> call)
    {
        try
        {
            Task task = call();
            if (!task.IsCompleted)
            {
                return "pending";
            }

            return task.Status == TaskStatus.RanToCompletion ? "ok" : "faulted " + task.Exception?.InnerException?.Message;
        }
        catch (Exception e)
        {
            return "threw " + e.Message;
        }
    }

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

    private static TaskCompletionSource<bool> NewSignal() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static Dictionary<string, string> KafkaConfig() =>
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "g",
            ["enable.auto.commit"] = "false",
        };

    private static async Task<AsyncKafkaConsumer<byte[], byte[]>> NewAsyncKafkaConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(KafkaConfig(), Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        return consumer;
    }

    private static async Task<AsyncMockConsumer<byte[], byte[]>> NewAsyncMockConsumer()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        consumer.UpdateBeginningOffset(Topic, 0, 0);
        return consumer;
    }

    private static async Task CloseAndDispose(IAsyncConsumer<byte[], byte[]> consumer)
    {
        try
        {
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
        catch (KafkaException)
        {
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    /// <summary>
    /// Python's <c>first</c> and <c>raising</c>: <c>first</c> commits with <c>raising</c> as the callback, then
    /// commits again, which runs <c>raising</c> inside it; <c>raising</c> throws after <c>first</c> has ended.
    /// </summary>
    private sealed class Nesting
    {
        private readonly List<string> _events = new List<string>();
        private readonly TaskCompletionSource<bool> _firstEnded = NewSignal();
        private readonly TaskCompletionSource<bool> _raisingEnded = NewSignal();
        private readonly AsyncKafkaConsumer<byte[], byte[]> _consumer;

        internal Nesting(AsyncKafkaConsumer<byte[], byte[]> consumer) => _consumer = consumer;

        internal CallbackException Raised { get; } = new CallbackException("bad");

        internal string[] Events => Snapshot(_events);

        internal Task RaisingEnded => _raisingEnded.Task;

        internal Task Queue()
        {
            AsyncDelegateCallback raising = new AsyncDelegateCallback(async (_, _) =>
            {
                await _firstEnded.Task;
                Add(_events, "raising");
                _raisingEnded.TrySetResult(true);
                throw Raised;
            });
            AsyncDelegateCallback first = new AsyncDelegateCallback(async (_, _) =>
            {
                await Task.Yield();
                await _consumer.CommitAsync(Empty(), raising);
                await _consumer.Commit(Empty());
                Add(_events, "first end");
                _firstEnded.TrySetResult(true);
            });
            return TestTimeout.Run(() => _consumer.CommitAsync(Empty(), first), s_deadline);
        }

        internal void Release() => _firstEnded.TrySetResult(true);
    }

    /// <summary>The exception the raising callback throws.</summary>
    private sealed class CallbackException : Exception
    {
        internal CallbackException(string message)
            : base(message)
        {
        }
    }

    /// <summary>A sync commit callback that runs the given action.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> _onComplete;

        internal DelegateCallback(Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> onComplete) =>
            _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }

    /// <summary>An async commit callback that runs the given function.</summary>
    private sealed class AsyncDelegateCallback : IAsyncOffsetCommitCallback
    {
        private readonly Func<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Task> _onComplete;

        internal AsyncDelegateCallback(
            Func<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Task> onComplete) =>
            _onComplete = onComplete;

        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }
}
