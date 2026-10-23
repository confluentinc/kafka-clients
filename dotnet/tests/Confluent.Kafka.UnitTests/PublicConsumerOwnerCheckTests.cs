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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The owner check on <see cref="KafkaConsumer{TKey, TValue}"/> and <see cref="AsyncKafkaConsumer{TKey, TValue}"/>:
/// another thread's call while one holds a use is refused before it reaches the core, the same thread nests, and
/// an explicit Close holds the consumer for its thread until it ends.
/// </summary>
public sealed class PublicConsumerOwnerCheckTests
{
    private const string Topic = "owner-check-topic";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int WakeupCode = -18;

    private const int SequenceCalls = 20;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void KafkaConsumer_AnotherThreadsCalls_WhileACallHoldsTheConsumer_AreRefused_AndWakeupAndHandlePass()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        try
        {
            consumer.Assign(new[] { s_tp0 });
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            consumer.CommitAsync(
                Empty(),
                new DelegateCallback(() => RunOnAnotherThread(() =>
                {
                    seen["Commit"] = Record.Exception(() => consumer.Commit(Empty()));
                    seen["Assignment"] = Record.Exception(() => consumer.Assignment());
                    seen["Metrics"] = Record.Exception(() => consumer.Metrics());
                    seen["Close"] = Record.Exception(() => consumer.Close());
                    seen["Handle"] = Record.Exception(() => consumer.Handle().Dispose());
                    seen["Wakeup"] = Record.Exception(consumer.Wakeup);
                })));

            Exception? delivering = Record.Exception(() => TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline));

            Assert.True(delivering is null || delivering is KafkaException { Code: WakeupCode }, delivering?.ToString());
            AssertRefused(seen["Commit"]);
            AssertRefused(seen["Assignment"]);
            AssertRefused(seen["Metrics"]);
            AssertRefused(seen["Close"]);
            Assert.Null(seen["Handle"]);
            Assert.Null(seen["Wakeup"]);
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_AnotherThreadsCalls_WhileACallHoldsTheConsumer_AreRefused_AndWakeupAndHandlePass()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            Task? close = null;
            await TestTimeout.Run(
                () => consumer.CommitAsync(
                    Empty(),
                    new DelegateCallback(() => RunOnAnotherThread(() =>
                    {
                        seen["Seek"] = Record.Exception(() => { _ = consumer.Seek(s_tp0, 1); });
                        seen["Assignment"] = Record.Exception(() => consumer.Assignment());
                        seen["Metrics"] = Record.Exception(() => consumer.Metrics());
                        close = CloseCatching(consumer);
                        seen["Handle"] = Record.Exception(() => consumer.Handle().Dispose());
                        seen["Wakeup"] = Record.Exception(consumer.Wakeup);
                    }))),
                s_deadline);

            Exception? delivering = await Record.ExceptionAsync(
                () => TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline));

            Assert.True(delivering is null || delivering is KafkaException { Code: WakeupCode }, delivering?.ToString());
            AssertRefused(seen["Seek"]);
            AssertRefused(seen["Assignment"]);
            AssertRefused(seen["Metrics"]);
            AssertRefused(await Record.ExceptionAsync(() => TestTimeout.Run(() => close!, s_deadline)));
            Assert.Null(seen["Handle"]);
            Assert.Null(seen["Wakeup"]);
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_TheSameThread_Nests_WhileItsOwnCallHoldsTheConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            Task<ConsumerRecords<byte[], byte[]>> poll = consumer.Poll(s_deadline);

            Exception? metrics = Record.Exception(() => consumer.Metrics());
            Exception? assignment = Record.Exception(() => consumer.Assignment());
            consumer.Wakeup();

            KafkaException woken = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => poll, s_deadline));
            Assert.Equal(WakeupCode, woken.Code);
            Assert.Null(metrics);
            Assert.True(
                assignment is null || assignment is InvalidOperationException { Message: ConcurrentMessage },
                assignment?.ToString());
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_AwaitedCallsInSequence_PassWhicheverThreadEachResumesOn()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            HashSet<int> callers = new HashSet<int>();
            await TestTimeout.Run(
                async () =>
                {
                    for (int call = 0; call < SequenceCalls; call++)
                    {
                        callers.Add(Environment.CurrentManagedThreadId);
                        await consumer.Seek(s_tp0, call).ConfigureAwait(false);
                        callers.Add(Environment.CurrentManagedThreadId);
                        Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
                        await Task.Run(() => { }).ConfigureAwait(false);
                    }
                },
                s_deadline);

            Assert.True(callers.Count > 1, "every call ran on one thread");
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public void KafkaConsumer_DuringAnExplicitClose_AnotherThreadIsRefused_ThenTheConsumerIsClosed()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        Thread? disposer = null;
        Exception? disposing = null;
        try
        {
            consumer.Assign(new[] { s_tp0 });
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            consumer.CommitAsync(
                Empty(),
                new DelegateCallback(() =>
                {
                    RunOnAnotherThread(() =>
                    {
                        seen["Assignment"] = Record.Exception(() => consumer.Assignment());
                        seen["Close"] = Record.Exception(() => consumer.Close());
                    });
                    disposer = new Thread(() => disposing = Record.Exception(consumer.Dispose));
                    disposer.IsBackground = true;
                    disposer.Start();
                }));

            TestTimeout.Run(consumer.Close, s_deadline);

            AssertRefused(seen["Assignment"]);
            AssertRefused(seen["Close"]);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
            Assert.True(disposer!.Join(s_deadline), "the other thread's Dispose never returned");
            Assert.Null(disposing);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_DuringAnExplicitClose_AnotherThreadIsRefused_ThenTheConsumerIsClosed()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        Thread? disposer = null;
        Exception? disposing = null;
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            Task? close = null;
            await TestTimeout.Run(
                () => consumer.CommitAsync(
                    Empty(),
                    new DelegateCallback(() =>
                    {
                        RunOnAnotherThread(() =>
                        {
                            seen["Assignment"] = Record.Exception(() => consumer.Assignment());
                            close = CloseCatching(consumer);
                        });
                        disposer = new Thread(() => disposing = Record.Exception(consumer.Dispose));
                        disposer.IsBackground = true;
                        disposer.Start();
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Close(), s_deadline);

            AssertRefused(seen["Assignment"]);
            AssertRefused(await Record.ExceptionAsync(() => TestTimeout.Run(() => close!, s_deadline)));
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
            Assert.True(disposer!.Join(s_deadline), "the other thread's Dispose never returned");
            Assert.Null(disposing);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumer_ACallbacksCallsOnTheDrainThread_PassTheOwnerCheck_AndMeetTheCore()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            string? drain = null;
            Exception? metrics = null;
            IReadOnlyDictionary<MetricName, IMetric>? read = null;
            Exception? subscription = null;
            await TestTimeout.Run(
                () => consumer.CommitAsync(
                    Empty(),
                    new DelegateCallback(() =>
                    {
                        drain = Thread.CurrentThread.Name;
                        metrics = Record.Exception(() => read = consumer.Metrics());
                        subscription = Record.Exception(() => consumer.Subscription());
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            Assert.Equal("confluent-kafka-consumer-pump", drain);
            Assert.Null(metrics);
            Assert.NotNull(read);
            AssertRefused(subscription);
            Assert.Empty(consumer.Subscription());
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static void AssertRefused(Exception? thrown)
    {
        InvalidOperationException refused = Assert.IsType<InvalidOperationException>(thrown);
        Assert.Equal(ConcurrentMessage, refused.Message);
    }

    private static void RunOnAnotherThread(Action action)
    {
        Exception? failure = null;
        Thread other = new Thread(() => failure = Record.Exception(action));
        other.IsBackground = true;
        other.Start();
        if (!other.Join(s_deadline))
        {
            throw new TimeoutException("the other thread's calls did not return");
        }

        if (failure is not null)
        {
            throw new InvalidOperationException("the other thread failed", failure);
        }
    }

    private static Task CloseCatching(IAsyncConsumer<byte[], byte[]> consumer)
    {
        try
        {
            return consumer.Close();
        }
        catch (Exception e)
        {
            return Task.FromException(e);
        }
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "owner-check-group",
        ["enable.auto.commit"] = "false",
    };

    private static KafkaConsumer<byte[], byte[]> NewKafkaConsumer() =>
        new KafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncKafkaConsumer<byte[], byte[]> NewAsyncKafkaConsumer() =>
        new AsyncKafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal DelegateCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }
}
