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
using System.Threading;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync <see cref="KafkaConsumer{TKey, TValue}"/>'s commit callback against an unreachable broker: the thread it
/// runs on, the single-owner guard around it, and how its exception leaves the call that ran it.
/// </summary>
public sealed class PublicKafkaConsumerCommitCallbackTests
{
    private const string Topic = "test";

    private const string Concurrent = "KafkaConsumer is not safe for multi-threaded access.";

    private const int WakeupCode = -18;

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(10);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void CommitCallback_RunsOnTheThreadOfALaterWaitingCall()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception, Thread Thread)> seen =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Thread)>();

        consumer.CommitAsync(Empty(), new DelegateCallback((o, e) => seen.Add((o, e, Thread.CurrentThread))));
        consumer.Commit(Empty());

        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception, Thread thread) =
            Assert.Single(seen);
        Assert.Null(offsets);
        Assert.Null(exception);
        Assert.Same(Thread.CurrentThread, thread);
    }

    [Fact]
    public void HandleCommitAsync_WithAnEmptyMapInsideACommitCallback_DeliversNullOffsets()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        using ConsumerHandle handle = consumer.Handle();
        List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> seen =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();
        Exception? failure = null;

        consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
        {
            try
            {
                handle.CommitAsync(Empty(), new DelegateCallback((o, e) => seen.Add((o, e))));
            }
            catch (Exception e)
            {
                failure = e;
            }
        }));
        Until(() => seen.Count > 0, () => consumer.Commit(Empty()));

        Assert.Null(failure);
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) = Assert.Single(seen);
        Assert.Null(offsets);
        Assert.Null(exception);
    }

    [Fact]
    public void CommitCallback_RunsInsideALaterCommitAsync()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        List<Thread> seen = new List<Thread>();

        consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => seen.Add(Thread.CurrentThread)));
        Assert.Empty(seen);

        Until(() => seen.Count > 0, () => consumer.CommitAsync(Empty(), Noop()));

        Assert.Same(Thread.CurrentThread, Assert.Single(seen));
    }

    [Fact]
    public void AnotherThreadDuringTheCommitCallback_MeetsTheGuard()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        Exception? other = null;
        bool returned = false;

        consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
        {
            Thread thread = new Thread(() =>
            {
                try
                {
                    consumer.Assignment();
                    returned = true;
                }
                catch (Exception e)
                {
                    other = e;
                }
            });
            thread.Start();
            Assert.True(thread.Join(s_wait));
        }));
        consumer.Commit(Empty());

        Assert.False(returned);
        InvalidOperationException concurrent = Assert.IsType<InvalidOperationException>(other);
        Assert.Equal(Concurrent, concurrent.Message);
    }

    [Fact]
    public void CommitAsyncFromAnotherThread_WhileOneRuns_MeetsTheGuard()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewListenerConsumer();
        using ManualResetEventSlim started = new ManualResetEventSlim();
        using ManualResetEventSlim release = new ManualResetEventSlim();
        List<Thread> ranOn = new List<Thread>();
        object? outcome = null;
        try
        {
            consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
            {
                ranOn.Add(Thread.CurrentThread);
                started.Set();
                release.Wait(s_wait);
            }));
            Thread worker = new Thread(() =>
            {
                try
                {
                    Stopwatch watch = Stopwatch.StartNew();
                    while (!started.IsSet && watch.Elapsed < s_wait)
                    {
                        consumer.CommitAsync(Empty(), Noop());
                    }

                    outcome = "returned";
                }
                catch (Exception e)
                {
                    outcome = e;
                }
            });
            worker.Start();
            Assert.True(started.Wait(s_wait));

            Stopwatch refusal = Stopwatch.StartNew();
            InvalidOperationException concurrent =
                Assert.Throws<InvalidOperationException>(() => consumer.CommitAsync(Empty(), Noop()));
            Assert.Equal(Concurrent, concurrent.Message);
            Assert.True(refusal.Elapsed < TimeSpan.FromSeconds(1), $"refused after {refusal.Elapsed}");

            release.Set();
            Assert.True(worker.Join(s_wait));
            Assert.Equal("returned", outcome);
            Assert.Same(worker, Assert.Single(ranOn));
        }
        finally
        {
            release.Set();
            TestTimeout.Teardown(consumer, () => consumer.Close());
            TestTimeout.Dispose(consumer);
        }
    }

    [Fact]
    public void CommitCallbacks_RunOnTheThreadOfTheCallDeliveringThem()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewListenerConsumer();
        Dictionary<string, Thread> seen = new Dictionary<string, Thread>();
        IOffsetCommitCallback Recording(string name) =>
            new DelegateCallback((_, _) =>
            {
                lock (seen)
                {
                    seen[name] = Thread.CurrentThread;
                }
            });
        bool Seen(string name)
        {
            lock (seen)
            {
                return seen.ContainsKey(name);
            }
        }

        try
        {
            Thread threadA = new Thread(() => consumer.CommitAsync(Empty(), Recording("first")));
            threadA.Start();
            Assert.True(threadA.Join(s_wait));

            Exception? failure = null;
            Thread threadB = new Thread(() =>
            {
                try
                {
                    Until(() => Seen("first"), () => consumer.CommitAsync(Empty(), Noop()));
                    consumer.CommitAsync(Empty(), Recording("second"));
                }
                catch (Exception e)
                {
                    failure = e;
                }
            });
            threadB.Start();
            Assert.True(threadB.Join(s_wait));
            Assert.Null(failure);
            Assert.Same(threadB, seen["first"]);
            Assert.False(Seen("second"));

            Until(() => Seen("second"), () => consumer.CommitAsync(Empty(), Noop()));
            Assert.Same(Thread.CurrentThread, seen["second"]);
        }
        finally
        {
            TestTimeout.Teardown(consumer, () => consumer.Close());
            TestTimeout.Dispose(consumer);
        }
    }

    [Theory]
    [InlineData("Commit")]
    [InlineData("CommitAsync")]
    [InlineData("Poll")]
    public void ARaisingCommitCallback_PropagatesFromTheDeliveringCall(string delivering)
    {
        CommitCallbackException raised = new CommitCallbackException("callback boom");
        List<string> events = new List<string>();
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        QueueARaisingCommitCallback(consumer, raised, events);

        if (delivering == "Poll")
        {
            KafkaException wrapped = Assert.Throws<KafkaException>(() => consumer.Poll(TimeSpan.Zero));
            Assert.Equal(typeof(KafkaException), wrapped.GetType());
            Assert.Equal(typeof(CommitCallbackException).FullName + ": callback boom", wrapped.Message);
            Assert.Same(raised, wrapped.InnerException);
        }
        else
        {
            CommitCallbackException propagated = Assert.Throws<CommitCallbackException>(() =>
            {
                if (delivering == "Commit")
                {
                    consumer.Commit(Empty());
                }
                else
                {
                    consumer.CommitAsync(Empty(), null);
                }
            });
            Assert.Same(raised, propagated);
            Assert.Equal("callback boom", propagated.Message);
        }

        Assert.Equal(new[] { "boom" }, events);
        consumer.Commit(Empty());
        Assert.Equal(new[] { "boom", "next" }, events);
        Assert.Empty(consumer.Poll(TimeSpan.Zero));
    }

    [Fact]
    public void ARaisingCommitCallback_PropagatesFromACommitInsideACallback()
    {
        CommitCallbackException raised = new CommitCallbackException("callback boom");
        List<Exception> seen = new List<Exception>();
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });

        consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
        {
            consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => throw raised));
            try
            {
                consumer.Commit(Empty());
            }
            catch (CommitCallbackException e)
            {
                seen.Add(e);
            }
        }));
        consumer.Commit(Empty());

        Assert.Same(raised, Assert.Single(seen));
    }

    [Fact]
    public void ARaisingCommitCallbacksKafkaException_PropagatesFromPollAsItIs()
    {
        KafkaException raised = new KafkaException("callback boom");
        List<string> events = new List<string>();
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        QueueARaisingCommitCallback(consumer, raised, events);

        KafkaException propagated = Assert.Throws<KafkaException>(() => consumer.Poll(TimeSpan.Zero));

        Assert.Same(raised, propagated);
        Assert.Equal(new[] { "boom" }, events);
    }

    [Fact]
    public void AWakeupFromACommitCallbackOfAnExpiredPoll_IsRaisedByThatPoll()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        try
        {
            consumer.Assign(new[] { s_tp0 });
            consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => consumer.Wakeup()));

            KafkaException woken = Assert.Throws<KafkaException>(() => consumer.Poll(TimeSpan.Zero));

            Assert.Equal(WakeupCode, woken.Code);
            Assert.Empty(consumer.Poll(TimeSpan.Zero));
        }
        finally
        {
            TestTimeout.Teardown(consumer, () => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)));
            TestTimeout.Dispose(consumer);
        }
    }

    [Theory]
    [InlineData("value-error")]
    [InlineData("kafka-error")]
    public void Close_WrapsARaisingCommitCallback(string kind)
    {
        Exception raised = kind == "value-error"
            ? new CommitCallbackException("callback boom")
            : new KafkaException("commit failed");
        List<string> events = new List<string>();
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer(enableAutoCommit: false);
        try
        {
            consumer.Assign(new[] { s_tp0 });
            QueueARaisingCommitCallback(consumer, raised, events);

            KafkaException wrapped = Assert.Throws<KafkaException>(
                () => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))));

            Assert.Equal(typeof(KafkaException), wrapped.GetType());
            Assert.Equal("Failed to close kafka consumer", wrapped.Message);
            Assert.Same(raised, wrapped.InnerException);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            TestTimeout.Dispose(consumer);
        }
    }

    [Fact]
    public void Close_SwallowsARaisingCommitCallbackOfTheAutoCommit()
    {
        CommitCallbackException raised = new CommitCallbackException("callback boom");
        List<string> events = new List<string>();
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer(enableAutoCommit: true);
        try
        {
            consumer.Assign(new[] { s_tp0 });
            QueueARaisingCommitCallback(consumer, raised, events);

            TestTimeout.Teardown(consumer, () => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(5))));

            Assert.Equal(new[] { "boom", "next" }, events);
        }
        finally
        {
            TestTimeout.Dispose(consumer);
        }
    }

    private static void QueueARaisingCommitCallback(
        KafkaConsumer<byte[], byte[]> consumer,
        Exception raised,
        List<string> events) =>
        consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
        {
            events.Add("boom");
            consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => events.Add("next")));
            throw raised;
        }));

    private static void Until(Func<bool> done, Action step)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (!done())
        {
            Assert.True(watch.Elapsed < s_wait, "the awaited callback never ran");
            step();
        }
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static IOffsetCommitCallback Noop() => new DelegateCallback((_, _) => { });

    private static KafkaConsumer<byte[], byte[]> NewConsumer(bool? enableAutoCommit = null)
    {
        Dictionary<string, string> config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "mock-group",
        };
        if (enableAutoCommit is bool autoCommit)
        {
            config["enable.auto.commit"] = autoCommit ? "true" : "false";
        }

        return new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
    }

    private static KafkaConsumer<byte[], byte[]> NewListenerConsumer()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        consumer.Subscribe(new[] { Topic }, new NoopListener());
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

    /// <summary>A listener that does nothing.</summary>
    private sealed class NoopListener : ConsumerRebalanceListenerBase
    {
        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
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
