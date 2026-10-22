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
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// How long each consumer keeps an offset-commit callback reachable: until it has reported, or, for a commit that
/// was refused, until the consumer is destroyed.
/// </summary>
public sealed class PublicConsumerCommitCallbackLifetimeTests
{
    private const string Topic = "commit-callback-lifetime-topic";

    private const int Commits = 20;

    private const int InvalidGroupIdCode = 24;

    private const string NoGroupId =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_settle = TimeSpan.FromSeconds(2);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    public static IEnumerable<object[]> Kinds() => new[]
    {
        new object[] { "KafkaConsumer" },
        new object[] { "MockConsumer" },
        new object[] { "AsyncKafkaConsumer" },
        new object[] { "AsyncMockConsumer" },
        new object[] { "AsyncKafkaConsumer+async" },
        new object[] { "AsyncMockConsumer+async" },
    };

    public static IEnumerable<object[]> RefusableKinds() => Kinds().Where(kind => (string)kind[0] != "MockConsumer");

    [Theory]
    [MemberData(nameof(Kinds))]
    public void TheCallback_OfACommitThatReported_IsReleased(string kind)
    {
        Subject consumer = Subject.Create(kind, withGroupId: true);
        try
        {
            Counter reported = new Counter();
            List<WeakReference> callbacks = new List<WeakReference>();
            for (int i = 0; i < Commits; i++)
            {
                callbacks.Add(CommitWithAFreshCallback(consumer, reported));
            }

            consumer.DeliverUntil(() => reported.Value == Commits);

            Assert.Equal(Commits, reported.Value);
            int alive = CountAlive(callbacks);
            Assert.True(alive <= 1, $"{alive} of {Commits} callbacks that reported are still reachable");
        }
        finally
        {
            consumer.Close();
        }
    }

    [Theory]
    [MemberData(nameof(RefusableKinds))]
    public void TheCallback_OfARefusedCommit_IsKeptUntilTheConsumerIsDestroyed_ThenReleased(string kind)
    {
        Subject consumer = Subject.Create(kind, withGroupId: false);
        List<WeakReference> callbacks;
        try
        {
            callbacks = consumer.RefuseCommitsWithFreshCallbacks(Commits);

            Collect();
            int kept = callbacks.FindAll(callback => callback.IsAlive).Count;
            Assert.True(kept == Commits, $"{Commits - kept} of {Commits} callbacks of refused commits were released before destroy");
        }
        finally
        {
            consumer.Close();
        }

        int alive = CountAlive(callbacks);
        Assert.True(alive <= 1, $"{alive} of {Commits} callbacks of refused commits are still reachable after destroy");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference CommitWithAFreshCallback(Subject consumer, Counter reported)
    {
        object callback = consumer.NewCallback(reported);
        consumer.CommitAsync(callback);
        return new WeakReference(callback);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference RefuseAFreshCommit(Subject consumer)
    {
        object callback = consumer.NewCallback(new Counter());
        consumer.Refuse(callback);
        return new WeakReference(callback);
    }

    private static int CountAlive(List<WeakReference> callbacks)
    {
        Stopwatch watch = Stopwatch.StartNew();
        int alive;
        do
        {
            Collect();
            alive = callbacks.FindAll(callback => callback.IsAlive).Count;
        }
        while (alive > 1 && watch.Elapsed < s_settle);

        return alive;
    }

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static void Await(Func<Task> operation)
    {
        try
        {
            TestTimeout.Run(() => operation().GetAwaiter().GetResult(), s_deadline);
        }
        catch (AggregateException wrapped) when (wrapped.InnerExceptions.Count == 1)
        {
            ExceptionDispatchInfo.Capture(wrapped.InnerExceptions[0]).Throw();
        }
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    /// <summary>One consumer of the kind a test names, and the commits the test makes through it.</summary>
    private sealed class Subject
    {
        private readonly IConsumer<byte[], byte[]>? _sync;
        private readonly IAsyncConsumer<byte[], byte[]>? _async;
        private readonly bool _mock;
        private readonly bool _asyncCallbacks;
        private readonly List<Task> _refusals = new List<Task>();

        private Subject(
            IConsumer<byte[], byte[]>? sync, IAsyncConsumer<byte[], byte[]>? async, bool mock, bool asyncCallbacks = false)
        {
            _sync = sync;
            _async = async;
            _mock = mock;
            _asyncCallbacks = asyncCallbacks;
        }

        internal static Subject Create(string kind, bool withGroupId) => kind switch
        {
            "AsyncKafkaConsumer+async" => new Subject(
                null, new AsyncKafkaConsumer<byte[], byte[]>(Config(withGroupId), Serdes.ByteArray, Serdes.ByteArray), false,
                asyncCallbacks: true),
            "AsyncMockConsumer+async" => new Subject(
                null, new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray), true, asyncCallbacks: true),
            "KafkaConsumer" => new Subject(
                new KafkaConsumer<byte[], byte[]>(Config(withGroupId), Serdes.ByteArray, Serdes.ByteArray), null, false),
            "MockConsumer" => new Subject(new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray), null, true),
            "AsyncKafkaConsumer" => new Subject(
                null, new AsyncKafkaConsumer<byte[], byte[]>(Config(withGroupId), Serdes.ByteArray, Serdes.ByteArray), false),
            "AsyncMockConsumer" => new Subject(
                null, new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray), true),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

        internal object NewCallback(Counter reported) =>
            _asyncCallbacks ? new AsyncCountingCallback(reported) : new CountingCallback(reported);

        internal void CommitAsync(object callback)
        {
            Dictionary<TopicPartition, OffsetAndMetadata> offsets = _mock ? Offsets(1) : Empty();
            if (_sync is not null)
            {
                _sync.CommitAsync(offsets, (IOffsetCommitCallback)callback);
                return;
            }

            Await(() => CommitAsync(offsets, callback));
        }

        internal void DeliverUntil(Func<bool> delivered)
        {
            Stopwatch watch = Stopwatch.StartNew();
            while (!delivered() && watch.Elapsed < s_deadline)
            {
                if (_mock)
                {
                    SpinWait.SpinUntil(delivered, s_deadline);
                }
                else if (_sync is not null)
                {
                    TestTimeout.Run(() => _sync.Commit(Empty()), s_deadline);
                }
                else
                {
                    Await(() => _async!.Commit(Empty()));
                }
            }
        }

        internal List<WeakReference> RefuseCommitsWithFreshCallbacks(int count) =>
            _mock ? RefuseWhileARebalanceRuns(count) : RefuseWithoutAGroupId(count);

        internal void Refuse(object callback)
        {
            if (_mock)
            {
                _refusals.Add(CommitAsync(Offsets(1), callback));
                return;
            }

            KafkaException refused = _sync is not null
                ? Assert.Throws<KafkaException>(() => _sync.CommitAsync(Offsets(1), (IOffsetCommitCallback)callback))
                : Assert.Throws<KafkaException>(() => Await(() => CommitAsync(Offsets(1), callback)));
            Assert.Equal(InvalidGroupIdCode, refused.Code);
            Assert.Equal(NoGroupId, refused.Message);
        }

        internal void Close()
        {
            CloseOptions zero = CloseOptions.Timeout(TimeSpan.Zero);
            if (_sync is not null)
            {
                TestTimeout.Run(() => _sync.Close(zero), s_deadline);
                return;
            }

            Await(() => _async!.Close(zero));
        }

        private Task CommitAsync(Dictionary<TopicPartition, OffsetAndMetadata> offsets, object callback) =>
            callback is IAsyncOffsetCommitCallback asyncCallback
                ? _async!.CommitAsync(offsets, asyncCallback)
                : _async!.CommitAsync(offsets, (IOffsetCommitCallback)callback);

        private List<WeakReference> RefuseWithoutAGroupId(int count)
        {
            List<WeakReference> callbacks = new List<WeakReference>();
            for (int i = 0; i < count; i++)
            {
                callbacks.Add(RefuseAFreshCommit(this));
            }

            return callbacks;
        }

        private List<WeakReference> RefuseWhileARebalanceRuns(int count)
        {
            List<WeakReference> callbacks = new List<WeakReference>();
            TopicPartition[] partitions = { s_tp0 };
            string[] topics = { Topic };
            AsyncMockConsumer<byte[], byte[]> asyncMock = (AsyncMockConsumer<byte[], byte[]>)_async!;
            using ManualResetEventSlim entered = new ManualResetEventSlim(false);
            using ManualResetEventSlim release = new ManualResetEventSlim(false);
            Await(() => asyncMock.Subscribe(
                topics,
                new Listener(() =>
                {
                    entered.Set();
                    _ = release.Wait(s_deadline);
                })));
            Task rebalance = asyncMock.Rebalance(partitions);
            try
            {
                Assert.True(entered.Wait(s_deadline), "the listener never ran");
                for (int i = 0; i < count; i++)
                {
                    callbacks.Add(RefuseAFreshCommit(this));
                }
            }
            finally
            {
                release.Set();
            }

            Await(() => rebalance);
            foreach (Task refusal in _refusals)
            {
                InvalidOperationException concurrent =
                    Assert.Throws<InvalidOperationException>(() => Await(() => refusal));
                Assert.Equal(ConcurrentMessage, concurrent.Message);
            }

            _refusals.Clear();
            Assert.Equal(count, callbacks.Count);
            return callbacks;
        }

        private static Dictionary<string, string> Config(bool withGroupId)
        {
            Dictionary<string, string> config = new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "127.0.0.1:1",
                ["group.protocol"] = "consumer",
            };
            if (withGroupId)
            {
                config["group.id"] = "commit-callback-lifetime-group";
            }

            return config;
        }
    }

    /// <summary>A thread-safe count of the commit callbacks that reported.</summary>
    private sealed class Counter
    {
        private int _value;

        internal int Value => Volatile.Read(ref _value);

        internal void Increment() => Interlocked.Increment(ref _value);
    }

    /// <summary>A sync commit callback that counts its reports.</summary>
    private sealed class CountingCallback : IOffsetCommitCallback
    {
        private readonly Counter _reported;

        internal CountingCallback(Counter reported)
        {
            _reported = reported;
        }

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _reported.Increment();
    }

    /// <summary>An async commit callback that counts its report after a yield.</summary>
    private sealed class AsyncCountingCallback : IAsyncOffsetCommitCallback
    {
        private readonly Counter _reported;

        internal AsyncCountingCallback(Counter reported)
        {
            _reported = reported;
        }

        public async Task OnComplete(
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
        {
            await Task.Yield();
            _reported.Increment();
        }
    }

    /// <summary>A rebalance listener that runs an action when partitions are assigned.</summary>
    private sealed class Listener : IConsumerRebalanceListener
    {
        private readonly Action _onAssigned;

        internal Listener(Action onAssigned)
        {
            _onAssigned = onAssigned;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }
}
