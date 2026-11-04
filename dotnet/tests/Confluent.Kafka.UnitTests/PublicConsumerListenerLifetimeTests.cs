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
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// How long each consumer keeps a rebalance listener, and the exception a listener throws, reachable: retained
/// while subscribed and across <c>Unsubscribe</c>, released by a replacing or listenerless subscribe and by close;
/// the listener a failed subscribe was given is released at once.
/// </summary>
public sealed class PublicConsumerListenerLifetimeTests
{
    private const string Topic = "listener-lifetime-topic";

    private const int FailedSubscribes = 20;

    private const string MutuallyExclusive = "Subscription to topics, partitions and pattern are mutually exclusive";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    public static IEnumerable<object[]> Kinds() => new[]
    {
        new object[] { "KafkaConsumer" },
        new object[] { "MockConsumer" },
        new object[] { "AsyncKafkaConsumer" },
        new object[] { "AsyncMockConsumer" },
        new object[] { "AsyncKafkaConsumer+async" },
        new object[] { "AsyncMockConsumer+async" },
    };

    [Theory]
    [MemberData(nameof(Kinds))]
    public void TheListener_IsRetainedWhileSubscribed_AndAcrossUnsubscribe_AndReleasedByClose(string kind)
    {
        Subject consumer = Subject.Create(kind);

        WeakReference listener = SubscribeAFreshListener(consumer);
        Collect();
        Assert.True(listener.IsAlive, "the listener was collected while subscribed");

        consumer.Unsubscribe();
        Collect();
        Assert.True(listener.IsAlive, "Unsubscribe released the listener");

        consumer.Close();
        Collect();
        Assert.False(listener.IsAlive, "Close did not release the listener");
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void TheListener_IsReleasedByAReplacingOrAListenerlessSubscribe(string kind)
    {
        Subject consumer = Subject.Create(kind);
        try
        {
            WeakReference first = SubscribeAFreshListener(consumer);
            WeakReference second = SubscribeAFreshListener(consumer);
            Collect();
            Assert.False(first.IsAlive, "a replacing subscribe did not release the listener");
            Assert.True(second.IsAlive, "the replacing listener was collected while subscribed");

            consumer.SubscribeListenerless();
            Collect();
            Assert.False(second.IsAlive, "a listenerless subscribe did not release the listener");
        }
        finally
        {
            consumer.Close();
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void TheListener_OfAFailedSubscribe_IsReleased(string kind)
    {
        Subject consumer = Subject.Create(kind);
        try
        {
            consumer.Assign();
            List<WeakReference> listeners = new List<WeakReference>();
            for (int i = 0; i < FailedSubscribes; i++)
            {
                listeners.Add(FailToSubscribeAFreshListener(consumer));
            }

            Collect();
            int alive = listeners.FindAll(listener => listener.IsAlive).Count;
            Assert.True(alive <= 1, $"{alive} of {FailedSubscribes} listeners of failed subscribes are still reachable");
        }
        finally
        {
            consumer.Close();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheListenerException_IsThrownAsTheSameInstance_AndReleasedOnceDropped(bool async)
    {
        Subject consumer = Subject.Create(async ? "AsyncMockConsumer+async" : "MockConsumer");
        try
        {
            WeakReference thrown = RebalanceIntoAFreshListenerException(consumer);
            Collect();
            Assert.False(thrown.IsAlive, "the listener's exception stayed reachable after it was dropped");
        }
        finally
        {
            consumer.Close();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SubscribeAFreshListener(Subject consumer)
    {
        Listener listener = new Listener(failure: null);
        consumer.Subscribe(listener);
        return new WeakReference(listener);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference FailToSubscribeAFreshListener(Subject consumer)
    {
        Listener listener = new Listener(failure: null);
        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(() => consumer.Subscribe(listener));
        Assert.Equal(MutuallyExclusive, refused.Message);
        return new WeakReference(listener);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference RebalanceIntoAFreshListenerException(Subject consumer)
    {
        Listener listener = new Listener(failure: () => new InvalidOperationException("listener failed"));
        consumer.Subscribe(listener);

        Exception caught = Assert.Throws<InvalidOperationException>(consumer.Rebalance);

        WeakReference raised = Assert.Single(listener.Raised);
        Assert.Same(raised.Target, caught);
        return raised;
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

    /// <summary>One of the four consumers, with a sync or an async listener, driven through one shape.</summary>
    private sealed class Subject
    {
        private static readonly string[] s_topics = { Topic };

        private readonly IConsumer<byte[], byte[]>? _sync;
        private readonly IAsyncConsumer<byte[], byte[]>? _async;
        private readonly bool _asyncListener;

        private Subject(IConsumer<byte[], byte[]>? sync, IAsyncConsumer<byte[], byte[]>? async, bool asyncListener)
        {
            _sync = sync;
            _async = async;
            _asyncListener = asyncListener;
        }

        internal static Subject Create(string kind) => kind switch
        {
            "KafkaConsumer" => new Subject(
                new KafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray), null, false),
            "MockConsumer" => new Subject(
                new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray), null, false),
            "AsyncKafkaConsumer" => new Subject(null, Real(), false),
            "AsyncMockConsumer" => new Subject(null, Mock(), false),
            "AsyncKafkaConsumer+async" => new Subject(null, Real(), true),
            "AsyncMockConsumer+async" => new Subject(null, Mock(), true),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

        internal void Subscribe(Listener listener)
        {
            if (_sync is not null)
            {
                _sync.Subscribe(s_topics, listener);
                return;
            }

            Await(() => _asyncListener
                ? _async!.Subscribe(s_topics, (IAsyncConsumerRebalanceListener)listener)
                : _async!.Subscribe(s_topics, (IConsumerRebalanceListener)listener));
        }

        internal void SubscribeListenerless()
        {
            if (_sync is not null)
            {
                _sync.Subscribe(s_topics);
                return;
            }

            Await(() => _async!.Subscribe(s_topics));
        }

        internal void Assign()
        {
            TopicPartition[] partitions = { new TopicPartition(Topic, 0) };
            if (_sync is not null)
            {
                _sync.Assign(partitions);
                return;
            }

            Await(() => _async!.Assign(partitions));
        }

        internal void Unsubscribe()
        {
            if (_sync is not null)
            {
                _sync.Unsubscribe();
                return;
            }

            Await(() => _async!.Unsubscribe());
        }

        internal void Rebalance()
        {
            TopicPartition[] partitions = { new TopicPartition(Topic, 0) };
            if (_sync is MockConsumer<byte[], byte[]> mock)
            {
                mock.Rebalance(partitions);
                return;
            }

            Await(() => ((AsyncMockConsumer<byte[], byte[]>)_async!).Rebalance(partitions));
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

        private static AsyncKafkaConsumer<byte[], byte[]> Real() =>
            new AsyncKafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

        private static AsyncMockConsumer<byte[], byte[]> Mock() =>
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        private static Dictionary<string, string> Config() => new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "listener-lifetime-group",
        };
    }

    /// <summary>A sync and async listener whose assigned method throws a fresh exception, each held weakly.</summary>
    private sealed class Listener : IConsumerRebalanceListener, IAsyncConsumerRebalanceListener
    {
        private readonly Func<Exception>? _failure;

        internal Listener(Func<Exception>? failure)
        {
            _failure = failure;
        }

        internal List<WeakReference> Raised { get; } = new List<WeakReference>();

        void IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        void IConsumerRebalanceListener.OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Fail();

        void IConsumerRebalanceListener.OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        Task IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Task.CompletedTask;

        async Task IAsyncConsumerRebalanceListener.OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            await Task.Yield();
            Fail();
        }

        Task IAsyncConsumerRebalanceListener.OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            Task.CompletedTask;

        private void Fail()
        {
            if (_failure is null)
            {
                return;
            }

            Exception failure = _failure();
            Raised.Add(new WeakReference(failure));
            throw failure;
        }
    }
}
