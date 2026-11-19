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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's default <c>onPartitionsLost</c>: the extension methods run a listener's own lost method when it implements
/// the <c>…WithPartitionsLost</c> interface and its revoked method otherwise, and every subscribe that takes a listener
/// refuses one with a public <c>OnPartitionsLost</c> the consumer would never call, right after the null check and
/// before registering anything.
/// </summary>
public sealed class PublicConsumerRebalanceListenerDefaultMethodTests
{
    private const string Topic = "listener-default-topic";

    private const string Prefix = "Confluent.Kafka.UnitTests.PublicConsumerRebalanceListenerDefaultMethodTests+";

    private const string NullListenerMessage = "RebalanceListener cannot be null";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly IReadOnlyCollection<string> s_topics = new[] { Topic };

    private static readonly SubscriptionPattern s_pattern = new SubscriptionPattern("listener-default-.*");

    private static readonly string[] s_consumers = { "KafkaConsumer", "AsyncKafkaConsumer", "MockConsumer", "AsyncMockConsumer" };

    /// <summary>Each consumer, each subscribe form, and each listener kind the consumer takes.</summary>
    public static TheoryData<string, bool, bool> Subscribes
    {
        get
        {
            TheoryData<string, bool, bool> cases = new TheoryData<string, bool, bool>();
            foreach (string consumer in s_consumers)
            {
                foreach (bool pattern in new[] { false, true })
                {
                    cases.Add(consumer, pattern, false);
                    if (consumer.StartsWith("Async", StringComparison.Ordinal))
                    {
                        cases.Add(consumer, pattern, true);
                    }
                }
            }

            return cases;
        }
    }

    /// <summary>Each consumer and each listener kind it takes.</summary>
    public static TheoryData<string, bool> Listeners
    {
        get
        {
            TheoryData<string, bool> cases = new TheoryData<string, bool>();
            foreach (string consumer in s_consumers)
            {
                cases.Add(consumer, false);
                if (consumer.StartsWith("Async", StringComparison.Ordinal))
                {
                    cases.Add(consumer, true);
                }
            }

            return cases;
        }
    }

    /// <summary>Each mock, each listener kind it takes, and whether the listener has its own lost method.</summary>
    public static TheoryData<string, bool, bool> LostCases => new TheoryData<string, bool, bool>
    {
        { "MockConsumer", false, false },
        { "MockConsumer", false, true },
        { "AsyncMockConsumer", false, false },
        { "AsyncMockConsumer", false, true },
        { "AsyncMockConsumer", true, false },
        { "AsyncMockConsumer", true, true },
    };

    /// <summary>Each mock, the form it is given a listener of both forms in, and whether that form has its own lost method.</summary>
    public static TheoryData<string, bool, bool> BothFormsLostCases => new TheoryData<string, bool, bool>
    {
        { "MockConsumer", false, false },
        { "MockConsumer", false, true },
        { "AsyncMockConsumer", false, false },
        { "AsyncMockConsumer", false, true },
        { "AsyncMockConsumer", true, false },
        { "AsyncMockConsumer", true, true },
    };

    [Fact]
    public void OnPartitionsLost_OnAWithPartitionsLostListener_RunsItsOwnMethod()
    {
        WithListener listener = new WithListener();
        TopicPartition[] partitions = { new TopicPartition(Topic, 1) };

        ((IConsumerRebalanceListener)listener).OnPartitionsLost(partitions);

        Assert.Equal(partitions, Assert.Single(listener.Lost));
        Assert.Empty(listener.Revoked);
    }

    [Fact]
    public async Task AsyncOnPartitionsLost_OnAWithPartitionsLostListener_RunsItsOwnMethod()
    {
        AsyncWithListener listener = new AsyncWithListener();
        TopicPartition[] partitions = { new TopicPartition(Topic, 1) };

        await ((IAsyncConsumerRebalanceListener)listener).OnPartitionsLost(partitions);

        Assert.Equal(partitions, Assert.Single(listener.Lost));
        Assert.Empty(listener.Revoked);
    }

    [Fact]
    public void OnPartitionsLost_OnANullListener_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => ConsumerRebalanceListenerExtensions.OnPartitionsLost(null!, Array.Empty<TopicPartition>()));

        Assert.Equal("listener", e.ParamName);
    }

    [Fact]
    public async Task AsyncOnPartitionsLost_OnANullListener_ThrowsArgumentNullException()
    {
        ArgumentNullException e = await Assert.ThrowsAsync<ArgumentNullException>(
            () => AsyncConsumerRebalanceListenerExtensions.OnPartitionsLost(null!, Array.Empty<TopicPartition>()));

        Assert.Equal("listener", e.ParamName);
    }

    [Theory]
    [MemberData(nameof(LostCases))]
    public async Task LosePartitions_RunsTheListenersOwnLostMethod_OrItsRevokedMethod(
        string consumer, bool asyncListener, bool withPartitionsLost)
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        object client = NewConsumer(consumer);
        try
        {
            IRecording listener = asyncListener
                ? withPartitionsLost ? new AsyncWithListener() : new AsyncPlainListener()
                : withPartitionsLost ? new WithListener() : new PlainListener();
            await TestTimeout.Run(() => Subscribe(client, pattern: false, listener), s_deadline);
            await TestTimeout.Run(() => Rebalance(client, new[] { tp0, tp1 }), s_deadline);

            await TestTimeout.Run(() => LosePartitions(client, new[] { tp0 }), s_deadline);

            Assert.Equal(new[] { tp0 }, Assert.Single(withPartitionsLost ? listener.Lost : listener.Revoked));
            Assert.Empty(withPartitionsLost ? listener.Revoked : listener.Lost);
            Assert.Equal(new[] { tp1 }, Assignment(client).ToArray());
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Subscribes))]
    public async Task Subscribe_RefusesAListenerWithAnOnPartitionsLostItWouldNeverCall(
        string consumer, bool pattern, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            IllegalArgumentException e = await Assert.ThrowsAsync<IllegalArgumentException>(
                () => Subscribe(client, pattern, asyncListener ? new StrayAsyncListener() : new StrayListener()));

            Assert.Equal(StrayMessage(asyncListener ? "StrayAsyncListener" : "StrayListener", asyncListener), e.Message);
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Subscribes))]
    public async Task Subscribe_RefusesAnInheritedOnPartitionsLostItWouldNeverCall(
        string consumer, bool pattern, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            IllegalArgumentException e = await Assert.ThrowsAsync<IllegalArgumentException>(
                () => Subscribe(
                    client, pattern, asyncListener ? new InheritedStrayAsyncListener() : new InheritedStrayListener()));

            Assert.Equal(
                StrayMessage(asyncListener ? "InheritedStrayAsyncListener" : "InheritedStrayListener", asyncListener),
                e.Message);
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Subscribes))]
    public async Task Subscribe_TakesAnExplicitWithPartitionsLostImplementation(
        string consumer, bool pattern, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            await TestTimeout.Run(
                () => Subscribe(client, pattern, asyncListener ? new ExplicitAsyncWithListener() : new ExplicitWithListener()),
                s_deadline);
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Subscribes))]
    public async Task Subscribe_TakesAWithPartitionsLostListenerWhoseMethodIsInherited(
        string consumer, bool pattern, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            await TestTimeout.Run(
                () => Subscribe(client, pattern, asyncListener ? new InheritedAsyncWithListener() : new InheritedWithListener()),
                s_deadline);
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Subscribes))]
    public async Task Subscribe_TakesAListenerOfBothFormsWhoseOnPartitionsLostIsTheOtherForms(
        string consumer, bool pattern, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            object listener = asyncListener ? new SyncLostAsyncListener() : new AsyncLostSyncListener();

            await TestTimeout.Run(
                () => Subscribe(client, pattern, listener, asyncListener, withNullTarget: false),
                s_deadline);
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(BothFormsLostCases))]
    public async Task LosePartitions_OnAListenerOfBothForms_RunsTheGivenFormsLostMethod_OrItsRevokedMethod(
        string consumer, bool asyncListener, bool withPartitionsLost)
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        object client = NewConsumer(consumer);
        try
        {
            IRecording listener = asyncListener == withPartitionsLost
                ? new AsyncLostSyncListener()
                : new SyncLostAsyncListener();
            await TestTimeout.Run(
                () => Subscribe(client, pattern: false, listener, asyncListener, withNullTarget: false),
                s_deadline);
            await TestTimeout.Run(() => Rebalance(client, new[] { tp0, tp1 }), s_deadline);

            await TestTimeout.Run(() => LosePartitions(client, new[] { tp0 }), s_deadline);

            Assert.Equal(new[] { tp0 }, Assert.Single(withPartitionsLost ? listener.Lost : listener.Revoked));
            Assert.Empty(withPartitionsLost ? listener.Revoked : listener.Lost);
            Assert.Equal(new[] { tp1 }, Assignment(client).ToArray());
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Subscribes))]
    public async Task Subscribe_ChecksForNullFirst_ThenTheListener_BeforeTheTopicsOrThePattern(
        string consumer, bool pattern, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            IllegalArgumentException nullListener = await Assert.ThrowsAsync<IllegalArgumentException>(
                () => Subscribe(client, pattern, null, asyncListener, withNullTarget: true));
            IllegalArgumentException stray = await Assert.ThrowsAsync<IllegalArgumentException>(
                () => Subscribe(
                    client,
                    pattern,
                    asyncListener ? new StrayAsyncListener() : new StrayListener(),
                    asyncListener,
                    withNullTarget: true));

            Assert.Equal(NullListenerMessage, nullListener.Message);
            Assert.Equal(StrayMessage(asyncListener ? "StrayAsyncListener" : "StrayListener", asyncListener), stray.Message);
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [MemberData(nameof(Listeners))]
    public async Task Subscribe_RefusedForTheListener_RegistersNothing(string consumer, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        try
        {
            IRecording stray = asyncListener ? new StrayAsyncListener() : new StrayListener();
            IRecording valid = asyncListener ? new AsyncPlainListener() : new PlainListener();

            await Assert.ThrowsAsync<IllegalArgumentException>(() => Subscribe(client, pattern: false, stray));
            Assert.Empty(Subscription(client));

            await TestTimeout.Run(() => Subscribe(client, pattern: false, valid), s_deadline);
            Assert.Equal(s_topics, Subscription(client).ToArray());
            if (consumer.EndsWith("MockConsumer", StringComparison.Ordinal))
            {
                await TestTimeout.Run(() => Rebalance(client, new[] { new TopicPartition(Topic, 0) }), s_deadline);
                Assert.Equal(new[] { new TopicPartition(Topic, 0) }, Assert.Single(valid.Assigned));
                Assert.Empty(stray.Assigned);
            }
        }
        finally
        {
            await DisposeBounded(client);
        }
    }

    [Theory]
    [InlineData("MockConsumer", false)]
    [InlineData("AsyncMockConsumer", false)]
    [InlineData("AsyncMockConsumer", true)]
    public async Task Subscribe_OnAClosedMock_ReportsTheListenerFirst(string consumer, bool asyncListener)
    {
        object client = NewConsumer(consumer);
        await DisposeBounded(client);

        IllegalArgumentException e = await Assert.ThrowsAsync<IllegalArgumentException>(
            () => Subscribe(client, pattern: false, asyncListener ? new StrayAsyncListener() : new StrayListener()));

        Assert.Equal(StrayMessage(asyncListener ? "StrayAsyncListener" : "StrayListener", asyncListener), e.Message);
    }

    private static string StrayMessage(string type, bool asyncListener) =>
        Prefix + type + " declares OnPartitionsLost(IReadOnlyCollection<TopicPartition>) but does not implement "
            + (asyncListener ? "IAsyncConsumerRebalanceListenerWithPartitionsLost" : "IConsumerRebalanceListenerWithPartitionsLost")
            + ", so the consumer would never call it.";

    private static object NewConsumer(string consumer) => consumer switch
    {
        "KafkaConsumer" => new KafkaConsumer<byte[], byte[]>(Configs(), Bytes(), Bytes()),
        "AsyncKafkaConsumer" => new AsyncKafkaConsumer<byte[], byte[]>(Configs(), Bytes(), Bytes()),
        "MockConsumer" => new MockConsumer<byte[], byte[]>(Bytes(), Bytes()),
        _ => new AsyncMockConsumer<byte[], byte[]>(Bytes(), Bytes()),
    };

    private static IDeserializer<byte[]> Bytes() => Serdes.ByteArray().Deserializer();

    private static Dictionary<string, object?> Configs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "listener-default-group",
            ["enable.auto.commit"] = "false",
        };

    private static Task Subscribe(object client, bool pattern, object listener) =>
        Subscribe(client, pattern, listener, listener is IAsyncConsumerRebalanceListener, withNullTarget: false);

    // A sync consumer's subscribe runs inside the returned delegate's call, so what it throws surfaces there too.
    private static Task Subscribe(object client, bool pattern, object? listener, bool asyncListener, bool withNullTarget)
    {
        IReadOnlyCollection<string> topics = withNullTarget ? null! : s_topics;
        SubscriptionPattern subscriptionPattern = withNullTarget ? null! : s_pattern;
        if (client is IConsumer<byte[], byte[]> sync)
        {
            IConsumerRebalanceListener callback = (IConsumerRebalanceListener)listener!;
            if (pattern)
            {
                sync.Subscribe(subscriptionPattern, callback);
            }
            else
            {
                sync.Subscribe(topics, callback);
            }

            return Task.CompletedTask;
        }

        IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)client;
        if (asyncListener)
        {
            IAsyncConsumerRebalanceListener callback = (IAsyncConsumerRebalanceListener)listener!;
            return pattern ? async.Subscribe(subscriptionPattern, callback) : async.Subscribe(topics, callback);
        }

        IConsumerRebalanceListener syncCallback = (IConsumerRebalanceListener)listener!;
        return pattern ? async.Subscribe(subscriptionPattern, syncCallback) : async.Subscribe(topics, syncCallback);
    }

    private static Task Rebalance(object client, IReadOnlyCollection<TopicPartition> assignment)
    {
        if (client is MockConsumer<byte[], byte[]> sync)
        {
            sync.Rebalance(assignment);
            return Task.CompletedTask;
        }

        return ((AsyncMockConsumer<byte[], byte[]>)client).Rebalance(assignment);
    }

    private static Task LosePartitions(object client, IReadOnlyCollection<TopicPartition> partitions)
    {
        if (client is MockConsumer<byte[], byte[]> sync)
        {
            sync.LosePartitions(partitions);
            return Task.CompletedTask;
        }

        return ((AsyncMockConsumer<byte[], byte[]>)client).LosePartitions(partitions);
    }

    private static IReadOnlyCollection<TopicPartition> Assignment(object client) => ((IConsumerCommon)client).Assignment();

    private static IReadOnlyCollection<string> Subscription(object client) => ((IConsumerCommon)client).Subscription();

    private static Task DisposeBounded(object client) =>
        client is IAsyncDisposable async ? TestTimeout.DisposeAsync(async) : Task.Run(() => TestTimeout.Dispose((IDisposable)client));

    /// <summary>The calls a test listener records.</summary>
    private interface IRecording
    {
        List<TopicPartition[]> Revoked { get; }

        List<TopicPartition[]> Assigned { get; }

        List<TopicPartition[]> Lost { get; }
    }

    /// <summary>Records each call; the base of the sync test listeners.</summary>
    private abstract class Recording : IRecording
    {
        public List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        public List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public List<TopicPartition[]> Lost { get; } = new List<TopicPartition[]>();

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Revoked.Add(partitions.ToArray());

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Assigned.Add(partitions.ToArray());
    }

    /// <summary>Records each call; the base of the async test listeners.</summary>
    private abstract class AsyncRecording : IRecording
    {
        public List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        public List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public List<TopicPartition[]> Lost { get; } = new List<TopicPartition[]>();

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
            Revoked.Add(partitions.ToArray());
            return Task.CompletedTask;
        }

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            Assigned.Add(partitions.ToArray());
            return Task.CompletedTask;
        }
    }

    /// <summary>A listener with no lost method of its own.</summary>
    private sealed class PlainListener : Recording, IConsumerRebalanceListener
    {
    }

    /// <summary>A listener with its own lost method.</summary>
    private sealed class WithListener : Recording, IConsumerRebalanceListenerWithPartitionsLost
    {
        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Lost.Add(partitions.ToArray());
    }

    /// <summary>A listener with a public lost method but not the interface that makes a consumer call it.</summary>
    private sealed class StrayListener : Recording, IConsumerRebalanceListener
    {
        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Lost.Add(partitions.ToArray());
    }

    /// <summary>A listener that implements its own lost method explicitly.</summary>
    private sealed class ExplicitWithListener : Recording, IConsumerRebalanceListenerWithPartitionsLost
    {
        void IConsumerRebalanceListenerWithPartitionsLost.OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            Lost.Add(partitions.ToArray());
    }

    /// <summary>A base class with a public lost method, which its derived listeners inherit.</summary>
    private abstract class LostRecording : Recording
    {
        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Lost.Add(partitions.ToArray());
    }

    /// <summary>A listener that inherits a public lost method but lacks the interface that makes a consumer call it.</summary>
    private sealed class InheritedStrayListener : LostRecording, IConsumerRebalanceListener
    {
    }

    /// <summary>A listener whose own lost method is inherited from its base class.</summary>
    private sealed class InheritedWithListener : LostRecording, IConsumerRebalanceListenerWithPartitionsLost
    {
    }

    /// <summary>An async listener with no lost method of its own.</summary>
    private sealed class AsyncPlainListener : AsyncRecording, IAsyncConsumerRebalanceListener
    {
    }

    /// <summary>An async listener with its own lost method.</summary>
    private sealed class AsyncWithListener : AsyncRecording, IAsyncConsumerRebalanceListenerWithPartitionsLost
    {
        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
            Lost.Add(partitions.ToArray());
            return Task.CompletedTask;
        }
    }

    /// <summary>An async listener with a public lost method but not the interface that makes a consumer call it.</summary>
    private sealed class StrayAsyncListener : AsyncRecording, IAsyncConsumerRebalanceListener
    {
        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
            Lost.Add(partitions.ToArray());
            return Task.CompletedTask;
        }
    }

    /// <summary>An async listener that implements its own lost method explicitly.</summary>
    private sealed class ExplicitAsyncWithListener : AsyncRecording, IAsyncConsumerRebalanceListenerWithPartitionsLost
    {
        Task IAsyncConsumerRebalanceListenerWithPartitionsLost.OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
            Lost.Add(partitions.ToArray());
            return Task.CompletedTask;
        }
    }

    /// <summary>A base class with a public async lost method, which its derived listeners inherit.</summary>
    private abstract class AsyncLostRecording : AsyncRecording
    {
        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
            Lost.Add(partitions.ToArray());
            return Task.CompletedTask;
        }
    }

    /// <summary>An async listener that inherits a public lost method but lacks the interface that makes a consumer call it.</summary>
    private sealed class InheritedStrayAsyncListener : AsyncLostRecording, IAsyncConsumerRebalanceListener
    {
    }

    /// <summary>An async listener whose own lost method is inherited from its base class.</summary>
    private sealed class InheritedAsyncWithListener : AsyncLostRecording, IAsyncConsumerRebalanceListenerWithPartitionsLost
    {
    }

    /// <summary>A listener of both forms whose public lost method is the async form's, the sync form keeping Java's default.</summary>
    private sealed class AsyncLostSyncListener
        : Recording, IConsumerRebalanceListener, IAsyncConsumerRebalanceListenerWithPartitionsLost
    {
        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
            Lost.Add(partitions.ToArray());
            return Task.CompletedTask;
        }

        Task IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
            OnPartitionsRevoked(partitions);
            return Task.CompletedTask;
        }

        Task IAsyncConsumerRebalanceListener.OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            OnPartitionsAssigned(partitions);
            return Task.CompletedTask;
        }
    }

    /// <summary>A listener of both forms whose public lost method is the sync form's, the async form keeping Java's default.</summary>
    private sealed class SyncLostAsyncListener
        : AsyncRecording, IAsyncConsumerRebalanceListener, IConsumerRebalanceListenerWithPartitionsLost
    {
        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Lost.Add(partitions.ToArray());

        void IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Revoked.Add(partitions.ToArray());

        void IConsumerRebalanceListener.OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            Assigned.Add(partitions.ToArray());
    }
}
