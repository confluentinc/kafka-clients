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
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Subscribe(SubscriptionPattern)</c> and its listener forms on the four consumers: Java's
/// <c>MockConsumerTest.testRe2JPatternSubscription</c> on both mocks, Java's
/// <c>AsyncKafkaConsumerTest.testSubscribeToRe2JPatternValidation</c> and
/// <c>testSubscribeToRe2JPatternThrowsIfNoGroupId</c> on both Kafka consumers over an unreachable broker, a null
/// pattern text or one holding a NUL, the mocks' listener across a rebalance, a refused subscribe's included, a
/// disposed consumer, and a closed mock.
/// </summary>
/// <remarks>
/// Not translated: Java's <c>testSubscribeToRe2JPatternGeneratesEvent</c>, which needs a stubbed application event
/// handler; <c>testSubscribePatternAgainstBrokerNotSupportingRegex</c>, which needs Java's <c>MockClient</c>; and
/// <c>KafkaConsumerTest.testSubscribeToRe2jPatternNotSupportedForClassicConsumer</c>, as the classic protocol is
/// refused when the consumer is built.
/// </remarks>
public sealed class PublicConsumerPatternSubscribeTests
{
    private const string Topic = "pattern-subscribe-topic";

    private const string ClosedMessage = "This consumer has already been closed.";

    private const string NullListenerMessage = "RebalanceListener cannot be null";

    private const string MutuallyExclusive = "Subscription to topics, partitions and pattern are mutually exclusive";

    private const string NullPatternText = "SubscriptionPattern.pattern must not be null.";

    private const string NulPatternText =
        "A subscription pattern must not contain a NUL character: such a string cannot be passed to the native client "
        + "unchanged.";

    private const string GroupIdNotDefined =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private const int InvalidGroupIdCode = 24;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly SubscriptionPattern s_nullText = new SubscriptionPattern(null!);

    private static readonly SubscriptionPattern s_nulText = new SubscriptionPattern("pay\0ments-.*");

    public static IEnumerable<object[]> Mocks() => new[]
    {
        new object[] { "MockConsumer" },
        new object[] { "AsyncMockConsumer" },
    };

    public static IEnumerable<object[]> RealConsumers() => new[]
    {
        new object[] { "KafkaConsumer" },
        new object[] { "AsyncKafkaConsumer" },
    };

    public static IEnumerable<object[]> AllConsumers() => Mocks().Concat(RealConsumers());

    public static IEnumerable<object[]> AllConsumersAndOverloads() =>
        AllConsumers().SelectMany(kind => new[] { new[] { kind[0], false }, new[] { kind[0], true } });

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task Re2JPatternSubscription_OnTheMocks(string kind)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        await AssertIllegalArgument("Topic pattern cannot be null", () => Subscribe(consumer, null!));
        await AssertIllegalArgument("Topic pattern cannot be empty", () => Subscribe(consumer, new SubscriptionPattern(string.Empty)));
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");
        await AssertIllegalArgument(NullListenerMessage, () => Subscribe(consumer, pattern, (IConsumerRebalanceListener)null!));

        await TestTimeout.Run(() => Subscribe(consumer, pattern), s_deadline);

        Assert.Empty(((IConsumerCommon)consumer).Subscription());
        IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
            () => TestTimeout.Run(() => SubscribeToTopics(consumer), s_deadline));
        Assert.Equal(MutuallyExclusive, refused.Message);
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task SubscribeToRe2JPatternValidation(string kind)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        await AssertIllegalArgument("Topic pattern to subscribe to cannot be null", () => Subscribe(consumer, null!));
        await AssertIllegalArgument(
            "Topic pattern to subscribe to cannot be empty", () => Subscribe(consumer, new SubscriptionPattern(string.Empty)));
        await TestTimeout.Run(() => Subscribe(consumer, new SubscriptionPattern("t*")), s_deadline);
        await AssertIllegalArgument(
            NullListenerMessage, () => Subscribe(consumer, new SubscriptionPattern("t*"), (IConsumerRebalanceListener)null!));
        await TestTimeout.Run(() => Subscribe(consumer, new SubscriptionPattern("t*"), new RecordingListener()), s_deadline);
        if (consumer is IAsyncConsumer<byte[], byte[]> async)
        {
            await AssertIllegalArgument(
                NullListenerMessage, () => async.Subscribe(new SubscriptionPattern("t*"), (IAsyncConsumerRebalanceListener)null!));
            await TestTimeout.Run(() => async.Subscribe(new SubscriptionPattern("t*"), new RecordingAsyncListener()), s_deadline);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task SubscribeToRe2JPattern_ThrowsIfNoGroupId(string kind)
    {
        object consumer = Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        await AssertGroupIdNotDefined(() => Subscribe(consumer, new SubscriptionPattern("t*")));
        await AssertGroupIdNotDefined(() => Subscribe(consumer, new SubscriptionPattern("t*"), new RecordingListener()));
        await AssertGroupIdNotDefined(() => Subscribe(consumer, null!));
        await AssertGroupIdNotDefined(() => Subscribe(consumer, null!, new RecordingListener()));
        if (consumer is IAsyncConsumer<byte[], byte[]> async)
        {
            await AssertGroupIdNotDefined(() => async.Subscribe(new SubscriptionPattern("t*"), new RecordingAsyncListener()));
        }
    }

    [Theory]
    [MemberData(nameof(AllConsumersAndOverloads))]
    public async Task ANullPatternText_ThrowsArgumentNull(string kind, bool withListener)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        await AssertNullPatternText(
            () => withListener ? Subscribe(consumer, s_nullText, new RecordingListener()) : Subscribe(consumer, s_nullText));
        if (withListener && consumer is IAsyncConsumer<byte[], byte[]> async)
        {
            await AssertNullPatternText(() => async.Subscribe(s_nullText, new RecordingAsyncListener()));
        }
    }

    [Theory]
    [MemberData(nameof(AllConsumersAndOverloads))]
    public async Task ANulInThePatternText_ThrowsArgument_AndSubscribesToNothing(string kind, bool withListener)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        await AssertNulPatternText(
            () => withListener ? Subscribe(consumer, s_nulText, new RecordingListener()) : Subscribe(consumer, s_nulText));
        if (withListener && consumer is IAsyncConsumer<byte[], byte[]> async)
        {
            await AssertNulPatternText(() => async.Subscribe(s_nulText, new RecordingAsyncListener()));
        }

        await TestTimeout.Run(() => SubscribeToTopics(consumer), s_deadline);
        Assert.Equal(new[] { "topic1" }, ((IConsumerCommon)consumer).Subscription());
    }

    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    [InlineData("AsyncMockConsumer+async")]
    public async Task OnTheMocks_APatternListener_FiresOnRebalance_UntilAPlainPatternSubscribeReleasesIt(string kind)
    {
        object consumer = Create(kind.Split('+')[0], groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        SubscriptionPattern pattern = new SubscriptionPattern(Topic + ".*");
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        bool asyncListener = kind.EndsWith("+async", StringComparison.Ordinal);
        RecordingListener listener = new RecordingListener();
        RecordingAsyncListener recordingAsync = new RecordingAsyncListener();
        IRecordingListener recording = asyncListener ? recordingAsync : listener;
        await TestTimeout.Run(
            () => asyncListener
                ? ((IAsyncConsumer<byte[], byte[]>)consumer).Subscribe(pattern, recordingAsync)
                : Subscribe(consumer, pattern, listener),
            s_deadline);

        await TestTimeout.Run(() => Rebalance(consumer, tp0, tp1), s_deadline);

        Assert.Empty(recording.Revoked);
        Assert.Equal(new[] { tp0, tp1 }, Assert.Single(recording.Assigned).OrderBy(tp => tp.Partition()));

        await TestTimeout.Run(() => Subscribe(consumer, pattern), s_deadline);
        await TestTimeout.Run(() => Rebalance(consumer, tp0), s_deadline);

        Assert.Empty(recording.Revoked);
        Assert.Single(recording.Assigned);
    }

    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    [InlineData("AsyncMockConsumer+async")]
    public async Task OnTheMocks_APatternListener_RefusedAfterTopics_IsTheOneTheNextRebalanceFires(string kind)
    {
        object consumer = Create(kind.Split('+')[0], groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        SubscriptionPattern pattern = new SubscriptionPattern(Topic + ".*");
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        RecordingListener first = new RecordingListener();
        await TestTimeout.Run(() => SubscribeToTopics(consumer, first), s_deadline);
        bool asyncListener = kind.EndsWith("+async", StringComparison.Ordinal);
        RecordingListener listener = new RecordingListener();
        RecordingAsyncListener recordingAsync = new RecordingAsyncListener();
        IRecordingListener recording = asyncListener ? recordingAsync : listener;

        IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
            () => TestTimeout.Run(
                () => asyncListener
                    ? ((IAsyncConsumer<byte[], byte[]>)consumer).Subscribe(pattern, recordingAsync)
                    : Subscribe(consumer, pattern, listener),
                s_deadline));
        Assert.Equal(MutuallyExclusive, refused.Message);

        await TestTimeout.RunOffThisThread(() => Rebalance(consumer, tp0, tp1), s_deadline);

        Assert.Empty(first.Assigned);
        Assert.Empty(recording.Revoked);
        Assert.Equal(new[] { tp0, tp1 }, Assert.Single(recording.Assigned).OrderBy(tp => tp.Partition()));
    }

    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task AfterDispose_BothOverloads_ReportTheClosedConsumer(string kind)
    {
        object consumer = Create(kind, groupId: true);
        TestTimeout.Dispose((IDisposable)consumer);
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");

        await AssertClosed(() => Subscribe(consumer, pattern));
        await AssertClosed(() => Subscribe(consumer, pattern, new RecordingListener()));
        if (consumer is IAsyncConsumer<byte[], byte[]> async)
        {
            await AssertClosed(() => async.Subscribe(pattern, new RecordingAsyncListener()));
        }
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnAClosedMock_ThePatternErrors_ComeBeforeTheClosedConsumer(string kind)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        await TestTimeout.Run(() => Close(consumer), s_deadline);

        await AssertIllegalArgument("Topic pattern cannot be null", () => Subscribe(consumer, null!));
        await AssertIllegalArgument("Topic pattern cannot be empty", () => Subscribe(consumer, new SubscriptionPattern(string.Empty)));
        await AssertNullPatternText(() => Subscribe(consumer, s_nullText));
        await AssertNulPatternText(() => Subscribe(consumer, s_nulText));
        await AssertNulPatternText(() => Subscribe(consumer, s_nulText, new RecordingListener()));
        if (consumer is IAsyncConsumer<byte[], byte[]> async)
        {
            await AssertNulPatternText(() => async.Subscribe(s_nulText, new RecordingAsyncListener()));
        }

        await AssertIllegalArgument(
            NullListenerMessage, () => Subscribe(consumer, new SubscriptionPattern("t.*"), (IConsumerRebalanceListener)null!));
        await AssertClosed(() => Subscribe(consumer, new SubscriptionPattern("t.*")));
    }

    private static object Create(string kind, bool groupId) => kind switch
    {
        "KafkaConsumer" => new KafkaConsumer<byte[], byte[]>(Config(groupId), Serdes.ByteArray(), Serdes.ByteArray()),
        "AsyncKafkaConsumer" => new AsyncKafkaConsumer<byte[], byte[]>(Config(groupId), Serdes.ByteArray(), Serdes.ByteArray()),
        "MockConsumer" => new MockConsumer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray()),
        _ => new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray()),
    };

    private static Dictionary<string, object?> Config(bool groupId)
    {
        Dictionary<string, object?> config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["enable.auto.commit"] = "false",
        };
        if (groupId)
        {
            config["group.id"] = "pattern-subscribe-group";
        }

        return config;
    }

    private static Task Subscribe(object consumer, SubscriptionPattern pattern)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Subscribe(pattern);
            return Task.CompletedTask;
        }

        return ((IAsyncConsumer<byte[], byte[]>)consumer).Subscribe(pattern);
    }

    private static Task Subscribe(object consumer, SubscriptionPattern pattern, IConsumerRebalanceListener listener)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Subscribe(pattern, listener);
            return Task.CompletedTask;
        }

        return ((IAsyncConsumer<byte[], byte[]>)consumer).Subscribe(pattern, listener);
    }

    private static Task SubscribeToTopics(object consumer)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Subscribe(new[] { "topic1" });
            return Task.CompletedTask;
        }

        return ((IAsyncConsumer<byte[], byte[]>)consumer).Subscribe(new[] { "topic1" });
    }

    private static Task SubscribeToTopics(object consumer, IConsumerRebalanceListener listener)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Subscribe(new[] { Topic }, listener);
            return Task.CompletedTask;
        }

        return ((IAsyncConsumer<byte[], byte[]>)consumer).Subscribe(new[] { Topic }, listener);
    }

    private static Task Rebalance(object consumer, params TopicPartition[] newAssignment)
    {
        if (consumer is MockConsumer<byte[], byte[]> sync)
        {
            sync.Rebalance(newAssignment);
            return Task.CompletedTask;
        }

        return ((AsyncMockConsumer<byte[], byte[]>)consumer).Rebalance(newAssignment);
    }

    private static Task Close(object consumer)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Close();
            return Task.CompletedTask;
        }

        return ((IAsyncConsumer<byte[], byte[]>)consumer).Close();
    }

    private static async Task AssertIllegalArgument(string message, Func<Task> call)
    {
        IllegalArgumentException thrown = await Assert.ThrowsAsync<IllegalArgumentException>(
            () => TestTimeout.Run(call, s_deadline));
        Assert.Equal(message, thrown.Message);
    }

    private static async Task AssertNullPatternText(Func<Task> call)
    {
        ArgumentNullException thrown = await Assert.ThrowsAsync<ArgumentNullException>(() => TestTimeout.Run(call, s_deadline));
        Assert.Equal("pattern", thrown.ParamName);
        Assert.StartsWith(NullPatternText, thrown.Message, StringComparison.Ordinal);
    }

    private static async Task AssertNulPatternText(Func<Task> call)
    {
        ArgumentException thrown = await Assert.ThrowsAsync<ArgumentException>(() => TestTimeout.Run(call, s_deadline));
        Assert.Equal("pattern", thrown.ParamName);
        Assert.StartsWith(NulPatternText, thrown.Message, StringComparison.Ordinal);
    }

    private static async Task AssertGroupIdNotDefined(Func<Task> call)
    {
        InvalidGroupIdException thrown = await Assert.ThrowsAsync<InvalidGroupIdException>(
            () => TestTimeout.Run(call, s_deadline));
        Assert.Equal(InvalidGroupIdCode, thrown.Code);
        Assert.Equal(GroupIdNotDefined, thrown.Message);
    }

    private static async Task AssertClosed(Func<Task> call)
    {
        IllegalStateException thrown = await Assert.ThrowsAsync<IllegalStateException>(() => TestTimeout.Run(call, s_deadline));
        Assert.Equal(ClosedMessage, thrown.Message);
    }

    private interface IRecordingListener
    {
        List<TopicPartition[]> Revoked { get; }

        List<TopicPartition[]> Assigned { get; }
    }

    private sealed class RecordingListener : IConsumerRebalanceListener, IRecordingListener
    {
        public List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        public List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Revoked.Add(partitions.ToArray());

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Assigned.Add(partitions.ToArray());

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Revoked.Add(partitions.ToArray());
    }

    private sealed class RecordingAsyncListener : IAsyncConsumerRebalanceListener, IRecordingListener
    {
        public List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        public List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

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

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
            Revoked.Add(partitions.ToArray());
            return Task.CompletedTask;
        }
    }
}
