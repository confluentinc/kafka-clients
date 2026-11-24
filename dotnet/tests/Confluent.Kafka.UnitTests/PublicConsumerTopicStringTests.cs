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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A topic, or the metadata of an offset to commit, holding a NUL character, on every consumer method that takes one, on
/// the four consumers: <see cref="ArgumentException"/> with the parameter's name, before anything reaches the native
/// client, which would read the string only up to the NUL. Java's checks come first (the closed check, a null element, the
/// group check, a negative target time), and a consumer call made from a rebalance listener is refused the same way.
/// </summary>
public sealed class PublicConsumerTopicStringTests
{
    private const string NulTopic = "t\0u";

    private const string NulTopicMessage =
        "A topic must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    private const string NulMetadataMessage =
        "Offset metadata must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    private const string ClosedMessage = "This consumer has already been closed.";

    private const string GroupIdNotDefined =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(5);

    private static readonly TopicPartition s_nulPartition = new TopicPartition(NulTopic, 0);

    private static readonly TopicPartition[] s_nulPartitions = { new TopicPartition("ok", 0), s_nulPartition };

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulTopicOffsets =
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_nulPartition] = new OffsetAndMetadata(5) };

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulMetadataOffsets =
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("ok", 0)] = new OffsetAndMetadata(5, "m\0x") };

    private static readonly IReadOnlyDictionary<TopicPartition, long> s_nulTimestamps =
        new Dictionary<TopicPartition, long> { [s_nulPartition] = 1 };

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

    /// <summary>One consumer call per method and overload that takes a topic, with the parameter it names.</summary>
    private static IEnumerable<(string Name, string Param, Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> TopicCalls() => new (string, string, Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)[]
    {
        ("Subscribe", "topics", c => c.Subscribe(new[] { "ok", NulTopic }), c => c.Subscribe(new[] { "ok", NulTopic })),
        ("Subscribe+listener", "topics", c => c.Subscribe(new[] { NulTopic }, new NoListener()), c => c.Subscribe(new[] { NulTopic }, new NoListener())),
        ("Assign", "partitions", c => c.Assign(s_nulPartitions), c => c.Assign(s_nulPartitions)),
        ("Pause", "partitions", c => c.Pause(s_nulPartitions), c => c.Pause(s_nulPartitions)),
        ("Resume", "partitions", c => c.Resume(s_nulPartitions), c => c.Resume(s_nulPartitions)),
        ("SeekToBeginning", "partitions", c => c.SeekToBeginning(s_nulPartitions), c => c.SeekToBeginning(s_nulPartitions)),
        ("SeekToEnd", "partitions", c => c.SeekToEnd(s_nulPartitions), c => c.SeekToEnd(s_nulPartitions)),
        ("Seek", "partition", c => c.Seek(s_nulPartition, 1), c => c.Seek(s_nulPartition, 1)),
        ("Seek+metadata", "partition", c => c.Seek(s_nulPartition, new OffsetAndMetadata(1)), c => c.Seek(s_nulPartition, new OffsetAndMetadata(1))),
        ("Position", "partition", c => c.Position(s_nulPartition), c => c.Position(s_nulPartition)),
        ("Position+timeout", "partition", c => c.Position(s_nulPartition, s_timeout), c => c.Position(s_nulPartition, s_timeout)),
        ("CurrentLag", "topicPartition", c => c.CurrentLag(s_nulPartition), c => c.CurrentLag(s_nulPartition)),
        ("Commit", "offsets", c => c.Commit(s_nulTopicOffsets), c => c.Commit(s_nulTopicOffsets)),
        ("Commit+timeout", "offsets", c => c.Commit(s_nulTopicOffsets, s_timeout), c => c.Commit(s_nulTopicOffsets, s_timeout)),
        ("CommitAsync", "offsets", c => c.CommitAsync(s_nulTopicOffsets, (IOffsetCommitCallback?)null), c => c.CommitAsync(s_nulTopicOffsets, (IOffsetCommitCallback?)null)),
        ("Committed", "partitions", c => c.Committed(s_nulPartitions), c => c.Committed(s_nulPartitions)),
        ("Committed+timeout", "partitions", c => c.Committed(s_nulPartitions, s_timeout), c => c.Committed(s_nulPartitions, s_timeout)),
        ("BeginningOffsets", "partitions", c => c.BeginningOffsets(s_nulPartitions), c => c.BeginningOffsets(s_nulPartitions)),
        ("BeginningOffsets+timeout", "partitions", c => c.BeginningOffsets(s_nulPartitions, s_timeout), c => c.BeginningOffsets(s_nulPartitions, s_timeout)),
        ("EndOffsets", "partitions", c => c.EndOffsets(s_nulPartitions), c => c.EndOffsets(s_nulPartitions)),
        ("EndOffsets+timeout", "partitions", c => c.EndOffsets(s_nulPartitions, s_timeout), c => c.EndOffsets(s_nulPartitions, s_timeout)),
        ("PartitionsFor", "topic", c => c.PartitionsFor(NulTopic), c => c.PartitionsFor(NulTopic)),
        ("PartitionsFor+timeout", "topic", c => c.PartitionsFor(NulTopic, s_timeout), c => c.PartitionsFor(NulTopic, s_timeout)),
    };

    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task EveryTopicArgument_WithANul_ThrowsArgumentException_AndLeavesTheConsumerUsable(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        foreach ((string name, string param, Action<IConsumer<byte[], byte[]>> sync, Func<IAsyncConsumer<byte[], byte[]>, Task> async) in TopicCalls())
        {
            AssertNul(await Outcome(consumer, sync, async), NulTopicMessage, param, name);
        }

        Assert.Empty(((IConsumerCommon)consumer).Subscription());
        Assert.Empty(((IConsumerCommon)consumer).Assignment());
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task OffsetsForTimes_WithANulTopic_ThrowsArgumentException(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        AssertNul(
            await Outcome(consumer, c => c.OffsetsForTimes(s_nulTimestamps), c => c.OffsetsForTimes(s_nulTimestamps)),
            NulTopicMessage,
            "timestampsToSearch",
            "OffsetsForTimes");
        AssertNul(
            await Outcome(consumer, c => c.OffsetsForTimes(s_nulTimestamps, s_timeout), c => c.OffsetsForTimes(s_nulTimestamps, s_timeout)),
            NulTopicMessage,
            "timestampsToSearch",
            "OffsetsForTimes+timeout");
    }

    // Java's MockConsumer answers every offsetsForTimes with UnsupportedOperationException, whatever the map holds.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnTheMocks_OffsetsForTimes_WithANulTopic_IsJavasMockAnswer(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        Exception? error = await Outcome(consumer, c => c.OffsetsForTimes(s_nulTimestamps), c => c.OffsetsForTimes(s_nulTimestamps));
        Assert.Equal("Not implemented yet.", Assert.IsType<UnsupportedOperationException>(error).Message);
    }

    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task CommitMetadata_WithANul_ThrowsArgumentException(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        AssertNul(await Outcome(consumer, c => c.Commit(s_nulMetadataOffsets), c => c.Commit(s_nulMetadataOffsets)), NulMetadataMessage, "offsets", "Commit");
        AssertNul(
            await Outcome(consumer, c => c.Commit(s_nulMetadataOffsets, s_timeout), c => c.Commit(s_nulMetadataOffsets, s_timeout)),
            NulMetadataMessage,
            "offsets",
            "Commit+timeout");
        AssertNul(
            await Outcome(
                consumer,
                c => c.CommitAsync(s_nulMetadataOffsets, (IOffsetCommitCallback?)null),
                c => c.CommitAsync(s_nulMetadataOffsets, (IOffsetCommitCallback?)null)),
            NulMetadataMessage,
            "offsets",
            "CommitAsync");
    }

    // Neither Java's seek nor the core reads the metadata: a NUL in it changes nothing.
    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task Seek_MetadataWithANul_IsAnsweredAsAnyMetadata(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition partition = new TopicPartition("ok", 0);
        OffsetAndMetadata plain = new OffsetAndMetadata(1, "m");
        OffsetAndMetadata nul = new OffsetAndMetadata(1, "m\0x");
        Exception? expected = await Outcome(consumer, c => c.Seek(partition, plain), c => c.Seek(partition, plain));
        Exception? actual = await Outcome(consumer, c => c.Seek(partition, nul), c => c.Seek(partition, nul));
        Assert.NotNull(expected);
        Assert.Equal(expected!.GetType(), actual?.GetType());
        Assert.Equal(expected.Message, actual!.Message);
    }

    // Java's AutoOffsetResetStrategy.fromString names no strategy that holds a NUL.
    [Theory]
    [InlineData("earliest\0x", "Unknown auto offset reset strategy: earliest\0x")]
    [InlineData("by_duration:PT1H\0", "Unable to parse duration string in by_duration offset reset strategy.")]
    public void TheMockConstructors_AStrategyWithANul_IsJavasIllegalArgumentException(string strategy, string message)
    {
        Assert.Equal(
            message,
            Assert.Throws<IllegalArgumentException>(
                () => new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer(), strategy)).Message);
        Assert.Equal(
            message,
            Assert.Throws<IllegalArgumentException>(
                () => new AsyncMockConsumer<byte[], byte[]>(new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer(), strategy)).Message);
    }

    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task AClosedConsumer_ReportsItsClosedState_BeforeTheNul(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        await TestTimeout.Run(() => Close(consumer), s_deadline);
        ((IDisposable)consumer).Dispose();
        foreach ((string name, string _, Action<IConsumer<byte[], byte[]>> sync, Func<IAsyncConsumer<byte[], byte[]>, Task> async) in TopicCalls())
        {
            Exception? error = await Outcome(consumer, sync, async);
            Assert.True(error is IllegalStateException, $"{name}: {error}");
            Assert.Equal(ClosedMessage, error!.Message);
        }
    }

    // Java reaches a later null element, which the NUL before it does not stop.
    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task ANullElementAfterTheNul_GetsJavasError(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        Exception? subscribe = await Outcome(
            consumer, c => c.Subscribe(new[] { NulTopic, null! }), c => c.Subscribe(new[] { NulTopic, null! }));
        Assert.Equal(
            "Topic collection to subscribe to cannot contain null or empty topic",
            Assert.IsType<IllegalArgumentException>(subscribe).Message);

        TopicPartition[] partitions = { s_nulPartition, null! };
        Exception? assign = await Outcome(consumer, c => c.Assign(partitions), c => c.Assign(partitions));
        Assert.Equal(
            "Topic partitions to assign to cannot have null or empty topic",
            Assert.IsType<IllegalArgumentException>(assign).Message);
    }

    // Java checks the group before it reads the topics, the offsets or the partitions.
    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WithoutAGroupId_TheGroupCheckComesBeforeTheNul(string kind)
    {
        object consumer = await Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        AssertGroupIdNotDefined(await Outcome(consumer, c => c.Subscribe(new[] { NulTopic }), c => c.Subscribe(new[] { NulTopic })));
        AssertGroupIdNotDefined(await Outcome(consumer, c => c.Commit(s_nulTopicOffsets), c => c.Commit(s_nulTopicOffsets)));
        AssertGroupIdNotDefined(
            await Outcome(consumer, c => c.Commit(s_nulTopicOffsets, s_timeout), c => c.Commit(s_nulTopicOffsets, s_timeout)));
        AssertGroupIdNotDefined(await Outcome(consumer, c => c.Committed(s_nulPartitions), c => c.Committed(s_nulPartitions)));
    }

    // Java's offsetsForTimes checks every target time before it reads a topic.
    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task ANegativeTargetTime_ComesBeforeTheNul(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long> { [s_nulPartition] = -1 };
        Exception? error = await Outcome(consumer, c => c.OffsetsForTimes(search), c => c.OffsetsForTimes(search));
        Assert.Equal(
            "The target time for partition " + NulTopic + "-0 is -1. The target time cannot be negative.",
            Assert.IsType<IllegalArgumentException>(error).Message);
    }

    // A call from a listener goes to the core's handle; the NUL is refused before it, as outside a callback.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task FromARebalanceListener_ANulTopic_ThrowsArgumentException(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        CallingListener listener = new CallingListener((IConsumerCommon)consumer);
        TopicPartition assigned = new TopicPartition("ok", 0);
        if (consumer is MockConsumer<byte[], byte[]> sync)
        {
            sync.Subscribe(new[] { "ok" }, listener);
            sync.Rebalance(new[] { assigned });
        }
        else
        {
            AsyncMockConsumer<byte[], byte[]> async = (AsyncMockConsumer<byte[], byte[]>)consumer;
            await TestTimeout.Run(() => async.Subscribe(new[] { "ok" }, listener), s_deadline);
            await TestTimeout.Run(() => async.Rebalance(new[] { assigned }), s_deadline);
        }

        Assert.Equal(new[] { "Seek", "Commit", "CommitAsync" }, listener.Errors.Select(e => e.Name));
        foreach ((string name, Exception? error) in listener.Errors)
        {
            AssertNul(error, NulTopicMessage, name == "Seek" ? "partition" : "offsets", name);
        }

        // The seek's metadata, which neither Java nor the core reads, is answered as plain metadata from a listener too.
        Assert.Equal(listener.PlainMetadataSeek?.GetType(), listener.NulMetadataSeek?.GetType());
        Assert.Equal(listener.PlainMetadataSeek?.Message, listener.NulMetadataSeek?.Message);
    }

    // Java's KIP-848 enforceRebalance only logs its reason, so a NUL in it is no error.
    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task EnforceRebalance_AReasonWithANul_DoesNotThrow(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        ((IConsumerCommon)consumer).EnforceRebalance("rea\0son");
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task TheMockHelpers_RefuseANul(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        Dictionary<TopicPartition, long> durations = new Dictionary<TopicPartition, long> { [s_nulPartition] = 1 };
        if (consumer is MockConsumer<byte[], byte[]> sync)
        {
            AssertNul(Record.Exception(() => sync.AddRecord(NulTopic, 0, 0, null, null)), NulTopicMessage, "topic", "AddRecord");
            AssertNul(Record.Exception(() => sync.UpdateBeginningOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateBeginningOffset");
            AssertNul(Record.Exception(() => sync.UpdateEndOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateEndOffset");
            AssertNul(Record.Exception(() => sync.UpdatePartitions(NulTopic, 1, 0, "h", 1)), NulTopicMessage, "topic", "UpdatePartitions");
            AssertNul(Record.Exception(() => sync.UpdateDurationOffsets(durations)), NulTopicMessage, "newOffsets", "UpdateDurationOffsets");
            AssertNul(Record.Exception(() => sync.Rebalance(s_nulPartitions)), NulTopicMessage, "newAssignment", "Rebalance");
            AssertNul(Record.Exception(() => sync.LosePartitions(s_nulPartitions)), NulTopicMessage, "partitionsLost", "LosePartitions");
        }
        else
        {
            AsyncMockConsumer<byte[], byte[]> async = (AsyncMockConsumer<byte[], byte[]>)consumer;
            AssertNul(Record.Exception(() => async.AddRecord(NulTopic, 0, 0, null, null)), NulTopicMessage, "topic", "AddRecord");
            AssertNul(Record.Exception(() => async.UpdateBeginningOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateBeginningOffset");
            AssertNul(Record.Exception(() => async.UpdateEndOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateEndOffset");
            AssertNul(Record.Exception(() => async.UpdatePartitions(NulTopic, 1, 0, "h", 1)), NulTopicMessage, "topic", "UpdatePartitions");
            AssertNul(Record.Exception(() => async.UpdateDurationOffsets(durations)), NulTopicMessage, "newOffsets", "UpdateDurationOffsets");
            AssertNul(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => async.Rebalance(s_nulPartitions), s_deadline)),
                NulTopicMessage,
                "newAssignment",
                "Rebalance");
            AssertNul(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => async.LosePartitions(s_nulPartitions), s_deadline)),
                NulTopicMessage,
                "partitionsLost",
                "LosePartitions");
        }

        Assert.Empty(((IConsumerCommon)consumer).Assignment());
    }

    private static void AssertNul(Exception? error, string message, string param, string name)
    {
        Assert.True(error is not null && error.GetType() == typeof(ArgumentException), $"{name}: {error}");
        ArgumentException refused = (ArgumentException)error!;
        Assert.Equal(param, refused.ParamName);
        Assert.Equal(new ArgumentException(message, param).Message, refused.Message);
    }

    private static void AssertGroupIdNotDefined(Exception? error) =>
        Assert.Equal(GroupIdNotDefined, Assert.IsType<InvalidGroupIdException>(error).Message);

    // The exception the call gives, thrown at the call or faulting the returned Task.
    private static async Task<Exception?> Outcome(
        object consumer,
        Action<IConsumer<byte[], byte[]>> sync,
        Func<IAsyncConsumer<byte[], byte[]>, Task> async)
    {
        if (consumer is IConsumer<byte[], byte[]> syncConsumer)
        {
            return Record.Exception(() => sync(syncConsumer));
        }

        return await Record.ExceptionAsync(
            () => TestTimeout.Run(() => async((IAsyncConsumer<byte[], byte[]>)consumer), s_deadline));
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

    private static async Task<object> Create(string kind, bool groupId) => kind switch
    {
        "KafkaConsumer" => KafkaConsumer<byte[], byte[]>.Create(Config(groupId), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        "AsyncKafkaConsumer" => await AsyncKafkaConsumer<byte[], byte[]>.Create(Config(groupId), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer()),
        "MockConsumer" => new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        _ => new AsyncMockConsumer<byte[], byte[]>(new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer()),
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
            config["group.id"] = "topic-string-group";
        }

        return config;
    }

    /// <summary>A listener that does nothing.</summary>
    private sealed class NoListener : IConsumerRebalanceListener
    {
        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>A listener that calls back into its consumer with a NUL topic and records what each call throws.</summary>
    private sealed class CallingListener : IConsumerRebalanceListener
    {
        private readonly IConsumerCommon _consumer;

        internal CallingListener(IConsumerCommon consumer) => _consumer = consumer;

        internal List<(string Name, Exception? Error)> Errors { get; } = new List<(string Name, Exception? Error)>();

        internal Exception? PlainMetadataSeek { get; private set; }

        internal Exception? NulMetadataSeek { get; private set; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            if (_consumer is IConsumer<byte[], byte[]> sync)
            {
                Errors.Add(("Seek", Record.Exception(() => sync.Seek(s_nulPartition, 1))));
                Errors.Add(("Commit", Record.Exception(() => sync.Commit(s_nulTopicOffsets))));
                Errors.Add(("CommitAsync", Record.Exception(() => sync.CommitAsync(s_nulTopicOffsets, (IOffsetCommitCallback?)null))));
                PlainMetadataSeek = Record.Exception(() => sync.Seek(partitions.First(), new OffsetAndMetadata(1, "m")));
                NulMetadataSeek = Record.Exception(() => sync.Seek(partitions.First(), new OffsetAndMetadata(1, "m\0x")));
                return;
            }

            IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)_consumer;
            Errors.Add(("Seek", Record.ExceptionAsync(() => async.Seek(s_nulPartition, 1)).GetAwaiter().GetResult()));
            Errors.Add(("Commit", Record.ExceptionAsync(() => async.Commit(s_nulTopicOffsets)).GetAwaiter().GetResult()));
            Errors.Add((
                "CommitAsync",
                Record.ExceptionAsync(() => async.CommitAsync(s_nulTopicOffsets, (IOffsetCommitCallback?)null)).GetAwaiter().GetResult()));
            PlainMetadataSeek = Record.ExceptionAsync(
                () => async.Seek(partitions.First(), new OffsetAndMetadata(1, "m"))).GetAwaiter().GetResult();
            NulMetadataSeek = Record.ExceptionAsync(
                () => async.Seek(partitions.First(), new OffsetAndMetadata(1, "m\0x"))).GetAwaiter().GetResult();
        }
    }
}
