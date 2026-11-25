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
using System.Threading;
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
/// group check, a negative target time, a blank topic), and a consumer call made from a rebalance listener is refused the
/// same way. Where Java looks the partition up in the assignment, which never holds a NUL topic, the answer is Java's for
/// a partition that is not assigned.
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

    private const string NulArgumentMessage =
        "A string argument must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    private const string PositionNotAssigned = "You can only check the position for partitions assigned to this consumer.";

    private const string NulNotAssigned = "No current assignment for partition t\0u-0";

    private const string BlankSubscribeTopicMessage = "Topic collection to subscribe to cannot contain null or empty topic";

    private const string BlankAssignTopicMessage = "Topic partitions to assign to cannot have null or empty topic";

    private const string UnparsableDurationMessage = "Unable to parse duration string in by_duration offset reset strategy.";

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

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulTopicThenNullKey =
        new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(s_nulPartition, new OffsetAndMetadata(5)),
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulMetadataThenNullKey =
        new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(new TopicPartition("ok", 0), new OffsetAndMetadata(5, "m\0x")),
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

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

    public static IEnumerable<object[]> AsyncConsumers() => new[]
    {
        new object[] { "AsyncMockConsumer" },
        new object[] { "AsyncKafkaConsumer" },
    };

    /// <summary>
    /// One consumer call per method and overload that takes a topic and does not look it up in the assignment, with the
    /// parameter it names.
    /// </summary>
    private static IEnumerable<(string Name, string Param, Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> TopicCalls() => new (string, string, Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)[]
    {
        ("Subscribe", "topics", c => c.Subscribe(new[] { "ok", NulTopic }), c => c.Subscribe(new[] { "ok", NulTopic })),
        ("Subscribe+listener", "topics", c => c.Subscribe(new[] { NulTopic }, new NoListener()), c => c.Subscribe(new[] { NulTopic }, new NoListener())),
        ("Assign", "partitions", c => c.Assign(s_nulPartitions), c => c.Assign(s_nulPartitions)),
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

    // The async CommitAsync with an async callback has its own check; TopicCalls and CommitMetadata_WithANul hold the forms
    // both flavours share. The closed check and, on AsyncKafkaConsumer, the group check come first, as for the others.
    [Theory]
    [MemberData(nameof(AsyncConsumers))]
    public async Task CommitAsyncWithAnAsyncCallback_ANulTopicOrMetadata_ThrowsArgumentException(string kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = (IAsyncConsumer<byte[], byte[]>)await Create(kind, groupId: true);
        using (TestTimeout.Disposal((IDisposable)consumer))
        {
            AssertNul(await AsyncCallbackCommit(consumer, s_nulTopicOffsets), NulTopicMessage, "offsets", "topic");
            AssertNul(await AsyncCallbackCommit(consumer, s_nulMetadataOffsets), NulMetadataMessage, "offsets", "metadata");
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
            Assert.Equal(ClosedMessage, Assert.IsType<IllegalStateException>(await AsyncCallbackCommit(consumer, s_nulTopicOffsets)).Message);
        }

        if (kind == "AsyncKafkaConsumer")
        {
            IAsyncConsumer<byte[], byte[]> noGroup = (IAsyncConsumer<byte[], byte[]>)await Create(kind, groupId: false);
            using IDisposable disposal = TestTimeout.Disposal((IDisposable)noGroup);
            AssertGroupIdNotDefined(await AsyncCallbackCommit(noGroup, s_nulTopicOffsets));
            AssertGroupIdNotDefined(await AsyncCallbackCommit(noGroup, s_nulMetadataOffsets));
        }
    }

    // A NUL is answered where a null key is, so a null key after it gets the null-key answer, as for the producer's
    // SendOffsetsToTransaction.
    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task ANullKeyAfterTheNul_GetsTheNullKeyAnswer(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        foreach (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets in new[] { s_nulTopicThenNullKey, s_nulMetadataThenNullKey })
        {
            AssertNullKey(await Outcome(consumer, c => c.Commit(offsets), c => c.Commit(offsets)), "Commit");
            AssertNullKey(await Outcome(consumer, c => c.Commit(offsets, s_timeout), c => c.Commit(offsets, s_timeout)), "Commit+timeout");
            AssertNullKey(
                await Outcome(
                    consumer,
                    c => c.CommitAsync(offsets, (IOffsetCommitCallback?)null),
                    c => c.CommitAsync(offsets, (IOffsetCommitCallback?)null)),
                "CommitAsync");
            if (consumer is IAsyncConsumer<byte[], byte[]> async)
            {
                AssertNullKey(await AsyncCallbackCommit(async, offsets), "CommitAsync+async callback");
            }
        }
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

    // Java's AutoOffsetResetStrategy.fromString names no strategy that holds a NUL, and gives no cause for a name it does
    // not know.
    [Theory]
    [InlineData("earliest\0x")]
    [InlineData("\0")]
    [InlineData("by_duration\0")]
    [InlineData("by_dur\0ation:PT1H")]
    public void TheMockConstructors_AStrategyWithANul_IsJavasIllegalArgumentException(string strategy)
    {
        foreach (IllegalArgumentException refused in MockConstructorRefusals(strategy))
        {
            Assert.Equal("Unknown auto offset reset strategy: " + strategy, refused.Message);
            Assert.Null(refused.InnerException);
        }
    }

    // Java's Duration.parse fails on the NUL, before fromString's negative check, and fromString wraps that failure as
    // its cause; the binding's answer is the one the core gives for a duration it cannot parse, cause included.
    [Theory]
    [InlineData("by_duration:PT1H\0")]
    [InlineData("by_duration:\0PT1H")]
    [InlineData("by_duration:-PT1H\0")]
    [InlineData("by_duration:\0")]
    public void TheMockConstructors_ADurationWithANul_CarriesTheParseFailure(string strategy)
    {
        IllegalArgumentException[] fromTheCore = MockConstructorRefusals("by_duration:1H").ToArray();
        IllegalArgumentException[] refusals = MockConstructorRefusals(strategy).ToArray();
        Assert.Equal(2, refusals.Length);
        for (int i = 0; i < refusals.Length; i++)
        {
            Assert.Equal(UnparsableDurationMessage, refusals[i].Message);
            IllegalArgumentException cause = Assert.IsType<IllegalArgumentException>(refusals[i].InnerException);
            Assert.Equal("Text cannot be parsed to a Duration", cause.Message);
            Assert.Null(cause.InnerException);

            Assert.Equal(fromTheCore[i].Message, refusals[i].Message);
            Assert.Equal(fromTheCore[i].InnerException?.GetType(), cause.GetType());
            Assert.Equal(fromTheCore[i].InnerException?.Message, cause.Message);
            Assert.Null(fromTheCore[i].InnerException?.InnerException);
        }
    }

    /// <summary>
    /// One call per method and overload that looks its partition up in the assignment, given the partition to name, with
    /// the partitions assigned before it ("ok-0") ahead of it where it takes a collection.
    /// </summary>
    private static IEnumerable<(string Name, Action<IConsumer<byte[], byte[]>, TopicPartition> Sync, Func<IAsyncConsumer<byte[], byte[]>, TopicPartition, Task> Async)> AssignmentCalls() => new (string, Action<IConsumer<byte[], byte[]>, TopicPartition>, Func<IAsyncConsumer<byte[], byte[]>, TopicPartition, Task>)[]
    {
        ("Seek", (c, tp) => c.Seek(tp, 1), (c, tp) => c.Seek(tp, 1)),
        ("Seek+metadata", (c, tp) => c.Seek(tp, new OffsetAndMetadata(1)), (c, tp) => c.Seek(tp, new OffsetAndMetadata(1))),
        ("Position", (c, tp) => c.Position(tp), (c, tp) => c.Position(tp)),
        ("Position+timeout", (c, tp) => c.Position(tp, s_timeout), (c, tp) => c.Position(tp, s_timeout)),
        ("Pause", (c, tp) => c.Pause(AfterOk(tp)), (c, tp) => c.Pause(AfterOk(tp))),
        ("Resume", (c, tp) => c.Resume(AfterOk(tp)), (c, tp) => c.Resume(AfterOk(tp))),
        ("SeekToBeginning", (c, tp) => c.SeekToBeginning(AfterOk(tp)), (c, tp) => c.SeekToBeginning(AfterOk(tp))),
        ("SeekToEnd", (c, tp) => c.SeekToEnd(AfterOk(tp)), (c, tp) => c.SeekToEnd(AfterOk(tp))),
        ("CurrentLag", (c, tp) => c.CurrentLag(tp), (c, tp) => c.CurrentLag(tp)),
    };

    // No topic holding a NUL is ever assigned (Assign and Subscribe refuse it, and no topic name holds one), so where Java
    // looks the partition up in the assignment, a NUL topic gets Java's answer for a partition that is not assigned: the
    // answer the core gives for a plain topic that is not assigned, the topic printed with its NUL. On the mocks,
    // CurrentLag answers 0, Java's MockConsumer answer for a partition with no end offset.
    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task WhereJavaLooksUpTheAssignment_ANulTopic_IsNotAssigned(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition[] ok = { new TopicPartition("ok", 0) };
        Assert.Null(await Outcome(consumer, c => c.Assign(ok), c => c.Assign(ok)));
        TopicPartition unassigned = new TopicPartition("x", 0);
        foreach ((string name, Action<IConsumer<byte[], byte[]>, TopicPartition> sync, Func<IAsyncConsumer<byte[], byte[]>, TopicPartition, Task> async) in AssignmentCalls())
        {
            Exception? nul = await Outcome(consumer, c => sync(c, s_nulPartition), c => async(c, s_nulPartition));
            if (name == "Pause")
            {
                // As for a null, the refused call pauses none of the partitions before the NUL topic.
                Assert.Empty(((IConsumerCommon)consumer).Paused());
            }

            if (name == "CurrentLag" && kind.Contains("Mock"))
            {
                Assert.True(nul is null, $"{name}: {nul}");
                Assert.Equal(0, await Lag(consumer, s_nulPartition));
                continue;
            }

            Exception? fromTheCore = await Outcome(consumer, c => sync(c, unassigned), c => async(c, unassigned));
            Assert.True(nul is not null && fromTheCore is not null, $"{name}: {nul} / {fromTheCore}");
            Assert.True(nul!.GetType() == fromTheCore!.GetType(), $"{name}: {nul} / {fromTheCore}");
            Assert.True(nul is IllegalStateException || (nul is IllegalArgumentException && kind.Contains("Mock")), $"{name}: {nul}");
            Assert.Equal(fromTheCore.Message.Replace("x-0", "t\0u-0"), nul.Message);
        }

        Assert.Equal(ok, ((IConsumerCommon)consumer).Assignment());
    }

    // AsyncMockConsumer answers 0 for a partition it can know nothing of (a null one, a null topic, a NUL topic) without a
    // native call, but an already cancelled token still ends the call first, as it does for any other partition.
    [Fact]
    public async Task OnTheAsyncMock_ACancelledCurrentLag_IsCancelledForAPartitionItAnswersWithZero()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(
            new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Exception? plain = await Record.ExceptionAsync(() => consumer.CurrentLag(new TopicPartition("x", 0), cancelled.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(plain);
        foreach (TopicPartition? partition in new[] { s_nulPartition, new TopicPartition(null!, 0), null })
        {
            Assert.Equal(0, await consumer.CurrentLag(partition!, CancellationToken.None));
            Exception? error = await Record.ExceptionAsync(() => consumer.CurrentLag(partition!, cancelled.Token));
            Assert.True(error?.GetType() == plain!.GetType(), $"{partition}: {error}");
        }
    }

    // Java finds the first partition that is not assigned in the collection's order, a null one or one with a NUL topic;
    // a partition with a null topic after the NUL topic is not reached.
    [Theory]
    [MemberData(nameof(AllConsumers))]
    public async Task ACollectionWithANulTopicAndANull_ReportsTheFirstInOrder(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition[] nulFirst = { s_nulPartition, null! };
        TopicPartition[] nullFirst = { null!, s_nulPartition };
        TopicPartition[] nulThenNullTopic = { s_nulPartition, new TopicPartition(null!, 0) };
        foreach ((string name, Action<IConsumer<byte[], byte[]>, IReadOnlyCollection<TopicPartition>> sync, Func<IAsyncConsumer<byte[], byte[]>, IReadOnlyCollection<TopicPartition>, Task> async) in new (string, Action<IConsumer<byte[], byte[]>, IReadOnlyCollection<TopicPartition>>, Func<IAsyncConsumer<byte[], byte[]>, IReadOnlyCollection<TopicPartition>, Task>)[]
        {
            ("Pause", (c, p) => c.Pause(p), (c, p) => c.Pause(p)),
            ("Resume", (c, p) => c.Resume(p), (c, p) => c.Resume(p)),
            ("SeekToBeginning", (c, p) => c.SeekToBeginning(p), (c, p) => c.SeekToBeginning(p)),
            ("SeekToEnd", (c, p) => c.SeekToEnd(p), (c, p) => c.SeekToEnd(p)),
        })
        {
            Exception? first = await Outcome(consumer, c => sync(c, nulFirst), c => async(c, nulFirst));
            Assert.True(first is IllegalStateException, $"{name}: {first}");
            Assert.Equal("No current assignment for partition t\0u-0", first!.Message);
            Exception? second = await Outcome(consumer, c => sync(c, nullFirst), c => async(c, nullFirst));
            Assert.True(second is IllegalStateException, $"{name}: {second}");
            Assert.Equal("No current assignment for partition null", second!.Message);
            Exception? third = await Outcome(consumer, c => sync(c, nulThenNullTopic), c => async(c, nulThenNullTopic));
            Assert.True(third is IllegalStateException, $"{name}: {third}");
            Assert.Equal(NulNotAssigned, third!.Message);
        }
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

        foreach ((string name, Action<IConsumer<byte[], byte[]>, TopicPartition> sync, Func<IAsyncConsumer<byte[], byte[]>, TopicPartition, Task> async) in AssignmentCalls())
        {
            Exception? error = await Outcome(consumer, c => sync(c, s_nulPartition), c => async(c, s_nulPartition));
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

    /// <summary>
    /// Topic sets holding a NUL topic and a blank one, a topic with no character above U+0020 (Java's <c>isBlank</c>,
    /// whose <c>trim()</c> strips a NUL too), in either order.
    /// </summary>
    public static IEnumerable<object[]> NulAndBlankTopics() =>
        from kind in RealConsumers()
        from topics in new[]
        {
            new[] { NulTopic, "" },
            new[] { "", NulTopic },
            new[] { "\0" },
            new[] { " \0 " },
            new[] { NulTopic, " \t" },
            new[] { NulTopic, "\u0001" },
        }
        select new object[] { kind[0], topics };

    /// <summary>
    /// Topic sets holding a NUL topic and no blank one: U+00A0 and U+3000 are white space to .NET, but above U+0020, so
    /// not blank to Java.
    /// </summary>
    public static IEnumerable<object[]> NulAndNoBlankTopics() =>
        from kind in RealConsumers()
        from topics in new[]
        {
            new[] { NulTopic },
            new[] { "\u00A0\0" },
            new[] { NulTopic, "\u00A0" },
            new[] { NulTopic, "\u3000" },
        }
        select new object[] { kind[0], topics };

    // Java's KafkaConsumer refuses a blank topic in subscribe and assign wherever it is in the collection, and Java has
    // no NUL check, so the blank topic is Java's answer; it is the answer the core gives for a blank topic it receives.
    [Theory]
    [MemberData(nameof(NulAndBlankTopics))]
    public async Task ABlankTopic_ComesBeforeTheNul(string kind, string[] topics)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition[] partitions = topics.Select(topic => new TopicPartition(topic, 0)).ToArray();

        Exception? subscribe = await Outcome(consumer, c => c.Subscribe(topics), c => c.Subscribe(topics));
        Assert.Equal(BlankSubscribeTopicMessage, Assert.IsType<IllegalArgumentException>(subscribe).Message);
        Exception? subscribeWithListener = await Outcome(
            consumer, c => c.Subscribe(topics, new NoListener()), c => c.Subscribe(topics, new NoListener()));
        Assert.Equal(BlankSubscribeTopicMessage, Assert.IsType<IllegalArgumentException>(subscribeWithListener).Message);
        Exception? assign = await Outcome(consumer, c => c.Assign(partitions), c => c.Assign(partitions));
        Assert.Equal(BlankAssignTopicMessage, Assert.IsType<IllegalArgumentException>(assign).Message);

        // The core's answer for a blank topic it receives, here one that is blank to Java and to the core alike.
        string[] blank = { " " };
        TopicPartition[] blankPartitions = { new TopicPartition(" ", 0) };
        Exception? fromTheCore = await Outcome(consumer, c => c.Subscribe(blank), c => c.Subscribe(blank));
        Assert.Equal(subscribe!.GetType(), fromTheCore?.GetType());
        Assert.Equal(subscribe.Message, fromTheCore!.Message);
        Exception? assignFromTheCore = await Outcome(consumer, c => c.Assign(blankPartitions), c => c.Assign(blankPartitions));
        Assert.Equal(assign!.GetType(), assignFromTheCore?.GetType());
        Assert.Equal(assign.Message, assignFromTheCore!.Message);

        Assert.Empty(((IConsumerCommon)consumer).Subscription());
        Assert.Empty(((IConsumerCommon)consumer).Assignment());
    }

    // A topic with characters above U+0020 is not blank to Java, white space to .NET or not, so the NUL is refused.
    [Theory]
    [MemberData(nameof(NulAndNoBlankTopics))]
    public async Task WithNoBlankTopic_TheNulIsRefused(string kind, string[] topics)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition[] partitions = topics.Select(topic => new TopicPartition(topic, 0)).ToArray();

        AssertNul(await Outcome(consumer, c => c.Subscribe(topics), c => c.Subscribe(topics)), NulTopicMessage, "topics", "Subscribe");
        AssertNul(
            await Outcome(consumer, c => c.Subscribe(topics, new NoListener()), c => c.Subscribe(topics, new NoListener())),
            NulTopicMessage,
            "topics",
            "Subscribe+listener");
        AssertNul(await Outcome(consumer, c => c.Assign(partitions), c => c.Assign(partitions)), NulTopicMessage, "partitions", "Assign");
        Assert.Empty(((IConsumerCommon)consumer).Subscription());
        Assert.Empty(((IConsumerCommon)consumer).Assignment());
    }

    // Java's subscribe checks the group before the topics; Java's assign makes no group check, so its blank topic still
    // comes before the NUL without a group.id.
    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WithoutAGroupId_ABlankTopicAndANul_GetTheGroupCheckOnSubscribe_AndTheBlankOnAssign(string kind)
    {
        object consumer = await Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        string[] topics = { NulTopic, "" };
        TopicPartition[] partitions = { s_nulPartition, new TopicPartition("", 0) };
        AssertGroupIdNotDefined(await Outcome(consumer, c => c.Subscribe(topics), c => c.Subscribe(topics)));
        Exception? assign = await Outcome(consumer, c => c.Assign(partitions), c => c.Assign(partitions));
        Assert.Equal(BlankAssignTopicMessage, Assert.IsType<IllegalArgumentException>(assign).Message);
    }

    // Java's MockConsumer has no blank check, so on the mocks the NUL is refused, as it is for any topic.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnTheMocks_ABlankTopic_DoesNotHideTheNul(string kind)
    {
        object consumer = await Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        foreach (string[] topics in new[] { new[] { NulTopic, "" }, new[] { "\0" } })
        {
            TopicPartition[] partitions = topics.Select(topic => new TopicPartition(topic, 0)).ToArray();
            AssertNul(await Outcome(consumer, c => c.Subscribe(topics), c => c.Subscribe(topics)), NulTopicMessage, "topics", "Subscribe");
            AssertNul(await Outcome(consumer, c => c.Assign(partitions), c => c.Assign(partitions)), NulTopicMessage, "partitions", "Assign");
        }

        Assert.Empty(((IConsumerCommon)consumer).Subscription());
        Assert.Empty(((IConsumerCommon)consumer).Assignment());
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

    // A call from a listener goes to the core's handle; the NUL is answered before it, as outside a callback.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task FromARebalanceListener_ANulTopic_IsAnsweredAsOutsideTheCallback(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        long? lag = -1;
        CallingListener listener = new CallingListener((IConsumerCommon)consumer, NulCalls(consumer, value => lag = value));
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

        string[] names =
        {
            "Seek", "Commit", "CommitAsync", "Position", "Pause", "Resume", "CurrentLag", "Committed", "PartitionsFor",
            "BeginningOffsets", "EndOffsets",
        };
        Assert.Equal(names, listener.Errors.Select(e => e.Name));
        Dictionary<string, Exception?> inListener = listener.Errors.ToDictionary(e => e.Name, e => e.Error);
        Assert.Equal(NulNotAssigned, Assert.IsType<IllegalStateException>(inListener["Seek"]).Message);
        AssertNul(inListener["Commit"], NulTopicMessage, "offsets", "Commit");
        AssertNul(inListener["CommitAsync"], NulTopicMessage, "offsets", "CommitAsync");
        Assert.Equal(PositionNotAssigned, Assert.IsType<IllegalArgumentException>(inListener["Position"]).Message);
        Assert.Equal(NulNotAssigned, Assert.IsType<IllegalStateException>(inListener["Pause"]).Message);
        Assert.Equal(NulNotAssigned, Assert.IsType<IllegalStateException>(inListener["Resume"]).Message);
        Assert.Null(inListener["CurrentLag"]);
        Assert.Equal(0, lag);
        AssertNul(inListener["Committed"], NulTopicMessage, "partitions", "Committed");
        AssertNul(inListener["PartitionsFor"], NulTopicMessage, "topic", "PartitionsFor");
        AssertNul(inListener["BeginningOffsets"], NulTopicMessage, "partitions", "BeginningOffsets");
        AssertNul(inListener["EndOffsets"], NulTopicMessage, "partitions", "EndOffsets");

        // Outside the callback each call gives the same answer.
        foreach ((string name, Func<Exception?> call) in NulCalls(consumer, _ => { }))
        {
            Exception? outside = call();
            Assert.True(outside?.GetType() == inListener[name]?.GetType(), $"{name}: {inListener[name]} / {outside}");
            Assert.Equal(inListener[name]?.Message, outside?.Message);
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
    public async Task TheMockHelpers_RefuseANul_AndAddRecordFindsItNotAssigned(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        Dictionary<TopicPartition, long> durations = new Dictionary<TopicPartition, long> { [s_nulPartition] = 1 };
        if (consumer is MockConsumer<byte[], byte[]> sync)
        {
            AssertAddRecordNotAssigned(Record.Exception(() => sync.AddRecord(NulTopic, 0, 0, null, null)), Record.Exception(() => sync.AddRecord("x", 0, 0, null, null)));
            AssertNul(Record.Exception(() => sync.UpdateBeginningOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateBeginningOffset");
            AssertNul(Record.Exception(() => sync.UpdateEndOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateEndOffset");
            AssertNul(Record.Exception(() => sync.UpdatePartitions(NulTopic, 1, 0, "h", 1)), NulTopicMessage, "topic", "UpdatePartitions");
            AssertNul(Record.Exception(() => sync.UpdatePartitions("ok", 1, 0, "h\0x", 1)), NulArgumentMessage, "leaderHost", "UpdatePartitions");
            AssertNul(Record.Exception(() => sync.UpdateDurationOffsets(durations)), NulTopicMessage, "newOffsets", "UpdateDurationOffsets");
            AssertNul(Record.Exception(() => sync.Rebalance(s_nulPartitions)), NulTopicMessage, "newAssignment", "Rebalance");
            AssertNul(Record.Exception(() => sync.LosePartitions(s_nulPartitions)), NulTopicMessage, "partitionsLost", "LosePartitions");
        }
        else
        {
            AsyncMockConsumer<byte[], byte[]> async = (AsyncMockConsumer<byte[], byte[]>)consumer;
            AssertAddRecordNotAssigned(Record.Exception(() => async.AddRecord(NulTopic, 0, 0, null, null)), Record.Exception(() => async.AddRecord("x", 0, 0, null, null)));
            AssertNul(Record.Exception(() => async.UpdateBeginningOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateBeginningOffset");
            AssertNul(Record.Exception(() => async.UpdateEndOffset(NulTopic, 0, 0)), NulTopicMessage, "topic", "UpdateEndOffset");
            AssertNul(Record.Exception(() => async.UpdatePartitions(NulTopic, 1, 0, "h", 1)), NulTopicMessage, "topic", "UpdatePartitions");
            AssertNul(Record.Exception(() => async.UpdatePartitions("ok", 1, 0, "h\0x", 1)), NulArgumentMessage, "leaderHost", "UpdatePartitions");
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

    // Java's MockConsumer checks the closed state first in addRecord and updatePartitions.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnAClosedMock_AddRecordAndUpdatePartitions_ReportTheClosedStateBeforeTheNul(string kind)
    {
        object consumer = await Create(kind, groupId: true);
        await TestTimeout.Run(() => Close(consumer), s_deadline);
        ((IDisposable)consumer).Dispose();
        List<Exception?> errors = new List<Exception?>();
        if (consumer is MockConsumer<byte[], byte[]> sync)
        {
            errors.Add(Record.Exception(() => sync.AddRecord(NulTopic, 0, 0, null, null)));
            errors.Add(Record.Exception(() => sync.UpdatePartitions(NulTopic, 1, 0, "h", 1)));
            errors.Add(Record.Exception(() => sync.UpdatePartitions("ok", 1, 0, "h\0x", 1)));
        }
        else
        {
            AsyncMockConsumer<byte[], byte[]> async = (AsyncMockConsumer<byte[], byte[]>)consumer;
            errors.Add(Record.Exception(() => async.AddRecord(NulTopic, 0, 0, null, null)));
            errors.Add(Record.Exception(() => async.UpdatePartitions(NulTopic, 1, 0, "h", 1)));
            errors.Add(Record.Exception(() => async.UpdatePartitions("ok", 1, 0, "h\0x", 1)));
        }

        Assert.Equal(3, errors.Count);
        foreach (Exception? error in errors)
        {
            Assert.Equal(ClosedMessage, Assert.IsType<IllegalStateException>(error).Message);
        }
    }

    // Java's MockConsumer refuses a record for a partition that is not assigned, the core's answer for a plain topic.
    private static void AssertAddRecordNotAssigned(Exception? nul, Exception? fromTheCore)
    {
        Assert.Equal(
            "Cannot add records for a partition that is not assigned to the consumer",
            Assert.IsType<IllegalStateException>(nul).Message);
        Assert.Equal(fromTheCore?.GetType(), nul!.GetType());
        Assert.Equal(fromTheCore!.Message, nul.Message);
    }

    // The ok partition the test assigned, then the partition to name.
    private static TopicPartition[] AfterOk(TopicPartition partition) => new[] { new TopicPartition("ok", 0), partition };

    // The lag CurrentLag answers, from either flavour.
    private static async Task<long?> Lag(object consumer, TopicPartition partition) =>
        consumer is IConsumer<byte[], byte[]> sync
            ? sync.CurrentLag(partition)
            : await ((IAsyncConsumer<byte[], byte[]>)consumer).CurrentLag(partition);

    private static void AssertNul(Exception? error, string message, string param, string name)
    {
        Assert.True(error is not null && error.GetType() == typeof(ArgumentException), $"{name}: {error}");
        ArgumentException refused = (ArgumentException)error!;
        Assert.Equal(param, refused.ParamName);
        Assert.Equal(new ArgumentException(message, param).Message, refused.Message);
    }

    // The refusal of each mock constructor, MockConsumer's then AsyncMockConsumer's, for the strategy.
    private static IEnumerable<IllegalArgumentException> MockConstructorRefusals(string strategy)
    {
        yield return Assert.Throws<IllegalArgumentException>(
            () => new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer(), strategy));
        yield return Assert.Throws<IllegalArgumentException>(
            () => new AsyncMockConsumer<byte[], byte[]>(new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer(), strategy));
    }

    // The calls a listener makes with a NUL topic, each giving what it throws; CurrentLag hands its lag to lag.
    private static List<(string Name, Func<Exception?> Call)> NulCalls(object consumer, Action<long?> lag)
    {
        TopicPartition[] nul = { s_nulPartition };
        if (consumer is IConsumer<byte[], byte[]> c)
        {
            return new List<(string, Func<Exception?>)>
            {
                ("Seek", () => Record.Exception(() => c.Seek(s_nulPartition, 1))),
                ("Commit", () => Record.Exception(() => c.Commit(s_nulTopicOffsets))),
                ("CommitAsync", () => Record.Exception(() => c.CommitAsync(s_nulTopicOffsets, (IOffsetCommitCallback?)null))),
                ("Position", () => Record.Exception(() => c.Position(s_nulPartition))),
                ("Pause", () => Record.Exception(() => c.Pause(nul))),
                ("Resume", () => Record.Exception(() => c.Resume(nul))),
                ("CurrentLag", () => Record.Exception(() => lag(c.CurrentLag(s_nulPartition)))),
                ("Committed", () => Record.Exception(() => c.Committed(nul))),
                ("PartitionsFor", () => Record.Exception(() => c.PartitionsFor(NulTopic))),
                ("BeginningOffsets", () => Record.Exception(() => c.BeginningOffsets(nul))),
                ("EndOffsets", () => Record.Exception(() => c.EndOffsets(nul))),
            };
        }

        IAsyncConsumer<byte[], byte[]> a = (IAsyncConsumer<byte[], byte[]>)consumer;
        Func<Exception?> Run(Func<Task> call) => () => Record.ExceptionAsync(call).GetAwaiter().GetResult();
        return new List<(string, Func<Exception?>)>
        {
            ("Seek", Run(() => a.Seek(s_nulPartition, 1))),
            ("Commit", Run(() => a.Commit(s_nulTopicOffsets))),
            ("CommitAsync", Run(() => a.CommitAsync(s_nulTopicOffsets, (IOffsetCommitCallback?)null))),
            ("Position", Run(() => a.Position(s_nulPartition))),
            ("Pause", Run(() => a.Pause(nul))),
            ("Resume", Run(() => a.Resume(nul))),
            ("CurrentLag", Run(async () => lag(await a.CurrentLag(s_nulPartition)))),
            ("Committed", Run(() => a.Committed(nul))),
            ("PartitionsFor", Run(() => a.PartitionsFor(NulTopic))),
            ("BeginningOffsets", Run(() => a.BeginningOffsets(nul))),
            ("EndOffsets", Run(() => a.EndOffsets(nul))),
        };
    }

    // CommitAsync(offsets, IAsyncOffsetCommitCallback), thrown at the call or faulting the returned Task.
    private static Task<Exception?> AsyncCallbackCommit(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        Record.ExceptionAsync(() => TestTimeout.Run(() => consumer.CommitAsync(offsets, new NoAsyncCommitCallback()), s_deadline));

    private static void AssertNullKey(Exception? error, string name)
    {
        Assert.True(error is not null && error.GetType() == typeof(ArgumentNullException), $"{name}: {error}");
        Assert.Equal("offsets", ((ArgumentNullException)error!).ParamName);
        Assert.Equal(new ArgumentNullException("offsets", "offsets must not contain null").Message, error.Message);
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

        private readonly IReadOnlyList<(string Name, Func<Exception?> Call)> _calls;

        internal CallingListener(IConsumerCommon consumer, IReadOnlyList<(string Name, Func<Exception?> Call)> calls)
        {
            _consumer = consumer;
            _calls = calls;
        }

        internal List<(string Name, Exception? Error)> Errors { get; } = new List<(string Name, Exception? Error)>();

        internal Exception? PlainMetadataSeek { get; private set; }

        internal Exception? NulMetadataSeek { get; private set; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            foreach ((string name, Func<Exception?> call) in _calls)
            {
                Errors.Add((name, call()));
            }

            if (_consumer is IConsumer<byte[], byte[]> sync)
            {
                PlainMetadataSeek = Record.Exception(() => sync.Seek(partitions.First(), new OffsetAndMetadata(1, "m")));
                NulMetadataSeek = Record.Exception(() => sync.Seek(partitions.First(), new OffsetAndMetadata(1, "m\0x")));
                return;
            }

            IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)_consumer;
            PlainMetadataSeek = Record.ExceptionAsync(
                () => async.Seek(partitions.First(), new OffsetAndMetadata(1, "m"))).GetAwaiter().GetResult();
            NulMetadataSeek = Record.ExceptionAsync(
                () => async.Seek(partitions.First(), new OffsetAndMetadata(1, "m\0x"))).GetAwaiter().GetResult();
        }
    }

    /// <summary>An async commit callback that does nothing.</summary>
    private sealed class NoAsyncCommitCallback : IAsyncOffsetCommitCallback
    {
        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            Task.CompletedTask;
    }
}
