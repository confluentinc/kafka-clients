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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>MockConsumerTest</c> case by case, on the typed records Java adds, and the mock consumers' Java helpers
/// (<c>updateDurationOffsets</c>, <c>setMaxPollRecords</c>, <c>setOffsetsException</c>, <c>scheduleNopPollTask</c>,
/// <c>lastPollTimeout</c>, <c>closed</c>) where Java has no case. Java's <c>testRebalanceListener</c> and the
/// <c>losePartitions</c> cases are in <see cref="PublicConsumerRebalanceListenerTests"/>,
/// <c>testRe2JPatternSubscription</c> in <see cref="PublicConsumerPatternSubscribeTests"/>.
/// </summary>
public sealed class PublicMockConsumerJavaHelpersTests
{
    private const string Earliest = "earliest";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(1);

    private static readonly TopicPartition s_test0 = new TopicPartition("test", 0);

    private static readonly TopicPartition s_test1 = new TopicPartition("test", 1);

    [Fact]
    public void TestSimpleMock()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { "test" });
        Assert.Equal(0, consumer.Poll(TimeSpan.Zero).Count());
        consumer.Rebalance(new[] { s_test0, s_test1 });
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        consumer.UpdateBeginningOffset("test", 0, 0L);
        consumer.UpdateBeginningOffset("test", 1, 0L);
        consumer.Seek(s_test0, 0);
        ConsumerRecord<string, string> rec1 = Record(0, "key1", "value1");
        ConsumerRecord<string, string> rec2 = Record(1, "key2", "value2");
        consumer.AddRecord(rec1);
        consumer.AddRecord(rec2);

        ConsumerRecords<string, string> recs = Poll(consumer);

        AssertSameRecords(new[] { rec1, rec2 }, recs);
        Assert.Equal(2L, consumer.Position(s_test0));
        Assert.Single(recs.NextOffsets());
        Assert.Equal(new OffsetAndMetadata(2, null, ""), recs.NextOffsets()[s_test0]);
        consumer.Commit();
        Assert.Equal(2L, consumer.Committed(new[] { s_test0 })[s_test0]!.Offset());
    }

    [Fact]
    public async Task TestSimpleMock_Async()
    {
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Subscribe(new[] { "test" });
        Assert.Equal(0, (await consumer.Poll(TimeSpan.Zero)).Count());
        await consumer.Rebalance(new[] { s_test0, s_test1 });
        consumer.UpdateBeginningOffset("test", 0, 0L);
        consumer.UpdateBeginningOffset("test", 1, 0L);
        await consumer.Seek(s_test0, 0);
        ConsumerRecord<string, string> rec1 = Record(0, "key1", "value1");
        ConsumerRecord<string, string> rec2 = Record(1, "key2", "value2");
        consumer.AddRecord(rec1);
        consumer.AddRecord(rec2);

        ConsumerRecords<string, string> recs = await PollAsync(consumer);

        AssertSameRecords(new[] { rec1, rec2 }, recs);
        Assert.Equal(2L, await consumer.Position(s_test0));
        Assert.Equal(new OffsetAndMetadata(2, null, ""), Assert.Single(recs.NextOffsets()).Value);
        await consumer.Commit();
        Assert.Equal(2L, (await consumer.Committed(new[] { s_test0 }))[s_test0]!.Offset());
    }

    [Fact]
    public void TestConsumerRecordsIsEmptyWhenReturningNoRecords()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_test0 });
        consumer.AddRecord(new ConsumerRecord<string, string>("test", 0, 0, null, null));
        consumer.UpdateEndOffset("test", 0, 1L);
        consumer.SeekToEnd(new[] { s_test0 });

        ConsumerRecords<string, string> records = Poll(consumer);

        Assert.Equal(0, records.Count());
        Assert.True(records.IsEmpty());
    }

    [Fact]
    public void ShouldNotClearRecordsForPausedPartitions()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition[] testPartitionList = { s_test0 };
        consumer.Assign(testPartitionList);
        consumer.AddRecord(new ConsumerRecord<string, string>("test", 0, 0, null, null));
        consumer.UpdateBeginningOffset("test", 0, 0L);
        consumer.SeekToBeginning(testPartitionList);

        consumer.Pause(testPartitionList);
        _ = Poll(consumer);
        consumer.Resume(testPartitionList);
        ConsumerRecords<string, string> recordsSecondPoll = Poll(consumer);

        Assert.Equal(1, recordsSecondPoll.Count());
        Assert.Single(recordsSecondPoll.NextOffsets());
        Assert.Equal(new OffsetAndMetadata(1, null, ""), recordsSecondPoll.NextOffsets()[s_test0]);
    }

    [Fact]
    public void EndOffsetsShouldBeIdempotent()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset("test", 0, 10L);
        // consumer.EndOffsets should NOT change the value of end offsets
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(10L, consumer.EndOffsets(new[] { s_test0 })[s_test0]);
        }

        consumer.UpdateEndOffset("test", 0, 11L);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(11L, consumer.EndOffsets(new[] { s_test0 })[s_test0]);
        }
    }

    [Fact]
    public void TestDurationBasedOffsetReset()
    {
        MockConsumer<string, string> consumer =
            new MockConsumer<string, string>("by_duration:PT1H");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { "test" });
        consumer.Rebalance(new[] { s_test0, s_test1 });
        consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [s_test0] = 10L, [s_test1] = 11L });
        ConsumerRecord<string, string> rec1 = Record(10L, "key1", "value1");
        ConsumerRecord<string, string> rec2 = Record(11L, "key2", "value2");
        consumer.AddRecord(rec1);
        consumer.AddRecord(rec2);

        ConsumerRecords<string, string> records = Poll(consumer);

        AssertSameRecords(new[] { rec1, rec2 }, records);
    }

    [Fact]
    public async Task TestDurationBasedOffsetReset_Async()
    {
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>("by_duration:PT1H");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Subscribe(new[] { "test" });
        await consumer.Rebalance(new[] { s_test0, s_test1 });
        consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [s_test0] = 10L, [s_test1] = 11L });
        ConsumerRecord<string, string> rec1 = Record(10L, "key1", "value1");
        ConsumerRecord<string, string> rec2 = Record(11L, "key2", "value2");
        consumer.AddRecord(rec1);
        consumer.AddRecord(rec2);

        ConsumerRecords<string, string> records = await PollAsync(consumer);

        AssertSameRecords(new[] { rec1, rec2 }, records);
    }

    [Fact]
    public void ShouldReturnMaxPollRecords()
    {
        MockConsumer<string, string> consumer = Assigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.SetMaxPollRecords(2L);
        Assert.Equal(2, Poll(consumer).Count());
        Assert.Equal(2, Poll(consumer).Count());

        consumer.SetMaxPollRecords(long.MaxValue);
        Assert.Equal(6, Poll(consumer).Count());
        Assert.Empty(Poll(consumer));
    }

    [Fact]
    public async Task ShouldReturnMaxPollRecords_Async()
    {
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Assign(new[] { s_test0 });
        consumer.UpdateBeginningOffset("test", 0, 0L);
        for (int offset = 0; offset < 10; offset++)
        {
            consumer.AddRecord(new ConsumerRecord<string, string>("test", 0, offset, null, null));
        }

        consumer.SetMaxPollRecords(2L);
        Assert.Equal(2, (await PollAsync(consumer)).Count());
        Assert.Equal(2, (await PollAsync(consumer)).Count());
        consumer.SetMaxPollRecords(long.MaxValue);
        Assert.Equal(6, (await PollAsync(consumer)).Count());
        Assert.True((await PollAsync(consumer)).IsEmpty());
    }

    [Fact]
    public void SetMaxPollRecords_BelowOne_ThrowsJavasError()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal(
            "MaxPollRecords must be strictly superior to 0",
            Assert.Throws<IllegalArgumentException>(() => consumer.SetMaxPollRecords(0L)).Message);
        Assert.Throws<IllegalArgumentException>(() => consumer.SetMaxPollRecords(-1L));
    }

    [Fact]
    public void SetOffsetsException_IsThrownOnceByTheNextOffsetQuery_TheSameInstance()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset("test", 0, 5L);
        consumer.UpdateEndOffset("test", 0, 9L);
        KafkaException injected = new KafkaException("offsets down");

        consumer.SetOffsetsException(injected);
        Assert.Same(injected, Assert.Throws<KafkaException>(() => consumer.BeginningOffsets(new[] { s_test0 })));
        Assert.Equal(5L, consumer.BeginningOffsets(new[] { s_test0 })[s_test0]);

        consumer.SetOffsetsException(injected);
        Assert.Same(injected, Assert.Throws<KafkaException>(() => consumer.EndOffsets(new[] { s_test0 })));
        Assert.Equal(9L, consumer.EndOffsets(new[] { s_test0 })[s_test0]);

        consumer.SetOffsetsException(new KafkaException("cleared"));
        consumer.SetOffsetsException(null);
        Assert.Equal(9L, consumer.EndOffsets(new[] { s_test0 })[s_test0]);
    }

    [Fact]
    public async Task SetOffsetsException_IsThrownOnceByTheNextOffsetQuery_Async()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset("test", 0, 5L);
        KafkaException injected = new KafkaException("offsets down");

        consumer.SetOffsetsException(injected);
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => consumer.BeginningOffsets(new[] { s_test0 })));
        Assert.Equal(5L, (await consumer.BeginningOffsets(new[] { s_test0 }))[s_test0]);
    }

    [Fact]
    public void ScheduleNopPollTask_LeavesThePollsRecordsAsTheyAre()
    {
        MockConsumer<string, string> consumer = Assigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.ScheduleNopPollTask();

        Assert.Equal(10, Poll(consumer).Count());
    }

    [Fact]
    public void LastPollTimeout_IsNullBeforeThePoll_ThenThePollsTimeout()
    {
        MockConsumer<string, string> consumer = Assigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Null(consumer.LastPollTimeout());
        TestTimeout.Run(() => consumer.Poll(TimeSpan.FromMilliseconds(250)), s_deadline);
        Assert.Equal(TimeSpan.FromMilliseconds(250), consumer.LastPollTimeout());
    }

    [Fact]
    public async Task LastPollTimeout_IsNullBeforeThePoll_ThenThePollsTimeout_Async()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Assign(new[] { s_test0 });
        consumer.UpdateBeginningOffset("test", 0, 0L);

        Assert.Null(consumer.LastPollTimeout());
        await TestTimeout.Run(() => consumer.Poll(TimeSpan.FromMilliseconds(250)), s_deadline);
        Assert.Equal(TimeSpan.FromMilliseconds(250), consumer.LastPollTimeout());
    }

    [Fact]
    public void LastPollTimeout_AnswersAnotherThreadWhileARebalanceListenerHoldsTheConsumer()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        bool workerAnswered = false;
        consumer.Subscribe(
            new[] { "test" },
            new AssignedListener(() =>
                workerAnswered = Task.Run(() => consumer.LastPollTimeout()).Wait(TimeSpan.FromSeconds(10))));

        TestTimeout.Run(() => consumer.Rebalance(new[] { s_test0 }), s_deadline);

        Assert.True(workerAnswered);
    }

    [Fact]
    public void Closed_IsFalseUntilTheConsumerCloses_AndTheOtherHelpersThenThrow()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.False(consumer.Closed());
        TestTimeout.Run(() => consumer.Close(), s_deadline);

        Assert.True(consumer.Closed());
        Assert.Throws<IllegalStateException>(() => consumer.LastPollTimeout());
        Assert.Throws<IllegalStateException>(() => consumer.SetMaxPollRecords(1L));
        Assert.Throws<IllegalStateException>(() => consumer.ScheduleNopPollTask());
        Assert.Throws<IllegalStateException>(() => consumer.SetOffsetsException(null));
        Assert.Throws<IllegalStateException>(
            () => consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [s_test0] = 1L }));
    }

    [Fact]
    public async Task Closed_IsTrueOnceClosedOrDisposed_Async()
    {
        AsyncMockConsumer<byte[], byte[]> closing = new AsyncMockConsumer<byte[], byte[]>("latest");
        using IDisposable closingDisposal = TestTimeout.Disposal(closing);
        Assert.False(closing.Closed());
        await TestTimeout.Run(() => closing.Close(), s_deadline);
        Assert.True(closing.Closed());

        AsyncMockConsumer<byte[], byte[]> disposed = new AsyncMockConsumer<byte[], byte[]>("latest");
        await TestTimeout.DisposeAsync(disposed);
        Assert.True(disposed.Closed());
    }

    // AutoOffsetResetStrategy.fromString's messages, which Java's MockConsumer(String) throws; Python's
    // test_constructor_rejects_an_invalid_strategy, with Java's "null" where Python says "None".
    [Theory]
    [InlineData(null, "Auto offset reset strategy is null")]
    [InlineData("by_duration", "<:duration> part is missing in by_duration auto offset reset strategy.")]
    [InlineData("EARLIEST", "Unknown auto offset reset strategy: EARLIEST")]
    [InlineData("by_duration:1H", "Unable to parse duration string in by_duration offset reset strategy.")]
    [InlineData("by_duration:-PT1H", "Unable to parse duration string in by_duration offset reset strategy.")]
    [InlineData("by_duration:PT", "Unable to parse duration string in by_duration offset reset strategy.")]
    public void Constructor_RejectsAnInvalidStrategy(string? strategy, string message)
    {
        Assert.Equal(
            message,
            Assert.Throws<IllegalArgumentException>(() => new MockConsumer<byte[], byte[]>(strategy!)).Message);
        Assert.Equal(
            message,
            Assert.Throws<IllegalArgumentException>(() => new AsyncMockConsumer<byte[], byte[]>(strategy!)).Message);
    }

    [Fact]
    public void UpdateDurationOffsets_RefusesANullMapOrANullTopic()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal("newOffsets", Assert.Throws<ArgumentNullException>(() => consumer.UpdateDurationOffsets(null!)).ParamName);
        Assert.Equal(
            "newOffsets",
            Assert.Throws<ArgumentNullException>(
                () => consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [new TopicPartition(null!, 0)] = 1L })).ParamName);
    }

    // Java's MockConsumerTest builds its consumer with the earliest reset strategy.
    private sealed class AssignedListener : IConsumerRebalanceListener
    {
        private readonly Action _onAssigned;

        internal AssignedListener(Action onAssigned) => _onAssigned = onAssigned;

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();
    }

    private static MockConsumer<string, string> Assigned()
    {
        MockConsumer<string, string> consumer =
            new MockConsumer<string, string>(Earliest);
        consumer.Assign(new[] { s_test0 });
        consumer.UpdateBeginningOffset("test", 0, 0L);
        for (int offset = 0; offset < 10; offset++)
        {
            consumer.AddRecord(new ConsumerRecord<string, string>("test", 0, offset, null, null));
        }

        return consumer;
    }

    // Java's ConsumerRecord(topic, partition, offset, 0L, CREATE_TIME, 0, 0, key, value, new RecordHeaders(),
    // Optional.empty()).
    private static ConsumerRecord<string, string> Record(long offset, string key, string value) =>
        new ConsumerRecord<string, string>(
            "test", 0, offset, 0L, TimestampType.CREATE_TIME, 0, 0, key, value, new RecordHeaders(), null);

    // Java asserts assertEquals(rec, iter.next()), the record itself, since its ConsumerRecord has no equals: the C
    // ABI carries no managed object, so the polled record is a new one holding the same key and value objects and
    // every field of the record added.
    private static void AssertSameRecords(
        IReadOnlyList<ConsumerRecord<string, string>> expected,
        ConsumerRecords<string, string> actual)
    {
        ConsumerRecord<string, string>[] polled = actual.ToArray();
        Assert.Equal(expected.Count, polled.Length);
        for (int i = 0; i < polled.Length; i++)
        {
            Assert.Same(expected[i].Key(), polled[i].Key());
            Assert.Same(expected[i].Value(), polled[i].Value());
            Assert.Equal(expected[i].Topic(), polled[i].Topic());
            Assert.Equal(expected[i].Partition(), polled[i].Partition());
            Assert.Equal(expected[i].Offset(), polled[i].Offset());
            Assert.Equal(expected[i].Timestamp(), polled[i].Timestamp());
            Assert.Equal(expected[i].TimestampType(), polled[i].TimestampType());
            Assert.Equal(expected[i].SerializedKeySize(), polled[i].SerializedKeySize());
            Assert.Equal(expected[i].SerializedValueSize(), polled[i].SerializedValueSize());
            Assert.Equal(expected[i].LeaderEpoch(), polled[i].LeaderEpoch());
            Assert.Empty(polled[i].Headers());
        }
    }

    private static ConsumerRecords<string, string> Poll(MockConsumer<string, string> consumer)
    {
        ConsumerRecords<string, string> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        return records;
    }

    private static async Task<ConsumerRecords<string, string>> PollAsync(AsyncMockConsumer<string, string> consumer)
    {
        ConsumerRecords<string, string> records = default!;
        await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);
        return records;
    }
}
