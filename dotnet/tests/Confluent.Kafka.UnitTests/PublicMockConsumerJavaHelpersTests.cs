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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The mock consumers' Java helpers: <c>updateDurationOffsets</c>, <c>setMaxPollRecords</c>,
/// <c>setOffsetsException</c>, <c>scheduleNopPollTask</c>, <c>lastPollTimeout</c> and <c>closed</c>, from Java's
/// <c>MockConsumerTest</c> where it has a case.
/// </summary>
public sealed class PublicMockConsumerJavaHelpersTests
{
    private const string Earliest = "earliest";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(1);

    private static readonly TopicPartition s_test0 = new TopicPartition("test", 0);

    private static readonly TopicPartition s_test1 = new TopicPartition("test", 1);

    [Fact]
    public void TestDurationBasedOffsetReset()
    {
        MockConsumer<string, string> consumer =
            new MockConsumer<string, string>(Serdes.String, Serdes.String, "by_duration:PT1H");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { "test" });
        consumer.Rebalance(new[] { s_test0, s_test1 });
        consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [s_test0] = 10L, [s_test1] = 11L });
        consumer.AddRecord("test", 0, 10L, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        consumer.AddRecord("test", 0, 11L, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));

        ConsumerRecords<string, string> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);

        Assert.Equal(new[] { 10L, 11L }, records.Select(record => record.Offset).ToArray());
        Assert.Equal(new[] { "key1", "key2" }, records.Select(record => record.Key).ToArray());
        Assert.Equal(new[] { "value1", "value2" }, records.Select(record => record.Value).ToArray());
    }

    [Fact]
    public async Task TestDurationBasedOffsetReset_Async()
    {
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(Serdes.String, Serdes.String, "by_duration:PT1H");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Subscribe(new[] { "test" });
        await consumer.Rebalance(new[] { s_test0, s_test1 });
        consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [s_test0] = 10L, [s_test1] = 11L });
        consumer.AddRecord("test", 0, 10L, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        consumer.AddRecord("test", 0, 11L, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));

        ConsumerRecords<string, string> records = default!;
        await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);

        Assert.Equal(new[] { 10L, 11L }, records.Select(record => record.Offset).ToArray());
    }

    [Fact]
    public void ShouldReturnMaxPollRecords()
    {
        MockConsumer<byte[], byte[]> consumer = Assigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.SetMaxPollRecords(2L);
        Assert.Equal(2, Poll(consumer).Count);
        Assert.Equal(2, Poll(consumer).Count);

        consumer.SetMaxPollRecords(long.MaxValue);
        Assert.Equal(6, Poll(consumer).Count);
        Assert.Empty(Poll(consumer));
    }

    [Fact]
    public async Task ShouldReturnMaxPollRecords_Async()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Assign(new[] { s_test0 });
        consumer.UpdateBeginningOffset("test", 0, 0L);
        for (int offset = 0; offset < 10; offset++)
        {
            consumer.AddRecord("test", 0, offset, null, null);
        }

        consumer.SetMaxPollRecords(2L);
        Assert.Equal(2, (await PollAsync(consumer)).Count);
        consumer.SetMaxPollRecords(long.MaxValue);
        Assert.Equal(8, (await PollAsync(consumer)).Count);
    }

    [Fact]
    public void SetMaxPollRecords_BelowOne_ThrowsJavasError()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal(
            "MaxPollRecords must be strictly superior to 0",
            Assert.Throws<IllegalArgumentException>(() => consumer.SetMaxPollRecords(0L)).Message);
        Assert.Throws<IllegalArgumentException>(() => consumer.SetMaxPollRecords(-1L));
    }

    [Fact]
    public void SetOffsetsException_IsThrownOnceByTheNextOffsetQuery_TheSameInstance()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
        MockConsumer<byte[], byte[]> consumer = Assigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.ScheduleNopPollTask();

        Assert.Equal(10, Poll(consumer).Count);
    }

    [Fact]
    public void LastPollTimeout_IsNullBeforeThePoll_ThenThePollsTimeout()
    {
        MockConsumer<byte[], byte[]> consumer = Assigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Null(consumer.LastPollTimeout());
        TestTimeout.Run(() => consumer.Poll(TimeSpan.FromMilliseconds(250)), s_deadline);
        Assert.Equal(TimeSpan.FromMilliseconds(250), consumer.LastPollTimeout());
    }

    [Fact]
    public async Task LastPollTimeout_IsNullBeforeThePoll_ThenThePollsTimeout_Async()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, Earliest);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Assign(new[] { s_test0 });
        consumer.UpdateBeginningOffset("test", 0, 0L);

        Assert.Null(consumer.LastPollTimeout());
        await TestTimeout.Run(() => consumer.Poll(TimeSpan.FromMilliseconds(250)), s_deadline);
        Assert.Equal(TimeSpan.FromMilliseconds(250), consumer.LastPollTimeout());
    }

    [Fact]
    public void Closed_IsFalseUntilTheConsumerCloses_AndTheOtherHelpersThenThrow()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
        AsyncMockConsumer<byte[], byte[]> closing = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable closingDisposal = TestTimeout.Disposal(closing);
        Assert.False(closing.Closed());
        await TestTimeout.Run(() => closing.Close(), s_deadline);
        Assert.True(closing.Closed());

        AsyncMockConsumer<byte[], byte[]> disposed = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(disposed);
        Assert.True(disposed.Closed());
    }

    [Fact]
    public void UpdateDurationOffsets_RefusesANullMapOrANullTopic()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal("newOffsets", Assert.Throws<ArgumentNullException>(() => consumer.UpdateDurationOffsets(null!)).ParamName);
        Assert.Equal(
            "newOffsets",
            Assert.Throws<ArgumentException>(
                () => consumer.UpdateDurationOffsets(new Dictionary<TopicPartition, long> { [default] = 1L })).ParamName);
    }

    // Java's MockConsumerTest builds its consumer with the earliest reset strategy.
    private static MockConsumer<byte[], byte[]> Assigned()
    {
        MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, Earliest);
        consumer.Assign(new[] { s_test0 });
        consumer.UpdateBeginningOffset("test", 0, 0L);
        for (int offset = 0; offset < 10; offset++)
        {
            consumer.AddRecord("test", 0, offset, null, null);
        }

        return consumer;
    }

    private static ConsumerRecords<byte[], byte[]> Poll(MockConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        return records;
    }

    private static async Task<ConsumerRecords<byte[], byte[]>> PollAsync(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> records = default!;
        await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);
        return records;
    }
}
