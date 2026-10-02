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
using System.Text;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M5/P8a — the <b>synchronous</b> consumer core loop exercised end to end through the
/// <b>public</b> <see cref="MockConsumer"/> / <see cref="IConsumer"/> surface (no broker):
/// assign → add records → <see cref="IConsumer.Poll"/> returns owned records; assign → seek
/// → <see cref="IConsumer.Position"/>; the broker-free commit family; subscribe / unsubscribe;
/// pause / resume; seekToBeginning / seekToEnd observed via a follow-up poll. Every blocking
/// <see cref="IConsumer.Poll"/> runs under a <see cref="TestTimeout"/> hang guard.
/// </summary>
/// <remarks>
/// The synchronous mirror of <c>PublicConsumerRoundTripTests</c> (the async suite): the sync
/// methods return their result directly (no <c>await</c>). Setup ops
/// (<see cref="IConsumer.Assign"/> / <see cref="IConsumerCommon.Seek(TopicPartition, long)"/> /
/// <see cref="MockConsumer.AddRecord"/> / commit) are instantaneous on the mock and called
/// directly; the potentially-blocking <see cref="IConsumer.Poll"/> is bounded by
/// <see cref="TestTimeout"/> so a stall fails fast rather than hanging the run.
/// </remarks>
public sealed class PublicSyncConsumerRoundTripTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    // SetPollError's injected failure is built via Error::local_illegal_state (a8205c5c),
    // which is a specific, meaningful code (LOCAL_ILLEGAL_STATE == -4) — not one of the
    // indistinct broker-free codes the M3/P3 finding #8 comment originally warned about.
    private const int LocalIllegalStateErrorCode = -4;

    private const string Topic = "sync-topic";
    private const int Partition = 0;

    // Assign + seek to offset 0 so the mock poll has a valid position — the canonical
    // broker-free poll setup, through the public sync MockConsumer surface.
    private static MockConsumer<byte[], byte[]> ReadyToPoll(string topic = Topic, int partition = Partition)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(topic, partition) });
        consumer.Seek(new TopicPartition(topic, partition), offset: 0);
        return consumer;
    }

    [Fact]
    public void Poll_RoundTripsAllPublicFields()
    {
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        byte[] key = Encoding.UTF8.GetBytes("key-1");
        byte[] value = Encoding.UTF8.GetBytes("value-1");
        consumer.AddRecord(Topic, Partition, offset: 42, key, value);

        ConsumerRecords<byte[], byte[]> records = Poll(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(Topic, record.Topic);
        Assert.Equal(Partition, record.Partition);
        Assert.Equal(42, record.Offset);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
        Assert.Empty(record.Headers);
        Assert.True(
            record.TimestampType is TimestampType.NoTimestampType
                or TimestampType.CreateTime
                or TimestampType.LogAppendTime);
    }

    [Fact]
    public void Poll_NonAsciiTopicAndBytes_RoundTripViaOutLen()
    {
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll(nonAsciiTopic);
        byte[] key = Encoding.UTF8.GetBytes("café-key-Ω");
        byte[] value = Encoding.UTF8.GetBytes("naïve-value-🎉");
        consumer.AddRecord(nonAsciiTopic, Partition, offset: 7, key, value);

        ConsumerRecords<byte[], byte[]> records = Poll(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(nonAsciiTopic, record.Topic);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public void Poll_TombstoneAndAbsentKey_MapToNull()
    {
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: null);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));
        Assert.Null(record.Key);
        Assert.Null(record.Value);
    }

    [Fact]
    public void Poll_EmptyKeyAndValue_MapToEmptyNonNull()
    {
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 2, key: Array.Empty<byte>(), value: Array.Empty<byte>());

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Empty(record.Key!);
        Assert.Empty(record.Value!);
    }

    [Fact]
    public void Poll_MultipleRecords_RoundTripInOrder()
    {
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        for (int i = 0; i < 5; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
        }

        ConsumerRecords<byte[], byte[]> records = Poll(consumer);

        Assert.Equal(5, records.Count);
        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, records.Select(r => r.Offset).ToArray());
    }

    [Fact]
    public void Poll_Empty_ReturnsNonNullZeroCount()
    {
        // Blocking Poll on an idle mock returns a non-null, empty ConsumerRecords — success,
        // not failure (the mock poll does not block for the full timeout; see the wakeup suite).
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();

        ConsumerRecords<byte[], byte[]> records = Poll(consumer);

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public void Poll_SetPollError_ThrowsKafkaException()
    {
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.SetPollError("boom");

        // The throwing poll is called DIRECTLY (not through the TestTimeout wrapper): the mock
        // poll takes the injected error synchronously (never blocks), so there is no hang to
        // guard, and TestTimeout.Run(Action) would surface the fault as an AggregateException.
        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.Poll(s_pollTimeout));

        Assert.Equal(LocalIllegalStateErrorCode, ex.Code);
        Assert.Contains("boom", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsRetriable);
    }

    [Fact]
    public void Poll_SetPollError_IsOneShot_ThenReusable()
    {
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.SetPollError("transient");

        // Direct call for the throwing poll (synchronous on the mock; see the sibling test).
        Assert.Throws<KafkaException>(() => consumer.Poll(s_pollTimeout));

        // The success poll keeps the TestTimeout hang guard.
        Assert.Empty(Poll(consumer));
    }

    // ---- Assign → Seek → Position ----

    [Fact]
    public void Seek_ThenPosition_RoundTrips()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });

        consumer.Seek(tp, 42L);

        Assert.Equal(42L, consumer.Position(tp));
    }

    // ---- Commit family (broker-free) ----

    [Fact]
    public void Commit_NoOffsets_BrokerFree_Succeeds()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        // No throw = success (the confirming commit lands broker-free on the mock).
        consumer.Commit();
    }

    [Fact]
    public void Commit_EmptyOffsets_BrokerFree_Succeeds()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        // An empty map commits nothing — a valid no-op, never a throw.
        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>());
    }

    [Fact]
    public void CommitAsync_FireAndForget_BrokerFree_Succeeds()
    {
        // The fire-and-forget commit (IConsumerCommon.CommitAsync) works from the sync consumer.
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        consumer.CommitAsync();
    }

    // ---- Subscribe / Unsubscribe (broker-free) ----

    [Fact]
    public void Subscribe_ThenSubscription_ReflectsTopics()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        consumer.Subscribe(new[] { "a", "b" });

        Assert.Equal(new HashSet<string> { "a", "b" }, new HashSet<string>(consumer.Subscription()));
    }

    [Fact]
    public void Unsubscribe_AfterSubscribe_ClearsSubscription()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Subscribe(new[] { "a" });

        consumer.Unsubscribe();

        Assert.Empty(consumer.Subscription());
    }

    // ---- Assign / Pause / Resume observable through the sync state reads ----

    [Fact]
    public void Assign_ThenAssignment_ReflectsPartitions()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, Partition);

        consumer.Assign(new[] { tp });

        Assert.Contains(tp, consumer.Assignment());
    }

    [Fact]
    public void Assign_Empty_ClearsAssignment()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        consumer.Assign(Array.Empty<TopicPartition>());

        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Pause_ThenPaused_ReflectsPartitions_ResumeClears()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });

        consumer.Pause(new[] { tp });
        Assert.Contains(tp, consumer.Paused());

        consumer.Resume(new[] { tp });
        Assert.Empty(consumer.Paused());
    }

    // ---- SeekToBeginning / SeekToEnd observed via a follow-up poll ----

    [Fact]
    public void SeekToBeginning_ResetsPositionObservedViaPoll()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });
        consumer.UpdateBeginningOffset(Topic, Partition, 10);

        consumer.SeekToBeginning(new[] { tp });

        // The reset is applied lazily on the next poll; position advances to the beginning offset.
        Poll(consumer);
        Assert.Equal(10L, consumer.Position(tp));
    }

    [Fact]
    public void SeekToEnd_ResetsPositionObservedViaPoll()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });
        consumer.UpdateEndOffset(Topic, Partition, 77);

        consumer.SeekToEnd(new[] { tp });

        Poll(consumer);
        Assert.Equal(77L, consumer.Position(tp));
    }

    // Every blocking Poll routes through the TestTimeout hang guard (PLAN §8) so a future
    // stall in the sync ABI or the mock fails the run fast instead of hanging it.
    private static ConsumerRecords<byte[], byte[]> Poll(IConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        TestTimeout.Run(() => result = consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
