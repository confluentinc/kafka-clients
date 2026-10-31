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
using System.Runtime.CompilerServices;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync consumer's core loop through <see cref="MockConsumer{TKey, TValue}"/>: Poll returns owned records,
/// Position after Seek, the commit family, Subscribe and Unsubscribe, Pause and Resume, and SeekToBeginning and
/// SeekToEnd seen through the next poll.
/// </summary>
public sealed class PublicSyncConsumerRoundTripTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "sync-topic";
    private const int Partition = 0;

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
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
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
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll(nonAsciiTopic);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
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
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: null);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));
        Assert.Null(record.Key);
        Assert.Null(record.Value);
    }

    [Fact]
    public void Poll_EmptyKeyAndValue_MapToEmptyNonNull()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
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
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
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
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerRecords<byte[], byte[]> records = Poll(consumer);

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public void Poll_SetPollException_ThrowsTheSameInstance()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        KafkaException injected = new KafkaException("boom");
        consumer.SetPollException(injected);

        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.Poll(s_pollTimeout));

        Assert.Same(injected, ex);
    }

    [Fact]
    public void Poll_SetPollExceptionNull_ClearsAPendingOne()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.SetPollException(new KafkaException("cleared"));
        consumer.SetPollException(null);

        Assert.Empty(Poll(consumer));
    }

    [Fact]
    public void Poll_SetPollException_IsOneShot_ThenReusable()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.SetPollException(new KafkaException("transient"));

        Assert.Throws<KafkaException>(() => consumer.Poll(s_pollTimeout));

        Assert.Empty(Poll(consumer));
    }

    [Fact]
    public void SetPollException_AClearedOne_IsReleased()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        WeakReference pending = SetAnUnreferencedPollException(consumer);

        Collect();
        Assert.True(pending.IsAlive, "the pending exception must stay alive until it is cleared");

        consumer.SetPollException(null);
        Collect();
        Assert.False(pending.IsAlive, "clearing must release the pending exception");
    }

    [Fact]
    public void SetPollException_APendingOne_IsReleasedWhenTheConsumerIsDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        WeakReference pending = SetAnUnreferencedPollException(consumer);

        TestTimeout.Dispose(consumer);
        Collect();
        Assert.False(pending.IsAlive, "disposing must release the pending exception");
    }

    [Fact]
    public void SetPollException_AConsumedOne_IsReleasedOnceThePollHasThrownIt()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        WeakReference consumed = PollAnUnreferencedPollException(consumer);

        Collect();
        Assert.False(consumed.IsAlive, "the poll that threw the exception must release it");
        Assert.Empty(Poll(consumer));
    }

    [Fact]
    public void SetPollException_AReplacedOne_IsReleased_AndItsReplacementIsHeld()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        WeakReference replaced = SetAnUnreferencedPollException(consumer);
        WeakReference replacement = SetAnUnreferencedPollException(consumer);

        Collect();
        Assert.False(replaced.IsAlive, "replacing must release the replaced exception");
        Assert.True(replacement.IsAlive, "the replacement must stay alive until it is consumed or cleared");

        Assert.Same(replacement.Target, PollFailure(consumer));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SetAnUnreferencedPollException(MockConsumer<byte[], byte[]> consumer)
    {
        KafkaException exception = new KafkaException("pending");
        consumer.SetPollException(exception);
        return new WeakReference(exception);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference PollAnUnreferencedPollException(MockConsumer<byte[], byte[]> consumer)
    {
        WeakReference pending = SetAnUnreferencedPollException(consumer);
        Assert.Same(pending.Target, PollFailure(consumer));
        return pending;
    }

    private static KafkaException PollFailure(IConsumer<byte[], byte[]> consumer)
    {
        AggregateException wrapped = Assert.Throws<AggregateException>(
            () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));
        return Assert.IsType<KafkaException>(Assert.Single(wrapped.InnerExceptions));
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    [Fact]
    public void Seek_ThenPosition_RoundTrips()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });

        consumer.Seek(tp, 42L);

        Assert.Equal(42L, consumer.Position(tp));
    }

    [Fact]
    public void Commit_NoOffsets_BrokerFree_Succeeds()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        consumer.Commit();
    }

    [Fact]
    public void Commit_EmptyOffsets_BrokerFree_Succeeds()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>());
    }

    [Fact]
    public void CommitAsync_FireAndForget_BrokerFree_Succeeds()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        consumer.CommitAsync();
    }

    [Fact]
    public void Subscribe_ThenSubscription_ReflectsTopics()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.Subscribe(new[] { "a", "b" });

        Assert.Equal(new HashSet<string> { "a", "b" }, new HashSet<string>(consumer.Subscription()));
    }

    [Fact]
    public void Unsubscribe_AfterSubscribe_ClearsSubscription()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { "a" });

        consumer.Unsubscribe();

        Assert.Empty(consumer.Subscription());
    }

    [Fact]
    public void Assign_ThenAssignment_ReflectsPartitions()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);

        consumer.Assign(new[] { tp });

        Assert.Contains(tp, consumer.Assignment());
    }

    [Fact]
    public void Assign_Empty_ClearsAssignment()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });

        consumer.Assign(Array.Empty<TopicPartition>());

        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Pause_ThenPaused_ReflectsPartitions_ResumeClears()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });

        consumer.Pause(new[] { tp });
        Assert.Contains(tp, consumer.Paused());

        consumer.Resume(new[] { tp });
        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public void SeekToBeginning_ResetsPositionObservedViaPoll()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });
        consumer.UpdateBeginningOffset(Topic, Partition, 10);

        consumer.SeekToBeginning(new[] { tp });

        // The reset is applied on the next poll.
        Poll(consumer);
        Assert.Equal(10L, consumer.Position(tp));
    }

    [Fact]
    public void SeekToEnd_ResetsPositionObservedViaPoll()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });
        consumer.UpdateEndOffset(Topic, Partition, 77);

        consumer.SeekToEnd(new[] { tp });

        Poll(consumer);
        Assert.Equal(77L, consumer.Position(tp));
    }

    private static ConsumerRecords<byte[], byte[]> Poll(IConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        TestTimeout.Run(() => result = consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
