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
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The round-trip through <see cref="AsyncMockConsumer{TKey, TValue}"/>: every field of a polled record, a non-ASCII
/// topic, key and value, the empty, tombstone and absent sentinels, and the failure path.
/// </summary>
public sealed class PublicConsumerRoundTripTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_settle = TimeSpan.FromSeconds(2);

    private const string Topic = "public-topic";
    private const int Partition = 0;

    private static async Task<AsyncMockConsumer<byte[], byte[]>> ReadyToPoll(string topic = Topic, int partition = Partition)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(
            () => consumer.Assign(new[] { new TopicPartition(topic, partition) }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(new TopicPartition(topic, partition), offset: 0), s_deadline);
        return consumer;
    }

    [Fact]
    public async Task Poll_RoundTripsAllPublicFields()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] key = Encoding.UTF8.GetBytes("key-1");
        byte[] value = Encoding.UTF8.GetBytes("value-1");
        consumer.AddRecord(Topic, Partition, offset: 42, key, value);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

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
    public async Task Poll_NonAsciiTopicAndBytes_RoundTripViaOutLen()
    {
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll(nonAsciiTopic);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] key = Encoding.UTF8.GetBytes("café-key-Ω");
        byte[] value = Encoding.UTF8.GetBytes("naïve-value-🎉");
        consumer.AddRecord(nonAsciiTopic, Partition, offset: 7, key, value);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(nonAsciiTopic, record.Topic);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public async Task Poll_TombstoneAndAbsentKey_MapToNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: null);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Null(record.Key);
        Assert.Null(record.Value);
    }

    [Fact]
    public async Task Poll_EmptyKeyAndValue_MapToEmptyNonNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 2, key: Array.Empty<byte>(), value: Array.Empty<byte>());

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Empty(record.Key!);
        Assert.Empty(record.Value!);
    }

    [Fact]
    public async Task Poll_MultipleRecords_RoundTripInOrder()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        for (int i = 0; i < 5; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
        }

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        Assert.Equal(5, records.Count);
        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, records.Select(r => r.Offset).ToArray());
    }

    [Fact]
    public async Task Poll_Empty_ReturnsNonNullZeroCount()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public async Task Poll_HeaderValueIsByteArray()
    {
        // The mock's AddRecord takes no headers, so only an empty Headers is reachable.
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.NotNull(record.Headers);
        Assert.Empty(record.Headers);
        Header header = new Header("hk", Encoding.UTF8.GetBytes("hv"));
        Assert.Equal("hk", header.Key);
        Assert.Equal(Encoding.UTF8.GetBytes("hv"), header.Value);
        Assert.Null(new Header("null-valued", null).Value);
    }

    [Fact]
    public async Task Poll_SetPollException_FaultsWithTheSameInstance()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        KafkaException injected = new KafkaException("boom");
        consumer.SetPollException(injected);

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer));

        Assert.Same(injected, ex);
    }

    [Fact]
    public async Task Poll_SetPollExceptionNull_ClearsAPendingOne()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.SetPollException(new KafkaException("cleared"));
        consumer.SetPollException(null);

        Assert.Empty(await Poll(consumer));
    }

    [Fact]
    public async Task Poll_SetPollException_IsOneShot_ThenReusable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.SetPollException(new KafkaException("transient"));

        await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer));

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);
        Assert.Empty(records);
    }

    [Fact]
    public async Task Poll_SetPollException_AConsumedOne_IsReleasedOnceThePollHasThrownIt()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        WeakReference consumed = await PollAnUnreferencedPollException(consumer);

        Assert.False(await StaysAlive(consumed), "the poll that threw the exception must release it");
        Assert.Empty(await Poll(consumer));
    }

    [Fact]
    public async Task Poll_RecordsErrorsEmpties_Churned_NoCorruption()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 100; i++)
        {
            switch (i % 3)
            {
                case 0:
                    consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
                    Assert.Single(await Poll(consumer));
                    break;

                case 1:
                    KafkaException injected = new KafkaException($"err-{i}");
                    consumer.SetPollException(injected);
                    Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer)));
                    break;

                default:
                    Assert.Empty(await Poll(consumer));
                    break;
            }
        }
    }

    [Fact]
    public async Task TimestampType_MapsAllThreeValues()
    {
        Assert.Equal(-1, (int)TimestampType.NoTimestampType);
        Assert.Equal(0, (int)TimestampType.CreateTime);
        Assert.Equal(1, (int)TimestampType.LogAppendTime);

        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, null, Encoding.UTF8.GetBytes("v"));
        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
        Assert.True(Enum.IsDefined(typeof(TimestampType), record.TimestampType));
    }

    private static async Task<ConsumerRecords<byte[], byte[]>> Poll(IAsyncConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SetAnUnreferencedPollException(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        KafkaException exception = new KafkaException("pending");
        consumer.SetPollException(exception);
        return new WeakReference(exception);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<WeakReference> PollAnUnreferencedPollException(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        WeakReference pending = SetAnUnreferencedPollException(consumer);
        KafkaException thrown = await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer));
        Assert.Same(pending.Target, thrown);
        return pending;
    }

    private static async Task<bool> StaysAlive(WeakReference reference)
    {
        Stopwatch watch = Stopwatch.StartNew();
        do
        {
            await Task.Yield();
            Collect();
        }
        while (reference.IsAlive && watch.Elapsed < s_settle);

        return reference.IsAlive;
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }
}
