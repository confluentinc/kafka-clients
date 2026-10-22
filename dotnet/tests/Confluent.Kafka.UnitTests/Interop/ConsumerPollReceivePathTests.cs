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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="NativeConsumer"/>'s PollWithCallback on the mock: the record round-trip (a non-ASCII topic, key and
/// value; absent and empty keys and values), the empty, failure and churn paths, survival under GC, and records that
/// stay valid after their batch is destroyed.
/// </summary>
public sealed class ConsumerPollReceivePathTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "poll-topic";
    private const int Partition = 0;

    private static Task<NativeConsumer> MockReadyToPoll(string topic = Topic, int partition = Partition)
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Assign(new[] { (topic, partition) });
        consumer.Seek(topic, partition, offset: 0);
        return Task.FromResult(consumer);
    }

    [Fact]
    public async Task PollWithCallback_RoundTripsAllFields()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] key = Encoding.UTF8.GetBytes("key-1");
        byte[] value = Encoding.UTF8.GetBytes("value-1");
        consumer.AddRecord(Topic, Partition, offset: 42, key, value);

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(Topic, record.Topic);
        Assert.Equal(Partition, record.Partition);
        Assert.Equal(42, record.Offset);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
        Assert.Empty(record.Headers);
    }

    [Fact]
    public async Task PollWithCallback_NonAsciiTopicAndBytes_RoundTripViaOutLen()
    {
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        NativeConsumer consumer = await MockReadyToPoll(nonAsciiTopic);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] key = Encoding.UTF8.GetBytes("café-key-Ω");
        byte[] value = Encoding.UTF8.GetBytes("naïve-value-🎉");
        consumer.AddRecord(nonAsciiTopic, Partition, offset: 7, key, value);

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(nonAsciiTopic, record.Topic);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public async Task PollWithCallback_TombstoneAndAbsentKey_MapToNull()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: null);

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Null(record.Key);
        Assert.Null(record.Value);
    }

    [Fact]
    public async Task PollWithCallback_EmptyKeyAndValue_MapToEmptyNonNull()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 2, key: Array.Empty<byte>(), value: Array.Empty<byte>());

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Empty(record.Key!);
        Assert.Empty(record.Value!);
    }

    [Fact]
    public async Task PollWithCallback_MultipleRecords_RoundTripInOrder()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        for (int i = 0; i < 5; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
        }

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);

        Assert.Equal(5, records.Count);
        long[] offsets = records.Select(r => r.Offset).ToArray();
        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, offsets);
    }

    [Fact]
    public async Task PollWithCallback_Empty_ReturnsNonNullZeroCount()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public async Task PollWithCallback_SetPollException_FaultsWithTheSameInstance()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        KafkaException injected = new KafkaException("boom");
        consumer.SetPollException(injected);

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => PollWithCallback(consumer), s_deadline));

        Assert.Same(injected, ex);
    }

    [Fact]
    public async Task PollWithCallback_SetPollExceptionNull_ClearsAPendingOne()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.SetPollException(new KafkaException("cleared"));
        consumer.SetPollException(null);

        Assert.Empty(await PollWithCallback(consumer));
    }

    [Fact]
    public async Task PollWithCallback_SetPollException_IsOneShot_ThenReusable()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.SetPollException(new KafkaException("transient"));

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => PollWithCallback(consumer), s_deadline));

        // As Java's setPollException, the error is consumed by the first poll.
        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);
        Assert.Empty(records);
    }

    [Fact]
    public async Task PollWithCallback_RecordsAndErrorsAndEmpties_Churned_NoCorruption()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 100; i++)
        {
            switch (i % 3)
            {
                case 0:
                    consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
                    ConsumerRecords<byte[], byte[]> ok = await PollWithCallback(consumer);
                    Assert.Single(ok);
                    break;

                case 1:
                    KafkaException injected = new KafkaException($"err-{i}");
                    consumer.SetPollException(injected);
                    Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(
                        () => TestTimeout.Run(() => PollWithCallback(consumer), s_deadline)));
                    break;

                default:
                    ConsumerRecords<byte[], byte[]> empty = await PollWithCallback(consumer);
                    Assert.Empty(empty);
                    break;
            }
        }
    }

    [Fact]
    public async Task PollWithCallback_InFlight_SurvivesAggressiveGc()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 50; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
            Task<ConsumerRecords<byte[], byte[]>> op = consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray, Serdes.ByteArray);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            ConsumerRecords<byte[], byte[]> records = await TestTimeoutResult(op);
            Assert.Single(records);
        }
    }

    [Fact]
    public async Task PollWithCallback_ResultSurvivesBeyondBatchDestroy()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] value = Encoding.UTF8.GetBytes("durable-value");
        consumer.AddRecord(Topic, Partition, offset: 99, key: null, value);

        ConsumerRecords<byte[], byte[]> records = await PollWithCallback(consumer);
        GC.Collect();
        GC.WaitForPendingFinalizers();

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(value, record.Value);
        Assert.Equal(Topic, record.Topic);
    }

    private static Task<ConsumerRecords<byte[], byte[]>> PollWithCallback(NativeConsumer consumer) =>
        consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray, Serdes.ByteArray);

    private static async Task<ConsumerRecords<byte[], byte[]>> TestTimeoutResult(Task<ConsumerRecords<byte[], byte[]>> op)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        await TestTimeout.Run(async () => result = await op, s_deadline);
        return result;
    }
}
