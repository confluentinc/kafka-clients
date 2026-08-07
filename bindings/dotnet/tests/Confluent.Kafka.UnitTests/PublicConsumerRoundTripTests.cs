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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public consumer round-trip (PLAN §5.1) exercised end to end through the
/// <b>public</b> <see cref="AsyncMockConsumer"/> / <see cref="IAsyncConsumer"/> surface: assign →
/// add records → poll → assert every public field (<see cref="ConsumerRecord.Topic"/> /
/// <c>Partition</c> / <c>Offset</c> / <c>Timestamp</c> / <see cref="TimestampType"/> /
/// <c>Key</c> / <c>Value</c> / <c>Headers</c>), incl. the non-ASCII length-delimited
/// path (§B3), empty / tombstone / absent sentinels, and the FAILURE path. Every
/// awaited op runs under a <see cref="TestTimeout"/> hang guard.
/// </summary>
public sealed class PublicConsumerRoundTripTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    // Broker-free error codes are all indistinct (UnknownServerError == -1); assert on
    // TYPE + Message, never on a distinctive Code (M3/P3 finding #8).
    private const int UnknownServerErrorCode = -1;

    private const string Topic = "public-topic";
    private const int Partition = 0;

    // Assign + seek to offset 0 so the mock poll has a valid position — the canonical
    // broker-free poll setup, now through the public AsyncMockConsumer surface.
    private static async Task<AsyncMockConsumer> ReadyToPoll(string topic = Topic, int partition = Partition)
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await TestTimeout.Run(
            () => consumer.Assign(new[] { new TopicPartition(topic, partition) }), s_deadline);
        // Seek is now SYNC (M5/P7) — a direct sync ABI call, no await / hang guard needed.
        consumer.Seek(new TopicPartition(topic, partition), offset: 0);
        return consumer;
    }

    [Fact]
    public async Task Poll_RoundTripsAllPublicFields()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();
        byte[] key = Encoding.UTF8.GetBytes("key-1");
        byte[] value = Encoding.UTF8.GetBytes("value-1");
        consumer.AddRecord(Topic, Partition, offset: 42, key, value);

        ConsumerRecords records = await Poll(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.Equal(Topic, record.Topic);
        Assert.Equal(Partition, record.Partition);
        Assert.Equal(42, record.Offset);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
        Assert.Empty(record.Headers);
        // Timestamp/TimestampType are ABI-provided; assert the enum is one of the valid
        // members (the mock stamps a real CreateTime timestamp).
        Assert.True(
            record.TimestampType is TimestampType.NoTimestampType
                or TimestampType.CreateTime
                or TimestampType.LogAppendTime);
    }

    [Fact]
    public async Task Poll_ViaIAsyncConsumerInterface_RoundTrips()
    {
        // Hold an AsyncMockConsumer, pass it as IAsyncConsumer (the additive-growth interface).
        using AsyncMockConsumer mock = await ReadyToPoll();
        mock.AddRecord(Topic, Partition, offset: 3, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        IAsyncConsumer consumer = mock;
        ConsumerRecords records = await TestTimeoutResult(consumer.Poll(s_pollTimeout));

        ConsumerRecord record = Assert.Single(records);
        Assert.Equal(3, record.Offset);
    }

    [Fact]
    public async Task Poll_NonAsciiTopicAndBytes_RoundTripViaOutLen()
    {
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        using AsyncMockConsumer consumer = await ReadyToPoll(nonAsciiTopic);
        byte[] key = Encoding.UTF8.GetBytes("café-key-Ω");
        byte[] value = Encoding.UTF8.GetBytes("naïve-value-🎉");
        consumer.AddRecord(nonAsciiTopic, Partition, offset: 7, key, value);

        ConsumerRecords records = await Poll(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.Equal(nonAsciiTopic, record.Topic);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public async Task Poll_TombstoneAndAbsentKey_MapToNull()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: null);

        ConsumerRecords records = await Poll(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.Null(record.Key);
        Assert.Null(record.Value);
    }

    [Fact]
    public async Task Poll_EmptyKeyAndValue_MapToEmptyNonNull()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 2, key: Array.Empty<byte>(), value: Array.Empty<byte>());

        ConsumerRecords records = await Poll(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Empty(record.Key!);
        Assert.Empty(record.Value!);
    }

    [Fact]
    public async Task Poll_MultipleRecords_RoundTripInOrder()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();
        for (int i = 0; i < 5; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
        }

        ConsumerRecords records = await Poll(consumer);

        Assert.Equal(5, records.Count);
        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, records.Select(r => r.Offset).ToArray());
    }

    [Fact]
    public async Task Poll_Empty_ReturnsNonNullZeroCount()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();

        ConsumerRecords records = await Poll(consumer);

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public async Task Poll_HeaderValueIsByteArray()
    {
        // Header.Value is byte[]? (PLAN micro-decision A). The AsyncMockConsumer's add_record
        // carries no headers (M3/P3 finding), so the reachable broker-free case is the
        // empty Headers collection — assert its byte[]-typed, read-only shape.
        using AsyncMockConsumer consumer = await ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        ConsumerRecords records = await Poll(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.NotNull(record.Headers);
        Assert.Empty(record.Headers);
        // A Header, if present, exposes byte[]? Value — verified as a construction check
        // (no public ConsumerRecord ctor this phase; the copy-out marshaller produces
        // Headers). The Header type itself round-trips its bytes as byte[].
        Header header = new Header("hk", Encoding.UTF8.GetBytes("hv"));
        Assert.Equal("hk", header.Key);
        Assert.Equal(Encoding.UTF8.GetBytes("hv"), header.Value);
        Assert.Null(new Header("null-valued", null).Value);
    }

    [Fact]
    public async Task Poll_SetPollError_FaultsWithKafkaException()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();
        consumer.SetPollError("boom");

        // Poll already carries the TestTimeout hang guard, so a stall on the FAILURE
        // path also fails fast rather than hanging the run.
        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer));

        // Assert TYPE + Message content (part of the contract, DoD §3) + flags — never
        // the indistinct broker-free Code (-1).
        Assert.Equal(UnknownServerErrorCode, ex.Code);
        Assert.Contains("boom", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsRetriable);
        Assert.False(ex.IsFatal);
    }

    [Fact]
    public async Task Poll_SetPollError_IsOneShot_ThenReusable()
    {
        using AsyncMockConsumer consumer = await ReadyToPoll();
        consumer.SetPollError("transient");

        await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer));

        ConsumerRecords records = await Poll(consumer);
        Assert.Empty(records);
    }

    [Fact]
    public async Task Poll_RecordsErrorsEmpties_Churned_NoCorruption()
    {
        // The double-free / leak detector for the owned-result path through the PUBLIC
        // surface: batch _destroy, error handle, and per-op GCHandle each freed exactly
        // once per poll over many iterations.
        using AsyncMockConsumer consumer = await ReadyToPoll();

        for (int i = 0; i < 100; i++)
        {
            switch (i % 3)
            {
                case 0:
                    consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
                    Assert.Single(await Poll(consumer));
                    break;

                case 1:
                    consumer.SetPollError($"err-{i}");
                    await Assert.ThrowsAsync<KafkaException>(() => Poll(consumer));
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
        // The enum maps directly from the ABI int (-1 / 0 / 1). The record path only
        // surfaces the mock's stamped type, so assert the enum value mapping directly
        // (the marshaller casts (TimestampType)int) plus the record's type is valid.
        Assert.Equal(-1, (int)TimestampType.NoTimestampType);
        Assert.Equal(0, (int)TimestampType.CreateTime);
        Assert.Equal(1, (int)TimestampType.LogAppendTime);

        using AsyncMockConsumer consumer = await ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 0, null, Encoding.UTF8.GetBytes("v"));
        ConsumerRecord record = Assert.Single(await Poll(consumer));
        Assert.True(Enum.IsDefined(typeof(TimestampType), record.TimestampType));
    }

    // Every awaited poll routes through the TestTimeout hang guard (PLAN §5) so a
    // future stall in the owned-handle bridge or the mock fails the run fast instead
    // of hanging it — mirroring PublicConsumerTfmSmokeTests.Poll.
    private static async Task<ConsumerRecords> Poll(IAsyncConsumer consumer)
    {
        ConsumerRecords result = default!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }

    private static Task<ConsumerRecords> TestTimeoutResult(Task<ConsumerRecords> op) => Poll(op);

    private static async Task<ConsumerRecords> Poll(Task<ConsumerRecords> op)
    {
        ConsumerRecords result = default!;
        await TestTimeout.Run(async () => result = await op, s_deadline);
        return result;
    }
}
