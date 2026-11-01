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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The typed poll through both mock consumers: a <c>&lt;string, long&gt;</c> round-trip, the
/// <c>&lt;byte[], byte[]&gt;</c> identity, and the three-state null model: an absent field is <c>default(T)</c> and the
/// deserializer is not called; an empty field deserializes a zero-length span; a present one, its bytes.
/// </summary>
/// <remarks>
/// <c>long?</c> tells a tombstone (null) from a present zero (0).
/// </remarks>
public sealed class PublicConsumerTypedPollTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "typed-topic";
    private const int Partition = 0;

    private static MockConsumer<TKey, TValue> ReadyToPoll<TKey, TValue>(
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
    {
        MockConsumer<TKey, TValue> consumer = new MockConsumer<TKey, TValue>(keyDeserializer, valueDeserializer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    [Fact]
    public void SyncPoll_StringLong_RoundTripsDecodedKeyAndValue()
    {
        MockConsumer<string, long> consumer = ReadyToPoll(Serdes.String, Serdes.Int64);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] key = Serdes.String.Serialize(Topic, "hello")!;
        byte[] value = Serdes.Int64.Serialize(Topic, 1_234_567_890_123L)!;
        consumer.AddRecord(Topic, Partition, offset: 11, key, value);

        ConsumerRecord<string, long> record = Assert.Single(Poll(consumer));

        Assert.Equal("hello", record.Key);
        Assert.Equal(1_234_567_890_123L, record.Value);
        Assert.Equal(11, record.Offset);
    }

    [Fact]
    public async Task AsyncPoll_StringLong_RoundTripsDecodedKeyAndValue()
    {
        AsyncMockConsumer<string, long> consumer =
            new AsyncMockConsumer<string, long>(Serdes.String, Serdes.Int64);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), offset: 0), s_deadline);
            consumer.AddRecord(
                Topic, Partition, offset: 12, Serdes.String.Serialize(Topic, "wörld-Ω")!, Serdes.Int64.Serialize(Topic, -7L)!);

            ConsumerRecords<string, long> records = default!;
            await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);

            ConsumerRecord<string, long> record = Assert.Single(records);
            Assert.Equal("wörld-Ω", record.Key);
            Assert.Equal(-7L, record.Value);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public void SyncPoll_ByteArrayIdentity_EquivalentToRawBytes()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        byte[] key = { 1, 2, 3 };
        byte[] value = { 9, 8, 7, 6 };
        consumer.AddRecord(Topic, Partition, offset: 13, key, value);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public void AbsentValue_ReturnsDefault_DeserializerNotInvoked()
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: new byte[] { 7 }, value: null);

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(0, record.Value);
        Assert.Equal(0, valueDeserializer.InvocationCount);
    }

    [Fact]
    public void AbsentValue_DoesNotInvoke_EvenAThrowingDeserializer()
    {
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 2, key: new byte[] { 7 }, value: null);

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(0, record.Value);
        Assert.Equal(0, valueDeserializer.InvocationCount);
    }

    [Fact]
    public void PresentEmptyValue_DeserializesZeroLengthSpan()
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 3, key: new byte[] { 7 }, value: Array.Empty<byte>());

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(1, valueDeserializer.InvocationCount);
        Assert.Equal(0, valueDeserializer.LastSpanLength);
        Assert.Equal(0, record.Value);
    }

    [Fact]
    public void PresentValue_DeserializesSpanOfItsLength()
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 4, key: new byte[] { 7 }, value: new byte[] { 1, 2, 3 });

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(1, valueDeserializer.InvocationCount);
        Assert.Equal(3, valueDeserializer.LastSpanLength);
        Assert.Equal(3, record.Value);
    }

    [Fact]
    public void NullableLong_DistinguishesTombstoneFromZero()
    {
        NullableInt64Deserializer tombstone = new NullableInt64Deserializer();
        {
            MockConsumer<byte[], long?> consumer = ReadyToPoll(Serdes.ByteArray, tombstone);
            using (TestTimeout.Disposal(consumer))
            {
                consumer.AddRecord(Topic, Partition, offset: 5, key: new byte[] { 7 }, value: null);
                ConsumerRecord<byte[], long?> record = Assert.Single(Poll(consumer));
                Assert.Null(record.Value);
                Assert.Equal(0, tombstone.InvocationCount);
            }
        }

        NullableInt64Deserializer zero = new NullableInt64Deserializer();
        {
            MockConsumer<byte[], long?> consumer = ReadyToPoll(Serdes.ByteArray, zero);
            using (TestTimeout.Disposal(consumer))
            {
                consumer.AddRecord(Topic, Partition, offset: 6, key: new byte[] { 7 }, value: Serdes.Int64.Serialize(Topic, 0L)!);
                ConsumerRecord<byte[], long?> record = Assert.Single(Poll(consumer));
                Assert.Equal(0L, record.Value);
                Assert.Equal(1, zero.InvocationCount);
            }
        }
    }

    private static ConsumerRecords<TKey, TValue> Poll<TKey, TValue>(IConsumer<TKey, TValue> consumer)
    {
        ConsumerRecords<TKey, TValue> result = default!;
        TestTimeout.Run(() => result = consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
