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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M6/P1b typed poll — the zero-copy typed receive path through the public generic
/// consumers (PLAN §5/§9). Covers the typed round-trip on <c>&lt;string, long&gt;</c> (sync
/// <see cref="MockConsumer{TKey, TValue}"/> + async <see cref="AsyncMockConsumer{TKey, TValue}"/>),
/// the <c>&lt;byte[], byte[]&gt;</c> identity equivalence, and the <b>three-state null
/// model</b> (PLAN decision C): an absent field returns <c>default(T)</c> <b>without</b>
/// invoking the deserializer; a present-but-empty field deserializes a zero-length span; a
/// present field deserializes the span. <c>long?</c> distinguishes a tombstone from a real
/// <c>0</c>.
/// </summary>
public sealed class PublicConsumerTypedPollTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "typed-topic";
    private const int Partition = 0;

    // Assign + seek to offset 0 so the mock poll has a valid position — the canonical
    // broker-free poll setup, parameterized over the two deserializers.
    private static MockConsumer<TKey, TValue> ReadyToPoll<TKey, TValue>(
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
    {
        MockConsumer<TKey, TValue> consumer = new MockConsumer<TKey, TValue>(keyDeserializer, valueDeserializer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    // ---- Typed round-trip: <string, long> ----

    [Fact]
    public void SyncPoll_StringLong_RoundTripsDecodedKeyAndValue()
    {
        using MockConsumer<string, long> consumer = ReadyToPoll(Serdes.String, Serdes.Int64);
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
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
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
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public void SyncPoll_ByteArrayIdentity_EquivalentToRawBytes()
    {
        // <byte[], byte[]> via Serdes.ByteArray is the identity serde — the decoded Key/Value
        // are byte[]? equal to the raw bytes queued (equivalence to the former bytes surface).
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll(Serdes.ByteArray, Serdes.ByteArray);
        byte[] key = { 1, 2, 3 };
        byte[] value = { 9, 8, 7, 6 };
        consumer.AddRecord(Topic, Partition, offset: 13, key, value);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    // ---- Three-state null model (PLAN decision C) ----

    [Fact]
    public void AbsentValue_ReturnsDefault_DeserializerNotInvoked()
    {
        // len < 0 (a tombstone value): default(int) == 0 AND the deserializer is never called.
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        using MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: new byte[] { 7 }, value: null);

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(0, record.Value);                 // default(int)
        Assert.Equal(0, valueDeserializer.InvocationCount); // NOT invoked (the crux of decision C)
    }

    [Fact]
    public void AbsentValue_DoesNotInvoke_EvenAThrowingDeserializer()
    {
        // The strongest form of "not invoked for absent": a deserializer that would throw if
        // called leaves the poll succeeding, because an absent value short-circuits to default.
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        using MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        consumer.AddRecord(Topic, Partition, offset: 2, key: new byte[] { 7 }, value: null);

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(0, record.Value);
        Assert.Equal(0, valueDeserializer.InvocationCount);
    }

    [Fact]
    public void PresentEmptyValue_DeserializesZeroLengthSpan()
    {
        // len == 0 with a non-null pointer: the deserializer IS invoked, with a zero-length
        // span — distinct from absent (where it is not invoked at all).
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        using MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        consumer.AddRecord(Topic, Partition, offset: 3, key: new byte[] { 7 }, value: Array.Empty<byte>());

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(1, valueDeserializer.InvocationCount); // invoked (unlike absent)
        Assert.Equal(0, valueDeserializer.LastSpanLength);  // with a 0-length span
        Assert.Equal(0, record.Value);
    }

    [Fact]
    public void PresentValue_DeserializesSpanOfItsLength()
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        using MockConsumer<byte[], int> consumer = ReadyToPoll(Serdes.ByteArray, valueDeserializer);
        consumer.AddRecord(Topic, Partition, offset: 4, key: new byte[] { 7 }, value: new byte[] { 1, 2, 3 });

        ConsumerRecord<byte[], int> record = Assert.Single(Poll(consumer));

        Assert.Equal(1, valueDeserializer.InvocationCount);
        Assert.Equal(3, valueDeserializer.LastSpanLength);
        Assert.Equal(3, record.Value);
    }

    [Fact]
    public void NullableLong_DistinguishesTombstoneFromZero()
    {
        // A tombstone (absent value) and a present 0L are indistinguishable with a non-nullable
        // long (both surface as 0); long? separates them: absent → null (deserializer not
        // called), present 8-byte zero → 0L (deserializer called).
        NullableInt64Deserializer tombstone = new NullableInt64Deserializer();
        using (MockConsumer<byte[], long?> consumer = ReadyToPoll(Serdes.ByteArray, tombstone))
        {
            consumer.AddRecord(Topic, Partition, offset: 5, key: new byte[] { 7 }, value: null);
            ConsumerRecord<byte[], long?> record = Assert.Single(Poll(consumer));
            Assert.Null(record.Value);                 // tombstone
            Assert.Equal(0, tombstone.InvocationCount); // not called
        }

        NullableInt64Deserializer zero = new NullableInt64Deserializer();
        using (MockConsumer<byte[], long?> consumer = ReadyToPoll(Serdes.ByteArray, zero))
        {
            consumer.AddRecord(Topic, Partition, offset: 6, key: new byte[] { 7 }, value: Serdes.Int64.Serialize(Topic, 0L)!);
            ConsumerRecord<byte[], long?> record = Assert.Single(Poll(consumer));
            Assert.Equal(0L, record.Value);            // a genuine 0, not a tombstone
            Assert.Equal(1, zero.InvocationCount);     // called
        }
    }

    // Every blocking Poll routes through the TestTimeout hang guard (PLAN §8).
    private static ConsumerRecords<TKey, TValue> Poll<TKey, TValue>(IConsumer<TKey, TValue> consumer)
    {
        ConsumerRecords<TKey, TValue> result = default!;
        TestTimeout.Run(() => result = consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
