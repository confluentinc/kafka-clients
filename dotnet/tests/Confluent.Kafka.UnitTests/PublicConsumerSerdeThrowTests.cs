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
using System.Globalization;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A throwing <see cref="IDeserializer{T}"/>: the sync Poll throws, and the async Poll's Task faults with, a
/// <see cref="RecordDeserializationException"/> carrying Java's message, the origin and the inner exception, and
/// repeated failures leave the consumer usable.
/// </summary>
public sealed class PublicConsumerSerdeThrowTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "serde-throw-topic";
    private const int Partition = 0;

    [Fact]
    public void SyncPoll_ValueDeserializerThrows_ThrowsRecordDeserializationException()
    {
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        MockConsumer<byte[], int> consumer = ReadyToPollSync(Serdes.ByteArray, valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 99, key: new byte[] { 1 }, value: new byte[] { 2, 3 });

        RecordDeserializationException ex = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.IsAssignableFrom<SerializationException>(ex);
        Assert.IsType<ThrowingDeserializer<int>.BoomException>(ex.InnerException);
        Assert.Equal(RecordDeserializationException.DeserializationExceptionOrigin.Value, ex.Origin);
        Assert.Equal(
            "Error deserializing VALUE for partition " + Topic + "-" + Partition.ToString(CultureInfo.InvariantCulture)
                + " at offset 99. If needed, please seek past the record to continue consumption.",
            ex.Message);
        Assert.Equal(99L, consumer.Position(new TopicPartition(Topic, Partition)));
    }

    [Fact]
    public void SyncPoll_KeyDeserializerThrows_ThrowsRecordDeserializationException_ForTheKey()
    {
        ThrowingDeserializer<int> keyDeserializer = new ThrowingDeserializer<int>();
        MockConsumer<int, byte[]> consumer = ReadyToPollSync(keyDeserializer, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 5, key: new byte[] { 1 }, value: new byte[] { 2 });

        RecordDeserializationException ex = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Equal(RecordDeserializationException.DeserializationExceptionOrigin.Key, ex.Origin);
        Assert.Equal(
            "Error deserializing KEY for partition " + Topic + "-" + Partition.ToString(CultureInfo.InvariantCulture)
                + " at offset 5. If needed, please seek past the record to continue consumption.",
            ex.Message);
        Assert.IsType<ThrowingDeserializer<int>.BoomException>(ex.InnerException);
    }

    [Fact]
    public async Task AsyncPoll_ValueDeserializerThrows_FaultsTaskWithRecordDeserializationException()
    {
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        AsyncMockConsumer<byte[], int> consumer =
            new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), offset: 0), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 7, key: new byte[] { 1 }, value: new byte[] { 2, 3, 4 });

            RecordDeserializationException ex = await Assert.ThrowsAsync<RecordDeserializationException>(
                () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));

            Assert.IsType<ThrowingDeserializer<int>.BoomException>(ex.InnerException);
            Assert.Equal(RecordDeserializationException.DeserializationExceptionOrigin.Value, ex.Origin);
            Assert.Equal(
                "Error deserializing VALUE for partition " + Topic + "-" + Partition.ToString(CultureInfo.InvariantCulture)
                    + " at offset 7. If needed, please seek past the record to continue consumption.",
                ex.Message);
            Assert.Equal(7L, await consumer.Position(new TopicPartition(Topic, Partition)));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task AsyncPoll_ThrowingDeserializer_Churned_DoesNotUnwindIntoNative()
    {
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        AsyncMockConsumer<byte[], int> consumer =
            new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), offset: 0), s_deadline);

            for (int i = 0; i < 50; i++)
            {
                consumer.AddRecord(Topic, Partition, offset: i, key: new byte[] { 1 }, value: new byte[] { 2 });
                await Assert.ThrowsAsync<RecordDeserializationException>(
                    () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));
            }
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static MockConsumer<TKey, TValue> ReadyToPollSync<TKey, TValue>(
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
    {
        MockConsumer<TKey, TValue> consumer = new MockConsumer<TKey, TValue>(keyDeserializer, valueDeserializer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }
}
