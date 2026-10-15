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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M6/P1b Decision E — the <b>mandatory</b> <see cref="SerializationException"/> wrap (PLAN
/// §6). A throw from a user <see cref="IDeserializer{T}"/> is caught by the typed-poll
/// marshaller and wrapped in a <see cref="SerializationException"/> (inner exception + topic
/// / partition / offset), surfacing as a synchronous throw on the <b>sync</b> path and a
/// faulted <see cref="Task"/> on the <b>async</b> path. The async wrap is mandatory (not
/// optional): the deserialize runs inside the poll completion callback on the core's
/// <b>foreign dispatcher thread</b>, where an escaping managed exception is UB — so the
/// churn test (a throwing deserializer polled repeatedly) doubles as the "does not unwind
/// into native" crash regression.
/// </summary>
public sealed class PublicConsumerSerdeThrowTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "serde-throw-topic";
    private const int Partition = 0;

    [Fact]
    public void SyncPoll_ValueDeserializerThrows_ThrowsSerializationExceptionWithContext()
    {
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        using MockConsumer<byte[], int> consumer = ReadyToPollSync(Serdes.ByteArray, valueDeserializer);
        consumer.AddRecord(Topic, Partition, offset: 99, key: new byte[] { 1 }, value: new byte[] { 2, 3 });

        // The mock poll + copy-out are synchronous on the caller thread, so the throw is
        // synchronous — call Poll directly (no TestTimeout hang guard needed; the mock poll
        // never blocks), mirroring the shipped sync SetPollError precedent.
        SerializationException ex = Assert.Throws<SerializationException>(() => consumer.Poll(s_pollTimeout));

        // Catchable as the flat base (Java parity, ffi §A5).
        Assert.IsAssignableFrom<KafkaException>(ex);
        // Inner is the exact user throw.
        Assert.IsType<ThrowingDeserializer<int>.BoomException>(ex.InnerException);
        // Message carries topic / value-vs-key / partition / offset context.
        Assert.Contains(Topic, ex.Message, StringComparison.Ordinal);
        Assert.Contains("value", ex.Message, StringComparison.Ordinal);
        Assert.Contains(Partition.ToString(CultureInfo.InvariantCulture), ex.Message, StringComparison.Ordinal);
        Assert.Contains("99", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SyncPoll_KeyDeserializerThrows_MessageSaysKey()
    {
        ThrowingDeserializer<int> keyDeserializer = new ThrowingDeserializer<int>();
        using MockConsumer<int, byte[]> consumer = ReadyToPollSync(keyDeserializer, Serdes.ByteArray);
        consumer.AddRecord(Topic, Partition, offset: 5, key: new byte[] { 1 }, value: new byte[] { 2 });

        SerializationException ex = Assert.Throws<SerializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Contains("key", ex.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingDeserializer<int>.BoomException>(ex.InnerException);
    }

    [Fact]
    public async Task AsyncPoll_ValueDeserializerThrows_FaultsTaskWithSerializationException()
    {
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        AsyncMockConsumer<byte[], int> consumer =
            new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
            consumer.AddRecord(Topic, Partition, offset: 7, key: new byte[] { 1 }, value: new byte[] { 2, 3, 4 });

            // The deserialize runs on the FOREIGN dispatcher thread inside the poll callback;
            // the wrap-and-fault (not an unwind into native) is what surfaces here.
            SerializationException ex = await Assert.ThrowsAsync<SerializationException>(
                () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));

            Assert.IsType<ThrowingDeserializer<int>.BoomException>(ex.InnerException);
            Assert.Contains("offset 7", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public async Task AsyncPoll_ThrowingDeserializer_Churned_DoesNotUnwindIntoNative()
    {
        // The dispatcher-thread safety regression: poll a throwing deserializer many times.
        // If the wrap ever let a managed exception escape into the native dispatcher frame,
        // the process would crash here rather than faulting each Task cleanly.
        ThrowingDeserializer<int> valueDeserializer = new ThrowingDeserializer<int>();
        AsyncMockConsumer<byte[], int> consumer =
            new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);

            for (int i = 0; i < 50; i++)
            {
                consumer.AddRecord(Topic, Partition, offset: i, key: new byte[] { 1 }, value: new byte[] { 2 });
                await Assert.ThrowsAsync<SerializationException>(
                    () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));
            }
        }
        finally
        {
            await consumer.DisposeAsync();
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
