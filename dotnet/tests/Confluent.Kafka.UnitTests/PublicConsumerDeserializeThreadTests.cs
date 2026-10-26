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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Where a typed poll deserializes: the sync Poll on the calling thread, the async Poll off the drain thread.
/// </summary>
public sealed class PublicConsumerDeserializeThreadTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "deser-thread-topic";
    private const int Partition = 0;

    [Fact]
    public void SyncPoll_DeserializesOnTheCallerThread()
    {
        RecordingDeserializer<byte[]> valueDeserializer = new RecordingDeserializer<byte[]>(Serdes.ByteArray);
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll(valueDeserializer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: new byte[] { 1 }, value: new byte[] { 2 });

        int callerThreadId = Environment.CurrentManagedThreadId;

        // Not wrapped in TestTimeout: the poll must run on this thread.
        ConsumerRecord<byte[], byte[]> record = Assert.Single(consumer.Poll(s_pollTimeout));

        Assert.Equal(new byte[] { 2 }, record.Value);
        Assert.Equal(1, valueDeserializer.InvocationCount);
        Assert.Equal(callerThreadId, valueDeserializer.LastThreadId);
    }

    [Fact]
    public async Task AsyncPoll_DeserializesOffThePumpThread()
    {
        RecordingDeserializer<byte[]> valueDeserializer = new RecordingDeserializer<byte[]>(Serdes.ByteArray);
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), offset: 0), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 2, key: new byte[] { 1 }, value: new byte[] { 2 });

            Task<ConsumerRecords<byte[], byte[]>> poll;
            using (new ConsumerPumpStall(consumer))
            {
                poll = consumer.Poll(s_pollTimeout);
            }

            ConsumerRecords<byte[], byte[]> records = default!;
            await TestTimeout.Run(async () => records = await poll, s_deadline);

            Assert.Single(records);
            Assert.Equal(1, valueDeserializer.InvocationCount);
            Assert.NotEqual(NativeConsumer.PumpThreadName, valueDeserializer.LastThreadName);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static MockConsumer<byte[], byte[]> ReadyToPoll(IDeserializer<byte[]> valueDeserializer)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, valueDeserializer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }
}
