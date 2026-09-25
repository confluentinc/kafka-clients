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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M6/P1b — which thread the deserialize runs on (PLAN §5). The <b>sync</b> typed poll
/// deserializes on the <b>caller's</b> thread (the copy-out is inline in
/// <c>Consumer_poll</c>'s return path); the <b>async</b> typed poll deserializes on the
/// core's <b>foreign dispatcher thread</b>, inside the poll completion callback, before the
/// awaiter's continuation resumes (which itself runs off-thread via
/// <c>RunContinuationsAsynchronously</c>). This is the documented headline consequence of
/// the zero-copy deserialize-before-destroy design.
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
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll(valueDeserializer);
        consumer.AddRecord(Topic, Partition, offset: 1, key: new byte[] { 1 }, value: new byte[] { 2 });

        int callerThreadId = Environment.CurrentManagedThreadId;

        // The mock poll + sync copy-out run synchronously on THIS thread (the mock poll never
        // blocks with a record queued), so no TestTimeout wrapper — and crucially the poll must
        // run on the caller thread for this assertion, which Task.Run would defeat.
        ConsumerRecord<byte[], byte[]> record = Assert.Single(consumer.Poll(s_pollTimeout));

        Assert.Equal(new byte[] { 2 }, record.Value);
        Assert.Equal(1, valueDeserializer.InvocationCount);
        Assert.Equal(callerThreadId, valueDeserializer.LastThreadId); // caller thread (sync)
    }

    [Fact]
    public async Task AsyncPoll_DeserializesOnTheDispatcherThread_NotTheCaller()
    {
        RecordingDeserializer<byte[]> valueDeserializer = new RecordingDeserializer<byte[]>(Serdes.ByteArray);
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
            consumer.AddRecord(Topic, Partition, offset: 2, key: new byte[] { 1 }, value: new byte[] { 2 });

            int callerThreadId = Environment.CurrentManagedThreadId;

            ConsumerRecords<byte[], byte[]> records = default!;
            await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);

            Assert.Single(records);
            Assert.Equal(1, valueDeserializer.InvocationCount);
            // The deserialize ran inside the poll callback on the core's foreign dispatcher
            // thread — NOT the caller's thread (PLAN §5). The await barrier publishes the
            // dispatcher-thread write of LastThreadId.
            Assert.NotEqual(callerThreadId, valueDeserializer.LastThreadId);
        }
        finally
        {
            await consumer.DisposeAsync();
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
