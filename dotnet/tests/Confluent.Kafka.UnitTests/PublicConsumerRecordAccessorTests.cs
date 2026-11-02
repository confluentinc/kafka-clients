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
/// The <see cref="ConsumerRecord{TKey, TValue}.LeaderEpoch"/>,
/// <see cref="ConsumerRecord{TKey, TValue}.SerializedKeySize"/> and
/// <see cref="ConsumerRecord{TKey, TValue}.SerializedValueSize"/> accessors, through both mock consumers.
/// </summary>
/// <remarks>
/// A mock-added record has both serialized sizes -1, whatever its key and value, and no leader epoch, so the sizes
/// Java reports for a present key or value, and a present epoch, are reachable only against a broker.
/// </remarks>
public sealed class PublicConsumerRecordAccessorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "accessor-topic";
    private const int Partition = 0;

    private static MockConsumer<byte[], byte[]> ReadyToPoll()
    {
        MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    [Fact]
    public void SyncPoll_NullKeyAndValue_SerializedSizesAreMinusOne()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Equal(-1, record.SerializedKeySize);
        Assert.Equal(-1, record.SerializedValueSize);
    }

    [Fact]
    public void SyncPoll_PresentKeyAndValue_MockReportsMinusOne_PositiveSizeIsIntegrationOnly()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, key: new byte[] { 1, 2, 3 }, value: new byte[] { 9, 8, 7, 6, 5 });

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Equal(-1, record.SerializedKeySize);
        Assert.Equal(-1, record.SerializedValueSize);
    }

    [Fact]
    public void SyncPoll_LeaderEpoch_MockAbsent_IsNull()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, key: new byte[] { 7 }, value: new byte[] { 8 });

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Null(record.LeaderEpoch);
    }

    [Fact]
    public async Task AsyncPoll_Accessors_MockReachableContract()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), offset: 0), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 0, key: new byte[] { 1, 2, 3 }, value: null);

            ConsumerRecords<byte[], byte[]> records = default!;
            await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);

            ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
            Assert.Null(record.LeaderEpoch);
            Assert.Equal(-1, record.SerializedKeySize);
            Assert.Equal(-1, record.SerializedValueSize);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static ConsumerRecords<byte[], byte[]> Poll(IConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        TestTimeout.Run(() => result = consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
