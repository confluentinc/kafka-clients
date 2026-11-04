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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke tests: round-trips through the mock consumers' public surface, with netstandard2.0-safe APIs
/// only so they compile on net462.
/// </summary>
public sealed class PublicConsumerTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "tfm-smoke-topic";
    private const int Partition = 0;

    [Fact]
    public async Task MockConsumer_AssignAddPollClose_RoundTrips()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), 0L), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 5, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

            ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

            ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
            Assert.Equal(Topic, record.Topic);
            Assert.Equal(5, record.Offset);
            Assert.Equal(Encoding.UTF8.GetBytes("k"), record.Key);
            Assert.Equal(Encoding.UTF8.GetBytes("v"), record.Value);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public async Task MockConsumer_SubscribeUnsubscribeClose_RoundTrips()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }), s_deadline);
            await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public async Task MockConsumer_OffsetMapQueries_MarshalOnTheTfmMatrix()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            consumer.UpdateBeginningOffset(Topic, Partition, 5);
            consumer.UpdateEndOffset(Topic, Partition, 99);
            TopicPartition[] request = { new TopicPartition(Topic, Partition) };

            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, long> begin = default!;
            await TestTimeout.Run(async () => begin = await consumer.BeginningOffsets(request), s_deadline);
            Assert.Equal(5, begin[new TopicPartition(Topic, Partition)]);

            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, long> end = default!;
            await TestTimeout.Run(async () => end = await consumer.EndOffsets(request), s_deadline);
            Assert.Equal(99, end[new TopicPartition(Topic, Partition)]);

            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed = default!;
            await TestTimeout.Run(async () => committed = await consumer.Committed(request), s_deadline);
            Assert.Empty(committed);

            await Assert.ThrowsAsync<NotSupportedException>(() => consumer.OffsetsForTimes(
                new System.Collections.Generic.Dictionary<TopicPartition, long>
                {
                    [new TopicPartition(Topic, Partition)] = 1_000L,
                }));
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public async Task MockConsumer_PartitionMetadataQueries_MarshalOnTheTfmMatrix()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            consumer.UpdatePartitions(Topic, partitionCount: 1, leaderId: 7, "broker-1", leaderPort: 9092);

            System.Collections.Generic.IReadOnlyList<PartitionInfo> partitions = default!;
            await TestTimeout.Run(async () => partitions = await consumer.PartitionsFor(Topic), s_deadline);
            PartitionInfo info = Assert.Single(partitions);
            Assert.Equal(Topic, info.Topic);
            Assert.Equal(7, info.Leader!.Id);
            Assert.Equal("broker-1", info.Leader.Host);

            System.Collections.Generic.IReadOnlyDictionary<string, System.Collections.Generic.IReadOnlyList<PartitionInfo>> map = default!;
            await TestTimeout.Run(async () => map = await consumer.ListTopics(), s_deadline);
            Assert.True(map.ContainsKey(Topic));
            Assert.Single(map[Topic]);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public void SyncMockConsumer_AssignAddPollClose_RoundTrips()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
            consumer.Seek(new TopicPartition(Topic, Partition), 0L);
            consumer.AddRecord(Topic, Partition, offset: 5, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

            ConsumerRecords<byte[], byte[]> records = default!;
            TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);

            ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
            Assert.Equal(Topic, record.Topic);
            Assert.Equal(5, record.Offset);
            Assert.Equal(Encoding.UTF8.GetBytes("k"), record.Key);
            Assert.Equal(Encoding.UTF8.GetBytes("v"), record.Value);

            Assert.Equal(6L, consumer.Position(new TopicPartition(Topic, Partition)));
        }
        finally
        {
            TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public void SyncMockConsumer_QueryFamily_MarshalsOnTheTfmMatrix()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            TopicPartition tp = new TopicPartition(Topic, Partition);
            consumer.Assign(new[] { tp });
            consumer.Commit(new System.Collections.Generic.Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [tp] = new OffsetAndMetadata(42, "meta-x", 7),
            });

            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                consumer.Committed(new[] { tp });
            Assert.Equal(42, committed[tp]!.Offset);
            Assert.Equal("meta-x", committed[tp]!.Metadata);
            Assert.Equal(7, committed[tp]!.LeaderEpoch);

            consumer.UpdateBeginningOffset(Topic, Partition, 5);
            consumer.UpdateEndOffset(Topic, Partition, 99);
            Assert.Equal(5, consumer.BeginningOffsets(new[] { tp })[tp]);
            Assert.Equal(99, consumer.EndOffsets(new[] { tp })[tp]);

            consumer.UpdatePartitions(Topic, partitionCount: 1, leaderId: 7, "broker-1", leaderPort: 9092);
            PartitionInfo info = Assert.Single(consumer.PartitionsFor(Topic));
            Assert.Equal(Topic, info.Topic);
            Assert.True(consumer.ListTopics().ContainsKey(Topic));

            Assert.Throws<NotSupportedException>(() => consumer.OffsetsForTimes(
                new System.Collections.Generic.Dictionary<TopicPartition, long> { [tp] = 1_000L }));
        }
        finally
        {
            TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public void SyncMockConsumer_ViaIConsumerInterface_RoundTrips()
    {
        MockConsumer<byte[], byte[]> mock = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            TopicPartition tp = new TopicPartition(Topic, Partition);
            IConsumer<byte[], byte[]> consumer = mock;
            consumer.Assign(new[] { tp });
            consumer.Seek(tp, 0L);
            mock.AddRecord(Topic, Partition, offset: 8, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

            ConsumerRecords<byte[], byte[]> records = default!;
            TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
            Assert.Equal(8, Assert.Single(records).Offset);

            consumer.Commit(new System.Collections.Generic.Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [tp] = new OffsetAndMetadata(5, "m"),
            });
            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                consumer.Committed(new[] { tp });
            Assert.Equal(5, committed[tp]!.Offset);
            Assert.Equal("m", committed[tp]!.Metadata);

            mock.UpdatePartitions(Topic, partitionCount: 1, leaderId: 7, "broker-1", leaderPort: 9092);
            PartitionInfo info = Assert.Single(consumer.PartitionsFor(Topic));
            Assert.Equal(Topic, info.Topic);
            Assert.True(consumer.ListTopics().ContainsKey(Topic));
        }
        finally
        {
            TestTimeout.Run(() => mock.Close(), s_deadline);
        }
    }

    [Fact]
    public void TypedMockConsumer_StringLong_RoundTripsOnTheTfmMatrix()
    {
        MockConsumer<string, long> consumer = new MockConsumer<string, long>(Serdes.String, Serdes.Int64);
        try
        {
            consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
            consumer.Seek(new TopicPartition(Topic, Partition), 0L);
            consumer.AddRecord(
                Topic, Partition, offset: 9, Serdes.String.Serialize(Topic, "tfm-key")!, Serdes.Int64.Serialize(Topic, 4242L)!);

            ConsumerRecords<string, long> records = default!;
            TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);

            ConsumerRecord<string, long> record = Assert.Single(records);
            Assert.Equal("tfm-key", record.Key);
            Assert.Equal(4242L, record.Value);
            Assert.Equal(9, record.Offset);
        }
        finally
        {
            TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public void MockConsumer_Metrics_MarshalsOnTheTfmMatrix()
    {
        MockConsumer<byte[], byte[]> sync = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(sync))
        {
            System.Collections.Generic.IReadOnlyDictionary<MetricName, IMetric> metrics = sync.Metrics();
            Assert.NotNull(metrics);
            Assert.Empty(metrics);
        }

        AsyncMockConsumer<byte[], byte[]> async = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(async))
        {
            Assert.Empty(async.Metrics());
        }
    }

    private static async Task<ConsumerRecords<byte[], byte[]>> Poll(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
