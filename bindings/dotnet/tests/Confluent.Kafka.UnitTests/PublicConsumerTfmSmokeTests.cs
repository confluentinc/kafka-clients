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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test (PLAN §5.7): the public <see cref="AsyncMockConsumer"/> create →
/// subscribe → assign → add → poll → close round-trip. It uses only APIs available on the
/// netstandard2.0 floor so it <b>compiles on net462</b> (via ns2.0) as well as net8.0 /
/// net10.0 — the net462 leg's <em>build</em> must pass locally even though its <em>run</em>
/// is Windows/CI-only; net10.0 runs locally. Nothing here uses <c>GC.GetTotalAllocatedBytes</c>
/// / <c>Task.IsCompletedSuccessfully</c> or other modern-only APIs.
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
        // The record-carrying round-trip uses Assign (AddRecord requires an assigned
        // partition; subscribe + assign are mutually exclusive in Kafka). Covers
        // create → assign → seek → add → poll → close on the TFM matrix.
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            consumer.Seek(new TopicPartition(Topic, Partition), 0L); // sync (M5/P7)
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
        // Covers the subscribe / unsubscribe wire on the TFM matrix (no records —
        // subscribe and assign are mutually exclusive, so records ride the assign leg).
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
        // The four M5/P4 offset-map queries marshal on ns2.0 / net8.0 / net10.0 (PLAN §7 case
        // 13). BeginningOffsets / EndOffsets carry data (the shipped update helpers), Committed
        // returns an empty map, OffsetsForTimes faults with unsupported_version — all reachable
        // broker-free with netstandard2.0-safe APIs.
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

            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> committed = default!;
            await TestTimeout.Run(async () => committed = await consumer.Committed(request), s_deadline);
            Assert.Empty(committed);

            await Assert.ThrowsAsync<KafkaException>(() => consumer.OffsetsForTimes(
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
        // The two M5/P5 partition-metadata queries marshal the nested tree (list/map ->
        // PartitionInfo -> Node) on ns2.0 / net8.0 / net10.0, using only netstandard2.0-safe
        // APIs so the net462 build leg passes. PartitionsFor / ListTopics carry data broker-free
        // via UpdatePartitions.
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
        // The M5/P8a SYNCHRONOUS consumer (MockConsumer / IConsumer) create → assign → seek →
        // add → poll → close round-trip on the TFM matrix, using only netstandard2.0-safe APIs
        // so the net462 build leg passes. Bounded by TestTimeout (the blocking Poll / Close).
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

            // Position advances past the consumed record (offset 5 → next position 6).
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
        // The M5/P8b SYNCHRONOUS query family marshals on ns2.0 / net8.0 / net10.0 (net462 build
        // leg included), using only netstandard2.0-safe APIs. Committed round-trips a real
        // offset/metadata/epoch (Assign → Commit → Committed); BeginningOffsets / EndOffsets carry
        // data via the update helpers; PartitionsFor / ListTopics carry data via UpdatePartitions;
        // OffsetsForTimes throws unsupported_version on the mock (the honesty case). All resolve
        // synchronously (no blocking), so no TestTimeout wrapper is needed.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            TopicPartition tp = new TopicPartition(Topic, Partition);
            consumer.Assign(new[] { tp });
            consumer.Commit(new System.Collections.Generic.Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [tp] = new OffsetAndMetadata(42, "meta-x", 7),
            });

            System.Collections.Generic.IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> committed =
                consumer.Committed(new[] { tp });
            Assert.Equal(42, committed[tp].Offset);
            Assert.Equal("meta-x", committed[tp].Metadata);
            Assert.Equal(7, committed[tp].LeaderEpoch);

            consumer.UpdateBeginningOffset(Topic, Partition, 5);
            consumer.UpdateEndOffset(Topic, Partition, 99);
            Assert.Equal(5, consumer.BeginningOffsets(new[] { tp })[tp]);
            Assert.Equal(99, consumer.EndOffsets(new[] { tp })[tp]);

            consumer.UpdatePartitions(Topic, partitionCount: 1, leaderId: 7, "broker-1", leaderPort: 9092);
            PartitionInfo info = Assert.Single(consumer.PartitionsFor(Topic));
            Assert.Equal(Topic, info.Topic);
            Assert.True(consumer.ListTopics().ContainsKey(Topic));

            Assert.Throws<KafkaException>(() => consumer.OffsetsForTimes(
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
        // Hold a MockConsumer, drive it through the IConsumer interface on the TFM matrix.
        MockConsumer<byte[], byte[]> mock = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            IConsumer<byte[], byte[]> consumer = mock;
            consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
            consumer.Seek(new TopicPartition(Topic, Partition), 0L);
            mock.AddRecord(Topic, Partition, offset: 8, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

            ConsumerRecords<byte[], byte[]> records = default!;
            TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);

            Assert.Equal(8, Assert.Single(records).Offset);
        }
        finally
        {
            TestTimeout.Run(() => mock.Close(), s_deadline);
        }
    }

    [Fact]
    public void TypedMockConsumer_StringLong_RoundTripsOnTheTfmMatrix()
    {
        // The M6/P1b TYPED zero-copy poll (a non-identity <string, long> serde pair) create →
        // assign → seek → add raw bytes → poll → assert decoded Key/Value → close, on the TFM
        // matrix (ns2.0-safe APIs so the net462 build leg passes). Proves the generic typed
        // consumer + the span-deserialize copy-out marshal on every target.
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

    private static async Task<ConsumerRecords<byte[], byte[]>> Poll(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
