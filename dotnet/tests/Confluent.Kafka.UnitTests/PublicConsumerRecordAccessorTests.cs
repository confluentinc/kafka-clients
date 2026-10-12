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
/// M9/P1 — the three <see cref="ConsumerRecord{TKey, TValue}"/> accessors completed this
/// phase: <see cref="ConsumerRecord{TKey, TValue}.LeaderEpoch"/>,
/// <see cref="ConsumerRecord{TKey, TValue}.SerializedKeySize"/> and
/// <see cref="ConsumerRecord{TKey, TValue}.SerializedValueSize"/>. Exercised through the
/// broker-free <see cref="MockConsumer{TKey, TValue}"/> (sync) and
/// <see cref="AsyncMockConsumer{TKey, TValue}"/> (async) receive path (PLAN §6).
/// </summary>
/// <remarks>
/// <para>
/// <b>Mock behavior finding (PLAN §6 verify-at-implementation).</b> The mock's
/// <c>AddRecord</c> path builds each native record via the core's <c>ConsumerRecord::new</c>,
/// which hard-codes both serialized sizes to the <c>NULL_SIZE</c> sentinel (<c>-1</c>)
/// <b>regardless</b> of the key/value length, and sets the leader epoch to <c>None</c>. So a
/// mock-added record reports <c>SerializedKeySize == -1</c>, <c>SerializedValueSize == -1</c>
/// and <c>LeaderEpoch == null</c> even when the key/value are non-empty. The
/// <b>positive</b> serialized-size case (Java returns the byte length) and the
/// <b>present</b> leader-epoch case are therefore <b>not injectable in Mode A</b> — they
/// need a real broker and are covered integration-only (no <c>AddRecord</c> overload is
/// added, which would require an extended mock ABI = Mode B, out of scope). These tests
/// assert exactly the reachable contract and pin the accessor plumbing (that each property
/// reflects the ABI value, not a managed constant).
/// </para>
/// </remarks>
public sealed class PublicConsumerRecordAccessorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "accessor-topic";
    private const int Partition = 0;

    // Assign + seek to offset 0 so the mock poll has a valid position (the canonical
    // broker-free poll setup — the byte[]/byte[] identity serde surfaces the raw payloads).
    private static MockConsumer<byte[], byte[]> ReadyToPoll()
    {
        MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    // ---- SerializedKeySize / SerializedValueSize ----

    [Fact]
    public void SyncPoll_NullKeyAndValue_SerializedSizesAreMinusOne()
    {
        // The genuine Java contract (and the mock value): a null key/value serializes to
        // size -1. This is the null→-1 assertion PLAN §6 keeps reachable via the mock.
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Equal(-1, record.SerializedKeySize);
        Assert.Equal(-1, record.SerializedValueSize);
    }

    [Fact]
    public void SyncPoll_PresentKeyAndValue_MockReportsMinusOne_PositiveSizeIsIntegrationOnly()
    {
        // A present 3-byte key + 5-byte value. Java would report 3 / 5, but the mock's
        // ConsumerRecord::new hard-codes NULL_SIZE (-1) irrespective of length (the §6
        // finding), so -1 is the reachable value here — the positive-size path is
        // integration-only. Asserting -1 characterizes the mock limit AND pins the
        // accessor plumbing; a future mock ABI that computed sizes (Mode B) would flip this
        // deliberately.
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 0, key: new byte[] { 1, 2, 3 }, value: new byte[] { 9, 8, 7, 6, 5 });

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Equal(-1, record.SerializedKeySize);
        Assert.Equal(-1, record.SerializedValueSize);
    }

    // ---- LeaderEpoch (absent case — the only mock-reachable state) ----

    [Fact]
    public void SyncPoll_LeaderEpoch_MockAbsent_IsNull()
    {
        // MockConsumer_add_record carries no leader epoch, so the record's epoch is None →
        // the presence-style accessor returns false → int? null. The present case is not
        // mock-injectable in Mode A (integration-only).
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 0, key: new byte[] { 7 }, value: new byte[] { 8 });

        ConsumerRecord<byte[], byte[]> record = Assert.Single(Poll(consumer));

        Assert.Null(record.LeaderEpoch);
    }

    // ---- Async mirror (AsyncMockConsumer) — same reachable contract on the async path ----

    [Fact]
    public async Task AsyncPoll_Accessors_MockReachableContract()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await TestTimeout.Run(
                () => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
            consumer.AddRecord(Topic, Partition, offset: 0, key: new byte[] { 1, 2, 3 }, value: null);

            ConsumerRecords<byte[], byte[]> records = default!;
            await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);

            ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
            Assert.Null(record.LeaderEpoch);            // absent (mock carries none)
            Assert.Equal(-1, record.SerializedKeySize); // mock quirk: -1 regardless (integration-only positive)
            Assert.Equal(-1, record.SerializedValueSize); // genuine -1: null value
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }

    // Every blocking Poll routes through the TestTimeout hang guard (PLAN §8).
    private static ConsumerRecords<byte[], byte[]> Poll(IConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        TestTimeout.Run(() => result = consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
