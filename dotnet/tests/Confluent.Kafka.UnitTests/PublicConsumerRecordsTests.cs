// Copyright 2026 Confluent Inc.
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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ConsumerRecords{TKey, TValue}"/>'s read members over a poll of two topics with two partitions each:
/// the records by partition and by topic, the partitions, the next offsets, <c>IsEmpty</c>, <c>EMPTY</c> and the
/// read-only views.
/// </summary>
public sealed class PublicConsumerRecordsTests
{
    private const string TopicA = "records-a";
    private const string TopicB = "records-b";

    // The core mock's partition order is new for each consumer, so 50 consumers that all read the failing partition
    // first would mean the order no longer varies.
    private const int MaxOrderAttempts = 50;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_a0 = new TopicPartition(TopicA, 0);
    private static readonly TopicPartition s_a1 = new TopicPartition(TopicA, 1);
    private static readonly TopicPartition s_b0 = new TopicPartition(TopicB, 0);
    private static readonly TopicPartition s_b1 = new TopicPartition(TopicB, 1);
    private static readonly TopicPartition[] s_all = { s_a0, s_a1, s_b0, s_b1 };

    private static readonly (TopicPartition Partition, long[] Offsets)[] s_added =
    {
        (s_a0, new[] { 0L, 1L }),
        (s_a1, new[] { 0L }),
        (s_b0, new[] { 0L, 1L, 2L }),
        (s_b1, new[] { 0L }),
    };

    [Fact]
    public void Partitions_AreThePolledPartitions_InTheOrderTheRecordsAreEnumerated()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Equal(s_all.OrderBy(Key), records.Partitions().OrderBy(Key));
        Assert.Equal(
            records.Select(r => new TopicPartition(r.Topic(), r.Partition())).Distinct(),
            records.Partitions());
    }

    [Fact]
    public void RecordsOfAPartition_AreItsRecords_InTheirOrder()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        foreach ((TopicPartition partition, long[] offsets) in s_added)
        {
            IReadOnlyList<ConsumerRecord<string, string>> list = records.Records(partition);
            Assert.Equal(offsets, list.Select(r => r.Offset()));
            Assert.All(list, r => Assert.Equal(partition, new TopicPartition(r.Topic(), r.Partition())));
            Assert.Equal(offsets.Select(o => Value(partition, o)), list.Select(r => r.Value()));
        }
    }

    [Fact]
    public void RecordsOfAPartitionNotPolled_OrOfADefaultPartition_IsEmpty()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Empty(records.Records(new TopicPartition(TopicA, 7)));
        Assert.Empty(records.Records(new TopicPartition("records-c", 0)));
        Assert.Empty(records.Records(default(TopicPartition)));
        Assert.Empty(records.Records(new TopicPartition(null!, 0)));
    }

    [Fact]
    public void RecordsOfATopic_AreItsPartitionsRecords_InPartitionsOrder()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        foreach (string topic in new[] { TopicA, TopicB })
        {
            ConsumerRecord<string, string>[] expected = records.Partitions()
                .Where(p => p.Topic() == topic)
                .SelectMany(p => records.Records(p))
                .ToArray();
            Assert.Equal(topic == TopicA ? 3 : 4, expected.Length);
            Assert.Equal(expected, records.Records(topic));
        }

        Assert.Empty(records.Records("records-c"));
    }

    [Fact]
    public void RecordsOfANullTopic_ThrowsIllegalArgumentException_AtTheCall()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(() => records.Records((string)null!));
        Assert.Equal("Topic must be non-null.", e.Message);
        Assert.Throws<IllegalArgumentException>(() => ConsumerRecords<string, string>.EMPTY.Records((string)null!));
    }

    [Fact]
    public void Enumeration_IsPartitionByPartition_InPartitionsOrder_AndCountsEveryRecord()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Equal(records.Partitions().SelectMany(p => records.Records(p)), records);
        Assert.Equal(7, records.Count());
        Assert.Equal(7, ((IReadOnlyCollection<ConsumerRecord<string, string>>)records).Count);
        Assert.False(records.IsEmpty());
    }

    [Fact]
    public void NextOffsets_AreEachPolledPartitionsLastOffsetPlusOne_WithNoEpochAndEmptyMetadata()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Equal(s_added.Length, records.NextOffsets().Count);
        foreach ((TopicPartition partition, long[] offsets) in s_added)
        {
            OffsetAndMetadata next = records.NextOffsets()[partition];
            Assert.Equal(offsets[offsets.Length - 1] + 1, next.Offset());
            Assert.Null(next.LeaderEpoch());
            Assert.Equal("", next.Metadata());
        }
    }

    [Fact]
    public void AnEmptyPoll_ReturnsEMPTY_TheInstanceEmptyReturns()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(Deserializer(), Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_a0 });
        consumer.Seek(s_a0, 0);

        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Same(ConsumerRecords<string, string>.EMPTY, records);
        Assert.Same(ConsumerRecords<string, string>.EMPTY, ConsumerRecords<string, string>.Empty());
        Assert.True(records.IsEmpty());
        Assert.Equal(0, records.Count());
        Assert.Empty(records.Partitions());
        Assert.Empty(records);
        Assert.Empty(records.Records(s_a0));
        Assert.Empty(records.Records(TopicA));
        Assert.Empty(records.NextOffsets());
    }

    [Fact]
    public void EMPTY_IsOneInstancePerPairOfTypeArguments()
    {
        object strings = ConsumerRecords<string, string>.EMPTY;
        object bytes = ConsumerRecords<byte[], byte[]>.EMPTY;

        Assert.NotSame(strings, bytes);
        Assert.Same(bytes, ConsumerRecords<byte[], byte[]>.Empty());
        Assert.Same(ConsumerRecords<int?, string>.EMPTY, ConsumerRecords<int?, string>.Empty());
        Assert.True(ConsumerRecords<byte[], byte[]>.EMPTY.IsEmpty());
    }

    [Fact]
    public void TheReturnedCollections_AreReadOnly()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);
        ConsumerRecord<string, string> record = records.First();

        foreach (IReadOnlyList<ConsumerRecord<string, string>> view in
            new[] { records.Records(s_a0), records.Records(new TopicPartition(TopicA, 7)) })
        {
            IList<ConsumerRecord<string, string>> list = Assert.IsAssignableFrom<IList<ConsumerRecord<string, string>>>(view);
            int count = list.Count;
            Assert.True(list.IsReadOnly);
            Assert.Throws<NotSupportedException>(() => list.Add(record));
            Assert.Throws<NotSupportedException>(() => list.Insert(0, record));
            Assert.Throws<NotSupportedException>(() => list.Remove(record));
            Assert.Throws<NotSupportedException>(() => list.Clear());
            if (count > 0)
            {
                Assert.Throws<NotSupportedException>(() => list[0] = record);
                Assert.Throws<NotSupportedException>(() => list.RemoveAt(0));
            }

            Assert.Equal(count, view.Count);
        }

        ICollection<TopicPartition> partitions =
            Assert.IsAssignableFrom<ICollection<TopicPartition>>(records.Partitions());
        Assert.True(partitions.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => partitions.Add(new TopicPartition("records-c", 0)));
        Assert.Throws<NotSupportedException>(() => partitions.Remove(s_a0));
        Assert.Throws<NotSupportedException>(() => partitions.Clear());
        IList<TopicPartition> partitionList = Assert.IsAssignableFrom<IList<TopicPartition>>(records.Partitions());
        Assert.Throws<NotSupportedException>(() => partitionList[0] = s_b1);
        Assert.Equal(4, records.Partitions().Count);

        Assert.False(records.Records(TopicA) is ICollection<ConsumerRecord<string, string>>);
        Assert.Equal(7, records.Count());
    }

    [Fact]
    public void AFailingDeserializer_CutsItsPartition_ToTheRecordsReturned()
    {
        MockConsumer<string, string> consumer =
            new MockConsumer<string, string>(Deserializer(), new FailingDeserializer("bad"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_a0 });
        consumer.Seek(s_a0, 0);
        consumer.AddRecord(TopicA, 0, 0, key: null, value: Bytes("a"));
        consumer.AddRecord(TopicA, 0, 1, key: null, value: Bytes("bad"));
        consumer.AddRecord(TopicA, 0, 2, key: null, value: Bytes("c"));

        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Equal(new[] { s_a0 }, records.Partitions());
        Assert.Equal(new[] { 0L }, records.Records(s_a0).Select(r => r.Offset()));
        Assert.Equal(1, records.Count());
        Assert.False(records.IsEmpty());
        Assert.Equal(new OffsetAndMetadata(1, null, ""), Assert.Single(records.NextOffsets()).Value);
    }

    [Fact]
    public void AFailingDeserializer_KeepsTheNextOffsetsBeforeIt_CutsItsOwn_AndGivesNoneAfterIt()
    {
        MockConsumer<string, string> consumer =
            new MockConsumer<string, string>(Deserializer(), new FailingDeserializer("bad"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition[] partitions = { s_a0, s_a1, s_b0 };
        consumer.Assign(partitions);
        foreach (TopicPartition partition in partitions)
        {
            consumer.Seek(partition, 0);
        }

        consumer.AddRecord(TopicA, 0, 0, key: null, value: Bytes("a"));
        consumer.AddRecord(TopicA, 0, 1, key: null, value: Bytes("b"));
        consumer.AddRecord(TopicA, 1, 0, key: null, value: Bytes("c"));
        consumer.AddRecord(TopicA, 1, 1, key: null, value: Bytes("bad"));
        consumer.AddRecord(TopicA, 1, 2, key: null, value: Bytes("d"));
        consumer.AddRecord(TopicB, 0, 0, key: null, value: Bytes("e"));

        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);

        Assert.Equal(s_a1, records.Partitions().Last());
        TopicPartition[] before = records.Partitions().Where(p => !p.Equals(s_a1)).ToArray();
        TopicPartition[] after = partitions.Except(records.Partitions()).ToArray();
        Assert.Equal(partitions.Length, before.Length + 1 + after.Length);
        Assert.Equal(before.Length + 1, records.NextOffsets().Count);
        foreach (TopicPartition partition in before)
        {
            long last = records.Records(partition).Last().Offset();
            Assert.Equal(new OffsetAndMetadata(last + 1, null, ""), records.NextOffsets()[partition]);
        }

        Assert.Equal(new[] { 0L }, records.Records(s_a1).Select(r => r.Offset()));
        Assert.Equal(new OffsetAndMetadata(1, null, ""), records.NextOffsets()[s_a1]);
        Assert.All(after, partition => Assert.False(records.NextOffsets().ContainsKey(partition)));
    }

    [Fact]
    public void AFailingDeserializer_GivesNoEntry_ToAPartitionWithNoRecordReturned()
    {
        // The core mock reads its partitions in an order of its own, new for each consumer: when it reads records-a-1
        // first, nothing is returned and the poll throws. New consumers are tried until one reads records-a-0 first.
        for (int attempt = 0; attempt < MaxOrderAttempts; attempt++)
        {
            ConsumerRecords<string, string>? records = PollAGoodPartitionAndAFailingOne();
            if (records is null)
            {
                continue;
            }

            Assert.Equal(new[] { s_a0 }, records.Partitions());
            Assert.Equal(new[] { 0L, 1L }, records.Records(s_a0).Select(r => r.Offset()));
            Assert.Empty(records.Records(s_a1));
            Assert.Equal(2, records.Count());
            return;
        }

        Assert.Fail($"The core mock read records-a-1 first in all {MaxOrderAttempts} consumers.");
    }

    [Fact]
    public async Task AsyncPoll_ReadsByPartitionAndTopic_AndAnEmptyPollReturnsEMPTY()
    {
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(Deserializer(), Deserializer());
        try
        {
            await TestTimeout.Run(() => consumer.Assign(s_all), s_deadline);
            foreach (TopicPartition partition in s_all)
            {
                await TestTimeout.Run(() => consumer.Seek(partition, 0), s_deadline);
            }

            ConsumerRecords<string, string> empty = await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline);
            Assert.Same(ConsumerRecords<string, string>.Empty(), empty);

            AddRecords(consumer.AddRecord);
            ConsumerRecords<string, string> records =
                await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline);

            Assert.Equal(s_all.OrderBy(Key), records.Partitions().OrderBy(Key));
            foreach ((TopicPartition partition, long[] offsets) in s_added)
            {
                Assert.Equal(offsets, records.Records(partition).Select(r => r.Offset()));
                Assert.Equal(
                    new OffsetAndMetadata(offsets[offsets.Length - 1] + 1, null, ""),
                    records.NextOffsets()[partition]);
            }

            Assert.Equal(records.Partitions().SelectMany(p => records.Records(p)), records);
            Assert.Equal(
                records.Partitions().Where(p => p.Topic() == TopicB).SelectMany(p => records.Records(p)),
                records.Records(TopicB));
            Assert.False(records.IsEmpty());
            Assert.Equal(7, records.Count());
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static MockConsumer<string, string> Ready()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(Deserializer(), Deserializer());
        try
        {
            consumer.Assign(s_all);
            foreach (TopicPartition partition in s_all)
            {
                consumer.Seek(partition, 0);
            }

            AddRecords(consumer.AddRecord);
        }
        catch
        {
            TestTimeout.Dispose(consumer);
            throw;
        }

        return consumer;
    }

    // Records a-0@0 and a-0@1 deserialize and a-1@0 fails. Returns the poll's records, or null when the poll threw
    // because the core read records-a-1 first.
    private static ConsumerRecords<string, string>? PollAGoodPartitionAndAFailingOne()
    {
        MockConsumer<string, string> consumer =
            new MockConsumer<string, string>(Deserializer(), new FailingDeserializer("bad"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_a0, s_a1 });
        consumer.Seek(s_a0, 0);
        consumer.Seek(s_a1, 0);
        consumer.AddRecord(TopicA, 0, 0, key: null, value: Bytes("a"));
        consumer.AddRecord(TopicA, 0, 1, key: null, value: Bytes("b"));
        consumer.AddRecord(TopicA, 1, 0, key: null, value: Bytes("bad"));

        try
        {
            return consumer.Poll(s_pollTimeout);
        }
        catch (RecordDeserializationException e)
        {
            Assert.Equal(s_a1, e.TopicPartition());
            return null;
        }
    }

    private static void AddRecords(Action<string, int, long, byte[]?, byte[]?> addRecord)
    {
        foreach ((TopicPartition partition, long[] offsets) in s_added)
        {
            foreach (long offset in offsets)
            {
                addRecord(partition.Topic(), partition.Partition(), offset, null, Bytes(Value(partition, offset)));
            }
        }
    }

    private static string Value(TopicPartition partition, long offset) => partition.ToString() + "@" + offset;

    private static string Key(TopicPartition partition) => partition.ToString();

    private static IDeserializer<string> Deserializer() => Serdes.String().Deserializer();

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>A UTF-8 deserializer that throws for the one value it is told to fail on.</summary>
    private sealed class FailingDeserializer : IDeserializer<string>
    {
        private readonly string _failOn;

        internal FailingDeserializer(string failOn)
        {
            _failOn = failOn;
        }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            string text = Encoding.UTF8.GetString(data.ToArray());
            return text == _failOn ? throw new FormatException("bad " + text) : text;
        }
    }
}
