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
using System.Globalization;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>ConsumerRecordsTest</c> run against <see cref="ConsumerRecords{TKey, TValue}"/>, plus its public
/// constructor's null checks, copies and read-only next offsets.
/// </summary>
public sealed class ConsumerRecordsTests
{
    private static readonly TopicPartition s_tp = new TopicPartition("topic", 0);

    [Fact]
    public void TestIterator()
    {
        string topic = "topic";
        int recordSize = 10;
        int partitionSize = 15;
        int emptyPartitionIndex = 3;
        ConsumerRecords<int?, string> records =
            BuildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, new[] { topic });
        using IEnumerator<ConsumerRecord<int?, string>> iterator = records.GetEnumerator();

        int recordCount = 0;
        int partitionCount = 0;
        int currentPartition = -1;

        while (iterator.MoveNext())
        {
            ConsumerRecord<int?, string> record = iterator.Current;
            ValidateEmptyPartition(record, emptyPartitionIndex);

            if (currentPartition != record.Partition())
            {
                partitionCount++;
                currentPartition = record.Partition();
            }

            ValidateRecordPayload(topic, record, currentPartition, recordCount, recordSize);
            recordCount++;
        }

        Assert.Equal(partitionSize, partitionCount + 1);
    }

    [Fact]
    public void TestRecordsByPartition()
    {
        string[] topics = { "topic1", "topic2" };
        int recordSize = 3;
        int partitionSize = 5;
        int emptyPartitionIndex = 2;

        ConsumerRecords<int?, string> consumerRecords =
            BuildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics);

        Assert.Equal(partitionSize * topics.Length, consumerRecords.NextOffsets().Count);
        foreach (string topic in topics)
        {
            for (int partition = 0; partition < partitionSize; partition++)
            {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                IReadOnlyList<ConsumerRecord<int?, string>> records = consumerRecords.Records(topicPartition);

                if (partition == emptyPartitionIndex)
                {
                    Assert.Empty(records);
                }
                else
                {
                    Assert.Equal(recordSize, records.Count);
                    ConsumerRecord<int?, string> lastRecord = records[recordSize - 1];
                    Assert.Equal(
                        new OffsetAndMetadata(lastRecord.Offset() + 1, lastRecord.LeaderEpoch(), ""),
                        consumerRecords.NextOffsets()[topicPartition]);
                    for (int i = 0; i < records.Count; i++)
                    {
                        ValidateRecordPayload(topic, records[i], partition, i, recordSize);
                    }
                }
            }
        }
    }

    [Fact]
    public void TestRecordsByNullTopic()
    {
        string nullTopic = null!;
        ConsumerRecords<int?, string> consumerRecords = ConsumerRecords<int?, string>.Empty();
        IllegalArgumentException exception =
            Assert.Throws<IllegalArgumentException>(() => consumerRecords.Records(nullTopic));
        Assert.Equal("Topic must be non-null.", exception.Message);
    }

    [Fact]
    public void TestRecordsByTopic()
    {
        string[] topics = { "topic1", "topic2", "topic3", "topic4" };
        int recordSize = 3;
        int partitionSize = 10;
        int emptyPartitionIndex = 6;
        int expectedTotalRecordSizeOfEachTopic = recordSize * (partitionSize - 1);

        ConsumerRecords<int?, string> consumerRecords =
            BuildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics);

        Assert.Equal(partitionSize * topics.Length, consumerRecords.NextOffsets().Count);

        foreach (string topic in topics)
        {
            IEnumerable<ConsumerRecord<int?, string>> records = consumerRecords.Records(topic);
            int recordCount = 0;
            int partitionCount = 0;
            int currentPartition = -1;

            foreach (ConsumerRecord<int?, string> record in records)
            {
                ValidateEmptyPartition(record, emptyPartitionIndex);

                if (currentPartition != record.Partition())
                {
                    partitionCount++;
                    currentPartition = record.Partition();
                }

                ValidateRecordPayload(topic, record, currentPartition, recordCount, recordSize);
                recordCount++;
            }

            Assert.Equal(partitionSize, partitionCount + 1);
            Assert.Equal(expectedTotalRecordSizeOfEachTopic, recordCount);
        }
    }

    [Fact]
    public void TestRecordsAreImmutable()
    {
        string topic = "topic";
        int recordSize = 3;
        int partitionSize = 6;
        int emptyPartitionIndex = 2;
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerRecord<int?, string> newRecord = new ConsumerRecord<int?, string>(
            topic, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 0, "0", new Headers(), null);
        ConsumerRecords<int?, string> records =
            BuildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, new[] { topic });
        ConsumerRecords<int?, string> emptyRecords = ConsumerRecords<int?, string>.Empty();

        Assert.Equal(partitionSize, records.NextOffsets().Count);
        AssertImmutable(records, topicPartition, topic, newRecord);
        Assert.Equal(recordSize * (partitionSize - 1), records.Count());

        AssertImmutable(emptyRecords, topicPartition, topic, newRecord);
        Assert.Equal(0, emptyRecords.Count());
    }

    [Fact(Skip = "tests the deprecated ConsumerRecords(Map) constructor, which the binding does not translate")]
    public void TestNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor()
    {
    }

    [Fact]
    public void TestNextOffsetsDoesNotLogErrorWhenConstructedWithNextOffsets()
    {
        Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> records =
            new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>>
            {
                [s_tp] = new[] { new ConsumerRecord<int?, string>("topic", 0, 0L, 0, "value") },
            };
        Dictionary<TopicPartition, OffsetAndMetadata> nextOffsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp] = new OffsetAndMetadata(1L) };

        ConsumerRecords<int?, string> consumerRecords = new ConsumerRecords<int?, string>(records, nextOffsets);

        Assert.NotEmpty(consumerRecords.NextOffsets());
        AssertSameOffsets(nextOffsets, consumerRecords.NextOffsets());
    }

    [Fact]
    public void TestNextOffsetsDoesNotLogErrorForEmptyRecords()
    {
        Assert.Empty(ConsumerRecords<object, object>.Empty().NextOffsets());
    }

    [Fact]
    public void NullRecords_ThrowsArgumentNullException_AtConstruction()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => new ConsumerRecords<int?, string>(null!, new Dictionary<TopicPartition, OffsetAndMetadata>()));
        Assert.Equal("records", e.ParamName);
    }

    [Fact]
    public void NullNextOffsets_ThrowsArgumentNullException_AtConstruction()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => new ConsumerRecords<int?, string>(Records(s_tp, OneRecord()), null!));
        Assert.Equal("nextOffsets", e.ParamName);
    }

    [Fact]
    public void ANullListInRecords_ThrowsArgumentNullException_AtConstruction()
    {
        Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> records =
            new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>>
            {
                [s_tp] = OneRecord(),
                [new TopicPartition("topic", 1)] = null!,
            };

        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => new ConsumerRecords<int?, string>(records, new Dictionary<TopicPartition, OffsetAndMetadata>()));
        Assert.Equal("records", e.ParamName);
    }

    [Fact]
    public void ANullOffsetInNextOffsets_ThrowsArgumentNullException_AtConstruction()
    {
        Dictionary<TopicPartition, OffsetAndMetadata> nextOffsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [s_tp] = new OffsetAndMetadata(1L),
            [new TopicPartition("topic", 1)] = null!,
        };

        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => new ConsumerRecords<int?, string>(Records(s_tp, OneRecord()), nextOffsets));
        Assert.Equal("nextOffsets", e.ParamName);
    }

    [Fact]
    public void ANullRecordInAList_IsKept()
    {
        ConsumerRecord<int?, string> record = new ConsumerRecord<int?, string>("topic", 0, 0L, 0, "value");
        ConsumerRecords<int?, string> records = new ConsumerRecords<int?, string>(
            Records(s_tp, new[] { record, null! }),
            new Dictionary<TopicPartition, OffsetAndMetadata>());

        ConsumerRecord<int?, string>[] expected = { record, null! };
        Assert.Equal(2, records.Count());
        Assert.Equal(expected, records.Records(s_tp));
        Assert.Equal(expected, records);
    }

    [Fact]
    public void TheMapsAndLists_AreCopied_SoChangingThemAfterwardsChangesNothing()
    {
        ConsumerRecord<int?, string> first = new ConsumerRecord<int?, string>("topic", 0, 0L, 0, "a");
        List<ConsumerRecord<int?, string>> list = new List<ConsumerRecord<int?, string>> { first };
        Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> records =
            new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> { [s_tp] = list };
        OffsetAndMetadata next = new OffsetAndMetadata(1L);
        Dictionary<TopicPartition, OffsetAndMetadata> nextOffsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp] = next };

        ConsumerRecords<int?, string> consumerRecords = new ConsumerRecords<int?, string>(records, nextOffsets);
        TopicPartition other = new TopicPartition("topic", 1);
        list.Add(new ConsumerRecord<int?, string>("topic", 0, 1L, 1, "b"));
        list[0] = new ConsumerRecord<int?, string>("topic", 0, 9L, 9, "z");
        records[other] = OneRecord();
        nextOffsets[s_tp] = new OffsetAndMetadata(9L);
        nextOffsets[other] = new OffsetAndMetadata(2L);

        Assert.Equal(new[] { first }, consumerRecords.Records(s_tp));
        Assert.Equal(new[] { first }, consumerRecords);
        Assert.Equal(new[] { s_tp }, consumerRecords.Partitions());
        Assert.Empty(consumerRecords.Records(other));
        Assert.Equal(1, consumerRecords.Count());
        Assert.Same(next, Assert.Single(consumerRecords.NextOffsets()).Value);
    }

    [Fact]
    public void APartitionGivenAnEmptyList_KeepsItsEntry()
    {
        ConsumerRecords<int?, string> records = new ConsumerRecords<int?, string>(
            Records(s_tp, Array.Empty<ConsumerRecord<int?, string>>()),
            new Dictionary<TopicPartition, OffsetAndMetadata>());

        Assert.False(records.IsEmpty());
        Assert.Equal(0, records.Count());
        Assert.Equal(new[] { s_tp }, records.Partitions());
        Assert.Empty(records.Records(s_tp));
        Assert.Empty(records);
        Assert.Empty(records.Records("topic"));
    }

    [Fact]
    public void NextOffsets_IsReadOnly_EvenThroughACast()
    {
        ConsumerRecords<int?, string> records = new ConsumerRecords<int?, string>(
            Records(s_tp, OneRecord()),
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp] = new OffsetAndMetadata(1L) });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets = records.NextOffsets();
        Assert.False(nextOffsets is Dictionary<TopicPartition, OffsetAndMetadata>);
        IDictionary<TopicPartition, OffsetAndMetadata> map =
            Assert.IsAssignableFrom<IDictionary<TopicPartition, OffsetAndMetadata>>(nextOffsets);
        Assert.True(map.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => map.Add(new TopicPartition("topic", 1), new OffsetAndMetadata(2L)));
        Assert.Throws<NotSupportedException>(() => map[s_tp] = new OffsetAndMetadata(2L));
        Assert.Throws<NotSupportedException>(() => map.Remove(s_tp));
        Assert.Throws<NotSupportedException>(() => map.Clear());
        Assert.Equal(new OffsetAndMetadata(1L), records.NextOffsets()[s_tp]);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> empty = ConsumerRecords<int?, string>.EMPTY.NextOffsets();
        Assert.Empty(empty);
        Assert.False(empty is ICollection<KeyValuePair<TopicPartition, OffsetAndMetadata>> { IsReadOnly: false });
        Assert.Empty(new ConsumerRecords<int?, string>(
            Records(s_tp, OneRecord()), new Dictionary<TopicPartition, OffsetAndMetadata>()).NextOffsets());
    }

    private static void AssertImmutable(
        ConsumerRecords<int?, string> records,
        TopicPartition topicPartition,
        string topic,
        ConsumerRecord<int?, string> newRecord)
    {
        IList<ConsumerRecord<int?, string>> partitionRecords =
            Assert.IsAssignableFrom<IList<ConsumerRecord<int?, string>>>(records.Records(topicPartition));
        Assert.Throws<NotSupportedException>(() => partitionRecords.Add(newRecord));
        ICollection<TopicPartition> partitions =
            Assert.IsAssignableFrom<ICollection<TopicPartition>>(records.Partitions());
        Assert.Throws<NotSupportedException>(() => partitions.Add(topicPartition));

        // A .NET enumerator has no remove, so the records of a topic must not be a collection that can change.
        Assert.False(records.Records(topic) is ICollection<ConsumerRecord<int?, string>>);
    }

    private static ConsumerRecords<int?, string> BuildTopicTestRecords(
        int recordSize,
        int partitionSize,
        int emptyPartitionIndex,
        IEnumerable<string> topics)
    {
        Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> partitionToRecords =
            new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>>();
        Dictionary<TopicPartition, OffsetAndMetadata> nextOffsets = new Dictionary<TopicPartition, OffsetAndMetadata>();
        foreach (string topic in topics)
        {
            for (int i = 0; i < partitionSize; i++)
            {
                List<ConsumerRecord<int?, string>> records = new List<ConsumerRecord<int?, string>>(recordSize);
                if (i != emptyPartitionIndex)
                {
                    for (int j = 0; j < recordSize; j++)
                    {
                        records.Add(new ConsumerRecord<int?, string>(
                            topic, i, j, 0L, TimestampType.CREATE_TIME, 0, 0, j,
                            j.ToString(CultureInfo.InvariantCulture), new Headers(), null));
                    }
                }

                TopicPartition tp = new TopicPartition(topic, i);
                partitionToRecords[tp] = records;
                nextOffsets[tp] = new OffsetAndMetadata(recordSize, null, "");
            }
        }

        return new ConsumerRecords<int?, string>(partitionToRecords, nextOffsets);
    }

    private static void ValidateEmptyPartition(ConsumerRecord<int?, string> record, int emptyPartitionIndex)
    {
        Assert.NotEqual(emptyPartitionIndex, record.Partition());
    }

    private static void ValidateRecordPayload(
        string topic,
        ConsumerRecord<int?, string> record,
        int currentPartition,
        int recordCount,
        int recordSize)
    {
        Assert.Equal(topic, record.Topic());
        Assert.Equal(currentPartition, record.Partition());
        Assert.Equal(recordCount % recordSize, record.Offset());
        Assert.Equal(recordCount % recordSize, record.Key());
        Assert.Equal((recordCount % recordSize).ToString(CultureInfo.InvariantCulture), record.Value());
    }

    private static void AssertSameOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> expected,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in expected)
        {
            Assert.True(actual.TryGetValue(entry.Key, out OffsetAndMetadata? value), entry.Key.ToString());
            Assert.Equal(entry.Value, value);
        }
    }

    private static Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> Records(
        TopicPartition partition, IReadOnlyList<ConsumerRecord<int?, string>> records) =>
        new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>> { [partition] = records };

    private static ConsumerRecord<int?, string>[] OneRecord() =>
        new[] { new ConsumerRecord<int?, string>("topic", 0, 0L, 0, "value") };
}
