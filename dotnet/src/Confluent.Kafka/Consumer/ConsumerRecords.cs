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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A container that holds the list of <see cref="ConsumerRecord{TKey, TValue}"/> per partition for a particular
/// topic, Java's <c>org.apache.kafka.clients.consumer.ConsumerRecords&lt;K, V&gt;</c>. There is one record list for
/// every topic partition returned by a poll.
/// </summary>
/// <remarks>
/// It needs no disposal, and it never changes. Its records are views of the poll's native batch, not copies: the
/// records, their <see cref="Confluent.Kafka.Common.Header.IHeaders"/>, each
/// <see cref="Confluent.Kafka.Common.Header.IHeader"/>, a header memory taken before <c>Value()</c>, and a key or value
/// that a deserializer returned as a memory over the batch's own bytes, each keep the whole batch in memory while it
/// is reachable, after later polls and after the consumer is disposed, as
/// <see cref="ConsumerRecord{TKey, TValue}"/> describes. To keep something longer without the batch, keep a copy: the
/// arrays <c>Value()</c> returns, a header added with <c>headers.Add(header.Key(), header.Value())</c>, or a key or
/// value deserialized to an owned type. A poll with neither records nor next offsets returns <see cref="EMPTY"/>, not
/// <see langword="null"/>.
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public sealed class ConsumerRecords<TKey, TValue> : IReadOnlyCollection<ConsumerRecord<TKey, TValue>>
{
    private static readonly ReadOnlyCollection<ConsumerRecord<TKey, TValue>> s_noRecords =
        new ReadOnlyCollection<ConsumerRecord<TKey, TValue>>(Array.Empty<ConsumerRecord<TKey, TValue>>());

    /// <summary>An empty instance, the one a poll with nothing to return gives.</summary>
    /// <remarks>Each pair of type arguments has its own instance.</remarks>
    public static readonly ConsumerRecords<TKey, TValue> EMPTY = new ConsumerRecords<TKey, TValue>(
        new List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>>(),
        EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance);

    private readonly List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> _records;
    private readonly Dictionary<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>> _byPartition;
    private readonly ReadOnlyCollection<TopicPartition> _partitions;
    // Null for a polled batch until the first NextOffsets() reads them from _batch, which no poll does.
    private IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? _nextOffsets;
    private readonly SafeConsumerRecordsHandle? _batch;
    private readonly int _count;

    /// <summary>Constructs a new ConsumerRecords with the given records and next offsets.</summary>
    /// <param name="records">
    /// The records for each partition. The map and each list are copied, in the map's order; a partition given an
    /// empty list keeps its entry.
    /// </param>
    /// <param name="nextOffsets">
    /// The next offset and metadata for each partition whose position was advanced during the poll call. These
    /// represent the offsets that the consumer will start reading from on the next poll. The map is copied.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="records"/> or a partition or a list in it, or <paramref name="nextOffsets"/> or a partition or
    /// an offset in it, is <see langword="null"/>.
    /// </exception>
    public ConsumerRecords(
        IReadOnlyDictionary<TopicPartition, IReadOnlyList<ConsumerRecord<TKey, TValue>>> records,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets)
        : this(CopyRecords(records), CopyNextOffsets(nextOffsets))
    {
    }

    private ConsumerRecords(
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> records,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? nextOffsets,
        SafeConsumerRecordsHandle? batch = null)
    {
        _records = records;
        _nextOffsets = nextOffsets;
        _batch = batch;
        _byPartition = new Dictionary<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>(records.Count);
        TopicPartition[] partitions = new TopicPartition[records.Count];
        int count = 0;
        for (int i = 0; i < records.Count; i++)
        {
            KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>> entry = records[i];
            _byPartition.Add(entry.Key, entry.Value);
            partitions[i] = entry.Key;
            count += entry.Value.Count;
        }

        _partitions = new ReadOnlyCollection<TopicPartition>(partitions);
        _count = count;
    }

    /// <summary>Get just the records for the given partition.</summary>
    /// <param name="partition">The partition to get records for.</param>
    /// <returns>
    /// The partition's records, in their order, as a read-only list; an empty list when this set holds
    /// none for it, as for a <see langword="null"/> partition.
    /// </returns>
    public IReadOnlyList<ConsumerRecord<TKey, TValue>> Records(TopicPartition partition) =>
        partition is not null
            && _byPartition.TryGetValue(partition, out ReadOnlyCollection<ConsumerRecord<TKey, TValue>>? records)
            ? records
            : s_noRecords;

    /// <summary>
    /// Get the next offsets and metadata corresponding to all topic partitions for which the position have been
    /// advanced in this poll call.
    /// </summary>
    /// <returns>The next offsets that the consumer will consume, as a read-only map.</returns>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> NextOffsets() => _nextOffsets ?? ReadNextOffsets();

    /// <summary>Get just the records for the given topic.</summary>
    /// <param name="topic">The topic to get records for.</param>
    /// <returns>
    /// The records of every partition of the topic, partition by partition in <see cref="Partitions()"/> order.
    /// </returns>
    /// <exception cref="IllegalArgumentException"><paramref name="topic"/> is <see langword="null"/>.</exception>
    public IEnumerable<ConsumerRecord<TKey, TValue>> Records(string topic)
    {
        if (topic is null)
        {
            throw new IllegalArgumentException("Topic must be non-null.");
        }

        return RecordsOf(topic);
    }

    /// <summary>Get the partitions which have records contained in this record set.</summary>
    /// <returns>
    /// The set of partitions with data in this record set (may be empty if no data was returned), read-only, in the
    /// order the records are enumerated.
    /// </returns>
    public IReadOnlyCollection<TopicPartition> Partitions() => _partitions;

    /// <summary>Enumerates the records partition by partition, in <see cref="Partitions()"/> order.</summary>
    /// <returns>An enumerator over all the records.</returns>
    public IEnumerator<ConsumerRecord<TKey, TValue>> GetEnumerator()
    {
        foreach (KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>> entry in _records)
        {
            ReadOnlyCollection<ConsumerRecord<TKey, TValue>> records = entry.Value;
            for (int i = 0; i < records.Count; i++)
            {
                yield return records[i];
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>The number of records for all topics.</summary>
    /// <returns>The number of records for all topics (<c>0</c> for an empty poll).</returns>
    public int Count() => _count;

    /// <summary>The number of records for all topics, the same as <see cref="Count()"/>.</summary>
    int IReadOnlyCollection<ConsumerRecord<TKey, TValue>>.Count => _count;

    /// <summary>Returns whether this container has any records.</summary>
    /// <returns><see langword="true"/> if there are no records, <see langword="false"/> otherwise.</returns>
    public bool IsEmpty() => _records.Count == 0;

    /// <summary>Returns an empty <see cref="ConsumerRecords{TKey, TValue}"/> instance.</summary>
    /// <returns>An empty <see cref="ConsumerRecords{TKey, TValue}"/>, <see cref="EMPTY"/>.</returns>
    public static ConsumerRecords<TKey, TValue> Empty() => EMPTY;

    // Wraps the per-partition records and next offsets of a poll with no records, or of one cut short by a failing
    // deserializer, without copying them: the caller gives up both.
    internal static ConsumerRecords<TKey, TValue> Owned(
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> records,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets) =>
        new ConsumerRecords<TKey, TValue>(records, ReadOnly(nextOffsets));

    // Wraps the poll's per-partition records without copying them, the next offsets read from `batch` on the first
    // NextOffsets(): the caller gives up the records.
    internal static ConsumerRecords<TKey, TValue> Owned(
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> records,
        SafeConsumerRecordsHandle batch) =>
        new ConsumerRecords<TKey, TValue>(records, null, batch);

    // The batch's next offsets, read once: a racing first call reads them too and returns the cached map. The
    // reference taken on the handle keeps the batch from being released during the read.
    private IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> ReadNextOffsets()
    {
        SafeConsumerRecordsHandle batch = _batch!;
        bool added = false;
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> read;
        try
        {
            batch.DangerousAddRef(ref added);
            read = ReadOnly(ConsumerRecordsMarshal.NextOffsets(batch.DangerousGetHandle()));
        }
        finally
        {
            if (added)
            {
                batch.DangerousRelease();
            }
        }

        return Interlocked.CompareExchange(ref _nextOffsets, read, null) ?? read;
    }

    private static List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> CopyRecords(
        IReadOnlyDictionary<TopicPartition, IReadOnlyList<ConsumerRecord<TKey, TValue>>> records)
    {
        if (records is null)
        {
            throw new ArgumentNullException(nameof(records));
        }

        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> copy =
            new List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>>(records.Count);
        foreach (KeyValuePair<TopicPartition, IReadOnlyList<ConsumerRecord<TKey, TValue>>> entry in records)
        {
            if (entry.Key is null)
            {
                throw new ArgumentNullException(nameof(records), "records must not contain null");
            }

            if (entry.Value is null)
            {
                throw new ArgumentNullException(nameof(records), "The records must not contain a null list.");
            }

            copy.Add(new KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>(
                entry.Key, CopyList(entry.Value)));
        }

        return copy;
    }

    private static ReadOnlyCollection<ConsumerRecord<TKey, TValue>> CopyList(
        IReadOnlyList<ConsumerRecord<TKey, TValue>> records)
    {
        if (records.Count == 0)
        {
            return s_noRecords;
        }

        ConsumerRecord<TKey, TValue>[] copy = new ConsumerRecord<TKey, TValue>[records.Count];
        for (int i = 0; i < copy.Length; i++)
        {
            copy[i] = records[i];
        }

        return new ReadOnlyCollection<ConsumerRecord<TKey, TValue>>(copy);
    }

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> CopyNextOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets)
    {
        if (nextOffsets is null)
        {
            throw new ArgumentNullException(nameof(nextOffsets));
        }

        Dictionary<TopicPartition, OffsetAndMetadata> copy =
            new Dictionary<TopicPartition, OffsetAndMetadata>(nextOffsets.Count);
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in nextOffsets)
        {
            if (entry.Key is null)
            {
                throw new ArgumentNullException(nameof(nextOffsets), "nextOffsets must not contain null");
            }

            if (entry.Value is null)
            {
                throw new ArgumentNullException(nameof(nextOffsets), "The next offsets must not contain a null value.");
            }

            copy.Add(entry.Key, entry.Value);
        }

        return ReadOnly(copy);
    }

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> ReadOnly(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets) =>
        nextOffsets.Count == 0
            ? EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance
            : nextOffsets is IDictionary<TopicPartition, OffsetAndMetadata> map && !map.IsReadOnly
                ? new ReadOnlyDictionary<TopicPartition, OffsetAndMetadata>(map)
                : nextOffsets;

    private IEnumerable<ConsumerRecord<TKey, TValue>> RecordsOf(string topic)
    {
        foreach (KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>> entry in _records)
        {
            if (!string.Equals(entry.Key.Topic(), topic, StringComparison.Ordinal))
            {
                continue;
            }

            ReadOnlyCollection<ConsumerRecord<TKey, TValue>> records = entry.Value;
            for (int i = 0; i < records.Count; i++)
            {
                yield return records[i];
            }
        }
    }
}
