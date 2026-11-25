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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a native poll batch into <see cref="ConsumerRecords{TKey, TValue}"/>, deserializing each key and value from
/// a span over the batch or, for a deserializer with the memory form, from a memory that keeps the batch alive, with
/// the batch's next offsets, and stops at the first record that fails to deserialize.
/// </summary>
internal static class ConsumerRecordsMarshal
{
    private static readonly IReadOnlyList<KeyValuePair<TopicPartition, long>> s_noRewind =
        Array.Empty<KeyValuePair<TopicPartition, long>>();

    // On a failure, the records before the failing one are returned, and rewind holds each partition's first
    // record not returned: the failing one, and the first record of every later partition in the batch. A
    // partition in rewind has as next offset its last returned record's offset + 1 with that record's leader epoch,
    // or none when no record of it was returned; every other partition keeps the batch's. A deserializer's headers
    // form, given when it has one, is called with the headers the record then carries.
    internal static ConsumerRecords<TKey, TValue> CopyOut<TKey, TValue>(
        IntPtr records,
        IDeserializer<TKey> keyDeserializer,
        IDeserializerWithHeaders<TKey>? keyWithHeaders,
        IDeserializer<TValue> valueDeserializer,
        IDeserializerWithHeaders<TValue>? valueWithHeaders,
        out RecordDeserializationException? error,
        out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind)
    {
        SafeConsumerRecordsHandle? batch = null;
        return CopyOut(
            records, keyDeserializer, keyWithHeaders, null, valueDeserializer, valueWithHeaders, null, ref batch,
            out error, out rewind);
    }

    // A deserializer's memory form, given when it has one, is called instead of its other forms, with a memory over
    // the batch. The batch then becomes `batch`, which owns `records` from that point, also when this method throws;
    // a batch with no records, or read with no memory form, leaves `batch` as given and `records` to the caller.
    internal static ConsumerRecords<TKey, TValue> CopyOut<TKey, TValue>(
        IntPtr records,
        IDeserializer<TKey> keyDeserializer,
        IDeserializerWithHeaders<TKey>? keyWithHeaders,
        IDeserializerWithReadOnlyMemory<TKey>? keyWithMemory,
        IDeserializer<TValue> valueDeserializer,
        IDeserializerWithHeaders<TValue>? valueWithHeaders,
        IDeserializerWithReadOnlyMemory<TValue>? valueWithMemory,
        ref SafeConsumerRecordsHandle? batch,
        out RecordDeserializationException? error,
        out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind)
    {
        DeserializingCopy<TKey, TValue> copy = new DeserializingCopy<TKey, TValue>(
            keyDeserializer, keyWithHeaders, keyWithMemory, valueDeserializer, valueWithHeaders, valueWithMemory);
        return CopyOut<TKey, TValue, DeserializingCopy<TKey, TValue>>(records, ref copy, ref batch, out error, out rewind);
    }

    // The mock consumer's copy-out: each record's key and value are taken from the mock's slots by the ids its C
    // record carries, never read as bytes. No deserializer runs, so it gives no error and no rewind, and no batch is
    // kept: `records` stays the caller's.
    internal static ConsumerRecords<TKey, TValue> CopyOut<TKey, TValue>(
        IntPtr records,
        MockRecordSlots<TKey, TValue> slots)
    {
        ResolvingCopy<TKey, TValue> copy = new ResolvingCopy<TKey, TValue>(slots);
        SafeConsumerRecordsHandle? batch = null;
        return CopyOut<TKey, TValue, ResolvingCopy<TKey, TValue>>(records, ref copy, ref batch, out _, out _);
    }

    // The one batch walk of both copy-outs. TCopy is a struct, so the JIT makes one body per source and calls its
    // Copy directly: the decode path pays no interface call and no allocation for the choice.
    private static ConsumerRecords<TKey, TValue> CopyOut<TKey, TValue, TCopy>(
        IntPtr records,
        ref TCopy copy,
        ref SafeConsumerRecordsHandle? batch,
        out RecordDeserializationException? error,
        out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind)
        where TCopy : struct, IRecordCopy<TKey, TValue>
    {
        error = null;
        rewind = s_noRewind;
        if (records == IntPtr.Zero)
        {
            return ConsumerRecords<TKey, TValue>.EMPTY;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ConsumerRecordsCount(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets = NextOffsets(records);
        if (count <= 0)
        {
            // A poll that only moved past control records gives next offsets and no records.
            return nextOffsets.Count == 0
                ? ConsumerRecords<TKey, TValue>.EMPTY
                : ConsumerRecords<TKey, TValue>.Owned(
                    new List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>>(0),
                    nextOffsets);
        }

        if (copy.KeepsBatch)
        {
            batch = new SafeConsumerRecordsHandle(records);
        }

        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> byPartition;
        IntPtr partitions = NativeMethods.ConsumerRecordsPartitions(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int partitionCount = Size(partitions);
            byPartition =
                new List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>>(partitionCount);
            RecordReader reader = default;
            for (int p = 0; p < partitionCount; p++)
            {
                IntPtr partition = Get(partitions, p);
                string topic = reader.Topic(partition);
                int partitionId = NativeMethods.TopicPartitionPartition(partition, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);

                IntPtr partitionRecords = NativeMethods.ConsumerRecordsRecordsWithPartition(records, partition, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                try
                {
                    int size = Size(partitionRecords);
                    List<ConsumerRecord<TKey, TValue>> list = new List<ConsumerRecord<TKey, TValue>>(size);
                    for (int i = 0; i < size; i++)
                    {
                        IntPtr record = Get(partitionRecords, i);
                        error = copy.Copy(ref reader, record, topic, partitionId, batch, list);
                        if (error is not null)
                        {
                            rewind = Rewind(records, partitions, p, error);
                            AddPartition(byPartition, topic, partitionId, list);
                            return Build(byPartition, Cut(nextOffsets, rewind, byPartition));
                        }
                    }

                    AddPartition(byPartition, topic, partitionId, list);
                }
                finally
                {
                    DestroyList(partitionRecords);
                }
            }
        }
        finally
        {
            DestroyList(partitions);
        }

        return Build(byPartition, nextOffsets);
    }

    // The awaiting form of CopyOut, for a consumer one of whose deserializers is awaited: one record at a time, key
    // then value, a built-in still called through its synchronous class. `copy.Batch` owns `records` from the moment
    // it is set, also when this method throws. Once the token has fired the copy-out ends before the next record and
    // gives no records, since the poll then ends canceled.
    internal static async Task CopyOutAsync<TKey, TValue>(
        IntPtr records,
        AsyncDeserializerCall<TKey> key,
        AsyncDeserializerCall<TValue> value,
        AsyncCopyOut<TKey, TValue> copy,
        CancellationToken cancellationToken)
    {
        if (records == IntPtr.Zero)
        {
            copy.Records = ConsumerRecords<TKey, TValue>.EMPTY;
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ConsumerRecordsCount(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets = NextOffsets(records);
        if (count <= 0)
        {
            copy.Records = nextOffsets.Count == 0
                ? ConsumerRecords<TKey, TValue>.EMPTY
                : ConsumerRecords<TKey, TValue>.Owned(
                    new List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>>(0),
                    nextOffsets);
            return;
        }

        SafeConsumerRecordsHandle batch = new SafeConsumerRecordsHandle(records);
        copy.Batch = batch;

        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> byPartition;
        IntPtr partitions = NativeMethods.ConsumerRecordsPartitions(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int partitionCount = Size(partitions);
            byPartition =
                new List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>>(partitionCount);
            RecordReader reader = default;
            for (int p = 0; p < partitionCount; p++)
            {
                IntPtr partition = Get(partitions, p);
                string topic = reader.Topic(partition);
                int partitionId = NativeMethods.TopicPartitionPartition(partition, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);

                IntPtr partitionRecords = NativeMethods.ConsumerRecordsRecordsWithPartition(records, partition, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                try
                {
                    int size = Size(partitionRecords);
                    List<ConsumerRecord<TKey, TValue>> list = new List<ConsumerRecord<TKey, TValue>>(size);
                    for (int i = 0; i < size; i++)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        IntPtr record = Get(partitionRecords, i);
                        RecordFields fields = reader.Fields(record);
                        RecordHeaders headers = CopyHeaders(record);
                        batch.AddBytes((long)Length(fields.Key) + Length(fields.Value));

                        TKey? deserializedKey = default;
                        TValue? deserializedValue = default;
                        Exception? cause = null;
                        RecordDeserializationException.DeserializationExceptionOrigin origin =
                            RecordDeserializationException.DeserializationExceptionOrigin.KEY;
                        if (fields.Key != IntPtr.Zero)
                        {
                            try
                            {
                                deserializedKey = await DeserializeAsync(
                                    key, batch, topic, headers, fields.Key, cancellationToken).ConfigureAwait(false);
                            }
                            catch (Exception thrown)
                            {
                                cause = thrown;
                            }
                        }

                        if (cause is null && fields.Value != IntPtr.Zero)
                        {
                            origin = RecordDeserializationException.DeserializationExceptionOrigin.VALUE;
                            try
                            {
                                deserializedValue = await DeserializeAsync(
                                    value, batch, topic, headers, fields.Value, cancellationToken).ConfigureAwait(false);
                            }
                            catch (Exception thrown)
                            {
                                cause = thrown;
                            }
                        }

                        if (cause is not null)
                        {
                            RecordDeserializationException error = NewError(
                                origin, new TopicPartition(topic, partitionId), fields.Offset, fields.Timestamp,
                                fields.TimestampType, fields.Key, fields.Value, headers, cause);
                            copy.Error = error;
                            copy.Rewind = Rewind(records, partitions, p, error);
                            AddPartition(byPartition, topic, partitionId, list);
                            copy.Records = Build(byPartition, Cut(nextOffsets, copy.Rewind, byPartition));
                            return;
                        }

                        list.Add(NewRecord(record, topic, partitionId, fields, deserializedKey, deserializedValue, headers));
                    }

                    AddPartition(byPartition, topic, partitionId, list);
                }
                finally
                {
                    DestroyList(partitionRecords);
                }
            }
        }
        finally
        {
            DestroyList(partitions);
        }

        copy.Records = Build(byPartition, nextOffsets);
    }

    internal static IReadOnlyList<KeyValuePair<TopicPartition, long>> FirstPositions(IntPtr records)
    {
        if (records == IntPtr.Zero)
        {
            return s_noRewind;
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr partitions = NativeMethods.ConsumerRecordsPartitions(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            List<KeyValuePair<TopicPartition, long>> positions = new List<KeyValuePair<TopicPartition, long>>();
            AppendFirstPositions(records, partitions, 0, positions);
            return positions;
        }
        finally
        {
            DestroyList(partitions);
        }
    }

    internal static void Destroy(IntPtr records)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerRecordsDestroy(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    // A partition none of whose records was returned gets no entry.
    private static void AddPartition<TKey, TValue>(
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> byPartition,
        string topic,
        int partition,
        List<ConsumerRecord<TKey, TValue>> records)
    {
        if (records.Count > 0)
        {
            byPartition.Add(new KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>(
                new TopicPartition(topic, partition), records.AsReadOnly()));
        }
    }

    private static ConsumerRecords<TKey, TValue> Build<TKey, TValue>(
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> byPartition,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets) =>
        byPartition.Count == 0 && nextOffsets.Count == 0
            ? ConsumerRecords<TKey, TValue>.EMPTY
            : ConsumerRecords<TKey, TValue>.Owned(byPartition, nextOffsets);

    // The batch's owned map is copied and freed here, before the batch it was read from is.
    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> NextOffsets(IntPtr records)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = NativeMethods.ConsumerRecordsNextOffsets(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            return OffsetMapMarshal.CopyOut(map);
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MapDestroy(map, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Cut<TKey, TValue>(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets,
        IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind,
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> byPartition)
    {
        Dictionary<TopicPartition, OffsetAndMetadata> cut =
            new Dictionary<TopicPartition, OffsetAndMetadata>(nextOffsets.Count + 1);
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in nextOffsets)
        {
            cut.Add(entry.Key, entry.Value);
        }

        foreach (KeyValuePair<TopicPartition, long> position in rewind)
        {
            cut.Remove(position.Key);
            ConsumerRecord<TKey, TValue>? last = LastReturned(byPartition, position.Key);
            if (last is not null)
            {
                cut[position.Key] = new OffsetAndMetadata(last.Offset() + 1, last.LeaderEpoch(), string.Empty);
            }
        }

        return cut;
    }

    private static ConsumerRecord<TKey, TValue>? LastReturned<TKey, TValue>(
        List<KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>>> byPartition,
        TopicPartition partition)
    {
        foreach (KeyValuePair<TopicPartition, ReadOnlyCollection<ConsumerRecord<TKey, TValue>>> entry in byPartition)
        {
            if (entry.Key.Equals(partition))
            {
                return entry.Value[entry.Value.Count - 1];
            }
        }

        return null;
    }

    private static List<KeyValuePair<TopicPartition, long>> Rewind(
        IntPtr records,
        IntPtr partitions,
        int failedIndex,
        RecordDeserializationException error)
    {
        List<KeyValuePair<TopicPartition, long>> rewind = new List<KeyValuePair<TopicPartition, long>>
        {
            new KeyValuePair<TopicPartition, long>(error.TopicPartition(), error.Offset()),
        };
        AppendFirstPositions(records, partitions, failedIndex + 1, rewind);
        return rewind;
    }

    private static void AppendFirstPositions(
        IntPtr records,
        IntPtr partitions,
        int fromIndex,
        List<KeyValuePair<TopicPartition, long>> positions)
    {
        int partitionCount = Size(partitions);
        for (int p = fromIndex; p < partitionCount; p++)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr partition = Get(partitions, p);
            string topic = Utf8Marshal.PtrToString(NativeMethods.TopicPartitionTopic(partition, ref outErr)) ?? string.Empty;
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int partitionId = NativeMethods.TopicPartitionPartition(partition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            IntPtr partitionRecords = NativeMethods.ConsumerRecordsRecordsWithPartition(records, partition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            try
            {
                if (Size(partitionRecords) > 0)
                {
                    long offset = NativeMethods.ConsumerRecordOffset(Get(partitionRecords, 0), ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    positions.Add(new KeyValuePair<TopicPartition, long>(new TopicPartition(topic, partitionId), offset));
                }
            }
            finally
            {
                DestroyList(partitionRecords);
            }
        }
    }

    private static int Size(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int size = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return size;
    }

    private static IntPtr Get(IntPtr list, int index)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr element = NativeMethods.ListGet(list, index, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return element;
    }

    private static void DestroyList(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static RecordHeaders CopyHeaders(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr recordHeaders = NativeMethods.ConsumerRecordHeaders(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (recordHeaders == IntPtr.Zero)
        {
            return new RecordHeaders();
        }

        IntPtr view = NativeMethods.RecordHeadersAsHeaders(recordHeaders, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr array = NativeMethods.HeadersToArray(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (array == IntPtr.Zero)
        {
            return new RecordHeaders();
        }

        try
        {
            int count = Size(array);
            if (count <= 0)
            {
                return new RecordHeaders();
            }

            List<IHeader> headers = new List<IHeader>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr header = NativeMethods.RecordHeaderAsHeader(Get(array, i), ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = NativeMethods.HeaderKey(header, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                KafkaBytes value = NativeMethods.HeaderValue(header, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                headers.Add(new RecordHeader(Utf8Marshal.PtrToString(key) ?? string.Empty, CopyBytes(value)));
            }

            return RecordHeaders.Wrap(headers);
        }
        finally
        {
            DestroyList(array);
        }
    }

    private static byte[]? CopyBytes(KafkaBytes bytes)
    {
        if (bytes.Data == IntPtr.Zero)
        {
            return null;
        }

        byte[] buffer = new byte[Math.Max(bytes.Length, 0)];
        if (buffer.Length > 0)
        {
            Marshal.Copy(bytes.Data, buffer, 0, buffer.Length);
        }

        return buffer;
    }

    private static unsafe byte[]? CopyBytes(IntPtr bytes) =>
        bytes == IntPtr.Zero ? null : CopyBytes(*(KafkaBytes*)bytes);

    private static unsafe int Length(IntPtr bytes) =>
        bytes == IntPtr.Zero ? 0 : Math.Max(((KafkaBytes*)bytes)->Length, 0);

    private static unsafe T? Deserialize<T>(
        IDeserializer<T> deserializer,
        IDeserializerWithHeaders<T>? withHeaders,
        IDeserializerWithReadOnlyMemory<T>? withMemory,
        SafeConsumerRecordsHandle? batch,
        string topic,
        IHeaders headers,
        IntPtr bytes)
    {
        KafkaBytes* view = (KafkaBytes*)bytes;
        int length = Math.Max(view->Length, 0);
        if (withMemory is not null)
        {
            return withMemory.Deserialize(topic, headers, batch!.HandOut(view->Data, length));
        }

        ReadOnlySpan<byte> data = new ReadOnlySpan<byte>((void*)view->Data, length);
        return withHeaders is null ? deserializer.Deserialize(topic, data) : withHeaders.Deserialize(topic, headers, data);
    }

    // A deserializer that may keep its memory is given one over the batch; any other awaited one is lent a memory of
    // its own, never shared with another call, which is revoked once its call has completed, so a memory it kept
    // cannot read the bytes of a later record.
    private static unsafe ValueTask<T?> DeserializeAsync<T>(
        AsyncDeserializerCall<T> call,
        SafeConsumerRecordsHandle batch,
        string topic,
        IHeaders headers,
        IntPtr bytes,
        CancellationToken cancellationToken)
    {
        if (call.Inner is not null)
        {
            return new ValueTask<T?>(Deserialize(
                call.Inner, call.InnerWithHeaders, call.InnerWithMemory, batch, topic, headers, bytes));
        }

        KafkaBytes* view = (KafkaBytes*)bytes;
        int length = Math.Max(view->Length, 0);
        if (call.Retains)
        {
            return call.DeserializeAwaited(topic, headers, batch.HandOut(view->Data, length), cancellationToken);
        }

        BytesMemoryManager? lent = batch.Lend(view->Data, length);
        return lent is null
            ? call.DeserializeAwaited(topic, headers, ReadOnlyMemory<byte>.Empty, cancellationToken)
            : DeserializeThenRevoke(call, lent, topic, headers, cancellationToken);
    }

    private static async ValueTask<T?> DeserializeThenRevoke<T>(
        AsyncDeserializerCall<T> call,
        BytesMemoryManager lent,
        string topic,
        IHeaders headers,
        CancellationToken cancellationToken)
    {
        try
        {
            return await call.DeserializeAwaited(topic, headers, lent.ReadOnlyMemory, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            lent.Revoke();
        }
    }

    private static ConsumerRecord<TKey, TValue> NewRecord<TKey, TValue>(
        IntPtr record,
        string topic,
        int partition,
        RecordFields fields,
        TKey? key,
        TValue? value,
        IHeaders headers)
    {
        IntPtr outErr = IntPtr.Zero;
        int serializedKeySize = NativeMethods.ConsumerRecordSerializedKeySize(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int serializedValueSize = NativeMethods.ConsumerRecordSerializedValueSize(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int leaderEpoch = NativeMethods.ConsumerRecordLeaderEpoch(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ConsumerRecord<TKey, TValue>(
            topic, partition, fields.Offset, fields.Timestamp, fields.TimestampType, serializedKeySize,
            serializedValueSize, key, value, headers, leaderEpoch < 0 ? null : leaderEpoch);
    }

    private static RecordDeserializationException NewError(
        RecordDeserializationException.DeserializationExceptionOrigin origin,
        TopicPartition partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        IntPtr key,
        IntPtr value,
        IHeaders headers,
        Exception cause) =>
        new RecordDeserializationException(
            origin,
            partition,
            offset,
            timestamp,
            timestampType,
            CopyBytes(key),
            CopyBytes(value),
            headers,
            "Error deserializing "
                + (origin == RecordDeserializationException.DeserializationExceptionOrigin.KEY ? "KEY" : "VALUE")
                + " for partition " + partition.ToString() + " at offset "
                + offset.ToString(CultureInfo.InvariantCulture)
                + ". If needed, please seek past the record to continue consumption.",
            cause);

    private static TimestampType ToTimestampType(int value) => value switch
    {
        1 => TimestampType.CREATE_TIME,
        2 => TimestampType.LOG_APPEND_TIME,
        _ => TimestampType.NO_TIMESTAMP_TYPE,
    };

    // What the awaiting copy-out reads of one record before it deserializes its key and value.
    private readonly struct RecordFields
    {
        internal RecordFields(long offset, long timestamp, TimestampType timestampType, IntPtr key, IntPtr value)
        {
            Offset = offset;
            Timestamp = timestamp;
            TimestampType = timestampType;
            Key = key;
            Value = value;
        }

        internal long Offset { get; }

        internal long Timestamp { get; }

        internal TimestampType TimestampType { get; }

        internal IntPtr Key { get; }

        internal IntPtr Value { get; }
    }

    // A topic decoded once per run of partitions of the same topic, and a timestamp type read once per singleton:
    // the batch is read partition by partition, so neither is decoded per record.
    private struct RecordReader
    {
        private IntPtr _topicPointer;
        private string? _topic;
        private IntPtr _timestampTypePointer;
        private TimestampType _timestampType;

        internal string Topic(IntPtr partition)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr pointer = NativeMethods.TopicPartitionTopic(partition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (_topic is null || !SameUtf8(pointer, _topicPointer))
            {
                _topic = Utf8Marshal.PtrToString(pointer) ?? string.Empty;
            }

            _topicPointer = pointer;
            return _topic;
        }

        internal RecordDeserializationException? Copy<TKey, TValue>(
            IntPtr record,
            string topic,
            int partition,
            IDeserializer<TKey> keyDeserializer,
            IDeserializerWithHeaders<TKey>? keyWithHeaders,
            IDeserializerWithReadOnlyMemory<TKey>? keyWithMemory,
            IDeserializer<TValue> valueDeserializer,
            IDeserializerWithHeaders<TValue>? valueWithHeaders,
            IDeserializerWithReadOnlyMemory<TValue>? valueWithMemory,
            SafeConsumerRecordsHandle? batch,
            List<ConsumerRecord<TKey, TValue>> records)
        {
            IntPtr outErr = IntPtr.Zero;
            long offset = NativeMethods.ConsumerRecordOffset(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            long timestamp = NativeMethods.ConsumerRecordTimestamp(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TimestampType timestampType = TimestampTypeOf(record);
            IntPtr key = NativeMethods.ConsumerRecordKey(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.ConsumerRecordValue(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            RecordHeaders headers = CopyHeaders(record);
            batch?.AddBytes((long)Length(key) + Length(value));

            TKey? deserializedKey = default;
            if (key != IntPtr.Zero)
            {
                try
                {
                    deserializedKey = Deserialize(
                        keyDeserializer, keyWithHeaders, keyWithMemory, batch, topic, headers, key);
                }
                catch (Exception cause)
                {
                    return NewError(
                        RecordDeserializationException.DeserializationExceptionOrigin.KEY,
                        new TopicPartition(topic, partition), offset, timestamp, timestampType, key, value, headers, cause);
                }
            }

            TValue? deserializedValue = default;
            if (value != IntPtr.Zero)
            {
                try
                {
                    deserializedValue = Deserialize(
                        valueDeserializer, valueWithHeaders, valueWithMemory, batch, topic, headers, value);
                }
                catch (Exception cause)
                {
                    return NewError(
                        RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
                        new TopicPartition(topic, partition), offset, timestamp, timestampType, key, value, headers, cause);
                }
            }

            int serializedKeySize = NativeMethods.ConsumerRecordSerializedKeySize(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int serializedValueSize = NativeMethods.ConsumerRecordSerializedValueSize(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int leaderEpoch = NativeMethods.ConsumerRecordLeaderEpoch(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            records.Add(new ConsumerRecord<TKey, TValue>(
                topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize,
                deserializedKey, deserializedValue, headers, leaderEpoch < 0 ? null : leaderEpoch));
            return null;
        }

        // The mock consumer's record: every field read as Copy reads it, the key and value taken from the mock's slots
        // by the ids in the C record's key and value slots, which are never dereferenced. The delivery count, which a
        // record the broker sends never carries here, is the one given to the mock's AddRecord.
        internal void Resolve<TKey, TValue>(
            IntPtr record,
            string topic,
            int partition,
            MockRecordSlots<TKey, TValue> slots,
            List<ConsumerRecord<TKey, TValue>> records)
        {
            IntPtr outErr = IntPtr.Zero;
            long offset = NativeMethods.ConsumerRecordOffset(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            long timestamp = NativeMethods.ConsumerRecordTimestamp(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TimestampType timestampType = TimestampTypeOf(record);
            IntPtr key = NativeMethods.ConsumerRecordKey(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.ConsumerRecordValue(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            RecordHeaders headers = CopyHeaders(record);
            int serializedKeySize = NativeMethods.ConsumerRecordSerializedKeySize(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int serializedValueSize = NativeMethods.ConsumerRecordSerializedValueSize(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int leaderEpoch = NativeMethods.ConsumerRecordLeaderEpoch(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            short deliveryCount = NativeMethods.ConsumerRecordDeliveryCount(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            slots.Take(key, value, topic, partition, out TKey? resolvedKey, out TValue? resolvedValue);
            records.Add(new ConsumerRecord<TKey, TValue>(
                topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize,
                resolvedKey, resolvedValue, headers, leaderEpoch < 0 ? null : leaderEpoch,
                deliveryCount < 0 ? null : deliveryCount));
        }

        internal RecordFields Fields(IntPtr record)
        {
            IntPtr outErr = IntPtr.Zero;
            long offset = NativeMethods.ConsumerRecordOffset(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            long timestamp = NativeMethods.ConsumerRecordTimestamp(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TimestampType timestampType = TimestampTypeOf(record);
            IntPtr key = NativeMethods.ConsumerRecordKey(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.ConsumerRecordValue(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return new RecordFields(offset, timestamp, timestampType, key, value);
        }

        private TimestampType TimestampTypeOf(IntPtr record)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr pointer = NativeMethods.ConsumerRecordTimestampType(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (pointer == IntPtr.Zero)
            {
                return TimestampType.NO_TIMESTAMP_TYPE;
            }

            if (pointer != _timestampTypePointer)
            {
                int value = NativeMethods.TimestampTypeEnum(pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                _timestampType = ToTimestampType(value);
                _timestampTypePointer = pointer;
            }

            return _timestampType;
        }

        private static unsafe bool SameUtf8(IntPtr a, IntPtr b)
        {
            if (a == b)
            {
                return true;
            }

            if (a == IntPtr.Zero || b == IntPtr.Zero)
            {
                return false;
            }

            byte* left = (byte*)a;
            byte* right = (byte*)b;
            int i = 0;
            while (left[i] == right[i])
            {
                if (left[i] == 0)
                {
                    return true;
                }

                i++;
            }

            return false;
        }
    }
    // One record's typed key and value, from its C record, added to `records`; an error stops the copy-out at that
    // record. Implemented by structs only, so the walk calls it without an interface dispatch (see CopyOut).
    private interface IRecordCopy<TKey, TValue>
    {
        // Whether the batch outlives the copy-out, owned by a SafeConsumerRecordsHandle.
        bool KeepsBatch { get; }

        RecordDeserializationException? Copy(
            ref RecordReader reader,
            IntPtr record,
            string topic,
            int partition,
            SafeConsumerRecordsHandle? batch,
            List<ConsumerRecord<TKey, TValue>> records);
    }

    // The real consumers' source: the slots hold the key and value bytes, read through the deserializers.
    private readonly struct DeserializingCopy<TKey, TValue> : IRecordCopy<TKey, TValue>
    {
        private readonly IDeserializer<TKey> _keyDeserializer;
        private readonly IDeserializerWithHeaders<TKey>? _keyWithHeaders;
        private readonly IDeserializerWithReadOnlyMemory<TKey>? _keyWithMemory;
        private readonly IDeserializer<TValue> _valueDeserializer;
        private readonly IDeserializerWithHeaders<TValue>? _valueWithHeaders;
        private readonly IDeserializerWithReadOnlyMemory<TValue>? _valueWithMemory;

        internal DeserializingCopy(
            IDeserializer<TKey> keyDeserializer,
            IDeserializerWithHeaders<TKey>? keyWithHeaders,
            IDeserializerWithReadOnlyMemory<TKey>? keyWithMemory,
            IDeserializer<TValue> valueDeserializer,
            IDeserializerWithHeaders<TValue>? valueWithHeaders,
            IDeserializerWithReadOnlyMemory<TValue>? valueWithMemory)
        {
            _keyDeserializer = keyDeserializer;
            _keyWithHeaders = keyWithHeaders;
            _keyWithMemory = keyWithMemory;
            _valueDeserializer = valueDeserializer;
            _valueWithHeaders = valueWithHeaders;
            _valueWithMemory = valueWithMemory;
        }

        // A memory form keeps the batch alive behind the memories it is given.
        public bool KeepsBatch => _keyWithMemory is not null || _valueWithMemory is not null;

        public RecordDeserializationException? Copy(
            ref RecordReader reader,
            IntPtr record,
            string topic,
            int partition,
            SafeConsumerRecordsHandle? batch,
            List<ConsumerRecord<TKey, TValue>> records) =>
            reader.Copy(
                record, topic, partition, _keyDeserializer, _keyWithHeaders, _keyWithMemory, _valueDeserializer,
                _valueWithHeaders, _valueWithMemory, batch, records);
    }

    // The mock consumer's source: the slots hold ids, never dereferenced; the key and value are taken from the
    // mock's table.
    private readonly struct ResolvingCopy<TKey, TValue> : IRecordCopy<TKey, TValue>
    {
        private readonly MockRecordSlots<TKey, TValue> _slots;

        internal ResolvingCopy(MockRecordSlots<TKey, TValue> slots)
        {
            _slots = slots;
        }

        public bool KeepsBatch => false;

        public RecordDeserializationException? Copy(
            ref RecordReader reader,
            IntPtr record,
            string topic,
            int partition,
            SafeConsumerRecordsHandle? batch,
            List<ConsumerRecord<TKey, TValue>> records)
        {
            reader.Resolve(record, topic, partition, _slots, records);
            return null;
        }
    }
}
