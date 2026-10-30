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
using System.Globalization;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a native poll batch into owned <see cref="ConsumerRecords{TKey, TValue}"/>, deserializing each key and
/// value from a span over the batch, and stops at the first record that fails to deserialize.
/// </summary>
internal static class ConsumerRecordsMarshal
{
    private static readonly IReadOnlyList<KeyValuePair<TopicPartition, long>> s_noRewind =
        Array.Empty<KeyValuePair<TopicPartition, long>>();

    // On a failure, the records before the failing one are returned, and rewind holds each partition's first
    // record not returned: the failing one, and the first record of every later partition in the batch.
    internal static ConsumerRecords<TKey, TValue> CopyOut<TKey, TValue>(
        IntPtr records,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        out RecordDeserializationException? error,
        out IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind)
    {
        error = null;
        rewind = s_noRewind;
        if (records == IntPtr.Zero)
        {
            return Empty<TKey, TValue>();
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ConsumerRecordsCount(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (count <= 0)
        {
            return Empty<TKey, TValue>();
        }

        List<ConsumerRecord<TKey, TValue>> list = new List<ConsumerRecord<TKey, TValue>>(count);
        IntPtr partitions = NativeMethods.ConsumerRecordsPartitions(records, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int partitionCount = Size(partitions);
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
                    for (int i = 0; i < size; i++)
                    {
                        IntPtr record = Get(partitionRecords, i);
                        error = reader.Copy(record, topic, partitionId, keyDeserializer, valueDeserializer, list);
                        if (error is not null)
                        {
                            rewind = Rewind(records, partitions, p, error);
                            return new ConsumerRecords<TKey, TValue>(list);
                        }
                    }
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

        return new ConsumerRecords<TKey, TValue>(list);
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

    private static ConsumerRecords<TKey, TValue> Empty<TKey, TValue>() =>
        new ConsumerRecords<TKey, TValue>(Array.Empty<ConsumerRecord<TKey, TValue>>());

    private static List<KeyValuePair<TopicPartition, long>> Rewind(
        IntPtr records,
        IntPtr partitions,
        int failedIndex,
        RecordDeserializationException error)
    {
        List<KeyValuePair<TopicPartition, long>> rewind = new List<KeyValuePair<TopicPartition, long>>
        {
            new KeyValuePair<TopicPartition, long>(error.TopicPartition, error.Offset),
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

    private static Headers CopyHeaders(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr recordHeaders = NativeMethods.ConsumerRecordHeaders(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (recordHeaders == IntPtr.Zero)
        {
            return Headers.Empty;
        }

        IntPtr view = NativeMethods.RecordHeadersAsHeaders(recordHeaders, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr array = NativeMethods.HeadersToArray(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (array == IntPtr.Zero)
        {
            return Headers.Empty;
        }

        try
        {
            int count = Size(array);
            if (count <= 0)
            {
                return Headers.Empty;
            }

            List<Header> headers = new List<Header>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr header = NativeMethods.RecordHeaderAsHeader(Get(array, i), ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = NativeMethods.HeaderKey(header, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                KafkaBytes value = NativeMethods.HeaderValue(header, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                headers.Add(new Header(Utf8Marshal.PtrToString(key) ?? string.Empty, CopyBytes(value)));
            }

            return new Headers(headers);
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

    private static unsafe T Deserialize<T>(IDeserializer<T> deserializer, string topic, IntPtr bytes)
    {
        KafkaBytes* view = (KafkaBytes*)bytes;
        return deserializer.Deserialize(topic, new ReadOnlySpan<byte>((void*)view->Data, Math.Max(view->Length, 0)));
    }

    private static RecordDeserializationException NewError(
        RecordDeserializationException.DeserializationExceptionOrigin origin,
        TopicPartition partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        IntPtr key,
        IntPtr value,
        Headers headers,
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
                + (origin == RecordDeserializationException.DeserializationExceptionOrigin.Key ? "KEY" : "VALUE")
                + " for partition " + partition.ToString() + " at offset "
                + offset.ToString(CultureInfo.InvariantCulture)
                + ". If needed, please seek past the record to continue consumption.",
            cause);

    private static TimestampType ToTimestampType(int value) => value switch
    {
        1 => TimestampType.CreateTime,
        2 => TimestampType.LogAppendTime,
        _ => TimestampType.NoTimestampType,
    };

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
            IDeserializer<TValue> valueDeserializer,
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
            Headers headers = CopyHeaders(record);

            TKey deserializedKey = default!;
            if (key != IntPtr.Zero)
            {
                try
                {
                    deserializedKey = Deserialize(keyDeserializer, topic, key);
                }
                catch (Exception cause)
                {
                    return NewError(
                        RecordDeserializationException.DeserializationExceptionOrigin.Key,
                        new TopicPartition(topic, partition), offset, timestamp, timestampType, key, value, headers, cause);
                }
            }

            TValue deserializedValue = default!;
            if (value != IntPtr.Zero)
            {
                try
                {
                    deserializedValue = Deserialize(valueDeserializer, topic, value);
                }
                catch (Exception cause)
                {
                    return NewError(
                        RecordDeserializationException.DeserializationExceptionOrigin.Value,
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
                topic, partition, offset, timestamp, timestampType, deserializedKey, deserializedValue, headers,
                leaderEpoch < 0 ? null : leaderEpoch, serializedKeySize, serializedValueSize));
            return null;
        }

        private TimestampType TimestampTypeOf(IntPtr record)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr pointer = NativeMethods.ConsumerRecordTimestampType(record, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (pointer == IntPtr.Zero)
            {
                return TimestampType.NoTimestampType;
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
}
