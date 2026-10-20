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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds a send's <c>kafka_producer_ProducerRecord_t</c> over the record's own bytes and hands it to the producer:
/// the blocking send borrows them for the call, the async send until its hand-over.
/// </summary>
internal static unsafe class ProducerSendMarshal
{
    private const int MaxStackTopicBytes = 1024;

    internal static IntPtr Send(
        SafeProducerHandle producer,
        in SerializedProducerRecord record,
        IntPtr callback,
        ref IntPtr outErr,
        out bool sent)
    {
        sent = false;
        ReadOnlySpan<byte> key = record.Key.HasValue ? record.Key.GetValueOrDefault().Span : default;
        ReadOnlySpan<byte> value = record.Value.HasValue ? record.Value.GetValueOrDefault().Span : default;
        byte empty = 0;
        fixed (byte* keyData = key)
        fixed (byte* valueData = value)
        {
            KafkaBytes keyBytes = new KafkaBytes(key.IsEmpty ? (IntPtr)(&empty) : (IntPtr)keyData, key.Length);
            KafkaBytes valueBytes = new KafkaBytes(value.IsEmpty ? (IntPtr)(&empty) : (IntPtr)valueData, value.Length);
            IntPtr nativeRecord = NewRecord(
                in record,
                record.Key.HasValue ? (IntPtr)(&keyBytes) : IntPtr.Zero,
                record.Value.HasValue ? (IntPtr)(&valueBytes) : IntPtr.Zero,
                ref outErr);
            if (nativeRecord == IntPtr.Zero)
            {
                return IntPtr.Zero;
            }

            sent = true;
            try
            {
                return NativeMethods.ProducerSendWithCallback(producer, nativeRecord, callback, ref outErr);
            }
            finally
            {
                Destroy(nativeRecord);
            }
        }
    }

    internal static bool SendWithHandOver(
        SafeProducerHandle producer,
        in SerializedProducerRecord record,
        ref SendPins pins,
        IntPtr callback,
        NativeMethods.ProducerSendCallback handOver,
        IntPtr opaque,
        ref IntPtr outErr)
    {
        pins.Pin(record.Key, record.Value, out IntPtr key, out IntPtr value);
        IntPtr nativeRecord = NewRecord(in record, key, value, ref outErr);
        if (nativeRecord == IntPtr.Zero)
        {
            return false;
        }

        try
        {
            NativeMethods.ProducerSendWithCallbackCb(producer, nativeRecord, callback, handOver, opaque);
        }
        finally
        {
            Destroy(nativeRecord);
        }

        return true;
    }

    private static IntPtr NewRecord(in SerializedProducerRecord record, IntPtr key, IntPtr value, ref IntPtr outErr)
    {
        string topic = record.Topic;
        int partition = record.Partition ?? -1;
        long timestamp = record.Timestamp ?? -1;
        int capacity = Encoding.UTF8.GetMaxByteCount(topic.Length) + 1;
        if (capacity <= MaxStackTopicBytes)
        {
            byte* buffer = stackalloc byte[capacity];
            int length;
            fixed (char* chars = topic)
            {
                length = Encoding.UTF8.GetBytes(chars, topic.Length, buffer, capacity - 1);
            }

            buffer[length] = 0;
            return NativeMethods.ProducerRecordWithPartitionTimestampKey(
                (IntPtr)buffer, partition, timestamp, key, value, ref outErr);
        }

        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(topic);
        return NativeMethods.ProducerRecordWithPartitionTimestampKey(
            pinned.Pointer, partition, timestamp, key, value, ref outErr);
    }

    private static void Destroy(IntPtr nativeRecord)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ProducerRecordDestroy(nativeRecord, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
