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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the native <c>kafka_consumer_ConsumerRecord_t</c> the mock consumer's <c>AddRecord</c> hands to the C mock,
/// from a <see cref="Consumer.ConsumerRecord{TKey, TValue}"/> whose key and value cross as slot ids.
/// </summary>
internal static class ConsumerRecordMarshal
{
    // The singleton of a TimestampType value, as the builder's set_timestamp_type takes it.
    internal static IntPtr TimestampTypePointer(TimestampType timestampType)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr pointer = timestampType switch
        {
            TimestampType.CREATE_TIME => NativeMethods.TimestampTypeCreateTime(ref outErr),
            TimestampType.LOG_APPEND_TIME => NativeMethods.TimestampTypeLogAppendTime(ref outErr),
            _ => NativeMethods.TimestampTypeNoTimestampType(ref outErr),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return pointer;
    }

    /// <summary>
    /// Builds a native record carrying every field of <paramref name="record"/>, its key and value replaced by the
    /// given slot values: the mock consumer's ids, which the core stores in the record's <c>void *</c> slots and
    /// never reads, frees or compares. The headers cross as a new native <c>RecordHeaders</c>, which the builder
    /// clones. The caller owns the returned handle and destroys it with <c>ConsumerRecordDestroy</c>.
    /// </summary>
    internal static IntPtr BuildRecord<TKey, TValue>(ConsumerRecord<TKey, TValue> record, IntPtr keySlot, IntPtr valueSlot)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr builder = NativeMethods.ConsumerRecordOptionsBuilderNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr headers = IntPtr.Zero;
        IntPtr options;
        try
        {
            SetFields(builder, record, keySlot, valueSlot, ref headers, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            options = NativeMethods.ConsumerRecordOptionsBuilderBuild(builder, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            Destroy(NativeMethods.ConsumerRecordOptionsBuilderDestroy, builder);
            if (headers != IntPtr.Zero)
            {
                Destroy(NativeMethods.RecordHeadersDestroy, headers);
            }
        }

        try
        {
            IntPtr nativeRecord = NativeMethods.ConsumerRecordWithOptions(options, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return nativeRecord;
        }
        finally
        {
            Destroy(NativeMethods.ConsumerRecordOptionsDestroy, options);
        }
    }

    // Stops at the first failure, leaving it in outErr.
    private static void SetFields<TKey, TValue>(
        IntPtr builder,
        ConsumerRecord<TKey, TValue> record,
        IntPtr keySlot,
        IntPtr valueSlot,
        ref IntPtr headers,
        ref IntPtr outErr)
    {
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(record.Topic()))
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetTopic(builder, topic.Pointer, ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetPartition(builder, record.Partition(), ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetOffset(builder, record.Offset(), ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetTimestamp(builder, record.Timestamp(), ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            IntPtr timestampType = TimestampTypePointer(record.TimestampType());
            NativeMethods.ConsumerRecordOptionsBuilderSetTimestampType(builder, timestampType, ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetSerializedKeySize(builder, record.SerializedKeySize(), ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetSerializedValueSize(
                builder, record.SerializedValueSize(), ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetKey(builder, keySlot, ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetValue(builder, valueSlot, ref outErr);
        }

        // -1 is the core's Optional.empty().
        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetLeaderEpoch(builder, record.LeaderEpoch() ?? -1, ref outErr);
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetDeliveryCount(builder, record.DeliveryCount() ?? (short)-1, ref outErr);
        }

        if (outErr != IntPtr.Zero)
        {
            return;
        }

        headers = NativeMethods.RecordHeadersNew(ref outErr);
        if (headers == IntPtr.Zero)
        {
            return;
        }

        // A user's IHeaders is copied through RecordHeaders, which reads it once and rejects a null header.
        List<IHeader>? entries = record.Headers() is RecordHeaders own
            ? own.Entries
            : new RecordHeaders(record.Headers()).Entries;
        if (entries is { Count: > 0 })
        {
            IntPtr view = NativeMethods.RecordHeadersAsHeaders(headers, ref outErr);
            for (int i = 0; outErr == IntPtr.Zero && i < entries.Count; i++)
            {
                ProducerSendMarshal.AddHeader(view, entries[i], ref outErr);
            }
        }

        if (outErr == IntPtr.Zero)
        {
            NativeMethods.ConsumerRecordOptionsBuilderSetHeaders(builder, headers, ref outErr);
        }
    }

    private delegate void DestroyFunction(IntPtr handle, ref IntPtr outErr);

    private static void Destroy(DestroyFunction destroy, IntPtr handle)
    {
        IntPtr outErr = IntPtr.Zero;
        destroy(handle, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
