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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Record;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Reads one field of a native <c>kafka_consumer_ConsumerRecord_t</c> of a polled batch, one call each: the getters
/// of a <see cref="Consumer.ConsumerRecord{TKey, TValue}"/> that wraps the batch, and the copy-out's cold paths. The
/// caller keeps the batch alive across the call.
/// </summary>
internal static class ConsumerRecordMarshal
{
    // The core's TimestampType values are static singletons, so each pointer is read once per process. A slot only
    // ever holds the pointer of its own value, so the unsynchronized writes race benignly.
    private static IntPtr s_noTimestampType;
    private static IntPtr s_createTime;
    private static IntPtr s_logAppendTime;

    internal static long Offset(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        long offset = NativeMethods.ConsumerRecordOffset(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return offset;
    }

    internal static long Timestamp(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        long timestamp = NativeMethods.ConsumerRecordTimestamp(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return timestamp;
    }

    internal static TimestampType TimestampTypeOf(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr pointer = NativeMethods.ConsumerRecordTimestampType(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (pointer == IntPtr.Zero || pointer == s_noTimestampType)
        {
            return TimestampType.NO_TIMESTAMP_TYPE;
        }

        if (pointer == s_createTime)
        {
            return TimestampType.CREATE_TIME;
        }

        if (pointer == s_logAppendTime)
        {
            return TimestampType.LOG_APPEND_TIME;
        }

        int value = NativeMethods.TimestampTypeEnum(pointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        switch (value)
        {
            case 1:
                s_createTime = pointer;
                return TimestampType.CREATE_TIME;
            case 2:
                s_logAppendTime = pointer;
                return TimestampType.LOG_APPEND_TIME;
            default:
                s_noTimestampType = pointer;
                return TimestampType.NO_TIMESTAMP_TYPE;
        }
    }

    internal static int SerializedKeySize(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        int size = NativeMethods.ConsumerRecordSerializedKeySize(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return size;
    }

    internal static int SerializedValueSize(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        int size = NativeMethods.ConsumerRecordSerializedValueSize(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return size;
    }

    // -1 is the core's Optional.empty().
    internal static int? LeaderEpoch(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        int leaderEpoch = NativeMethods.ConsumerRecordLeaderEpoch(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return leaderEpoch < 0 ? null : leaderEpoch;
    }

    // -1 is the core's Optional.empty().
    internal static short? DeliveryCount(IntPtr record)
    {
        IntPtr outErr = IntPtr.Zero;
        short deliveryCount = NativeMethods.ConsumerRecordDeliveryCount(record, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return deliveryCount < 0 ? null : deliveryCount;
    }
}
