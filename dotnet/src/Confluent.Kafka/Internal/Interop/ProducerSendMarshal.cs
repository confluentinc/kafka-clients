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

using Confluent.Kafka.Common.Header;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds a send's <c>kafka_producer_ProducerRecord_t</c> through the record OptionsBuilder, over the record's own key
/// and value bytes and with copies of its headers, then its <c>kafka_producer_Callback_t</c>, which the send consumes,
/// and hands both to the producer: the blocking send borrows the key and value for the call, the async send until its
/// hand-over.
/// </summary>
internal static unsafe class ProducerSendMarshal
{
    private const int MaxStackStringBytes = 1024;

    private const string RecordParameter = "record";

    internal static IntPtr Send(
        SafeProducerHandle producer,
        in SerializedProducerRecord record,
        DeliveryState state,
        ref IntPtr outErr,
        out bool sent)
    {
        sent = false;
        RejectAlteredStrings(in record);
        ReadOnlySpan<byte> key = record.Key.HasValue ? record.Key.GetValueOrDefault().Span : default;
        ReadOnlySpan<byte> value = record.Value.HasValue ? record.Value.GetValueOrDefault().Span : default;
        byte empty = 0;
        fixed (byte* keyData = key)
        fixed (byte* valueData = value)
        {
            KafkaBytes keyBytes = new KafkaBytes(key.IsEmpty ? (IntPtr)(&empty) : (IntPtr)keyData, key.Length);
            KafkaBytes valueBytes = new KafkaBytes(value.IsEmpty ? (IntPtr)(&empty) : (IntPtr)valueData, value.Length);
            IntPtr nativeRecord = BuildRecord(
                in record,
                record.Key.HasValue ? (IntPtr)(&keyBytes) : IntPtr.Zero,
                record.Value.HasValue ? (IntPtr)(&valueBytes) : IntPtr.Zero,
                ref outErr);
            if (nativeRecord == IntPtr.Zero)
            {
                return IntPtr.Zero;
            }

            try
            {
                IntPtr callback = state.Register();
                sent = true;
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
        DeliveryState state,
        NativeMethods.ProducerSendCallback handOver,
        IntPtr opaque,
        ref IntPtr outErr)
    {
        RejectAlteredStrings(in record);
        pins.Pin(record.Key, record.Value, out IntPtr key, out IntPtr value);
        IntPtr nativeRecord = BuildRecord(in record, key, value, ref outErr);
        if (nativeRecord == IntPtr.Zero)
        {
            return false;
        }

        try
        {
            IntPtr callback = state.Register();
            NativeMethods.ProducerSendWithCallbackCb(producer, nativeRecord, callback, handOver, opaque);
        }
        finally
        {
            Destroy(nativeRecord);
        }

        return true;
    }

    // Java sends a NUL in a topic or a header key whole, with no error; the C ABI would cut the string at the NUL, so
    // the string is rejected before anything is pinned. An unpaired surrogate crosses as '?', as Java writes it.
    // Python's mocks, pure Python, accept such a record; the mocks here build it through the C ABI and reject it too.
    private static void RejectAlteredStrings(in SerializedProducerRecord record)
    {
        AdminStrings.Validate(record.Topic, AdminStrings.InvalidTopicMessage, RecordParameter);
        List<Header>? entries = record.Headers?.Entries;
        if (entries is null)
        {
            return;
        }

        for (int i = 0; i < entries.Count; i++)
        {
            AdminStrings.Validate(entries[i].Key, AdminStrings.InvalidHeaderKeyMessage, RecordParameter);
        }
    }

    internal static IntPtr BuildRecord(in SerializedProducerRecord record, IntPtr key, IntPtr value, ref IntPtr outErr)
    {
        IntPtr builder = NativeMethods.ProducerRecordOptionsBuilderNew(ref outErr);
        if (builder == IntPtr.Zero)
        {
            return IntPtr.Zero;
        }

        IntPtr headers = IntPtr.Zero;
        IntPtr options;
        try
        {
            if (!SetFields(builder, in record, key, value, ref headers, ref outErr))
            {
                return IntPtr.Zero;
            }

            options = NativeMethods.ProducerRecordOptionsBuilderBuild(builder, ref outErr);
            if (options == IntPtr.Zero)
            {
                return IntPtr.Zero;
            }
        }
        finally
        {
            DestroyBuilder(builder);
            if (headers != IntPtr.Zero)
            {
                DestroyHeaders(headers);
            }
        }

        try
        {
            return NativeMethods.ProducerRecordWithOptions(options, ref outErr);
        }
        finally
        {
            DestroyOptions(options);
        }
    }

    private static bool SetFields(
        IntPtr builder,
        in SerializedProducerRecord record,
        IntPtr key,
        IntPtr value,
        ref IntPtr headers,
        ref IntPtr outErr)
    {
        SetTopic(builder, record.Topic, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            return false;
        }

        NativeMethods.ProducerRecordOptionsBuilderSetPartition(builder, record.Partition ?? -1, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            return false;
        }

        NativeMethods.ProducerRecordOptionsBuilderSetTimestamp(builder, record.Timestamp ?? -1, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            return false;
        }

        NativeMethods.ProducerRecordOptionsBuilderSetKey(builder, key, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            return false;
        }

        NativeMethods.ProducerRecordOptionsBuilderSetValue(builder, value, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            return false;
        }

        List<Header>? entries = record.Headers?.Entries;
        if (entries is null || entries.Count == 0)
        {
            return true;
        }

        headers = NativeMethods.RecordHeadersNew(ref outErr);
        if (headers == IntPtr.Zero)
        {
            return false;
        }

        IntPtr view = NativeMethods.RecordHeadersAsHeaders(headers, ref outErr);
        for (int i = 0; outErr == IntPtr.Zero && i < entries.Count; i++)
        {
            AddHeader(view, entries[i], ref outErr);
        }

        if (outErr != IntPtr.Zero)
        {
            return false;
        }

        NativeMethods.ProducerRecordOptionsBuilderSetHeaders(builder, headers, ref outErr);
        return outErr == IntPtr.Zero;
    }

    private static void SetTopic(IntPtr builder, string topic, ref IntPtr outErr)
    {
        int capacity = Utf8Marshal.QuestionMarkUtf8.GetMaxByteCount(topic.Length) + 1;
        if (capacity <= MaxStackStringBytes)
        {
            byte* buffer = stackalloc byte[capacity];
            WriteUtf8(topic, buffer, capacity);
            NativeMethods.ProducerRecordOptionsBuilderSetTopic(builder, (IntPtr)buffer, ref outErr);
            return;
        }

        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(topic);
        NativeMethods.ProducerRecordOptionsBuilderSetTopic(builder, pinned.Pointer, ref outErr);
    }

    // Headers_add_with_key_value copies the key and the value, so both are pinned for the call only. An empty value
    // crosses as a non-null pointer: fixed over an empty array yields NULL, which the ABI reads as a null value.
    private static void AddHeader(IntPtr view, Header header, ref IntPtr outErr)
    {
        byte[]? value = header.Value;
        byte empty = 0;
        fixed (byte* valueData = value)
        {
            KafkaBytes valueBytes = value is null
                ? default
                : new KafkaBytes(value.Length == 0 ? (IntPtr)(&empty) : (IntPtr)valueData, value.Length);
            string key = header.Key;
            int capacity = Utf8Marshal.QuestionMarkUtf8.GetMaxByteCount(key.Length) + 1;
            if (capacity <= MaxStackStringBytes)
            {
                byte* buffer = stackalloc byte[capacity];
                WriteUtf8(key, buffer, capacity);
                NativeMethods.HeadersAddWithKeyValue(view, (IntPtr)buffer, valueBytes, ref outErr);
                return;
            }

            using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(key);
            NativeMethods.HeadersAddWithKeyValue(view, pinned.Pointer, valueBytes, ref outErr);
        }
    }

    private static void WriteUtf8(string value, byte* buffer, int capacity)
    {
        int length;
        fixed (char* chars = value)
        {
            length = Utf8Marshal.QuestionMarkUtf8.GetBytes(chars, value.Length, buffer, capacity - 1);
        }

        buffer[length] = 0;
    }

    private static void DestroyBuilder(IntPtr builder)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ProducerRecordOptionsBuilderDestroy(builder, ref outErr);
        FreeError(outErr);
    }

    private static void DestroyHeaders(IntPtr headers)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.RecordHeadersDestroy(headers, ref outErr);
        FreeError(outErr);
    }

    private static void DestroyOptions(IntPtr options)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ProducerRecordOptionsDestroy(options, ref outErr);
        FreeError(outErr);
    }

    private static void Destroy(IntPtr nativeRecord)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ProducerRecordDestroy(nativeRecord, ref outErr);
        FreeError(outErr);
    }

    private static void FreeError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }
}
