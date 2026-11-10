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
using System.Runtime.InteropServices;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The native record the send builds through the record OptionsBuilder, read back: its topic, partition, timestamp,
/// key, value and headers, a header's null value as <see langword="null"/>, an empty one as an empty array, and a
/// non-ASCII or long key decoded unchanged; a record without headers has none.
/// </summary>
public sealed class ProducerRecordBuildTests
{
    private const string NonAsciiTopic = "tópico-ключ";
    private const string NonAsciiKey = "clé-ключ-鍵-🔑";

    private static readonly string s_longKey = new string('k', 400) + "-é";

    private static readonly byte[] s_valueWithNul = { 0x01, 0x00, 0xFF };

    [Fact]
    public void BuildRecord_WithHeaders_ReadsBackEveryFieldAndEachHeaderInOrder()
    {
        Headers headers = new Headers()
            .Add("null-value", null)
            .Add("empty-value", Array.Empty<byte>())
            .Add(NonAsciiKey, s_valueWithNul)
            .Add("dup", Bytes("a"))
            .Add("dup", Bytes("b"))
            .Add(s_longKey, Bytes("long"));
        byte[] key = Bytes("key");
        byte[] value = Bytes("value");
        using NativeBytes keyBytes = new NativeBytes(key);
        using NativeBytes valueBytes = new NativeBytes(value);
        SerializedProducerRecord record = new SerializedProducerRecord(NonAsciiTopic, 3, 42L, key, value, headers);

        IntPtr native = Build(record, keyBytes.Pointer, valueBytes.Pointer);
        try
        {
            Assert.Equal(NonAsciiTopic, Topic(native));
            Assert.Equal(3, Read(native, RecordNative.Partition));
            Assert.Equal(42L, Read(native, RecordNative.Timestamp));
            Assert.Equal(keyBytes.Pointer, Read(native, RecordNative.Key));
            Assert.Equal(valueBytes.Pointer, Read(native, RecordNative.Value));

            Assert.Collection(
                HeadersOf(native),
                header => AssertHeader("null-value", null, header),
                header => AssertHeader("empty-value", Array.Empty<byte>(), header),
                header => AssertHeader(NonAsciiKey, s_valueWithNul, header),
                header => AssertHeader("dup", Bytes("a"), header),
                header => AssertHeader("dup", Bytes("b"), header),
                header => AssertHeader(s_longKey, Bytes("long"), header));
        }
        finally
        {
            Destroy(native);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BuildRecord_WithoutHeaders_HasNone_AndJavasNullsForTheUnsetFields(bool emptyHeaders)
    {
        SerializedProducerRecord record = new SerializedProducerRecord(
            "plain", partition: null, timestamp: null, key: null, value: null, emptyHeaders ? new Headers() : null);

        IntPtr native = Build(record, IntPtr.Zero, IntPtr.Zero);
        try
        {
            Assert.Equal("plain", Topic(native));
            Assert.Equal(-1, Read(native, RecordNative.Partition));
            Assert.Equal(-1L, Read(native, RecordNative.Timestamp));
            Assert.Equal(IntPtr.Zero, Read(native, RecordNative.Key));
            Assert.Equal(IntPtr.Zero, Read(native, RecordNative.Value));
            Assert.Empty(HeadersOf(native));
        }
        finally
        {
            Destroy(native);
        }
    }

    [Fact]
    public void BuildRecord_AHeadersRemovedToEmpty_SendsNoHeaders()
    {
        Headers headers = new Headers().Add("gone", Bytes("x")).Remove("gone");
        byte[] value = Bytes("v");
        using NativeBytes valueBytes = new NativeBytes(value);
        SerializedProducerRecord record = new SerializedProducerRecord("t", 0, null, null, value, headers);

        IntPtr native = Build(record, IntPtr.Zero, valueBytes.Pointer);
        try
        {
            Assert.Empty(HeadersOf(native));
        }
        finally
        {
            Destroy(native);
        }
    }

    internal static IntPtr Build(in SerializedProducerRecord record, IntPtr key, IntPtr value)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = ProducerSendMarshal.BuildRecord(in record, key, value, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, native);
        return native;
    }

    private static string? Topic(IntPtr native)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr topic = RecordNative.Topic(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return Utf8Marshal.PtrToString(topic);
    }

    internal static T Read<T>(IntPtr native, Getter<T> getter)
    {
        IntPtr outErr = IntPtr.Zero;
        T result = getter(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return result;
    }

    private static List<(string? Key, byte[]? Value)> HeadersOf(IntPtr native)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr recordHeaders = RecordNative.Headers(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, recordHeaders);
        IntPtr view = NativeMethods.RecordHeadersAsHeaders(recordHeaders, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr array = NativeMethods.HeadersToArray(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(array, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<(string? Key, byte[]? Value)> headers = new List<(string? Key, byte[]? Value)>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr element = NativeMethods.ListGet(array, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr header = NativeMethods.RecordHeaderAsHeader(element, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = NativeMethods.HeaderKey(header, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                KafkaBytes value = NativeMethods.HeaderValue(header, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                headers.Add((Utf8Marshal.PtrToString(key), CopyBytes(value)));
            }

            return headers;
        }
        finally
        {
            NativeMethods.ListDestroy(array, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    private static byte[]? CopyBytes(KafkaBytes bytes)
    {
        if (bytes.Data == IntPtr.Zero)
        {
            return null;
        }

        byte[] copy = new byte[bytes.Length];
        Marshal.Copy(bytes.Data, copy, 0, copy.Length);
        return copy;
    }

    private static void AssertHeader(string key, byte[]? value, (string? Key, byte[]? Value) header)
    {
        Assert.Equal(key, header.Key);
        if (value is null)
        {
            Assert.Null(header.Value);
        }
        else
        {
            Assert.NotNull(header.Value);
            Assert.Equal(value, header.Value);
        }
    }

    internal static void Destroy(IntPtr native)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ProducerRecordDestroy(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    internal delegate T Getter<T>(IntPtr self, ref IntPtr outErr);

    /// <summary>A <c>kafka_Bytes_t</c> and a copy of its bytes in native memory, as the send passes a key or value.</summary>
    private sealed class NativeBytes : IDisposable
    {
        private readonly IntPtr _data;

        internal NativeBytes(byte[] bytes)
        {
            _data = Marshal.AllocHGlobal(Math.Max(bytes.Length, 1));
            Marshal.Copy(bytes, 0, _data, bytes.Length);
            Pointer = Marshal.AllocHGlobal(Marshal.SizeOf<KafkaBytes>());
            Marshal.StructureToPtr(new KafkaBytes(_data, bytes.Length), Pointer, fDeleteOld: false);
        }

        internal IntPtr Pointer { get; }

        public void Dispose()
        {
            Marshal.FreeHGlobal(Pointer);
            Marshal.FreeHGlobal(_data);
        }
    }

    /// <summary>The native record's getters, which the producer interop tests read back.</summary>
    internal static class RecordNative
    {
        private const string DllName = "confluent_kafka";

        internal static readonly Getter<int> Partition = PartitionOf;
        internal static readonly Getter<long> Timestamp = TimestampOf;
        internal static readonly Getter<IntPtr> Key = KeyOf;
        internal static readonly Getter<IntPtr> Value = ValueOf;

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_topic_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Topic(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_headers_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Headers(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_partition_err", CallingConvention = CallingConvention.Cdecl)]
        private static extern int PartitionOf(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
        private static extern long TimestampOf(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_key_err", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr KeyOf(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_value_err", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ValueOf(IntPtr self, ref IntPtr outErr);
    }
}
