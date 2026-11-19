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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The record-header copy-out of the mock consumer's poll: a record without headers has an empty, non-null list, and
/// a record with headers hands them out in their order, each key decoded from UTF-8 and each value copied, a null
/// value as <see langword="null"/> and an empty one as an empty array.
/// </summary>
/// <remarks>
/// The mock consumer's <c>AddRecord</c> builds its record with ConsumerRecord_new, which takes no headers, so the
/// record with headers is built here through ConsumerRecord_with_options and added with MockConsumer_add_record.
/// </remarks>
public sealed class ConsumerPollHeadersTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "hdr-topic";
    private const int Partition = 0;
    private const string NonAsciiKey = "clé-ключ-鍵-🔑";

    private static readonly byte[] s_valueWithNul = { 0x01, 0x00, 0xFF };

    private delegate void NativeDestroy(IntPtr self, ref IntPtr outErr);

    [Fact]
    public async Task PollWithCallback_RecordWithoutHeaders_HasEmptyHeaders()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(Topic, Partition, offset: 0);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);

        ConsumerRecords<byte[], byte[]> records = await consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray().Deserializer(), null, Serdes.ByteArray().Deserializer(), null);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.NotNull(record.Headers());
        Assert.Empty(record.Headers());
    }

    [Fact]
    public async Task PollWithCallback_RecordWithHeaders_CopiesEachHeaderInOrder()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(Topic, Partition, offset: 0);
        using IDisposable valueBlock = AddRecordWithHeaders(
            consumer,
            Topic,
            Partition,
            offset: 0,
            key: null,
            value: null,
            ("null-value", null),
            (NonAsciiKey, s_valueWithNul),
            ("empty-value", Array.Empty<byte>()));

        ConsumerRecords<byte[], byte[]> records = await consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray().Deserializer(), null, Serdes.ByteArray().Deserializer(), null);

        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Collection(
            record.Headers(),
            header =>
            {
                Assert.Equal("null-value", header.Key());
                Assert.Null(header.Value());
            },
            header =>
            {
                Assert.Equal(NonAsciiKey, header.Key());
                Assert.Equal(s_valueWithNul, header.Value());
            },
            header =>
            {
                Assert.Equal("empty-value", header.Key());
                Assert.NotNull(header.Value());
                Assert.Empty(header.Value()!);
            });
    }

    // Adds a record with this key, value and headers to a mock. The mock shares the key's and value's kafka_Bytes_t
    // until the records holding them are destroyed, so the blocks are freed by the object returned, disposed after the
    // consumer.
    internal static IDisposable AddRecordWithHeaders(
        NativeConsumer consumer,
        string topic,
        int partition,
        long offset,
        byte[]? key,
        byte[]? value,
        params (string Key, byte[]? Value)[] headers)
    {
        IntPtr outErr = IntPtr.Zero;
        UnmanagedBytes keyBlock = new UnmanagedBytes(key);
        UnmanagedBytes valueBlock = new UnmanagedBytes(value, keyBlock);
        IntPtr recordHeaders = TestNativeMethods.RecordHeadersNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr builder = IntPtr.Zero;
        IntPtr options = IntPtr.Zero;
        IntPtr record = IntPtr.Zero;
        try
        {
            IntPtr view = NativeMethods.RecordHeadersAsHeaders(recordHeaders, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach ((string headerKey, byte[]? headerValue) in headers)
            {
                AddHeader(view, headerKey, headerValue);
            }

            builder = TestNativeMethods.OptionsBuilderNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            using (Utf8Marshal.PinnedUtf8String pinnedTopic = Utf8Marshal.Pin(topic))
            {
                TestNativeMethods.OptionsBuilderSetTopic(builder, pinnedTopic.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            TestNativeMethods.OptionsBuilderSetPartition(builder, partition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TestNativeMethods.OptionsBuilderSetOffset(builder, offset, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TestNativeMethods.OptionsBuilderSetKey(builder, keyBlock.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TestNativeMethods.OptionsBuilderSetValue(builder, valueBlock.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            TestNativeMethods.OptionsBuilderSetHeaders(builder, recordHeaders, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            options = TestNativeMethods.OptionsBuilderBuild(builder, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            record = TestNativeMethods.ConsumerRecordWithOptions(options, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            SafeConsumerHandle handle = consumer.Handle;
            bool added = false;
            try
            {
                NativeMethods.MockConsumerAddRecord(handle.DangerousAddRefClass(ref added), record, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
            finally
            {
                if (added)
                {
                    handle.DangerousRelease();
                }
            }
        }
        catch
        {
            valueBlock.Dispose();
            throw;
        }
        finally
        {
            Destroy(NativeMethods.ConsumerRecordDestroy, record);
            Destroy(TestNativeMethods.OptionsDestroy, options);
            Destroy(TestNativeMethods.OptionsBuilderDestroy, builder);
            Destroy(TestNativeMethods.RecordHeadersDestroy, recordHeaders);
        }

        return valueBlock;
    }

    private static void AddHeader(IntPtr headers, string key, byte[]? value)
    {
        IntPtr outErr = IntPtr.Zero;
        using Utf8Marshal.PinnedUtf8String pinnedKey = Utf8Marshal.Pin(key);
        if (value is null)
        {
            TestNativeMethods.HeadersAddWithKeyValue(headers, pinnedKey.Pointer, default, ref outErr);
        }
        else
        {
            GCHandle pinnedValue = GCHandle.Alloc(value.Length == 0 ? new byte[1] : value, GCHandleType.Pinned);
            try
            {
                KafkaBytes bytes = new KafkaBytes(pinnedValue.AddrOfPinnedObject(), value.Length);
                TestNativeMethods.HeadersAddWithKeyValue(headers, pinnedKey.Pointer, bytes, ref outErr);
            }
            finally
            {
                pinnedValue.Free();
            }
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static void Destroy(NativeDestroy destroy, IntPtr self)
    {
        IntPtr outErr = IntPtr.Zero;
        destroy(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    /// <summary>
    /// A kafka_Bytes_t and its bytes in unmanaged memory, or a null pointer for no bytes; freed once, with the block it
    /// was given to free after it.
    /// </summary>
    private sealed class UnmanagedBytes : IDisposable
    {
        private readonly IDisposable? _next;

        private IntPtr _data;

        internal UnmanagedBytes(byte[]? bytes, IDisposable? next = null)
        {
            _next = next;
            if (bytes is null)
            {
                return;
            }

            _data = Marshal.AllocHGlobal(Math.Max(bytes.Length, 1));
            Marshal.Copy(bytes, 0, _data, bytes.Length);
            Pointer = Marshal.AllocHGlobal(Marshal.SizeOf<KafkaBytes>());
            Marshal.StructureToPtr(new KafkaBytes(_data, bytes.Length), Pointer, fDeleteOld: false);
        }

        internal IntPtr Pointer { get; private set; }

        public void Dispose()
        {
            if (Pointer != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(Pointer);
                Pointer = IntPtr.Zero;
            }

            if (_data != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_data);
                _data = IntPtr.Zero;
            }

            _next?.Dispose();
        }
    }

    /// <summary>
    /// The record-headers and record-options entry points the binding does not import. The test owns and destroys
    /// the headers, the builder (which build consumes), the options and the record; set_headers, add_with_key_value,
    /// with_options and add_record copy what they are given, and the Headers view is borrowed from the headers.
    /// </summary>
    private static class TestNativeMethods
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_common_header_internals_RecordHeaders_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr RecordHeadersNew(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_header_internals_RecordHeaders_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void RecordHeadersDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_header_Headers_add_with_key_value", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void HeadersAddWithKeyValue(IntPtr self, IntPtr key, KafkaBytes value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr OptionsBuilderNew(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_topic_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderSetTopic(IntPtr self, IntPtr topic, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_partition_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderSetPartition(IntPtr self, int partition, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_offset_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderSetOffset(IntPtr self, long offset, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_key_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderSetKey(IntPtr self, IntPtr key, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_value_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderSetValue(IntPtr self, IntPtr value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_set_headers_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderSetHeaders(IntPtr self, IntPtr headers, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_build", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr OptionsBuilderBuild(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptionsBuilder_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsBuilderDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecordOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_with_options_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerRecordWithOptions(IntPtr options, ref IntPtr outErr);
    }
}
