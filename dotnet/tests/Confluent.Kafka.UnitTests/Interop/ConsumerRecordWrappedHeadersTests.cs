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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// A polled record's headers over the core's views of the batch: they are read on first use, once, under the
/// batch's lock; a header's key is decoded and cached on first use and its value copied once and cached, while
/// <see cref="HeaderExtensions.ValueMemory(IHeader)"/> is a view of the batch that outlives the next poll and the
/// consumer.
/// </summary>
public sealed class ConsumerRecordWrappedHeadersTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "wrapped-hdr-topic";
    private const int Partition = 0;

    private static readonly byte[] s_value = { 0x01, 0x00, 0xFF, 0x7F };

    [Fact]
    public async Task ValueMemory_IsAViewOfTheBatch_ReadableAfterTheNextPollAndAfterTheConsumerIsDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IHeader header;
        ReadOnlyMemory<byte>? memory;
        try
        {
            consumer.Assign(new[] { (Topic, Partition) });
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
            using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
                consumer, Topic, Partition, offset: 0, key: null, value: null, ("h", s_value));

            ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
            header = Assert.Single(record.Headers());
            memory = header.ValueMemory();

            Assert.NotNull(memory);
            Assert.True(MemoryMarshal.TryGetMemoryManager(memory.Value, out BytesMemoryManager? manager));
            Assert.False(MemoryMarshal.TryGetArray(memory.Value, out _));
            Assert.Equal(s_value, memory.Value.ToArray());

            Assert.Empty(await Poll(consumer));
            Assert.Equal(s_value, memory.Value.ToArray());
            Assert.False(manager!.Batch.IsClosed);
        }
        finally
        {
            consumerDisposal.Dispose();
        }

        Assert.Equal(s_value, memory.Value.ToArray());
        Assert.Equal(s_value, header.ValueMemory()!.Value.ToArray());
        Assert.Equal(s_value, header.Value());
    }

    [Fact]
    public async Task Value_IsCopiedOnce_AndReturnsTheSameArrayOnEveryCall()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null, ("h", s_value));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
        IHeader header = Assert.Single(record.Headers());

        byte[]? first = header.Value();
        Assert.Equal(s_value, first);
        Assert.Same(first, header.Value());
        Assert.Same(header.Key(), header.Key());
        Assert.Same(record.Headers(), record.Headers());
        Assert.Same(header, Assert.Single(record.Headers()));
    }

    [Fact]
    public async Task NullAndEmptyValues_AreNullAndEmpty_InBothForms()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null,
            ("null-value", null), ("empty-value", Array.Empty<byte>()));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
        IHeader[] headers = record.Headers().ToArray();

        Assert.Equal("null-value", headers[0].Key());
        Assert.Null(headers[0].ValueMemory());
        Assert.Null(headers[0].Value());
        Assert.Null(headers[0].ValueMemory());

        Assert.Equal("empty-value", headers[1].Key());
        ReadOnlyMemory<byte>? empty = headers[1].ValueMemory();
        Assert.NotNull(empty);
        Assert.True(empty.Value.IsEmpty);
        Assert.False(MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(empty.Value, out _));
        Assert.Same(Array.Empty<byte>(), headers[1].Value());
    }

    [Fact]
    public void BuiltHeader_ValueMemory_IsItsArray_OrNullForANullValue()
    {
        byte[] value = { 1, 2, 3 };
        IHeader header = new RecordHeader("k", value);

        ReadOnlyMemory<byte>? memory = header.ValueMemory();

        Assert.NotNull(memory);
        Assert.True(MemoryMarshal.TryGetArray(memory.Value, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);
        Assert.Null(new RecordHeader("k", null).ValueMemory());
    }

    [Fact]
    public async Task HeadersNoDeserializerTakes_AreNotRead_UntilTheirFirstUse()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null, ("h", s_value));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));

        Assert.Null(Field(record, "_headers"));
        IHeaders headers = record.Headers();
        Assert.IsType<SafeConsumerRecordsHandle>(Field(headers, "_state"));

        IHeader header = Assert.Single(headers);
        Assert.IsType<List<IHeader>>(Field(headers, "_state"));
        Assert.Null(Field(header, "_key"));
        Assert.Null(Field(header, "_value"));
        Assert.Equal("h", header.Key());
        Assert.Null(Field(header, "_value"));
    }

    [Fact]
    public async Task FirstUseFromManyThreads_ReadsTheHeadersOnce_AndEveryThreadSeesTheSameObjects()
    {
        const int Records = 32;
        const int Threads = 8;
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        List<IDisposable> blocks = new List<IDisposable>();
        try
        {
            for (int offset = 0; offset < Records; offset++)
            {
                blocks.Add(ConsumerPollHeadersTests.AddRecordWithHeaders(
                    consumer, Topic, Partition, offset, key: null, value: null,
                    ("a", new[] { (byte)offset }), ("b", s_value)));
            }

            ConsumerRecords<byte[], byte[]> records = await Poll(consumer);
            Assert.Equal(Records, records.Count());
            foreach (ConsumerRecord<byte[], byte[]> record in records)
            {
                (IHeaders Headers, IHeader[] Entries, string Key, byte[]? Value)[] seen =
                    new (IHeaders, IHeader[], string, byte[]?)[Threads];
                using Barrier barrier = new Barrier(Threads);
                Task[] tasks = Enumerable.Range(0, Threads).Select(i => Task.Factory.StartNew(
                    () =>
                    {
                        barrier.SignalAndWait();
                        IHeaders headers = record.Headers();
                        IHeader[] entries = headers.ToArray();
                        seen[i] = (headers, entries, entries[1].Key(), entries[1].Value());
                    },
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)).ToArray();
                await Task.WhenAll(tasks);

                foreach ((IHeaders Headers, IHeader[] Entries, string Key, byte[]? Value) one in seen)
                {
                    Assert.Same(seen[0].Headers, one.Headers);
                    Assert.Equal(2, one.Entries.Length);
                    Assert.Same(seen[0].Entries[0], one.Entries[0]);
                    Assert.Same(seen[0].Entries[1], one.Entries[1]);
                    Assert.Same(seen[0].Key, one.Key);
                    Assert.Same(seen[0].Value, one.Value);
                }

                Assert.Equal("b", seen[0].Key);
                Assert.Equal(s_value, seen[0].Value);
                Assert.Equal(new[] { (byte)record.Offset() }, seen[0].Entries[0].Value());
            }
        }
        finally
        {
            foreach (IDisposable block in blocks)
            {
                block.Dispose();
            }
        }
    }

    [Fact]
    public async Task ValueMemory_IsAViewOfTheBatchBeforeValue_AndTheArrayValueReturnsAfterIt()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null,
            ("h", s_value), ("empty-value", Array.Empty<byte>()));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
        IHeader header = record.Headers().ToArray()[0];

        ReadOnlyMemory<byte> before = header.ValueMemory()!.Value;
        Assert.True(MemoryMarshal.TryGetMemoryManager(before, out BytesMemoryManager? _));
        Assert.Equal(s_value, SpanOf(header));

        byte[] value = header.Value()!;
        ReadOnlyMemory<byte> after = header.ValueMemory()!.Value;
        Assert.True(MemoryMarshal.TryGetArray(after, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);
        Assert.Equal(0, segment.Offset);
        Assert.Equal(value.Length, segment.Count);

        // A write into the array Value() returns shows through every later form, as Java's RecordHeader keeps only
        // that array once value() has copied it; the memory taken before stays a view of the batch.
        value[0] = 0xFE;
        byte[] written = { 0xFE, 0x00, 0xFF, 0x7F };
        Assert.Equal(written, header.ValueMemory()!.Value.ToArray());
        Assert.Equal(written, SpanOf(header));
        Assert.Equal(s_value, before.ToArray());
        Assert.True(header.Equals(new RecordHeader("h", written)));
        Assert.False(header.Equals(new RecordHeader("h", s_value)));

        IHeader empty = record.Headers().ToArray()[1];
        byte[] emptyValue = empty.Value()!;
        Assert.True(MemoryMarshal.TryGetArray(empty.ValueMemory()!.Value, out ArraySegment<byte> emptySegment));
        Assert.Same(emptyValue, emptySegment.Array);
    }

    [Fact]
    public async Task ValueAndValueMemory_FirstCalledFromManyThreads_AgreeOnTheValue()
    {
        const int Records = 32;
        const int Threads = 8;
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        List<IDisposable> blocks = new List<IDisposable>();
        try
        {
            for (int offset = 0; offset < Records; offset++)
            {
                blocks.Add(ConsumerPollHeadersTests.AddRecordWithHeaders(
                    consumer, Topic, Partition, offset, key: null, value: null, ("h", s_value)));
            }

            ConsumerRecords<byte[], byte[]> records = await Poll(consumer);
            Assert.Equal(Records, records.Count());
            foreach (ConsumerRecord<byte[], byte[]> record in records)
            {
                IHeader header = Assert.Single(record.Headers());
                byte[]?[] values = new byte[]?[Threads];
                ReadOnlyMemory<byte>?[] memories = new ReadOnlyMemory<byte>?[Threads];
                using Barrier barrier = new Barrier(Threads);
                Task[] tasks = Enumerable.Range(0, Threads).Select(i => Task.Factory.StartNew(
                    () =>
                    {
                        barrier.SignalAndWait();
                        if (i % 2 == 0)
                        {
                            values[i] = header.Value();
                        }
                        else
                        {
                            memories[i] = header.ValueMemory();
                        }
                    },
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)).ToArray();
                await Task.WhenAll(tasks);

                byte[] value = header.Value()!;
                for (int i = 0; i < Threads; i++)
                {
                    if (i % 2 == 0)
                    {
                        Assert.Same(value, values[i]);
                        continue;
                    }

                    // Each memory is either a view of the batch, taken before the array was cached, or that array.
                    ReadOnlyMemory<byte> memory = memories[i]!.Value;
                    Assert.Equal(s_value, memory.ToArray());
                    if (MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment))
                    {
                        Assert.Same(value, segment.Array);
                    }
                    else
                    {
                        Assert.True(MemoryMarshal.TryGetMemoryManager(memory, out BytesMemoryManager? _));
                    }
                }

                Assert.True(MemoryMarshal.TryGetArray(header.ValueMemory()!.Value, out ArraySegment<byte> last));
                Assert.Same(value, last.Array);
            }
        }
        finally
        {
            foreach (IDisposable block in blocks)
            {
                block.Dispose();
            }
        }
    }

    [Fact]
    public void AKeyDecodedWithAnEmbeddedNul_IsRefusedByAProducer_NotForwardedCutAtTheNul()
    {
        // A fetched key keeps a NUL byte (DecodeKey above), so forwarding such a header refuses the send, as the
        // producer refuses any header key the C ABI would cut. The mocks cannot hold such a key (it crosses as a C
        // string), so the decoded key is given to a built header, whose Key() the send reads the same way.
        string key;
        unsafe
        {
            byte[] withNul = { (byte)'a', 0x00, (byte)'b' };
            fixed (byte* nul = withNul)
            {
                key = RecordHeader.DecodeKey(new KafkaBytes((IntPtr)nul, withNul.Length));
            }
        }

        MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
            "t", (int?)null, (byte[]?)null, (byte[]?)null, new[] { new RecordHeader(key, s_value) });

        ArgumentException e = Assert.Throws<ArgumentException>(() => producer.Send(record));

        Assert.StartsWith(
            "A header key must not contain a NUL character: such a string cannot be passed to the native client "
                + "unchanged.",
            e.Message);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public unsafe void DecodeKey_DecodesUtf8_KeepingAnEmbeddedNul_AndReplacingInvalidBytes()
    {
        // The mocks reject such keys before the core holds them (a header key crosses as a C string), so the decoding
        // the batch's key bytes go through is exercised directly; a broker can send either key.
        byte[] withNul = { (byte)'a', 0x00, (byte)'b' };
        byte[] invalid = { (byte)'x', 0xFF, 0xC3 };
        byte[] nonAscii = Encoding.UTF8.GetBytes("clé-🔑");
        fixed (byte* nul = withNul)
        fixed (byte* bad = invalid)
        fixed (byte* text = nonAscii)
        {
            Assert.Equal("a\0b", RecordHeader.DecodeKey(new KafkaBytes((IntPtr)nul, withNul.Length)));
            Assert.Equal("x��", RecordHeader.DecodeKey(new KafkaBytes((IntPtr)bad, invalid.Length)));
            Assert.Equal("clé-🔑", RecordHeader.DecodeKey(new KafkaBytes((IntPtr)text, nonAscii.Length)));
            Assert.Equal(string.Empty, RecordHeader.DecodeKey(new KafkaBytes((IntPtr)text, 0)));
        }

        Assert.Equal(string.Empty, RecordHeader.DecodeKey(default));
    }

    private static Task<ConsumerRecords<byte[], byte[]>> Poll(NativeConsumer consumer) =>
        consumer.PollWithCallback(
            s_pollTimeout,
            new AsyncDeserializerCall<byte[]>(new AsyncByteArrayDeserializer()),
            new AsyncDeserializerCall<byte[]>(new AsyncByteArrayDeserializer()));

    // The bytes the internal ValueSpan gives, which the producer send reads.
    private static byte[]? SpanOf(IHeader header) =>
        ((RecordHeader)header).ValueSpan(out ReadOnlySpan<byte> value) ? value.ToArray() : null;

    private static object? Field(object target, string name) =>
        target.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(target);
}
