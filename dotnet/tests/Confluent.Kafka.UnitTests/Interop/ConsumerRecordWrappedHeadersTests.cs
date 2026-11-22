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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// A polled record's headers over the core's views of the batch: they are read on first use, once, under the
/// batch's lock; a header's key is decoded and cached on first use and its value copied once and cached, while
/// <see cref="Header.ValueMemory"/> is a view of the batch that outlives the next poll and the consumer.
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
        Header header;
        ReadOnlyMemory<byte>? memory;
        try
        {
            consumer.Assign(new[] { (Topic, Partition) });
            consumer.Seek(Topic, Partition, offset: 0);
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
        consumer.Seek(Topic, Partition, offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null, ("h", s_value));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
        Header header = Assert.Single(record.Headers());

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
        consumer.Seek(Topic, Partition, offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null,
            ("null-value", null), ("empty-value", Array.Empty<byte>()));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));
        Header[] headers = record.Headers().ToArray();

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
        Header header = new Header("k", value);

        ReadOnlyMemory<byte>? memory = header.ValueMemory();

        Assert.NotNull(memory);
        Assert.True(MemoryMarshal.TryGetArray(memory.Value, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);
        Assert.Null(new Header("k", null).ValueMemory());
    }

    [Fact]
    public async Task HeadersNoDeserializerTakes_AreNotRead_UntilTheirFirstUse()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(Topic, Partition, offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, Partition, offset: 0, key: null, value: null, ("h", s_value));

        ConsumerRecord<byte[], byte[]> record = Assert.Single(await Poll(consumer));

        Assert.Null(Field(record, "_headers"));
        Headers headers = record.Headers();
        Assert.IsType<SafeConsumerRecordsHandle>(Field(headers, "_state"));

        Header header = Assert.Single(headers);
        Assert.IsType<List<Header>>(Field(headers, "_state"));
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
        consumer.Seek(Topic, Partition, offset: 0);
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
                (Headers Headers, Header[] Entries, string Key, byte[]? Value)[] seen =
                    new (Headers, Header[], string, byte[]?)[Threads];
                using Barrier barrier = new Barrier(Threads);
                Task[] tasks = Enumerable.Range(0, Threads).Select(i => Task.Factory.StartNew(
                    () =>
                    {
                        barrier.SignalAndWait();
                        Headers headers = record.Headers();
                        Header[] entries = headers.ToArray();
                        seen[i] = (headers, entries, entries[1].Key(), entries[1].Value());
                    },
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)).ToArray();
                await Task.WhenAll(tasks);

                foreach ((Headers Headers, Header[] Entries, string Key, byte[]? Value) one in seen)
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
            Assert.Equal("a\0b", Header.DecodeKey(new KafkaBytes((IntPtr)nul, withNul.Length)));
            Assert.Equal("x��", Header.DecodeKey(new KafkaBytes((IntPtr)bad, invalid.Length)));
            Assert.Equal("clé-🔑", Header.DecodeKey(new KafkaBytes((IntPtr)text, nonAscii.Length)));
            Assert.Equal(string.Empty, Header.DecodeKey(new KafkaBytes((IntPtr)text, 0)));
        }

        Assert.Equal(string.Empty, Header.DecodeKey(default));
    }

    private static Task<ConsumerRecords<byte[], byte[]>> Poll(NativeConsumer consumer) =>
        consumer.PollWithCallback(
            s_pollTimeout,
            new AsyncDeserializerCall<byte[]>(new AsyncByteArrayDeserializer()),
            new AsyncDeserializerCall<byte[]>(new AsyncByteArrayDeserializer()));

    private static object? Field(object target, string name) =>
        target.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(target);
}
