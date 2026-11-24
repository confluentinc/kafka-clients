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
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="HeaderExtensions.ValueMemory(IHeader)"/>: a view of the batch for a header of a polled record, the array
/// <see cref="IHeader.Value()"/> returns for a built <see cref="RecordHeader"/> and for a header class of the user's
/// own, <see langword="null"/> for a null value, and <see cref="ArgumentNullException"/> for a null header, as the
/// other extension methods of the binding.
/// </summary>
public sealed class HeaderExtensionsTests
{
    private const string Topic = "header-extensions";

    private static readonly byte[] s_value = { 0x01, 0x00, 0xFF };

    [Fact]
    public void ValueMemory_NullHeader_ThrowsArgumentNullException()
    {
        ArgumentNullException e =
            Assert.Throws<ArgumentNullException>(() => HeaderExtensions.ValueMemory(null!));

        Assert.Equal("header", e.ParamName);
    }

    [Fact]
    public void ValueMemory_OfAUserHeader_IsTheArrayItsValueReturns_ReadOnEveryCall()
    {
        UserHeader header = new UserHeader("k", s_value);

        ReadOnlyMemory<byte>? memory = header.ValueMemory();

        Assert.NotNull(memory);
        Assert.True(MemoryMarshal.TryGetArray(memory.Value, out ArraySegment<byte> segment));
        Assert.Same(s_value, segment.Array);
        Assert.Equal(0, segment.Offset);
        Assert.Equal(s_value.Length, segment.Count);
        Assert.Equal(1, header.ValueCalls);

        header.ValueMemory();
        Assert.Equal(2, header.ValueCalls);
    }

    [Fact]
    public void ValueMemory_OfAUserHeaderWithANullValue_IsNull_AndWithAnEmptyValue_IsEmpty()
    {
        Assert.Null(new UserHeader("k", null).ValueMemory());

        ReadOnlyMemory<byte>? empty = new UserHeader("k", Array.Empty<byte>()).ValueMemory();
        Assert.NotNull(empty);
        Assert.True(empty.Value.IsEmpty);
    }

    [Fact]
    public void ValueMemory_OfABuiltRecordHeader_IsItsArray_OrNullForANullValue()
    {
        IHeader header = new RecordHeader("k", s_value);

        ReadOnlyMemory<byte>? memory = header.ValueMemory();

        Assert.True(MemoryMarshal.TryGetArray(memory!.Value, out ArraySegment<byte> segment));
        Assert.Same(s_value, segment.Array);
        Assert.Null(((IHeader)new RecordHeader("k", null)).ValueMemory());
    }

    [Fact]
    public async Task ValueMemory_OfAPolledHeader_IsAViewOfTheBatch_ThenTheArrayValueReturns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { (Topic, 0) });
        consumer.Seek(new TopicPartition(Topic, 0), offset: 0);
        using IDisposable blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, 0, offset: 0, key: null, value: null, ("h", s_value), ("n", null));

        ConsumerRecords<byte[], byte[]> records = await consumer.PollWithCallback(
            TimeSpan.FromMilliseconds(100),
            new AsyncDeserializerCall<byte[]>(new AsyncByteArrayDeserializer()),
            new AsyncDeserializerCall<byte[]>(new AsyncByteArrayDeserializer()));
        IHeader[] headers = Assert.Single(records).Headers().ToArray();

        ReadOnlyMemory<byte> view = HeaderExtensions.ValueMemory(headers[0])!.Value;
        Assert.True(MemoryMarshal.TryGetMemoryManager(view, out BytesMemoryManager? _));
        Assert.Equal(s_value, view.ToArray());

        byte[] value = headers[0].Value()!;
        ReadOnlyMemory<byte> after = HeaderExtensions.ValueMemory(headers[0])!.Value;
        Assert.True(MemoryMarshal.TryGetArray(after, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);

        Assert.Null(HeaderExtensions.ValueMemory(headers[1]));
    }

    private sealed class UserHeader : IHeader
    {
        private readonly string _key;
        private readonly byte[]? _value;

        internal UserHeader(string key, byte[]? value)
        {
            _key = key;
            _value = value;
        }

        internal int ValueCalls { get; private set; }

        public string Key() => _key;

        public byte[]? Value()
        {
            ValueCalls++;
            return _value;
        }
    }
}
