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
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests.Internal;

/// <summary>
/// <see cref="SerializedProducerRecord.Serialize"/> keeps the memory each serializer returned: an absent key or value
/// stays absent, an empty one stays present, and an array slice or native memory keeps its backing, offset and length.
/// </summary>
public sealed class SerializedProducerRecordTests
{
    private const string Topic = "serialized-record-topic";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Serialize_NullAndEmpty_StayDistinct(bool mock)
    {
        SerializedProducerRecord nullKey = Serialize(null, ReadOnlyMemory<byte>.Empty, mock);
        Assert.False(nullKey.Key.HasValue);
        Assert.True(nullKey.Value.HasValue);
        Assert.Equal(0, nullKey.Value.GetValueOrDefault().Length);

        SerializedProducerRecord nullValue = Serialize(ReadOnlyMemory<byte>.Empty, null, mock);
        Assert.True(nullValue.Key.HasValue);
        Assert.Equal(0, nullValue.Key.GetValueOrDefault().Length);
        Assert.False(nullValue.Value.HasValue);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Serialize_ArraySlices_KeepTheirArrayOffsetAndLength(bool mock)
    {
        byte[] keyArray = new byte[16];
        byte[] valueArray = new byte[32];

        SerializedProducerRecord record = Serialize(
            new ReadOnlyMemory<byte>(keyArray, 3, 5), new ReadOnlyMemory<byte>(valueArray, 3, 20), mock);

        AssertArraySlice(keyArray, 3, 5, record.Key);
        AssertArraySlice(valueArray, 3, 20, record.Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Serialize_NativeMemory_KeepsItsManagerStartAndLength(bool mock)
    {
        using NativeMemoryManager key = new NativeMemoryManager(16);
        using NativeMemoryManager value = new NativeMemoryManager(64);

        SerializedProducerRecord record = Serialize(key.Memory.Slice(3, 8), value.Memory, mock);

        AssertNative(key, 3, 8, record.Key);
        AssertNative(value, 0, 64, record.Value);
    }

    private static SerializedProducerRecord Serialize(
        ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte>? value, bool mock) =>
        SerializedProducerRecord.Serialize(
            new ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(Topic, value: value, key: key),
            MemorySerializer.Instance,
            null,
            MemorySerializer.Instance,
            null,
            mock);

    private static void AssertArraySlice(byte[] array, int offset, int length, ReadOnlyMemory<byte>? actual)
    {
        Assert.True(actual.HasValue);
        Assert.Equal(length, actual.GetValueOrDefault().Length);
        Assert.True(MemoryMarshal.TryGetArray(actual.GetValueOrDefault(), out ArraySegment<byte> segment));
        Assert.Same(array, segment.Array);
        Assert.Equal(offset, segment.Offset);
        Assert.Equal(length, segment.Count);
    }

    private static void AssertNative(NativeMemoryManager manager, int start, int length, ReadOnlyMemory<byte>? actual)
    {
        Assert.True(actual.HasValue);
        Assert.Equal(length, actual.GetValueOrDefault().Length);
        Assert.True(MemoryMarshal.TryGetMemoryManager(
            actual.GetValueOrDefault(), out NativeMemoryManager? actualManager, out int actualStart, out int actualLength));
        Assert.Same(manager, actualManager);
        Assert.Equal(start, actualStart);
        Assert.Equal(length, actualLength);
    }
}
