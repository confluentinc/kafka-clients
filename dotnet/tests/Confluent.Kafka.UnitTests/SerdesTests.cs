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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the built-in <see cref="Serdes"/>: round trips, Java's byte-level wire vectors, a null input serialized as
/// absent and an empty one as present, and malformed input mapped to <see cref="SerializationException"/> with Java's
/// messages; and the return type of <see cref="ISerializer{T}.Serialize"/>.
/// </summary>
public sealed class SerdesTests
{
    private const string Topic = "topic";

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("café")]
    [InlineData("日本語 🎉")]
    public void String_RoundTrips(string value)
    {
        byte[]? bytes = Serdes.String.Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        string decoded = Serdes.String.Deserialize(Topic, bytes);
        Assert.Equal(value, decoded);
    }

    [Fact]
    public void String_WireFormat_IsUtf8()
    {
        byte[]? bytes = Serdes.String.Serialize(Topic, "café")?.ToArray();
        Assert.Equal(new byte[] { 0x63, 0x61, 0x66, 0xC3, 0xA9 }, bytes);
    }

    [Fact]
    public void String_UnpairedSurrogate_SerializesAsQuestionMark_AsJavasGetBytes()
    {
        Assert.Equal(new byte[] { 0x61, 0x3F, 0x62 }, Serdes.String.Serialize(Topic, "a\uD800b")?.ToArray());
        Assert.Equal(new byte[] { 0x61, 0x3F, 0x62 }, Serdes.String.Serialize(Topic, "a\uDC00b")?.ToArray());
        Assert.Equal(new byte[] { 0xF0, 0x9F, 0x8E, 0x88 }, Serdes.String.Serialize(Topic, "🎈")?.ToArray());
    }

    [Fact]
    public void ISerializer_Serialize_ReturnsNullableReadOnlyMemory()
    {
        MethodInfo serialize = Assert.Single(typeof(ISerializer<>).GetMethods());
        Assert.Equal(nameof(ISerializer<object>.Serialize), serialize.Name);
        Assert.Equal(typeof(ReadOnlyMemory<byte>?), serialize.ReturnType);
    }

    [Fact]
    public void String_SerializeNull_ReturnsNull()
    {
        string? nil = null;
        Assert.Null(Serdes.String.Serialize(Topic, nil!));
    }

    [Fact]
    public void String_SerializeEmpty_ReturnsEmptyNotNull()
    {
        ReadOnlyMemory<byte>? serialized = Serdes.String.Serialize(Topic, string.Empty);
        Assert.True(serialized.HasValue);
        Assert.Equal(0, serialized.GetValueOrDefault().Length);
    }

    [Fact]
    public void ByteArray_RoundTrips_AndDeserializeCopiesOut()
    {
        byte[] value = { 0x00, 0x01, 0xFE, 0xFF };

        ReadOnlyMemory<byte>? serialized = Serdes.ByteArray.Serialize(Topic, value);
        Assert.True(serialized.HasValue);
        Assert.True(MemoryMarshal.TryGetArray(serialized.Value, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);
        Assert.Equal(0, segment.Offset);
        Assert.Equal(value.Length, segment.Count);

        byte[] decoded = Serdes.ByteArray.Deserialize(Topic, value);
        Assert.Equal(value, decoded);
        Assert.NotSame(value, decoded);
    }

    [Fact]
    public void ByteArray_DeserializeEmpty_ReturnsEmptyOwnedArray()
    {
        byte[] decoded = Serdes.ByteArray.Deserialize(Topic, ReadOnlySpan<byte>.Empty);
        Assert.Empty(decoded);
    }

    [Fact]
    public void ByteArray_SerializeNull_ReturnsNull()
    {
        byte[]? nil = null;
        Assert.Null(Serdes.ByteArray.Serialize(Topic, nil!));
    }

    [Fact]
    public void ByteArray_SerializeEmpty_ReturnsEmptyNotNull()
    {
        ReadOnlyMemory<byte>? serialized = Serdes.ByteArray.Serialize(Topic, Array.Empty<byte>());
        Assert.True(serialized.HasValue);
        Assert.Equal(0, serialized.GetValueOrDefault().Length);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(256)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void Int32_RoundTrips(int value)
    {
        byte[]? bytes = Serdes.Int32.Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Int32.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Int32_WireFormat_IsBigEndian()
    {
        Assert.Equal(new byte[] { 0x00, 0x00, 0x00, 0x01 }, Serdes.Int32.Serialize(Topic, 1)?.ToArray());
        Assert.Equal(new byte[] { 0x00, 0x00, 0x01, 0x00 }, Serdes.Int32.Serialize(Topic, 256)?.ToArray());
        Assert.Equal(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, Serdes.Int32.Serialize(Topic, -1)?.ToArray());
        Assert.Equal(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF }, Serdes.Int32.Serialize(Topic, int.MaxValue)?.ToArray());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void Int64_RoundTrips(long value)
    {
        byte[]? bytes = Serdes.Int64.Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Int64.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Int64_WireFormat_IsBigEndian()
    {
        Assert.Equal(
            new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 },
            Serdes.Int64.Serialize(Topic, 1L)?.ToArray());
        Assert.Equal(
            new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 },
            Serdes.Int64.Serialize(Topic, 0x0102030405060708L)?.ToArray());
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(1.0)]
    [InlineData(-1.5)]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void Double_RoundTrips(double value)
    {
        byte[]? bytes = Serdes.Double.Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Double.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Double_NaN_RoundTrips()
    {
        byte[]? bytes = Serdes.Double.Serialize(Topic, double.NaN)?.ToArray();
        Assert.NotNull(bytes);
        Assert.True(double.IsNaN(Serdes.Double.Deserialize(Topic, bytes)));
    }

    [Fact]
    public void Double_WireFormat_IsBigEndianDoubleToLongBits()
    {
        Assert.Equal(
            new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            Serdes.Double.Serialize(Topic, 1.0)?.ToArray());

        Assert.Equal(
            new byte[] { 0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            Serdes.Double.Serialize(Topic, double.NaN)?.ToArray());
    }

    [Fact]
    public void Guid_RoundTrips()
    {
        Guid value = Guid.NewGuid();
        byte[]? bytes = Serdes.Guid.Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Guid.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Guid_WireFormat_IsCanonicalLowercaseStringUtf8_NotRawBytes()
    {
        var value = new Guid("A1B2C3D4-E5F6-7788-99AA-BBCCDDEEFF00");
        const string canonical = "a1b2c3d4-e5f6-7788-99aa-bbccddeeff00";

        byte[]? bytes = Serdes.Guid.Serialize(Topic, value)?.ToArray();

        Assert.Equal(Encoding.UTF8.GetBytes(canonical), bytes);
        Assert.Equal(36, bytes!.Length);
        Assert.NotEqual(16, bytes.Length);
    }

    [Fact]
    public void Guid_DeserializeParsesCanonicalString()
    {
        byte[] wire = Encoding.UTF8.GetBytes("12345678-9abc-def0-1234-56789abcdef0");
        Guid decoded = Serdes.Guid.Deserialize(Topic, wire);
        Assert.Equal(new Guid("12345678-9abc-def0-1234-56789abcdef0"), decoded);
    }

    [Fact]
    public void Null_SerializeReturnsNull_DeserializeReturnsDefault()
    {
        Assert.Null(Serdes.Null.Serialize(Topic, null));
        Assert.Null(Serdes.Null.Serialize(Topic, new object()));
        Assert.Null(Serdes.Null.Deserialize(Topic, ReadOnlySpan<byte>.Empty));
        Assert.Null(Serdes.Null.Deserialize(Topic, new byte[] { 1, 2, 3 }));
    }

    [Fact]
    public void Int32_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Int32.Deserialize(Topic, new byte[] { 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by IntegerDeserializer is not 4", ex.Message);
    }

    [Fact]
    public void Int64_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Int64.Deserialize(Topic, new byte[] { 0x00, 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by LongDeserializer is not 8", ex.Message);
    }

    [Fact]
    public void Double_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Double.Deserialize(Topic, new byte[] { 0x00, 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by Deserializer is not 8", ex.Message);
    }

    [Fact]
    public void Guid_MalformedText_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Guid.Deserialize(Topic, Encoding.UTF8.GetBytes("not-a-uuid")));
        Assert.Equal("Error parsing data into UUID", ex.Message);
        Assert.NotNull(ex.InnerException);
    }

    [Fact]
    public void SerializationException_IsCatchableAsKafkaException()
    {
        KafkaException ex = Assert.ThrowsAny<KafkaException>(
            () => Serdes.Int32.Deserialize(Topic, new byte[] { 0x00 }));
        Assert.IsType<SerializationException>(ex);
        Assert.IsAssignableFrom<KafkaException>(ex);

        Assert.Equal(-15, ex.Code);
        Assert.False(ex is Common.Errors.RetriableException);
    }
}
