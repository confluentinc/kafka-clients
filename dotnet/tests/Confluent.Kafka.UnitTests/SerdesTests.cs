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
        byte[]? bytes = Serdes.String().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        string? decoded = Serdes.String().Deserializer().Deserialize(Topic, bytes);
        Assert.Equal(value, decoded);
    }

    [Fact]
    public void String_WireFormat_IsUtf8()
    {
        byte[]? bytes = Serdes.String().Serializer().Serialize(Topic, "café")?.ToArray();
        Assert.Equal(new byte[] { 0x63, 0x61, 0x66, 0xC3, 0xA9 }, bytes);
    }

    [Fact]
    public void String_UnpairedSurrogate_SerializesAsQuestionMark_AsJavasGetBytes()
    {
        Assert.Equal(new byte[] { 0x61, 0x3F, 0x62 }, Serdes.String().Serializer().Serialize(Topic, "a\uD800b")?.ToArray());
        Assert.Equal(new byte[] { 0x61, 0x3F, 0x62 }, Serdes.String().Serializer().Serialize(Topic, "a\uDC00b")?.ToArray());
        Assert.Equal(new byte[] { 0xF0, 0x9F, 0x8E, 0x88 }, Serdes.String().Serializer().Serialize(Topic, "🎈")?.ToArray());
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
        Assert.Null(Serdes.String().Serializer().Serialize(Topic, nil));
    }

    [Fact]
    public void String_SerializeEmpty_ReturnsEmptyNotNull()
    {
        ReadOnlyMemory<byte>? serialized = Serdes.String().Serializer().Serialize(Topic, string.Empty);
        Assert.True(serialized.HasValue);
        Assert.Equal(0, serialized.GetValueOrDefault().Length);
    }

    [Fact]
    public void ByteArray_RoundTrips_AndDeserializeCopiesOut()
    {
        byte[] value = { 0x00, 0x01, 0xFE, 0xFF };

        ReadOnlyMemory<byte>? serialized = Serdes.ByteArray().Serializer().Serialize(Topic, value);
        Assert.True(serialized.HasValue);
        Assert.True(MemoryMarshal.TryGetArray(serialized.Value, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);
        Assert.Equal(0, segment.Offset);
        Assert.Equal(value.Length, segment.Count);

        byte[]? decoded = Serdes.ByteArray().Deserializer().Deserialize(Topic, value);
        Assert.Equal(value, decoded);
        Assert.NotSame(value, decoded);
    }

    [Fact]
    public void ByteArray_DeserializeEmpty_ReturnsEmptyOwnedArray()
    {
        byte[]? decoded = Serdes.ByteArray().Deserializer().Deserialize(Topic, ReadOnlySpan<byte>.Empty);
        Assert.NotNull(decoded);
        Assert.Empty(decoded);
    }

    [Fact]
    public void ByteArray_SerializeNull_ReturnsNull()
    {
        byte[]? nil = null;
        Assert.Null(Serdes.ByteArray().Serializer().Serialize(Topic, nil));
    }

    [Fact]
    public void ByteArray_SerializeEmpty_ReturnsEmptyNotNull()
    {
        ReadOnlyMemory<byte>? serialized = Serdes.ByteArray().Serializer().Serialize(Topic, Array.Empty<byte>());
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
    public void Integer_RoundTrips(int value)
    {
        byte[]? bytes = Serdes.Integer().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Integer().Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void Integer_WireFormat_IsBigEndian()
    {
        Assert.Equal(new byte[] { 0x00, 0x00, 0x00, 0x01 }, Serdes.Integer().Serializer().Serialize(Topic, 1)?.ToArray());
        Assert.Equal(new byte[] { 0x00, 0x00, 0x01, 0x00 }, Serdes.Integer().Serializer().Serialize(Topic, 256)?.ToArray());
        Assert.Equal(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, Serdes.Integer().Serializer().Serialize(Topic, -1)?.ToArray());
        Assert.Equal(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF }, Serdes.Integer().Serializer().Serialize(Topic, int.MaxValue)?.ToArray());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void Long_RoundTrips(long value)
    {
        byte[]? bytes = Serdes.Long().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Long().Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void Long_WireFormat_IsBigEndian()
    {
        Assert.Equal(
            new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 },
            Serdes.Long().Serializer().Serialize(Topic, 1L)?.ToArray());
        Assert.Equal(
            new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 },
            Serdes.Long().Serializer().Serialize(Topic, 0x0102030405060708L)?.ToArray());
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
        byte[]? bytes = Serdes.Double().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Double().Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void Double_NaN_RoundTrips()
    {
        byte[]? bytes = Serdes.Double().Serializer().Serialize(Topic, double.NaN)?.ToArray();
        Assert.NotNull(bytes);
        Assert.True(double.IsNaN(Serdes.Double().Deserializer().Deserialize(Topic, bytes)!.Value));
    }

    [Fact]
    public void Double_WireFormat_IsBigEndianDoubleToLongBits()
    {
        Assert.Equal(
            new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            Serdes.Double().Serializer().Serialize(Topic, 1.0)?.ToArray());

        Assert.Equal(
            new byte[] { 0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            Serdes.Double().Serializer().Serialize(Topic, double.NaN)?.ToArray());
    }

    [Fact]
    public void UUID_RoundTrips()
    {
        Guid value = Guid.NewGuid();
        byte[]? bytes = Serdes.UUID().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.UUID().Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void UUID_WireFormat_IsCanonicalLowercaseStringUtf8_NotRawBytes()
    {
        var value = new Guid("A1B2C3D4-E5F6-7788-99AA-BBCCDDEEFF00");
        const string canonical = "a1b2c3d4-e5f6-7788-99aa-bbccddeeff00";

        byte[]? bytes = Serdes.UUID().Serializer().Serialize(Topic, value)?.ToArray();

        Assert.Equal(Encoding.UTF8.GetBytes(canonical), bytes);
        Assert.Equal(36, bytes!.Length);
        Assert.NotEqual(16, bytes.Length);
    }

    [Fact]
    public void UUID_DeserializeParsesCanonicalString()
    {
        byte[] wire = Encoding.UTF8.GetBytes("12345678-9abc-def0-1234-56789abcdef0");
        Guid? decoded = Serdes.UUID().Deserializer().Deserialize(Topic, wire);
        Assert.Equal(new Guid("12345678-9abc-def0-1234-56789abcdef0"), decoded);
    }

    [Fact]
    public void Void_SerializeReturnsNull_DeserializeThrowsForAnyData()
    {
        Assert.Null(Serdes.Void().Serializer().Serialize(Topic, null));
        Assert.Null(Serdes.Void().Serializer().Serialize(Topic, new object()));
        IllegalArgumentException empty = Assert.Throws<IllegalArgumentException>(
            () => Serdes.Void().Deserializer().Deserialize(Topic, ReadOnlySpan<byte>.Empty));
        Assert.Equal("Data should be null for a VoidDeserializer.", empty.Message);
        IllegalArgumentException data = Assert.Throws<IllegalArgumentException>(
            () => Serdes.Void().Deserializer().Deserialize(Topic, new byte[] { 1, 2, 3 }));
        Assert.Equal("Data should be null for a VoidDeserializer.", data.Message);
    }

    [Fact]
    public void Integer_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Integer().Deserializer().Deserialize(Topic, new byte[] { 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by IntegerDeserializer is not 4", ex.Message);
    }

    [Fact]
    public void Long_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Long().Deserializer().Deserialize(Topic, new byte[] { 0x00, 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by LongDeserializer is not 8", ex.Message);
    }

    [Fact]
    public void Double_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Double().Deserializer().Deserialize(Topic, new byte[] { 0x00, 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by DoubleDeserializer is not 8", ex.Message);
    }

    [Fact]
    public void UUID_MalformedText_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.UUID().Deserializer().Deserialize(Topic, Encoding.UTF8.GetBytes("not-a-uuid")));
        Assert.Equal("Error parsing data into UUID", ex.Message);
        IllegalArgumentException cause = Assert.IsType<IllegalArgumentException>(ex.InnerException);
        Assert.Equal("Invalid UUID string: not-a-uuid", cause.Message);
    }

    [Theory]
    [InlineData("0F14D0AB-9605-4A62-A9E4-5ED26688389B", "0f14d0ab-9605-4a62-a9e4-5ed26688389b")]
    [InlineData("1-1-1-1-1", "00000001-0001-0001-0001-000000000001")]
    [InlineData("+1-1-1-1-1", "00000001-0001-0001-0001-000000000001")]
    [InlineData("fffffffff-1-1-1-1", "ffffffff-0001-0001-0001-000000000001")]
    [InlineData("1-1-1-1-\uFF11\uFF21", "00000001-0001-0001-0001-00000000001a")]
    public void UUID_Deserialize_ReadsWhatJavasFromStringReads(string text, string expected)
    {
        Assert.Equal(new Guid(expected), Serdes.UUID().Deserializer().Deserialize(Topic, Encoding.UTF8.GetBytes(text)));
    }

    [Theory]
    [InlineData("not a uuid", "Invalid UUID string: not a uuid")]
    [InlineData("0f14d0ab96054a62a9e45ed26688389b", "Invalid UUID string: 0f14d0ab96054a62a9e45ed26688389b")]
    [InlineData("1-1-1-1-1-1", "Invalid UUID string: 1-1-1-1-1-1")]
    [InlineData("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "UUID string too large")]
    [InlineData("g-1-1-1-1", "Error at index 0 in: \"g\"")]
    [InlineData("1--1-1-1", "For input string: \"\" under radix 16")]
    [InlineData("1-1-1-1-10000000000000000", "Error at index 16 in: \"10000000000000000\"")]
    public void UUID_Deserialize_RefusesWhatJavasFromStringRefuses(string text, string causeMessage)
    {
        AssertRefusedUuid(text, causeMessage);
    }

    [Theory]
    [InlineData("{0f14d0ab-9605-4a62-a9e4-5ed26688389b}", true, "UUID string too large")]
    [InlineData("(0f14d0ab-9605-4a62-a9e4-5ed26688389b)", true, "UUID string too large")]
    [InlineData("0f14d0ab96054a62a9e45ed26688389b", true, "Invalid UUID string: 0f14d0ab96054a62a9e45ed26688389b")]
    [InlineData(" 0f14d0ab-9605-4a62-a9e4-5ed26688389b ", true, "UUID string too large")]
    [InlineData("{1-1-1-1-1}", false, "Error at index 0 in: \"{1\"")]
    [InlineData(" 1-1-1-1-1", false, "Error at index 0 in: \" 1\"")]
    public void UUID_Deserialize_RefusesTheLayoutsOnlyGuidParseReads(string text, bool guidParseReadsIt, string causeMessage)
    {
        if (guidParseReadsIt)
        {
            Assert.Equal(new Guid("0f14d0ab-9605-4a62-a9e4-5ed26688389b"), Guid.Parse(text));
        }

        AssertRefusedUuid(text, causeMessage);
    }

    [Fact]
    public void Boolean_WireFormat_IsOneByte()
    {
        Assert.Equal(new byte[] { 0x01 }, Serdes.Boolean().Serializer().Serialize(Topic, true)?.ToArray());
        Assert.Equal(new byte[] { 0x00 }, Serdes.Boolean().Serializer().Serialize(Topic, false)?.ToArray());
        Assert.Equal(true, Serdes.Boolean().Deserializer().Deserialize(Topic, new byte[] { 0x01 }));
        Assert.Equal(false, Serdes.Boolean().Deserializer().Deserialize(Topic, new byte[] { 0x00 }));
    }

    [Fact]
    public void Boolean_SerializeNull_ReturnsNull()
    {
        Assert.Null(Serdes.Boolean().Serializer().Serialize(Topic, null));
    }

    [Theory]
    [InlineData((byte)0x02, "2")]
    [InlineData((byte)0x7F, "127")]
    [InlineData((byte)0x80, "-128")]
    [InlineData((byte)0xFF, "-1")]
    public void Boolean_UnexpectedByte_ThrowsJavasMessage_WithTheByteSigned(byte b, string printed)
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Boolean().Deserializer().Deserialize(Topic, new[] { b }));
        Assert.Equal("Unexpected byte received by BooleanDeserializer: " + printed, ex.Message);
    }

    [Fact]
    public void Boolean_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Boolean().Deserializer().Deserialize(Topic, new byte[] { 0x01, 0x00 }));
        Assert.Equal("Size of data received by BooleanDeserializer is not 1", ex.Message);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData((short)-1)]
    [InlineData((short)256)]
    [InlineData(short.MaxValue)]
    [InlineData(short.MinValue)]
    public void Short_RoundTrips(short value)
    {
        byte[]? bytes = Serdes.Short().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Short().Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void Short_WireFormat_IsBigEndian()
    {
        Assert.Equal(new byte[] { 0x00, 0x01 }, Serdes.Short().Serializer().Serialize(Topic, (short)1)?.ToArray());
        Assert.Equal(new byte[] { 0x01, 0x00 }, Serdes.Short().Serializer().Serialize(Topic, (short)256)?.ToArray());
        Assert.Equal(new byte[] { 0xFF, 0xFF }, Serdes.Short().Serializer().Serialize(Topic, (short)-1)?.ToArray());
        Assert.Equal(new byte[] { 0x7F, 0xFF }, Serdes.Short().Serializer().Serialize(Topic, short.MaxValue)?.ToArray());
        Assert.Equal(new byte[] { 0x80, 0x00 }, Serdes.Short().Serializer().Serialize(Topic, short.MinValue)?.ToArray());
        Assert.Equal((short)-2, Serdes.Short().Deserializer().Deserialize(Topic, new byte[] { 0xFF, 0xFE }));
        Assert.Equal((short)0x0102, Serdes.Short().Deserializer().Deserialize(Topic, new byte[] { 0x01, 0x02 }));
    }

    [Fact]
    public void Short_SerializeNull_ReturnsNull()
    {
        Assert.Null(Serdes.Short().Serializer().Serialize(Topic, null));
    }

    [Fact]
    public void Short_WrongLength_ThrowsSerializationException()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Short().Deserializer().Deserialize(Topic, new byte[] { 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by ShortDeserializer is not 2", ex.Message);
        var one = Assert.Throws<SerializationException>(
            () => Serdes.Short().Deserializer().Deserialize(Topic, new byte[] { 0x01 }));
        Assert.Equal("Size of data received by ShortDeserializer is not 2", one.Message);
    }

    [Theory]
    [InlineData(0.0f)]
    [InlineData(1.0f)]
    [InlineData(-2.5f)]
    [InlineData(float.MaxValue)]
    [InlineData(float.MinValue)]
    [InlineData(float.Epsilon)]
    [InlineData(float.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    public void Float_RoundTrips(float value)
    {
        byte[]? bytes = Serdes.Float().Serializer().Serialize(Topic, value)?.ToArray();
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Float().Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void Float_WireFormat_IsBigEndianFloatToRawIntBits()
    {
        Assert.Equal(new byte[] { 0x3F, 0x80, 0x00, 0x00 }, Serdes.Float().Serializer().Serialize(Topic, 1.0f)?.ToArray());
        Assert.Equal(new byte[] { 0xC0, 0x20, 0x00, 0x00 }, Serdes.Float().Serializer().Serialize(Topic, -2.5f)?.ToArray());
        Assert.Equal(new byte[] { 0x80, 0x00, 0x00, 0x00 }, Serdes.Float().Serializer().Serialize(Topic, -0.0f)?.ToArray());
        Assert.Equal(
            new byte[] { 0x7F, 0xC0, 0x00, 0x00 },
            Serdes.Float().Serializer().Serialize(Topic, FloatOfBits(0x7FC00000))?.ToArray());
        Assert.Equal(
            new byte[] { 0xFF, 0xC0, 0x00, 0x05 },
            Serdes.Float().Serializer().Serialize(Topic, FloatOfBits(unchecked((int)0xFFC00005)))?.ToArray());

        // A signaling NaN keeps its quiet bit clear only in a 64-bit process: in a 32-bit x86 one, a float returned
        // from a method passes through the x87 stack, which sets that bit, as Java's Float.intBitsToFloat notes.
        if (Environment.Is64BitProcess)
        {
            Assert.Equal(
                new byte[] { 0xFF, 0x80, 0x00, 0x05 },
                Serdes.Float().Serializer().Serialize(Topic, FloatOfBits(unchecked((int)0xFF800005)))?.ToArray());
        }
    }

    [Fact]
    public void Float_Deserialize_KeepsTheRawBits()
    {
        Assert.Equal(1.0f, Serdes.Float().Deserializer().Deserialize(Topic, new byte[] { 0x3F, 0x80, 0x00, 0x00 }));
        float? negativeZero = Serdes.Float().Deserializer().Deserialize(Topic, new byte[] { 0x80, 0x00, 0x00, 0x00 });
        Assert.Equal(unchecked((int)0x80000000), BitsOfFloat(negativeZero!.Value));
        float? nan = Serdes.Float().Deserializer().Deserialize(Topic, new byte[] { 0xFF, 0xC0, 0x00, 0x05 });
        Assert.Equal(unchecked((int)0xFFC00005), BitsOfFloat(nan!.Value));

        // As in Float_WireFormat_IsBigEndianFloatToRawIntBits, a signaling NaN keeps its quiet bit clear only in a
        // 64-bit process.
        if (Environment.Is64BitProcess)
        {
            float? signaling = Serdes.Float().Deserializer().Deserialize(Topic, new byte[] { 0xFF, 0x80, 0x00, 0x05 });
            Assert.Equal(unchecked((int)0xFF800005), BitsOfFloat(signaling!.Value));
        }
    }

    [Fact]
    public void Float_SerializeNull_ReturnsNull()
    {
        Assert.Null(Serdes.Float().Serializer().Serialize(Topic, null));
    }

    [Fact]
    public void Float_WrongLength_ThrowsJavasMessage()
    {
        var ex = Assert.Throws<SerializationException>(
            () => Serdes.Float().Deserializer().Deserialize(Topic, new byte[] { 0x00, 0x00, 0x01 }));
        Assert.Equal("Size of data received by Deserializer is not 4", ex.Message);
    }

    [Fact]
    public void ByteBuffer_Serialize_ReturnsTheMemoryAsGiven_AnArrayASliceOrNativeMemory()
    {
        byte[] array = { 0x00, 0x01, 0xFE, 0xFF };
        ReadOnlyMemory<byte> whole = array;
        ReadOnlyMemory<byte> slice = new ReadOnlyMemory<byte>(array, 1, 2);
        using NativeMemoryManager native = new NativeMemoryManager(4);
        ReadOnlyMemory<byte> nativeMemory = native.Memory;

        foreach (ReadOnlyMemory<byte> memory in new[] { whole, slice, nativeMemory })
        {
            ReadOnlyMemory<byte>? serialized = Serdes.ByteBuffer().Serializer().Serialize(Topic, memory);
            Assert.True(serialized.HasValue);
            Assert.True(memory.Equals(serialized.GetValueOrDefault()));
        }
    }

    [Fact]
    public void ByteBuffer_SerializeNull_ReturnsNull_AndEmptyStaysEmpty()
    {
        Assert.Null(Serdes.ByteBuffer().Serializer().Serialize(Topic, null));
        ReadOnlyMemory<byte>? empty = Serdes.ByteBuffer().Serializer().Serialize(Topic, ReadOnlyMemory<byte>.Empty);
        Assert.True(empty.HasValue);
        Assert.Equal(0, empty.GetValueOrDefault().Length);
    }

    [Fact]
    public void ByteBuffer_Deserialize_CopiesTheBytesIntoANewArray()
    {
        byte[] data = { 0x00, 0x01, 0xFE, 0xFF };

        ReadOnlyMemory<byte>? decoded = Serdes.ByteBuffer().Deserializer().Deserialize(Topic, data);

        Assert.True(decoded.HasValue);
        Assert.Equal(data, decoded.GetValueOrDefault().ToArray());
        Assert.True(MemoryMarshal.TryGetArray(decoded.GetValueOrDefault(), out ArraySegment<byte> segment));
        Assert.NotSame(data, segment.Array);
        Assert.Equal(0, segment.Offset);
        Assert.Equal(data.Length, segment.Array!.Length);
        data[0] = 0x7F;
        Assert.Equal(0x00, decoded.GetValueOrDefault().Span[0]);
    }

    [Fact]
    public void ByteBuffer_DeserializeEmpty_ReturnsAnEmptyMemoryNotNull()
    {
        ReadOnlyMemory<byte>? decoded = Serdes.ByteBuffer().Deserializer().Deserialize(Topic, ReadOnlySpan<byte>.Empty);
        Assert.True(decoded.HasValue);
        Assert.Equal(0, decoded.GetValueOrDefault().Length);
    }

    [Fact]
    public void SerializationException_IsCatchableAsKafkaException()
    {
        KafkaException ex = Assert.ThrowsAny<KafkaException>(
            () => Serdes.Integer().Deserializer().Deserialize(Topic, new byte[] { 0x00 }));
        Assert.IsType<SerializationException>(ex);
        Assert.IsAssignableFrom<KafkaException>(ex);

        Assert.Equal(-15, ex.Code);
        Assert.False(ex is Common.Errors.RetriableException);
    }

    private static float FloatOfBits(int bits) => BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);

    private static int BitsOfFloat(float value) => BitConverter.ToInt32(BitConverter.GetBytes(value), 0);

    private static void AssertRefusedUuid(string text, string causeMessage)
    {
        SerializationException ex = Assert.Throws<SerializationException>(
            () => Serdes.UUID().Deserializer().Deserialize(Topic, Encoding.UTF8.GetBytes(text)));
        Assert.Equal("Error parsing data into UUID", ex.Message);
        IllegalArgumentException cause = Assert.IsType<IllegalArgumentException>(ex.InnerException);
        Assert.Equal(causeMessage, cause.Message);
    }
}
