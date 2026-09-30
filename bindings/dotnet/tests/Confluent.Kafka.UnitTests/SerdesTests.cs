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
using System.Text;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The built-in <see cref="Serdes"/>: bidirectional round-trips, <b>byte-level
/// Java-wire-parity vectors</b> (DoD §3 — the guard a wrong endianness or a raw-bytes
/// UUID would slip past a round-trip-only test), and malformed-input mapping to
/// <see cref="SerializationException"/> with Java's exact messages. Wire formats are
/// verified against <c>org.apache.kafka.common.serialization.*</c> (Apache Kafka 4.2).
/// </summary>
public sealed class SerdesTests
{
    private const string Topic = "topic";

    // ---- String (UTF-8) ---------------------------------------------------------

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("café")]            // non-ASCII (Latin-1 supplement)
    [InlineData("日本語 🎉")]        // multi-byte + surrogate pair
    public void String_RoundTrips(string value)
    {
        byte[]? bytes = Serdes.String.Serialize(Topic, value);
        Assert.NotNull(bytes);
        string decoded = Serdes.String.Deserialize(Topic, bytes);
        Assert.Equal(value, decoded);
    }

    [Fact]
    public void String_WireFormat_IsUtf8()
    {
        // Java StringSerializer: data.getBytes(UTF_8).
        byte[]? bytes = Serdes.String.Serialize(Topic, "café");
        Assert.Equal(new byte[] { 0x63, 0x61, 0x66, 0xC3, 0xA9 }, bytes);
    }

    [Fact]
    public void String_SerializeNull_ReturnsNull()
    {
        // Java StringSerializer returns null for null input (nullable-return parity).
        string? nil = null;
        Assert.Null(Serdes.String.Serialize(Topic, nil!));
    }

    // ---- ByteArray (identity) ---------------------------------------------------

    [Fact]
    public void ByteArray_RoundTrips_AndDeserializeCopiesOut()
    {
        byte[] value = { 0x00, 0x01, 0xFE, 0xFF };

        byte[]? serialized = Serdes.ByteArray.Serialize(Topic, value);
        Assert.Same(value, serialized); // identity — same reference (Java parity)

        byte[] decoded = Serdes.ByteArray.Deserialize(Topic, value);
        Assert.Equal(value, decoded);
        Assert.NotSame(value, decoded); // deserialize copies the borrowed span out
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

    // ---- Int32 (4 bytes big-endian) ---------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(256)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void Int32_RoundTrips(int value)
    {
        byte[]? bytes = Serdes.Int32.Serialize(Topic, value);
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Int32.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Int32_WireFormat_IsBigEndian()
    {
        // Java IntegerSerializer: 1 -> {0x00,0x00,0x00,0x01}. Little-endian would be
        // {0x01,0x00,0x00,0x00}; 256 -> {0x00,0x00,0x01,0x00} pins the byte order.
        Assert.Equal(new byte[] { 0x00, 0x00, 0x00, 0x01 }, Serdes.Int32.Serialize(Topic, 1));
        Assert.Equal(new byte[] { 0x00, 0x00, 0x01, 0x00 }, Serdes.Int32.Serialize(Topic, 256));
        Assert.Equal(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, Serdes.Int32.Serialize(Topic, -1));
        Assert.Equal(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF }, Serdes.Int32.Serialize(Topic, int.MaxValue));
    }

    // ---- Int64 (8 bytes big-endian) ---------------------------------------------

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void Int64_RoundTrips(long value)
    {
        byte[]? bytes = Serdes.Int64.Serialize(Topic, value);
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Int64.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Int64_WireFormat_IsBigEndian()
    {
        // Java LongSerializer: 1L -> {0,0,0,0,0,0,0,1}.
        Assert.Equal(
            new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 },
            Serdes.Int64.Serialize(Topic, 1L));
        Assert.Equal(
            new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 },
            Serdes.Int64.Serialize(Topic, 0x0102030405060708L));
    }

    // ---- Double (8 bytes big-endian IEEE-754) -----------------------------------

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
        byte[]? bytes = Serdes.Double.Serialize(Topic, value);
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Double.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Double_NaN_RoundTrips()
    {
        byte[]? bytes = Serdes.Double.Serialize(Topic, double.NaN);
        Assert.NotNull(bytes);
        Assert.True(double.IsNaN(Serdes.Double.Deserialize(Topic, bytes)));
    }

    [Fact]
    public void Double_WireFormat_IsBigEndianDoubleToLongBits()
    {
        // Java DoubleSerializer: doubleToLongBits(1.0) = 0x3FF0000000000000, big-endian.
        Assert.Equal(
            new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            Serdes.Double.Serialize(Topic, 1.0));

        // NaN canonicalizes to 0x7ff8000000000000L (Java doubleToLongBits).
        Assert.Equal(
            new byte[] { 0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            Serdes.Double.Serialize(Topic, double.NaN));
    }

    // ---- Guid (UUID.toString() -> UTF-8) ----------------------------------------

    [Fact]
    public void Guid_RoundTrips()
    {
        Guid value = Guid.NewGuid();
        byte[]? bytes = Serdes.Guid.Serialize(Topic, value);
        Assert.NotNull(bytes);
        Assert.Equal(value, Serdes.Guid.Deserialize(Topic, bytes));
    }

    [Fact]
    public void Guid_WireFormat_IsCanonicalLowercaseStringUtf8_NotRawBytes()
    {
        // ⚠ Java UUIDSerializer serializes UUID.toString() as UTF-8 — the 36-char
        // canonical lowercase form — NOT the 16 raw bytes. Constructed from UPPERCASE
        // to prove the wire form is lowercased (Guid.ToString("D") == UUID.toString()).
        var value = new Guid("A1B2C3D4-E5F6-7788-99AA-BBCCDDEEFF00");
        const string canonical = "a1b2c3d4-e5f6-7788-99aa-bbccddeeff00";

        byte[]? bytes = Serdes.Guid.Serialize(Topic, value);

        Assert.Equal(Encoding.UTF8.GetBytes(canonical), bytes);
        Assert.Equal(36, bytes!.Length);   // string form, not 16 raw bytes
        Assert.NotEqual(16, bytes.Length);
    }

    [Fact]
    public void Guid_DeserializeParsesCanonicalString()
    {
        byte[] wire = Encoding.UTF8.GetBytes("12345678-9abc-def0-1234-56789abcdef0");
        Guid decoded = Serdes.Guid.Deserialize(Topic, wire);
        Assert.Equal(new Guid("12345678-9abc-def0-1234-56789abcdef0"), decoded);
    }

    // ---- Null (VoidSerializer) --------------------------------------------------

    [Fact]
    public void Null_SerializeReturnsNull_DeserializeReturnsDefault()
    {
        // Java VoidSerializer always returns null; VoidDeserializer yields null.
        Assert.Null(Serdes.Null.Serialize(Topic, null));
        Assert.Null(Serdes.Null.Serialize(Topic, new object()));
        Assert.Null(Serdes.Null.Deserialize(Topic, ReadOnlySpan<byte>.Empty));
        Assert.Null(Serdes.Null.Deserialize(Topic, new byte[] { 1, 2, 3 }));
    }

    // ---- Malformed input -> SerializationException (Java's exact messages) -------

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
        // Java's byte[] overload message says "Deserializer", not "DoubleDeserializer".
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
        Assert.NotNull(ex.InnerException); // the underlying parse failure is preserved
    }

    // ---- SerializationException is a KafkaException -----------------------------

    [Fact]
    public void SerializationException_IsCatchableAsKafkaException()
    {
        // Base-type catch: SerializationException derives from KafkaException, so an
        // existing catch (KafkaException) sees it (ThrowsAny allows the subclass).
        KafkaException ex = Assert.ThrowsAny<KafkaException>(
            () => Serdes.Int32.Deserialize(Topic, new byte[] { 0x00 }));
        Assert.IsType<SerializationException>(ex);
        Assert.IsAssignableFrom<KafkaException>(ex);

        // A serde error originates in the binding, not the core: no error code / flags.
        Assert.Equal(0, ex.Code);
        Assert.False(ex.IsRetriable);
        Assert.False(ex.IsFatal);
    }
}
