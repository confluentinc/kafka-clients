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

namespace Confluent.Kafka.Internal.Serialization;

/// <summary>
/// IEEE-754 double serde — mirrors Java's <c>DoubleSerializer</c> /
/// <c>DoubleDeserializer</c>: the 8 raw <c>Double.doubleToLongBits</c> bytes,
/// <b>big-endian</b>. Backs <see cref="Serdes.Double"/>.
/// </summary>
internal sealed class DoubleSerde : ISerde<double>
{
    // Java's canonical NaN from Double.doubleToLongBits (0x7ff8000000000000L).
    // BitConverter.DoubleToInt64Bits is the raw-bits form (like doubleToRawLongBits)
    // and does NOT canonicalize, so canonicalize here to stay wire-identical to Java
    // for NaN payloads.
    private const long CanonicalNaNBits = 0x7ff8000000000000L;

    // Java DoubleSerializer.serialize: bits = Double.doubleToLongBits(data), then 8
    // bytes big-endian.
    public byte[]? Serialize(string topic, double data)
    {
        long bits = double.IsNaN(data) ? CanonicalNaNBits : BitConverter.DoubleToInt64Bits(data);
        return new[]
        {
            (byte)(bits >> 56),
            (byte)(bits >> 48),
            (byte)(bits >> 40),
            (byte)(bits >> 32),
            (byte)(bits >> 24),
            (byte)(bits >> 16),
            (byte)(bits >> 8),
            (byte)bits,
        };
    }

    // Java DoubleDeserializer.deserialize(byte[]): length must be exactly 8, else
    // SerializationException. NOTE the message says "Deserializer", not
    // "DoubleDeserializer" — a Java quirk on the byte[] overload (which this
    // span-based method mirrors); matched verbatim for wire/behaviour parity.
    public double Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (data.Length != 8)
        {
            throw new SerializationException(
                "Size of data received by Deserializer is not 8");
        }

        long value = 0;
        foreach (byte b in data)
        {
            value <<= 8;
            value |= (long)(b & 0xFF);
        }

        return BitConverter.Int64BitsToDouble(value);
    }
}
