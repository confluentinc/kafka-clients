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
/// 32-bit integer serde — mirrors Java's <c>IntegerSerializer</c> /
/// <c>IntegerDeserializer</c>: 4 bytes, <b>big-endian</b>. Backs
/// <see cref="Serdes.Int32"/>.
/// </summary>
internal sealed class Int32Serde : ISerde<int>
{
    // Java IntegerSerializer.serialize: { (byte)(data>>>24), (byte)(data>>>16),
    // (byte)(data>>>8), data.byteValue() } — 4 bytes big-endian. The (byte) cast keeps
    // the low 8 bits, so an arithmetic `>>` on a negative value is byte-identical to
    // Java's logical `>>>` here.
    public byte[]? Serialize(string topic, int data) =>
        new[]
        {
            (byte)(data >> 24),
            (byte)(data >> 16),
            (byte)(data >> 8),
            (byte)data,
        };

    // Java IntegerDeserializer.deserialize: length must be exactly 4, else
    // SerializationException with this exact message; accumulate big-endian.
    public int Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (data.Length != 4)
        {
            throw new SerializationException(
                "Size of data received by IntegerDeserializer is not 4");
        }

        int value = 0;
        foreach (byte b in data)
        {
            value <<= 8;
            value |= b & 0xFF;
        }

        return value;
    }
}
