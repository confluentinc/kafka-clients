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
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.Internal.Serialization;

/// <summary>
/// The serde behind <see cref="Serdes.Int32"/>: 4 big-endian bytes, as Java's <c>IntegerSerializer</c> and
/// <c>IntegerDeserializer</c>.
/// </summary>
internal sealed class Int32Serde : ISerde<int>
{
    public byte[]? Serialize(string topic, int data) =>
        new[]
        {
            (byte)(data >> 24),
            (byte)(data >> 16),
            (byte)(data >> 8),
            (byte)data,
        };

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
