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

using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>DoubleDeserializer</c>: 8 big-endian IEEE-754 bytes as a <see cref="double"/>, Java's nullable
/// <c>Double</c>.
/// </summary>
public sealed class DoubleDeserializer : IDeserializer<double?>
{
    /// <inheritdoc/>
    public double? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (data.Length != 8)
        {
            throw new SerializationException("Size of data received by DoubleDeserializer is not 8");
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
