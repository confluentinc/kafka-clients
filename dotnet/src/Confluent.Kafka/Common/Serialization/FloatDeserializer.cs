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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>FloatDeserializer</c>: 4 big-endian IEEE-754 bytes as a <see cref="float"/>, a NaN's payload kept, Java's
/// nullable <c>Float</c>.
/// </summary>
public sealed class FloatDeserializer : IDeserializer<float?>
{
    /// <inheritdoc/>
    public float? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (data.Length != 4)
        {
            throw new SerializationException("Size of data received by Deserializer is not 4");
        }

        int value = 0;
        foreach (byte b in data)
        {
            value <<= 8;
            value |= b & 0xFF;
        }

        return JavaFloat.IntBitsToFloat(value);
    }
}
