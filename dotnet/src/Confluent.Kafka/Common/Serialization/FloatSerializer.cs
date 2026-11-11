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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>FloatSerializer</c>: the 4 big-endian bytes of Java's <c>Float.floatToRawIntBits</c> (a NaN's payload
/// kept), and <see langword="null"/> as <see langword="null"/>, Java's nullable <c>Float</c>.
/// </summary>
public sealed class FloatSerializer : ISerializer<float?>
{
    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, float? data)
    {
        if (data is not float value)
        {
            return null;
        }

        int bits = JavaFloat.FloatToRawIntBits(value);
        return new[]
        {
            (byte)(bits >> 24),
            (byte)(bits >> 16),
            (byte)(bits >> 8),
            (byte)bits,
        };
    }
}
