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

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>DoubleSerializer</c>: the 8 big-endian bytes of Java's <c>Double.doubleToLongBits</c> (every NaN as the
/// canonical one), and <see langword="null"/> as <see langword="null"/>, Java's nullable <c>Double</c>.
/// </summary>
public sealed class DoubleSerializer : ISerializer<double?>
{
    private const long CanonicalNaNBits = 0x7ff8000000000000L;

    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, double? data)
    {
        if (data is not double value)
        {
            return null;
        }

        long bits = double.IsNaN(value) ? CanonicalNaNBits : BitConverter.DoubleToInt64Bits(value);
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
}
