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
/// Java's <c>IntegerSerializer</c>: an <see cref="int"/> as its 4 big-endian bytes, and <see langword="null"/> as
/// <see langword="null"/>, Java's nullable <c>Integer</c>.
/// </summary>
public sealed class IntegerSerializer : ISerializer<int?>
{
    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, int? data)
    {
        if (data is not int value)
        {
            return null;
        }

        return new[]
        {
            (byte)(value >> 24),
            (byte)(value >> 16),
            (byte)(value >> 8),
            (byte)value,
        };
    }
}
