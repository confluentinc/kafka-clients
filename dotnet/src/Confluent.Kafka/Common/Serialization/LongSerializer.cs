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
/// Java's <c>LongSerializer</c>: a <see cref="long"/> as its 8 big-endian bytes, and <see langword="null"/> as
/// <see langword="null"/>, Java's nullable <c>Long</c>.
/// </summary>
public sealed class LongSerializer : ISerializer<long?>
{
    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, long? data)
    {
        if (data is not long value)
        {
            return null;
        }

        return new[]
        {
            (byte)(value >> 56),
            (byte)(value >> 48),
            (byte)(value >> 40),
            (byte)(value >> 32),
            (byte)(value >> 24),
            (byte)(value >> 16),
            (byte)(value >> 8),
            (byte)value,
        };
    }
}
