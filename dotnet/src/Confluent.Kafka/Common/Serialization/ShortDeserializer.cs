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
/// Java's <c>ShortDeserializer</c>: 2 big-endian bytes as a <see cref="short"/>, Java's nullable <c>Short</c>.
/// </summary>
public sealed class ShortDeserializer : IDeserializer<short?>
{
    /// <inheritdoc/>
    public short? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (data.Length != 2)
        {
            throw new SerializationException("Size of data received by ShortDeserializer is not 2");
        }

        return unchecked((short)((data[0] << 8) | data[1]));
    }
}
