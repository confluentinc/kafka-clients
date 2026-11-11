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
using System.Globalization;

using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>BooleanDeserializer</c>: the byte <c>0x01</c> as <see langword="true"/> and <c>0x00</c> as
/// <see langword="false"/>, Java's nullable <c>Boolean</c>. Any other byte is refused with Java's message, which prints
/// it signed, as Java's <c>byte</c> (<c>0xFF</c> as <c>-1</c>).
/// </summary>
public sealed class BooleanDeserializer : IDeserializer<bool?>
{
    private const byte True = 0x01;
    private const byte False = 0x00;

    /// <inheritdoc/>
    public bool? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (data.Length != 1)
        {
            throw new SerializationException("Size of data received by BooleanDeserializer is not 1");
        }

        byte b = data[0];
        if (b == True)
        {
            return true;
        }

        if (b == False)
        {
            return false;
        }

        throw new SerializationException(
            "Unexpected byte received by BooleanDeserializer: " + unchecked((sbyte)b).ToString(CultureInfo.InvariantCulture));
    }
}
