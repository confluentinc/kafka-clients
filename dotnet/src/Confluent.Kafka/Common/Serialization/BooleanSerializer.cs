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
/// Java's <c>BooleanSerializer</c>: one byte, <c>0x01</c> for <see langword="true"/> and <c>0x00</c> for
/// <see langword="false"/>, and <see langword="null"/> as <see langword="null"/>, Java's nullable <c>Boolean</c>.
/// </summary>
public sealed class BooleanSerializer : ISerializer<bool?>
{
    private const byte True = 0x01;
    private const byte False = 0x00;

    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, bool? data)
    {
        if (data is not bool value)
        {
            return null;
        }

        return new[] { value ? True : False };
    }
}
