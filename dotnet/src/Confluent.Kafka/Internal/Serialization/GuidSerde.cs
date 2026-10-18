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
using System.Globalization;
using System.Text;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal.Serialization;

/// <summary>
/// The serde behind <see cref="Serdes.Guid"/>: a UUID as the UTF-8 bytes of its lowercase hyphenated string form, not
/// its 16 raw bytes, as Java's <c>UUIDSerializer</c> and <c>UUIDDeserializer</c>.
/// </summary>
internal sealed class GuidSerde : ISerde<System.Guid>
{
    public byte[]? Serialize(string topic, System.Guid data) =>
        Encoding.UTF8.GetBytes(data.ToString("D", CultureInfo.InvariantCulture));

    public System.Guid Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        string text = Utf8Marshal.GetString(data);
        try
        {
            return System.Guid.Parse(text);
        }
        catch (FormatException e)
        {
            throw new SerializationException("Error parsing data into UUID", e);
        }
        catch (OverflowException e)
        {
            throw new SerializationException("Error parsing data into UUID", e);
        }
    }
}
