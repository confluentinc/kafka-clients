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
/// UUID serde — mirrors Java's <c>UUIDSerializer</c> / <c>UUIDDeserializer</c>. Backs
/// <see cref="Serdes.Guid"/>.
/// </summary>
/// <remarks>
/// ⚠ Java serializes the UUID's <b>string form</b>, not its 16 raw bytes:
/// <c>UUIDSerializer.serialize</c> is <c>data.toString().getBytes(UTF_8)</c> and
/// <c>UUIDDeserializer.deserialize</c> is <c>UUID.fromString(new String(data, UTF_8))</c>.
/// So the wire payload is the 36-char canonical lowercase form (e.g.
/// <c>"12345678-1234-5678-1234-567812345678"</c>) as UTF-8 — <b>not</b>
/// <c>Guid.ToByteArray()</c>, whose field byte order differs from Java's UUID layout.
/// Going through the string form sidesteps that endianness mismatch entirely and is
/// byte-identical to Java: <see cref="Guid.ToString()"/> and Java's
/// <c>UUID.toString()</c> both emit the same canonical lowercase text.
/// </remarks>
internal sealed class GuidSerde : ISerde<System.Guid>
{
    // Java UUIDSerializer.serialize: data.toString().getBytes(UTF_8). Guid.ToString()
    // (default "D" format) is the lowercase 8-4-4-4-12 canonical form == UUID.toString().
    public byte[]? Serialize(string topic, System.Guid data) =>
        Encoding.UTF8.GetBytes(data.ToString("D", CultureInfo.InvariantCulture));

    // Java UUIDDeserializer.deserialize: UUID.fromString(new String(data, UTF_8)); an
    // IllegalArgumentException (bad UUID text) -> SerializationException("Error parsing
    // data into UUID", e). Guid parsing throws FormatException/OverflowException for the
    // same malformed-text cases.
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
