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
using System.Collections.Generic;

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>UUIDDeserializer</c>: the string form of a UUID, in the configured encoding, as a <see cref="Guid"/>, read
/// as Java's <c>UUID.fromString</c> reads it. The encoding is UTF-8 until
/// <see cref="Configure(IReadOnlyDictionary{string, object}, bool)"/> finds a string under
/// <c>key.deserializer.encoding</c> or <c>value.deserializer.encoding</c>, as its <c>isKey</c> says, or else under
/// <c>deserializer.encoding</c>. As in Java, <c>Configure</c> stores the name unchecked and <c>Deserialize</c> refuses
/// it when it is none of UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE, US-ASCII and ISO-8859-1 (a name
/// or an alias), where Java accepts every charset its runtime has.
/// </summary>
public sealed class UUIDDeserializer : IDeserializerWithConfigure<Guid?>
{
    private string _encoding = "UTF-8";

    private JavaCharset? _charset = JavaCharset.Utf8;

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        string? name = JavaCharset.ConfiguredName(
            configs, isKey ? "key.deserializer.encoding" : "value.deserializer.encoding", "deserializer.encoding");
        if (name is not null)
        {
            _encoding = name;
            _charset = JavaCharset.ForName(name);
        }
    }

    /// <inheritdoc/>
    /// <exception cref="SerializationException">
    /// The configured encoding is not supported ("Error when deserializing ByteBuffer to UUID due to unsupported
    /// encoding X", caused by an <see cref="IllegalArgumentException"/>), or the text is not a UUID ("Error parsing
    /// data into UUID").
    /// </exception>
    public Guid? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        if (_charset is null)
        {
            throw new SerializationException(
                "Error when deserializing ByteBuffer to UUID due to unsupported encoding " + _encoding,
                new IllegalArgumentException(_encoding));
        }

        string text = _charset.Decode(data);
        try
        {
            return JavaUuid.FromString(text);
        }
        catch (IllegalArgumentException e)
        {
            throw new SerializationException("Error parsing data into UUID", e);
        }
    }
}
