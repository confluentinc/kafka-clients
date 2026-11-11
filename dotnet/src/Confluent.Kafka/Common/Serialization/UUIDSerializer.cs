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
using System.Globalization;

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>UUIDSerializer</c>: a UUID as the bytes of its lowercase hyphenated string form, not its 16 raw bytes, in
/// the configured encoding, and <see langword="null"/> as <see langword="null"/>. The encoding is UTF-8 until
/// <see cref="Configure(IReadOnlyDictionary{string, object}, bool)"/> finds a string under
/// <c>key.serializer.encoding</c> or <c>value.serializer.encoding</c>, as its <c>isKey</c> says, or else under
/// <c>serializer.encoding</c>. As in Java, <c>Configure</c> stores the name unchecked and <c>Serialize</c> refuses it
/// when it is none of UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE, US-ASCII and ISO-8859-1 (a name or
/// an alias), where Java accepts every charset its runtime has.
/// </summary>
public sealed class UUIDSerializer : ISerializerWithConfigure<Guid?>
{
    private string _encoding = "UTF-8";

    private JavaCharset? _charset = JavaCharset.Utf8;

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        string? name = JavaCharset.ConfiguredName(
            configs, isKey ? "key.serializer.encoding" : "value.serializer.encoding", "serializer.encoding");
        if (name is not null)
        {
            _encoding = name;
            _charset = JavaCharset.ForName(name);
        }
    }

    /// <inheritdoc/>
    /// <exception cref="SerializationException">
    /// <paramref name="data"/> is not <see langword="null"/> and the configured encoding is not supported: "Error when
    /// serializing UUID to byte[] due to unsupported encoding X", with no cause, as in Java.
    /// </exception>
    public ReadOnlyMemory<byte>? Serialize(string topic, Guid? data)
    {
        if (data is not Guid value)
        {
            return null;
        }

        if (_charset is null)
        {
            throw new SerializationException(
                "Error when serializing UUID to byte[] due to unsupported encoding " + _encoding);
        }

        return _charset.Encode(value.ToString("D", CultureInfo.InvariantCulture));
    }
}
