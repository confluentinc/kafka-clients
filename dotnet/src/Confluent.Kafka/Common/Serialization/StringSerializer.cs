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
/// Java's <c>StringSerializer</c>: a string as its bytes in the configured encoding, and <see langword="null"/> as
/// <see langword="null"/>. The encoding is UTF-8 until
/// <see cref="Configure(IReadOnlyDictionary{string, object}, bool)"/> finds a string under
/// <c>key.serializer.encoding</c> or <c>value.serializer.encoding</c>, as its <c>isKey</c> says, or else under
/// <c>serializer.encoding</c>; any other value leaves it unchanged. It is a Java charset name or alias, one of UTF-8,
/// UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE, US-ASCII and ISO-8859-1, written with Java's bytes (in UTF-8
/// an unpaired surrogate is <c>'?'</c>, as Java's <c>String.getBytes(UTF_8)</c> writes it).
/// </summary>
public sealed class StringSerializer : ISerializerWithConfigure<string>
{
    private JavaCharset _encoding = JavaCharset.Utf8;

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="SerializationException">
    /// The encoding is none of these names: "Unsupported encoding X", caused by an
    /// <see cref="IllegalArgumentException"/>, where Java accepts every charset its runtime has.
    /// </exception>
    public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        string? name = JavaCharset.ConfiguredName(
            configs, isKey ? "key.serializer.encoding" : "value.serializer.encoding", "serializer.encoding");
        if (name is not null)
        {
            _encoding = JavaCharset.Configured(name);
        }
    }

    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, string? data) =>
        data is null ? (ReadOnlyMemory<byte>?)null : _encoding.Encode(data);
}
