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

using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Common.Header;

/// <summary>
/// A record header, Java's <c>Header</c>: a key and an optional value. The headers of a
/// <see cref="ConsumerRecord{TKey, TValue}"/> are owned copies.
/// </summary>
/// <remarks>
/// <see cref="Value"/> is a <c>byte[]</c>, as Java's <c>Header.value()</c> is, not a
/// <see cref="System.ReadOnlyMemory{T}"/>.
/// </remarks>
public sealed class Header
{
    /// <summary>Initializes a header with a key and a value, which may be <see langword="null"/>.</summary>
    public Header(string key, byte[]? value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>The header key.</summary>
    public string Key { get; }

    /// <summary>The header value, or <see langword="null"/> for a null value.</summary>
    public byte[]? Value { get; }
}
