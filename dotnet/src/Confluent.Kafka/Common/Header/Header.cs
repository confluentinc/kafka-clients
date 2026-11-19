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

using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Common.Header;

/// <summary>
/// A record header, Java's <c>Header</c>: a key and an optional value, with the equality, hash and string form of
/// Java's <c>RecordHeader</c>. The headers of a <see cref="ConsumerRecord{TKey, TValue}"/> are owned copies.
/// </summary>
/// <remarks>
/// <see cref="Value()"/> is a <c>byte[]</c>, as Java's <c>Header.value()</c> is, not a
/// <see cref="System.ReadOnlyMemory{T}"/>.
/// </remarks>
public sealed class Header
{
    private readonly string _key;
    private readonly byte[]? _value;

    /// <summary>Initializes a header with a key and a value, which may be <see langword="null"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public Header(string key, byte[]? value)
    {
        _key = key ?? throw new ArgumentNullException(nameof(key), "Null header keys are not permitted");
        _value = value;
    }

    /// <summary>The header key.</summary>
    /// <returns>The header key.</returns>
    public string Key() => _key;

    /// <summary>The header value, or <see langword="null"/> for a null value.</summary>
    /// <returns>The header value, or <see langword="null"/> for a null value.</returns>
    public byte[]? Value() => _value;

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is Header other
            && string.Equals(Key(), other.Key(), StringComparison.Ordinal)
            && ValueEquals(Value(), other.Value());
    }

    /// <inheritdoc/>
    public override int GetHashCode() => unchecked((31 * StringComparer.Ordinal.GetHashCode(Key())) + ValueHashCode(Value()));

    /// <inheritdoc/>
    public override string ToString() => "RecordHeader(key = " + Key() + ", value = " + ValueToString(Value()) + ")";

    private static bool ValueEquals(byte[]? a, byte[]? b)
    {
        if (a is null || b is null)
        {
            return a is null && b is null;
        }

        return a.AsSpan().SequenceEqual(b);
    }

    // Java's Arrays.hashCode(byte[]): 0 for null, else the 31-fold over the signed bytes.
    private static int ValueHashCode(byte[]? value)
    {
        if (value is null)
        {
            return 0;
        }

        int result = 1;
        foreach (byte b in value)
        {
            result = unchecked((31 * result) + (sbyte)b);
        }

        return result;
    }

    // Java's Arrays.toString(byte[]): "null", or the signed bytes in brackets.
    private static string ValueToString(byte[]? value)
    {
        if (value is null)
        {
            return "null";
        }

        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < value.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(((sbyte)value[i]).ToString(CultureInfo.InvariantCulture));
        }

        return builder.Append(']').ToString();
    }
}
