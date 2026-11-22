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
using System.Runtime.CompilerServices;
using System.Text;

using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Header.Internals;

/// <summary>
/// A record header: a key and an optional value, with Java's equality, hash and string form, so it equals only another
/// <see cref="RecordHeader"/>. The headers of a <see cref="ConsumerRecord{TKey, TValue}"/> are owned copies.
/// </summary>
/// <remarks>
/// <see cref="Value()"/> is a <c>byte[]</c>, as Java's <c>Header.value()</c> is, not a
/// <see cref="System.ReadOnlyMemory{T}"/>.
/// </remarks>
internal sealed class RecordHeader : IHeader
{
    // Java's four fields in two, so a header built from a string and an array holds no buffer field: the key is its
    // string or its buffer, the value null, its array or its buffer, until the first read replaces the buffer under
    // the lock.
    private volatile object _key;
    private volatile object? _value;

    /// <summary>Initializes a header with a key and a value, which may be <see langword="null"/>.</summary>
    /// <param name="key">The header's key.</param>
    /// <param name="value">The header's value, or <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public RecordHeader(string key, byte[]? value)
    {
        _key = key ?? throw new ArgumentNullException(nameof(key), "Null header keys are not permitted");
        _value = value;
    }

    /// <summary>
    /// Initializes a header over a key buffer and a value buffer, which may be <see langword="null"/>. The key is
    /// decoded as UTF-8, and the value copied to a new array, on first read; the header then drops the buffer.
    /// </summary>
    /// <param name="keyBuffer">The header's key, UTF-8 encoded.</param>
    /// <param name="valueBuffer">The header's value, or <see langword="null"/>.</param>
    public RecordHeader(ReadOnlyMemory<byte> keyBuffer, ReadOnlyMemory<byte>? valueBuffer)
    {
        _key = new StrongBox<ReadOnlyMemory<byte>>(keyBuffer);
        _value = valueBuffer.HasValue
            ? new StrongBox<ReadOnlyMemory<byte>>(valueBuffer.GetValueOrDefault())
            : null;
    }

    /// <inheritdoc/>
    public string Key()
    {
        if (_key is string key)
        {
            return key;
        }

        lock (this)
        {
            if (_key is StrongBox<ReadOnlyMemory<byte>> buffer)
            {
                _key = JavaCharset.Utf8.Decode(buffer.Value.Span);
            }

            return (string)_key;
        }
    }

    /// <inheritdoc/>
    public byte[]? Value()
    {
        object? value = _value;
        if (value is not StrongBox<ReadOnlyMemory<byte>>)
        {
            return (byte[]?)value;
        }

        lock (this)
        {
            if (_value is StrongBox<ReadOnlyMemory<byte>> buffer)
            {
                _value = buffer.Value.ToArray();
            }

            return (byte[]?)_value;
        }
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is RecordHeader other
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
