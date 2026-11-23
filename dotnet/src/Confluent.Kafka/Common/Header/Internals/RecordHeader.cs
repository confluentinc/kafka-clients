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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common.Header.Internals;

/// <summary>
/// A record header: a key and an optional value, with Java's equality, hash and string form, so it equals only another
/// <see cref="RecordHeader"/>. A header of a polled <see cref="ConsumerRecord{TKey, TValue}"/> reads its key and value
/// from the polled batch on first use, as Java's <c>RecordHeader</c> decodes its key and copies its value on first use.
/// </summary>
/// <remarks>
/// <see cref="Value()"/> is a <c>byte[]</c>, as Java's <c>Header.value()</c> is; <see cref="ValueMemory()"/>, which
/// users reach through <see cref="HeaderExtensions.ValueMemory(IHeader)"/>, gives the same bytes as a
/// <see cref="ReadOnlyMemory{T}"/>, which for a polled header is a view of the batch, not a copy, until
/// <see cref="Value()"/> has copied the value, and that array afterwards.
/// </remarks>
internal sealed class RecordHeader : IHeader
{
    // The polled batch a fetched header reads its key and value from, and the header's view in it; null for a header
    // built from a key and a value or from buffers.
    private readonly SafeConsumerRecordsHandle? _batch;
    private readonly IntPtr _header;

    // The key is its string or, for a header built from buffers, its buffer until the first read replaces it under the
    // lock; for a fetched header null until its first read decodes it. The value is null, its array or, for a header
    // built from buffers, its buffer until the first read replaces it under the lock; for a fetched header it is
    // copied on its first read, once _valueRead is set.
    private volatile object? _key;
    private volatile object? _value;
    private volatile bool _valueRead;

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

    // A header of a polled record: `header` is a kafka_common_header_Header_t view, valid as long as `batch`.
    internal RecordHeader(SafeConsumerRecordsHandle batch, IntPtr header)
    {
        _batch = batch;
        _header = header;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A polled header decodes its key once, on the first call, from the key's UTF-8 bytes, a NUL byte included.
    /// </remarks>
    public string Key()
    {
        object? key = _key;
        if (key is string text)
        {
            return text;
        }

        if (_batch is not null)
        {
            return ReadKey();
        }

        lock (this)
        {
            if (_key is StrongBox<ReadOnlyMemory<byte>> buffer)
            {
                _key = JavaCharset.Utf8.Decode(buffer.Value.Span);
            }

            return (string)_key!;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A polled header copies its value out of the batch once, on the first call, and returns that array on every
    /// call, as Java's <c>RecordHeader.value()</c> does.
    /// </remarks>
    public byte[]? Value()
    {
        if (_batch is not null)
        {
            return _valueRead ? (byte[]?)_value : ReadValue();
        }

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

    // The header value as a read-only memory, or null for a null value; an empty value is an empty memory. For a
    // header of a polled record the memory is a view of the polled batch, with no copy, which keeps the batch alive
    // while it is reachable, until Value() has been called; from then on it is the array Value() returns. A memory
    // taken before the first Value() call stays a view of the batch. For a header built from buffers it is the value
    // buffer until Value() has copied it, then that array; for a header built with a byte[] it is that array.
    internal ReadOnlyMemory<byte>? ValueMemory()
    {
        // _valueRead is set after _value is published, so once it is seen the cached array is the value.
        if (_batch is null || _valueRead)
        {
            // Not a conditional with a null branch: null converts to ReadOnlyMemory<byte> through its byte[]
            // conversion, giving an empty memory instead of null.
            object? value = _value;
            if (value is null)
            {
                return null;
            }

            if (value is StrongBox<ReadOnlyMemory<byte>> buffer)
            {
                return buffer.Value;
            }

            return new ReadOnlyMemory<byte>((byte[])value);
        }

        KafkaBytes bytes = ReadValueBytes();
        if (bytes.Data == IntPtr.Zero)
        {
            return null;
        }

        ReadOnlyMemory<byte> memory = _batch.HandOut(bytes.Data, bytes.Length);
        GC.KeepAlive(_batch);
        return memory;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not RecordHeader other || !string.Equals(Key(), other.Key(), StringComparison.Ordinal))
        {
            return false;
        }

        bool hasValue = ValueSpan(out ReadOnlySpan<byte> value);
        bool otherHasValue = other.ValueSpan(out ReadOnlySpan<byte> otherValue);
        bool equal = hasValue == otherHasValue && (!hasValue || value.SequenceEqual(otherValue));
        GC.KeepAlive(this);
        GC.KeepAlive(other);
        return equal;
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int keyHash = StringComparer.Ordinal.GetHashCode(Key());
        bool hasValue = ValueSpan(out ReadOnlySpan<byte> value);
        int valueHash = hasValue ? ValueHashCode(value) : 0;
        GC.KeepAlive(this);
        return unchecked((31 * keyHash) + valueHash);
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        string key = Key();
        bool hasValue = ValueSpan(out ReadOnlySpan<byte> value);
        string text = hasValue ? ValueToString(value) : "null";
        GC.KeepAlive(this);
        return "RecordHeader(key = " + key + ", value = " + text + ")";
    }

    // An owned copy, which references no batch: a header not read from a batch itself.
    internal RecordHeader ToOwned() => _batch is null ? this : new RecordHeader(Key(), Value());

    // Decodes a key's UTF-8 bytes as Java's new String(bytes, UTF_8), a NUL byte included; an invalid sequence
    // becomes U+FFFD.
    internal static unsafe string DecodeKey(KafkaBytes bytes) =>
        bytes.Data == IntPtr.Zero || bytes.Length <= 0
            ? string.Empty
            : JavaCharset.Utf8.Decode(new ReadOnlySpan<byte>((void*)bytes.Data, bytes.Length));

    // The value, without a copy, by ValueMemory's rule: the built array or buffer, the array Value() cached once it
    // was called, or else a view of the batch, which the caller keeps alive (by keeping this header alive) while it
    // reads the span. False for a null value.
    internal unsafe bool ValueSpan(out ReadOnlySpan<byte> value)
    {
        if (_batch is null || _valueRead)
        {
            object? current = _value;
            switch (current)
            {
                case null:
                    value = default;
                    return false;
                case StrongBox<ReadOnlyMemory<byte>> buffer:
                    value = buffer.Value.Span;
                    return true;
                default:
                    value = (byte[])current;
                    return true;
            }
        }

        KafkaBytes bytes = ReadValueBytes();
        if (bytes.Data == IntPtr.Zero)
        {
            value = default;
            return false;
        }

        value = new ReadOnlySpan<byte>((void*)bytes.Data, Math.Max(bytes.Length, 0));
        return true;
    }

    private string ReadKey()
    {
        IntPtr outErr = IntPtr.Zero;
        KafkaBytes bytes = NativeMethods.HeaderKeyBytes(_header, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string key = DecodeKey(bytes);
        GC.KeepAlive(_batch);
        return (string?)Interlocked.CompareExchange(ref _key, key, null) ?? key;
    }

    private byte[]? ReadValue()
    {
        KafkaBytes bytes = ReadValueBytes();
        byte[]? copy = null;
        if (bytes.Data != IntPtr.Zero)
        {
            copy = bytes.Length <= 0 ? Array.Empty<byte>() : new byte[bytes.Length];
            if (copy.Length > 0)
            {
                Marshal.Copy(bytes.Data, copy, 0, copy.Length);
            }
        }

        GC.KeepAlive(_batch);
        byte[]? value = copy is null ? null : (byte[]?)Interlocked.CompareExchange(ref _value, copy, null) ?? copy;
        _valueRead = true;
        return value;
    }

    // The caller keeps the batch alive until it has read the view.
    private KafkaBytes ReadValueBytes()
    {
        IntPtr outErr = IntPtr.Zero;
        KafkaBytes bytes = NativeMethods.HeaderValue(_header, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return bytes;
    }

    // Java's Arrays.hashCode(byte[]) of a non-null array: the 31-fold over the signed bytes.
    private static int ValueHashCode(ReadOnlySpan<byte> value)
    {
        int result = 1;
        foreach (byte b in value)
        {
            result = unchecked((31 * result) + (sbyte)b);
        }

        return result;
    }

    // Java's Arrays.toString(byte[]) of a non-null array: the signed bytes in brackets.
    private static string ValueToString(ReadOnlySpan<byte> value)
    {
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
