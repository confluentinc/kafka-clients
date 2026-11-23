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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common.Header;

/// <summary>
/// A record header, Java's <c>Header</c>: a key and an optional value, with the equality, hash and string form of
/// Java's <c>RecordHeader</c>. A header of a polled <see cref="ConsumerRecord{TKey, TValue}"/> reads its key and value
/// from the polled batch on first use, as Java's <c>RecordHeader</c> decodes its key and copies its value on first use.
/// </summary>
/// <remarks>
/// <see cref="Value()"/> is a <c>byte[]</c>, as Java's <c>Header.value()</c> is; <see cref="ValueMemory()"/> gives
/// the same bytes as a <see cref="ReadOnlyMemory{T}"/>, which for a polled header is a view of the batch, not a copy,
/// until <see cref="Value()"/> has copied the value, and that array afterwards.
/// </remarks>
public sealed class Header
{
    // The polled batch a fetched header reads its key and value from, and the header's view in it; null for a header
    // built with a key and a value.
    private readonly SafeConsumerRecordsHandle? _batch;
    private readonly IntPtr _header;

    // A fetched header's key, decoded on its first read, and its value, copied on its first read once _valueRead is
    // set; always set for a built header.
    private string? _key;
    private byte[]? _value;
    private volatile bool _valueRead;

    /// <summary>Initializes a header with a key and a value, which may be <see langword="null"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public Header(string key, byte[]? value)
    {
        _key = key ?? throw new ArgumentNullException(nameof(key), "Null header keys are not permitted");
        _value = value;
    }

    // A header of a polled record: `header` is a kafka_common_header_Header_t view, valid as long as `batch`.
    internal Header(SafeConsumerRecordsHandle batch, IntPtr header)
    {
        _batch = batch;
        _header = header;
    }

    /// <summary>The header key.</summary>
    /// <returns>The header key.</returns>
    /// <remarks>
    /// A polled header decodes its key once, on the first call, from the key's UTF-8 bytes, a NUL byte included.
    /// </remarks>
    public string Key() => _key ?? ReadKey();

    /// <summary>The header value, or <see langword="null"/> for a null value.</summary>
    /// <returns>The header value, or <see langword="null"/> for a null value.</returns>
    /// <remarks>
    /// A polled header copies its value out of the batch once, on the first call, and returns that array on every
    /// call, as Java's <c>RecordHeader.value()</c> does.
    /// </remarks>
    public byte[]? Value() => _batch is null || _valueRead ? _value : ReadValue();

    /// <summary>
    /// The header value as a read-only memory, or <see langword="null"/> for a null value; an empty value is an empty
    /// memory.
    /// </summary>
    /// <returns>The header value, or <see langword="null"/> for a null value.</returns>
    /// <remarks>
    /// <para>
    /// For a header of a polled record the memory is a view of the polled batch, with no copy, and keeps the batch
    /// alive while it is reachable, until <see cref="Value()"/> has been called; from then on it is the array
    /// <see cref="Value()"/> returns, so a write into that array shows in both, as Java's <c>RecordHeader</c> keeps
    /// only the array once <c>value()</c> has copied it. A memory taken before the first <see cref="Value()"/> call
    /// stays a view of the batch. For a header built with a <c>byte[]</c> it is that array.
    /// </para>
    /// <para>
    /// <em>Deviation from Java:</em> Java's <c>Header</c> has only <c>value()</c>. A <c>byte[]</c> cannot view the
    /// batch, and the consumer's key and value already have a zero-copy memory form.
    /// </para>
    /// </remarks>
    public ReadOnlyMemory<byte>? ValueMemory()
    {
        // _valueRead is set after _value is published, so once it is seen the cached array is the value.
        if (_batch is null || _valueRead)
        {
            // Not a conditional with a null branch: null converts to ReadOnlyMemory<byte> through its byte[]
            // conversion, giving an empty memory instead of null.
            if (_value is null)
            {
                return null;
            }

            return new ReadOnlyMemory<byte>(_value);
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

        if (obj is not Header other || !string.Equals(Key(), other.Key(), StringComparison.Ordinal))
        {
            return false;
        }

        bool hasValue = ValueSpan(out ReadOnlySpan<byte> value);
        bool otherHasValue = other.ValueSpan(out ReadOnlySpan<byte> otherValue);
        bool equal = hasValue == otherHasValue && (!hasValue || value.SequenceEqual(otherValue));
        GC.KeepAlive(_batch);
        GC.KeepAlive(other._batch);
        return equal;
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int keyHash = StringComparer.Ordinal.GetHashCode(Key());
        bool hasValue = ValueSpan(out ReadOnlySpan<byte> value);
        int valueHash = hasValue ? ValueHashCode(value) : 0;
        GC.KeepAlive(_batch);
        return unchecked((31 * keyHash) + valueHash);
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        string key = Key();
        bool hasValue = ValueSpan(out ReadOnlySpan<byte> value);
        string text = hasValue ? ValueToString(value) : "null";
        GC.KeepAlive(_batch);
        return "RecordHeader(key = " + key + ", value = " + text + ")";
    }

    // An owned copy, which references no batch: a built header itself.
    internal Header ToOwned() => _batch is null ? this : new Header(Key(), Value());

    // Decodes a key's UTF-8 bytes, a NUL byte included; an invalid sequence becomes U+FFFD.
    internal static unsafe string DecodeKey(KafkaBytes bytes) =>
        bytes.Data == IntPtr.Zero || bytes.Length <= 0
            ? string.Empty
            : Encoding.UTF8.GetString((byte*)bytes.Data, bytes.Length);

    private string ReadKey()
    {
        IntPtr outErr = IntPtr.Zero;
        KafkaBytes bytes = NativeMethods.HeaderKeyBytes(_header, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string key = DecodeKey(bytes);
        GC.KeepAlive(_batch);
        return Interlocked.CompareExchange(ref _key, key, null) ?? key;
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
        byte[]? value = copy is null ? null : Interlocked.CompareExchange(ref _value, copy, null) ?? copy;
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

    // The value, without a copy, by ValueMemory's rule: the built array, the array Value() cached once it was called,
    // or else a view of the batch, which the caller keeps alive (by keeping this header alive) while it reads the
    // span. False for a null value.
    internal unsafe bool ValueSpan(out ReadOnlySpan<byte> value)
    {
        if (_batch is null || _valueRead)
        {
            value = _value;
            return _value is not null;
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
