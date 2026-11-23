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

using Confluent.Kafka.Common.Header.Internals;

namespace Confluent.Kafka.Common.Header;

/// <summary>A header is a key-value pair.</summary>
public interface IHeader
{
    /// <summary>Returns the key of the header.</summary>
    /// <returns>The header's key; must not be <see langword="null"/>.</returns>
    string Key();

    /// <summary>Returns the value of the header.</summary>
    /// <returns>The header's value; may be <see langword="null"/>.</returns>
    byte[]? Value();
}

/// <summary>Extension methods for <see cref="IHeader"/>.</summary>
public static class HeaderExtensions
{
    /// <summary>
    /// Returns the value of <paramref name="header"/> as a read-only memory, or <see langword="null"/> for a null value;
    /// an empty value is an empty memory.
    /// </summary>
    /// <param name="header">The header.</param>
    /// <returns>The header's value, or <see langword="null"/> for a null value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="header"/> is null.</exception>
    /// <remarks>
    /// <para>
    /// For a header of a polled record the memory is a view of the polled batch, with no copy, and keeps the batch
    /// alive while it is reachable, until <see cref="IHeader.Value()"/> has been called; from then on it is the array
    /// <see cref="IHeader.Value()"/> returns, so a write into that array shows in both, as Java's <c>RecordHeader</c>
    /// keeps only the array once <c>value()</c> has copied it. A memory taken before the first
    /// <see cref="IHeader.Value()"/> call stays a view of the batch. For any other header it is the array
    /// <see cref="IHeader.Value()"/> returns.
    /// </para>
    /// <para>
    /// <em>Deviation from Java:</em> Java's <c>Header</c> has only <c>value()</c>. A <c>byte[]</c> cannot view the
    /// batch, and the consumer's key and value already have a zero-copy memory form.
    /// </para>
    /// </remarks>
    public static ReadOnlyMemory<byte>? ValueMemory(this IHeader header)
    {
        if (header is null)
        {
            throw new ArgumentNullException(nameof(header));
        }

        if (header is RecordHeader recordHeader)
        {
            return recordHeader.ValueMemory();
        }

        // Not a conditional with a null branch: null converts to ReadOnlyMemory<byte> through its byte[] conversion,
        // giving an empty memory instead of null.
        byte[]? value = header.Value();
        if (value is null)
        {
            return null;
        }

        return new ReadOnlyMemory<byte>(value);
    }
}
