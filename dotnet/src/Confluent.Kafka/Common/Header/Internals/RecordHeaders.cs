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
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Confluent.Kafka.Common.Header.Internals;

/// <summary>
/// A record's headers, with Java's equality, hash, string form and read-only state, over the elements' own equality,
/// hash and string form. A <see langword="null"/> header, or a <see langword="null"/> key given to <c>Add</c>, throws
/// <see cref="ArgumentNullException"/>, and a <see langword="null"/> key to look up or remove
/// <see cref="IllegalArgumentException"/>. A consumed record's headers are its own and writable, and empty, not null,
/// when it has none.
/// </summary>
public sealed class RecordHeaders : IHeaders
{
    // Allocated on the first Add, or when built from non-empty input, so a record without headers costs one object.
    private List<IHeader>? _headers;
    private volatile bool _isReadOnly;

    /// <summary>Initializes empty headers.</summary>
    public RecordHeaders()
    {
    }

    /// <summary>
    /// Initializes headers with a copy of <paramref name="headers"/>: a <see langword="null"/> array gives empty
    /// headers, and a <see langword="null"/> element throws <see cref="ArgumentNullException"/>.
    /// </summary>
    public RecordHeaders(IHeader[]? headers)
        : this((IEnumerable<IHeader>?)headers)
    {
    }

    /// <summary>
    /// Initializes headers with a copy of <paramref name="headers"/>: a <see langword="null"/> sequence gives empty
    /// headers, and a <see langword="null"/> element throws <see cref="ArgumentNullException"/>. A copy of a
    /// <see cref="RecordHeaders"/> is writable.
    /// </summary>
    public RecordHeaders(IEnumerable<IHeader>? headers)
    {
        if (headers is RecordHeaders other)
        {
            if (other._headers is { Count: > 0 } source)
            {
                _headers = new List<IHeader>(source);
            }

            return;
        }

        if (headers is null)
        {
            return;
        }

        if (headers is ICollection<IHeader> collection)
        {
            if (collection.Count == 0)
            {
                return;
            }

            _headers = new List<IHeader>(collection.Count);
        }

        foreach (IHeader header in headers)
        {
            if (header is null)
            {
                throw new ArgumentNullException(nameof(headers), "Header cannot be null.");
            }

            (_headers ??= new List<IHeader>()).Add(header);
        }
    }

    private RecordHeaders(List<IHeader> headers)
    {
        _headers = headers;
    }

    // The send reads the list by index, so it allocates no enumerator.
    internal List<IHeader>? Entries => _headers;

    /// <inheritdoc/>
    public IHeaders Add(IHeader header)
    {
        if (header is null)
        {
            throw new ArgumentNullException(nameof(header), "Header cannot be null.");
        }

        CanWrite();
        (_headers ??= new List<IHeader>()).Add(header);
        return this;
    }

    /// <inheritdoc/>
    public IHeaders Add(string key, byte[]? value) => Add(new RecordHeader(key, value));

    /// <inheritdoc/>
    public IHeaders Remove(string key)
    {
        CanWrite();
        CheckKey(key);
        List<IHeader>? headers = _headers;
        if (headers is null)
        {
            return this;
        }

        int first = 0;
        while (first < headers.Count && !string.Equals(headers[first].Key(), key, StringComparison.Ordinal))
        {
            first++;
        }

        if (first == headers.Count)
        {
            return this;
        }

        int kept = first;
        for (int i = first + 1; i < headers.Count; i++)
        {
            IHeader header = headers[i];
            if (!string.Equals(header.Key(), key, StringComparison.Ordinal))
            {
                headers[kept++] = header;
            }
        }

        headers.RemoveRange(kept, headers.Count - kept);
        return this;
    }

    /// <inheritdoc/>
    public IHeader? LastHeader(string key)
    {
        CheckKey(key);
        List<IHeader>? headers = _headers;
        if (headers is null)
        {
            return null;
        }

        for (int i = headers.Count - 1; i >= 0; i--)
        {
            if (string.Equals(headers[i].Key(), key, StringComparison.Ordinal))
            {
                return headers[i];
            }
        }

        return null;
    }

    /// <inheritdoc/>
    public IEnumerable<IHeader> Headers(string key)
    {
        CheckKey(key);
        return FilterByKey(key);
    }

    /// <summary>
    /// Makes the headers read-only: every later <c>Add</c> or <c>Remove</c> throws <see cref="IllegalStateException"/>.
    /// </summary>
    public void SetReadOnly() => _isReadOnly = true;

    /// <summary>Returns whether the headers are read-only.</summary>
    /// <returns><see langword="true"/> once <see cref="SetReadOnly()"/> has run.</returns>
    public bool IsReadOnly() => _isReadOnly;

    /// <inheritdoc/>
    public IHeader[] ToArray() => _headers is { Count: > 0 } headers ? headers.ToArray() : Array.Empty<IHeader>();

    /// <inheritdoc/>
    public IEnumerator<IHeader> GetEnumerator() =>
        _headers is { } headers ? headers.GetEnumerator() : ((IEnumerable<IHeader>)Array.Empty<IHeader>()).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not RecordHeaders other)
        {
            return false;
        }

        int count = _headers?.Count ?? 0;
        if (count != (other._headers?.Count ?? 0))
        {
            return false;
        }

        for (int i = 0; i < count; i++)
        {
            if (!_headers![i].Equals(other._headers![i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int result = 1;
        if (_headers is { } headers)
        {
            foreach (IHeader header in headers)
            {
                result = unchecked((31 * result) + header.GetHashCode());
            }
        }

        return result;
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        StringBuilder builder = new StringBuilder("RecordHeaders(headers = [");
        if (_headers is { } headers)
        {
            for (int i = 0; i < headers.Count; i++)
            {
                if (i > 0)
                {
                    builder.Append(", ");
                }

                builder.Append(headers[i].ToString() ?? "null");
            }
        }

        return builder.Append("], isReadOnly = ").Append(_isReadOnly ? "true" : "false").Append(')').ToString();
    }

    internal static RecordHeaders Wrap(List<IHeader> headers) => new RecordHeaders(headers);

    private static void CheckKey(string key)
    {
        if (key is null)
        {
            throw new IllegalArgumentException("key cannot be null.");
        }
    }

    private void CanWrite()
    {
        if (_isReadOnly)
        {
            throw new IllegalStateException("RecordHeaders has been closed.");
        }
    }

    private IEnumerable<IHeader> FilterByKey(string key)
    {
        List<IHeader>? headers = _headers;
        if (headers is null)
        {
            yield break;
        }

        foreach (IHeader header in headers)
        {
            if (string.Equals(header.Key(), key, StringComparison.Ordinal))
            {
                yield return header;
            }
        }
    }
}
