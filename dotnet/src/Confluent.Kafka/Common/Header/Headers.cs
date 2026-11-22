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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common.Header;

/// <summary>
/// A record's headers, in the order they were added, a key possibly repeated: Java's <c>Headers</c>, with the
/// constructors, equality, hash, string form and read-only state of Java's <c>RecordHeaders</c>. A consumed record's
/// headers are its own and writable, and empty, not null, when it has none: they read the record's headers from the
/// polled batch on first use, as headers whose keys and values are views of the batch until read.
/// </summary>
public sealed class Headers : IEnumerable<Header>
{
    // Two fields, so the object stays at the size of a list reference and a flag, as a send without headers
    // allocates one per record.
    //
    // _state is the List<Header>, allocated on the first Add or when built from non-empty input, so a record without
    // headers costs one object; or, for a polled record's headers not read yet, the polled batch. The first use of any
    // member reads them into the list under the batch's lock: the core's RecordHeaders__as_Headers must not run
    // concurrently on one record's headers.
    private volatile object? _state;

    // While _state is the batch, the native record whose headers these are; afterwards s_readOnlyMark once read-only,
    // else zero. The read-only headers are always read first, so the two uses never overlap.
    private volatile IntPtr _record;

    private static readonly IntPtr s_readOnlyMark = new IntPtr(-1);

    /// <summary>Initializes empty headers, Java's <c>RecordHeaders()</c>.</summary>
    public Headers()
    {
    }

    /// <summary>
    /// Initializes headers with a copy of <paramref name="headers"/>, Java's <c>RecordHeaders(Header[])</c>: a
    /// <see langword="null"/> array gives empty headers, and a <see langword="null"/> element throws
    /// <see cref="ArgumentNullException"/>.
    /// </summary>
    public Headers(Header[]? headers)
        : this((IEnumerable<Header>?)headers)
    {
    }

    /// <summary>
    /// Initializes headers with a copy of <paramref name="headers"/>, Java's
    /// <c>RecordHeaders(Iterable&lt;Header&gt;)</c>: a <see langword="null"/> sequence gives empty headers, and a
    /// <see langword="null"/> element throws <see cref="ArgumentNullException"/>. A copy of a <see cref="Headers"/>
    /// is writable.
    /// </summary>
    public Headers(IEnumerable<Header>? headers)
    {
        if (headers is Headers other)
        {
            if (other.Materialized() is { Count: > 0 } source)
            {
                _state = new List<Header>(source);
            }

            return;
        }

        if (headers is null)
        {
            return;
        }

        List<Header>? list = null;
        if (headers is ICollection<Header> collection)
        {
            if (collection.Count == 0)
            {
                return;
            }

            list = new List<Header>(collection.Count);
        }

        foreach (Header header in headers)
        {
            if (header is null)
            {
                throw new ArgumentNullException(nameof(headers), "Header cannot be null.");
            }

            (list ??= new List<Header>()).Add(header);
        }

        _state = list;
    }

    // The headers of `record` in the polled `batch`, read on first use.
    internal Headers(SafeConsumerRecordsHandle batch, IntPtr record)
    {
        _record = record;
        _state = batch;
    }

    internal bool IsReadOnly => _record == s_readOnlyMark;

    // The send reads the list by index, so it allocates no enumerator.
    internal List<Header>? Entries => Materialized();

    /// <summary>
    /// Adds a header to the end, returning these headers, so calls can be chained.
    /// </summary>
    /// <param name="header">The header to add.</param>
    /// <returns>These headers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="header"/> is <see langword="null"/>.</exception>
    /// <exception cref="IllegalStateException">The headers are read-only.</exception>
    public Headers Add(Header header)
    {
        if (header is null)
        {
            throw new ArgumentNullException(nameof(header), "Header cannot be null.");
        }

        CanWrite();
        List<Header>? list = Materialized();
        if (list is null)
        {
            list = new List<Header>();
            _state = list;
        }

        list.Add(header);
        return this;
    }

    /// <summary>
    /// Creates a header from <paramref name="key"/> and <paramref name="value"/> and adds it to the end, returning
    /// these headers, so calls can be chained.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <param name="value">The header value, which may be <see langword="null"/>.</param>
    /// <returns>These headers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    /// <exception cref="IllegalStateException">The headers are read-only.</exception>
    public Headers Add(string key, byte[]? value) => Add(new Header(key, value));

    /// <summary>
    /// Removes every header with the given key, returning these headers, so calls can be chained.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns>These headers.</returns>
    /// <exception cref="IllegalStateException">The headers are read-only.</exception>
    /// <exception cref="IllegalArgumentException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public Headers Remove(string key)
    {
        CanWrite();
        CheckKey(key);
        List<Header>? headers = Materialized();
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
            Header header = headers[i];
            if (!string.Equals(header.Key(), key, StringComparison.Ordinal))
            {
                headers[kept++] = header;
            }
        }

        headers.RemoveRange(kept, headers.Count - kept);
        return this;
    }

    /// <summary>Returns the last header with the given key, or <see langword="null"/> when there is none.</summary>
    /// <param name="key">The key to look up.</param>
    /// <returns>The last header with that key, or <see langword="null"/>.</returns>
    /// <exception cref="IllegalArgumentException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public Header? LastHeader(string key)
    {
        CheckKey(key);
        List<Header>? headers = Materialized();
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

    /// <summary>
    /// Returns every header with the given key, in the order they were added, Java's <c>headers(String)</c>. Each
    /// enumeration reads the current headers.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <returns>The headers with that key.</returns>
    /// <exception cref="IllegalArgumentException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public IEnumerable<Header> GetHeaders(string key)
    {
        CheckKey(key);
        return FilterByKey(key);
    }

    /// <summary>Returns every header, in the order they were added.</summary>
    /// <returns>The headers; an empty array when there are none.</returns>
    public Header[] ToArray() => Materialized() is { Count: > 0 } headers ? headers.ToArray() : Array.Empty<Header>();

    /// <inheritdoc/>
    public IEnumerator<Header> GetEnumerator() =>
        Materialized() is { } headers ? headers.GetEnumerator() : ((IEnumerable<Header>)Array.Empty<Header>()).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not Headers other)
        {
            return false;
        }

        List<Header>? headers = Materialized();
        List<Header>? otherHeaders = other.Materialized();
        int count = headers?.Count ?? 0;
        if (count != (otherHeaders?.Count ?? 0))
        {
            return false;
        }

        for (int i = 0; i < count; i++)
        {
            if (!headers![i].Equals(otherHeaders![i]))
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
        if (Materialized() is { } headers)
        {
            foreach (Header header in headers)
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
        if (Materialized() is { } headers)
        {
            for (int i = 0; i < headers.Count; i++)
            {
                if (i > 0)
                {
                    builder.Append(", ");
                }

                builder.Append(headers[i].ToString());
            }
        }

        return builder.Append("], isReadOnly = ").Append(IsReadOnly ? "true" : "false").Append(')').ToString();
    }

    internal void SetReadOnly()
    {
        Materialized();
        _record = s_readOnlyMark;
    }

    // Owned copies, which reference no batch, of these headers.
    internal Headers ToOwned()
    {
        List<Header>? headers = Materialized();
        Headers copy = new Headers();
        if (headers is { Count: > 0 })
        {
            List<Header> owned = new List<Header>(headers.Count);
            foreach (Header header in headers)
            {
                owned.Add(header.ToOwned());
            }

            copy._state = owned;
        }

        return copy;
    }

    // The list, read from the batch first when these are a polled record's headers not read yet.
    private List<Header>? Materialized()
    {
        object? state = _state;
        if (state is SafeConsumerRecordsHandle batch)
        {
            Materialize(batch);
            state = _state;
        }

        return (List<Header>?)state;
    }

    private void Materialize(SafeConsumerRecordsHandle batch)
    {
        lock (batch)
        {
            if (!ReferenceEquals(_state, batch))
            {
                return;
            }

            List<Header>? headers = ConsumerRecordsMarshal.ReadHeaders(batch, _record);
            _record = IntPtr.Zero;
            _state = headers;
        }
    }

    private static void CheckKey(string key)
    {
        if (key is null)
        {
            throw new IllegalArgumentException("key cannot be null.");
        }
    }

    private void CanWrite()
    {
        if (IsReadOnly)
        {
            throw new IllegalStateException("RecordHeaders has been closed.");
        }
    }

    private IEnumerable<Header> FilterByKey(string key)
    {
        List<Header>? headers = Materialized();
        if (headers is null)
        {
            yield break;
        }

        foreach (Header header in headers)
        {
            if (string.Equals(header.Key(), key, StringComparison.Ordinal))
            {
                yield return header;
            }
        }
    }
}
