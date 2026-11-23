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

namespace Confluent.Kafka.Common.Header.Internals;

/// <summary>
/// A record's headers, with Java's equality, hash, string form and read-only state, over the elements' own equality,
/// hash and string form. A <see langword="null"/> header, or a <see langword="null"/> key given to <c>Add</c>, throws
/// <see cref="ArgumentNullException"/>, and a <see langword="null"/> key to look up or remove
/// <see cref="IllegalArgumentException"/>. A consumed record's headers are its own and writable, and empty, not null,
/// when it has none: they read the record's headers from the polled batch on first use, as headers whose keys and
/// values are views of the batch until read.
/// </summary>
internal sealed class RecordHeaders : IHeaders
{
    // Two fields, so the object stays at the size of a list reference and a flag, as a send without headers
    // allocates one per record.
    //
    // _state is the List<IHeader>, allocated on the first Add or when built from non-empty input, so a record without
    // headers costs one object; or, for a polled record's headers not read yet, the polled batch. The first use of any
    // member reads them into the list under the batch's lock: the core's RecordHeaders__as_Headers must not run
    // concurrently on one record's headers.
    private volatile object? _state;

    // While _state is the batch, the native record whose headers these are; afterwards s_readOnlyMark once read-only,
    // else zero. The read-only headers are always read first, so the two uses never overlap.
    private volatile IntPtr _record;

    private static readonly IntPtr s_readOnlyMark = new IntPtr(-1);

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
            if (other.Materialized() is { Count: > 0 } source)
            {
                _state = new List<IHeader>(source);
            }

            return;
        }

        if (headers is null)
        {
            return;
        }

        List<IHeader>? list = null;
        if (headers is ICollection<IHeader> collection)
        {
            if (collection.Count == 0)
            {
                return;
            }

            list = new List<IHeader>(collection.Count);
        }

        foreach (IHeader header in headers)
        {
            if (header is null)
            {
                throw new ArgumentNullException(nameof(headers), "Header cannot be null.");
            }

            (list ??= new List<IHeader>()).Add(header);
        }

        _state = list;
    }

    // The headers of `record` in the polled `batch`, read on first use.
    internal RecordHeaders(SafeConsumerRecordsHandle batch, IntPtr record)
    {
        _record = record;
        _state = batch;
    }

    // The send reads the list by index, so it allocates no enumerator.
    internal List<IHeader>? Entries => Materialized();

    /// <inheritdoc/>
    public IHeaders Add(IHeader header)
    {
        if (header is null)
        {
            throw new ArgumentNullException(nameof(header), "Header cannot be null.");
        }

        CanWrite();
        List<IHeader>? list = Materialized();
        if (list is null)
        {
            list = new List<IHeader>();
            _state = list;
        }

        list.Add(header);
        return this;
    }

    /// <inheritdoc/>
    public IHeaders Add(string key, byte[]? value) => Add(new RecordHeader(key, value));

    /// <inheritdoc/>
    public IHeaders Remove(string key)
    {
        CanWrite();
        CheckKey(key);
        List<IHeader>? headers = Materialized();
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
        List<IHeader>? headers = Materialized();
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
    public void SetReadOnly()
    {
        // Read first: the mark overwrites the native record pointer the read needs.
        Materialized();
        _record = s_readOnlyMark;
    }

    /// <summary>Returns whether the headers are read-only.</summary>
    /// <returns><see langword="true"/> once <see cref="SetReadOnly()"/> has run.</returns>
    public bool IsReadOnly() => _record == s_readOnlyMark;

    /// <inheritdoc/>
    public IHeader[] ToArray() => Materialized() is { Count: > 0 } headers ? headers.ToArray() : Array.Empty<IHeader>();

    /// <inheritdoc/>
    public IEnumerator<IHeader> GetEnumerator() =>
        Materialized() is { } headers ? headers.GetEnumerator() : ((IEnumerable<IHeader>)Array.Empty<IHeader>()).GetEnumerator();

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

        List<IHeader>? headers = Materialized();
        List<IHeader>? otherHeaders = other.Materialized();
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
        if (Materialized() is { } headers)
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

        return builder.Append("], isReadOnly = ").Append(IsReadOnly() ? "true" : "false").Append(')').ToString();
    }

    // Copies, which reference no batch, of these headers: each RecordHeader read from a batch is copied, every other
    // header kept.
    internal RecordHeaders ToOwned()
    {
        List<IHeader>? headers = Materialized();
        RecordHeaders copy = new RecordHeaders();
        if (headers is { Count: > 0 })
        {
            List<IHeader> owned = new List<IHeader>(headers.Count);
            foreach (IHeader header in headers)
            {
                owned.Add(header is RecordHeader recordHeader ? recordHeader.ToOwned() : header);
            }

            copy._state = owned;
        }

        return copy;
    }

    // The list, read from the batch first when these are a polled record's headers not read yet.
    private List<IHeader>? Materialized()
    {
        object? state = _state;
        if (state is SafeConsumerRecordsHandle batch)
        {
            Materialize(batch);
            state = _state;
        }

        return (List<IHeader>?)state;
    }

    private void Materialize(SafeConsumerRecordsHandle batch)
    {
        lock (batch)
        {
            if (!ReferenceEquals(_state, batch))
            {
                return;
            }

            List<IHeader>? headers = ConsumerRecordsMarshal.ReadHeaders(batch, _record);
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
        if (IsReadOnly())
        {
            throw new IllegalStateException("RecordHeaders has been closed.");
        }
    }

    private IEnumerable<IHeader> FilterByKey(string key)
    {
        List<IHeader>? headers = Materialized();
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
