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

namespace Confluent.Kafka.Common.Header;

/// <summary>
/// A record's headers, in record order: Java's <c>Headers</c>, built as Java's <c>RecordHeaders</c> builds it,
/// read-only and without its mutating and lookup members. A consumed record's headers are empty, not null, when it
/// has none.
/// </summary>
public sealed class Headers : IReadOnlyList<Header>
{
    internal static readonly Headers Empty = Wrap(Array.Empty<Header>());

    private readonly IReadOnlyList<Header> _headers;

    /// <summary>Initializes empty headers, Java's <c>RecordHeaders()</c>.</summary>
    public Headers()
        : this((IEnumerable<Header>?)null)
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
    /// <see langword="null"/> element throws <see cref="ArgumentNullException"/>.
    /// </summary>
    public Headers(IEnumerable<Header>? headers)
    {
        List<Header> copy = new List<Header>();
        if (headers is Headers other)
        {
            copy.AddRange(other._headers);
        }
        else if (headers != null)
        {
            foreach (Header header in headers)
            {
                if (header is null)
                {
                    throw new ArgumentNullException(nameof(headers), "Header cannot be null.");
                }

                copy.Add(header);
            }
        }

        _headers = copy;
    }

    private Headers(IReadOnlyList<Header> headers)
    {
        _headers = headers;
    }

    /// <inheritdoc/>
    public int Count => _headers.Count;

    /// <inheritdoc/>
    public Header this[int index] => _headers[index];

    /// <inheritdoc/>
    public IEnumerator<Header> GetEnumerator() => _headers.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    internal static Headers Wrap(IReadOnlyList<Header> headers) => new Headers(headers);
}
