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

using System.Collections;
using System.Collections.Generic;

namespace Confluent.Kafka;

/// <summary>
/// The read-only, ordered collection of <see cref="Header"/> on a
/// <see cref="ConsumerRecord"/> — the .NET realization of Java's
/// <c>org.apache.kafka.common.header.Headers</c>, clipped to today's ABI (a read-only
/// view of the copied-out headers). An empty record yields a shared empty instance
/// (<see cref="Count"/> <c>== 0</c>).
/// </summary>
/// <remarks>
/// Clipped to today's ABI: Java's mutating / lookup members (<c>add</c>,
/// <c>remove</c>, <c>lastHeader</c>, <c>headers(key)</c>) are not exposed this phase —
/// the receive-path surface is read-only. Headers are owned copies (§B3/§B4), so
/// nothing native-backed escapes.
/// </remarks>
public sealed class Headers : IReadOnlyList<Header>
{
    /// <summary>A shared empty instance for records with no headers (no allocation).</summary>
    internal static readonly Headers Empty = new Headers(System.Array.Empty<Header>());

    private readonly IReadOnlyList<Header> _headers;

    /// <summary>
    /// Initializes the collection from the owned headers produced by the copy-out.
    /// </summary>
    internal Headers(IReadOnlyList<Header> headers)
    {
        _headers = headers;
    }

    /// <summary>The number of headers (<c>0</c> when the record has none).</summary>
    public int Count => _headers.Count;

    /// <summary>The header at <paramref name="index"/>, in record order.</summary>
    /// <param name="index">The zero-based index.</param>
    public Header this[int index] => _headers[index];

    /// <summary>Enumerates the headers in record order.</summary>
    public IEnumerator<Header> GetEnumerator() => _headers.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
