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
/// The headers of a <see cref="ConsumerRecord{TKey, TValue}"/>, in record order: Java's <c>Headers</c>, read-only
/// and without its mutating and lookup members. Empty, not null, when the record has none.
/// </summary>
public sealed class Headers : IReadOnlyList<Header>
{
    internal static readonly Headers Empty = new Headers(System.Array.Empty<Header>());

    private readonly IReadOnlyList<Header> _headers;

    internal Headers(IReadOnlyList<Header> headers)
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
}
