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

namespace Confluent.Kafka.Internal;

/// <summary>
/// A shared, immutable empty <see cref="IReadOnlyDictionary{TKey, TValue}"/> — the
/// analog of <see cref="System.Array.Empty{T}()"/> for the offset-map copy-out
/// marshallers' empty-result path (an empty request, or a query that resolved to no
/// entries). Returned instead of allocating a fresh empty
/// <see cref="Dictionary{TKey, TValue}"/> per empty result, mirroring the
/// <c>ConsumerRecordsMarshal</c> use of <see cref="System.Array.Empty{T}()"/> for an
/// empty batch (allocation-budget hygiene, DoD §10).
/// </summary>
/// <remarks>
/// netstandard2.0 has no built-in empty read-only dictionary (no
/// <c>ReadOnlyDictionary&lt;,&gt;.Empty</c>), so this is a tiny hand-rolled shape. It is
/// read-only and stateless, so the single <see cref="Instance"/> is safe to share across
/// threads and results.
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
internal sealed class EmptyReadOnlyDictionary<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>
    where TKey : notnull
{
    /// <summary>The shared empty instance.</summary>
    internal static readonly EmptyReadOnlyDictionary<TKey, TValue> Instance =
        new EmptyReadOnlyDictionary<TKey, TValue>();

    private EmptyReadOnlyDictionary()
    {
    }

    /// <inheritdoc/>
    public int Count => 0;

    /// <inheritdoc/>
    public IEnumerable<TKey> Keys => Array.Empty<TKey>();

    /// <inheritdoc/>
    public IEnumerable<TValue> Values => Array.Empty<TValue>();

    /// <inheritdoc/>
    public TValue this[TKey key] => throw new KeyNotFoundException();

    /// <inheritdoc/>
    public bool ContainsKey(TKey key) => false;

    /// <inheritdoc/>
    public bool TryGetValue(TKey key, out TValue value)
    {
        value = default!;
        return false;
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        yield break;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
