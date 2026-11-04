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
/// A shared, stateless empty <see cref="IReadOnlyDictionary{TKey, TValue}"/>, the analog of
/// <see cref="System.Array.Empty{T}()"/> that a map copy-out returns for an empty result.
/// </summary>
internal sealed class EmptyReadOnlyDictionary<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>
    where TKey : notnull
{
    internal static readonly EmptyReadOnlyDictionary<TKey, TValue> Instance =
        new EmptyReadOnlyDictionary<TKey, TValue>();

    private EmptyReadOnlyDictionary()
    {
    }

    public int Count => 0;

    public IEnumerable<TKey> Keys => Array.Empty<TKey>();

    public IEnumerable<TValue> Values => Array.Empty<TValue>();

    public TValue this[TKey key] => throw new KeyNotFoundException();

    public bool ContainsKey(TKey key) => false;

    public bool TryGetValue(TKey key, out TValue value)
    {
        value = default!;
        return false;
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        yield break;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
