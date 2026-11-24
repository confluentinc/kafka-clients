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

using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A read-only map over the given entries, in their order. Unlike the BCL's maps it can hold a <see langword="null"/>
/// key, as a user's own map can.
/// </summary>
internal sealed class EntryMap<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>
{
    private readonly KeyValuePair<TKey, TValue>[] _entries;

    internal EntryMap(params KeyValuePair<TKey, TValue>[] entries) => _entries = entries;

    public int Count => _entries.Length;

    public IEnumerable<TKey> Keys => _entries.Select(entry => entry.Key);

    public IEnumerable<TValue> Values => _entries.Select(entry => entry.Value);

    public TValue this[TKey key] =>
        TryGetValue(key, out TValue value) ? value : throw new KeyNotFoundException();

    public bool ContainsKey(TKey key) => TryGetValue(key, out _);

    public bool TryGetValue(TKey key, out TValue value)
    {
        foreach (KeyValuePair<TKey, TValue> entry in _entries)
        {
            if (EqualityComparer<TKey>.Default.Equals(entry.Key, key))
            {
                value = entry.Value;
                return true;
            }
        }

        value = default!;
        return false;
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => ((IEnumerable<KeyValuePair<TKey, TValue>>)_entries).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
