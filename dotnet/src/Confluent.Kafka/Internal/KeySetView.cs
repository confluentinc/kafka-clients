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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The keys of a read-only dictionary as a read-only collection, Java's <c>Map.keySet()</c>: a view, so it reads
/// the dictionary on each use.
/// </summary>
internal sealed class KeySetView<TKey, TValue> : IReadOnlyCollection<TKey>
{
    private readonly IReadOnlyDictionary<TKey, TValue> _map;

    internal KeySetView(IReadOnlyDictionary<TKey, TValue> map)
    {
        _map = map;
    }

    public int Count => _map.Count;

    public IEnumerator<TKey> GetEnumerator() => _map.Keys.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
