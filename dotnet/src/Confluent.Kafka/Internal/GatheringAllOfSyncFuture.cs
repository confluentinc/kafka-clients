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
using System.Collections.Generic;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A sync gathering all-of (M15/P14 S1, PLAN §3.3): an all-of over an ordered list of keyed sources whose value is one
/// dictionary of every key's value — Java's <c>allOf(...).thenApply(...)</c> behind a result's <c>all()</c>, the sync
/// twin of the async results' gathering <c>All()</c>.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">Each key's value type.</typeparam>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>All-of first.</b> <see cref="Get"/> waits for every source in list order and, if any failed, rethrows the first
/// failure in list order — exactly <see cref="AllOfSyncFuture"/>, through the same helper.
/// </description></item>
/// <item><description>
/// <b>Built once, memoized.</b> Once every source has succeeded, the dictionary is built under this instance's lock,
/// once, keyed with the comparer given at construction, and every later <see cref="Get"/> returns that same instance.
/// A key that is equal to an earlier one under the comparer replaces it (the indexer, not <c>Add</c>): Java's
/// <c>put</c>, which never throws.
/// </description></item>
/// <item><description>
/// <b>Done when every source is.</b> <see cref="IsDone"/> never blocks.
/// </description></item>
/// </list>
/// </remarks>
internal sealed class GatheringAllOfSyncFuture<TKey, TValue> : SyncFutureSource<IReadOnlyDictionary<TKey, TValue>>
    where TKey : notnull
{
    private readonly TKey[] _keys;
    private readonly SyncFutureSource<TValue>[] _values;
    private readonly IEqualityComparer<TKey> _comparer;
    private volatile IReadOnlyDictionary<TKey, TValue>? _gathered;

    /// <summary>Creates a gathering all-of over <paramref name="entries"/>, in their order.</summary>
    /// <param name="entries">Each key with its source, in the operation's key order. Copied; later changes are not
    /// seen.</param>
    /// <param name="comparer">The comparer the gathered dictionary is keyed with.</param>
    internal GatheringAllOfSyncFuture(
        IReadOnlyList<KeyValuePair<TKey, SyncFutureSource<TValue>>> entries, IEqualityComparer<TKey> comparer)
    {
        if (entries is null)
        {
            throw new ArgumentNullException(nameof(entries));
        }

        _comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
        _keys = new TKey[entries.Count];
        _values = new SyncFutureSource<TValue>[entries.Count];
        for (int index = 0; index < _keys.Length; index++)
        {
            _keys[index] = entries[index].Key;
            _values[index] = entries[index].Value;
        }
    }

    /// <inheritdoc/>
    internal override bool IsDone => AllOfSyncFuture.AreAllDone(_values);

    /// <inheritdoc/>
    internal override IReadOnlyDictionary<TKey, TValue> Get()
    {
        IReadOnlyDictionary<TKey, TValue>? gathered = _gathered;
        if (gathered is not null)
        {
            return gathered;
        }

        AllOfSyncFuture.WaitForEvery(_values)?.Throw();
        return Gather();
    }

    private IReadOnlyDictionary<TKey, TValue> Gather()
    {
        lock (this)
        {
            IReadOnlyDictionary<TKey, TValue>? gathered = _gathered;
            if (gathered is null)
            {
                Dictionary<TKey, TValue> values = new Dictionary<TKey, TValue>(_keys.Length, _comparer);
                for (int index = 0; index < _keys.Length; index++)
                {
                    // Every source has succeeded, so this neither waits nor throws.
                    values[_keys[index]] = _values[index].Get();
                }

                gathered = values;
                _gathered = gathered;
            }

            return gathered;
        }
    }
}
