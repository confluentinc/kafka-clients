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
using System.Linq;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The compositions the sync admin Results (M15/P14 S4) share at their public boundary — the sync twin of
/// <see cref="AsyncAdminFutures"/>: a keyed view of the sync flavour's latches presented as
/// <see cref="KafkaFuture{T}"/>s, a void erasure, a projection, an all-of and a gathering all-of.
/// </summary>
/// <remarks>
/// <para>
/// Each composition is one of the S1 composites — <see cref="ProjectedSyncFuture{TIn, TOut}"/>,
/// <see cref="AllOfSyncFuture"/>, <see cref="GatheringAllOfSyncFuture{TKey, TValue}"/> — over the
/// <see cref="KafkaFuture{T}.Source"/> of the futures it is given, so every future a sync Result hands out waits on the
/// thread that calls <c>Get()</c>. Nothing here uses a <c>Task</c>, a <c>TaskCompletionSource</c> or
/// <c>Task.Run</c>, and nothing needs a thread-pool thread (M15/P14 PLAN §3.3, D3, D5).
/// </para>
/// <para>
/// The sources are read when the composition is built, so a <see langword="default"/> future throws
/// <see cref="InvalidOperationException"/> at that call — where the async twin's <c>Get()</c> on a default
/// <see cref="AsyncKafkaFuture{T}"/> throws too. Only internal code can put a default future into a Result.
/// </para>
/// <para>Host scaffolding with no Java counterpart (DoD #7): it only re-types the boundary.</para>
/// </remarks>
internal static class SyncAdminFutures
{
    /// <summary>
    /// A read-only view of <paramref name="sources"/> whose entries are <see cref="KafkaFuture{T}"/>s over the same
    /// latches. See <see cref="SyncFutureView{TKey, TValue}"/>.
    /// </summary>
    internal static IReadOnlyDictionary<TKey, KafkaFuture<TValue>> Wrap<TKey, TValue>(
        IReadOnlyDictionary<TKey, SyncFutureSource<TValue>> sources)
        where TKey : notnull =>
        new SyncFutureView<TKey, TValue>(sources);

    /// <summary>
    /// Java's <c>thenApply</c>: a future whose value is <paramref name="project"/> applied to
    /// <paramref name="future"/>'s — lazily, on the first <c>Get()</c>'s thread, and memoized
    /// (<see cref="ProjectedSyncFuture{TIn, TOut}"/>).
    /// </summary>
    internal static KafkaFuture<TOut> Project<TIn, TOut>(KafkaFuture<TIn> future, Func<TIn, TOut> project) =>
        new KafkaFuture<TOut>(new ProjectedSyncFuture<TIn, TOut>(future.Source, project));

    /// <summary>
    /// Java's <c>allOf</c> over <paramref name="futures"/>, in their order: it waits for every one and then rethrows
    /// the first failure in that order (<see cref="AllOfSyncFuture"/>) — the failure <c>Task.WhenAll</c> surfaces on
    /// the async flavour, for the same order.
    /// </summary>
    internal static KafkaFuture AllOf<TValue>(IEnumerable<KafkaFuture<TValue>> futures) =>
        new KafkaFuture(new AllOfSyncFuture(futures.Select(static future => (SyncFutureSource)future.Source).ToList()));

    /// <summary>
    /// <see cref="AllOf{TValue}(IEnumerable{KafkaFuture{TValue}})"/> over futures without a value.
    /// </summary>
    internal static KafkaFuture AllOf(IEnumerable<KafkaFuture> futures) =>
        new KafkaFuture(new AllOfSyncFuture(futures.Select(static future => future.Source).ToList()));

    /// <summary>
    /// Java's <c>allOf(...).thenApply(...)</c> gathering every value into one map (a <c>DescribeTopicsResult</c>
    /// aggregate): it waits for every entry of <paramref name="futures"/> in its order, rethrows the first failure in
    /// that order, and otherwise returns the values keyed with <paramref name="comparer"/>, an equal key replacing the
    /// earlier one as Java's <c>HashMap.put</c> does (<see cref="GatheringAllOfSyncFuture{TKey, TValue}"/>).
    /// </summary>
    internal static KafkaFuture<IReadOnlyDictionary<TKey, TValue>> GatherAll<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, KafkaFuture<TValue>>> futures, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        new KafkaFuture<IReadOnlyDictionary<TKey, TValue>>(
            new GatheringAllOfSyncFuture<TKey, TValue>(
                futures
                    .Select(static entry => new KeyValuePair<TKey, SyncFutureSource<TValue>>(entry.Key, entry.Value.Source))
                    .ToList(),
                comparer));

    /// <summary>
    /// A void-keyed operation's latches as Java's <c>Map&lt;K, KafkaFuture&lt;Void&gt;&gt;</c>: one
    /// <see cref="KafkaFuture"/> per key over the <b>same</b> latch — no second completion — keyed with the
    /// operation's own <paramref name="comparer"/> and in its key order. The <c>bool</c> is the void bridge's internal
    /// success token and does not reach the public surface. Built once per Result, so a key's future is equal on every
    /// read.
    /// </summary>
    internal static IReadOnlyDictionary<TKey, KafkaFuture> Erase<TKey>(
        IReadOnlyDictionary<TKey, SyncFutureSource<bool>> sources, IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        Dictionary<TKey, KafkaFuture> erased = new Dictionary<TKey, KafkaFuture>(sources.Count, comparer);
        foreach (KeyValuePair<TKey, SyncFutureSource<bool>> entry in sources)
        {
            erased.Add(entry.Key, new KafkaFuture(entry.Value));
        }

        return erased;
    }
}

/// <summary>
/// A read-only dictionary over a dictionary of sync latches, whose every entry is a <see cref="KafkaFuture{T}"/> over
/// the source entry's <b>same</b> latch (M15/P14 S4) — the sync twin of <see cref="AsyncFutureView{TKey, TValue}"/>.
/// </summary>
/// <remarks>
/// <para>
/// A <b>view</b>, not a copy: every member reads the source when called, so the view keeps the source's key order, its
/// key comparer and its membership, and a missing or null key fails exactly as the source's lookup does. Built once
/// per Result, it is the same view instance on every access, and a key's future is equal on every read (equality is
/// the latch's identity).
/// </para>
/// <para>
/// No second latch is created, and a future is a struct, so a read allocates nothing. Host scaffolding with no Java
/// counterpart (DoD #7).
/// </para>
/// </remarks>
internal sealed class SyncFutureView<TKey, TValue> : IReadOnlyDictionary<TKey, KafkaFuture<TValue>>
    where TKey : notnull
{
    private readonly IReadOnlyDictionary<TKey, SyncFutureSource<TValue>> _sources;

    internal SyncFutureView(IReadOnlyDictionary<TKey, SyncFutureSource<TValue>> sources) => _sources = sources;

    public int Count => _sources.Count;

    public IEnumerable<TKey> Keys => _sources.Keys;

    public IEnumerable<KafkaFuture<TValue>> Values =>
        _sources.Values.Select(static source => new KafkaFuture<TValue>(source));

    public KafkaFuture<TValue> this[TKey key] => new KafkaFuture<TValue>(_sources[key]);

    public bool ContainsKey(TKey key) => _sources.ContainsKey(key);

    public bool TryGetValue(TKey key, out KafkaFuture<TValue> value)
    {
        if (_sources.TryGetValue(key, out SyncFutureSource<TValue>? source))
        {
            value = new KafkaFuture<TValue>(source);
            return true;
        }

        value = default;
        return false;
    }

    public IEnumerator<KeyValuePair<TKey, KafkaFuture<TValue>>> GetEnumerator()
    {
        foreach (KeyValuePair<TKey, SyncFutureSource<TValue>> entry in _sources)
        {
            yield return new KeyValuePair<TKey, KafkaFuture<TValue>>(entry.Key, new KafkaFuture<TValue>(entry.Value));
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
