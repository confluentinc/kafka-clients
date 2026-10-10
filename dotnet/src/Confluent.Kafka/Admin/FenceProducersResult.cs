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
using System.Globalization;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.FenceProducers"/> — Java's
/// <c>org.apache.kafka.clients.admin.FenceProducersResult</c> (<c>:31</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ Java stores one future per id carrying a <c>ProducerIdAndEpoch</c> (<c>:33</c>) but never
/// publishes that pair as itself: <see cref="FencedProducers"/> erases it
/// (<c>thenApply(p -&gt; null)</c>, <c>:46</c>) and <see cref="ProducerId"/> /
/// <see cref="EpochId"/> project one field each. Hence there is no public
/// <c>ProducerIdAndEpoch</c> type here (M15/P8 D50).
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its id (or, for
/// <see cref="All"/>, every id) has completed, and needs no thread-pool thread: the futures here are composed on the
/// waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncFenceProducersResult"/>
public sealed class FenceProducersResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<ProducerIdAndEpoch>> _values;
    private readonly IReadOnlyDictionary<string, KafkaFuture> _erasedValues;

    internal FenceProducersResult(
        IReadOnlyDictionary<string, KafkaFuture<ProducerIdAndEpoch>> values,
        IEqualityComparer<string> comparer)
    {
        _values = values;

        // Java's fencedProducers() erases the payload (:46). Each entry wraps the SAME latch as
        // the id's typed future, so it completes exactly when that one does, with the same
        // failure. Java mints a fresh map per call; this one is built once, which is
        // observationally identical for an immutable view.
        Dictionary<string, KafkaFuture> erased = new Dictionary<string, KafkaFuture>(values.Count, comparer);
        foreach (KeyValuePair<string, KafkaFuture<ProducerIdAndEpoch>> entry in values)
        {
            erased.Add(entry.Key, entry.Value.Erase());
        }

        _erasedValues = erased;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static FenceProducersResult From(KeyedAdminOperation<string, ProducerIdAndEpoch> operation) =>
        new FenceProducersResult(SyncAdminFutures.Wrap(operation.SyncSources), operation.KeyComparer);

    /// <summary>
    /// One payload-free future per requested transactional id — Java's
    /// <c>fencedProducers()</c> (<c>:44</c>), whose values are <c>KafkaFuture&lt;Void&gt;</c>.
    /// </summary>
    public IReadOnlyDictionary<string, KafkaFuture> FencedProducers => _erasedValues;

    /// <summary>
    /// The producer id generated while initializing that transaction — Java's
    /// <c>producerId(String)</c> (<c>:53</c>).
    /// </summary>
    /// <param name="transactionalId">An id from <see cref="FencedProducers"/>.</param>
    /// <returns>A future over that id's producer id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> was not included in the request — Java's
    /// <c>IllegalArgumentException</c> (<c>:74-77</c>), thrown synchronously as Java's is.
    /// </exception>
    public KafkaFuture<long> ProducerId(string transactionalId) =>
        FindAndApply(transactionalId, static value => value.ProducerId);

    /// <summary>
    /// The epoch generated while initializing that transaction — Java's
    /// <c>epochId(String)</c> (<c>:60</c>), a <see langword="short"/> as Java's is.
    /// </summary>
    /// <param name="transactionalId">An id from <see cref="FencedProducers"/>.</param>
    /// <returns>A future over that id's epoch.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> was not included in the request — see
    /// <see cref="ProducerId"/>.
    /// </exception>
    public KafkaFuture<short> EpochId(string transactionalId) =>
        FindAndApply(transactionalId, static value => value.Epoch);

    /// <summary>
    /// Completes when <b>every</b> producer has been fenced, and fails with the first failure
    /// otherwise — Java's <c>all()</c> (<c>:67</c>).
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    /// <remarks>
    /// Every id is waited for first, so a failure fails the batch with the first failure in key order, as Java's
    /// <c>allOf</c> does. Each call builds a new future.
    /// </remarks>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);

    /// <summary>Java's <c>findAndApply</c> (<c>:71-78</c>).</summary>
    private KafkaFuture<TResult> FindAndApply<TResult>(
        string transactionalId, Func<ProducerIdAndEpoch, TResult> followup)
    {
        // Synchronous, as Java's is: the lookup happens before the thenApply, so an
        // unrequested id is a usage error at the call site rather than a future's outcome.
        if (transactionalId is null)
        {
            throw new ArgumentNullException(nameof(transactionalId));
        }

        if (!_values.TryGetValue(transactionalId, out KafkaFuture<ProducerIdAndEpoch> source))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "TransactionalId `{0}` was not included in the request",
                    transactionalId),
                nameof(transactionalId));
        }

        // A new projection per call, as Java's thenApply allocates one; each is memoized.
        return SyncAdminFutures.Project(source, followup);
    }
}
