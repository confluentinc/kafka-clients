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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.FenceProducers"/> — Java's
/// <c>org.apache.kafka.clients.admin.FenceProducersResult</c> (<c>:31</c>).
/// </summary>
/// <remarks>
/// ⚠ Java stores one future per id carrying a <c>ProducerIdAndEpoch</c> (<c>:33</c>) but never
/// publishes that pair as itself: <see cref="FencedProducers"/> erases it
/// (<c>thenApply(p -&gt; null)</c>, <c>:46</c>) and <see cref="ProducerId"/> /
/// <see cref="EpochId"/> project one field each. Hence there is no public
/// <c>ProducerIdAndEpoch</c> type here (M15/P8 D50).
/// </remarks>
public sealed class FenceProducersResult
{
    private readonly IReadOnlyDictionary<string, Task<ProducerIdAndEpoch>> _values;
    private readonly IReadOnlyDictionary<string, Task> _erasedValues;

    internal FenceProducersResult(
        IReadOnlyDictionary<string, Task<ProducerIdAndEpoch>> values,
        IEqualityComparer<string> comparer)
    {
        _values = values;

        // Java's fencedProducers() erases the payload (:46). The erasure is a reference
        // upcast, so each entry is the SAME Task instance and no second fault can go
        // unobserved. Java mints a fresh map per call; this one is built once, which is
        // observationally identical for an immutable view.
        Dictionary<string, Task> erased = new Dictionary<string, Task>(values.Count, comparer);
        foreach (KeyValuePair<string, Task<ProducerIdAndEpoch>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _erasedValues = erased;
    }

    /// <summary>
    /// One payload-free awaitable per requested transactional id — Java's
    /// <c>fencedProducers()</c> (<c>:44</c>), whose values are <c>KafkaFuture&lt;Void&gt;</c>.
    /// </summary>
    public IReadOnlyDictionary<string, Task> FencedProducers => _erasedValues;

    /// <summary>
    /// The producer id generated while initializing that transaction — Java's
    /// <c>producerId(String)</c> (<c>:53</c>).
    /// </summary>
    /// <param name="transactionalId">An id from <see cref="FencedProducers"/>.</param>
    /// <returns>An awaitable over that id's producer id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> was not included in the request — Java's
    /// <c>IllegalArgumentException</c> (<c>:74-77</c>), thrown synchronously as Java's is.
    /// </exception>
    public Task<long> ProducerId(string transactionalId) =>
        FindAndApply(transactionalId, static value => value.ProducerId);

    /// <summary>
    /// The epoch generated while initializing that transaction — Java's
    /// <c>epochId(String)</c> (<c>:60</c>), a <see langword="short"/> as Java's is.
    /// </summary>
    /// <param name="transactionalId">An id from <see cref="FencedProducers"/>.</param>
    /// <returns>An awaitable over that id's epoch.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> was not included in the request — see
    /// <see cref="ProducerId"/>.
    /// </exception>
    public Task<short> EpochId(string transactionalId) =>
        FindAndApply(transactionalId, static value => value.Epoch);

    /// <summary>
    /// Completes when <b>every</b> producer has been fenced, and faults with the first failure
    /// otherwise — Java's <c>all()</c> (<c>:67</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);

    /// <summary>Java's <c>findAndApply</c> (<c>:71-78</c>).</summary>
    private Task<TResult> FindAndApply<TResult>(
        string transactionalId, Func<ProducerIdAndEpoch, TResult> followup)
    {
        // Synchronous, as Java's is: the lookup happens before the thenApply, so an
        // unrequested id is a usage error at the call site rather than a task outcome.
        if (transactionalId is null)
        {
            throw new ArgumentNullException(nameof(transactionalId));
        }

        if (!_values.TryGetValue(transactionalId, out Task<ProducerIdAndEpoch>? source))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "TransactionalId `{0}` was not included in the request",
                    transactionalId),
                nameof(transactionalId));
        }

        return Project(source, followup);

        static async Task<TResult> Project(
            Task<ProducerIdAndEpoch> source, Func<ProducerIdAndEpoch, TResult> followup) =>
            followup(await source.ConfigureAwait(false));
    }
}
