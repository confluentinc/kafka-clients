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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.FenceProducers"/> — Java's
/// <c>org.apache.kafka.clients.admin.FenceProducersResult</c>.
/// </summary>
/// <remarks>
/// Java stores one future per id carrying a <c>ProducerIdAndEpoch</c> but never publishes that
/// pair: <see cref="FencedProducers"/> erases it and <see cref="ProducerId"/> /
/// <see cref="EpochId"/> project one field each, so there is no public <c>ProducerIdAndEpoch</c>
/// type here.
/// </remarks>
/// <seealso cref="AsyncFenceProducersResult"/>
public sealed class FenceProducersResult
{
    private readonly IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> _futures;
    private readonly SafeAdminResultHandle _native;

    private FenceProducersResult(
        IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One payload-free future per requested transactional id — Java's
    /// <c>fencedProducers()</c>, whose values are <see cref="KafkaFuture"/>s, as here.
    /// </summary>
    /// <remarks>Each access builds a new map of new futures, as Java's <c>fencedProducers()</c> does.</remarks>
    public IReadOnlyDictionary<string, KafkaFuture> FencedProducers
    {
        get
        {
            Dictionary<string, KafkaFuture> fenced = new Dictionary<string, KafkaFuture>(_futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, KafkaFutureImpl<ProducerIdAndEpoch>> entry in _futures)
            {
                fenced.Add(entry.Key, new KafkaFuture(entry.Value.ThenApply<object?>(static _ => null)));
            }

            return fenced;
        }
    }

    /// <summary>
    /// The producer id generated while initializing that transaction — Java's
    /// <c>producerId(String)</c>.
    /// </summary>
    /// <param name="transactionalId">An id from <see cref="FencedProducers"/>.</param>
    /// <returns>A new future over that id's producer id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> was not included in the request — Java's
    /// <c>IllegalArgumentException</c>, thrown synchronously as Java's is.
    /// </exception>
    public KafkaFuture<long> ProducerId(string transactionalId) =>
        new KafkaFuture<long>(Find(_futures, transactionalId).ThenApply(static value => value.ProducerId));

    /// <summary>
    /// The epoch generated while initializing that transaction — Java's
    /// <c>epochId(String)</c>, a <see langword="short"/> as Java's is.
    /// </summary>
    /// <param name="transactionalId">An id from <see cref="FencedProducers"/>.</param>
    /// <returns>A new future over that id's epoch.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="transactionalId"/> was not included in the request — see
    /// <see cref="ProducerId"/>.
    /// </exception>
    public KafkaFuture<short> EpochId(string transactionalId) =>
        new KafkaFuture<short>(Find(_futures, transactionalId).ThenApply(static value => value.Epoch));

    /// <summary>
    /// Completes when every producer has been fenced, and fails with the first failure to complete
    /// otherwise — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    /// <remarks>Each call builds a new future.</remarks>
    public KafkaFuture All() => new KafkaFuture(KafkaFutureImpl.AllOf(new List<KafkaFutureImpl>(_futures.Values)));

    internal static FenceProducersResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> Producers, SafeAdminResultHandle Result) native) =>
        new FenceProducersResult(native.Producers, native.Result);

    internal static KafkaFutureImpl<ProducerIdAndEpoch> Find(
        IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> futures, string transactionalId)
    {
        if (transactionalId is null)
        {
            throw new ArgumentNullException(nameof(transactionalId));
        }

        return futures.TryGetValue(transactionalId, out KafkaFutureImpl<ProducerIdAndEpoch>? future)
            ? future
            : throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "TransactionalId `{0}` was not included in the request",
                    transactionalId),
                nameof(transactionalId));
    }
}
