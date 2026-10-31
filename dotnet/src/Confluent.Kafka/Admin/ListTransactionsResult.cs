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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>listTransactions</c> call — Java's
/// <c>org.apache.kafka.clients.admin.ListTransactionsResult</c>.
/// </summary>
/// <remarks>
/// A broker that fails is not a call failure: <see cref="ByBrokerId"/> keeps that failure as its
/// entry's own future, while <see cref="AllByBrokerId"/> and <see cref="All"/> fail with the first
/// error. When the broker list cannot be discovered, all three fail with that error.
/// <see cref="ByBrokerId"/> resolves once the brokers are known, and each broker's future when
/// that broker finishes, as in Java.
/// </remarks>
/// <seealso cref="AsyncListTransactionsResult"/>
public sealed class ListTransactionsResult
{
    private readonly KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> _future;
    private readonly SafeAdminResultHandle _native;

    private ListTransactionsResult(
        KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> future,
        SafeAdminResultHandle native)
    {
        _future = future;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// Every listing in the cluster, failing with the first error — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding every broker's listings, flattened.</returns>
    /// <remarks>Each call builds a new future.</remarks>
    public KafkaFuture<IReadOnlyCollection<TransactionListing>> All() =>
        new KafkaFuture<IReadOnlyCollection<TransactionListing>>(
            AllByBroker(_future).ThenApply<IReadOnlyCollection<TransactionListing>>(Flatten));

    /// <summary>
    /// One future per broker, keeping each broker's own outcome — Java's <c>byBrokerId()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the per-broker map. It fails only if the broker list could not be
    /// discovered; a broker whose listing failed appears as a failed entry.
    /// </returns>
    /// <remarks>Each call builds a new future over the same per-broker futures, as Java's <c>byBrokerId()</c> does.</remarks>
    public KafkaFuture<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>> ByBrokerId() =>
        new KafkaFuture<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>>(
            ByBroker(_future, static broker => new KafkaFuture<IReadOnlyCollection<TransactionListing>>(broker)));

    /// <summary>
    /// Every listing keyed by the broker hosting it, failing with the first error — Java's
    /// <c>allByBrokerId()</c>.
    /// </summary>
    /// <returns>A future yielding the broker-keyed map of listings.</returns>
    /// <remarks>
    /// Fail-fast, as in Java: it fails with a broker's error as soon as that broker fails, the first
    /// to complete rather than the lowest broker id. With no brokers it never completes, as Java's.
    /// Each call builds a new future.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> AllByBrokerId() =>
        new KafkaFuture<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(AllByBroker(_future));

    internal static ListTransactionsResult OfClient(
        (KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> ByBrokerId,
            SafeAdminResultHandle Result) native) =>
        new ListTransactionsResult(native.ByBrokerId, native.Result);

    internal static KafkaFutureImpl<IReadOnlyDictionary<int, TFuture>> ByBroker<TFuture>(
        KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> future,
        Func<KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>, TFuture> wrap)
    {
        KafkaFutureImpl<IReadOnlyDictionary<int, TFuture>> byBroker = new KafkaFutureImpl<IReadOnlyDictionary<int, TFuture>>();
        future.AddListener(_ =>
        {
            if (future.Failure is { } failure)
            {
                byBroker.CompleteWithError(failure);
                return;
            }

            IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> brokers = future.GetNow(null!);
            Dictionary<int, TFuture> wrapped = new Dictionary<int, TFuture>(brokers.Count);
            foreach (KeyValuePair<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> broker in brokers)
            {
                wrapped.Add(broker.Key, wrap(broker.Value));
            }

            byBroker.Complete(wrapped);
        });
        return byBroker;
    }

    internal static KafkaFutureImpl<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> AllByBroker(
        KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> future)
    {
        KafkaFutureImpl<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> all =
            new KafkaFutureImpl<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>();
        future.AddListener(_ =>
        {
            if (future.Failure is { } failure)
            {
                all.CompleteWithError(failure);
                return;
            }

            IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> brokers = future.GetNow(null!);
            object gate = new object();
            Dictionary<int, IReadOnlyCollection<TransactionListing>> listings =
                new Dictionary<int, IReadOnlyCollection<TransactionListing>>(brokers.Count);
            HashSet<int> remaining = new HashSet<int>(brokers.Keys);
            bool decided = false;
            foreach (KeyValuePair<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> broker in brokers)
            {
                int brokerId = broker.Key;
                KafkaFutureImpl<IReadOnlyCollection<TransactionListing>> listing = broker.Value;
                listing.AddListener(settled =>
                {
                    Exception? error = listing.Failure;
                    bool complete = false;
                    lock (gate)
                    {
                        if (decided)
                        {
                            return;
                        }

                        if (error is null)
                        {
                            listings.Add(brokerId, listing.GetNow(null!));
                            remaining.Remove(brokerId);
                            complete = remaining.Count == 0;
                        }

                        decided = error is not null || complete;
                    }

                    if (error is not null)
                    {
                        all.CompleteWithError(error);
                    }
                    else if (complete)
                    {
                        all.Complete(listings);
                    }
                });
            }
        });
        return all;
    }

    internal static IReadOnlyCollection<TransactionListing> Flatten(
        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> brokers)
    {
        List<TransactionListing> listings = new List<TransactionListing>();
        foreach (IReadOnlyCollection<TransactionListing> broker in brokers.Values)
        {
            listings.AddRange(broker);
        }

        return listings;
    }
}
