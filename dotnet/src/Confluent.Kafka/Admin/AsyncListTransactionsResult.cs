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
/// <seealso cref="ListTransactionsResult"/>
public sealed class AsyncListTransactionsResult
{
    private readonly KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> _future;
    private readonly SafeAdminResultHandle _native;

    private AsyncListTransactionsResult(
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
    public AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>> All() =>
        new AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>(
            ListTransactionsResult.AllByBroker(_future)
                .ThenApply<IReadOnlyCollection<TransactionListing>>(ListTransactionsResult.Flatten));

    /// <summary>
    /// One future per broker, keeping each broker's own outcome — Java's <c>byBrokerId()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the per-broker map. It fails only if the broker list could not be
    /// discovered; a broker whose listing failed appears as a faulted entry.
    /// </returns>
    /// <remarks>Each call builds a new future over the same per-broker futures, as Java's <c>byBrokerId()</c> does.</remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>>> ByBrokerId() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>>>(
            ListTransactionsResult.ByBroker(
                _future, static broker => new AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>(broker)));

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
    public AsyncKafkaFuture<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> AllByBrokerId() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(
            ListTransactionsResult.AllByBroker(_future));

    internal static AsyncListTransactionsResult OfClient(
        (KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> ByBrokerId,
            SafeAdminResultHandle Result) native) =>
        new AsyncListTransactionsResult(native.ByBrokerId, native.Result);
}
