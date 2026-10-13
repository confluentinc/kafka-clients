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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>listTransactions</c> call — Java's
/// <c>org.apache.kafka.clients.admin.ListTransactionsResult</c> (<c>:34</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The three views differ in what they do with a <em>per-broker</em> failure.</b> Java
/// holds one future over a map of per-broker futures (<c>:35</c>), so a broker that fails is
/// not a call failure: <see cref="ByBrokerId"/> <b>keeps</b> that failure as one entry's own
/// future, which is the whole point of the view ("if a partial listing is sufficient",
/// <c>:57-59</c>), while <see cref="AllByBrokerId"/> and <see cref="All"/> fail with the first
/// error they encounter. The call itself fails only when the broker list could not be
/// discovered — and then all three views fail with that error.
/// </para>
/// <para>
/// ⚠ <b>The two stages resolve independently, as in Java.</b> <see cref="ByBrokerId"/>
/// resolves as soon as the brokers are known, while their own futures may still be running,
/// and each broker's future resolves when <em>that</em> broker finishes — a fast broker is
/// not held up by a slow or failing one.
/// </para>
/// <para>
/// A listing's <see cref="TransactionListing.State"/> is decoded from Java's
/// <c>TransactionState.toString()</c> spelling, the same table
/// <see cref="ListTransactionsOptions.FilteredStates"/> writes.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on managed latches until its view has completed, and needs no
/// thread-pool thread: the futures here are composed on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncListTransactionsResult"/>
public sealed class ListTransactionsResult
{
    private readonly ListTransactionsAdminOperation _operation;

    private readonly KafkaFuture<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>>
        _byBrokerId;

    /// <summary>Wraps a sync operation's two stages — Java's <c>byBrokerId()</c> and its per-broker futures.</summary>
    /// <param name="operation">
    /// An <see cref="AdminFlavour.Sync"/> <c>listTransactions</c> operation: its outer latch resolves with the broker
    /// map, each value that broker's own latch, which may still be pending when the map resolves.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="operation"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Async"/>.</exception>
    internal ListTransactionsResult(ListTransactionsAdminOperation operation)
    {
        _operation = operation ?? throw new ArgumentNullException(nameof(operation));

        // Built once, so ByBrokerId() hands back an equal future on every call, as the async
        // twin's eager projection does; the projection is memoized, so every Get() returns the
        // same map, whose per-broker futures are the brokers' own latches.
        _byBrokerId = SyncAdminFutures.Project(
            new KafkaFuture<IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>>(
                operation.SyncByBrokerId),
            static brokers => SyncAdminFutures.Wrap(brokers));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> <c>listTransactions</c> operation: the one place it
    /// is built from an admin operation (M15/P14).
    /// </summary>
    internal static ListTransactionsResult From(ListTransactionsAdminOperation operation) =>
        new ListTransactionsResult(operation);

    /// <summary>
    /// Every listing in the cluster, failing with the first error — Java's <c>all()</c>
    /// (<c>:48</c>).
    /// </summary>
    /// <returns>A future yielding every broker's listings, flattened.</returns>
    /// <remarks>
    /// Java's <c>allByBrokerId().thenApply(...)</c>, so it fails exactly when, and with what, an
    /// <see cref="AllByBrokerId"/> requested by the same call does. Each call builds a new future.
    /// </remarks>
    public KafkaFuture<IReadOnlyCollection<TransactionListing>> All() =>
        SyncAdminFutures.Project(AllByBrokerId(), Flatten);

    /// <summary>
    /// One future per broker, <b>keeping</b> each broker's own outcome — Java's
    /// <c>byBrokerId()</c> (<c>:68</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the per-broker map. It fails only if the broker list could not be
    /// discovered; a broker whose listing failed appears as a failed entry.
    /// </returns>
    public KafkaFuture<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>> ByBrokerId() =>
        _byBrokerId;

    /// <summary>
    /// Every listing keyed by the broker hosting it, failing with the first error — Java's
    /// <c>allByBrokerId()</c> (<c>:92</c>).
    /// </summary>
    /// <returns>A future yielding the broker-keyed map of listings.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Fail-fast, as in Java</b> (<c>:92-118</c>): it completes with the full map once
    /// every broker has succeeded, and fails with a broker's error <b>as soon as that broker
    /// fails</b> — it does not wait for the brokers still running. "First" is first to
    /// <em>complete</em>, not the lowest broker id: of the brokers already complete when this
    /// view is requested (none, if the brokers are not yet known), the first that failed in
    /// discovery order; otherwise the first to fail after that, in time. Java registers one
    /// <c>whenComplete</c> per broker, in its map's order, once the view is requested and the
    /// brokers are known, and <see cref="AsyncListTransactionsResult.AllByBrokerId"/> reports the
    /// same broker.
    /// </para>
    /// <para>
    /// ⚠ <b>Deviation, an empty map:</b> with no brokers this completes with an empty map. Java's
    /// own loop never completes in that case — its <c>allFuture</c> is completed only from a
    /// per-broker callback, and there are none — which is a quirk, not a contract; the core's
    /// own synchronous listing returns an empty result there too.
    /// </para>
    /// <para>
    /// Each call builds a new future, as Java allocates a new <c>allFuture</c> per call. One
    /// future gathers its map once, so every <c>Get()</c> of it returns the same map, or
    /// rethrows the same broker's exception instance.
    /// </para>
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> AllByBrokerId() =>
        new KafkaFuture<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(
            new FailFastGroupSyncFuture(_operation));

    private static IReadOnlyCollection<TransactionListing> Flatten(
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
