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
using System.Threading.Tasks;

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
/// task, which is the whole point of the view ("if a partial listing is sufficient",
/// <c>:57-59</c>), while <see cref="AllByBrokerId"/> and <see cref="All"/> fail with the first
/// error they encounter. The call itself fails only when the broker list could not be
/// discovered — and then all three views fail with that error.
/// </para>
/// <para>
/// ⚠ <b>The two stages resolve independently, as in Java.</b> <see cref="ByBrokerId"/>
/// resolves as soon as the brokers are known, while their own tasks may still be running,
/// and each broker's task resolves when <em>that</em> broker finishes — a fast broker is
/// not held up by a slow or failing one.
/// </para>
/// <para>
/// A listing's <see cref="TransactionListing.State"/> is decoded from Java's
/// <c>TransactionState.toString()</c> spelling, the same table
/// <see cref="ListTransactionsOptions.FilteredStates"/> writes.
/// </para>
/// </remarks>
public sealed class ListTransactionsResult
{
    private readonly Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>
        _byBrokerId;

    /// <summary>Wraps the outer, broker-keyed future — Java's <c>byBrokerId()</c>.</summary>
    /// <param name="byBrokerId">
    /// The future of the broker map; each value is that broker's own task, which may still be
    /// pending when the map resolves.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="byBrokerId"/> is null.</exception>
    internal ListTransactionsResult(
        Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>> byBrokerId)
    {
        _byBrokerId = byBrokerId ?? throw new ArgumentNullException(nameof(byBrokerId));
    }

    /// <summary>
    /// Every listing in the cluster, failing with the first error — Java's <c>all()</c>
    /// (<c>:48</c>).
    /// </summary>
    /// <returns>A task yielding every broker's listings, flattened.</returns>
    public Task<IReadOnlyCollection<TransactionListing>> All() => Flatten(AllByBrokerId());

    /// <summary>
    /// One task per broker, <b>keeping</b> each broker's own outcome — Java's
    /// <c>byBrokerId()</c> (<c>:68</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the per-broker map. It fails only if the broker list could not be
    /// discovered; a broker whose listing failed appears as a faulted entry.
    /// </returns>
    public Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>> ByBrokerId() =>
        _byBrokerId;

    /// <summary>
    /// Every listing keyed by the broker hosting it, failing with the first error — Java's
    /// <c>allByBrokerId()</c> (<c>:92</c>).
    /// </summary>
    /// <returns>A task yielding the broker-keyed map of listings.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Fail-fast, as in Java</b> (<c>:92-118</c>): it completes with the full map once
    /// every broker has succeeded, and fails with a broker's error <b>as soon as that broker
    /// fails</b> — it does not wait for the brokers still running. "First" is first to
    /// <em>complete</em>, not the lowest broker id.
    /// </para>
    /// <para>
    /// ⚠ <b>Deviation, an empty map:</b> with no brokers this completes with an empty map. Java's
    /// own loop never completes in that case — its <c>allFuture</c> is completed only from a
    /// per-broker callback, and there are none — which is a quirk, not a contract; the core's
    /// own synchronous listing returns an empty result there too.
    /// </para>
    /// </remarks>
    public Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> AllByBrokerId() =>
        Gather(_byBrokerId);

    private static async Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> Gather(
        Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>> byBrokerId)
    {
        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await byBrokerId.ConfigureAwait(false);

        // In completion order, so the first broker to FAIL fails the view without waiting for
        // the ones still running — Java's allByBrokerId(). Task.WhenAll would wait for all.
        List<Task<IReadOnlyCollection<TransactionListing>>> pending =
            new List<Task<IReadOnlyCollection<TransactionListing>>>(brokers.Values);
        while (pending.Count > 0)
        {
            Task<IReadOnlyCollection<TransactionListing>> finished =
                await Task.WhenAny(pending).ConfigureAwait(false);
            if (finished.IsFaulted || finished.IsCanceled)
            {
                // Rethrows that broker's own exception (not an AggregateException).
                await finished.ConfigureAwait(false);
            }

            pending.Remove(finished);
        }

        Dictionary<int, IReadOnlyCollection<TransactionListing>> listings =
            new Dictionary<int, IReadOnlyCollection<TransactionListing>>(
                brokers.Count, EqualityComparer<int>.Default);
        foreach (KeyValuePair<int, Task<IReadOnlyCollection<TransactionListing>>> broker in brokers)
        {
            listings.Add(broker.Key, broker.Value.Result);
        }

        return listings;
    }

    private static async Task<IReadOnlyCollection<TransactionListing>> Flatten(
        Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> allByBrokerId)
    {
        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> brokers =
            await allByBrokerId.ConfigureAwait(false);

        List<TransactionListing> listings = new List<TransactionListing>();
        foreach (IReadOnlyCollection<TransactionListing> broker in brokers.Values)
        {
            listings.AddRange(broker);
        }

        return listings;
    }
}
