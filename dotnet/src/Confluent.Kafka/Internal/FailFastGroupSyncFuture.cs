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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The sync fail-fast group (M15/P14 S5f, PLAN §3.3): the value of one sync
/// <see cref="ListTransactionsResult.AllByBrokerId"/> — every broker's listings in one map, or the first broker failure
/// <b>in time</b>, as Java's <c>allByBrokerId()</c> completes through a <c>whenComplete</c> per broker
/// (<c>ListTransactionsResult.java:92-118</c>).
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>Two waits, on the caller's thread.</b> <see cref="Get"/> waits for the outer stage (the broker discovery), whose
/// failure it rethrows as the outer latch's own instance; then on the operation's group monitor
/// (<see cref="ListTransactionsAdminOperation.WaitForBrokerGroup"/>), which wakes when every broker has completed or as
/// soon as one fails, and whose first failure it rethrows — that broker's own exception instance. A broker still pending
/// when another fails does not delay the failure. No thread-pool thread is involved.
/// </description></item>
/// <item><description>
/// <b>Not an all-of.</b> <see cref="AllOfSyncFuture"/> waits for every source in key order and reports the first failure
/// in key order (Java's <c>allOf</c>); this reports the first failure in time and waits for nothing after it. It is the
/// only composite that does.
/// </description></item>
/// <item><description>
/// <b>Built once, memoized.</b> Once every broker has succeeded the map is built under this instance's lock, once, in
/// the discovery order the outer stage carries, and every later <see cref="Get"/> returns that same instance. With no
/// broker it is an empty map (the recorded deviation on <see cref="ListTransactionsResult.AllByBrokerId"/>: Java's
/// never completes).
/// </description></item>
/// <item><description>
/// <b>Done when its outcome is.</b> <see cref="IsDone"/> never blocks: the outer stage is done, and either it failed or
/// the group has an outcome.
/// </description></item>
/// </list>
/// <para>
/// Host scaffolding with no Java counterpart (DoD #7): it is Java's per-broker <c>whenComplete</c> chain, re-expressed
/// as a wait on the caller's thread because a latch has no completion hook.
/// </para>
/// </remarks>
internal sealed class FailFastGroupSyncFuture
    : SyncFutureSource<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>
{
    private readonly ListTransactionsAdminOperation _operation;
    private readonly SyncFutureSource<IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>>
        _byBrokerId;

    private volatile IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>? _gathered;

    /// <summary>Creates the group over <paramref name="operation"/>'s two stages.</summary>
    /// <param name="operation">A <see cref="AdminFlavour.Sync"/> <c>listTransactions</c> operation.</param>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Async"/>.</exception>
    internal FailFastGroupSyncFuture(ListTransactionsAdminOperation operation)
    {
        _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        _byBrokerId = operation.SyncByBrokerId;
    }

    /// <inheritdoc/>
    internal override bool IsDone => _byBrokerId.IsDone && _operation.IsBrokerGroupDone;

    /// <inheritdoc/>
    internal override IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> Get()
    {
        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>? gathered = _gathered;
        if (gathered is not null)
        {
            return gathered;
        }

        // The discovery first: its failure is every view's, and the group is armed only once it has succeeded.
        IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>> brokers = _byBrokerId.Get();

        // Then every broker, or the first to fail in time.
        _operation.WaitForBrokerGroup()?.Throw();
        return Gather(brokers);
    }

    private IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> Gather(
        IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>> brokers)
    {
        lock (this)
        {
            IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>? gathered = _gathered;
            if (gathered is null)
            {
                Dictionary<int, IReadOnlyCollection<TransactionListing>> listings =
                    new Dictionary<int, IReadOnlyCollection<TransactionListing>>(brokers.Count, EqualityComparer<int>.Default);
                foreach (KeyValuePair<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>> broker in brokers)
                {
                    // The group succeeded, so every broker has: this neither waits nor throws.
                    listings[broker.Key] = broker.Value.Get();
                }

                gathered = listings;
                _gathered = gathered;
            }

            return gathered;
        }
    }
}
