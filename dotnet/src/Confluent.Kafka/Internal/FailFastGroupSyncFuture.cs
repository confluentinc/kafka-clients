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
/// <see cref="ListTransactionsResult.AllByBrokerId"/> — every broker's listings in one map, or the broker failure
/// Java's <c>allByBrokerId()</c> reports through its <c>whenComplete</c> per broker
/// (<c>ListTransactionsResult.java:92-118</c>): of the brokers already complete when the view registers, the first
/// faulted one in discovery order; otherwise the first to fail after it, in time.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>Registered when built.</b> Java registers inside the outer future's <c>whenComplete</c>, so its registration
/// point is the later of the call and the outer future's completion; the async flavour's gather registers when its
/// <c>await</c> of the outer stage resumes, inside the call if that stage is already done. This takes the group's
/// <see cref="ListTransactionsAdminOperation.BrokerGroupWatermark"/> in its constructor, which runs in the
/// <c>AllByBrokerId()</c> call: the count of brokers completed by then, or <c>0</c> while the brokers are not yet
/// published — no broker can complete before they are, so that view registers at their publication.
/// </description></item>
/// <item><description>
/// <b>Two waits, on the caller's thread.</b> <see cref="Get"/> waits for the outer stage (the broker discovery), whose
/// failure it rethrows as the outer latch's own instance; then on the operation's group monitor
/// (<see cref="ListTransactionsAdminOperation.WaitForBrokerGroup"/>), which wakes when every broker has completed or as
/// soon as one fails, and returns this view's winner by the rule above — that broker's own exception instance. A broker
/// still pending when the winner is known does not delay the failure. No thread-pool thread is involved.
/// </description></item>
/// <item><description>
/// <b>Not an all-of.</b> <see cref="AllOfSyncFuture"/> waits for every source in key order and reports the first failure
/// in key order (Java's <c>allOf</c>); this waits for nothing after the first failure. It is the only composite that
/// does.
/// </description></item>
/// <item><description>
/// <b>One instance per call, memoized within it.</b> Each <c>AllByBrokerId()</c> call builds a new instance with its own
/// watermark, as Java allocates a new <c>allFuture</c> per call and the async flavour composes a new gather per call.
/// Once every broker has succeeded the map is built under this instance's lock, once, in the discovery order the outer
/// stage carries, and every later <see cref="Get"/> returns that same instance; a failure is picked afresh by each
/// <see cref="Get"/>, deterministically, so it rethrows the same instance. With no broker it is an empty map (the
/// recorded deviation on <see cref="ListTransactionsResult.AllByBrokerId"/>: Java's never completes).
/// </description></item>
/// <item><description>
/// <b>Done when its outcome is.</b> <see cref="IsDone"/> never blocks: the outer stage is done, and either it failed or
/// the group has an outcome — which does not depend on the watermark.
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

    private readonly int _watermark;

    private volatile IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>? _gathered;

    /// <summary>Creates the group over <paramref name="operation"/>'s two stages, registered now.</summary>
    /// <param name="operation">A <see cref="AdminFlavour.Sync"/> <c>listTransactions</c> operation.</param>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Async"/>.</exception>
    internal FailFastGroupSyncFuture(ListTransactionsAdminOperation operation)
    {
        _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        _byBrokerId = operation.SyncByBrokerId;
        _watermark = operation.BrokerGroupWatermark();
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

        // Then every broker, or this view's winner among the failures.
        _operation.WaitForBrokerGroup(_watermark)?.Throw();
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
