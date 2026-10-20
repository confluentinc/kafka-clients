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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The flavour adapter for <c>listTransactions</c> (M15/P14 S5f): the three views of a <see cref="ListTransactionsResult"/>
/// or an <see cref="AsyncListTransactionsResult"/> over one <see cref="ListTransactionsAdminOperation"/> of the same
/// flavour, the per-broker futures of a resolved <c>ByBrokerId()</c> map, and the operation's own outer stage, each
/// observed through a <see cref="FutureProbe"/>, so a sync view that would hang fails the row within the deadline
/// instead of hanging the run.
/// </summary>
/// <remarks>
/// Shared by <see cref="AdminP14SyncCompositionTests"/>, whose rows complete the operation through its entry points by
/// hand, and <see cref="AdminListTransactionsTwoStageTests"/>, whose rows drive it through production's submit, the
/// real <c>AdminCallbacks</c> trampolines and the countdown: one adapter, so the two suites read the views alike.
/// </remarks>
internal sealed class ListTransactionsHarness
{
    private readonly ListTransactionsResult? _sync;
    private readonly AsyncListTransactionsResult? _async;

    private ListTransactionsHarness(
        ListTransactionsAdminOperation operation, ListTransactionsResult? sync, AsyncListTransactionsResult? async)
    {
        Operation = operation;
        _sync = sync;
        _async = async;
    }

    internal ListTransactionsAdminOperation Operation { get; }

    /// <summary>A new operation of <paramref name="flavour"/>, wrapped in that flavour's Result.</summary>
    internal static ListTransactionsHarness For(AdminFlavour flavour) =>
        Over(new ListTransactionsAdminOperation(flavour), flavour);

    /// <summary>
    /// <paramref name="operation"/>, built for <paramref name="flavour"/>, wrapped in that flavour's Result through its
    /// <c>From</c>, as the client of that flavour wraps it.
    /// </summary>
    internal static ListTransactionsHarness Over(ListTransactionsAdminOperation operation, AdminFlavour flavour) =>
        flavour == AdminFlavour.Sync
            ? new ListTransactionsHarness(operation, ListTransactionsResult.From(operation), null)
            : new ListTransactionsHarness(operation, null, AsyncListTransactionsResult.From(operation));

    internal FutureProbe ByBrokerId() =>
        _sync is not null ? FutureProbe.Of(_sync.ByBrokerId()) : FutureProbe.Of(_async!.ByBrokerId());

    internal FutureProbe AllByBrokerId() =>
        _sync is not null ? FutureProbe.Of(_sync.AllByBrokerId()) : FutureProbe.Of(_async!.AllByBrokerId());

    internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

    /// <summary>
    /// The operation's own outer stage, which the Result's <c>ByBrokerId()</c> projects. It completes on the thread that
    /// completes it — the countdown's last release, say — where the async Result's projection completes on a
    /// continuation, so it is the witness to read with <see cref="FutureProbe.IsDone"/> right after that release.
    /// </summary>
    internal FutureProbe OuterStage() =>
        _sync is not null
            ? FutureProbe.Of(
                new KafkaFuture<IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>>(
                    Operation.SyncByBrokerId))
            : FutureProbe.Of(
                new AsyncKafkaFuture<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>(
                    Operation.ByBrokerId));

    /// <summary>The broker ids of a resolved <c>ByBrokerId()</c> map, in its order.</summary>
    internal IReadOnlyList<int> BrokerIdsOf(object? byBrokerId) =>
        _sync is not null
            ? Assert.IsAssignableFrom<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>>(
                byBrokerId).Keys.ToList()
            : Assert.IsAssignableFrom<IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>>>(
                byBrokerId).Keys.ToList();

    /// <summary><paramref name="brokerId"/>'s own future, from a resolved <c>ByBrokerId()</c> map.</summary>
    internal FutureProbe BrokerOf(object? byBrokerId, int brokerId) =>
        _sync is not null
            ? FutureProbe.Of(
                Assert.IsAssignableFrom<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>>(
                    byBrokerId)[brokerId])
            : FutureProbe.Of(
                Assert.IsAssignableFrom<IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>>>(
                    byBrokerId)[brokerId]);

    /// <summary>Whether <c>ByBrokerId()</c> hands out an equal future on every call.</summary>
    internal bool ByBrokerIdEqualAcrossCalls() =>
        _sync is not null ? _sync.ByBrokerId() == _sync.ByBrokerId() : _async!.ByBrokerId() == _async.ByBrokerId();

    /// <summary>Whether <c>AllByBrokerId()</c> and <c>All()</c> each hand out a new future per call.</summary>
    internal bool AggregatesFreshPerCall() =>
        _sync is not null
            ? _sync.AllByBrokerId() != _sync.AllByBrokerId() && _sync.All() != _sync.All()
            : _async!.AllByBrokerId() != _async.AllByBrokerId() && _async.All() != _async.All();
}
