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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 F5 — <c>listTransactions</c>' two-stage bridge: a broker-discovery callback
/// fired once, then one callback per discovered broker, sharing one <c>user_data</c>
/// (header, <c>kafka_admin_AdminClient_list_transactions_async</c>).
/// </summary>
/// <remarks>
/// <para>
/// The trampolines are driven through <b>production's own submit</b> (the
/// <see cref="NativeAdminClient.NativeListTransactionsSubmit"/> seam), with the callbacks and
/// the <c>user_data</c> that submit handed native, fired the way the header says the core
/// fires them. The mock admin client cannot drive this RPC — it refuses it at discovery
/// (<c>PublicAdminP8Tests.ListTransactions_FailsEveryViewWithJavasOwnMessage</c>) — and no
/// ABI constructor builds a <c>ListTransactionsResult_t</c>, so a <b>successful</b> per-broker
/// callback is exercised one layer down, on <see cref="ListTransactionsAdminOperation"/>
/// itself; its end-to-end proof is the broker-backed suite.
/// </para>
/// <para>
/// ⚠ <b>Every row that drives the countdown runs on both flavours</b> (M15/P14 S5f) — each
/// seam row, the countdown-zero row and the <see cref="AdminOperation.AddPendingCallbacks"/>
/// row. The operation's completion code branches on <see cref="AdminFlavour"/>: on the sync
/// flavour every per-broker completion, the countdown-zero sweep's included, must also count
/// down the fail-fast <c>BrokerGroup</c> that a sync <c>AllByBrokerId().Get()</c> waits on, so
/// a row run on one flavour pins half of that code. Each sync view is read through
/// <see cref="ListTransactionsHarness"/>'s <see cref="FutureProbe"/>s: pending is a
/// <c>Get()</c> parked on a dedicated thread, and settled is that same <c>Get()</c> returning
/// within the deadline, so a sync view that would hang fails the row. A repeated broker id runs
/// on both flavours too, at the seam and one layer down:
/// <see cref="ListTransactionsAdminOperation.PublishBrokers"/> folds a repeat into one entry in a
/// separate loop per flavour, and only the sync loop's output arms that group. Of the
/// operation-level rows only <see cref="TheFailFastViews_FailWithTheFirstBrokerToFail_NotTheLowestId"/>
/// stays async-only; its flavour pair is the <c>two-fail-first-in-time</c> <c>listTransactions</c>
/// row of <see cref="AdminP14SyncCompositionTests"/>.
/// </para>
/// <para>
/// <see cref="SafeHandle.IsClosed"/> after the client's own <c>Dispose</c> is the witness of
/// the operation's last release (see <see cref="AdminP9CountdownTests"/>): the span-the-op
/// reference is dropped only when the countdown reaches zero.
/// </para>
/// </remarks>
public sealed class AdminListTransactionsTwoStageTests
{
    private const string Sync = "sync";

    private const string Async = "async";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- through the seam ----------------------------------------------------------------

    /// <summary>
    /// Java's two stages resolve independently: the map of per-broker futures resolves as
    /// soon as the brokers are known, while every broker is still running.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task TheDiscovery_ResolvesByBrokerId_WhileEveryBrokerIsStillPending(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;

        Discover(submission, 3, 1, 2);

        object? brokers = await views.ByBrokerId().Outcome();
        Assert.Equal(new[] { 1, 2, 3 }, views.BrokerIdsOf(brokers).OrderBy(static id => id).ToArray());
        foreach (int id in views.BrokerIdsOf(brokers))
        {
            Assert.False(views.BrokerOf(brokers, id).IsDone, "no broker has finished");
        }

        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        allByBrokerId.AssertPending();
        all.AssertPending();

        FailBroker(submission, 1, 14, "broker 1 loading");
        FailBroker(submission, 2, 14, "broker 2 loading");
        FailBroker(submission, 3, 14, "broker 3 loading");

        // Which broker a gathering view reports depends on how the three failures interleave
        // with the view's own wait; that it settles with one of them does not.
        string[] loading = { "broker 1 loading", "broker 2 loading", "broker 3 loading" };
        Assert.Contains(Assert.IsType<KafkaException>(await allByBrokerId.Outcome()).Message, loading);
        Assert.Contains(Assert.IsType<KafkaException>(await all.Outcome()).Message, loading);
    }

    /// <summary>
    /// A broker's failure faults <b>that broker's</b> future only — the point of Java's
    /// <c>byBrokerId()</c> ("if a partial listing is sufficient").
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task EachBroker_CompletesIndependently_WithItsOwnError(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        Discover(submission, 1, 2);

        object? brokers = await views.ByBrokerId().Outcome();
        FutureProbe first = views.BrokerOf(brokers, 1);
        FutureProbe second = views.BrokerOf(brokers, 2);
        first.AssertPending();
        second.AssertPending();

        FailBroker(submission, 1, 15, "broker 1 has no coordinator");

        AssertFailure(await first.Outcome(), 15, "broker 1 has no coordinator");

        // Broker 2 has its own callback still to come.
        second.AssertPending();

        FailBroker(submission, 2, 31, "broker 2 refused");

        AssertFailure(await second.Outcome(), 31, "broker 2 refused");
    }

    /// <summary>
    /// <c>allByBrokerId()</c> and <c>all()</c> fail fast, as Java's do: the first broker to
    /// fail fails them without waiting for a broker still running.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task AllByBrokerIdAndAll_FailFast_WhileAnotherBrokerIsPending(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        Discover(submission, 1, 2);

        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        allByBrokerId.AssertPending();
        all.AssertPending();

        FailBroker(submission, 2, 31, "broker 2 refused");

        KafkaException viaMap = AssertFailure(await allByBrokerId.Outcome(), 31, "broker 2 refused");
        Assert.Same(viaMap, await all.Outcome());

        object? brokers = await views.ByBrokerId().Outcome();
        Assert.False(views.BrokerOf(brokers, 1).IsDone, "the fail-fast views must not have waited for broker 1");

        FailBroker(submission, 1, 14, "broker 1 loading");
    }

    /// <summary>
    /// A discovery failure is the call's only failure, and all three views fail with it — the
    /// same exception, carrying the core's code and message.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ADiscoveryError_FailsAllThreeViews_WithTheSameError(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;

        FutureProbe byBrokerId = views.ByBrokerId();
        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        byBrokerId.AssertPending();
        allByBrokerId.AssertPending();
        all.AssertPending();

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the discovery callback is still owed");

        submission.ByBrokerIdCallback(IntPtr.Zero, 0, MakeError(7, "metadata timed out"), submission.UserData);
        Assert.True(handle.IsClosed, "a failed discovery is the operation's last callback");

        KafkaException failure = AssertFailure(await byBrokerId.Outcome(), 7, "metadata timed out");
        Assert.Same(failure, await allByBrokerId.Outcome());
        Assert.Same(failure, await all.Outcome());
    }

    /// <summary>
    /// No brokers: the map is empty, and — a recorded deviation from a Java quirk — the two
    /// gathering views resolve empty instead of never completing.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task NoBrokers_ResolveEveryViewEmpty_AndReleaseOnTheDiscovery(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;

        FutureProbe allByBrokerId = views.AllByBrokerId();
        allByBrokerId.AssertPending();
        TestTimeout.Run(admin.Dispose, s_deadline);

        submission.ByBrokerIdCallback(IntPtr.Zero, 0, IntPtr.Zero, submission.UserData);
        Assert.True(handle.IsClosed, "no per-broker callback follows an empty discovery");

        Assert.Empty(views.BrokerIdsOf(await views.ByBrokerId().Outcome()));
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(
            await allByBrokerId.Outcome()));
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyCollection<TransactionListing>>(await views.All().Outcome()));
    }

    /// <summary>
    /// ⚠⚠ Plan §9 R3 — the differential. The discovery adds the per-broker callbacks to the
    /// countdown <b>before</b> it releases its own, so the operation (its <c>GCHandle</c> and
    /// the span-the-op client reference) is released by the last per-broker callback and not
    /// a moment earlier.
    /// </summary>
    /// <remarks>
    /// Each "not released" assertion runs <b>before</b> the next callback fires, so a
    /// released-too-early mutant fails here instead of recovering a freed <c>GCHandle</c>.
    /// </remarks>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task TheOperation_IsReleasedByTheLastPerBrokerCallback_AndNoEarlier(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the discovery callback is still owed");

        Discover(submission, 1, 2);
        Assert.False(handle.IsClosed, "two per-broker callbacks are still owed");
        object? brokers = await views.ByBrokerId().Outcome();

        FailBroker(submission, 2, 14, "broker 2 loading");
        Assert.False(handle.IsClosed, "one per-broker callback is still owed");

        FailBroker(submission, 1, 14, "broker 1 loading");
        Assert.True(handle.IsClosed, "the last per-broker callback must run the deferred destroy");

        AssertFailure(await views.BrokerOf(brokers, 1).Outcome(), 14, "broker 1 loading");
        AssertFailure(await views.BrokerOf(brokers, 2).Outcome(), 14, "broker 2 loading");

        // Requested with both brokers complete: the first faulted in discovery order.
        AssertFailure(await views.AllByBrokerId().Outcome(), 14, "broker 1 loading");
    }

    /// <summary>
    /// The discovery can fire <b>inline</b>, on the submitting thread, before the submit
    /// returns (the header says so for a NULL admin). The per-broker callbacks still keep the
    /// operation alive after the submit returns.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task AnInlineDiscovery_StillWaitsForItsBrokers(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour, inline: static submitted => Discover(submitted, 5));
        ListTransactionsHarness views = submission.Views;

        FutureProbe byBrokerId = views.ByBrokerId();
        Assert.True(byBrokerId.IsDone, "the inline discovery ran before the submit returned");
        object? brokers = await byBrokerId.Outcome();
        Assert.Equal(new[] { 5 }, views.BrokerIdsOf(brokers).ToArray());

        FutureProbe broker = views.BrokerOf(brokers, 5);
        FutureProbe allByBrokerId = views.AllByBrokerId();
        broker.AssertPending();
        allByBrokerId.AssertPending();

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "broker 5's callback is still owed");

        FailBroker(submission, 5, 14, "broker 5 loading");
        Assert.True(handle.IsClosed, "broker 5's callback was the last one");

        KafkaException failure = AssertFailure(await broker.Outcome(), 14, "broker 5 loading");
        Assert.Same(failure, await allByBrokerId.Outcome());
    }

    /// <summary>
    /// An inline discovery <b>failure</b> — the header's path when the RPC cannot be
    /// submitted — faults the result before the submit returns, and the submit token alone
    /// then releases the operation at the submit boundary.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task AnInlineDiscoveryError_FaultsBeforeTheSubmitReturns_AndReleasesThere(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(
            admin,
            flavour,
            inline: static submitted => submitted.ByBrokerIdCallback(
                IntPtr.Zero, 0, MakeError(7, "could not submit"), submitted.UserData));
        ListTransactionsHarness views = submission.Views;

        FutureProbe byBrokerId = views.ByBrokerId();
        Assert.True(byBrokerId.IsDone, "the inline discovery ran before the submit returned");

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "nothing is owed once the submit has returned");

        KafkaException failure = AssertFailure(await views.All().Outcome(), 7, "could not submit");
        Assert.Same(failure, await byBrokerId.Outcome());
    }

    /// <summary>
    /// ⚠ Plan §9 R4 — the per-broker callbacks are not serialized. Fired from two threads at
    /// once, every broker's future gets exactly its own outcome and the operation is
    /// released exactly once, at the end.
    /// </summary>
    /// <remarks>
    /// On the sync flavour each of those completions also stamps and counts down the fail-fast
    /// group under its lock: a gathering view requested before the threads start settles, and
    /// one requested once they have finished reports broker 1 — the first faulted in discovery
    /// order — as the async flavour's does.
    /// </remarks>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task PerBrokerCallbacks_FromTwoThreadsAtOnce_EachResolveTheirOwnBroker(string flavour)
    {
        const int brokerCount = 64;
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        TestTimeout.Run(admin.Dispose, s_deadline);

        int[] ids = Enumerable.Range(1, brokerCount).ToArray();
        Discover(submission, ids);
        object? brokers = await views.ByBrokerId().Outcome();
        FutureProbe early = views.AllByBrokerId();
        early.AssertPending();

        // Build every error up front, so the two threads only fire.
        IntPtr[] errors = ids.Select(static id => MakeError(14, "broker " + id + " loading")).ToArray();

        using Barrier start = new Barrier(2);
        Thread evens = new Thread(() => FireEvery(submission, ids, errors, start, remainder: 0));
        Thread odds = new Thread(() => FireEvery(submission, ids, errors, start, remainder: 1));
        evens.Start();
        odds.Start();
        Assert.True(evens.Join(s_deadline), "the even-broker thread did not finish");
        Assert.True(odds.Join(s_deadline), "the odd-broker thread did not finish");

        Assert.True(handle.IsClosed, "every per-broker callback has landed");
        foreach (int id in ids)
        {
            AssertFailure(await views.BrokerOf(brokers, id).Outcome(), 14, "broker " + id + " loading");
        }

        // Which broker failed first in time is a race between the two threads; that the view
        // requested before them settles with one of their errors is not.
        Assert.Equal(14, Assert.IsType<KafkaException>(await early.Outcome()).Code);
        AssertFailure(await views.AllByBrokerId().Outcome(), 14, "broker 1 loading");
    }

    /// <summary>
    /// A per-broker callback with neither a value nor an error breaks the header's "exactly
    /// one" contract; that broker faults with a message naming it rather than reading NULL.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task APerBrokerCallbackWithNeitherValueNorError_FaultsThatBroker(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        TestTimeout.Run(admin.Dispose, s_deadline);
        Discover(submission, 7);

        FutureProbe all = views.All();
        all.AssertPending();

        submission.Callback(7, IntPtr.Zero, IntPtr.Zero, submission.UserData);
        Assert.True(handle.IsClosed, "the malformed callback still releases its slot");

        AssertFailure(
            await all.Outcome(), 0, "The listTransactions result carried neither listings nor an error for broker 7.");
    }

    /// <summary>
    /// A callback naming a broker the discovery never announced "cannot happen" per the
    /// header. It resolves nothing and still releases its slot; the broker that then never
    /// answered is faulted at countdown zero rather than left hanging — on the sync flavour
    /// that fault also counts the broker down in the fail-fast group, or the gathering views
    /// would wait for it for ever.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task AnUnannouncedBroker_ResolvesNothing_AndTheSilentBrokerFaultsAtZero(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        TestTimeout.Run(admin.Dispose, s_deadline);
        Discover(submission, 1);

        object? brokers = await views.ByBrokerId().Outcome();
        Assert.Equal(new[] { 1 }, views.BrokerIdsOf(brokers).ToArray());
        FutureProbe silent = views.BrokerOf(brokers, 1);
        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        silent.AssertPending();
        allByBrokerId.AssertPending();
        all.AssertPending();

        FailBroker(submission, 99, 31, "an unannounced broker");
        Assert.True(handle.IsClosed, "the count the discovery announced has been released");

        KafkaException failure = AssertFailure(
            await silent.Outcome(), 0, "The listTransactions result contained no entry for broker 1.");
        Assert.Same(failure, await allByBrokerId.Outcome());
        Assert.Same(failure, await all.Outcome());
    }

    /// <summary>
    /// A positive broker count with a NULL id array breaks the header's contract; reading it
    /// would dereference NULL on a foreign thread. The call fails instead, and the
    /// per-broker callbacks native still owes are absorbed without leaking the operation.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task APositiveCountWithNoIds_FailsTheCall_AndStillBalancesTheCountdown(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        TestTimeout.Run(admin.Dispose, s_deadline);

        FutureProbe byBrokerId = views.ByBrokerId();
        FutureProbe all = views.All();
        byBrokerId.AssertPending();

        submission.ByBrokerIdCallback(IntPtr.Zero, 2, IntPtr.Zero, submission.UserData);
        Assert.False(handle.IsClosed, "native still owes the two per-broker callbacks it announced");

        KafkaException failure = AssertFailure(
            await byBrokerId.Outcome(), 0, "The listTransactions broker discovery reported 2 broker(s) but no broker ids.");
        Assert.Same(failure, await all.Outcome());

        FailBroker(submission, 1, 14, "broker 1 loading");
        Assert.False(handle.IsClosed, "one per-broker callback is still owed");
        FailBroker(submission, 2, 14, "broker 2 loading");
        Assert.True(handle.IsClosed, "both announced callbacks have landed");
    }

    /// <summary>
    /// A discovery that repeats a broker id breaks the header's promise of distinct ids. The
    /// repeat is one entry, as in Java's map, and every view still settles. The countdown counts
    /// the callbacks native announced, so the repeat's own callback releases a slot and resolves
    /// nothing: its broker has been answered already.
    /// </summary>
    /// <remarks>
    /// <see cref="ListTransactionsAdminOperation.PublishBrokers"/> folds the repeat in a separate
    /// loop per flavour, and only the sync loop's output arms the fail-fast group. A repeat that
    /// reached that group would fail the discovery inside the trampoline, and all three views with
    /// it, on the sync flavour alone.
    /// </remarks>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ARepeatedBrokerId_IsOneEntry_AndEveryViewSettles(string flavour)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, flavour);
        ListTransactionsHarness views = submission.Views;
        TestTimeout.Run(admin.Dispose, s_deadline);

        Discover(submission, 1, 2, 1);
        Assert.False(handle.IsClosed, "native still owes the three per-broker callbacks it announced");

        object? brokers = await views.ByBrokerId().Outcome();
        Assert.Equal(new[] { 1, 2 }, views.BrokerIdsOf(brokers).OrderBy(static id => id).ToArray());
        FutureProbe first = views.BrokerOf(brokers, 1);
        FutureProbe second = views.BrokerOf(brokers, 2);
        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        first.AssertPending();
        second.AssertPending();
        allByBrokerId.AssertPending();
        all.AssertPending();

        FailBroker(submission, 1, 14, "broker 1 loading");
        Assert.False(handle.IsClosed, "two per-broker callbacks are still owed");

        KafkaException loading = AssertFailure(await first.Outcome(), 14, "broker 1 loading");
        Assert.Same(loading, await allByBrokerId.Outcome());
        Assert.Same(loading, await all.Outcome());
        second.AssertPending();

        // The repeat's own callback: broker 1 keeps its first outcome.
        FailBroker(submission, 1, 31, "broker 1 again");
        Assert.False(handle.IsClosed, "broker 2's callback is still owed");
        Assert.Same(loading, await views.BrokerOf(brokers, 1).Outcome());
        second.AssertPending();

        FailBroker(submission, 2, 31, "broker 2 refused");
        Assert.True(handle.IsClosed, "the three announced callbacks have landed");

        AssertFailure(await second.Outcome(), 31, "broker 2 refused");

        // Requested with both brokers complete: the first faulted in discovery order.
        Assert.Same(loading, await views.AllByBrokerId().Outcome());
        Assert.Same(loading, await views.All().Outcome());
    }

    // ---- the operation -------------------------------------------------------------------

    /// <summary>
    /// A broker's listings reach that broker's own entry and every view — the success half no
    /// ABI constructor lets the seam drive (see the class remarks) — and a repeated id is one
    /// entry, as in Java's map. Both run on each flavour, for the reason
    /// <see cref="ARepeatedBrokerId_IsOneEntry_AndEveryViewSettles"/> gives.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ABrokersListings_ReachItsOwnEntry_AndEveryView(string flavour)
    {
        ListTransactionsHarness views = ListTransactionsHarness.For(FlavourOf(flavour));
        ListTransactionsAdminOperation operation = views.Operation;

        TransactionListing first = new TransactionListing("txn-a", 11L, TransactionState.Ongoing);
        TransactionListing second = new TransactionListing("txn-b", 12L, TransactionState.CompleteCommit);
        IReadOnlyCollection<TransactionListing> brokerOne = new[] { first };
        IReadOnlyCollection<TransactionListing> brokerTwo = new[] { second };

        // A repeated id is one entry, as in Java's map.
        operation.PublishBrokers(new[] { 1, 2, 1 });
        operation.SetBrokerResult(1, brokerOne);

        object? brokers = await views.ByBrokerId().Outcome();
        Assert.Equal(new[] { 1, 2 }, views.BrokerIdsOf(brokers).OrderBy(static id => id).ToArray());
        Assert.Same(brokerOne, await views.BrokerOf(brokers, 1).Outcome());
        FutureProbe unanswered = views.BrokerOf(brokers, 2);
        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        unanswered.AssertPending();
        allByBrokerId.AssertPending();
        all.AssertPending();

        // An unannounced broker is inert, not a throw.
        operation.SetBrokerResult(5, brokerTwo);
        operation.SetBrokerException(5, new KafkaException("ignored"));
        unanswered.AssertPending();
        allByBrokerId.AssertPending();

        operation.SetBrokerResult(2, brokerTwo);
        Assert.Same(brokerTwo, await unanswered.Outcome());

        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> map =
            Assert.IsAssignableFrom<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(
                await allByBrokerId.Outcome());
        Assert.Equal(new[] { 1, 2 }, map.Keys.OrderBy(static id => id).ToArray());
        Assert.Same(brokerOne, map[1]);
        Assert.Same(brokerTwo, map[2]);

        IReadOnlyCollection<TransactionListing> listings =
            Assert.IsAssignableFrom<IReadOnlyCollection<TransactionListing>>(await all.Outcome());
        Assert.Equal(2, listings.Count);
        Assert.Contains(first, listings);
        Assert.Contains(second, listings);
    }

    /// <summary>
    /// "First" is first to <em>complete</em>, not the lowest broker id: a view requested while
    /// every broker is pending fails with whichever broker fails first.
    /// </summary>
    [Fact]
    public async Task TheFailFastViews_FailWithTheFirstBrokerToFail_NotTheLowestId()
    {
        ListTransactionsAdminOperation operation = new ListTransactionsAdminOperation(AdminFlavour.Async);
        AsyncListTransactionsResult result = new AsyncListTransactionsResult(operation.ByBrokerId);
        operation.PublishBrokers(new[] { 1, 2, 3 });

        Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> allByBrokerId =
            result.AllByBrokerId().Get();
        Task<IReadOnlyCollection<TransactionListing>> all = result.All().Get();

        KafkaException third = new KafkaException("broker 3 failed first");
        operation.SetBrokerException(3, third);

        Assert.Same(third, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => allByBrokerId), s_deadline));
        Assert.Same(third, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => all), s_deadline));
    }

    /// <summary>
    /// Every broker answered from two threads at once: the gathering views settle with every
    /// broker's own listings. On the sync flavour that needs each completion counted down in
    /// the fail-fast group, whose outstanding count reaches zero only when all of them have
    /// been. The seam cannot drive a successful per-broker callback (see the class remarks), so
    /// this is the success half of
    /// <see cref="PerBrokerCallbacks_FromTwoThreadsAtOnce_EachResolveTheirOwnBroker"/>, one
    /// layer down.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task PerBrokerResults_FromTwoThreadsAtOnce_SettleTheGatheringViews(string flavour)
    {
        const int brokerCount = 64;
        ListTransactionsHarness views = ListTransactionsHarness.For(FlavourOf(flavour));
        ListTransactionsAdminOperation operation = views.Operation;
        int[] ids = Enumerable.Range(1, brokerCount).ToArray();
        operation.PublishBrokers(ids);

        FutureProbe allByBrokerId = views.AllByBrokerId();
        FutureProbe all = views.All();
        allByBrokerId.AssertPending();
        all.AssertPending();

        IReadOnlyCollection<TransactionListing>[] listings = ids
            .Select(static id => (IReadOnlyCollection<TransactionListing>)new[]
            {
                new TransactionListing("txn-" + id, id, TransactionState.Ongoing),
            })
            .ToArray();

        using Barrier start = new Barrier(2);
        Thread evens = new Thread(() => AnswerEvery(operation, ids, listings, start, remainder: 0));
        Thread odds = new Thread(() => AnswerEvery(operation, ids, listings, start, remainder: 1));
        evens.Start();
        odds.Start();
        Assert.True(evens.Join(s_deadline), "the even-broker thread did not finish");
        Assert.True(odds.Join(s_deadline), "the odd-broker thread did not finish");

        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> map =
            Assert.IsAssignableFrom<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(
                await allByBrokerId.Outcome());
        Assert.Equal(ids, map.Keys.OrderBy(static id => id).ToArray());
        foreach (int id in ids)
        {
            Assert.Same(listings[id - 1], map[id]);
        }

        Assert.Equal(
            brokerCount, Assert.IsAssignableFrom<IReadOnlyCollection<TransactionListing>>(await all.Outcome()).Count);
    }

    /// <summary>
    /// At countdown zero, whatever nothing completed is faulted rather than left to hang: the
    /// outer stage when no discovery landed, and each announced broker that never answered —
    /// which, on the sync flavour, must also be counted down in the fail-fast group, or a sync
    /// <c>AllByBrokerId().Get()</c> / <c>All().Get()</c> would wait for it for ever.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task CountdownZero_FaultsEverythingNothingCompleted(string flavour)
    {
        ListTransactionsHarness undiscovered = ListTransactionsHarness.For(FlavourOf(flavour));
        FutureProbe noByBrokerId = undiscovered.ByBrokerId();
        FutureProbe noAllByBrokerId = undiscovered.AllByBrokerId();
        noByBrokerId.AssertPending();
        noAllByBrokerId.AssertPending();

        undiscovered.Operation.SetPendingCallbacks(0);
        undiscovered.Operation.ReleaseSubmitToken();

        KafkaException noResult = AssertFailure(
            await noByBrokerId.Outcome(), 0, "The listTransactions call completed without delivering a result.");
        Assert.Same(noResult, await noAllByBrokerId.Outcome());

        ListTransactionsHarness discovered = ListTransactionsHarness.For(FlavourOf(flavour));
        discovered.Operation.SetPendingCallbacks(0);
        discovered.Operation.PublishBrokers(new[] { 4, 6 });
        discovered.Operation.SetBrokerResult(6, Array.Empty<TransactionListing>());

        object? brokers = await discovered.ByBrokerId().Outcome();
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyCollection<TransactionListing>>(
            await discovered.BrokerOf(brokers, 6).Outcome()));
        FutureProbe unansweredBroker = discovered.BrokerOf(brokers, 4);
        FutureProbe allByBrokerId = discovered.AllByBrokerId();
        FutureProbe all = discovered.All();
        unansweredBroker.AssertPending();
        allByBrokerId.AssertPending();
        all.AssertPending();

        discovered.Operation.ReleaseSubmitToken();

        KafkaException unanswered = AssertFailure(
            await unansweredBroker.Outcome(), 0, "The listTransactions result contained no entry for broker 4.");
        Assert.Same(unanswered, await allByBrokerId.Outcome());
        Assert.Same(unanswered, await all.Outcome());
    }

    /// <summary>
    /// <see cref="AdminOperation.AddPendingCallbacks"/> extends a running countdown, and
    /// refuses — loudly — to resurrect a released one.
    /// </summary>
    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task AddPendingCallbacks_ExtendsARunningCountdown_AndRefusesAReleasedOne(string flavour)
    {
        ListTransactionsHarness harness = ListTransactionsHarness.For(FlavourOf(flavour));
        ListTransactionsAdminOperation operation = harness.Operation;
        FutureProbe outerStage = harness.OuterStage();
        operation.SetPendingCallbacks(1);
        operation.AddPendingCallbacks(2);
        operation.AddPendingCallbacks(0);
        operation.AddPendingCallbacks(-3);

        // 1 + the submit token + 2 = 4 releases; the outer stage is the zero witness.
        for (int release = 0; release < 3; release++)
        {
            operation.ReleaseOne();
            Assert.False(outerStage.IsDone, "the countdown has not reached zero");
        }

        operation.ReleaseOne();
        Assert.True(outerStage.IsDone, "the fourth release reaches zero");
        AssertFailure(
            await outerStage.Outcome(), 0, "The listTransactions call completed without delivering a result.");

        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => operation.AddPendingCallbacks(3));
        Assert.Equal(
            "Cannot add 3 pending callback(s) to an admin operation that has already been released.",
            refused.Message);

        // A non-positive add stays a no-op even after release.
        operation.AddPendingCallbacks(0);
    }

    // ---- the P/Invoke --------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ The declaration's parameter order, pinned against the header — the F5 crash was
    /// exactly a stale declaration handing native one callback in the wrong slot, which
    /// compiles and marshals without complaint.
    /// </summary>
    [Fact]
    public void TheSubmitPInvoke_MatchesTheHeadersParameterOrder()
    {
        MethodInfo method = Assert.Single(
            typeof(NativeMethods).GetMethods(BindingFlags.NonPublic | BindingFlags.Static),
            static candidate => candidate.Name == nameof(NativeMethods.AdminClientListTransactionsAsync));

        Type[] expected =
        {
            typeof(IntPtr), // const kafka_admin_AdminClient_t *admin
            typeof(IntPtr[]), // const char *const *states
            typeof(int), // int32_t state_count
            typeof(long[]), // const int64_t *producer_ids
            typeof(int), // int32_t producer_id_count
            typeof(long), // int64_t duration_ms
            typeof(IntPtr), // const char *transactional_id_pattern
            typeof(int), // int32_t timeout_ms
            typeof(AdminCallbacks.ListTransactionsByBrokerIdCallback), // by_broker_id_callback
            typeof(AdminCallbacks.ListTransactionsCallback), // callback
            typeof(IntPtr), // void *user_data
        };
        Assert.Equal(expected, method.GetParameters().Select(static parameter => parameter.ParameterType).ToArray());
        Assert.Equal(typeof(void), method.ReturnType);

        DllImportAttribute import = Assert.IsType<DllImportAttribute>(
            method.GetCustomAttribute(typeof(DllImportAttribute)));
        Assert.Equal("kafka_admin_AdminClient_list_transactions_async", import.EntryPoint);
        Assert.Equal(CallingConvention.Cdecl, import.CallingConvention);

        // Both callback typedefs are Cdecl function pointers with the header's parameters.
        AssertCdecl(
            typeof(AdminCallbacks.ListTransactionsByBrokerIdCallback),
            typeof(IntPtr), typeof(int), typeof(IntPtr), typeof(IntPtr));
        AssertCdecl(
            typeof(AdminCallbacks.ListTransactionsCallback),
            typeof(int), typeof(IntPtr), typeof(IntPtr), typeof(IntPtr));
    }

    // ---- helpers -------------------------------------------------------------------------

    /// <summary>
    /// Submits through production's own submit for a client of <paramref name="flavour"/>, then
    /// wraps the operation in that flavour's Result, as the client does once the submit has
    /// returned.
    /// </summary>
    private static Submission Submit(NativeAdminClient admin, string flavour, Action<Submission>? inline = null)
    {
        AdminFlavour adminFlavour = FlavourOf(flavour);
        Submission submission = new Submission();
        ListTransactionsAdminOperation operation = admin.ListTransactionsOperation(
            options: null,
            adminFlavour,
            (handle, states, stateCount, producerIds, producerIdCount, durationMs, pattern,
             timeoutMs, byBrokerIdCallback, callback, data) =>
            {
                submission.ByBrokerIdCallback = byBrokerIdCallback;
                submission.Callback = callback;
                submission.UserData = data;
                inline?.Invoke(submission);
            });
        submission.Views = ListTransactionsHarness.Over(operation, adminFlavour);
        return submission;
    }

    private static AdminFlavour FlavourOf(string flavour) => flavour switch
    {
        Sync => AdminFlavour.Sync,
        Async => AdminFlavour.Async,
        _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
    };

    /// <summary>
    /// Asserts <paramref name="outcome"/> — a <see cref="FutureProbe"/>'s — is a
    /// <see cref="KafkaException"/> with exactly this code and message, and returns it.
    /// </summary>
    private static KafkaException AssertFailure(object? outcome, int code, string message)
    {
        KafkaException failure = Assert.IsType<KafkaException>(outcome);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
        return failure;
    }

    /// <summary>Fires the discovery callback with a borrowed, pinned id array.</summary>
    private static void Discover(Submission submission, params int[] brokerIds)
    {
        GCHandle pin = GCHandle.Alloc(brokerIds, GCHandleType.Pinned);
        try
        {
            submission.ByBrokerIdCallback(pin.AddrOfPinnedObject(), brokerIds.Length, IntPtr.Zero, submission.UserData);
        }
        finally
        {
            pin.Free();
        }
    }

    /// <summary>Fires one broker's callback with an owned error and no value.</summary>
    private static void FailBroker(Submission submission, int brokerId, int code, string message) =>
        submission.Callback(brokerId, IntPtr.Zero, MakeError(code, message), submission.UserData);

    private static void FireEvery(
        Submission submission, int[] ids, IntPtr[] errors, Barrier start, int remainder)
    {
        start.SignalAndWait();
        for (int index = 0; index < ids.Length; index++)
        {
            if (ids[index] % 2 == remainder)
            {
                submission.Callback(ids[index], IntPtr.Zero, errors[index], submission.UserData);
            }
        }
    }

    private static void AnswerEvery(
        ListTransactionsAdminOperation operation,
        int[] ids,
        IReadOnlyCollection<TransactionListing>[] listings,
        Barrier start,
        int remainder)
    {
        start.SignalAndWait();
        for (int index = 0; index < ids.Length; index++)
        {
            if (ids[index] % 2 == remainder)
            {
                operation.SetBrokerResult(ids[index], listings[index]);
            }
        }
    }

    private static void AssertCdecl(Type delegateType, params Type[] parameters)
    {
        UnmanagedFunctionPointerAttribute pointer = Assert.IsType<UnmanagedFunctionPointerAttribute>(
            delegateType.GetCustomAttribute(typeof(UnmanagedFunctionPointerAttribute)));
        Assert.Equal(CallingConvention.Cdecl, pointer.CallingConvention);

        MethodInfo invoke = delegateType.GetMethod("Invoke")!;
        Assert.Equal(parameters, invoke.GetParameters().Select(static parameter => parameter.ParameterType).ToArray());
        Assert.Equal(typeof(void), invoke.ReturnType);
    }

    /// <inheritdoc cref="AdminOperationLifetimeTests"/>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    /// <summary>What production's submit handed native, captured through the seam.</summary>
    private sealed class Submission
    {
        /// <summary>The Result of the submit's flavour over the operation it returned, read through probes.</summary>
        internal ListTransactionsHarness Views { get; set; } = null!;

        internal AdminCallbacks.ListTransactionsByBrokerIdCallback ByBrokerIdCallback { get; set; } = null!;

        internal AdminCallbacks.ListTransactionsCallback Callback { get; set; } = null!;

        internal IntPtr UserData { get; set; }
    }
}
