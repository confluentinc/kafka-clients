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
/// <see cref="SafeHandle.IsClosed"/> after the client's own <c>Dispose</c> is the witness of
/// the operation's last release (see <see cref="AdminP9CountdownTests"/>): the span-the-op
/// reference is dropped only when the countdown reaches zero.
/// </para>
/// </remarks>
public sealed class AdminListTransactionsTwoStageTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- through the seam ----------------------------------------------------------------

    /// <summary>
    /// Java's two stages resolve independently: the map of per-broker awaitables resolves as
    /// soon as the brokers are known, while every broker is still running.
    /// </summary>
    [Fact]
    public async Task TheDiscovery_ResolvesByBrokerId_WhileEveryBrokerIsStillPending()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        Submission submission = Submit(admin);

        Discover(submission, 3, 1, 2);

        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await TestTimeout.Run(() => submission.Result.ByBrokerId(), s_deadline);
        Assert.Equal(new[] { 1, 2, 3 }, brokers.Keys.OrderBy(static id => id).ToArray());
        Assert.All(brokers.Values, static broker => Assert.False(broker.IsCompleted));

        Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> allByBrokerId =
            submission.Result.AllByBrokerId();
        Task<IReadOnlyCollection<TransactionListing>> all = submission.Result.All();
        Assert.False(allByBrokerId.IsCompleted, "no broker has finished");
        Assert.False(all.IsCompleted, "no broker has finished");

        FailBroker(submission, 1, 14, "broker 1 loading");
        FailBroker(submission, 2, 14, "broker 2 loading");
        FailBroker(submission, 3, 14, "broker 3 loading");
    }

    /// <summary>
    /// A broker's failure faults <b>that broker's</b> awaitable only — the point of Java's
    /// <c>byBrokerId()</c> ("if a partial listing is sufficient").
    /// </summary>
    [Fact]
    public async Task EachBroker_CompletesIndependently_WithItsOwnError()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        Submission submission = Submit(admin);
        Discover(submission, 1, 2);

        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await TestTimeout.Run(() => submission.Result.ByBrokerId(), s_deadline);

        FailBroker(submission, 1, 15, "broker 1 has no coordinator");

        KafkaException first = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => brokers[1]), s_deadline);
        Assert.Equal(15, first.Code);
        Assert.Equal("broker 1 has no coordinator", first.Message);
        Assert.False(brokers[2].IsCompleted, "broker 2 has its own callback still to come");

        FailBroker(submission, 2, 31, "broker 2 refused");

        KafkaException second = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => brokers[2]), s_deadline);
        Assert.Equal(31, second.Code);
        Assert.Equal("broker 2 refused", second.Message);
    }

    /// <summary>
    /// <c>allByBrokerId()</c> and <c>all()</c> fail fast, as Java's do: the first broker to
    /// fail fails them without waiting for a broker still running.
    /// </summary>
    [Fact]
    public async Task AllByBrokerIdAndAll_FailFast_WhileAnotherBrokerIsPending()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        Submission submission = Submit(admin);
        Discover(submission, 1, 2);

        Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> allByBrokerId =
            submission.Result.AllByBrokerId();
        Task<IReadOnlyCollection<TransactionListing>> all = submission.Result.All();

        FailBroker(submission, 2, 31, "broker 2 refused");

        KafkaException viaMap = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => allByBrokerId), s_deadline);
        KafkaException viaAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => all), s_deadline);
        Assert.Equal(31, viaMap.Code);
        Assert.Equal("broker 2 refused", viaMap.Message);
        Assert.Same(viaMap, viaAll);

        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await submission.Result.ByBrokerId();
        Assert.False(brokers[1].IsCompleted, "the fail-fast views must not have waited for broker 1");

        FailBroker(submission, 1, 14, "broker 1 loading");
    }

    /// <summary>
    /// A discovery failure is the call's only failure, and all three views fail with it — the
    /// same exception, carrying the core's code and message.
    /// </summary>
    [Fact]
    public async Task ADiscoveryError_FailsAllThreeViews_WithTheSameError()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the discovery callback is still owed");

        submission.ByBrokerIdCallback(IntPtr.Zero, 0, MakeError(7, "metadata timed out"), submission.UserData);
        Assert.True(handle.IsClosed, "a failed discovery is the operation's last callback");

        KafkaException byBrokerId = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => submission.Result.ByBrokerId()), s_deadline);
        KafkaException allByBrokerId = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => submission.Result.AllByBrokerId()), s_deadline);
        KafkaException all = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => submission.Result.All()), s_deadline);

        Assert.Equal(7, byBrokerId.Code);
        Assert.Equal("metadata timed out", byBrokerId.Message);
        Assert.Same(byBrokerId, allByBrokerId);
        Assert.Same(byBrokerId, all);
    }

    /// <summary>
    /// No brokers: the map is empty, and — a recorded deviation from a Java quirk — the two
    /// gathering views resolve empty instead of never completing.
    /// </summary>
    [Fact]
    public async Task NoBrokers_ResolveEveryViewEmpty_AndReleaseOnTheDiscovery()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);
        TestTimeout.Run(admin.Dispose, s_deadline);

        submission.ByBrokerIdCallback(IntPtr.Zero, 0, IntPtr.Zero, submission.UserData);
        Assert.True(handle.IsClosed, "no per-broker callback follows an empty discovery");

        Assert.Empty(await TestTimeout.Run(() => submission.Result.ByBrokerId(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => submission.Result.AllByBrokerId(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => submission.Result.All(), s_deadline));
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
    [Fact]
    public void TheOperation_IsReleasedByTheLastPerBrokerCallback_AndNoEarlier()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the discovery callback is still owed");

        Discover(submission, 1, 2);
        Assert.False(handle.IsClosed, "two per-broker callbacks are still owed");

        FailBroker(submission, 2, 14, "broker 2 loading");
        Assert.False(handle.IsClosed, "one per-broker callback is still owed");

        FailBroker(submission, 1, 14, "broker 1 loading");
        Assert.True(handle.IsClosed, "the last per-broker callback must run the deferred destroy");
    }

    /// <summary>
    /// The discovery can fire <b>inline</b>, on the submitting thread, before the submit
    /// returns (the header says so for a NULL admin). The per-broker callbacks still keep the
    /// operation alive after the submit returns.
    /// </summary>
    [Fact]
    public async Task AnInlineDiscovery_StillWaitsForItsBrokers()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin, inline: static submitted => Discover(submitted, 5));

        Assert.Equal(TaskStatus.RanToCompletion, submission.Result.ByBrokerId().Status);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "broker 5's callback is still owed");

        FailBroker(submission, 5, 14, "broker 5 loading");
        Assert.True(handle.IsClosed, "broker 5's callback was the last one");

        KafkaException broker = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(async () => await (await submission.Result.ByBrokerId())[5]),
            s_deadline);
        Assert.Equal(14, broker.Code);
        Assert.Equal("broker 5 loading", broker.Message);
    }

    /// <summary>
    /// An inline discovery <b>failure</b> — the header's path when the RPC cannot be
    /// submitted — faults the result before the submit returns, and the submit token alone
    /// then releases the operation at the submit boundary.
    /// </summary>
    [Fact]
    public async Task AnInlineDiscoveryError_FaultsBeforeTheSubmitReturns_AndReleasesThere()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(
            admin,
            inline: static submitted => submitted.ByBrokerIdCallback(
                IntPtr.Zero, 0, MakeError(7, "could not submit"), submitted.UserData));

        Assert.True(submission.Result.ByBrokerId().IsFaulted, "the inline discovery ran before the submit returned");

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "nothing is owed once the submit has returned");

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => submission.Result.All()), s_deadline);
        Assert.Equal(7, failure.Code);
        Assert.Equal("could not submit", failure.Message);
    }

    /// <summary>
    /// ⚠ Plan §9 R4 — the per-broker callbacks are not serialized. Fired from two threads at
    /// once, every broker's awaitable gets exactly its own outcome and the operation is
    /// released exactly once, at the end.
    /// </summary>
    [Fact]
    public async Task PerBrokerCallbacks_FromTwoThreadsAtOnce_EachResolveTheirOwnBroker()
    {
        const int brokerCount = 64;
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);
        TestTimeout.Run(admin.Dispose, s_deadline);

        int[] ids = Enumerable.Range(1, brokerCount).ToArray();
        Discover(submission, ids);
        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await TestTimeout.Run(() => submission.Result.ByBrokerId(), s_deadline);

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
            KafkaException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => brokers[id]), s_deadline);
            Assert.Equal(14, failure.Code);
            Assert.Equal("broker " + id + " loading", failure.Message);
        }
    }

    /// <summary>
    /// A per-broker callback with neither a value nor an error breaks the header's "exactly
    /// one" contract; that broker faults with a message naming it rather than reading NULL.
    /// </summary>
    [Fact]
    public async Task APerBrokerCallbackWithNeitherValueNorError_FaultsThatBroker()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);
        TestTimeout.Run(admin.Dispose, s_deadline);
        Discover(submission, 7);

        submission.Callback(7, IntPtr.Zero, IntPtr.Zero, submission.UserData);
        Assert.True(handle.IsClosed, "the malformed callback still releases its slot");

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => submission.Result.All()), s_deadline);
        Assert.Equal(0, failure.Code);
        Assert.Equal("The listTransactions result carried neither listings nor an error for broker 7.", failure.Message);
    }

    /// <summary>
    /// A callback naming a broker the discovery never announced "cannot happen" per the
    /// header. It resolves nothing and still releases its slot; the broker that then never
    /// answered is faulted at countdown zero rather than left hanging.
    /// </summary>
    [Fact]
    public async Task AnUnannouncedBroker_ResolvesNothing_AndTheSilentBrokerFaultsAtZero()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);
        TestTimeout.Run(admin.Dispose, s_deadline);
        Discover(submission, 1);

        FailBroker(submission, 99, 31, "an unannounced broker");
        Assert.True(handle.IsClosed, "the count the discovery announced has been released");

        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await submission.Result.ByBrokerId();
        Assert.Equal(new[] { 1 }, brokers.Keys.ToArray());

        KafkaException silent = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => brokers[1]), s_deadline);
        Assert.Equal(0, silent.Code);
        Assert.Equal("The listTransactions result contained no entry for broker 1.", silent.Message);
    }

    /// <summary>
    /// A positive broker count with a NULL id array breaks the header's contract; reading it
    /// would dereference NULL on a foreign thread. The call fails instead, and the
    /// per-broker callbacks native still owes are absorbed without leaking the operation.
    /// </summary>
    [Fact]
    public async Task APositiveCountWithNoIds_FailsTheCall_AndStillBalancesTheCountdown()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        Submission submission = Submit(admin);
        TestTimeout.Run(admin.Dispose, s_deadline);

        submission.ByBrokerIdCallback(IntPtr.Zero, 2, IntPtr.Zero, submission.UserData);
        Assert.False(handle.IsClosed, "native still owes the two per-broker callbacks it announced");

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => submission.Result.ByBrokerId()), s_deadline);
        Assert.Equal(0, failure.Code);
        Assert.Equal("The listTransactions broker discovery reported 2 broker(s) but no broker ids.", failure.Message);
        Assert.Same(failure, await Assert.ThrowsAsync<KafkaException>(() => submission.Result.All()));

        FailBroker(submission, 1, 14, "broker 1 loading");
        Assert.False(handle.IsClosed, "one per-broker callback is still owed");
        FailBroker(submission, 2, 14, "broker 2 loading");
        Assert.True(handle.IsClosed, "both announced callbacks have landed");
    }

    // ---- the operation -------------------------------------------------------------------

    /// <summary>
    /// A broker's listings reach that broker's own entry and every view — the success half no
    /// ABI constructor lets the seam drive (see the class remarks).
    /// </summary>
    [Fact]
    public async Task ABrokersListings_ReachItsOwnEntry_AndEveryView()
    {
        ListTransactionsAdminOperation operation = new ListTransactionsAdminOperation();
        ListTransactionsResult result = new ListTransactionsResult(operation.ByBrokerId);

        TransactionListing first = new TransactionListing("txn-a", 11L, TransactionState.Ongoing);
        TransactionListing second = new TransactionListing("txn-b", 12L, TransactionState.CompleteCommit);
        IReadOnlyCollection<TransactionListing> brokerOne = new[] { first };
        IReadOnlyCollection<TransactionListing> brokerTwo = new[] { second };

        // A repeated id is one entry, as in Java's map.
        operation.PublishBrokers(new[] { 1, 2, 1 });
        operation.SetBrokerResult(1, brokerOne);

        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await TestTimeout.Run(() => result.ByBrokerId(), s_deadline);
        Assert.Equal(2, brokers.Count);
        Assert.Same(brokerOne, await brokers[1]);
        Assert.False(brokers[2].IsCompleted, "broker 2 has not been answered");

        // An unannounced broker is inert, not a throw.
        operation.SetBrokerResult(5, brokerTwo);
        operation.SetBrokerException(5, new KafkaException("ignored"));

        operation.SetBrokerResult(2, brokerTwo);

        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> map =
            await TestTimeout.Run(() => result.AllByBrokerId(), s_deadline);
        Assert.Equal(new[] { 1, 2 }, map.Keys.OrderBy(static id => id).ToArray());
        Assert.Same(brokerOne, map[1]);
        Assert.Same(brokerTwo, map[2]);

        IReadOnlyCollection<TransactionListing> all = await TestTimeout.Run(() => result.All(), s_deadline);
        Assert.Equal(2, all.Count);
        Assert.Contains(first, all);
        Assert.Contains(second, all);
    }

    /// <summary>
    /// "First" is first to <em>complete</em>, not the lowest broker id: a view requested while
    /// every broker is pending fails with whichever broker fails first.
    /// </summary>
    [Fact]
    public async Task TheFailFastViews_FailWithTheFirstBrokerToFail_NotTheLowestId()
    {
        ListTransactionsAdminOperation operation = new ListTransactionsAdminOperation();
        ListTransactionsResult result = new ListTransactionsResult(operation.ByBrokerId);
        operation.PublishBrokers(new[] { 1, 2, 3 });

        Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> allByBrokerId =
            result.AllByBrokerId();
        Task<IReadOnlyCollection<TransactionListing>> all = result.All();

        KafkaException third = new KafkaException("broker 3 failed first");
        operation.SetBrokerException(3, third);

        Assert.Same(third, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => allByBrokerId), s_deadline));
        Assert.Same(third, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => all), s_deadline));
    }

    /// <summary>
    /// At countdown zero, whatever nothing completed is faulted rather than left to hang: the
    /// outer stage when no discovery landed, and each announced broker that never answered.
    /// </summary>
    [Fact]
    public async Task CountdownZero_FaultsEverythingNothingCompleted()
    {
        ListTransactionsAdminOperation undiscovered = new ListTransactionsAdminOperation();
        undiscovered.SetPendingCallbacks(0);
        undiscovered.ReleaseSubmitToken();

        KafkaException noResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => undiscovered.ByBrokerId), s_deadline);
        Assert.Equal(0, noResult.Code);
        Assert.Equal("The listTransactions call completed without delivering a result.", noResult.Message);

        ListTransactionsAdminOperation discovered = new ListTransactionsAdminOperation();
        discovered.SetPendingCallbacks(0);
        discovered.PublishBrokers(new[] { 4, 6 });
        discovered.SetBrokerResult(6, Array.Empty<TransactionListing>());
        discovered.ReleaseSubmitToken();

        IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers =
            await discovered.ByBrokerId;
        Assert.Empty(await brokers[6]);
        KafkaException unanswered = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => brokers[4]), s_deadline);
        Assert.Equal(0, unanswered.Code);
        Assert.Equal("The listTransactions result contained no entry for broker 4.", unanswered.Message);
    }

    /// <summary>
    /// <see cref="AdminOperation.AddPendingCallbacks"/> extends a running countdown, and
    /// refuses — loudly — to resurrect a released one.
    /// </summary>
    [Fact]
    public void AddPendingCallbacks_ExtendsARunningCountdown_AndRefusesAReleasedOne()
    {
        ListTransactionsAdminOperation operation = new ListTransactionsAdminOperation();
        operation.SetPendingCallbacks(1);
        operation.AddPendingCallbacks(2);
        operation.AddPendingCallbacks(0);
        operation.AddPendingCallbacks(-3);

        // 1 + the submit token + 2 = 4 releases; the outer stage is the zero witness.
        for (int release = 0; release < 3; release++)
        {
            operation.ReleaseOne();
            Assert.False(operation.ByBrokerId.IsCompleted, "the countdown has not reached zero");
        }

        operation.ReleaseOne();
        Assert.True(operation.ByBrokerId.IsFaulted, "the fourth release reaches zero");

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

    private static Submission Submit(NativeAdminClient admin, Action<Submission>? inline = null)
    {
        Submission submission = new Submission();
        submission.Result = admin.ListTransactions(
            options: null,
            (handle, states, stateCount, producerIds, producerIdCount, durationMs, pattern,
             timeoutMs, byBrokerIdCallback, callback, data) =>
            {
                submission.ByBrokerIdCallback = byBrokerIdCallback;
                submission.Callback = callback;
                submission.UserData = data;
                inline?.Invoke(submission);
            });
        return submission;
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
        internal ListTransactionsResult Result { get; set; } = null!;

        internal AdminCallbacks.ListTransactionsByBrokerIdCallback ByBrokerIdCallback { get; set; } = null!;

        internal AdminCallbacks.ListTransactionsCallback Callback { get; set; } = null!;

        internal IntPtr UserData { get; set; }
    }
}
