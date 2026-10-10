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
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The completion bridge for <c>listTransactions</c> — the one admin RPC whose result is a
/// <b>future of futures</b>: Java's <c>ListTransactionsResult</c> holds
/// <c>KafkaFuture&lt;Map&lt;Integer, KafkaFutureImpl&lt;Collection&lt;TransactionListing&gt;&gt;&gt;&gt;</c>
/// (<c>ListTransactionsResult.java:35</c>), so the map completes when the brokers are known
/// and each broker's own future completes independently of the others.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this type exists (DoD §7 — not a Java class).</b> Java builds the two stages by
/// future chaining inside <c>KafkaAdminClient</c>. Here the two stages arrive as two ABI
/// callbacks sharing one <c>user_data</c> (header,
/// <c>kafka_admin_AdminClient_list_transactions_async</c>): the broker-discovery callback,
/// fired exactly once, and one per-broker callback per discovered broker. One object has to
/// own both stages — the outer completion and the per-broker completions — and the rooting they
/// share, exactly as every other <see cref="AdminOperation"/> subclass owns its own. Neither
/// <see cref="KeyedAdminOperation{TKey, TValue}"/> (its keys are known before the submit) nor
/// <see cref="SingleAdminOperation{TValue}"/> (one completion, no inner stage) can express it.
/// </para>
/// <para>
/// <b>Two flavours (M15/P14 S5f).</b> Like every other operation it is built for one
/// <see cref="AdminFlavour"/>, and both stages are of that flavour's kind: a
/// <see cref="TaskAdminCompletion{T}"/> each on <see cref="AdminFlavour.Async"/> (read through
/// <see cref="ByBrokerId"/>), a <see cref="SyncCompletion{T}"/> latch with no owner each on
/// <see cref="AdminFlavour.Sync"/> (read through <see cref="SyncByBrokerId"/>). The other flavour's
/// view throws <see cref="InvalidOperationException"/>. <c>AdminCallbacks</c>, the countdown and
/// the entry points are the same for both.
/// </para>
/// <para>
/// <b>The fail-fast group (sync flavour only, PLAN §3.3).</b> Java's <c>allByBrokerId()</c>
/// completes on the first broker to fail <em>in time</em>, through a <c>whenComplete</c> per
/// broker (<c>ListTransactionsResult.java:92-118</c>); a latch has no completion hook, and gains
/// none. So the sync operation keeps a small group monitor of its own — the number of brokers
/// still outstanding and the first failure — which <see cref="SetBrokerResult"/>,
/// <see cref="SetBrokerException"/> and the countdown-zero sweep update under its lock, waking
/// every waiter, whenever <b>they</b> complete a broker's latch.
/// <see cref="WaitForBrokerGroup"/> is how a <see cref="FailFastGroupSyncFuture"/> waits on it.
/// The monitor is armed by <see cref="PublishBrokers"/> before the outer stage completes, so a
/// waiter that has seen the outer stage succeed sees an armed group; and it is never held across
/// anything else (no native call, no latch lock, no user code). The async flavour keeps no
/// monitor: its fail-fast is the <c>Task.WhenAny</c> loop in its Result.
/// </para>
/// <para>
/// ⚠⚠ <b>The countdown is extended mid-flight.</b> The submitter arms it for the discovery
/// callback alone (plus the submit token); the discovery callback then adds one per
/// announced broker through <see cref="AdminOperation.AddPendingCallbacks"/> <b>before</b>
/// it releases its own count, so the count cannot touch zero while a per-broker callback is
/// still due. The <see cref="System.Runtime.InteropServices.GCHandle"/> and the span-the-op
/// client reference are released by the last of all of them.
/// </para>
/// <para>
/// ⚠ <b>The callbacks are not serialized on one thread</b> (the header says so explicitly).
/// Two per-broker callbacks can run at once, so the per-broker map is <b>published once</b>
/// by the discovery callback (<see cref="Volatile"/>) and is <b>read-only</b> from then on,
/// and every completion is a <c>TrySet*</c>. The header also guarantees no per-broker
/// callback fires before the discovery callback has <em>returned</em>, so publication always
/// precedes every lookup.
/// </para>
/// <para>
/// Every async-flavour completion is built with
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> (<see cref="TaskAdminCompletion{T}"/>)
/// — the discovery callback runs synchronously on the submitting thread when the RPC cannot
/// be submitted at all, and otherwise on the core's dispatcher (or a tokio worker), and no
/// awaiter's continuation may run there. A sync-flavour latch runs no continuation at all.
/// </para>
/// </remarks>
internal sealed class ListTransactionsAdminOperation : AdminOperation
{
    private const string OperationName = "listTransactions";

    private readonly AdminFlavour _flavour;

    // The outer stage: exactly one of the two, by flavour.
    private readonly TaskAdminCompletion<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>?
        _byBrokerIdTask;

    private readonly SyncCompletion<IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>>?
        _byBrokerIdSync;

    // The sync flavour's fail-fast group monitor; null on the async flavour.
    private readonly BrokerGroup? _group;

    // Written once, by the discovery callback; read-only after that (see the type remarks).
    private Dictionary<int, IAdminCompletion<IReadOnlyCollection<TransactionListing>>>? _perBroker;

    /// <summary>
    /// Creates the outer stage of the <paramref name="flavour"/>'s kind, before anything is
    /// submitted. The per-broker completions are created by <see cref="PublishBrokers"/>, of the
    /// same kind.
    /// </summary>
    /// <param name="flavour">
    /// Which completions the two stages get: <see cref="TaskAdminCompletion{T}"/>s
    /// (<see cref="AdminFlavour.Async"/>, read through <see cref="ByBrokerId"/>) or
    /// <see cref="SyncCompletion{T}"/> latches and the fail-fast group monitor
    /// (<see cref="AdminFlavour.Sync"/>, read through <see cref="SyncByBrokerId"/>).
    /// </param>
    internal ListTransactionsAdminOperation(AdminFlavour flavour)
    {
        _flavour = flavour;
        switch (flavour)
        {
            case AdminFlavour.Async:
                _byBrokerIdTask =
                    new TaskAdminCompletion<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>();
                break;

            case AdminFlavour.Sync:
                // No owner, as for every admin latch (PLAN §3.4, D10).
                _byBrokerIdSync =
                    new SyncCompletion<IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>>(
                        owner: null);
                _group = new BrokerGroup();
                break;

            default:
                throw AdminFlavourViews.UnknownFlavour(flavour);
        }
    }

    /// <summary>
    /// The async flavour's outer stage — Java's <c>byBrokerId()</c>. Available before the
    /// submit, so the synchronous C# method can hand it back immediately.
    /// </summary>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Sync"/>.</exception>
    internal Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>> ByBrokerId =>
        _byBrokerIdTask?.Task ?? throw AdminFlavourViews.WrongView(OperationName, _flavour, nameof(ByBrokerId));

    /// <summary>
    /// The sync flavour's outer stage — Java's <c>byBrokerId()</c>, as a latch whose value maps
    /// each broker to its own latch, in discovery order. Available before the submit, like
    /// <see cref="ByBrokerId"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Async"/>.</exception>
    internal SyncFutureSource<IReadOnlyDictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>>
        SyncByBrokerId =>
        _byBrokerIdSync ?? throw AdminFlavourViews.WrongView(OperationName, _flavour, nameof(SyncByBrokerId));

    /// <summary>
    /// Whether the fail-fast group has an outcome — every broker completed, or one failed — or never forms because
    /// the discovery failed. Never blocks. Meaningful once <see cref="SyncByBrokerId"/> is done: the broker map is
    /// published before the outer stage succeeds, so a done outer stage with no map is a failed discovery.
    /// </summary>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Async"/>.</exception>
    internal bool IsBrokerGroupDone =>
        (_group ?? throw AdminFlavourViews.WrongView(OperationName, _flavour, nameof(IsBrokerGroupDone))).IsDone
        || Volatile.Read(ref _perBroker) is null;

    /// <summary>
    /// The broker discovery failed — the call's only failure. Java: <c>byBrokerId()</c>,
    /// <c>allByBrokerId()</c> and <c>all()</c> all fail with it.
    /// </summary>
    /// <param name="exception">The discovery error.</param>
    internal void FailDiscovery(Exception exception)
    {
        if (_byBrokerIdTask is not null)
        {
            _byBrokerIdTask.TrySetException(exception);
        }
        else
        {
            _byBrokerIdSync!.TrySetException(exception);
        }
    }

    /// <summary>
    /// The broker discovery succeeded: creates one completion per announced broker, publishes
    /// the map, then completes the outer stage with the per-broker futures.
    /// </summary>
    /// <param name="brokerIds">
    /// The announced broker ids, already copied out of the borrowed native array. The header
    /// promises them distinct; a repeated id would be one entry, as in Java's map.
    /// </param>
    /// <remarks>
    /// ⚠ The caller must already have added the per-broker callbacks to the countdown — see
    /// the type remarks. This method only builds and publishes. On the sync flavour it also arms
    /// the fail-fast group with the number of distinct brokers, before the map is published and
    /// before the outer stage completes.
    /// </remarks>
    internal void PublishBrokers(IReadOnlyList<int> brokerIds)
    {
        Dictionary<int, IAdminCompletion<IReadOnlyCollection<TransactionListing>>> completions =
            new Dictionary<int, IAdminCompletion<IReadOnlyCollection<TransactionListing>>>(brokerIds.Count);

        if (_byBrokerIdTask is not null)
        {
            Dictionary<int, Task<IReadOnlyCollection<TransactionListing>>> tasks =
                new Dictionary<int, Task<IReadOnlyCollection<TransactionListing>>>(brokerIds.Count);
            for (int i = 0; i < brokerIds.Count; i++)
            {
                int brokerId = brokerIds[i];
                if (completions.ContainsKey(brokerId))
                {
                    continue;
                }

                TaskAdminCompletion<IReadOnlyCollection<TransactionListing>> completion =
                    new TaskAdminCompletion<IReadOnlyCollection<TransactionListing>>();
                completions.Add(brokerId, completion);
                tasks.Add(brokerId, completion.Task);
            }

            // Publish before the outer stage completes, and before this callback returns — the
            // per-broker callbacks, possibly on other threads, look their broker up in this map.
            Volatile.Write(ref _perBroker, completions);
            _byBrokerIdTask.TrySetResult(tasks);
            return;
        }

        Dictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>> latches =
            new Dictionary<int, SyncFutureSource<IReadOnlyCollection<TransactionListing>>>(brokerIds.Count);
        for (int i = 0; i < brokerIds.Count; i++)
        {
            int brokerId = brokerIds[i];
            if (completions.ContainsKey(brokerId))
            {
                continue;
            }

            SyncCompletion<IReadOnlyCollection<TransactionListing>> completion =
                new SyncCompletion<IReadOnlyCollection<TransactionListing>>(owner: null);
            completions.Add(brokerId, completion);
            latches.Add(brokerId, completion);
        }

        // Arm the group first: no broker can complete before the map is published, and no waiter
        // reaches the group before the outer stage below has succeeded.
        _group!.Arm(completions.Count);
        Volatile.Write(ref _perBroker, completions);
        _byBrokerIdSync!.TrySetResult(latches);
    }

    /// <summary>
    /// Whether the discovery announced <paramref name="brokerId"/>. The header guarantees
    /// every per-broker callback names an announced broker; this is the defensive check.
    /// </summary>
    /// <param name="brokerId">The broker a per-broker callback named.</param>
    /// <returns><see langword="true"/> if that broker has a completion.</returns>
    internal bool IsAnnounced(int brokerId) =>
        Volatile.Read(ref _perBroker)?.ContainsKey(brokerId) == true;

    /// <summary>Resolves one broker's own future with its copied-out listings.</summary>
    /// <param name="brokerId">The broker.</param>
    /// <param name="listings">That broker's listings.</param>
    internal void SetBrokerResult(int brokerId, IReadOnlyCollection<TransactionListing> listings)
    {
        IAdminCompletion<IReadOnlyCollection<TransactionListing>>? completion = CompletionFor(brokerId);
        if (completion is not null && completion.TrySetResult(listings))
        {
            _group?.Complete(failure: null);
        }
    }

    /// <summary>Faults one broker's own future — and only that one.</summary>
    /// <param name="brokerId">The broker.</param>
    /// <param name="exception">That broker's error.</param>
    internal void SetBrokerException(int brokerId, Exception exception) =>
        FailBroker(CompletionFor(brokerId), exception);

    /// <summary>
    /// Waits, on the calling thread, until every broker has completed or one has failed, and
    /// returns that first failure in time, or <see langword="null"/> if every broker succeeded.
    /// </summary>
    /// <returns>The first broker failure in time, captured once; <see langword="null"/> if none failed.</returns>
    /// <remarks>
    /// ⚠ Only after <see cref="SyncByBrokerId"/> has <b>succeeded</b>: the group is armed by
    /// <see cref="PublishBrokers"/>, and an unarmed group would wait for ever.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The operation is <see cref="AdminFlavour.Async"/>.</exception>
    internal ExceptionDispatchInfo? WaitForBrokerGroup() =>
        (_group ?? throw AdminFlavourViews.WrongView(OperationName, _flavour, nameof(WaitForBrokerGroup))).Wait();

    /// <summary>
    /// ⚠ <b>Countdown zero, never per callback</b> — a broker whose callback has not landed
    /// yet is late, not missing. Faults whatever nothing completed, so no waiter can hang:
    /// a no-op on every path the header describes.
    /// </summary>
    protected override void OnAllCallbacksComplete()
    {
        bool outerCompleted = _byBrokerIdTask?.Task.IsCompleted ?? _byBrokerIdSync!.IsDone;
        if (!outerCompleted)
        {
            FailDiscovery(new KafkaException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "The {0} call completed without delivering a result.",
                    OperationName)));
        }

        Dictionary<int, IAdminCompletion<IReadOnlyCollection<TransactionListing>>>? completions =
            Volatile.Read(ref _perBroker);
        if (completions is null)
        {
            return;
        }

        foreach (KeyValuePair<int, IAdminCompletion<IReadOnlyCollection<TransactionListing>>> entry in completions)
        {
            if (!entry.Value.IsCompleted)
            {
                FailBroker(
                    entry.Value,
                    new KafkaException(
                        string.Format(
                            CultureInfo.InvariantCulture,
                            "The {0} result contained no entry for broker {1}.",
                            OperationName,
                            entry.Key)));
            }
        }
    }

    // Every per-broker failure goes through here, so the group counts exactly the completions
    // that this call made.
    private void FailBroker(IAdminCompletion<IReadOnlyCollection<TransactionListing>>? completion, Exception exception)
    {
        if (completion is not null && completion.TrySetException(exception))
        {
            _group?.Complete(exception);
        }
    }

    private IAdminCompletion<IReadOnlyCollection<TransactionListing>>? CompletionFor(int brokerId)
    {
        Dictionary<int, IAdminCompletion<IReadOnlyCollection<TransactionListing>>>? completions =
            Volatile.Read(ref _perBroker);
        return completions is not null
            && completions.TryGetValue(
                brokerId, out IAdminCompletion<IReadOnlyCollection<TransactionListing>>? completion)
            ? completion
            : null;
    }

    /// <summary>
    /// The sync flavour's fail-fast group monitor (PLAN §3.3): how many brokers are still
    /// outstanding, and the first to fail in time. Host scaffolding with no Java counterpart
    /// (DoD #7): it stands in for the <c>whenComplete</c> per broker that Java's
    /// <c>allByBrokerId()</c> registers, which a latch cannot offer.
    /// </summary>
    /// <remarks>
    /// Locks on <c>this</c>, as <see cref="SyncCompletion{T}"/> does: the instance is private to
    /// its operation and never handed out. The lock is taken only for a few field updates, and
    /// <see cref="Complete"/> runs after the broker's own latch has released its lock, so no
    /// thread holds both.
    /// </remarks>
    private sealed class BrokerGroup
    {
        private int _remaining;
        private ExceptionDispatchInfo? _firstFailure;
        private volatile bool _done;

        /// <summary>Whether the group has an outcome. Never blocks.</summary>
        internal bool IsDone => _done;

        /// <summary>
        /// Arms the group for <paramref name="brokers"/> distinct brokers. A group of none is done at once, with no
        /// failure.
        /// </summary>
        internal void Arm(int brokers)
        {
            lock (this)
            {
                _remaining = brokers;
                if (brokers == 0)
                {
                    _done = true;
                    Monitor.PulseAll(this);
                }
            }
        }

        /// <summary>
        /// One broker completed — <paramref name="failure"/> if it failed. Called once per
        /// broker, by whichever call completed its latch.
        /// </summary>
        internal void Complete(Exception? failure)
        {
            lock (this)
            {
                _remaining--;
                if (failure is not null && _firstFailure is null)
                {
                    _firstFailure = ExceptionDispatchInfo.Capture(failure);
                }

                if (!_done && (_remaining == 0 || _firstFailure is not null))
                {
                    // Published last: a reader that sees it set sees the outcome, which no
                    // later completion changes (the first failure is kept, never replaced).
                    _done = true;
                    Monitor.PulseAll(this);
                }
            }
        }

        /// <summary>Waits until the group has an outcome, and returns its first failure, if any.</summary>
        internal ExceptionDispatchInfo? Wait()
        {
            if (!_done)
            {
                lock (this)
                {
                    while (!_done)
                    {
                        Monitor.Wait(this);
                    }
                }
            }

            return _firstFailure;
        }
    }
}
