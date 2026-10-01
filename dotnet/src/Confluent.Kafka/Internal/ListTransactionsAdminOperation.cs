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
/// own both stages — the outer source and the per-broker sources — and the rooting they
/// share, exactly as every other <see cref="AdminOperation"/> subclass owns its own. Neither
/// <see cref="KeyedAdminOperation{TKey, TValue}"/> (its keys are known before the submit) nor
/// <see cref="SingleAdminOperation{TValue}"/> (one source, no inner stage) can express it.
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
/// Every source is built with <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>
/// — the discovery callback runs synchronously on the submitting thread when the RPC cannot
/// be submitted at all, and otherwise on the core's dispatcher (or a tokio worker), and no
/// awaiter's continuation may run there.
/// </para>
/// </remarks>
internal sealed class ListTransactionsAdminOperation : AdminOperation
{
    private const string OperationName = "listTransactions";

    private readonly TaskCompletionSource<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>
        _byBrokerId =
            new TaskCompletionSource<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>(
                TaskCreationOptions.RunContinuationsAsynchronously);

    // Written once, by the discovery callback; read-only after that (see the type remarks).
    private Dictionary<int, TaskCompletionSource<IReadOnlyCollection<TransactionListing>>>? _perBroker;

    /// <summary>
    /// The outer stage — Java's <c>byBrokerId()</c>. Available before the submit, so the
    /// synchronous C# method can hand it back immediately.
    /// </summary>
    internal Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>> ByBrokerId =>
        _byBrokerId.Task;

    /// <summary>
    /// The broker discovery failed — the call's only failure. Java: <c>byBrokerId()</c>,
    /// <c>allByBrokerId()</c> and <c>all()</c> all fail with it.
    /// </summary>
    /// <param name="exception">The discovery error.</param>
    internal void FailDiscovery(Exception exception) => _byBrokerId.TrySetException(exception);

    /// <summary>
    /// The broker discovery succeeded: creates one source per announced broker, publishes
    /// the map, then completes the outer stage with the per-broker awaitables.
    /// </summary>
    /// <param name="brokerIds">
    /// The announced broker ids, already copied out of the borrowed native array. The header
    /// promises them distinct; a repeated id would be one entry, as in Java's map.
    /// </param>
    /// <remarks>
    /// ⚠ The caller must already have added the per-broker callbacks to the countdown — see
    /// the type remarks. This method only builds and publishes.
    /// </remarks>
    internal void PublishBrokers(IReadOnlyList<int> brokerIds)
    {
        Dictionary<int, TaskCompletionSource<IReadOnlyCollection<TransactionListing>>> sources =
            new Dictionary<int, TaskCompletionSource<IReadOnlyCollection<TransactionListing>>>(brokerIds.Count);
        Dictionary<int, Task<IReadOnlyCollection<TransactionListing>>> tasks =
            new Dictionary<int, Task<IReadOnlyCollection<TransactionListing>>>(brokerIds.Count);

        for (int i = 0; i < brokerIds.Count; i++)
        {
            int brokerId = brokerIds[i];
            if (sources.ContainsKey(brokerId))
            {
                continue;
            }

            TaskCompletionSource<IReadOnlyCollection<TransactionListing>> source =
                new TaskCompletionSource<IReadOnlyCollection<TransactionListing>>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
            sources.Add(brokerId, source);
            tasks.Add(brokerId, source.Task);
        }

        // Publish before the outer stage completes, and before this callback returns — the
        // per-broker callbacks, possibly on other threads, look their broker up in this map.
        Volatile.Write(ref _perBroker, sources);
        _byBrokerId.TrySetResult(tasks);
    }

    /// <summary>
    /// Whether the discovery announced <paramref name="brokerId"/>. The header guarantees
    /// every per-broker callback names an announced broker; this is the defensive check.
    /// </summary>
    /// <param name="brokerId">The broker a per-broker callback named.</param>
    /// <returns><see langword="true"/> if that broker has a source.</returns>
    internal bool IsAnnounced(int brokerId) =>
        Volatile.Read(ref _perBroker)?.ContainsKey(brokerId) == true;

    /// <summary>Resolves one broker's own awaitable with its copied-out listings.</summary>
    /// <param name="brokerId">The broker.</param>
    /// <param name="listings">That broker's listings.</param>
    internal void SetBrokerResult(int brokerId, IReadOnlyCollection<TransactionListing> listings) =>
        SourceFor(brokerId)?.TrySetResult(listings);

    /// <summary>Faults one broker's own awaitable — and only that one.</summary>
    /// <param name="brokerId">The broker.</param>
    /// <param name="exception">That broker's error.</param>
    internal void SetBrokerException(int brokerId, Exception exception) =>
        SourceFor(brokerId)?.TrySetException(exception);

    /// <summary>
    /// ⚠ <b>Countdown zero, never per callback</b> — a broker whose callback has not landed
    /// yet is late, not missing. Faults whatever nothing completed, so no awaiter can hang:
    /// a no-op on every path the header describes.
    /// </summary>
    protected override void OnAllCallbacksComplete()
    {
        if (!_byBrokerId.Task.IsCompleted)
        {
            _byBrokerId.TrySetException(new KafkaException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "The {0} call completed without delivering a result.",
                    OperationName)));
        }

        Dictionary<int, TaskCompletionSource<IReadOnlyCollection<TransactionListing>>>? sources =
            Volatile.Read(ref _perBroker);
        if (sources is null)
        {
            return;
        }

        foreach (KeyValuePair<int, TaskCompletionSource<IReadOnlyCollection<TransactionListing>>> entry in sources)
        {
            if (!entry.Value.Task.IsCompleted)
            {
                entry.Value.TrySetException(new KafkaException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "The {0} result contained no entry for broker {1}.",
                        OperationName,
                        entry.Key)));
            }
        }
    }

    private TaskCompletionSource<IReadOnlyCollection<TransactionListing>>? SourceFor(int brokerId)
    {
        Dictionary<int, TaskCompletionSource<IReadOnlyCollection<TransactionListing>>>? sources =
            Volatile.Read(ref _perBroker);
        return sources is not null
            && sources.TryGetValue(
                brokerId, out TaskCompletionSource<IReadOnlyCollection<TransactionListing>>? source)
            ? source
            : null;
    }
}
