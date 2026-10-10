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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AlterConsumerGroupOffsets"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult</c>
/// (<c>AlterConsumerGroupOffsetsResult.java:33</c>): <b>one</b> completion over a map whose
/// values are the per-partition outcomes.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-partition failure is a map VALUE here, not a failed completion</b> — the
/// same shape as <see cref="ElectLeadersResult"/>. Java's stored field is
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c> (<c>:33</c>): one future
/// whose resolved map carries every partition's own <c>Errors</c> code, <c>Errors.NONE</c>
/// meaning success. So a failed partition resolves the <em>same</em> successful completion as
/// every other partition; <see cref="PartitionResult"/> is what turns a non-<c>NONE</c>
/// entry into a failure, exactly where Java's <c>partitionResult</c> does.
/// </para>
/// <para>
/// ⚠ <b>A whole-request failure fails the completion itself</b>, not the map. So both
/// accessors rethrow that error unchanged — <see cref="PartitionResult"/> even for a
/// partition that was never requested, because Java's continuation tests the
/// <c>throwable</c> before it looks at the map (<c>:46-47</c>).
/// </para>
/// <para>
/// ⚠ <b><see cref="All"/> is the core's outcome, not a derivation over the map.</b> The one
/// native result carries both the per-partition errors and Java's <c>all()</c> outcome; both
/// are read off it before the completion resolves, so <see cref="All"/> rethrows the core's
/// failure — including Java's aggregate message naming every failed partition — rather than
/// rebuilding it here.
/// </para>
/// <para>
/// ⚠ <b>The ABI accessor set cannot tell you the map-value shape.</b>
/// <c>kafka_admin_AlterConsumerGroupOffsetsResult_t</c> exposes the same indexed accessors
/// (<c>count</c>, <c>get_topic</c>, <c>get_partition</c>, <c>get_error</c>, <c>destroy</c>)
/// as <c>kafka_admin_ElectLeadersResult_t</c> and
/// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> — accessor-set identity is
/// evidence of nothing (<c>admin-client.md</c> §1.4); the Java return type is what
/// separates them. See <c>AdminCallbacks.AlterConsumerGroupOffsetsOptionalError</c> for the
/// reader that carries this.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the request has completed, and needs
/// no thread-pool thread: the futures here are projections of the one latch, composed on the waiting thread, never
/// over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAlterConsumerGroupOffsetsResult"/>
public sealed class AlterConsumerGroupOffsetsResult
{
    private readonly KafkaFuture<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> _future;

    /// <summary>
    /// Wraps the single completion — Java's package-private
    /// <c>AlterConsumerGroupOffsetsResult(KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;)</c>
    /// (<c>:35-37</c>) — resolved with the per-partition map and the core's own
    /// <c>all()</c> outcome.
    /// </summary>
    /// <param name="future">
    /// The single completion: <c>PerKey</c> is the per-partition map (a <see langword="null"/>
    /// value is Java's <c>Errors.NONE</c>), <c>All</c> the fault Java's <c>all()</c> reports,
    /// or <see langword="null"/> when it succeeds.
    /// </param>
    internal AlterConsumerGroupOffsetsResult(
        KafkaFuture<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> future)
    {
        _future = future;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AlterConsumerGroupOffsetsResult From(
        SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation) =>
        new AlterConsumerGroupOffsetsResult(
            new KafkaFuture<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
                operation.SyncSource));

    /// <summary>
    /// The outcome of altering the offset for one partition — Java's
    /// <c>partitionResult(TopicPartition)</c> (<c>:42-63</c>).
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A future that completes when that partition's offset was altered successfully, and
    /// fails otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown by the returned future's <c>Get()</c> (not out of this call — Java's own
    /// check happens inside the <c>whenComplete</c> continuation, not before it):
    /// <paramref name="partition"/> was not part of this request. Java throws
    /// <c>IllegalArgumentException</c> on the same condition (<c>:48-50</c>). A whole-request
    /// failure is delivered instead, when there is one — Java tests it first.
    /// </exception>
    /// <remarks>
    /// A fresh future per call, as Java allocates a fresh <c>KafkaFutureImpl</c> per
    /// <c>partitionResult</c> call (<c>:43</c>). Within one such future the check runs once, on the first
    /// <c>Get()</c>'s thread, and every later <c>Get()</c> rethrows the same exception instance. Admin is
    /// batch/administrative, so there is no per-record path to budget (<c>admin-client.md</c> §10).
    /// </remarks>
    public KafkaFuture PartitionResult(TopicPartition partition) =>
        SyncAdminFutures.Project(_future, outcome =>
        {
            // A call-level failure propagates through the projection's source — Java's `throwable != null`
            // branch (:46-47), which is tested before the map is looked at.
            if (!outcome.PerKey.TryGetValue(partition, out KafkaException? error))
            {
                // Java's `IllegalArgumentException` (:48-50) — text is Java's verbatim.
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Alter offset for partition \"{0}\" was not attempted",
                        partition));
            }

            // `error == null` here mirrors Java's `Errors.NONE` (:53-54); a non-null value is
            // that partition's own error and is thrown as-is (:56).
            if (error is not null)
            {
                throw error;
            }

            return true;
        }).Erase();

    /// <summary>
    /// Completes when every partition's offset was altered successfully, and faults
    /// otherwise — Java's <c>all()</c> (<c>:67-82</c>).
    /// </summary>
    /// <returns>A future representing the whole request. Its <c>Get()</c> waits for the request.</returns>
    /// <remarks>
    /// <para>
    /// The fault is the core's <c>kafka_admin_AlterConsumerGroupOffsetsResult_all</c>,
    /// rethrown unchanged: a failed partition's error carrying the message
    /// <c>Failed altering group offsets for the following partitions: [&lt;topic&gt;-&lt;partition&gt;, ...]</c>,
    /// with <b>every</b> failed partition listed, sorted by topic name then partition id —
    /// Java's <c>thenApply</c> body (<c>:68-81</c>). A whole-request failure fails the
    /// completion instead and propagates unchanged through the projection, as it
    /// does through Java's <c>thenApply</c>.
    /// </para>
    /// <para>
    /// A fresh future per call, as Java allocates a fresh <c>KafkaFutureImpl</c> via
    /// <c>thenApply</c> per <c>all()</c> call. Admin is batch/administrative, so there is
    /// no per-record path to budget (<c>admin-client.md</c> §10).
    /// </para>
    /// </remarks>
    public KafkaFuture All() =>
        SyncAdminFutures.Project(_future, static outcome =>
        {
            // A call-level failure propagates through the projection's source.
            if (outcome.All is not null)
            {
                throw outcome.All;
            }

            return true;
        }).Erase();
}
