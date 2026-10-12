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
/// The result of <see cref="IAdmin.DeleteConsumerGroupOffsets"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult</c>
/// (<c>DeleteConsumerGroupOffsetsResult.java:31-32</c>): one completion over a map whose
/// values are the per-partition outcomes, **plus** the original request's partition set —
/// the second stored field Java carries and <see cref="AlterConsumerGroupOffsetsResult"/>
/// does not.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-partition failure is a map VALUE here, not a failed completion</b> — the
/// same shape as <see cref="AlterConsumerGroupOffsetsResult"/>. Java's stored future is
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c> (<c>:31</c>). A
/// whole-request failure fails the completion itself, and both accessors rethrow it
/// unchanged, as Java's continuations test the <c>throwable</c> first.
/// </para>
/// <para>
/// ⚠ <b>Two distinct "partition not found" errors, delivered differently.</b> Java's
/// <c>partitionResult(TopicPartition)</c> (<c>:43-57</c>) checks the partition against the
/// <b>original request set</b> <i>before</i> touching the future — a partition never named
/// in the request fails <b>synchronously</b>, out of the method call itself, with an
/// <see cref="ArgumentException"/> in Java's wording (<c>:44-46</c>). A partition that
/// <i>was</i> requested but is missing from the broker's response is a <b>second, distinct</b>
/// condition — Java's <c>KafkaAdminClient.getSubLevelError</c> (<c>:84-85</c>) — and it is
/// the <b>core</b> that reports it: that partition's map value is the core's own error,
/// thrown by the returned future's <c>Get()</c>.
/// </para>
/// <para>
/// ⚠ <b><see cref="All"/> reports only the FIRST failing partition</b> (Java's javadoc:
/// "If not, the first partition error shall be returned", <c>:61</c>) — unlike
/// <see cref="AlterConsumerGroupOffsetsResult.All"/>, which aggregates every failure into one
/// message. Which partition is first is the core's choice (topic name, then partition id —
/// Java iterates its set in unspecified order), read off the native result together with
/// the map, so <see cref="All"/> rethrows that outcome rather than re-deriving it here.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the request has completed, and needs
/// no thread-pool thread: the futures here are projections of the one latch, composed on the waiting thread, never
/// over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDeleteConsumerGroupOffsetsResult"/>
public sealed class DeleteConsumerGroupOffsetsResult
{
    private readonly KafkaFuture<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> _future;
    private readonly IReadOnlyCollection<TopicPartition> _partitions;

    /// <summary>
    /// Wraps the single completion and the original request's partition set — Java's
    /// package-private
    /// <c>DeleteConsumerGroupOffsetsResult(KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;,
    /// Set&lt;TopicPartition&gt;)</c> (<c>:35-38</c>).
    /// </summary>
    /// <param name="future">
    /// The single completion: <c>PerKey</c> is the per-partition map (a <see langword="null"/>
    /// value is Java's <c>Errors.NONE</c>), <c>All</c> the fault Java's <c>all()</c> reports,
    /// or <see langword="null"/> when it succeeds.
    /// </param>
    /// <param name="partitions">The original request's (de-duplicated) partition set.</param>
    internal DeleteConsumerGroupOffsetsResult(
        KafkaFuture<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> future,
        IReadOnlyCollection<TopicPartition> partitions)
    {
        _future = future;
        _partitions = partitions;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DeleteConsumerGroupOffsetsResult From(
        (
            SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> Operation,
            IReadOnlyCollection<TopicPartition> Partitions) submitted) =>
        new DeleteConsumerGroupOffsetsResult(
            new KafkaFuture<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
                submitted.Operation.SyncSource),
            submitted.Partitions);

    /// <summary>
    /// The outcome of deleting the committed offset for one partition — Java's
    /// <c>partitionResult(TopicPartition)</c> (<c>:43-57</c>).
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A future that completes when that partition's offset was deleted successfully, and
    /// fails otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> was not part of this request — thrown
    /// <b>synchronously</b>, out of this call, mirroring Java's own synchronous
    /// precondition check (<c>:44-46</c>) which runs before the future is even consulted.
    /// </exception>
    /// <remarks>
    /// A fresh future per call, as Java allocates a fresh <c>KafkaFutureImpl</c> per <c>partitionResult</c> call
    /// (<c>:47</c>). Admin is batch/administrative, so there is no per-record path to budget
    /// (<c>admin-client.md</c> §10). The request-set check runs in this method, before the future is built, so Java's
    /// synchronous precondition check stays synchronous; only the rest is the future's projection.
    /// </remarks>
    public KafkaFuture PartitionResult(TopicPartition partition)
    {
        if (!Contains(_partitions, partition))
        {
            // Java's `IllegalArgumentException` (:44-46) — text is Java's verbatim, no quotes.
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Partition {0} was not included in the original request",
                    partition));
        }

        return SyncAdminFutures.Project(_future, outcome =>
        {
            // A call-level failure propagates through the projection's source — Java's `throwable != null`
            // branch (:50-51).
            //
            // The indexer, not TryGetValue: `partition` passed the request-set check above, and
            // the core reports one row per requested partition, so a miss is a core contract
            // violation — failing loudly beats reporting a success nobody observed. A value is
            // that partition's own outcome, including the core's "not included in the response"
            // error for a requested partition the broker did not answer (:52).
            KafkaException? error = outcome.PerKey[partition];
            if (error is not null)
            {
                throw error;
            }

            return true;
        }).Erase();
    }

    /// <summary>
    /// Completes when every partition in the original request had its offset deleted
    /// successfully, and faults with the <b>first</b> failing partition's outcome
    /// otherwise — Java's <c>all()</c> (<c>:63-79</c>).
    /// </summary>
    /// <returns>A future representing the whole request. Its <c>Get()</c> waits for the request.</returns>
    /// <remarks>
    /// The fault is the core's <c>kafka_admin_DeleteConsumerGroupOffsetsResult_all</c>,
    /// rethrown unchanged: the outcome <see cref="PartitionResult"/> reports for the first
    /// failing requested partition in topic-name-then-partition-id order, with no aggregate
    /// message naming every failure. A whole-request failure
    /// fails the completion instead and propagates unchanged through the
    /// projection (<c>:67-68</c>). A fresh future per call, as Java allocates a
    /// fresh <c>KafkaFutureImpl</c> per <c>all()</c> call (<c>:64</c>).
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

    private static bool Contains(IReadOnlyCollection<TopicPartition> partitions, TopicPartition partition)
    {
        foreach (TopicPartition candidate in partitions)
        {
            if (candidate.Equals(partition))
            {
                return true;
            }
        }

        return false;
    }
}
