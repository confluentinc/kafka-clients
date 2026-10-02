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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AlterConsumerGroupOffsets"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult</c>
/// (<c>AlterConsumerGroupOffsetsResult.java:33</c>): <b>one</b> awaitable over a map whose
/// values are the per-partition outcomes.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-partition failure is a map VALUE here, not a faulted awaitable</b> — the
/// same shape as <see cref="ElectLeadersResult"/>. Java's stored field is
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c> (<c>:33</c>): one future
/// whose resolved map carries every partition's own <c>Errors</c> code, <c>Errors.NONE</c>
/// meaning success. So a failed partition resolves the <em>same</em> successful task as
/// every other partition; <see cref="PartitionResult"/> is what turns a non-<c>NONE</c>
/// entry into a fault, exactly where Java's <c>partitionResult</c> does.
/// </para>
/// <para>
/// ⚠ <b>A whole-request failure faults the awaitable itself</b>, not the map. So both
/// accessors rethrow that error unchanged — <see cref="PartitionResult"/> even for a
/// partition that was never requested, because Java's continuation tests the
/// <c>throwable</c> before it looks at the map (<c>:46-47</c>).
/// </para>
/// <para>
/// ⚠ <b><see cref="All"/> is the core's outcome, not a derivation over the map.</b> The one
/// native result carries both the per-partition errors and Java's <c>all()</c> outcome; both
/// are read off it before the awaitable resolves, so <see cref="All"/> rethrows the core's
/// fault — including Java's aggregate message naming every failed partition — rather than
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
/// </remarks>
public sealed class AlterConsumerGroupOffsetsResult
{
    private readonly Task<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>AlterConsumerGroupOffsetsResult(KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;)</c>
    /// (<c>:35-37</c>) — resolved with the per-partition map and the core's own
    /// <c>all()</c> outcome.
    /// </summary>
    /// <param name="future">
    /// The single awaitable: <c>PerKey</c> is the per-partition map (a <see langword="null"/>
    /// value is Java's <c>Errors.NONE</c>), <c>All</c> the fault Java's <c>all()</c> reports,
    /// or <see langword="null"/> when it succeeds.
    /// </param>
    internal AlterConsumerGroupOffsetsResult(
        Task<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> future)
    {
        _future = future;
    }

    /// <summary>
    /// The outcome of altering the offset for one partition — Java's
    /// <c>partitionResult(TopicPartition)</c> (<c>:42-63</c>).
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A task that completes when that partition's offset was altered successfully, and
    /// faults otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Delivered on the returned <see cref="Task"/> (not thrown synchronously — Java's own
    /// check happens inside the <c>whenComplete</c> continuation, not before it):
    /// <paramref name="partition"/> was not part of this request. Java throws
    /// <c>IllegalArgumentException</c> on the same condition (<c>:48-50</c>). A whole-request
    /// failure is delivered instead, when there is one — Java tests it first.
    /// </exception>
    /// <remarks>
    /// A fresh task per call, as Java allocates a fresh <c>KafkaFutureImpl</c> per
    /// <c>partitionResult</c> call (<c>:43</c>). Admin is batch/administrative, so there is
    /// no per-record path to budget (<c>admin-client.md</c> §10).
    /// </remarks>
    public async Task PartitionResult(TopicPartition partition)
    {
        // A call-level failure propagates by the await itself — Java's `throwable != null`
        // branch (:46-47), which is tested before the map is looked at.
        IReadOnlyDictionary<TopicPartition, KafkaException?> perKey = (await _future.ConfigureAwait(false)).PerKey;

        if (!perKey.TryGetValue(partition, out KafkaException? error))
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
    }

    /// <summary>
    /// Completes when every partition's offset was altered successfully, and faults
    /// otherwise — Java's <c>all()</c> (<c>:67-82</c>).
    /// </summary>
    /// <returns>A task representing the whole request.</returns>
    /// <remarks>
    /// <para>
    /// The fault is the core's <c>kafka_admin_AlterConsumerGroupOffsetsResult_all</c>,
    /// rethrown unchanged: a failed partition's error carrying the message
    /// <c>Failed altering group offsets for the following partitions: [&lt;topic&gt;-&lt;partition&gt;, ...]</c>,
    /// with <b>every</b> failed partition listed, sorted by topic name then partition id —
    /// Java's <c>thenApply</c> body (<c>:68-81</c>). A whole-request failure faults the
    /// awaitable instead and propagates unchanged through the <see langword="await"/>, as it
    /// does through Java's <c>thenApply</c>.
    /// </para>
    /// <para>
    /// A fresh task per call, as Java allocates a fresh <c>KafkaFutureImpl</c> via
    /// <c>thenApply</c> per <c>all()</c> call. Admin is batch/administrative, so there is
    /// no per-record path to budget (<c>admin-client.md</c> §10).
    /// </para>
    /// </remarks>
    public async Task All()
    {
        // A call-level failure propagates by the await itself.
        KafkaException? all = (await _future.ConfigureAwait(false)).All;

        if (all is not null)
        {
            throw all;
        }
    }
}
