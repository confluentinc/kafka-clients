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
/// every other partition; only <see cref="PartitionResult"/> and <see cref="All"/> turn a
/// non-<c>NONE</c> entry into a fault, exactly where Java's <c>partitionResult</c> /
/// <c>all()</c> do.
/// </para>
/// <para>
/// ⚠ <b>The ABI cannot tell you this, and reading it alone gets it wrong.</b>
/// <c>kafka_admin_AlterConsumerGroupOffsetsResult_t</c> exposes the same accessor set
/// (<c>count</c>, <c>get_topic</c>, <c>get_partition</c>, <c>get_error</c>, <c>destroy</c>)
/// as <c>kafka_admin_ElectLeadersResult_t</c> and
/// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> — accessor-set identity is
/// evidence of nothing (<c>admin-client.md</c> §1.4); the Java return type is what
/// separates them. See <c>AdminCallbacks.AlterConsumerGroupOffsetsOptionalError</c> for the
/// reader that carries this.
/// </para>
/// <para>
/// <b>Why one awaitable and not one per partition.</b> Java holds a single future over the
/// whole per-partition map, so the ABI agrees: the result's per-key error slots are
/// resolved together and reported as one aggregate, the same reasoning
/// <see cref="ElectLeadersResult"/> already documents.
/// </para>
/// </remarks>
public sealed class AlterConsumerGroupOffsetsResult
{
    private readonly Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>AlterConsumerGroupOffsetsResult(KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;)</c>
    /// (<c>:35-37</c>).
    /// </summary>
    internal AlterConsumerGroupOffsetsResult(
        Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> future)
    {
        _future = future;
    }

    /// <summary>
    /// The outcome of altering the offset for one partition — Java's
    /// <c>partitionResult(TopicPartition)</c> (<c>:39-53</c>).
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A task that completes when that partition's offset was altered successfully, and
    /// faults otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Delivered on the returned <see cref="Task"/> (not thrown synchronously — this method
    /// mirrors <see cref="Task"/>-returning-method convention, and Java's own check happens
    /// inside the <c>whenComplete</c> continuation, not before it): <paramref name="partition"/>
    /// was not part of this request. Java throws <c>IllegalArgumentException</c> on the same
    /// condition (<c>:47-48</c>).
    /// </exception>
    /// <remarks>
    /// A fresh task per call, as Java allocates a fresh <c>KafkaFutureImpl</c> per
    /// <c>partitionResult</c> call (<c>:40</c>). Admin is batch/administrative, so there is
    /// no per-record path to budget (<c>admin-client.md</c> §10).
    /// </remarks>
    public async Task PartitionResult(TopicPartition partition)
    {
        // A call-level failure propagates by the await itself — Java's `throwable != null`
        // branch (:42-43).
        IReadOnlyDictionary<TopicPartition, KafkaException?> results = await _future.ConfigureAwait(false);

        if (!results.TryGetValue(partition, out KafkaException? error))
        {
            // Java's `IllegalArgumentException` (:44-46) — text is Java's verbatim.
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Alter offset for partition \"{0}\" was not attempted",
                    partition));
        }

        // `error == null` here mirrors Java's `Errors.NONE` (:49-50); a non-null value is
        // that partition's own error and is thrown as-is (:51-52).
        if (error is not null)
        {
            throw error;
        }
    }

    /// <summary>
    /// Completes when every partition's offset was altered successfully, and faults
    /// otherwise — Java's <c>all()</c> (<c>:58-71</c>).
    /// </summary>
    /// <returns>A task representing the whole request.</returns>
    /// <remarks>
    /// <para>
    /// Unlike <see cref="ElectLeadersResult.All"/>, Java does not stop at the first
    /// failure it sees — it first collects every failed partition
    /// (<c>partitionsFailed</c>, <c>:61-64</c>) and only then throws, so the thrown
    /// exception's message names <b>every</b> failed partition, not only the first. The
    /// exception itself carries the <b>first</b> failing partition's error code and
    /// retriable flag (there is no way to merge two different codes into one), matching
    /// Java throwing that partition's own <c>error.exception(message)</c> — Java
    /// overrides the message on the thrown instance (<c>:66</c>), which is mirrored here
    /// by constructing a fresh <see cref="KafkaException"/> carrying the same code/
    /// retriable pair with the aggregate message.
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
        IReadOnlyDictionary<TopicPartition, KafkaException?> results = await _future.ConfigureAwait(false);

        KafkaException? firstFailure = null;
        List<TopicPartition>? failedPartitions = null;
        foreach (KeyValuePair<TopicPartition, KafkaException?> entry in results)
        {
            if (entry.Value is not null)
            {
                firstFailure ??= entry.Value;
                (failedPartitions ??= new List<TopicPartition>()).Add(entry.Key);
            }
        }

        if (firstFailure is null)
        {
            return;
        }

        // Java's `"Failed altering group offsets for the following partitions: " +
        // partitionsFailed` (:65), where `partitionsFailed` is a `List<TopicPartition>`
        // whose `toString()` renders as "[tp1, tp2]".
        throw new KafkaException(
            firstFailure.Code,
            string.Format(
                CultureInfo.InvariantCulture,
                "Failed altering group offsets for the following partitions: [{0}]",
                string.Join(", ", failedPartitions!)),
            firstFailure.IsRetriable);
    }
}
