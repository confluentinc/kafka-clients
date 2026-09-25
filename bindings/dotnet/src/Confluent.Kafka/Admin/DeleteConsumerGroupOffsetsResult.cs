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
/// The result of <see cref="IAdmin.DeleteConsumerGroupOffsets"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult</c>
/// (<c>DeleteConsumerGroupOffsetsResult.java:33-34</c>): one awaitable over a map whose
/// values are the per-partition outcomes, **plus** the original request's partition set —
/// the second stored field Java carries and <see cref="AlterConsumerGroupOffsetsResult"/>
/// does not.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-partition failure is a map VALUE here, not a faulted awaitable</b> — the
/// same shape as <see cref="AlterConsumerGroupOffsetsResult"/>. Java's stored future is
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;</c> (<c>:33</c>).
/// </para>
/// <para>
/// ⚠ <b>Two distinct "partition not found" errors, delivered differently.</b> Java's
/// <c>partitionResult(TopicPartition)</c> (<c>:39-53</c>) checks the partition against the
/// <b>original request set</b> <i>before</i> touching the future — a partition never named
/// in the request faults <b>synchronously</b>, out of the method call itself, not via the
/// returned <see cref="Task"/>. Only once that check passes does it consult the resolved
/// map; a partition that <i>was</i> requested but is missing from the map is a
/// <b>second, distinct</b> condition, surfaced asynchronously via
/// <c>KafkaAdminClient.getSubLevelError</c> (<c>KafkaAdminClient.java:5197-5203</c>). Both
/// translate to <see cref="ArgumentException"/> with Java's own wording, but the wording and
/// the delivery mechanism both differ — see <see cref="PartitionResult"/>.
/// </para>
/// <para>
/// ⚠ <b><see cref="All"/> reports only the FIRST failing partition</b> (sorted by topic then
/// partition, Java's javadoc: "If not, the first partition error shall be returned",
/// <c>:58-71</c>) — unlike <see cref="AlterConsumerGroupOffsetsResult.All"/>, which
/// aggregates every failure into one message. The thrown exception is that partition's own
/// error (or its "missing from response" <see cref="ArgumentException"/>), not a synthesized
/// aggregate.
/// </para>
/// </remarks>
public sealed class DeleteConsumerGroupOffsetsResult
{
    private readonly Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> _future;
    private readonly IReadOnlyCollection<TopicPartition> _partitions;

    /// <summary>
    /// Wraps the single awaitable and the original request's partition set — Java's
    /// package-private
    /// <c>DeleteConsumerGroupOffsetsResult(KafkaFuture&lt;Map&lt;TopicPartition, Errors&gt;&gt;,
    /// Set&lt;TopicPartition&gt;)</c> (<c>:35-38</c>).
    /// </summary>
    internal DeleteConsumerGroupOffsetsResult(
        Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> future,
        IReadOnlyCollection<TopicPartition> partitions)
    {
        _future = future;
        _partitions = partitions;
    }

    /// <summary>
    /// The outcome of deleting the committed offset for one partition — Java's
    /// <c>partitionResult(TopicPartition)</c> (<c>:39-53</c>).
    /// </summary>
    /// <param name="partition">A partition from the request this result came from.</param>
    /// <returns>
    /// A task that completes when that partition's offset was deleted successfully, and
    /// faults otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> was not part of this request — thrown
    /// <b>synchronously</b>, out of this call, mirroring Java's own synchronous
    /// precondition check (<c>:47-48</c>) which runs before the future is even consulted.
    /// </exception>
    /// <remarks>
    /// A fresh task per call for the async continuation, as Java allocates a fresh
    /// <c>KafkaFutureImpl</c> per <c>partitionResult</c> call (<c>:40</c>). Admin is
    /// batch/administrative, so there is no per-record path to budget
    /// (<c>admin-client.md</c> §10). This method itself is deliberately <b>not</b>
    /// <see langword="async"/>: an <see langword="async"/> method defers even a
    /// pre-<see langword="await"/> throw into the returned <see cref="Task"/>, which would
    /// turn Java's synchronous precondition check into an asynchronous fault — so the
    /// synchronous check lives directly in this method, and only the rest is delegated to
    /// a private <see langword="async"/> helper.
    /// </remarks>
    public Task PartitionResult(TopicPartition partition)
    {
        if (!Contains(_partitions, partition))
        {
            // Java's `IllegalArgumentException` (:47-48) — text is Java's verbatim, no quotes.
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Partition {0} was not included in the original request",
                    partition));
        }

        return PartitionResultAsync(partition);
    }

    private async Task PartitionResultAsync(TopicPartition partition)
    {
        // A call-level failure propagates by the await itself — Java's `throwable != null`
        // branch inside `getSubLevelError`'s caller.
        IReadOnlyDictionary<TopicPartition, KafkaException?> results = await _future.ConfigureAwait(false);

        ThrowIfSubLevelError(results, partition);
    }

    /// <summary>
    /// Completes when every partition in the original request had its offset deleted
    /// successfully, and faults with the <b>first</b> failing partition's own error
    /// otherwise — Java's <c>all()</c> (<c>:58-71</c>).
    /// </summary>
    /// <returns>A task representing the whole request.</returns>
    /// <remarks>
    /// Unlike <see cref="AlterConsumerGroupOffsetsResult.All"/>, Java's javadoc here reads
    /// "If not, the first partition error shall be returned" — so this reports exactly one
    /// partition's own error (by topic-then-partition order, matching the Rust core's sort),
    /// not an aggregate message naming every failure. A fresh task per call, as Java
    /// allocates a fresh <c>KafkaFutureImpl</c> via <c>thenApply</c> per <c>all()</c> call.
    /// </remarks>
    public async Task All()
    {
        IReadOnlyDictionary<TopicPartition, KafkaException?> results = await _future.ConfigureAwait(false);

        List<TopicPartition> sorted = new List<TopicPartition>(_partitions);
        sorted.Sort((left, right) =>
        {
            int byTopic = string.CompareOrdinal(left.Topic, right.Topic);
            return byTopic != 0 ? byTopic : left.Partition.CompareTo(right.Partition);
        });

        foreach (TopicPartition partition in sorted)
        {
            ThrowIfSubLevelError(results, partition);
        }
    }

    /// <summary>
    /// Java's <c>KafkaAdminClient.getSubLevelError</c> (<c>KafkaAdminClient.java:5197-5203</c>):
    /// a partition missing from the resolved map faults with an <see cref="ArgumentException"/>
    /// distinct from <see cref="PartitionResult"/>'s synchronous "not in the original request"
    /// one; a present entry with a non-null value is that partition's own error.
    /// </summary>
    private static void ThrowIfSubLevelError(
        IReadOnlyDictionary<TopicPartition, KafkaException?> results, TopicPartition partition)
    {
        if (!results.TryGetValue(partition, out KafkaException? error))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Offset deletion result for partition \"{0}\" was not included in the response",
                    partition));
        }

        if (error is not null)
        {
            throw error;
        }
    }

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
