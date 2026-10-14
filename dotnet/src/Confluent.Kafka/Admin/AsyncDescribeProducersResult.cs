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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>describeProducers</c> call — Java's
/// <c>org.apache.kafka.clients.admin.DescribeProducersResult</c> (<c>:28</c>): one future
/// per topic partition.
/// </summary>
/// <remarks>
/// ⚠ Java publishes <b>no</b> per-partition map, only the <see cref="PartitionResult"/> lookup
/// and <see cref="All"/> (<c>:36</c>, <c>:44</c>).
/// </remarks>
/// <seealso cref="DescribeProducersResult"/>
public sealed class AsyncDescribeProducersResult
{
    private readonly IReadOnlyDictionary<TopicPartition, Task<DescribeProducersResult.PartitionProducerState>> _futures;

    /// <summary>Wraps one future per topic partition.</summary>
    /// <param name="futures">One future per requested partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    internal AsyncDescribeProducersResult(
        IReadOnlyDictionary<TopicPartition, Task<DescribeProducersResult.PartitionProducerState>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncDescribeProducersResult From(
        KeyedAdminOperation<TopicPartition, DescribeProducersResult.PartitionProducerState> operation) =>
        new AsyncDescribeProducersResult(operation.Tasks);

    /// <summary>One partition's producers — Java's <c>partitionResult(TopicPartition)</c> (<c>:36</c>).</summary>
    /// <param name="partition">A topic partition from the request.</param>
    /// <returns>That partition's future.</returns>
    /// <exception cref="ArgumentException">
    /// The partition was not in the request — Java's <c>IllegalArgumentException</c>
    /// (<c>:39-40</c>), thrown <b>synchronously</b> because it is a usage error.
    /// </exception>
    public AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState> PartitionResult(TopicPartition partition) =>
        _futures.TryGetValue(partition, out Task<DescribeProducersResult.PartitionProducerState>? future)
            ? new AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>(future)
            : throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Topic partition {0} was not included in the request",
                    partition),
                nameof(partition));

    /// <summary>
    /// Every partition's producers in one map, succeeding only if every partition did —
    /// Java's <c>all()</c> (<c>:44</c>).
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure.</returns>
    /// <remarks>
    /// ⚠ <b>Recorded divergence</b> — see <see cref="AsyncDescribeTransactionsResult.All"/>, which
    /// states it once for both siblings: Java wraps this one's unreachable gather failure in a
    /// <c>KafkaException</c> (<c>:54</c>) and that one's in a plain <c>RuntimeException</c>,
    /// and neither wrap is expressible here.
    /// </remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>>(Gather(_futures));

    private static async Task<IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>> Gather(
        IReadOnlyDictionary<TopicPartition, Task<DescribeProducersResult.PartitionProducerState>> futures)
    {
        // WhenAll first, so the aggregate faults with the first failure exactly as Java's
        // allOf(...).thenApply(...) does rather than with whichever key enumerates first.
        await Task.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<TopicPartition, DescribeProducersResult.PartitionProducerState> states =
            new Dictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>(
                futures.Count, EqualityComparer<TopicPartition>.Default);
        foreach (KeyValuePair<TopicPartition, Task<DescribeProducersResult.PartitionProducerState>> entry in futures)
        {
            states.Add(entry.Key, entry.Value.Result);
        }

        return states;
    }
}
