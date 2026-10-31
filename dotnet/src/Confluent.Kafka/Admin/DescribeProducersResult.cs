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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>describeProducers</c> call — Java's
/// <c>org.apache.kafka.clients.admin.DescribeProducersResult</c>: one future
/// per topic partition.
/// </summary>
/// <remarks>
/// Java publishes no per-partition map, only the <see cref="PartitionResult"/> lookup and
/// <see cref="All"/>.
/// </remarks>
/// <seealso cref="AsyncDescribeProducersResult"/>
public sealed class DescribeProducersResult
{
    private readonly IReadOnlyDictionary<TopicPartition, KafkaFuture<PartitionProducerState>> _futures;
    private readonly SafeAdminResultHandle _native;

    private DescribeProducersResult(
        IReadOnlyDictionary<TopicPartition, KafkaFuture<PartitionProducerState>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>One partition's producers — Java's <c>partitionResult(TopicPartition)</c>.</summary>
    /// <param name="partition">A topic partition from the request.</param>
    /// <returns>That partition's future; every call returns an equal future.</returns>
    /// <exception cref="ArgumentException">
    /// The partition was not in the request (Java's <c>IllegalArgumentException</c>), thrown at the call.
    /// </exception>
    public KafkaFuture<PartitionProducerState> PartitionResult(TopicPartition partition) =>
        _futures.TryGetValue(partition, out KafkaFuture<PartitionProducerState> future)
            ? future
            : throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Topic partition {0} was not included in the request",
                    partition),
                nameof(partition));

    /// <summary>
    /// Every partition's producers in one map, succeeding only if every partition did —
    /// Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure to complete.</returns>
    public KafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionProducerState>> All()
    {
        IReadOnlyDictionary<TopicPartition, KafkaFuture<PartitionProducerState>> futures = _futures;
        KafkaFuture<PartitionProducerState>[] values = new KafkaFuture<PartitionProducerState>[futures.Count];
        int next = 0;
        foreach (KafkaFuture<PartitionProducerState> future in futures.Values)
        {
            values[next++] = future;
        }

        return KafkaFuture.AllOf(values).ThenApply<IReadOnlyDictionary<TopicPartition, PartitionProducerState>>(() =>
        {
            Dictionary<TopicPartition, PartitionProducerState> states = new Dictionary<TopicPartition, PartitionProducerState>(futures.Count);
            foreach (KeyValuePair<TopicPartition, KafkaFuture<PartitionProducerState>> entry in futures)
            {
                states.Add(entry.Key, entry.Value.GetNow(null!));
            }

            return states;
        });
    }

    internal static DescribeProducersResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<PartitionProducerState>> Partitions, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, KafkaFuture<PartitionProducerState>> futures =
            new Dictionary<TopicPartition, KafkaFuture<PartitionProducerState>>(native.Partitions.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<PartitionProducerState>> entry in native.Partitions)
        {
            futures.Add(entry.Key, new KafkaFuture<PartitionProducerState>(entry.Value));
        }

        return new DescribeProducersResult(futures, native.Result);
    }

    /// <summary>
    /// The active producers of one partition — Java's nested
    /// <c>DescribeProducersResult.PartitionProducerState</c>.
    /// </summary>
    /// <remarks>It has no value equality, as Java declares no <c>equals</c> or <c>hashCode</c> here.</remarks>
    public sealed class PartitionProducerState
    {
        private readonly List<ProducerState> _activeProducers;

        /// <summary>Creates a partition state — Java's public constructor.</summary>
        /// <param name="activeProducers">That partition's active producers.</param>
        /// <exception cref="ArgumentNullException"><paramref name="activeProducers"/> is null.</exception>
        public PartitionProducerState(IEnumerable<ProducerState> activeProducers)
        {
            if (activeProducers is null)
            {
                throw new ArgumentNullException(nameof(activeProducers));
            }

            _activeProducers = new List<ProducerState>(activeProducers);
        }

        /// <summary>
        /// The active producers, in the order the broker reported them — Java's
        /// <c>activeProducers()</c>.
        /// </summary>
        public IReadOnlyList<ProducerState> ActiveProducers => _activeProducers;

        /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
        /// <returns>The rendering.</returns>
        public override string ToString() =>
            string.Format(
                CultureInfo.InvariantCulture,
                "PartitionProducerState(activeProducers=[{0}])",
                string.Join(", ", _activeProducers));
    }
}
