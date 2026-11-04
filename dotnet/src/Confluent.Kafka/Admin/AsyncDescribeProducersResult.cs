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
/// <seealso cref="DescribeProducersResult"/>
public sealed class AsyncDescribeProducersResult
{
    private readonly IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>> _futures;
    private readonly SafeAdminResultHandle _native;

    private AsyncDescribeProducersResult(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>> futures,
        SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>One partition's producers — Java's <c>partitionResult(TopicPartition)</c>.</summary>
    /// <param name="partition">A topic partition from the request.</param>
    /// <returns>That partition's future.</returns>
    /// <exception cref="ArgumentException">
    /// The partition was not in the request (Java's <c>IllegalArgumentException</c>), thrown at the call.
    /// </exception>
    public AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState> PartitionResult(TopicPartition partition) =>
        _futures.TryGetValue(partition, out AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState> future)
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
    /// <returns>A future yielding the whole map, or faulting with the first failure.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>> All()
    {
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>> futures = _futures;
        AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>[] values =
            new AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>[futures.Count];
        int next = 0;
        foreach (AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState> future in futures.Values)
        {
            values[next++] = future;
        }

        return AsyncKafkaFuture.AllOf(values)
            .ThenApply<IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>>(() =>
            {
                Dictionary<TopicPartition, DescribeProducersResult.PartitionProducerState> states =
                    new Dictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>(futures.Count);
                foreach (KeyValuePair<TopicPartition, AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>> entry in futures)
                {
                    states.Add(entry.Key, entry.Value.GetNow(null!));
                }

                return states;
            });
    }

    internal static AsyncDescribeProducersResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>> Partitions, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>> futures =
            new Dictionary<TopicPartition, AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>>(native.Partitions.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>> entry in native.Partitions)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<DescribeProducersResult.PartitionProducerState>(entry.Value));
        }

        return new AsyncDescribeProducersResult(futures, native.Result);
    }
}
