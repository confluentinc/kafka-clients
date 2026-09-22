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
/// The result of a <c>describeProducers</c> call — Java's
/// <c>org.apache.kafka.clients.admin.DescribeProducersResult</c> (<c>:28</c>): one awaitable
/// per topic partition.
/// </summary>
/// <remarks>
/// ⚠ Java publishes <b>no</b> per-partition map, only the <see cref="PartitionResult"/> lookup
/// and <see cref="All"/> (<c>:36</c>, <c>:44</c>).
/// </remarks>
public sealed class DescribeProducersResult
{
    private readonly IReadOnlyDictionary<TopicPartition, Task<PartitionProducerState>> _futures;

    /// <summary>Wraps one awaitable per topic partition.</summary>
    /// <param name="futures">One awaitable per requested partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    internal DescribeProducersResult(
        IReadOnlyDictionary<TopicPartition, Task<PartitionProducerState>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>One partition's producers — Java's <c>partitionResult(TopicPartition)</c> (<c>:36</c>).</summary>
    /// <param name="partition">A topic partition from the request.</param>
    /// <returns>That partition's awaitable.</returns>
    /// <exception cref="ArgumentException">
    /// The partition was not in the request — Java's <c>IllegalArgumentException</c>
    /// (<c>:39-40</c>), thrown <b>synchronously</b> because it is a usage error.
    /// </exception>
    public Task<PartitionProducerState> PartitionResult(TopicPartition partition) =>
        _futures.TryGetValue(partition, out Task<PartitionProducerState>? future)
            ? future
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
    /// <returns>A task yielding the whole map, or faulting with the first failure.</returns>
    /// <remarks>
    /// ⚠ <b>Recorded divergence</b> — see <see cref="DescribeTransactionsResult.All"/>, which
    /// states it once for both siblings: Java wraps this one's unreachable gather failure in a
    /// <c>KafkaException</c> (<c>:54</c>) and that one's in a plain <c>RuntimeException</c>,
    /// and neither wrap is expressible here.
    /// </remarks>
    public Task<IReadOnlyDictionary<TopicPartition, PartitionProducerState>> All() => Gather(_futures);

    private static async Task<IReadOnlyDictionary<TopicPartition, PartitionProducerState>> Gather(
        IReadOnlyDictionary<TopicPartition, Task<PartitionProducerState>> futures)
    {
        // WhenAll first, so the aggregate faults with the first failure exactly as Java's
        // allOf(...).thenApply(...) does rather than with whichever key enumerates first.
        await Task.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<TopicPartition, PartitionProducerState> states =
            new Dictionary<TopicPartition, PartitionProducerState>(
                futures.Count, EqualityComparer<TopicPartition>.Default);
        foreach (KeyValuePair<TopicPartition, Task<PartitionProducerState>> entry in futures)
        {
            states.Add(entry.Key, entry.Value.Result);
        }

        return states;
    }

    /// <summary>
    /// The active producers of one partition — Java's nested
    /// <c>DescribeProducersResult.PartitionProducerState</c> (<c>:61</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ Java declares no <c>equals</c>/<c>hashCode</c> here, so neither is added — unlike
    /// <see cref="ProducerState"/>, which declares both.
    /// </remarks>
    public sealed class PartitionProducerState
    {
        private readonly List<ProducerState> _activeProducers;

        /// <summary>Creates a partition state — Java's public constructor (<c>:64</c>).</summary>
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
        /// <c>activeProducers()</c> (<c>:68</c>).
        /// </summary>
        public IReadOnlyList<ProducerState> ActiveProducers => _activeProducers;

        /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:73</c>).</summary>
        /// <returns>The rendering.</returns>
        public override string ToString() =>
            string.Format(
                CultureInfo.InvariantCulture,
                "PartitionProducerState(activeProducers=[{0}])",
                string.Join(", ", _activeProducers));
    }
}
