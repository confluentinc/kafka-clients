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
using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// A topic name, partition number and the broker id of the replica — the .NET realization
/// of Java's <c>org.apache.kafka.common.TopicPartitionReplica</c>
/// (<c>TopicPartitionReplica.java:26</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Value equality is load-bearing.</b> This type keys <b>both</b>
/// <see cref="Admin.AlterReplicaLogDirsResult"/> and
/// <see cref="Admin.DescribeReplicaLogDirsResult"/>, so a wrong
/// <see cref="Equals(object?)"/> / <see cref="GetHashCode"/> produces a result map whose
/// keys the caller cannot look up. Both mirror Java exactly — <c>:74</c> compares
/// partition, broker id and topic; <c>:52-63</c> combines the same three with the same
/// <c>31 *</c> chain.
/// </para>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common</c> — not
/// <c>clients.admin</c> — so it sits at the root <c>Confluent.Kafka</c> namespace
/// (decision D13), beside <see cref="TopicPartition"/>, <see cref="Node"/> and
/// <see cref="Uuid"/>.
/// </para>
/// <para>
/// <b>Accessors are properties</b> (decision D18): each is a pure managed field read that
/// does no P/Invoke and cannot throw, which is CLAUDE.md §3's "non-blocking getter → sync
/// property" row. Java's are methods only because Java has no properties.
/// </para>
/// <para>
/// ⚠ <b>No constructor-time validation beyond the null check, deliberately.</b> Java
/// rejects a null topic (<c>:34</c> <c>Objects.requireNonNull</c>) and nothing else — a
/// negative partition is constructible there, so it is here
/// (<c>definition-of-done.md</c> §7 forbids adding validation Java does not have). The
/// submit path guards what the ABI actually requires.
/// </para>
/// </remarks>
public sealed class TopicPartitionReplica
{
    /// <summary>
    /// Initializes a replica reference — Java's
    /// <c>TopicPartitionReplica(String topic, int partition, int brokerId)</c> (<c>:33</c>).
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="brokerId">The id of the broker hosting the replica.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    public TopicPartitionReplica(string topic, int partition, int brokerId)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Partition = partition;
        BrokerId = brokerId;
    }

    /// <summary>The topic name — Java's <c>topic()</c> (<c>:39</c>).</summary>
    public string Topic { get; }

    /// <summary>The partition number — Java's <c>partition()</c> (<c>:43</c>).</summary>
    public int Partition { get; }

    /// <summary>The broker hosting the replica — Java's <c>brokerId()</c> (<c>:47</c>).</summary>
    public int BrokerId { get; }

    /// <summary>Value equality over all three fields — Java's <c>equals</c> (<c>:66</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same replica.</returns>
    public override bool Equals(object? obj) =>
        obj is TopicPartitionReplica other
        && Partition == other.Partition
        && BrokerId == other.BrokerId
        && string.Equals(Topic, other.Topic, StringComparison.Ordinal);

    /// <summary>
    /// The hash of all three fields — Java's <c>hashCode</c> (<c>:52</c>), same fields and
    /// same <c>31 *</c> chain. (Java memoizes it in a field; that is an allocation-free
    /// micro-optimisation with no observable effect, and admin is not a hot path.)
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Topic);
            hash = (hash * 31) + Partition;
            hash = (hash * 31) + BrokerId;
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:78</c>).</summary>
    /// <returns>The rendering, <c>topic-partition-brokerId</c>.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", Topic, Partition, BrokerId);
}
