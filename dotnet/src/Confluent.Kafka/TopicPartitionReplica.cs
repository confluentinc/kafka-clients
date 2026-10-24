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
/// A topic name, partition number and the broker id of the replica — Java's
/// <c>org.apache.kafka.common.TopicPartitionReplica</c>.
/// </summary>
/// <remarks>
/// Value equality over all three fields, as Java's: the type keys
/// <see cref="Admin.AlterReplicaLogDirsResult"/> and
/// <see cref="Admin.DescribeReplicaLogDirsResult"/>. As in Java, only a null topic is rejected; a
/// negative partition is constructible.
/// </remarks>
public sealed class TopicPartitionReplica
{
    /// <summary>Initializes a replica reference — Java's constructor.</summary>
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

    /// <summary>The topic name — Java's <c>topic()</c>.</summary>
    public string Topic { get; }

    /// <summary>The partition number — Java's <c>partition()</c>.</summary>
    public int Partition { get; }

    /// <summary>The broker hosting the replica — Java's <c>brokerId()</c>.</summary>
    public int BrokerId { get; }

    /// <summary>Value equality over all three fields — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same replica.</returns>
    public override bool Equals(object? obj) =>
        obj is TopicPartitionReplica other
        && Partition == other.Partition
        && BrokerId == other.BrokerId
        && string.Equals(Topic, other.Topic, StringComparison.Ordinal);

    /// <summary>
    /// The hash of all three fields — Java's <c>hashCode</c>, the same <c>31 *</c> chain.
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

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering, <c>topic-partition-brokerId</c>.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", Topic, Partition, BrokerId);
}
