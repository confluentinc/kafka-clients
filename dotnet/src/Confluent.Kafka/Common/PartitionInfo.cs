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
using System.Linq;
using System.Text;

namespace Confluent.Kafka.Common;

/// <summary>
/// Metadata about one partition of a topic, Java's <c>PartitionInfo</c>, as <c>PartitionsFor</c> and
/// <c>ListTopics</c> return it.
/// </summary>
/// <remarks>Unlike Java's <c>PartitionInfo</c>, it has no value equality.</remarks>
public sealed class PartitionInfo
{
    internal PartitionInfo(
        string topic,
        int partition,
        Node? leader,
        IReadOnlyList<Node> replicas,
        IReadOnlyList<Node> inSyncReplicas,
        IReadOnlyList<Node> offlineReplicas)
    {
        Topic = topic;
        Partition = partition;
        Leader = leader;
        Replicas = replicas;
        InSyncReplicas = inSyncReplicas;
        OfflineReplicas = offlineReplicas;
    }

    /// <summary>The topic name.</summary>
    public string Topic { get; }

    /// <summary>The partition id.</summary>
    public int Partition { get; }

    /// <summary>The partition's leader, or <see langword="null"/> when it has none.</summary>
    public Node? Leader { get; }

    /// <summary>The partition's replicas.</summary>
    public IReadOnlyList<Node> Replicas { get; }

    /// <summary>The subset of <see cref="Replicas"/> that are in sync with the leader.</summary>
    public IReadOnlyList<Node> InSyncReplicas { get; }

    /// <summary>The subset of <see cref="Replicas"/> that are offline.</summary>
    public IReadOnlyList<Node> OfflineReplicas { get; }

    /// <summary>
    /// Value equality over the topic, the partition, the leader and the three replica lists in order, Java's
    /// <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same partition.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is PartitionInfo other
            && string.Equals(Topic, other.Topic, StringComparison.Ordinal)
            && Partition == other.Partition
            && Equals(Leader, other.Leader)
            && Replicas.SequenceEqual(other.Replicas)
            && InSyncReplicas.SequenceEqual(other.InSyncReplicas)
            && OfflineReplicas.SequenceEqual(other.OfflineReplicas));

    /// <summary>The hash of the members <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + (Topic is null ? 0 : StringComparer.Ordinal.GetHashCode(Topic));
            hash = (hash * 31) + Partition;
            hash = (hash * 31) + (Leader?.GetHashCode() ?? 0);
            hash = (hash * 31) + NodesHash(Replicas);
            hash = (hash * 31) + NodesHash(InSyncReplicas);
            hash = (hash * 31) + NodesHash(OfflineReplicas);
            return hash;
        }
    }

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "Partition(topic = {0}, partition = {1}, leader = {2}, replicas = {3}, isr = {4}, offlineReplicas = {5})",
            Topic,
            Partition,
            Leader is null ? "none" : Leader.Id.ToString(CultureInfo.InvariantCulture),
            FormatNodeIds(Replicas),
            FormatNodeIds(InSyncReplicas),
            FormatNodeIds(OfflineReplicas));

    private static int NodesHash(IReadOnlyList<Node> nodes)
    {
        int hash = 1;
        foreach (Node node in nodes)
        {
            hash = unchecked((31 * hash) + (node?.GetHashCode() ?? 0));
        }

        return hash;
    }

    private static string FormatNodeIds(IReadOnlyList<Node> nodes)
    {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < nodes.Count; i++)
        {
            builder.Append(nodes[i].Id.ToString(CultureInfo.InvariantCulture));
            if (i < nodes.Count - 1)
            {
                builder.Append(',');
            }
        }

        builder.Append(']');
        return builder.ToString();
    }
}
