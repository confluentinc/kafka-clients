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
public sealed class PartitionInfo
{
    private readonly string _topic;
    private readonly int _partition;
    private readonly Node? _leader;
    private readonly Node[] _replicas;
    private readonly Node[] _inSyncReplicas;
    private readonly Node[] _offlineReplicas;

    /// <summary>
    /// Creates the metadata of a partition with no offline replicas, Java's
    /// <c>PartitionInfo(String, int, Node, Node[], Node[])</c>.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition id.</param>
    /// <param name="leader">The partition's leader, or <see langword="null"/> when it has none.</param>
    /// <param name="replicas">The partition's replicas.</param>
    /// <param name="inSyncReplicas">The replicas in sync with the leader.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/>, <paramref name="replicas"/> or <paramref name="inSyncReplicas"/> is null.
    /// </exception>
    public PartitionInfo(
        string topic,
        int partition,
        Node? leader,
        Node[] replicas,
        Node[] inSyncReplicas)
        : this(topic, partition, leader, replicas, inSyncReplicas, Array.Empty<Node>())
    {
    }

    /// <summary>
    /// Creates the metadata of a partition, Java's <c>PartitionInfo(String, int, Node, Node[], Node[], Node[])</c>.
    /// The arrays are kept as given, not copied, as Java keeps them.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition id.</param>
    /// <param name="leader">The partition's leader, or <see langword="null"/> when it has none.</param>
    /// <param name="replicas">The partition's replicas.</param>
    /// <param name="inSyncReplicas">The replicas in sync with the leader.</param>
    /// <param name="offlineReplicas">The replicas that are offline.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/>, <paramref name="replicas"/>, <paramref name="inSyncReplicas"/> or
    /// <paramref name="offlineReplicas"/> is null.
    /// </exception>
    public PartitionInfo(
        string topic,
        int partition,
        Node? leader,
        Node[] replicas,
        Node[] inSyncReplicas,
        Node[] offlineReplicas)
    {
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
        _partition = partition;
        _leader = leader;
        _replicas = replicas ?? throw new ArgumentNullException(nameof(replicas));
        _inSyncReplicas = inSyncReplicas ?? throw new ArgumentNullException(nameof(inSyncReplicas));
        _offlineReplicas = offlineReplicas ?? throw new ArgumentNullException(nameof(offlineReplicas));
    }

    /// <summary>The topic name.</summary>
    /// <returns>The topic name.</returns>
    public string Topic() => _topic;

    /// <summary>The partition id.</summary>
    /// <returns>The partition id.</returns>
    public int Partition() => _partition;

    /// <summary>The partition's leader, or <see langword="null"/> when it has none.</summary>
    /// <returns>The partition's leader, or <see langword="null"/> when it has none.</returns>
    public Node? Leader() => _leader;

    /// <summary>The partition's replicas.</summary>
    /// <returns>The partition's replicas.</returns>
    public Node[] Replicas() => _replicas;

    /// <summary>The subset of <see cref="Replicas()"/> that are in sync with the leader.</summary>
    /// <returns>The subset of <see cref="Replicas()"/> that are in sync with the leader.</returns>
    public Node[] InSyncReplicas() => _inSyncReplicas;

    /// <summary>The subset of <see cref="Replicas()"/> that are offline.</summary>
    /// <returns>The subset of <see cref="Replicas()"/> that are offline.</returns>
    public Node[] OfflineReplicas() => _offlineReplicas;

    /// <summary>
    /// Value equality over the topic, the partition, the leader and the three replica lists in order, Java's
    /// <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same partition.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is PartitionInfo other
            && string.Equals(_topic, other._topic, StringComparison.Ordinal)
            && _partition == other._partition
            && Equals(_leader, other._leader)
            && _replicas.SequenceEqual(other._replicas)
            && _inSyncReplicas.SequenceEqual(other._inSyncReplicas)
            && _offlineReplicas.SequenceEqual(other._offlineReplicas));

    /// <summary>The hash of the members <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + (_topic is null ? 0 : StringComparer.Ordinal.GetHashCode(_topic));
            hash = (hash * 31) + _partition;
            hash = (hash * 31) + (_leader?.GetHashCode() ?? 0);
            hash = (hash * 31) + NodesHash(_replicas);
            hash = (hash * 31) + NodesHash(_inSyncReplicas);
            hash = (hash * 31) + NodesHash(_offlineReplicas);
            return hash;
        }
    }

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "Partition(topic = {0}, partition = {1}, leader = {2}, replicas = {3}, isr = {4}, offlineReplicas = {5})",
            _topic,
            _partition,
            _leader is null ? "none" : _leader.Id().ToString(CultureInfo.InvariantCulture),
            FormatNodeIds(_replicas),
            FormatNodeIds(_inSyncReplicas),
            FormatNodeIds(_offlineReplicas));

    private static int NodesHash(Node[] nodes)
    {
        int hash = 1;
        foreach (Node node in nodes)
        {
            hash = unchecked((31 * hash) + (node?.GetHashCode() ?? 0));
        }

        return hash;
    }

    private static string FormatNodeIds(Node[] nodes)
    {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < nodes.Length; i++)
        {
            builder.Append(nodes[i].Id().ToString(CultureInfo.InvariantCulture));
            if (i < nodes.Length - 1)
            {
                builder.Append(',');
            }
        }

        builder.Append(']');
        return builder.ToString();
    }
}
