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

using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Confluent.Kafka.Common;

/// <summary>
/// Metadata about one partition of a topic, Java's <c>PartitionInfo</c>, as <c>PartitionsFor</c> and
/// <c>ListTopics</c> return it.
/// </summary>
/// <remarks>Unlike Java's <c>PartitionInfo</c>, it has no value equality.</remarks>
public sealed class PartitionInfo
{
    private readonly string _topic;
    private readonly int _partition;
    private readonly Node? _leader;
    private readonly IReadOnlyList<Node> _replicas;
    private readonly IReadOnlyList<Node> _inSyncReplicas;
    private readonly IReadOnlyList<Node> _offlineReplicas;

    internal PartitionInfo(
        string topic,
        int partition,
        Node? leader,
        IReadOnlyList<Node> replicas,
        IReadOnlyList<Node> inSyncReplicas,
        IReadOnlyList<Node> offlineReplicas)
    {
        _topic = topic;
        _partition = partition;
        _leader = leader;
        _replicas = replicas;
        _inSyncReplicas = inSyncReplicas;
        _offlineReplicas = offlineReplicas;
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
    public IReadOnlyList<Node> Replicas() => _replicas;

    /// <summary>The subset of <see cref="Replicas()"/> that are in sync with the leader.</summary>
    /// <returns>The subset of <see cref="Replicas()"/> that are in sync with the leader.</returns>
    public IReadOnlyList<Node> InSyncReplicas() => _inSyncReplicas;

    /// <summary>The subset of <see cref="Replicas()"/> that are offline.</summary>
    /// <returns>The subset of <see cref="Replicas()"/> that are offline.</returns>
    public IReadOnlyList<Node> OfflineReplicas() => _offlineReplicas;

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

    private static string FormatNodeIds(IReadOnlyList<Node> nodes)
    {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < nodes.Count; i++)
        {
            builder.Append(nodes[i].Id().ToString(CultureInfo.InvariantCulture));
            if (i < nodes.Count - 1)
            {
                builder.Append(',');
            }
        }

        builder.Append(']');
        return builder.ToString();
    }
}
