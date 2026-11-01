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
