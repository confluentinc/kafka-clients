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

namespace Confluent.Kafka;

/// <summary>
/// Metadata about one partition of a topic — the .NET realization of Java's
/// <c>org.apache.kafka.common.PartitionInfo</c>. Returned (as a list) by
/// <c>IAsyncConsumer.PartitionsFor(string)</c> and (per topic) by
/// <c>IAsyncConsumer.ListTopics()</c>. The fields are owned copies read out of a
/// borrowed (Category-4) ABI <c>PartitionInfo_t</c> element of an owned root container,
/// which is destroyed after the copy-out (ffi-marshalling.md §B2/§B3;
/// consumer-threading.md §27).
/// </summary>
/// <remarks>
/// The binding's first public type composed of another public type
/// (<see cref="Node"/>): it holds a leader <see cref="Node"/> and three lists of
/// <see cref="Node"/>. A <c>sealed class</c> (not a <c>readonly struct</c>), matching
/// the <see cref="OffsetAndTimestamp"/> precedent — a result payload read once, never a
/// hot-path map key. Immutable. Carries <see cref="ToString"/> only (no
/// <c>IEquatable</c>): it is a query-result value, not a dictionary key (the E1
/// value-type precedent).
/// </remarks>
public sealed class PartitionInfo
{
    /// <summary>
    /// Initializes a new instance from the values copied out of the ABI element.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition id.</param>
    /// <param name="leader">
    /// The node currently acting as leader, or <see langword="null"/> if the partition
    /// has no leader (Java's <c>leader()</c> can be null).
    /// </param>
    /// <param name="replicas">The complete set of replica nodes for the partition.</param>
    /// <param name="inSyncReplicas">The subset of <paramref name="replicas"/> that are in sync.</param>
    /// <param name="offlineReplicas">The subset of <paramref name="replicas"/> that are offline.</param>
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

    /// <summary>
    /// The node currently acting as leader for the partition, or <see langword="null"/>
    /// if there is none (Java's <c>leader()</c> is nullable).
    /// </summary>
    public Node? Leader { get; }

    /// <summary>The complete set of replica nodes for the partition.</summary>
    public IReadOnlyList<Node> Replicas { get; }

    /// <summary>The subset of <see cref="Replicas"/> that are in sync with the leader.</summary>
    public IReadOnlyList<Node> InSyncReplicas { get; }

    /// <summary>The subset of <see cref="Replicas"/> that are offline.</summary>
    public IReadOnlyList<Node> OfflineReplicas { get; }

    /// <summary>
    /// Returns a string of the form
    /// <c>"Partition(topic = …, partition = …, leader = …, replicas = […], isr = […],
    /// offlineReplicas = […])"</c>, mirroring Java's <c>PartitionInfo.toString()</c>
    /// (the node collections render as their id lists, the leader as its id).
    /// </summary>
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

    /// <summary>
    /// Renders a node list as its comma-separated id list (Java's
    /// <c>PartitionInfo.formatNodeIds</c>), e.g. <c>[0,1,2]</c>.
    /// </summary>
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
