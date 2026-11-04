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

namespace Confluent.Kafka.Common;

/// <summary>
/// Leadership and replica information for one topic partition — Java's
/// <c>org.apache.kafka.common.TopicPartitionInfo</c>, reached through
/// <see cref="Admin.TopicDescription.Partitions"/>.
/// </summary>
/// <remarks>
/// <see cref="Elr"/> and <see cref="LastKnownElr"/> are <see langword="null"/> when the broker
/// reported no eligible-leader-replica set, which differs from an empty set. A partition reported
/// with only one of the two sets reads both as absent, as Python's binding does. Value equality
/// is deliberately not implemented, although Java's class overrides <c>equals</c>, as for
/// <see cref="Node"/> and <see cref="PartitionInfo"/>.
/// </remarks>
public sealed class TopicPartitionInfo
{
    private readonly List<Node> _replicas;
    private readonly List<Node> _inSyncReplicas;
    private readonly List<Node>? _elr;
    private readonly List<Node>? _lastKnownElr;

    /// <summary>
    /// Creates an instance with no eligible-leader-replica information — Java's
    /// four-argument constructor, which leaves
    /// <see cref="Elr"/> and <see cref="LastKnownElr"/> <see langword="null"/>.
    /// </summary>
    /// <param name="partition">The partition id.</param>
    /// <param name="leader">The partition leader, or <see langword="null"/> if there is none.</param>
    /// <param name="replicas">
    /// The replicas, in replica-assignment order (the preferred replica is first).
    /// </param>
    /// <param name="inSyncReplicas">The in-sync replicas — Java's <c>isr</c>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="replicas"/> or <paramref name="inSyncReplicas"/> is null.
    /// </exception>
    public TopicPartitionInfo(
        int partition,
        Node? leader,
        IEnumerable<Node> replicas,
        IEnumerable<Node> inSyncReplicas)
        : this(Copy(replicas, nameof(replicas)), Copy(inSyncReplicas, nameof(inSyncReplicas)), null, null, partition, leader)
    {
    }

    /// <summary>
    /// Creates an instance with eligible-leader-replica information — Java's six-argument
    /// constructor.
    /// </summary>
    /// <param name="partition">The partition id.</param>
    /// <param name="leader">The partition leader, or <see langword="null"/> if there is none.</param>
    /// <param name="replicas">
    /// The replicas, in replica-assignment order (the preferred replica is first).
    /// </param>
    /// <param name="inSyncReplicas">The in-sync replicas — Java's <c>isr</c>.</param>
    /// <param name="elr">
    /// The eligible leader replicas; an <em>empty</em> collection means "reported, and empty". Use the
    /// four-argument constructor for a partition whose broker reported no such set.
    /// </param>
    /// <param name="lastKnownElr">The last known eligible leader replicas, as <paramref name="elr"/>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="replicas"/>, <paramref name="inSyncReplicas"/>, <paramref name="elr"/> or
    /// <paramref name="lastKnownElr"/> is null.
    /// </exception>
    public TopicPartitionInfo(
        int partition,
        Node? leader,
        IEnumerable<Node> replicas,
        IEnumerable<Node> inSyncReplicas,
        IEnumerable<Node> elr,
        IEnumerable<Node> lastKnownElr)
        : this(
            Copy(replicas, nameof(replicas)),
            Copy(inSyncReplicas, nameof(inSyncReplicas)),
            Copy(elr, nameof(elr)),
            Copy(lastKnownElr, nameof(lastKnownElr)),
            partition,
            leader)
    {
    }

    private TopicPartitionInfo(
        List<Node> replicas,
        List<Node> inSyncReplicas,
        List<Node>? elr,
        List<Node>? lastKnownElr,
        int partition,
        Node? leader)
    {
        Partition = partition;
        Leader = leader;
        _replicas = replicas;
        _inSyncReplicas = inSyncReplicas;
        _elr = elr;
        _lastKnownElr = lastKnownElr;
    }

    private static List<Node> Copy(IEnumerable<Node> nodes, string parameterName) =>
        nodes is null ? throw new ArgumentNullException(parameterName) : new List<Node>(nodes);

    /// <summary>The partition id — Java's <c>partition()</c>.</summary>
    public int Partition { get; }

    /// <summary>
    /// The partition leader, or <see langword="null"/> if there is none — Java's
    /// <c>leader()</c>.
    /// </summary>
    public Node? Leader { get; }

    /// <summary>
    /// The replicas, in replica-assignment order (the preferred replica is first) —
    /// Java's <c>replicas()</c>.
    /// </summary>
    public IReadOnlyList<Node> Replicas => _replicas;

    /// <summary>
    /// The in-sync replicas; the order is unspecified — Java's <c>isr()</c>.
    /// </summary>
    /// <remarks>Named as <see cref="PartitionInfo.InSyncReplicas"/>, rather than <c>Isr</c>.</remarks>
    public IReadOnlyList<Node> InSyncReplicas => _inSyncReplicas;

    /// <summary>
    /// The eligible leader replicas, or <see langword="null"/> if the broker reported no
    /// such set — Java's <c>elr()</c>. The order is unspecified.
    /// </summary>
    public IReadOnlyList<Node>? Elr => _elr;

    /// <summary>
    /// The last known eligible leader replicas, or <see langword="null"/> if the broker
    /// reported no such set — Java's <c>lastKnownElr()</c>. The order is unspecified.
    /// </summary>
    public IReadOnlyList<Node>? LastKnownElr => _lastKnownElr;

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>, including its
    /// <c>N/A</c> spelling for an absent ELR set.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(partition={0}, leader={1}, replicas={2}, isr={3}, elr={4}, lastKnownElr={5})",
            Partition,
            Leader,
            Join(_replicas),
            Join(_inSyncReplicas),
            _elr is null ? "N/A" : Join(_elr),
            _lastKnownElr is null ? "N/A" : Join(_lastKnownElr));

    private static string Join(IEnumerable<Node> nodes) =>
        string.Join(", ", nodes.Select(static node => node.ToString()));
}
