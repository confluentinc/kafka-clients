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

namespace Confluent.Kafka;

/// <summary>
/// Leadership and replica information for one topic partition — the .NET realization of
/// Java's <c>org.apache.kafka.common.TopicPartitionInfo</c>. Reached through
/// <see cref="Admin.TopicDescription.Partitions"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common</c>, so this lives at
/// the root <c>Confluent.Kafka</c> namespace rather than under
/// <c>Confluent.Kafka.Admin</c> — the same rule that placed <see cref="TopicCollection"/>
/// and <see cref="Uuid"/> there, and the reason the parent roadmap's file-layout sketch
/// (which listed it under <c>Admin/</c>) is not followed here.
/// </para>
/// <para>
/// ⚠ <b><see cref="Elr"/> and <see cref="LastKnownElr"/> distinguish <em>absent</em> from
/// <em>empty</em>, and the distinction is real.</b> Java's four-argument constructor
/// leaves both <c>null</c> — "the broker did not report an eligible-leader-replica set at
/// all" (a pre-KIP-966 broker, <c>TopicPartitionInfo.java:63-70</c>) — which is a
/// different fact from "reported, and it is empty". The C ABI carries the discriminant
/// explicitly (<c>has_elr</c> / <c>has_last_known_elr</c> alongside the counts) precisely
/// because a count of 0 cannot express it, so collapsing either to an empty collection
/// would destroy information the ABI went out of its way to preserve.
/// </para>
/// <para>
/// <b>Value equality is deliberately not implemented</b>, although Java's class overrides
/// <c>equals</c>/<c>hashCode</c>. That follows the shipped <see cref="Node"/> /
/// <see cref="PartitionInfo"/> precedent in this binding: these types are produced by the
/// binding and consumed by the caller, never used as dictionary keys, and adding value
/// equality later is non-breaking whereas removing it is not. (<see cref="Uuid"/> is the
/// exception, because it <em>is</em> a key.)
/// </para>
/// </remarks>
public sealed class TopicPartitionInfo
{
    private readonly List<Node> _replicas;
    private readonly List<Node> _inSyncReplicas;
    private readonly List<Node>? _elr;
    private readonly List<Node>? _lastKnownElr;

    /// <summary>
    /// Creates an instance with no eligible-leader-replica information — Java's
    /// four-argument constructor (<c>TopicPartitionInfo.java:63</c>), which leaves
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
        : this(partition, leader, replicas, inSyncReplicas, elr: null, lastKnownElr: null)
    {
    }

    /// <summary>
    /// Creates an instance with eligible-leader-replica information — Java's
    /// six-argument constructor (<c>TopicPartitionInfo.java:47</c>).
    /// </summary>
    /// <param name="partition">The partition id.</param>
    /// <param name="leader">The partition leader, or <see langword="null"/> if there is none.</param>
    /// <param name="replicas">
    /// The replicas, in replica-assignment order (the preferred replica is first).
    /// </param>
    /// <param name="inSyncReplicas">The in-sync replicas — Java's <c>isr</c>.</param>
    /// <param name="elr">
    /// The eligible leader replicas, or <see langword="null"/> if the broker reported
    /// none at all (Java's <c>elr() == null</c>). An <em>empty</em> collection means
    /// "reported, and empty" — a different fact.
    /// </param>
    /// <param name="lastKnownElr">
    /// The last known eligible leader replicas, with the same
    /// <see langword="null"/>-versus-empty meaning as <paramref name="elr"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="replicas"/> or <paramref name="inSyncReplicas"/> is null.
    /// </exception>
    public TopicPartitionInfo(
        int partition,
        Node? leader,
        IEnumerable<Node> replicas,
        IEnumerable<Node> inSyncReplicas,
        IEnumerable<Node>? elr,
        IEnumerable<Node>? lastKnownElr)
    {
        if (replicas is null)
        {
            throw new ArgumentNullException(nameof(replicas));
        }

        if (inSyncReplicas is null)
        {
            throw new ArgumentNullException(nameof(inSyncReplicas));
        }

        Partition = partition;
        Leader = leader;
        _replicas = new List<Node>(replicas);
        _inSyncReplicas = new List<Node>(inSyncReplicas);

        // null stays null — see the absent-versus-empty note in the class remarks.
        _elr = elr is null ? null : new List<Node>(elr);
        _lastKnownElr = lastKnownElr is null ? null : new List<Node>(lastKnownElr);
    }

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
    /// <remarks>
    /// Named <c>InSyncReplicas</c> rather than <c>Isr</c>, matching the shipped
    /// <see cref="PartitionInfo.InSyncReplicas"/> (Java's <c>inSyncReplicas()</c> on that
    /// class) so the two partition-metadata types spell the same concept the same way.
    /// </remarks>
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
