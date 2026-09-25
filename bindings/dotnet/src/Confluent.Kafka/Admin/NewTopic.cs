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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A topic to create — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.NewTopic</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The two forms are mutually exclusive, and that is enforced by the constructor
/// set rather than left to the caller.</b> Java has a partition-count/replication-factor
/// form and a replica-assignment form, and the ABI switches request shape the moment any
/// assignment is set: with assignments present, <c>num_partitions</c> and
/// <c>replication_factor</c> are <b>not sent</b> at all. Making
/// <see cref="ReplicasAssignments"/> reachable only through its own constructor means a
/// caller cannot express the contradictory "both" state and then be surprised by which
/// half the broker honoured.
/// </para>
/// <para>
/// A <see langword="null"/> <see cref="NumPartitions"/> / <see cref="ReplicationFactor"/>
/// is Java's <c>Optional.empty()</c>: the broker's <c>num.partitions</c> /
/// <c>default.replication.factor</c> applies.
/// </para>
/// </remarks>
public sealed class NewTopic
{
    /// <summary>
    /// Initializes a topic with an explicit partition count and replication factor —
    /// Java's <c>NewTopic(String, int, short)</c>.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="numPartitions">The number of partitions.</param>
    /// <param name="replicationFactor">The replication factor.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public NewTopic(string name, int numPartitions, short replicationFactor)
        : this(name, (int?)numPartitions, (short?)replicationFactor)
    {
    }

    /// <summary>
    /// Initializes a topic, leaving the partition count and/or replication factor unset
    /// so the broker's defaults apply — Java's
    /// <c>NewTopic(String, Optional&lt;Integer&gt;, Optional&lt;Short&gt;)</c>.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="numPartitions">The number of partitions, or <see langword="null"/> for the broker default.</param>
    /// <param name="replicationFactor">The replication factor, or <see langword="null"/> for the broker default.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="numPartitions"/> or <paramref name="replicationFactor"/> is
    /// negative. The ABI reads a negative value as "unset", so a negative here would be
    /// silently reinterpreted rather than honoured — pass <see langword="null"/> to mean
    /// unset (ffi §B5).
    /// </exception>
    public NewTopic(string name, int? numPartitions, short? replicationFactor)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));

        if (numPartitions is < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(numPartitions),
                numPartitions,
                "Number of partitions must not be negative; pass null to use the broker default.");
        }

        if (replicationFactor is < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(replicationFactor),
                replicationFactor,
                "Replication factor must not be negative; pass null to use the broker default.");
        }

        NumPartitions = numPartitions;
        ReplicationFactor = replicationFactor;
    }

    /// <summary>
    /// Initializes a topic from an explicit replica assignment — Java's
    /// <c>NewTopic(String, Map&lt;Integer, List&lt;Integer&gt;&gt;)</c>. The map is
    /// partition index → the broker ids holding that partition's replicas, the first of
    /// which is the preferred leader. This form does <b>not</b> send a partition count
    /// or replication factor; they are implied by the assignment.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="replicasAssignments">Partition index → replica broker ids.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="replicasAssignments"/> is null, or a
    /// map value is null.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">A partition index is negative.</exception>
    public NewTopic(string name, IReadOnlyDictionary<int, IReadOnlyList<int>> replicasAssignments)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        if (replicasAssignments is null)
        {
            throw new ArgumentNullException(nameof(replicasAssignments));
        }

        // Copy + validate BEFORE anything native (ffi §B5): the ABI silently no-ops on a
        // null broker-id array and maps a negative partition index unpredictably, so a
        // bad assignment must be rejected here rather than half-applied there.
        Dictionary<int, IReadOnlyList<int>> copy = new Dictionary<int, IReadOnlyList<int>>();
        foreach (KeyValuePair<int, IReadOnlyList<int>> entry in replicasAssignments)
        {
            if (entry.Key < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(replicasAssignments),
                    entry.Key,
                    "Partition index in a replica assignment must not be negative.");
            }

            if (entry.Value is null)
            {
                throw new ArgumentNullException(
                    nameof(replicasAssignments),
                    "A replica assignment must not have a null broker-id list.");
            }

            copy[entry.Key] = new List<int>(entry.Value);
        }

        ReplicasAssignments = copy;
    }

    /// <summary>The topic name (Java's <c>name()</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// The number of partitions, or <see langword="null"/> for the broker default
    /// (Java's <c>numPartitions()</c>). Always <see langword="null"/> when
    /// <see cref="ReplicasAssignments"/> is set.
    /// </summary>
    public int? NumPartitions { get; }

    /// <summary>
    /// The replication factor, or <see langword="null"/> for the broker default
    /// (Java's <c>replicationFactor()</c>). Always <see langword="null"/> when
    /// <see cref="ReplicasAssignments"/> is set.
    /// </summary>
    public short? ReplicationFactor { get; }

    /// <summary>
    /// The explicit replica assignment, or <see langword="null"/> when the topic was
    /// described by partition count and replication factor instead (Java's
    /// <c>replicasAssignments()</c>).
    /// </summary>
    public IReadOnlyDictionary<int, IReadOnlyList<int>>? ReplicasAssignments { get; }

    /// <summary>
    /// The topic-level configuration to create the topic with, or
    /// <see langword="null"/> for none. One C# property expresses both of Java's
    /// members: reading it is <c>configs()</c>, assigning it is <c>configs(Map)</c>.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Configs { get; set; }
}
