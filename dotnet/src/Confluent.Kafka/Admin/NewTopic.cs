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
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A topic to create — Java's <c>org.apache.kafka.clients.admin.NewTopic</c>.
/// </summary>
/// <remarks>
/// <para>
/// The partition-count form and the replica-assignment form are separate constructors, as in
/// Java; with an assignment no partition count or replication factor is sent.
/// </para>
/// <para>
/// A <see langword="null"/> <see cref="NumPartitions"/> or <see cref="ReplicationFactor"/> is
/// Java's <c>Optional.empty()</c> and <c>-1</c> is Java's <c>Optional.of(-1)</c>: both leave the
/// broker default, and <see cref="Equals(object?)"/> and <see cref="ToString"/> tell them apart,
/// as Java's do.
/// </para>
/// <para>
/// Two recorded deviations: a value below <c>-1</c> throws, where Java sends it and the broker
/// rejects the topic, because the core reads every negative as unset and would create the topic
/// with the defaults; and a <see langword="null"/> name throws, where Java fails only that
/// topic's future.
/// </para>
/// </remarks>
public sealed class NewTopic
{
    /// <summary>
    /// Initializes a topic with an explicit partition count and replication factor —
    /// Java's <c>NewTopic(String, int, short)</c>.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="numPartitions">The number of partitions, or <c>-1</c> for the broker default.</param>
    /// <param name="replicationFactor">The replication factor, or <c>-1</c> for the broker default.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> is null: a recorded deviation; see the type remarks.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="numPartitions"/> or <paramref name="replicationFactor"/> is below
    /// <c>-1</c>: a recorded deviation; see the type remarks.
    /// </exception>
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
    /// <param name="numPartitions">
    /// The number of partitions, or <see langword="null"/> (Java's <c>Optional.empty()</c>)
    /// or <c>-1</c> (Java's <c>Optional.of(-1)</c>) for the broker default.
    /// </param>
    /// <param name="replicationFactor">
    /// The replication factor, or <see langword="null"/> or <c>-1</c> for the broker
    /// default.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> is null: a recorded deviation; see the type remarks.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="numPartitions"/> or <paramref name="replicationFactor"/> is below
    /// <c>-1</c>: a recorded deviation; see the type remarks.
    /// </exception>
    public NewTopic(string name, int? numPartitions, short? replicationFactor)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));

        if (numPartitions is < -1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(numPartitions),
                numPartitions,
                "Number of partitions must be non-negative, or -1 (or null) for the broker default.");
        }

        if (replicationFactor is < -1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(replicationFactor),
                replicationFactor,
                "Replication factor must be non-negative, or -1 (or null) for the broker default.");
        }

        NumPartitions = numPartitions;
        ReplicationFactor = replicationFactor;
    }

    /// <summary>
    /// Initializes a topic from an explicit replica assignment — Java's
    /// <c>NewTopic(String, Map&lt;Integer, List&lt;Integer&gt;&gt;)</c>. The map is
    /// partition index → the broker ids holding that partition's replicas, the first of
    /// which is the preferred leader. This form sends no partition count or replication
    /// factor; they are implied by the assignment.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="replicasAssignments">Partition index → replica broker ids. It is copied.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="replicasAssignments"/> is null, or a
    /// map value is null. A null <paramref name="name"/> is a recorded deviation; see the type
    /// remarks.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">A partition index is negative.</exception>
    public NewTopic(string name, IReadOnlyDictionary<int, IReadOnlyList<int>> replicasAssignments)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        if (replicasAssignments is null)
        {
            throw new ArgumentNullException(nameof(replicasAssignments));
        }

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
    /// (Java's <c>numPartitions()</c>). <c>-1</c> when the caller passed <c>-1</c>, which
    /// also means the broker default and is kept apart from <see langword="null"/>, as in Java.
    /// Always <see langword="null"/> when <see cref="ReplicasAssignments"/> is set.
    /// </summary>
    public int? NumPartitions { get; }

    /// <summary>
    /// The replication factor, or <see langword="null"/> for the broker default
    /// (Java's <c>replicationFactor()</c>). <c>-1</c> when the caller passed <c>-1</c>,
    /// which also means the broker default. Always <see langword="null"/> when
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
    /// <remarks>
    /// <para>
    /// The dictionary is held by reference, as Java holds its map, and takes part in
    /// <see cref="Equals(object?)"/> and <see cref="GetHashCode"/>, so assigning this property or
    /// mutating the dictionary changes the hash code, as in Java.
    /// </para>
    /// <para>
    /// A <see langword="null"/> value is accepted here, but the core cannot send it, so
    /// <c>CreateTopics</c> rejects the topic with <see cref="System.ArgumentException"/> before
    /// anything is sent, as the Python client does. The annotation stays <c>string</c> so that a
    /// <c>Dictionary&lt;string, string&gt;</c> can be assigned without a warning.
    /// </para>
    /// </remarks>
    public IReadOnlyDictionary<string, string>? Configs { get; set; }

    /// <summary>
    /// Value equality over all five fields — Java's <c>equals</c>: the name (ordinal), the
    /// partition count and the replication factor (so <c>-1</c> is not equal to
    /// <see langword="null"/>, as <c>Optional.of(-1)</c> is not equal to <c>Optional.empty()</c>),
    /// the replica assignment (same partitions, and the same broker ids in the same order for
    /// each) and the configuration (the same entries, compared ordinally whatever the
    /// dictionaries' comparers). Neither map's iteration order matters.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same topic request.</returns>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is NewTopic other
            && string.Equals(Name, other.Name, StringComparison.Ordinal)
            && NumPartitions == other.NumPartitions
            && ReplicationFactor == other.ReplicationFactor
            && AssignmentsEqual(ReplicasAssignments, other.ReplicasAssignments)
            && ConfigsEqual(Configs, other.Configs);
    }

    /// <summary>
    /// The hash of all five fields — Java's <c>hashCode</c>, the <c>31 *</c> fold of
    /// <c>Objects.hash</c>, where each map contributes <c>Map.hashCode</c>'s order-independent sum
    /// of per-entry hashes.
    /// </summary>
    /// <remarks>
    /// It changes when <see cref="Configs"/> is reassigned or the dictionary behind it is
    /// mutated, as Java's does.
    /// </remarks>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (31 * hash) + StringComparer.Ordinal.GetHashCode(Name);

            hash = (31 * hash) + NumPartitions.GetHashCode();
            hash = (31 * hash) + ReplicationFactor.GetHashCode();
            hash = (31 * hash) + AssignmentsHash(ReplicasAssignments);
            hash = (31 * hash) + ConfigsHash(Configs);
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>: <c>default</c> for an unset
    /// partition count or replication factor, and each map in Java's <c>AbstractMap.toString</c>
    /// form — <c>{0=[1, 2]}</c>, <c>{k=v}</c> — or <c>null</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Concat(
            "(name=",
            Name,
            ", numPartitions=",
            NumPartitions is null ? "default" : NumPartitions.Value.ToString(CultureInfo.InvariantCulture),
            ", replicationFactor=",
            ReplicationFactor is null ? "default" : ReplicationFactor.Value.ToString(CultureInfo.InvariantCulture),
            ", replicasAssignments=",
            RenderAssignments(ReplicasAssignments),
            ", configs=",
            RenderConfigs(Configs),
            ")");

    private static bool AssignmentsEqual(
        IReadOnlyDictionary<int, IReadOnlyList<int>>? left,
        IReadOnlyDictionary<int, IReadOnlyList<int>>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        foreach (KeyValuePair<int, IReadOnlyList<int>> entry in left)
        {
            if (!right.TryGetValue(entry.Key, out IReadOnlyList<int>? otherIds)
                || entry.Value.Count != otherIds.Count)
            {
                return false;
            }

            for (int i = 0; i < entry.Value.Count; i++)
            {
                if (entry.Value[i] != otherIds[i])
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool ConfigsEqual(
        IReadOnlyDictionary<string, string>? left, IReadOnlyDictionary<string, string>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        List<KeyValuePair<string, string>> leftEntries = SortedOrdinally(left);
        List<KeyValuePair<string, string>> rightEntries = SortedOrdinally(right);
        if (leftEntries.Count != rightEntries.Count)
        {
            return false;
        }

        for (int i = 0; i < leftEntries.Count; i++)
        {
            if (!string.Equals(leftEntries[i].Key, rightEntries[i].Key, StringComparison.Ordinal)
                || !string.Equals(leftEntries[i].Value, rightEntries[i].Value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static List<KeyValuePair<string, string>> SortedOrdinally(IReadOnlyDictionary<string, string> configs)
    {
        List<KeyValuePair<string, string>> entries = new List<KeyValuePair<string, string>>(configs);
        entries.Sort(static (x, y) =>
        {
            int byKey = string.CompareOrdinal(x.Key, y.Key);
            return byKey != 0 ? byKey : string.CompareOrdinal(x.Value, y.Value);
        });
        return entries;
    }

    private static int AssignmentsHash(IReadOnlyDictionary<int, IReadOnlyList<int>>? assignments)
    {
        if (assignments is null)
        {
            return 0;
        }

        unchecked
        {
            int sum = 0;
            foreach (KeyValuePair<int, IReadOnlyList<int>> entry in assignments)
            {
                int listHash = 1;
                foreach (int brokerId in entry.Value)
                {
                    listHash = (31 * listHash) + brokerId;
                }

                sum += entry.Key ^ listHash;
            }

            return sum;
        }
    }

    private static int ConfigsHash(IReadOnlyDictionary<string, string>? configs)
    {
        if (configs is null)
        {
            return 0;
        }

        unchecked
        {
            int sum = 0;
            foreach (KeyValuePair<string, string> entry in configs)
            {
                sum += (entry.Key is null ? 0 : StringComparer.Ordinal.GetHashCode(entry.Key))
                    ^ (entry.Value is null ? 0 : StringComparer.Ordinal.GetHashCode(entry.Value));
            }

            return sum;
        }
    }

    private static string RenderAssignments(IReadOnlyDictionary<int, IReadOnlyList<int>>? assignments)
    {
        if (assignments is null)
        {
            return "null";
        }

        StringBuilder builder = new StringBuilder("{");
        bool first = true;
        foreach (KeyValuePair<int, IReadOnlyList<int>> entry in assignments)
        {
            if (!first)
            {
                builder.Append(", ");
            }

            first = false;
            builder.Append(entry.Key.ToString(CultureInfo.InvariantCulture)).Append("=[");
            for (int i = 0; i < entry.Value.Count; i++)
            {
                if (i > 0)
                {
                    builder.Append(", ");
                }

                builder.Append(entry.Value[i].ToString(CultureInfo.InvariantCulture));
            }

            builder.Append(']');
        }

        return builder.Append('}').ToString();
    }

    private static string RenderConfigs(IReadOnlyDictionary<string, string>? configs)
    {
        if (configs is null)
        {
            return "null";
        }

        StringBuilder builder = new StringBuilder("{");
        bool first = true;
        foreach (KeyValuePair<string, string> entry in configs)
        {
            if (!first)
            {
                builder.Append(", ");
            }

            first = false;

            builder.Append(entry.Key ?? "null").Append('=').Append(entry.Value ?? "null");
        }

        return builder.Append('}').ToString();
    }
}
