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
/// <c>default.replication.factor</c> applies. <c>-1</c> is Java's
/// <c>Optional.of(-1)</c> — <c>CreateTopicsRequest.NO_NUM_PARTITIONS</c> /
/// <c>NO_REPLICATION_FACTOR</c> (<c>CreateTopicsRequest.java:82-83</c>) — which sends the
/// same request, so the broker default applies to it as well. The two are nonetheless
/// <b>kept apart</b>: <c>-1</c> is stored as <c>-1</c>, and <see cref="Equals(object?)"/>
/// and <see cref="ToString"/> distinguish it from <see langword="null"/> exactly as Java's
/// <c>equals</c> / <c>toString</c> distinguish <c>Optional.of(-1)</c> from
/// <c>Optional.empty()</c> (<c>NewTopic.java:149-174</c>).
/// </para>
/// <para>
/// <b>Recorded divergence (<c>definition-of-done.md</c> §7): a value below <c>-1</c> is
/// rejected, where Java accepts it.</b> Java sends such a value and the broker rejects the
/// topic. The ABI cannot carry it: <c>kafka_admin_NewTopic_new</c> reads <em>every</em>
/// negative as "unset", so forwarding <c>-5</c> would silently create the topic with the
/// broker's defaults instead of failing it (ffi §B5). <c>-1</c> is the one negative whose
/// meaning survives that reading, because it already <em>is</em> "unset".
/// </para>
/// <para>
/// ⚠ <b>Stricter than Java — a <see langword="null"/> name is rejected by every
/// constructor</b> with <see cref="ArgumentNullException"/>. Java's <c>NewTopic</c> stores
/// the name without checking (<c>NewTopic.java:57</c>, <c>:72</c>), and
/// <c>createTopics</c> then fails only that topic's future with
/// <c>InvalidTopicException</c> (<c>topicNameIsUnrepresentable</c>,
/// <c>KafkaAdminClient.java:1739</c>, used at <c>:1787</c>). Whether the binding should
/// reproduce that per-key failure instead of rejecting up front is open: it is one
/// instance of the binding-wide up-front-rejection pattern (audit X10).
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
    /// <paramref name="name"/> is null — stricter than Java; see the type remarks.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="numPartitions"/> or <paramref name="replicationFactor"/> is below
    /// <c>-1</c> — see the recorded divergence in the type remarks.
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
    /// <paramref name="name"/> is null — stricter than Java; see the type remarks.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="numPartitions"/> or <paramref name="replicationFactor"/> is below
    /// <c>-1</c>. The ABI reads every negative value as "unset", so such a value would be
    /// silently reinterpreted rather than sent — see the recorded divergence in the type
    /// remarks (ffi §B5).
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

        // -1 is stored as -1, NOT folded to null: Java keeps Optional.of(-1) and
        // Optional.empty() apart in equals/toString, and so does this type. Both reach
        // kafka_admin_NewTopic_new as -1 (NewTopicMarshal), which is Java's wire value.
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
    /// map value is null. A null <paramref name="name"/> is rejected stricter than Java;
    /// see the type remarks.
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
    /// (Java's <c>numPartitions()</c>). <c>-1</c> when the caller passed <c>-1</c>, which
    /// also means the broker default — see the type remarks for why the two are kept
    /// apart. Always <see langword="null"/> when <see cref="ReplicasAssignments"/> is set.
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
    /// The dictionary is held by reference, as Java holds its map, and it takes part in
    /// <see cref="Equals(object?)"/> and <see cref="GetHashCode"/> — so assigning this
    /// property, or mutating the dictionary behind it, <b>changes the hash code</b>, exactly
    /// as it does in Java. Do not do either while the topic is a key in a hash-based
    /// collection.
    /// <para>
    /// A <see langword="null"/> <b>value</b> is accepted and sent as Java's null config value,
    /// as <c>configs(Map)</c> sends it — distinct from the empty string. The annotation stays
    /// <c>string</c> rather than <c>string?</c> so that an ordinary
    /// <c>Dictionary&lt;string, string&gt;</c> can be assigned without a nullability warning;
    /// equality, the hash code and <see cref="ToString"/> all handle a null value (M15/P13.3
    /// D16).
    /// </para>
    /// </remarks>
    public IReadOnlyDictionary<string, string>? Configs { get; set; }

    /// <summary>
    /// Value equality over all five fields — Java's <c>equals</c>
    /// (<c>NewTopic.java:159-169</c>): the name (ordinal), the partition count and the
    /// replication factor (so <c>-1</c> is not equal to <see langword="null"/>, as
    /// <c>Optional.of(-1)</c> is not equal to <c>Optional.empty()</c>), the replica
    /// assignment (same partitions, and the same broker ids in the same order for each) and
    /// the configuration (the same entries, compared ordinally). Neither map's iteration
    /// order matters.
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
    /// The hash of all five fields — Java's <c>hashCode</c> (<c>NewTopic.java:171-174</c>),
    /// the <c>31 *</c> fold of <c>Objects.hash</c>, where each map contributes
    /// <c>Map.hashCode</c>'s order-independent sum of per-entry hashes.
    /// </summary>
    /// <remarks>
    /// Mutable, as Java's is: it changes when <see cref="Configs"/> is reassigned or the
    /// dictionary behind it is mutated — see <see cref="Configs"/>.
    /// </remarks>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (31 * hash) + StringComparer.Ordinal.GetHashCode(Name);

            // Nullable<T>.GetHashCode() is 0 when empty — Java's Optional.hashCode().
            hash = (31 * hash) + NumPartitions.GetHashCode();
            hash = (31 * hash) + ReplicationFactor.GetHashCode();
            hash = (31 * hash) + AssignmentsHash(ReplicasAssignments);
            hash = (31 * hash) + ConfigsHash(Configs);
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c> (<c>NewTopic.java:149-157</c>):
    /// <c>default</c> for an unset partition count or replication factor, and each map in
    /// Java's <c>AbstractMap.toString</c> form — <c>{0=[1, 2]}</c>, <c>{k=v}</c> — or
    /// <c>null</c>.
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

    /// <summary>
    /// Java's <c>Map.equals</c> over the replica assignment. Both sides are always this
    /// type's own copies (default <see cref="int"/> keys, non-null <see cref="List{T}"/>
    /// values), so a keyed lookup is safe here in a way it is not for
    /// <see cref="Configs"/>.
    /// </summary>
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

    /// <summary>
    /// Java's <c>Map.equals</c> over the configuration, made <b>independent of either
    /// dictionary's own key comparer</b>.
    /// </summary>
    /// <remarks>
    /// ⚠ <see cref="Configs"/> is the caller's dictionary, and a .NET dictionary may carry
    /// any comparer — <see cref="StringComparer.OrdinalIgnoreCase"/> is common. A keyed
    /// lookup through <c>other.TryGetValue</c> would then find <c>"a"</c> under
    /// <c>"A"</c> in one direction and not the other, and would call two topics equal whose
    /// ordinal hashes differ. Java cannot meet this (a <c>HashMap&lt;String, String&gt;</c>
    /// always compares keys ordinally), so the faithful reading is ordinal on both keys and
    /// values: compare the two entry sets sorted ordinally. That is symmetric, and consistent
    /// with <see cref="ConfigsHash"/> for every dictionary implementation.
    /// </remarks>
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

    /// <summary>
    /// Java's <c>Map.hashCode</c> over the replica assignment: the sum of
    /// <c>key.hashCode() ^ value.hashCode()</c>, where a list's hash is <c>List.hashCode</c>'s
    /// ordered <c>31 *</c> fold. The sum makes it independent of iteration order.
    /// </summary>
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

    /// <summary>
    /// Java's <c>Map.hashCode</c> over the configuration: the order-independent sum of
    /// ordinal key and value hashes, matching <see cref="ConfigsEqual"/>.
    /// </summary>
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

    /// <summary>Java's <c>AbstractMap.toString</c> of the assignment: <c>{0=[1, 2], 1=[3]}</c>.</summary>
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

    /// <summary>Java's <c>AbstractMap.toString</c> of the configuration: <c>{k=v, k2=v2}</c>.</summary>
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

            // Java's StringBuilder.append(Object) prints a null key or value as "null".
            builder.Append(entry.Key ?? "null").Append('=').Append(entry.Value ?? "null");
        }

        return builder.Append('}').ToString();
    }
}
