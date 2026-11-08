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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A description of one topic, the per-key value of <see cref="DescribeTopicsResult"/> — Java's
/// <c>org.apache.kafka.clients.admin.TopicDescription</c>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="AuthorizedOperations"/> is <see langword="null"/> when the operations are not
/// known (the broker was not asked, or did not answer), and empty when the broker answered that
/// none is authorized, as Java's accessor.
/// </para>
/// </remarks>
public sealed class TopicDescription
{
    private readonly List<TopicPartitionInfo> _partitions;
    private readonly List<AclOperation>? _authorizedOperations;

    /// <summary>
    /// Creates a description with an empty authorized-operations set and
    /// <see cref="Uuid.ZERO_UUID"/> as the topic id — Java's three-argument constructor.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="internal">Whether the topic is internal to Kafka.</param>
    /// <param name="partitions">
    /// One entry per partition; the index is the partition id.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="partitions"/> is null.
    /// </exception>
    public TopicDescription(string name, bool @internal, IEnumerable<TopicPartitionInfo> partitions)
        : this(name, @internal, partitions, Array.Empty<AclOperation>())
    {
    }

    /// <summary>
    /// Creates a description with <see cref="Uuid.ZERO_UUID"/> as the topic id — Java's
    /// four-argument constructor.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="internal">Whether the topic is internal to Kafka.</param>
    /// <param name="partitions">
    /// One entry per partition; the index is the partition id.
    /// </param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> when they are not known.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="partitions"/> is null.
    /// </exception>
    public TopicDescription(
        string name,
        bool @internal,
        IEnumerable<TopicPartitionInfo> partitions,
        IEnumerable<AclOperation>? authorizedOperations)
        : this(name, @internal, partitions, authorizedOperations, Uuid.ZERO_UUID)
    {
    }

    /// <summary>Creates a description — Java's five-argument constructor.</summary>
    /// <param name="name">The topic name.</param>
    /// <param name="internal">Whether the topic is internal to Kafka.</param>
    /// <param name="partitions">
    /// One entry per partition; the index is the partition id.
    /// </param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> when they are not known.
    /// </param>
    /// <param name="topicId">The topic id.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="partitions"/> is null.
    /// </exception>
    public TopicDescription(
        string name,
        bool @internal,
        IEnumerable<TopicPartitionInfo> partitions,
        IEnumerable<AclOperation>? authorizedOperations,
        Uuid topicId)
    {
        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        Name = name ?? throw new ArgumentNullException(nameof(name));
        IsInternal = @internal;
        _partitions = new List<TopicPartitionInfo>(partitions);

        _authorizedOperations =
            authorizedOperations is null ? null : new List<AclOperation>(authorizedOperations);
        TopicId = topicId;
    }

    /// <summary>The topic name — Java's <c>name()</c>.</summary>
    public string Name { get; }

    /// <summary>
    /// Whether the topic is internal to Kafka (for example <c>__consumer_offsets</c>) —
    /// Java's <c>isInternal()</c>.
    /// </summary>
    public bool IsInternal { get; }

    /// <summary>The topic id — Java's <c>topicId()</c>.</summary>
    public Uuid TopicId { get; }

    /// <summary>
    /// One entry per partition, where the index is the partition id — Java's
    /// <c>partitions()</c>.
    /// </summary>
    public IReadOnlyList<TopicPartitionInfo> Partitions => _partitions;

    /// <summary>
    /// The operations the caller is authorized to perform on this topic, or
    /// <see langword="null"/> when they are not known — Java's <c>authorizedOperations()</c>.
    /// An empty collection means they were reported and none is authorized.
    /// </summary>
    /// <remarks>
    /// A collection, where Java returns a <c>Set</c>: <c>IReadOnlySet&lt;T&gt;</c> post-dates
    /// netstandard2.0.
    /// </remarks>
    public IReadOnlyCollection<AclOperation>? AuthorizedOperations => _authorizedOperations;

    /// <summary>
    /// Value equality over the name, the internal flag, the partitions and the authorized operations (as a set),
    /// Java's <c>equals</c>; the topic id is not compared, as in Java.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same topic.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is TopicDescription other
            && IsInternal == other.IsInternal
            && string.Equals(Name, other.Name, StringComparison.Ordinal)
            && _partitions.SequenceEqual(other._partitions)
            && OperationsMatch(_authorizedOperations, other._authorizedOperations));

    /// <summary>The hash of the members <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Name);
            hash = (hash * 31) + IsInternal.GetHashCode();
            foreach (TopicPartitionInfo partition in _partitions)
            {
                hash = (hash * 31) + (partition?.GetHashCode() ?? 0);
            }

            hash = (hash * 31) + OperationsHash(_authorizedOperations);
            return hash;
        }
    }

    /// <summary>A diagnostic rendering — Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(name={0}, internal={1}, partitions={2}, authorizedOperations={3})",
            Name,
            IsInternal ? "true" : "false",
            string.Join(",", _partitions.Select(static partition => partition.ToString())),
            _authorizedOperations is null ? "null" : "[" + string.Join(", ", _authorizedOperations) + "]");

    private static bool OperationsMatch(List<AclOperation>? left, List<AclOperation>? right) =>
        left is null || right is null
            ? left is null && right is null
            : new HashSet<AclOperation>(left).SetEquals(right);

    private static int OperationsHash(List<AclOperation>? operations)
    {
        if (operations is null)
        {
            return 0;
        }

        int hash = 0;
        foreach (AclOperation operation in new HashSet<AclOperation>(operations))
        {
            hash = unchecked(hash + ((int)operation).GetHashCode());
        }

        return hash;
    }
}
