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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A description of one topic — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.TopicDescription</c>, and the per-key value of
/// <see cref="DescribeTopicsResult"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="AuthorizedOperations"/> distinguishes <em>absent</em> from
/// <em>empty</em>.</b> Java's accessor documents "or <b>null</b> if this is not known"
/// (<c>TopicDescription.java:129</c>), which is the case where the broker was never asked
/// (<c>DescribeTopicsOptions.IncludeAuthorizedOperations</c> left false) or did not
/// answer — <em>not</em> the case where it answered "none are authorized". The C ABI
/// carries the discriminant explicitly
/// (<c>kafka_admin_TopicDescription_has_authorized_operations</c> beside the count)
/// because a count of 0 covers both, so collapsing <see langword="null"/> into an empty
/// collection would discard information the ABI deliberately preserved.
/// </para>
/// <para>
/// <b>Value equality is deliberately not implemented</b>, although Java's class overrides
/// <c>equals</c>/<c>hashCode</c> — the same recorded choice as
/// <see cref="TopicPartitionInfo"/>, and consistent with every other value type this
/// binding has shipped except <see cref="Uuid"/> (which is a dictionary key and needs it).
/// </para>
/// </remarks>
public sealed class TopicDescription
{
    private readonly List<TopicPartitionInfo> _partitions;
    private readonly List<AclOperation>? _authorizedOperations;

    /// <summary>
    /// Creates a description with a <b>reported-but-empty</b> authorized-operations set
    /// and <see cref="Uuid.Zero"/> as the topic id — Java's three-argument constructor
    /// (<c>TopicDescription.java:64</c>), which passes <c>Collections.emptySet()</c>.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="isInternal">Whether the topic is internal to Kafka.</param>
    /// <param name="partitions">
    /// One entry per partition; the index is the partition id.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="partitions"/> is null.
    /// </exception>
    public TopicDescription(string name, bool isInternal, IEnumerable<TopicPartitionInfo> partitions)
        : this(name, isInternal, partitions, Array.Empty<AclOperation>())
    {
    }

    /// <summary>
    /// Creates a description with <see cref="Uuid.Zero"/> as the topic id — Java's
    /// four-argument constructor (<c>TopicDescription.java:77</c>), which passes
    /// <c>Uuid.ZERO_UUID</c>.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="isInternal">Whether the topic is internal to Kafka.</param>
    /// <param name="partitions">
    /// One entry per partition; the index is the partition id.
    /// </param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> if the broker reported none
    /// at all. See the absent-versus-empty note in the type remarks.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="partitions"/> is null.
    /// </exception>
    public TopicDescription(
        string name,
        bool isInternal,
        IEnumerable<TopicPartitionInfo> partitions,
        IEnumerable<AclOperation>? authorizedOperations)
        : this(name, isInternal, partitions, authorizedOperations, Uuid.Zero)
    {
    }

    /// <summary>
    /// Creates a description — Java's five-argument constructor
    /// (<c>TopicDescription.java:92</c>).
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="isInternal">Whether the topic is internal to Kafka.</param>
    /// <param name="partitions">
    /// One entry per partition; the index is the partition id.
    /// </param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> if the broker reported none
    /// at all. See the absent-versus-empty note in the type remarks.
    /// </param>
    /// <param name="topicId">The topic id.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="partitions"/> is null.
    /// </exception>
    public TopicDescription(
        string name,
        bool isInternal,
        IEnumerable<TopicPartitionInfo> partitions,
        IEnumerable<AclOperation>? authorizedOperations,
        Uuid topicId)
    {
        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        Name = name ?? throw new ArgumentNullException(nameof(name));
        IsInternal = isInternal;
        _partitions = new List<TopicPartitionInfo>(partitions);

        // null stays null — see the absent-versus-empty note in the type remarks.
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
    /// <see langword="null"/> if the broker reported none at all — Java's
    /// <c>authorizedOperations()</c>. An <b>empty</b> collection means "reported, and
    /// none are authorized"; see the absent-versus-empty note in the type remarks.
    /// </summary>
    /// <remarks>
    /// Java returns a <c>Set</c>; <c>IReadOnlySet&lt;T&gt;</c> post-dates the
    /// netstandard2.0 floor, so this is an <c>IReadOnlyCollection&lt;T&gt;</c> — the same
    /// substitution the consumer surface already makes for <c>Assignment()</c> and
    /// friends.
    /// </remarks>
    public IReadOnlyCollection<AclOperation>? AuthorizedOperations => _authorizedOperations;

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(name={0}, internal={1}, partitions={2}, authorizedOperations={3})",
            Name,
            IsInternal,
            string.Join(",", _partitions.Select(static partition => partition.ToString())),
            _authorizedOperations is null ? "null" : "[" + string.Join(", ", _authorizedOperations) + "]");
}
