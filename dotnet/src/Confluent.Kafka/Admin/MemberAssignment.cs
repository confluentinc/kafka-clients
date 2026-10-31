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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The partitions assigned to one group member — Java's
/// <c>org.apache.kafka.clients.admin.MemberAssignment</c>.
/// </summary>
/// <remarks>
/// Java's <c>Set</c> is an <c>IReadOnlyCollection</c>, as <c>IReadOnlySet</c> post-dates
/// netstandard2.0; the constructor collapses duplicates, and <see cref="Equals(object)"/> and
/// <see cref="GetHashCode"/> are order-insensitive, as a <c>Set</c>'s are.
/// </remarks>
public sealed class MemberAssignment
{
    private readonly List<TopicPartition> _topicPartitions;

    /// <summary>
    /// Creates an assignment over the given partitions — Java's constructor.
    /// </summary>
    /// <param name="topicPartitions">
    /// The assigned partitions, or <see langword="null"/> for none, as in Java. Duplicates are
    /// collapsed and the collection is copied, as Java's <c>Set.copyOf</c>.
    /// </param>
    public MemberAssignment(IEnumerable<TopicPartition>? topicPartitions)
    {
        if (topicPartitions is null)
        {
            _topicPartitions = new List<TopicPartition>();
            return;
        }

        var seen = new HashSet<TopicPartition>();
        _topicPartitions = new List<TopicPartition>();
        foreach (var topicPartition in topicPartitions)
        {
            if (seen.Add(topicPartition))
            {
                _topicPartitions.Add(topicPartition);
            }
        }
    }

    /// <summary>
    /// The partitions assigned to the group member — Java's <c>topicPartitions()</c>. Never
    /// <see langword="null"/>; empty means "assigned nothing".
    /// </summary>
    public IReadOnlyCollection<TopicPartition> TopicPartitions => _topicPartitions;

    /// <summary>
    /// Compares the assigned partitions as a set — Java's <c>equals</c>, which delegates to
    /// <c>Set.equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when <paramref name="obj"/> assigns the same partitions.</returns>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is MemberAssignment other
            && _topicPartitions.Count == other._topicPartitions.Count
            && new HashSet<TopicPartition>(_topicPartitions).SetEquals(other._topicPartitions);
    }

    /// <summary>
    /// The sum of the assigned partitions' hashes — Java's <c>hashCode</c>, which is
    /// <c>Set.hashCode()</c>: order-insensitive, as <see cref="Equals(object)"/> is.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 0;
            foreach (var topicPartition in _topicPartitions)
            {
                hash += topicPartition.GetHashCode();
            }

            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// The partitions render in the order they were given, where Java's follow its <c>Set</c>'s
    /// unspecified order: a recorded deviation that makes the rendering stable.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(topicPartitions={0})",
            string.Join(",", _topicPartitions.Select(static topicPartition => topicPartition.ToString())));
}
