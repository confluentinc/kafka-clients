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
using System.Linq;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Which of one consumer group's committed offsets to list — Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec</c>, one per group in the map
/// handed to <c>listConsumerGroupOffsets</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's setter <c>topicPartitions(Collection)</c> and its getter <c>topicPartitions()</c> are
/// the one property <see cref="TopicPartitions"/>, as on the options types.
/// </para>
/// <para>
/// <see langword="null"/>, Java's unset field, lists every partition of the group; an empty
/// collection lists none.
/// </para>
/// <para>
/// The collection is copied on assignment, in order and with duplicates, where Java stores the
/// reference, and <see cref="Equals(object)"/> compares it element by element in order, where
/// Java defers to the collection's own <c>equals</c>: both recorded deviations, so that equality,
/// the hash and the request stay those of the selection that was assigned.
/// </para>
/// </remarks>
public sealed class ListConsumerGroupOffsetsSpec
{
    private IReadOnlyList<TopicPartition>? _topicPartitions;

    /// <summary>
    /// The partitions whose offsets are to be listed for the group, or
    /// <see langword="null"/> for every partition the group has committed offsets for —
    /// Java's <c>topicPartitions()</c> and <c>topicPartitions(Collection)</c>.
    /// </summary>
    /// <remarks>
    /// <see langword="null"/> and an empty collection are different requests. The value read
    /// back is a copy of what was assigned, not the same instance.
    /// </remarks>
    public IReadOnlyCollection<TopicPartition>? TopicPartitions
    {
        get => _topicPartitions;
        set => _topicPartitions = value is null ? null : new List<TopicPartition>(value);
    }

    /// <summary>
    /// Compares the selected partitions — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when <paramref name="obj"/> selects the same partitions.</returns>
    /// <remarks>
    /// An unset selection equals only another unset selection, never an empty one. The
    /// comparison is order-sensitive.
    /// </remarks>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not ListConsumerGroupOffsetsSpec other)
        {
            return false;
        }

        if (_topicPartitions is null || other._topicPartitions is null)
        {
            return _topicPartitions is null && other._topicPartitions is null;
        }

        return _topicPartitions.SequenceEqual(other._topicPartitions);
    }

    /// <summary>
    /// Hashes the selected partitions — Java's <c>hashCode</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    /// <remarks>
    /// An order-sensitive fold, as a Java <c>List</c>'s hash is, so it agrees with
    /// <see cref="Equals(object)"/>. The value differs from Java's.
    /// </remarks>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            if (_topicPartitions is null)
            {
                return (hash * 31) + 0;
            }

            int elements = 1;
            foreach (var topicPartition in _topicPartitions)
            {
                elements = (elements * 31) + topicPartition.GetHashCode();
            }

            return (hash * 31) + elements;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// An unset selection renders as <c>null</c> and a set one as <c>[first, second]</c>, so an
    /// empty selection renders as <c>[]</c>, as in Java.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ListConsumerGroupOffsetsSpec(topicPartitions={0})",
            _topicPartitions is null
                ? "null"
                : "[" + string.Join(", ", _topicPartitions.Select(static topicPartition => topicPartition.ToString())) + "]");
}
