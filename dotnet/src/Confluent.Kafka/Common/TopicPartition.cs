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
using System.Globalization;

namespace Confluent.Kafka.Common;

/// <summary>A topic and a partition, Java's <c>TopicPartition</c>, with value equality.</summary>
/// <remarks>
/// The constructor, like Java's, stores a <see langword="null"/> topic and a negative partition as given, though
/// <see cref="Topic()"/> is annotated non-nullable.
/// </remarks>
public sealed class TopicPartition : IEquatable<TopicPartition>
{
    private readonly string _topic;
    private readonly int _partition;
    private int _hash;

    /// <summary>Initializes a pair, storing both arguments as given.</summary>
    public TopicPartition(string topic, int partition)
    {
        _topic = topic;
        _partition = partition;
    }

    /// <summary>The topic name.</summary>
    /// <returns>The topic name.</returns>
    public string Topic() => _topic;

    /// <summary>The partition index.</summary>
    /// <returns>The partition index.</returns>
    public int Partition() => _partition;

    /// <summary>
    /// Determines whether two <see cref="TopicPartition"/> values are equal; two <see langword="null"/> references
    /// are equal.
    /// </summary>
    public static bool operator ==(TopicPartition? left, TopicPartition? right) =>
        ReferenceEquals(left, right) || (left is not null && left.Equals(right));

    /// <summary>
    /// Determines whether two <see cref="TopicPartition"/> values are not equal.
    /// </summary>
    public static bool operator !=(TopicPartition? left, TopicPartition? right) => !(left == right);

    /// <inheritdoc/>
    public bool Equals(TopicPartition? other) =>
        other is not null
        && (ReferenceEquals(this, other)
            || (_partition == other._partition && string.Equals(_topic, other._topic, StringComparison.Ordinal)));

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is TopicPartition other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        // Cached as Java's: recomputed while it is 0, so a racing first call only repeats the computation.
        if (_hash != 0)
        {
            return _hash;
        }

        int topicHash = _topic is null ? 0 : StringComparer.Ordinal.GetHashCode(_topic);
        int result = unchecked((topicHash * 397) ^ _partition);
        _hash = result;
        return result;
    }

    /// <inheritdoc/>
    public override string ToString() => Format(_topic, _partition);

    internal static string Format(string? topic, int partition) =>
        (topic ?? "null") + "-" + partition.ToString(CultureInfo.InvariantCulture);
}
