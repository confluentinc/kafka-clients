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

namespace Confluent.Kafka;

/// <summary>A topic and a partition, Java's <c>TopicPartition</c>, with value equality.</summary>
/// <remarks>
/// The constructor, like Java's, stores a <see langword="null"/> topic and a negative partition as given, though
/// <see cref="Topic"/> is annotated non-nullable.
/// </remarks>
public readonly struct TopicPartition : IEquatable<TopicPartition>
{
    /// <summary>Initializes a pair, storing both arguments as given.</summary>
    public TopicPartition(string topic, int partition)
    {
        Topic = topic;
        Partition = partition;
    }

    /// <summary>The topic name.</summary>
    public string Topic { get; }

    /// <summary>The partition index.</summary>
    public int Partition { get; }

    /// <summary>
    /// Determines whether two <see cref="TopicPartition"/> values are equal.
    /// </summary>
    public static bool operator ==(TopicPartition left, TopicPartition right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="TopicPartition"/> values are not equal.
    /// </summary>
    public static bool operator !=(TopicPartition left, TopicPartition right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(TopicPartition other) =>
        Partition == other.Partition && string.Equals(Topic, other.Topic, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is TopicPartition other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int topicHash = Topic is null ? 0 : StringComparer.Ordinal.GetHashCode(Topic);
        return unchecked((topicHash * 397) ^ Partition);
    }

    /// <inheritdoc/>
    public override string ToString() =>
        (Topic ?? "null") + "-" + Partition.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
