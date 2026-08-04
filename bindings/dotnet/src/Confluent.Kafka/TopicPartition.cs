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

/// <summary>
/// A <c>(topic, partition)</c> pair — the .NET realization of Java's
/// <c>org.apache.kafka.common.TopicPartition</c>. A <c>readonly struct</c> with value
/// equality (so it is usable as a dictionary / set key), mirroring the Java type's
/// immutable, equatable shape. The input type for <c>IConsumer.SeekAsync(...)</c>.
/// </summary>
/// <remarks>
/// Because this is a <c>readonly struct</c>, a <c>default(TopicPartition)</c> is always
/// constructible and bypasses the constructor's argument validation: its <see cref="Topic"/>
/// is <see langword="null"/> and its <see cref="Partition"/> is <c>0</c>. (Java's
/// <c>TopicPartition</c> is a class, so the equivalent there is a <see langword="null"/>
/// reference rather than an instance with a null field; this matches
/// confluent-kafka-dotnet's struct choice.) <see cref="Equals(TopicPartition)"/> and
/// <see cref="GetHashCode"/> are null-safe for that state, and <see cref="ToString"/>
/// reflects it — the null <see cref="Topic"/> interpolates as an empty string (e.g.
/// <c>"-0"</c>). No public API in this binding produces a <c>default</c> value; it can
/// only arise from explicitly writing <c>default(TopicPartition)</c>.
/// </remarks>
public readonly struct TopicPartition : IEquatable<TopicPartition>
{
    /// <summary>
    /// Initializes a new <see cref="TopicPartition"/>.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition index (non-negative).</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    public TopicPartition(string topic, int partition)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

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

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — the topic
    /// (ordinal) and partition both match.
    /// </summary>
    public bool Equals(TopicPartition other) =>
        Partition == other.Partition && string.Equals(Topic, other.Topic, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is TopicPartition other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        // A default (uninitialized) struct has a null Topic; guard so GetHashCode
        // never dereferences null.
        int topicHash = Topic is null ? 0 : StringComparer.Ordinal.GetHashCode(Topic);
        return unchecked((topicHash * 397) ^ Partition);
    }

    /// <summary>
    /// Returns the Java-style <c>"topic-partition"</c> string (e.g. <c>"orders-3"</c>).
    /// </summary>
    public override string ToString() => $"{Topic}-{Partition.ToString(System.Globalization.CultureInfo.InvariantCulture)}";
}
