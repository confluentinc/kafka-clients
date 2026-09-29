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
/// immutable, equatable shape. The input type for <c>IAsyncConsumer.Seek(...)</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>No argument validation, as in Java.</b> The constructor stores whatever it is given —
/// Java's <c>TopicPartition(String topic, int partition)</c> assigns both fields and checks
/// neither (<c>TopicPartition.java:32-35</c>) — so a <see langword="null"/> topic and a
/// negative partition (Java's <c>-1</c> "unknown partition", which its own producer builds on
/// the failure path, <c>KafkaProducer.java:1620</c>) are both representable. What the value
/// <em>means</em> is decided where it is used: an admin RPC hands a negative partition to the
/// core, which answers the way Java does (a per-key or whole-request
/// <c>"The given partition index -1 is not valid."</c> for the two reassignment RPCs; elsewhere
/// the core or the broker answers); the consumer's operations keep their own stricter-than-Java
/// "Partition must not be negative." precondition (M15/P13.2 D11). A <see langword="null"/>
/// topic is rejected by every operation that would send it, with that operation's own
/// exception, because the C ABI has no representation for it (M15/P13.2 D1).
/// </para>
/// <para>
/// Because this is a <c>readonly struct</c>, a <c>default(TopicPartition)</c> is also always
/// constructible: its <see cref="Topic"/> is <see langword="null"/> and its
/// <see cref="Partition"/> is <c>0</c>, which makes it equal to
/// <c>new TopicPartition(null!, 0)</c>. (Java's <c>TopicPartition</c> is a class, so a
/// <see langword="null"/> <em>reference</em> is also possible there; this matches
/// confluent-kafka-dotnet's struct choice.) <see cref="Equals(TopicPartition)"/> and
/// <see cref="GetHashCode"/> are null-safe for a null topic, and <see cref="ToString"/>
/// prints it as Java's string concatenation does — <c>"null-0"</c>.
/// </para>
/// <para>
/// <b>The annotation stays non-nullable</b> (M15/P13.2 D10) although the constructor accepts
/// <see langword="null"/> at run time — exactly as <c>default(TopicPartition)</c> already
/// yielded a null <see cref="Topic"/> behind the same annotation. It describes what this
/// binding hands back: a null topic cannot cross the C ABI, so every
/// <see cref="TopicPartition"/> the client returns has a non-null topic. A caller that builds
/// one with a null topic opts out of that with <c>null!</c>.
/// </para>
/// </remarks>
public readonly struct TopicPartition : IEquatable<TopicPartition>
{
    /// <summary>
    /// Initializes a new <see cref="TopicPartition"/>, storing both arguments as given — Java's
    /// <c>TopicPartition(String, int)</c> (<c>TopicPartition.java:32-35</c>), which validates
    /// neither.
    /// </summary>
    /// <param name="topic">
    /// The topic name. <see langword="null"/> is accepted and stored (see the type's remarks),
    /// but no operation can send it.
    /// </param>
    /// <param name="partition">
    /// The partition index. A negative value is accepted and stored; the operation it is passed
    /// to decides what it means.
    /// </param>
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
        // A null Topic is constructible (the ctor stores it, as Java's does) and is what a
        // default (uninitialized) struct holds; guard so GetHashCode never dereferences null.
        int topicHash = Topic is null ? 0 : StringComparer.Ordinal.GetHashCode(Topic);
        return unchecked((topicHash * 397) ^ Partition);
    }

    /// <summary>
    /// Returns the Java-style <c>"topic-partition"</c> string (e.g. <c>"orders-3"</c>) —
    /// Java's <c>topic + "-" + partition</c> (<c>TopicPartition.java:70</c>), so a
    /// <see langword="null"/> topic prints as <c>"null"</c> (<c>"null-5"</c>) and a negative
    /// partition keeps its sign (<c>"orders--1"</c>).
    /// </summary>
    public override string ToString() =>
        (Topic ?? "null") + "-" + Partition.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
