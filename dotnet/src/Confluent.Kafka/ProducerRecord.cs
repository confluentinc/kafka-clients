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

namespace Confluent.Kafka;

/// <summary>
/// A record to send to a topic, Java's <c>org.apache.kafka.clients.producer.ProducerRecord&lt;K, V&gt;</c>.
/// </summary>
/// <remarks>
/// The producer calls its serializers also for a <see langword="null"/> key or value, as Java does. A
/// <see langword="null"/> result is an absent field (no key, or a tombstone), and an empty result a present, empty
/// one. For a value type <c>TValue?</c> is not <c>Nullable&lt;T&gt;</c>: a value-type tombstone needs a nullable
/// value type, such as <c>long?</c>, with a matching serde. A negative <see cref="Timestamp"/> is not rejected,
/// where Java's constructor rejects it.
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class ProducerRecord<TKey, TValue>
{
    /// <summary>
    /// Initializes a record from the topic and value, as Java's <c>ProducerRecord(topic, value)</c>, and the optional
    /// key, partition and timestamp.
    /// </summary>
    /// <param name="topic">The destination topic (required, non-null).</param>
    /// <param name="value">
    /// The record value, or <see langword="null"/> for a tombstone (subject to the serializer). An empty value
    /// differs from a null one only if the serializer keeps the difference, as <see cref="Serdes.ByteArray"/> does.
    /// </param>
    /// <param name="key">
    /// The record key, or <see langword="null"/> for no key (subject to the serializer).
    /// </param>
    /// <param name="partition">
    /// The target partition, or <see langword="null"/> to let the producer choose. Must be
    /// non-negative when specified.
    /// </param>
    /// <param name="timestamp">
    /// The record timestamp (milliseconds since epoch), or <see langword="null"/> to let the
    /// producer stamp it.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/> is specified and negative.
    /// </exception>
    public ProducerRecord(
        string topic,
        TValue? value,
        TKey? key = default,
        int? partition = null,
        long? timestamp = null)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));

        if (partition.HasValue && partition.Value < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition),
                partition.Value,
                "Partition must not be negative.");
        }

        Partition = partition;
        Timestamp = timestamp;
        Key = key;
        Value = value;
    }

    /// <summary>The destination topic.</summary>
    public string Topic { get; }

    /// <summary>
    /// The target partition, or <see langword="null"/> to let the producer choose one.
    /// </summary>
    public int? Partition { get; }

    /// <summary>
    /// The record timestamp in milliseconds since epoch, or <see langword="null"/> to let the
    /// producer stamp it.
    /// </summary>
    public long? Timestamp { get; }

    /// <summary>The record key, or <see langword="null"/> for no key.</summary>
    public TKey? Key { get; }

    /// <summary>The record value, or <see langword="null"/> for a tombstone.</summary>
    public TValue? Value { get; }

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ProducerRecord(topic={0}, partition={1}, timestamp={2}, key={3}, value={4})",
            Topic,
            Partition is { } p ? p.ToString(CultureInfo.InvariantCulture) : "null",
            Timestamp is { } t ? t.ToString(CultureInfo.InvariantCulture) : "null",
            Key?.ToString() ?? "null",
            Value?.ToString() ?? "null");
}
