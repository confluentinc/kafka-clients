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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.Producer;

/// <summary>
/// A record to send to a topic, Java's <c>org.apache.kafka.clients.producer.ProducerRecord&lt;K, V&gt;</c>.
/// </summary>
/// <remarks>
/// The producer calls its serializers also for a <see langword="null"/> key or value, as Java does. A
/// <see langword="null"/> result is an absent field (no key, or a tombstone), and an empty result a present, empty
/// one. For a value type <c>TValue?</c> is not <c>Nullable&lt;T&gt;</c>: a value-type tombstone needs a nullable
/// value type, such as <c>long?</c>, with a matching serde.
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class ProducerRecord<TKey, TValue>
{
    private readonly string _topic;
    private readonly int? _partition;
    private readonly long? _timestamp;
    private readonly TKey? _key;
    private readonly TValue? _value;
    private readonly Headers _headers;

    /// <summary>
    /// Initializes a record from the topic and value, as Java's <c>ProducerRecord(topic, value)</c>, and the optional
    /// key, partition, timestamp and headers.
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
    /// producer stamp it. Must be non-negative when specified.
    /// </param>
    /// <param name="headers">
    /// The headers to include in the record, copied into the record's own <see cref="Headers()"/>; a
    /// <see langword="null"/> sequence gives no headers.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="headers"/> contains a <see langword="null"/> header.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="topic"/> is null, or <paramref name="timestamp"/> or <paramref name="partition"/> is
    /// specified and negative; the timestamp is checked first.
    /// </exception>
    public ProducerRecord(
        string topic,
        TValue? value,
        TKey? key = default,
        int? partition = null,
        long? timestamp = null,
        IEnumerable<Header>? headers = null)
    {
        _topic = topic ?? throw new IllegalArgumentException("Topic cannot be null.");

        if (timestamp is { } t && t < 0)
        {
            throw new IllegalArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Invalid timestamp: {0}. Timestamp should always be non-negative or null.",
                    t));
        }

        if (partition is { } p && p < 0)
        {
            throw new IllegalArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Invalid partition: {0}. Partition number should always be non-negative or null.",
                    p));
        }

        _partition = partition;
        _timestamp = timestamp;
        _key = key;
        _value = value;
        _headers = new Headers(headers);
    }

    /// <summary>The destination topic.</summary>
    /// <returns>The destination topic.</returns>
    public string Topic() => _topic;

    /// <summary>
    /// The target partition, or <see langword="null"/> to let the producer choose one.
    /// </summary>
    /// <returns>The target partition, or <see langword="null"/> to let the producer choose one.</returns>
    public int? Partition() => _partition;

    /// <summary>
    /// The record timestamp in milliseconds since epoch, or <see langword="null"/> to let the
    /// producer stamp it.
    /// </summary>
    /// <returns>The record timestamp in milliseconds since epoch, or <see langword="null"/> to let the producer
    /// stamp it.</returns>
    public long? Timestamp() => _timestamp;

    /// <summary>The record key, or <see langword="null"/> for no key.</summary>
    /// <returns>The record key, or <see langword="null"/> for no key.</returns>
    public TKey? Key() => _key;

    /// <summary>The record value, or <see langword="null"/> for a tombstone.</summary>
    /// <returns>The record value, or <see langword="null"/> for a tombstone.</returns>
    public TValue? Value() => _value;

    /// <summary>
    /// The record's own headers, a writable copy of the constructor's <c>headers</c>, empty when there were none.
    /// Once a <see cref="KafkaProducer{TKey, TValue}"/> or <see cref="AsyncKafkaProducer{TKey, TValue}"/> has
    /// serialized the record for a send, they are read-only: <c>Add</c> and <c>Remove</c> throw
    /// <see cref="InvalidOperationException"/>. The mock producers leave them writable.
    /// </summary>
    /// <returns>The record's own headers, a writable copy of the constructor's <c>headers</c>, empty when there
    /// were none.</returns>
    public Headers Headers() => _headers;

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is ProducerRecord<TKey, TValue> other
            && EqualityComparer<TKey?>.Default.Equals(Key(), other.Key())
            && Partition() == other.Partition()
            && string.Equals(Topic(), other.Topic(), StringComparison.Ordinal)
            && Headers().Equals(other.Headers())
            && EqualityComparer<TValue?>.Default.Equals(Value(), other.Value())
            && Timestamp() == other.Timestamp();
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int result = StringComparer.Ordinal.GetHashCode(_topic);
        result = unchecked((31 * result) + (_partition ?? 0));
        result = unchecked((31 * result) + _headers.GetHashCode());
        result = unchecked((31 * result) + (_key is null ? 0 : EqualityComparer<TKey>.Default.GetHashCode(_key)));
        result = unchecked((31 * result) + (_value is null ? 0 : EqualityComparer<TValue>.Default.GetHashCode(_value)));
        return unchecked((31 * result) + (_timestamp is { } t ? t.GetHashCode() : 0));
    }

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ProducerRecord(topic={0}, partition={1}, headers={2}, key={3}, value={4}, timestamp={5})",
            Topic(),
            Partition() is { } p ? p.ToString(CultureInfo.InvariantCulture) : "null",
            Headers().ToString(),
            Key()?.ToString() ?? "null",
            Value()?.ToString() ?? "null",
            Timestamp() is { } t ? t.ToString(CultureInfo.InvariantCulture) : "null");
}
