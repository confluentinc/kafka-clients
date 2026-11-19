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
/// value type, such as the <c>long?</c> of <c>Serdes.Long()</c>.
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
    /// Creates a record with a specified timestamp to be sent to a specified topic and partition.
    /// </summary>
    /// <param name="topic">The topic the record will be appended to.</param>
    /// <param name="partition">
    /// The partition to which the record should be sent, or <see langword="null"/> to let the producer choose one.
    /// </param>
    /// <param name="timestamp">
    /// The timestamp of the record, in milliseconds since epoch. If <see langword="null"/>, the producer will assign
    /// the timestamp using the current time.
    /// </param>
    /// <param name="key">The key that will be included in the record.</param>
    /// <param name="value">
    /// The record contents. An empty value differs from a <see langword="null"/> one only if the serializer keeps the
    /// difference, as <see cref="Serdes.ByteArray()"/> does.
    /// </param>
    /// <param name="headers">
    /// The headers that will be included in the record, copied into the record's own <see cref="Headers()"/>; a
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
        int? partition,
        long? timestamp,
        TKey? key,
        TValue? value,
        IEnumerable<Header>? headers)
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

    /// <summary>
    /// Creates a record with a specified timestamp to be sent to a specified topic and partition.
    /// </summary>
    /// <param name="topic">The topic the record will be appended to.</param>
    /// <param name="partition">
    /// The partition to which the record should be sent, or <see langword="null"/> to let the producer choose one.
    /// </param>
    /// <param name="timestamp">
    /// The timestamp of the record, in milliseconds since epoch. If <see langword="null"/>, the producer will assign
    /// the timestamp using the current time.
    /// </param>
    /// <param name="key">The key that will be included in the record.</param>
    /// <param name="value">The record contents.</param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="topic"/> is null, or <paramref name="timestamp"/> or <paramref name="partition"/> is
    /// specified and negative; the timestamp is checked first.
    /// </exception>
    public ProducerRecord(string topic, int? partition, long? timestamp, TKey? key, TValue? value)
        : this(topic, partition, timestamp, key, value, null)
    {
    }

    /// <summary>Creates a record to be sent to a specified topic and partition.</summary>
    /// <param name="topic">The topic the record will be appended to.</param>
    /// <param name="partition">
    /// The partition to which the record should be sent, or <see langword="null"/> to let the producer choose one.
    /// </param>
    /// <param name="key">The key that will be included in the record.</param>
    /// <param name="value">The record contents.</param>
    /// <param name="headers">
    /// The headers that will be included in the record, copied into the record's own <see cref="Headers()"/>; a
    /// <see langword="null"/> sequence gives no headers.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="headers"/> contains a <see langword="null"/> header.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="topic"/> is null, or <paramref name="partition"/> is specified and negative.
    /// </exception>
    public ProducerRecord(string topic, int? partition, TKey? key, TValue? value, IEnumerable<Header>? headers)
        : this(topic, partition, null, key, value, headers)
    {
    }

    /// <summary>Creates a record to be sent to a specified topic and partition.</summary>
    /// <param name="topic">The topic the record will be appended to.</param>
    /// <param name="partition">
    /// The partition to which the record should be sent, or <see langword="null"/> to let the producer choose one.
    /// </param>
    /// <param name="key">The key that will be included in the record.</param>
    /// <param name="value">The record contents.</param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="topic"/> is null, or <paramref name="partition"/> is specified and negative.
    /// </exception>
    public ProducerRecord(string topic, int? partition, TKey? key, TValue? value)
        : this(topic, partition, null, key, value, null)
    {
    }

    /// <summary>Creates a record to be sent to Kafka.</summary>
    /// <param name="topic">The topic the record will be appended to.</param>
    /// <param name="key">The key that will be included in the record.</param>
    /// <param name="value">The record contents.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="topic"/> is null.</exception>
    public ProducerRecord(string topic, TKey? key, TValue? value)
        : this(topic, null, null, key, value, null)
    {
    }

    /// <summary>Creates a record with no key.</summary>
    /// <param name="topic">The topic this record should be sent to.</param>
    /// <param name="value">The record contents.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="topic"/> is null.</exception>
    public ProducerRecord(string topic, TValue? value)
        : this(topic, null, null, default, value, null)
    {
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

    /// <summary>
    /// Whether <paramref name="obj"/> is a record with an equal topic, partition, headers, key, value and timestamp. A
    /// key or value of type <c>ReadOnlyMemory&lt;byte&gt;</c> or <c>ReadOnlyMemory&lt;byte&gt;?</c> is compared by its
    /// bytes, as Java's <c>ByteBuffer</c> is, and a <see langword="null"/> one equals only a <see langword="null"/>
    /// one; any other key or value by its own <c>Equals</c>, so a <c>byte[]</c> by reference, as Java's arrays are.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two records are equal.</returns>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is ProducerRecord<TKey, TValue> other
            && FieldEquals(Key(), other.Key())
            && Partition() == other.Partition()
            && string.Equals(Topic(), other.Topic(), StringComparison.Ordinal)
            && Headers().Equals(other.Headers())
            && FieldEquals(Value(), other.Value())
            && Timestamp() == other.Timestamp();
    }

    /// <summary>
    /// The hash of the topic, partition, headers, key, value and timestamp. A <c>ReadOnlyMemory&lt;byte&gt;</c> key or
    /// value hashes its bytes as Java's <c>ByteBuffer.hashCode</c> does, so equal records hash alike.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        int result = StringComparer.Ordinal.GetHashCode(_topic);
        result = unchecked((31 * result) + (_partition ?? 0));
        result = unchecked((31 * result) + _headers.GetHashCode());
        result = unchecked((31 * result) + FieldHashCode(_key));
        result = unchecked((31 * result) + FieldHashCode(_value));
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

    private static bool FieldEquals<T>(T? left, T? right)
    {
        if (IsByteBuffer<T>())
        {
            if (left is ReadOnlyMemory<byte> leftBytes)
            {
                return right is ReadOnlyMemory<byte> rightBytes && leftBytes.Span.SequenceEqual(rightBytes.Span);
            }

            return right is not ReadOnlyMemory<byte>;
        }

        return EqualityComparer<T?>.Default.Equals(left, right);
    }

    private static int FieldHashCode<T>(T? value)
    {
        if (IsByteBuffer<T>())
        {
            return value is ReadOnlyMemory<byte> bytes ? ByteBufferHashCode(bytes.Span) : 0;
        }

        return value is null ? 0 : EqualityComparer<T>.Default.GetHashCode(value);
    }

    private static bool IsByteBuffer<T>() =>
        typeof(T) == typeof(ReadOnlyMemory<byte>) || typeof(T) == typeof(ReadOnlyMemory<byte>?);

    private static int ByteBufferHashCode(ReadOnlySpan<byte> bytes)
    {
        int hash = 1;
        for (int i = bytes.Length - 1; i >= 0; i--)
        {
            hash = unchecked((31 * hash) + (sbyte)bytes[i]);
        }

        return hash;
    }
}
