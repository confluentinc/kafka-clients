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
/// A typed record to publish to a topic — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.ProducerRecord&lt;K, V&gt;</c>, passed to
/// <see cref="IAsyncProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, System.Threading.CancellationToken)"/> /
/// <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/> (and their
/// <see cref="IDeliveryCallback"/>-taking overloads).
/// The producer serializes <typeparamref name="TKey"/> / <typeparamref name="TValue"/> to bytes
/// with the serializers supplied to its constructor (M11/P5).
/// </summary>
/// <remarks>
/// <para>
/// <b>Generic-only (M11/P5, PLAN §3.1).</b> The producer surface is generic-only, mirroring the
/// consumer's M6/P1b conversion — there is no bytes-specialized sibling. Bytes users write
/// <c>ProducerRecord&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>.
/// </para>
/// <para>
/// <b>Clipped to today's ABI (CLAUDE.md §3).</b> The current <c>kafka_producer_Producer_send</c>
/// takes topic / partition / timestamp / key / value only, so this omits Java's <c>headers()</c> —
/// the ABI's send path has no headers field. It arrives additively when the ABI grows to carry it.
/// </para>
/// <para>
/// <b>Three-state key / value (PLAN §3.4).</b> <see cref="Key"/> / <see cref="Value"/> are
/// <c>TKey?</c> / <c>TValue?</c>: a <see langword="null"/> (reference) key/value is handed to the
/// serializer unchanged (Java-faithful — the serializer is <b>always</b> invoked, even on null),
/// and the serializer's <c>byte[]?</c> return drives the wire sentinel — <see langword="null"/>
/// bytes → an absent field (no key / tombstone), empty bytes → present-but-empty. For a value type
/// <c>T</c> (e.g. <c>ProducerRecord&lt;string, long&gt;</c>), <c>T?</c> is the default-nullable
/// annotation, <b>not</b> <c>Nullable&lt;T&gt;</c> — a <c>0L</c> value is a present value, not a
/// tombstone; a user needing a value-type tombstone uses a nullable value type (e.g.
/// <c>&lt;string, long?&gt;</c>) with a matching serde.
/// </para>
/// <para>
/// <b>Immutable, and validated in the constructor (Java-faithful).</b> Java's
/// <c>ProducerRecord</c> constructor rejects a null topic and a negative partition; this mirrors
/// that — the checks live here, not on the send path, so the invariants hold for the record's
/// whole life and every precondition fires (before any native call) at construction (ffi §A5).
/// A negative <see cref="Timestamp"/> is <b>not</b> rejected here (a pre-existing .NET choice,
/// orthogonal to the generic conversion — PLAN §3.9): <c>null</c> means "let the producer stamp
/// it", and a non-null value is forwarded verbatim.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class ProducerRecord<TKey, TValue>
{
    /// <summary>
    /// Initializes a new record. Mirrors the Java / Python constructor argument order
    /// (<c>topic</c>, <c>value</c>, then optional <c>key</c> / <c>partition</c> /
    /// <c>timestamp</c>).
    /// </summary>
    /// <param name="topic">The destination topic (required, non-null).</param>
    /// <param name="value">
    /// The record value, or <see langword="null"/> for a tombstone (subject to the serializer —
    /// PLAN §3.4). An empty value is distinct from a null one only if the serializer preserves the
    /// distinction (e.g. <see cref="Serdes.ByteArray"/>).
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

    /// <summary>
    /// Returns a string of the form
    /// <c>"ProducerRecord(topic=…, partition=…, timestamp=…, key=…, value=…)"</c>, echoing Java's
    /// <c>ProducerRecord.toString()</c> shape (the key / value render via their own
    /// <see cref="object.ToString"/>, <c>null</c> when absent).
    /// </summary>
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
