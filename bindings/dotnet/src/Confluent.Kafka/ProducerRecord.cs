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
/// A record to publish to a topic — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.ProducerRecord</c>, passed to
/// <see cref="IAsyncProducer.Send(ProducerRecord, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Clipped to today's ABI (CLAUDE.md §3, PLAN §2/§5).</b> The current
/// <c>kafka_producer_Producer_send</c> takes topic / partition / timestamp / key / value only,
/// so this omits Java's <c>headers()</c> — the ABI's send path has no headers field. It arrives
/// additively when the ABI grows to carry it. The interim key/value type is
/// <see cref="ReadOnlyMemory{Byte}"/> (bytes-only; the typed generic producer is deferred).
/// </para>
/// <para>
/// <b>Immutable, and validated in the constructor (Java-faithful).</b> Java's
/// <c>ProducerRecord</c> constructor rejects a null topic and a negative partition; this mirrors
/// that — the checks live here, not on the send path, so the invariants hold for the record's
/// whole life and every precondition fires (before any native call) at construction (ffi §A5;
/// PLAN §4 decision 9 — a recorded placement choice, since the plan lists them on the send path,
/// but the record ctor is the Java-faithful home and is still strictly before any P/Invoke).
/// A negative <see cref="Timestamp"/> is <b>not</b> rejected here: <c>null</c> means "let the
/// producer stamp it" (the send path passes the ABI's <c>-1</c> sentinel), and a non-null value
/// is forwarded verbatim.
/// </para>
/// </remarks>
public sealed class ProducerRecord
{
    /// <summary>
    /// Initializes a new record. Mirrors the Java / Python constructor argument order
    /// (<c>topic</c>, <c>value</c>, then optional <c>key</c> / <c>partition</c> /
    /// <c>timestamp</c>).
    /// </summary>
    /// <param name="topic">The destination topic (required, non-null).</param>
    /// <param name="value">
    /// The record value, or <see langword="null"/> for a tombstone (a null value). An empty
    /// (zero-length) value is distinct from a null one.
    /// </param>
    /// <param name="key">
    /// The record key, or <see langword="null"/> for no key. An empty (zero-length) key is
    /// distinct from a null one.
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
        ReadOnlyMemory<byte>? value,
        ReadOnlyMemory<byte>? key = null,
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
    public ReadOnlyMemory<byte>? Key { get; }

    /// <summary>The record value, or <see langword="null"/> for a tombstone.</summary>
    public ReadOnlyMemory<byte>? Value { get; }

    /// <summary>
    /// Returns a string of the form
    /// <c>"ProducerRecord(topic=…, partition=…, timestamp=…, keyBytes=…, valueBytes=…)"</c>,
    /// echoing Java's <c>ProducerRecord.toString()</c> shape (byte payloads are summarized by
    /// length, not dumped).
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ProducerRecord(topic={0}, partition={1}, timestamp={2}, keyBytes={3}, valueBytes={4})",
            Topic,
            Partition is { } p ? p.ToString(CultureInfo.InvariantCulture) : "null",
            Timestamp is { } t ? t.ToString(CultureInfo.InvariantCulture) : "null",
            Key is { } k ? k.Length.ToString(CultureInfo.InvariantCulture) : "null",
            Value is { } v ? v.Length.ToString(CultureInfo.InvariantCulture) : "null");
}
