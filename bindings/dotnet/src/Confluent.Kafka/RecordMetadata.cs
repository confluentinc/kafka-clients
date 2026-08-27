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

using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// The metadata for a record that has been acknowledged by the cluster — the .NET realization of
/// Java's <c>org.apache.kafka.clients.producer.RecordMetadata</c>, returned by
/// <see cref="IAsyncProducer{TKey, TValue}.Send"/> / <see cref="IProducer{TKey, TValue}.Send"/>.
/// Non-generic — it carries no key/value (only topic / partition / offset / timestamp), so it is
/// <b>not</b> parameterized (PLAN §3.2).
/// </summary>
/// <remarks>
/// <para>
/// <b>Clipped to today's ABI (CLAUDE.md §3, PLAN §2/§5).</b> The current
/// <c>kafka_producer_RecordMetadata_t</c> exposes topic / partition / offset / timestamp only,
/// so this omits Java's serialized-size and <c>hasOffset()</c> / <c>hasTimestamp()</c>
/// accessors — the ABI does not surface them yet. They arrive additively when it does.
/// </para>
/// <para>
/// The fields are owned copies read out of a flat transient <c>RecordMetadata_t</c> handle
/// (ffi §A2 Category 2) on the completion pump: the topic is copied out via
/// <c>Utf8Marshal.PtrToString</c> before the handle is destroyed, so this value never references
/// native memory. A <c>sealed class</c> (matching the <see cref="PartitionInfo"/> /
/// <see cref="OffsetAndTimestamp"/> result-payload precedent), immutable.
/// </para>
/// </remarks>
public sealed class RecordMetadata
{
    /// <summary>
    /// Initializes a new instance from the values copied out of the ABI metadata handle. Used by
    /// the completion pump's copy-out.
    /// </summary>
    /// <param name="topic">The topic the record was published to.</param>
    /// <param name="partition">The partition the record was published to.</param>
    /// <param name="offset">The offset the record was assigned.</param>
    /// <param name="timestamp">The record's timestamp in milliseconds since epoch.</param>
    internal RecordMetadata(string topic, int partition, long offset, long timestamp)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
    }

    /// <summary>The topic the record was published to.</summary>
    public string Topic { get; }

    /// <summary>The partition the record was published to.</summary>
    public int Partition { get; }

    /// <summary>The offset the record was assigned in its partition.</summary>
    public long Offset { get; }

    /// <summary>The record's timestamp in milliseconds since epoch.</summary>
    public long Timestamp { get; }

    /// <summary>
    /// Returns a string of the form <c>"topic-partition@offset"</c>, mirroring Java's
    /// <c>RecordMetadata.toString()</c>.
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{0}-{1}@{2}",
            Topic,
            Partition,
            Offset);
}
