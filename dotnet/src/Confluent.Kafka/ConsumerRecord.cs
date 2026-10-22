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

namespace Confluent.Kafka;

/// <summary>
/// A record returned by <c>Poll</c>, Java's <c>org.apache.kafka.clients.consumer.ConsumerRecord&lt;K, V&gt;</c>.
/// </summary>
/// <remarks>
/// <para>
/// The key and value are the deserializers' output; the topic and the headers are owned copies, so the record
/// stays valid after the poll's native batch is freed. There is no public constructor: records come only from
/// <c>Poll</c>.
/// </para>
/// <para>
/// An absent key, or an absent value (a tombstone), is <c>default(TKey)</c> / <c>default(TValue)</c>, and the
/// deserializer is not called; an empty one is deserialized from an empty span. To tell a tombstone from a zero
/// value, use a nullable value type, such as <c>ConsumerRecord&lt;string, long?&gt;</c>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public sealed class ConsumerRecord<TKey, TValue>
{
    /// <summary>The timestamp of a record with no timestamp, Java's <c>NO_TIMESTAMP</c>.</summary>
    public const long NoTimestamp = -1;

    internal ConsumerRecord(
        string topic,
        int partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        TKey key,
        TValue value,
        Headers headers,
        int? leaderEpoch,
        int serializedKeySize,
        int serializedValueSize)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        TimestampType = timestampType;
        Key = key;
        Value = value;
        Headers = headers;
        LeaderEpoch = leaderEpoch;
        SerializedKeySize = serializedKeySize;
        SerializedValueSize = serializedValueSize;
    }

    /// <summary>The topic name.</summary>
    public string Topic { get; }

    /// <summary>The partition the record was read from.</summary>
    public int Partition { get; }

    /// <summary>The record's offset within its partition.</summary>
    public long Offset { get; }

    /// <summary>
    /// The record timestamp in milliseconds since the epoch, or
    /// <see cref="NoTimestamp"/> (<c>-1</c>) when absent.
    /// </summary>
    public long Timestamp { get; }

    /// <summary>The kind of <see cref="Timestamp"/> the record carries.</summary>
    public TimestampType TimestampType { get; }

    /// <summary>
    /// The deserialized key, or <c>default(TKey)</c> when the key is absent; the deserializer is not called then.
    /// </summary>
    public TKey Key { get; }

    /// <summary>
    /// The deserialized value, or <c>default(TValue)</c> for a tombstone; the deserializer is not called then.
    /// </summary>
    public TValue Value { get; }

    /// <summary>The record headers; empty when the record has none.</summary>
    public Headers Headers { get; }

    /// <summary>
    /// The record's leader epoch, or <see langword="null"/> when the record format has none (Java's
    /// <c>Optional&lt;Integer&gt; leaderEpoch()</c>).
    /// </summary>
    public int? LeaderEpoch { get; }

    /// <summary>
    /// The size of the serialized, uncompressed key in bytes, or <c>-1</c> if the key is
    /// <see langword="null"/> (Java's <c>serializedKeySize()</c>).
    /// </summary>
    public int SerializedKeySize { get; }

    /// <summary>
    /// The size of the serialized, uncompressed value in bytes, or <c>-1</c> if the value is
    /// <see langword="null"/> (Java's <c>serializedValueSize()</c>).
    /// </summary>
    public int SerializedValueSize { get; }
}
