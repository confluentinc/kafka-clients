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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

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
/// deserializer is not called; an empty one is deserialized from an empty span. That default is
/// <see langword="null"/> for a reference type and for a nullable value type, such as the <c>long?</c> of
/// <c>Serdes.Long()</c>, so <c>ConsumerRecord&lt;string, long?&gt;</c> reads a tombstone as <see langword="null"/>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public sealed class ConsumerRecord<TKey, TValue>
{
    /// <summary>The timestamp of a record with no timestamp, Java's <c>NO_TIMESTAMP</c>.</summary>
    public const long NO_TIMESTAMP = -1;

    /// <summary>The serialized size of a null key or value, Java's <c>NULL_SIZE</c>.</summary>
    public const int NULL_SIZE = -1;

    private readonly string _topic;
    private readonly int _partition;
    private readonly long _offset;
    private readonly long _timestamp;
    private readonly TimestampType _timestampType;
    private readonly TKey? _key;
    private readonly TValue? _value;
    private readonly Headers _headers;
    private readonly int? _leaderEpoch;
    private readonly int _serializedKeySize;
    private readonly int _serializedValueSize;

    internal ConsumerRecord(
        string topic,
        int partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        TKey? key,
        TValue? value,
        Headers headers,
        int? leaderEpoch,
        int serializedKeySize,
        int serializedValueSize)
    {
        _topic = topic;
        _partition = partition;
        _offset = offset;
        _timestamp = timestamp;
        _timestampType = timestampType;
        _key = key;
        _value = value;
        _headers = headers;
        _leaderEpoch = leaderEpoch;
        _serializedKeySize = serializedKeySize;
        _serializedValueSize = serializedValueSize;
    }

    /// <summary>The topic name.</summary>
    /// <returns>The topic name.</returns>
    public string Topic() => _topic;

    /// <summary>The partition the record was read from.</summary>
    /// <returns>The partition the record was read from.</returns>
    public int Partition() => _partition;

    /// <summary>The record's offset within its partition.</summary>
    /// <returns>The record's offset within its partition.</returns>
    public long Offset() => _offset;

    /// <summary>
    /// The record timestamp in milliseconds since the epoch, or
    /// <see cref="NO_TIMESTAMP"/> (<c>-1</c>) when absent.
    /// </summary>
    /// <returns>The record timestamp in milliseconds since the epoch, or <see cref="NO_TIMESTAMP"/> (<c>-1</c>)
    /// when absent.</returns>
    public long Timestamp() => _timestamp;

    /// <summary>The kind of <see cref="Timestamp()"/> the record carries.</summary>
    /// <returns>The kind of <see cref="Timestamp()"/> the record carries.</returns>
    public TimestampType TimestampType() => _timestampType;

    /// <summary>
    /// The deserialized key, or <c>default(TKey)</c> when the key is absent; the deserializer is not called then.
    /// </summary>
    /// <returns>The deserialized key, or <c>default(TKey)</c> when the key is absent; the deserializer is not
    /// called then.</returns>
    public TKey? Key() => _key;

    /// <summary>
    /// The deserialized value, or <c>default(TValue)</c> for a tombstone; the deserializer is not called then.
    /// </summary>
    /// <returns>The deserialized value, or <c>default(TValue)</c> for a tombstone; the deserializer is not called
    /// then.</returns>
    public TValue? Value() => _value;

    /// <summary>The record headers; empty when the record has none.</summary>
    /// <returns>The record headers; empty when the record has none.</returns>
    public Headers Headers() => _headers;

    /// <summary>
    /// The record's leader epoch, or <see langword="null"/> when the record format has none (Java's
    /// <c>Optional&lt;Integer&gt; leaderEpoch()</c>).
    /// </summary>
    /// <returns>The record's leader epoch, or <see langword="null"/> when the record format has none.</returns>
    public int? LeaderEpoch() => _leaderEpoch;

    /// <summary>
    /// The size of the serialized, uncompressed key in bytes, or <c>-1</c> if the key is
    /// <see langword="null"/> (Java's <c>serializedKeySize()</c>).
    /// </summary>
    /// <returns>The size of the serialized, uncompressed key in bytes, or <c>-1</c> if the key is
    /// <see langword="null"/>.</returns>
    public int SerializedKeySize() => _serializedKeySize;

    /// <summary>
    /// The size of the serialized, uncompressed value in bytes, or <c>-1</c> if the value is
    /// <see langword="null"/> (Java's <c>serializedValueSize()</c>).
    /// </summary>
    /// <returns>The size of the serialized, uncompressed value in bytes, or <c>-1</c> if the value is
    /// <see langword="null"/>.</returns>
    public int SerializedValueSize() => _serializedValueSize;

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> form: the timestamp is named by Java's
    /// <c>TimestampType</c> text (<c>CreateTime</c>), and the delivery count, which only a share group's records
    /// carry, is <c>null</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        "ConsumerRecord(topic = " + _topic
        + ", partition = " + _partition.ToString(CultureInfo.InvariantCulture)
        + ", leaderEpoch = " + JavaText.ValueOf(_leaderEpoch)
        + ", offset = " + _offset.ToString(CultureInfo.InvariantCulture)
        + ", " + TimestampTypeMethods.ToString(_timestampType)
        + " = " + _timestamp.ToString(CultureInfo.InvariantCulture)
        + ", deliveryCount = null"
        + ", serialized key size = " + _serializedKeySize.ToString(CultureInfo.InvariantCulture)
        + ", serialized value size = " + _serializedValueSize.ToString(CultureInfo.InvariantCulture)
        + ", headers = " + _headers
        + ", key = " + JavaText.ValueOf(_key)
        + ", value = " + JavaText.ValueOf(_value) + ")";
}
