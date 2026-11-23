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
using System.Threading;

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A key/value pair to be received from Kafka, with the topic name and partition number it is received from and its
/// offset in that partition, Java's <c>org.apache.kafka.clients.consumer.ConsumerRecord&lt;K, V&gt;</c>.
/// </summary>
/// <remarks>
/// <para>
/// A polled record's key and value are the deserializers' output, and its topic is an owned copy. Its headers are read
/// from the poll's native batch on their first use, and its other fields on each call, so the record keeps the batch in
/// memory while it is reachable, after later polls and after the consumer is disposed, and the garbage collector frees
/// it.
/// </para>
/// <para>
/// A deserializer that may keep its memory,
/// <see cref="Confluent.Kafka.Common.Serialization.IDeserializerWithReadOnlyMemory{T}"/> on a synchronous consumer and
/// <see cref="Confluent.Kafka.Common.Serialization.IAsyncDeserializerWithRetention{T}"/> on an asynchronous one, may
/// return a memory over the batch's own bytes, as <c>Serdes.ReadOnlyMemory()</c>'s and
/// <see cref="Confluent.Kafka.Common.Serialization.AsyncReadOnlyMemoryDeserializer"/> do. The batch then stays in
/// memory while such a key or value is reachable, with or without its record, after later polls and after the
/// consumer is disposed, and the garbage collector frees it. A span taken from such a
/// memory is valid only while the memory is reachable: code that keeps only the span, or a pointer into it, holds a
/// pin for that time, <c>using MemoryHandle pin = memory.Pin();</c>.
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
    // A polled record's headers are made on the first call of Headers() unless a deserializer took them during the
    // poll; a constructed record's are set by its constructor.
    private IHeaders? _headers;
    private readonly int? _leaderEpoch;
    private readonly int _serializedKeySize;
    private readonly int _serializedValueSize;
    private readonly short? _deliveryCount;

    // The native batch a polled record reads its fields from, and the record in it; null for a constructed record.
    private readonly SafeConsumerRecordsHandle? _batch;
    private readonly IntPtr _record;

    /// <summary>
    /// Creates a record to be received from a specified topic and partition (provided for compatibility with Kafka 0.9
    /// before the message format supported timestamps and before serialized metadata were exposed).
    /// </summary>
    /// <param name="topic">The topic this record is received from.</param>
    /// <param name="partition">The partition of the topic this record is received from.</param>
    /// <param name="offset">The offset of this record in the corresponding Kafka partition.</param>
    /// <param name="key">The key of the record, if one exists (<see langword="null"/> is allowed).</param>
    /// <param name="value">The record contents.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="topic"/> is <see langword="null"/>.</exception>
    public ConsumerRecord(string topic, int partition, long offset, TKey? key, TValue? value)
        : this(
            topic,
            partition,
            offset,
            NO_TIMESTAMP,
            Common.Record.TimestampType.NO_TIMESTAMP_TYPE,
            NULL_SIZE,
            NULL_SIZE,
            key,
            value,
            new RecordHeaders(),
            null)
    {
    }

    /// <summary>Creates a record to be received from a specified topic and partition.</summary>
    /// <param name="topic">The topic this record is received from.</param>
    /// <param name="partition">The partition of the topic this record is received from.</param>
    /// <param name="offset">The offset of this record in the corresponding Kafka partition.</param>
    /// <param name="timestamp">The timestamp of the record.</param>
    /// <param name="timestampType">The timestamp type.</param>
    /// <param name="serializedKeySize">The length of the serialized key.</param>
    /// <param name="serializedValueSize">The length of the serialized value.</param>
    /// <param name="key">The key of the record, if one exists (<see langword="null"/> is allowed).</param>
    /// <param name="value">The record contents.</param>
    /// <param name="headers">The headers of the record, kept as given.</param>
    /// <param name="leaderEpoch">
    /// The leader epoch of the record, or <see langword="null"/> (it may be absent for legacy record formats).
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="topic"/> or <paramref name="headers"/> is <see langword="null"/>; the topic is checked first.
    /// </exception>
    public ConsumerRecord(
        string topic,
        int partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        int serializedKeySize,
        int serializedValueSize,
        TKey? key,
        TValue? value,
        IHeaders headers,
        int? leaderEpoch)
        : this(
            topic,
            partition,
            offset,
            timestamp,
            timestampType,
            serializedKeySize,
            serializedValueSize,
            key,
            value,
            headers,
            leaderEpoch,
            null)
    {
    }

    /// <summary>Creates a record to be received from a specified topic and partition.</summary>
    /// <param name="topic">The topic this record is received from.</param>
    /// <param name="partition">The partition of the topic this record is received from.</param>
    /// <param name="offset">The offset of this record in the corresponding Kafka partition.</param>
    /// <param name="timestamp">The timestamp of the record.</param>
    /// <param name="timestampType">The timestamp type.</param>
    /// <param name="serializedKeySize">The length of the serialized key.</param>
    /// <param name="serializedValueSize">The length of the serialized value.</param>
    /// <param name="key">The key of the record, if one exists (<see langword="null"/> is allowed).</param>
    /// <param name="value">The record contents.</param>
    /// <param name="headers">The headers of the record, kept as given.</param>
    /// <param name="leaderEpoch">
    /// The leader epoch of the record, or <see langword="null"/> (it may be absent for legacy record formats).
    /// </param>
    /// <param name="deliveryCount">
    /// The delivery count of the record, or <see langword="null"/> (it may be absent when deliveries are not counted).
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="topic"/> or <paramref name="headers"/> is <see langword="null"/>; the topic is checked first.
    /// </exception>
    public ConsumerRecord(
        string topic,
        int partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        int serializedKeySize,
        int serializedValueSize,
        TKey? key,
        TValue? value,
        IHeaders headers,
        int? leaderEpoch,
        short? deliveryCount)
    {
        _topic = topic ?? throw new IllegalArgumentException("Topic cannot be null");
        _headers = headers ?? throw new IllegalArgumentException("Headers cannot be null");
        _partition = partition;
        _offset = offset;
        _timestamp = timestamp;
        _timestampType = timestampType;
        _serializedKeySize = serializedKeySize;
        _serializedValueSize = serializedValueSize;
        _key = key;
        _value = value;
        _leaderEpoch = leaderEpoch;
        _deliveryCount = deliveryCount;
    }

    // A polled record of `batch`, whose fields other than these are read from `record` on each call.
    internal ConsumerRecord(
        SafeConsumerRecordsHandle batch,
        IntPtr record,
        string topic,
        int partition,
        TKey? key,
        TValue? value,
        RecordHeaders? headers)
    {
        _batch = batch;
        _record = record;
        _topic = topic;
        _partition = partition;
        _key = key;
        _value = value;
        _headers = headers;
    }

    /// <summary>The topic name.</summary>
    /// <returns>The topic name.</returns>
    public string Topic() => _topic;

    /// <summary>The partition the record was read from.</summary>
    /// <returns>The partition the record was read from.</returns>
    public int Partition() => _partition;

    /// <summary>The record's offset within its partition.</summary>
    /// <returns>The record's offset within its partition.</returns>
    public long Offset()
    {
        if (_batch is null)
        {
            return _offset;
        }

        long offset = ConsumerRecordMarshal.Offset(_record);
        GC.KeepAlive(_batch);
        return offset;
    }

    /// <summary>
    /// The record timestamp in milliseconds since the epoch, or
    /// <see cref="NO_TIMESTAMP"/> (<c>-1</c>) when absent.
    /// </summary>
    /// <returns>The record timestamp in milliseconds since the epoch, or <see cref="NO_TIMESTAMP"/> (<c>-1</c>)
    /// when absent.</returns>
    public long Timestamp()
    {
        if (_batch is null)
        {
            return _timestamp;
        }

        long timestamp = ConsumerRecordMarshal.Timestamp(_record);
        GC.KeepAlive(_batch);
        return timestamp;
    }

    /// <summary>The kind of <see cref="Timestamp()"/> the record carries.</summary>
    /// <returns>The kind of <see cref="Timestamp()"/> the record carries.</returns>
    public TimestampType TimestampType()
    {
        if (_batch is null)
        {
            return _timestampType;
        }

        Common.Record.TimestampType timestampType = ConsumerRecordMarshal.TimestampTypeOf(_record);
        GC.KeepAlive(_batch);
        return timestampType;
    }

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
    public IHeaders Headers() => _headers ?? CreateHeaders();

    /// <summary>
    /// The record's leader epoch, or <see langword="null"/> when the record format has none (Java's
    /// <c>Optional&lt;Integer&gt; leaderEpoch()</c>).
    /// </summary>
    /// <returns>The record's leader epoch, or <see langword="null"/> when the record format has none.</returns>
    public int? LeaderEpoch()
    {
        if (_batch is null)
        {
            return _leaderEpoch;
        }

        int? leaderEpoch = ConsumerRecordMarshal.LeaderEpoch(_record);
        GC.KeepAlive(_batch);
        return leaderEpoch;
    }

    /// <summary>
    /// The delivery count of the record, if available (Java's <c>Optional&lt;Short&gt; deliveryCount()</c>).
    /// Deliveries are counted for records delivered by share groups.
    /// </summary>
    /// <returns>
    /// The delivery count, or <see langword="null"/> when deliveries are not counted, as for every record this
    /// consumer polls.
    /// </returns>
    public short? DeliveryCount()
    {
        if (_batch is null)
        {
            return _deliveryCount;
        }

        short? deliveryCount = ConsumerRecordMarshal.DeliveryCount(_record);
        GC.KeepAlive(_batch);
        return deliveryCount;
    }

    /// <summary>
    /// The size of the serialized, uncompressed key in bytes, or <c>-1</c> if the key is
    /// <see langword="null"/> (Java's <c>serializedKeySize()</c>).
    /// </summary>
    /// <returns>The size of the serialized, uncompressed key in bytes, or <c>-1</c> if the key is
    /// <see langword="null"/>.</returns>
    public int SerializedKeySize()
    {
        if (_batch is null)
        {
            return _serializedKeySize;
        }

        int size = ConsumerRecordMarshal.SerializedKeySize(_record);
        GC.KeepAlive(_batch);
        return size;
    }

    /// <summary>
    /// The size of the serialized, uncompressed value in bytes, or <c>-1</c> if the value is
    /// <see langword="null"/> (Java's <c>serializedValueSize()</c>).
    /// </summary>
    /// <returns>The size of the serialized, uncompressed value in bytes, or <c>-1</c> if the value is
    /// <see langword="null"/>.</returns>
    public int SerializedValueSize()
    {
        if (_batch is null)
        {
            return _serializedValueSize;
        }

        int size = ConsumerRecordMarshal.SerializedValueSize(_record);
        GC.KeepAlive(_batch);
        return size;
    }

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> form: the timestamp is named by Java's
    /// <c>TimestampType</c> text (<c>CreateTime</c>), and an absent leader epoch or delivery count is <c>null</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        "ConsumerRecord(topic = " + _topic
        + ", partition = " + _partition.ToString(CultureInfo.InvariantCulture)
        + ", leaderEpoch = " + JavaText.ValueOf(LeaderEpoch())
        + ", offset = " + Offset().ToString(CultureInfo.InvariantCulture)
        + ", " + TimestampTypeMethods.ToString(TimestampType())
        + " = " + Timestamp().ToString(CultureInfo.InvariantCulture)
        + ", deliveryCount = " + JavaText.ValueOf(DeliveryCount())
        + ", serialized key size = " + SerializedKeySize().ToString(CultureInfo.InvariantCulture)
        + ", serialized value size = " + SerializedValueSize().ToString(CultureInfo.InvariantCulture)
        + ", headers = " + Headers()
        + ", key = " + JavaText.ValueOf(_key)
        + ", value = " + JavaText.ValueOf(_value) + ")";

    // One instance per record, whichever thread asks first.
    private IHeaders CreateHeaders()
    {
        IHeaders headers = new RecordHeaders(_batch!, _record);
        return Interlocked.CompareExchange(ref _headers, headers, null) ?? headers;
    }
}
