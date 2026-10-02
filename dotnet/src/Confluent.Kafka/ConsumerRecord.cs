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
/// A single record read from a poll — the .NET realization of Java's generic
/// <c>org.apache.kafka.clients.consumer.ConsumerRecord&lt;K, V&gt;</c>, clipped to today's
/// ABI. The <see cref="Key"/> / <see cref="Value"/> are the <b>deserialized</b>
/// <typeparamref name="TKey"/> / <typeparamref name="TValue"/> produced during the
/// receive-path poll by the consumer's <see cref="IDeserializer{T}"/>s; the topic and the
/// materialized <see cref="Headers"/> are owned copies made during the copy-out on the
/// (caller or dispatcher) poll thread (ffi-marshalling.md §6.4). <b>Nothing native-backed
/// escapes</b> — the record stays valid after the underlying batch is destroyed,
/// sidestepping the §B4 use-after-free hazard.
/// </summary>
/// <remarks>
/// <para>
/// <b>Generic-only (PLAN M6/P1b, decision B).</b> Java has a single generic
/// <c>ConsumerRecord&lt;K, V&gt;</c> with no bytes-specialized sibling; bytes users write
/// <c>ConsumerRecord&lt;byte[], byte[]&gt;</c> (paired with <see cref="Serdes.ByteArray"/>).
/// This binding matches: there is no non-generic <c>ConsumerRecord</c>.
/// </para>
/// <para>
/// <b>Poll-output-only this phase (PLAN decision 4).</b> There is no public constructor:
/// a <see cref="ConsumerRecord{TKey, TValue}"/> is produced only by
/// <c>IConsumer&lt;TKey, TValue&gt;.Poll(...)</c> /
/// <c>IAsyncConsumer&lt;TKey, TValue&gt;.Poll(...)</c>. A public constructor (and Java's
/// <c>addRecord(ConsumerRecord)</c> mock helper) remain deferred to a later additive phase;
/// <see cref="LeaderEpoch"/> / <see cref="SerializedKeySize"/> /
/// <see cref="SerializedValueSize"/> complete the Java accessor surface (M9/P1). Java's
/// <c>deliveryCount()</c> (a KIP-932 share-group accessor) stays out — Python-parity, PLAN §4.
/// </para>
/// <para>
/// <b>Null / tombstone → <c>default(T)</c> (PLAN decision C, three-state null model).</b>
/// An <b>absent</b> key (no key) or an absent value (a tombstone) surfaces as
/// <c>default(TKey)</c> / <c>default(TValue)</c> <b>without</b> invoking the deserializer:
/// <c>null</c> for a reference type such as <c>byte[]</c> / <c>string</c>, or the
/// zero-value for a value type (e.g. <c>0L</c> for <c>long</c>). A <b>present-but-empty</b>
/// key/value (a zero-length payload) <em>is</em> deserialized (from a zero-length span). To
/// distinguish a tombstone from a legitimate zero value, use a nullable value type — e.g.
/// <c>ConsumerRecord&lt;string, long?&gt;</c>, where an absent value is <c>null</c> and a
/// present <c>0</c> is <c>0L</c>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public sealed class ConsumerRecord<TKey, TValue>
{
    /// <summary>
    /// The timestamp value for a record with no timestamp. The .NET realization of Java's
    /// <c>public static final long NO_TIMESTAMP</c> (<c>ConsumerRecord.java</c>), public so
    /// callers can distinguish an absent timestamp from a real one without hardcoding
    /// <c>-1</c>.
    /// </summary>
    public const long NoTimestamp = -1;

    /// <summary>
    /// Initializes an owned record from values already copied out of (topic / headers) or
    /// deserialized from (key / value) the borrowed native batch. Internal because a
    /// <see cref="ConsumerRecord{TKey, TValue}"/> is poll-output-only this phase (no public
    /// constructor, PLAN decision 4); callers must pass owned values — no borrowed pointer
    /// may be captured (the copy-out contract, ffi §6.4; the key/value are the
    /// deserializer's owned <typeparamref name="TKey"/> / <typeparamref name="TValue"/>).
    /// </summary>
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

    /// <summary>The topic name (an owned copy of the length-delimited slice, §B3).</summary>
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
    /// The deserialized key of type <typeparamref name="TKey"/>, or
    /// <c>default(TKey)</c> when the key is absent (the deserializer is not invoked for an
    /// absent key — PLAN decision C).
    /// </summary>
    public TKey Key { get; }

    /// <summary>
    /// The deserialized value of type <typeparamref name="TValue"/>, or
    /// <c>default(TValue)</c> when the value is absent (a tombstone; the deserializer is not
    /// invoked — PLAN decision C).
    /// </summary>
    public TValue Value { get; }

    /// <summary>The record headers; empty when the record has none.</summary>
    public Headers Headers { get; }

    /// <summary>
    /// The leader epoch for the record if available, or <see langword="null"/> for legacy
    /// record formats (Java's <c>Optional&lt;Integer&gt; leaderEpoch()</c> — present → the
    /// epoch, absent → <see langword="null"/>).
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
