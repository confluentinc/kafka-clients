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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The internal bytes-carrier for the send path — the serialized shape the bytes-based
/// <see cref="NativeProducer"/> send methods consume (M11/P5, PLAN §3.3/§5.1). It is the
/// former public bytes <c>ProducerRecord</c> shape, demoted to <c>internal</c> when the
/// producer surface went generic-only: <see cref="ProducerRecord{TKey, TValue}"/> is now the
/// public type, its <c>TKey</c> / <c>TValue</c> serialized to bytes <b>above</b> this carrier
/// (CLAUDE.md §11 — serialize in the binding, before the P/Invoke), and this struct holds the
/// exact field set the native send path reads: <c>topic</c> / <c>partition</c> /
/// <c>timestamp</c> / <c>key</c> / <c>value</c>.
/// </summary>
/// <remarks>
/// <para>
/// A <c>readonly struct</c>, not a class, so it never hits the managed heap per send — the only
/// per-record allocation on the typed send path is the serializer's output <c>byte[]</c>
/// (PLAN §3.6, DoD §10). <see cref="Key"/> / <see cref="Value"/> wrap the serializer output with
/// <b>no extra copy</b> (a <see cref="ReadOnlyMemory{Byte}"/> over the returned array), preserving
/// the send-path zero-copy contract (ffi §A4).
/// </para>
/// <para>
/// <b>Absent vs present.</b> A <see langword="null"/> <see cref="Key"/> / <see cref="Value"/> means
/// the serializer returned <see langword="null"/> (a no-key / tombstone), which the native send path
/// maps to the ABI's absent sentinel (<c>IntPtr.Zero</c> + <c>len -1</c>); a present (non-null)
/// <see cref="ReadOnlyMemory{Byte}"/> — even zero-length — maps to a present send (ffi §A4).
/// </para>
/// </remarks>
internal readonly struct SerializedProducerRecord
{
    /// <summary>
    /// Initializes a new carrier from already-serialized fields.
    /// </summary>
    /// <param name="topic">The destination topic (non-null; validated by the public record ctor).</param>
    /// <param name="partition">The target partition, or <see langword="null"/> to let the producer choose.</param>
    /// <param name="timestamp">The timestamp in ms, or <see langword="null"/> to let the producer stamp it.</param>
    /// <param name="key">The serialized key, or <see langword="null"/> for no key.</param>
    /// <param name="value">The serialized value, or <see langword="null"/> for a tombstone.</param>
    internal SerializedProducerRecord(
        string topic,
        int? partition,
        long? timestamp,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value)
    {
        Topic = topic;
        Partition = partition;
        Timestamp = timestamp;
        Key = key;
        Value = value;
    }

    /// <summary>The destination topic.</summary>
    internal string Topic { get; }

    /// <summary>The target partition, or <see langword="null"/> to let the producer choose one.</summary>
    internal int? Partition { get; }

    /// <summary>The record timestamp in ms since epoch, or <see langword="null"/> to let the producer stamp it.</summary>
    internal long? Timestamp { get; }

    /// <summary>The serialized key, or <see langword="null"/> for no key (absent).</summary>
    internal ReadOnlyMemory<byte>? Key { get; }

    /// <summary>The serialized value, or <see langword="null"/> for a tombstone (absent).</summary>
    internal ReadOnlyMemory<byte>? Value { get; }

    /// <summary>
    /// Serializes a public <see cref="ProducerRecord{TKey, TValue}"/> into this bytes-carrier
    /// (M11/P5, PLAN §3.4/§3.5). The serializer is <b>always invoked</b>, even for a null
    /// <c>TKey</c> / <c>TValue</c> (Java-faithful — <c>KafkaProducer.doSend</c> calls
    /// <c>serialize(topic, …, record.key())</c> unconditionally), and its <c>byte[]?</c> return
    /// drives the absent (<see langword="null"/>) / present sentinel: a <see langword="null"/>
    /// result becomes an absent field, any non-null (even empty) result a present one.
    /// </summary>
    /// <remarks>
    /// <b>Mandatory <see cref="SerializationException"/> wrap (PLAN §3.5).</b> Any throw from a
    /// user serializer is wrapped in a <see cref="SerializationException"/> carrying the topic
    /// context and the original as the inner exception — Java wraps serializer failures in
    /// <c>SerializationException</c>. This mirrors the consumer's M6/P1b deserialize wrap
    /// (<c>ConsumerRecordsMarshal.DeserializeField</c>): a built-in serde that already throws a
    /// <see cref="SerializationException"/> is wrapped verbatim so every serializer failure gets
    /// the uniform topic-context wrap. Unlike the consumer, this runs on the caller's thread
    /// (pre-native, both sync and async send) — there is no foreign-thread UB concern; the wrap is
    /// for Java-contract fidelity, and it therefore throws <b>synchronously</b> for both the sync
    /// and async <c>Send</c> (the async <c>Send</c> serializes inline before enqueuing to the pump,
    /// exactly as it already throws its <c>ArgumentNullException</c> precondition synchronously).
    /// </remarks>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="record">The public record to serialize (non-null; validated by the caller).</param>
    /// <param name="keySerializer">The key serializer.</param>
    /// <param name="valueSerializer">The value serializer.</param>
    /// <returns>The serialized carrier.</returns>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value.</exception>
    internal static SerializedProducerRecord Serialize<TKey, TValue>(
        ProducerRecord<TKey, TValue> record,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer)
    {
        byte[]? keyBytes = SerializeField(keySerializer, record.Topic, record.Key, isKey: true);
        byte[]? valueBytes = SerializeField(valueSerializer, record.Topic, record.Value, isKey: false);

        return new SerializedProducerRecord(
            record.Topic,
            record.Partition,
            record.Timestamp,
            // null → absent; a non-null array wraps zero-copy into ReadOnlyMemory (no copy).
            keyBytes is null ? (ReadOnlyMemory<byte>?)null : keyBytes,
            valueBytes is null ? (ReadOnlyMemory<byte>?)null : valueBytes);
    }

    /// <summary>
    /// Invokes one serializer (always, even on a null <paramref name="data"/> — PLAN §3.4) and
    /// wraps any throw in a <see cref="SerializationException"/> with topic context (PLAN §3.5).
    /// </summary>
    private static byte[]? SerializeField<T>(ISerializer<T> serializer, string topic, T? data, bool isKey)
    {
        try
        {
            // Invoke-on-null is Java-faithful (PLAN §3.4): the serializer is handed the possibly-null
            // key/value unchanged. The `!` reflects that `ISerializer<T>.Serialize` annotates its
            // parameter `T` (non-null), yet its own contract explicitly permits — and maps to null
            // bytes — a null input (Java's `Serializer` returns `null` for `null` data).
            return serializer.Serialize(topic, data!);
        }
        catch (Exception exception)
        {
            // MANDATORY catch-and-wrap (PLAN §3.5): any user-serializer throw becomes a
            // SerializationException carrying the topic context, the original as the inner
            // exception. Wrapped verbatim (mirrors the consumer's DeserializeField), so a serde
            // that already throws SerializationException still gets the uniform topic-context wrap.
            throw new SerializationException(
                $"Error serializing {(isKey ? "key" : "value")} for topic '{topic}'.",
                exception);
        }
    }
}
