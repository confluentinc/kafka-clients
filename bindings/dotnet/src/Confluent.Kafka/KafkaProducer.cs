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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// The real <b>synchronous</b>, typed Kafka producer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.KafkaProducer&lt;K, V&gt;</c> (which is synchronous). A thin,
/// Java-shaped forwarder over the internal <see cref="NativeProducer"/> lifecycle wrapper (the same
/// wrapper the async <see cref="AsyncKafkaProducer{TKey, TValue}"/> composes — the two client families
/// are siblings over one native producer, not one wrapping the other) plus the two serializers. All
/// Kafka logic lives in the Rust core; this type only restores the Java (blocking) shape and
/// serializes <typeparamref name="TKey"/> / <typeparamref name="TValue"/> to bytes above the core.
/// </summary>
/// <remarks>
/// <para>
/// <b>Blocking, direct sync C ABI (inherited from <see cref="NativeProducer"/>).</b> Each operation
/// calls the synchronous C ABI directly; the core's blocking call parks the caller thread inside the
/// Rust multi-thread runtime (deadlock-free, ffi §A1) — not sync-over-async. The sync producer starts
/// <b>no</b> completion pump (only the async
/// <see cref="AsyncKafkaProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, System.Threading.CancellationToken)"/>
/// does), so a sync-only producer spins no background thread.
/// </para>
/// <para>
/// <b>Serialize above the bytes core (M11/P5, CLAUDE.md §11).</b> <c>Send</c> serializes the
/// record's key / value to bytes on the caller's thread (before the P/Invoke) and forwards the
/// internal bytes carrier to the blocking
/// <see cref="NativeProducer.Send(Internal.SerializedProducerRecord, Internal.DeliveryRegistration)"/>.
/// A serializer throw is wrapped in a <see cref="SerializationException"/> (Java-faithful).
/// </para>
/// <para>
/// <b>Both of Java's <c>send</c> signatures (M14/P1).</b>
/// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback)"/> adds Java's
/// <c>send(record, Callback)</c>; on this synchronous surface the callback runs <b>inline on the
/// calling thread</b>, before <c>Send</c> returns or throws. See <see cref="IDeliveryCallback"/>.
/// </para>
/// <para>
/// <b>Concurrent <c>Send</c> is supported — do NOT add your own lock (M11/P8, Minor 13).</b>
/// The Rust core serializes through the producer's internal <c>Mutex</c> (ffi §A1: "concurrent
/// <c>Send</c> is safe — don't add your own lock"; a binding-side send lock is listed there as an
/// anti-pattern). The <b>single-owner</b> constraint applies to <b>teardown</b>: <see cref="Close"/>
/// and <see cref="Dispose"/> are one-shot and latched, so do not race teardown against itself.
/// (This paragraph previously claimed the producer was "not thread-safe"; that was false and is
/// corrected here — see <see cref="IProducer{TKey, TValue}"/> for the full note.)
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="Close"/> is the explicit graceful close that <em>surfaces</em> a close
/// failure; <see cref="Dispose"/> is the teardown that swallows it. Both are idempotent and gated by
/// a single atomic closed flag. There is no <c>DisposeAsync</c> — this is the synchronous surface.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class KafkaProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly NativeProducer _native;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;

    /// <summary>
    /// Creates a real producer from a configuration map and the key / value serializers (Java
    /// <c>KafkaProducer(Map, Serializer&lt;K&gt;, Serializer&lt;V&gt;)</c>). Config keys are the Java
    /// dotted names (e.g. <c>bootstrap.servers</c>); values are strings.
    /// </summary>
    /// <param name="config">The producer configuration.</param>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="config"/>, <paramref name="keySerializer"/>, or
    /// <paramref name="valueSerializer"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    public KafkaProducer(
        IReadOnlyDictionary<string, string> config,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.Create(config);
    }

    /// <inheritdoc/>
    public RecordMetadata Send(ProducerRecord<TKey, TValue> record)
    {
        // Precondition (ffi §A5): null record BEFORE any serialize / P-Invoke.
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        return SendValidated(record, callback: null);
    }

    /// <inheritdoc/>
    public RecordMetadata Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback)
    {
        // Preconditions (ffi §A5) in Java's own order: the RECORD first — Java's doSend is reached
        // through interceptors.onSend(record), which touches the record before the callback is ever
        // examined — then the CALLBACK (M14/P1 decision D8: rejected although Java accepts null,
        // because the one-arg overload already spells "no callback" and the parameter is non-nullable
        // under #nullable enable). Both are thrown BEFORE the closed check, the serialize and any
        // P/Invoke, so neither a disposed producer nor a stateful serializer can mask them — and
        // per D5 a throw out of Send means the callback is NOT invoked.
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        return SendValidated(record, callback);
    }

    /// <summary>
    /// The shared send skin behind both <c>Send</c> overloads (M14/P1): closed-check → serialize →
    /// forward, building the delivery-callback carrier only when one was supplied. Each overload
    /// validates its own arguments first, so this deliberately does <b>not</b> re-check them (a
    /// second guard would shadow the real one).
    /// </summary>
    private RecordMetadata SendValidated(ProducerRecord<TKey, TValue> record, IDeliveryCallback? callback)
    {
        // Closed-check BEFORE the serialize (M11/P8, Minor 8), Java-faithful: Java's
        // KafkaProducer.doSend calls throwIfProducerClosed() before serializing the key/value.
        // Without it a STATEFUL serializer runs for a record that can never be sent — a Schema
        // Registry serializer REGISTERS A SCHEMA as a side effect — and a serializer throw masks
        // the real ObjectDisposedException behind a SerializationException. The order is
        // null-check -> closed-check -> serialize: the null check stays first so the documented
        // precondition order (pinned by Send_NullRecord_ThrownBeforeDisposedCheck_EvenWhenClosed)
        // is unchanged. The native layer re-checks; that is the race re-check, not a duplicate.
        _native.ThrowIfClosed();

        // Serialize the key/value to bytes on THIS thread (a serializer throw surfaces synchronously
        // as a SerializationException — Java-faithful, and per D5 fires no callback), then forward
        // the bytes carrier to the blocking send.
        SerializedProducerRecord serialized =
            SerializedProducerRecord.Serialize(record, _keySerializer, _valueSerializer);

        // The carrier holds the two fields Java's -1 placeholder metadata needs (D6): the topic, and
        // the record's EXPLICIT partition or -1 when it let the producer choose. Allocated only on
        // the callback-taking path (D11).
        DeliveryRegistration? delivery = callback is null
            ? null
            : new DeliveryRegistration(callback, record.Topic, record.Partition ?? -1);

        return _native.Send(serialized, delivery);
    }

    /// <inheritdoc/>
    public void Flush() => _native.Flush();

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _native.PartitionsFor(topic);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <inheritdoc/>
    public void Close() => _native.Close();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
