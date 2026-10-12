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
/// A broker-free <b>synchronous</b>, typed Kafka producer for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.MockProducer&lt;K, V&gt;</c>. A thin, Java-shaped forwarder over
/// the internal <see cref="NativeProducer"/> (like <see cref="KafkaProducer{TKey, TValue}"/>), the sync
/// sibling of <see cref="AsyncMockProducer{TKey, TValue}"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Mock-takes-serializers is Java-faithful (M11/P5, PLAN §7).</b> Java's <c>MockProducer</c> takes
/// <c>Serializer&lt;K&gt;</c> / <c>Serializer&lt;V&gt;</c> and serializes records into its
/// <c>history</c>. This mock does the same — <c>Send</c> serializes the key / value to bytes
/// exactly like the real client, then forwards to the bytes-based native mock (<see cref="HistoryCount()"/>
/// reflects the count).
/// </para>
/// <para>
/// <b>Send + the send-control helpers.</b> Beyond the <see cref="IProducer{TKey, TValue}"/> surface
/// (<c>Send</c> / <see cref="Flush"/> / <see cref="PartitionsFor"/> / <see cref="Close"/>),
/// this mock exposes the Java <c>MockProducer</c> send-driving helpers as inherent methods on the
/// concrete type (not on the interface) — <see cref="CompleteNext"/> / <see cref="ErrorNext"/> /
/// <see cref="HistoryCount()"/> / <see cref="Clear"/> — mirroring <see cref="AsyncMockProducer{TKey, TValue}"/>
/// and the consumer's mock-only precedent.
/// </para>
/// <para>
/// <b>Delivery callbacks come for free, and that is Java-parity (M14/P1 decision D10).</b>
/// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback)"/> shares the real client's
/// plumbing, and Java's own <c>MockProducer</c> keeps the per-record callbacks and fires them from
/// <c>completeNext()</c> / <c>errorNext()</c> — which is behaviourally what
/// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> do here, through the core, with the timing
/// deviation recorded below. No mock-specific callback helper is added.
/// </para>
/// <para>
/// <b>Manual (<c>autoComplete: false</c>) sends take one thread (M11/P4.2 decision D13).</b>
/// <c>Send</c> returns once the mock has accepted the record, and the returned future's
/// <see cref="KafkaFuture{T}.Get"/> blocks until <see cref="CompleteNext"/> / <see cref="ErrorNext"/>
/// resolves the send, so <c>var f = mock.Send(r); mock.CompleteNext(); f.Get();</c> works on one
/// thread. ⚠ Recorded deviation: Java's <c>completeNext</c> fires the callback and completes the
/// future before it returns; here both follow on the producer's send-completion pump a moment after
/// <see cref="CompleteNext"/> returns, as on <see cref="AsyncMockProducer{TKey, TValue}"/>. So call
/// <c>Get()</c> before asserting on a callback: the callback has run by the time <c>Get()</c>
/// returns. With <c>autoComplete: true</c> (the default) each send resolves on its own.
/// </para>
/// <para>
/// <b>Honest reachability caveat — <see cref="PartitionsFor"/> returns an EMPTY list.</b> The only
/// mock ctor builds an empty cluster, so <see cref="PartitionsFor"/> succeeds broker-free but returns
/// an empty <see cref="IReadOnlyList{PartitionInfo}"/> for every topic. A populated list is
/// integration-only (a real <see cref="KafkaProducer{TKey, TValue}"/> does live metadata). This is a
/// success with an empty result, not a fault.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class MockProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly NativeProducer _native;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;

    /// <summary>
    /// Creates a broker-free mock producer with the key / value serializers (Java-faithful — Java's
    /// <c>MockProducer</c> takes serializers, PLAN §7).
    /// </summary>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <param name="autoComplete">
    /// When <see langword="true"/> (the default), the mock resolves each send automatically. When
    /// <see langword="false"/>, <c>Send</c> still returns at once, and the returned future's
    /// <see cref="KafkaFuture{T}.Get"/> blocks until <see cref="CompleteNext"/> /
    /// <see cref="ErrorNext"/> resolves the send — which the sending thread may itself call first
    /// (M11/P4.2 decision D13). Flush / close on a mock resolve broker-free regardless of this flag.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null.
    /// </exception>
    public MockProducer(
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        bool autoComplete = true)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.CreateMock(autoComplete);
    }

    /// <inheritdoc/>
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record)
    {
        // Serialize above the bytes core (identical to the real client — Java-faithful, PLAN §7):
        // null-record precondition first (ffi §A5).
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        return SendValidated(record, callback: null);
    }

    /// <inheritdoc/>
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback)
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
    private KafkaFuture<RecordMetadata> SendValidated(ProducerRecord<TKey, TValue> record, IDeliveryCallback? callback)
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

        // Serialize on THIS thread (a serializer throw surfaces synchronously — and per D5 fires no
        // callback), then forward the bytes carrier to the native send.
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

    /// <summary>
    /// Completes the next pending send successfully (Java <c>MockProducer.completeNext()</c> /
    /// Python <c>complete_next()</c>) — for a mock created with <c>autoComplete: false</c>. The send's
    /// callback and its future's completion follow on the producer's send-completion pump a moment
    /// after this returns, whereas Java's <c>completeNext</c> does both before returning (M11/P4.2
    /// decision D13), so call the future's <see cref="KafkaFuture{T}.Get"/> before asserting on a
    /// callback. The thread that sent may call this itself. Inherent on the concrete mock (not on
    /// <see cref="IProducer{TKey, TValue}"/>), mirroring the consumer's mock-only helpers.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Completes the next pending send with an error (Java <c>MockProducer.errorNext(...)</c> /
    /// Python <c>error_next(code, message)</c>) — for a mock created with <c>autoComplete: false</c>.
    /// The send's future's <see cref="KafkaFuture{T}.Get"/> then throws a <see cref="KafkaException"/>
    /// carrying <paramref name="code"/> and <paramref name="message"/> (or the default message for the
    /// code when <paramref name="message"/> is <see langword="null"/>). As with
    /// <see cref="CompleteNext"/>, the callback and the future's completion follow on the pump a
    /// moment after this returns, and the thread that sent may call this itself (M11/P4.2 decision
    /// D13).
    /// </summary>
    /// <param name="code">The Kafka error code to complete the send with.</param>
    /// <param name="message">The error message, or <see langword="null"/> for the code's default message.</param>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool ErrorNext(int code, string? message = null) => _native.MockErrorNext(code, message);

    /// <summary>
    /// The number of records in the sent history (Java <c>MockProducer.history().size()</c> /
    /// Python <c>history_count()</c>). A <b>method</b>, not a property — per the FDG precedent
    /// (the consumer's <c>Assignment()</c> / <c>Subscription()</c> / <c>Paused()</c> are methods
    /// because each does a P/Invoke and can throw) and Python <c>history_count()</c> parity: this
    /// P/Invokes <c>MockProducerHistoryCount</c> and can throw <see cref="ObjectDisposedException"/>.
    /// The ABI exposes only a count, so Java's <c>history()</c> record list is not surfaced
    /// (CLAUDE.md §3).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public int HistoryCount() => _native.MockHistoryCount();

    /// <summary>
    /// Clears the sent history and any pending completions (Java <c>MockProducer.clear()</c> /
    /// Python <c>clear()</c>).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public void Clear() => _native.MockClear();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
