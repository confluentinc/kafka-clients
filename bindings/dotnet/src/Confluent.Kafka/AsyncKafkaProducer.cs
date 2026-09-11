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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// The real, typed Kafka producer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.KafkaProducer&lt;K, V&gt;</c>. A thin, Java-shaped forwarder
/// over the internal <see cref="NativeProducer"/> lifecycle wrapper (which owns the native handle
/// and the completion bridge, ffi-marshalling.md §A) plus the two serializers. All Kafka logic
/// lives in the Rust core; this type only restores the Java shape and serializes
/// <typeparamref name="TKey"/> / <typeparamref name="TValue"/> to bytes above the bytes-based core.
/// </summary>
/// <remarks>
/// <para>
/// <b>Send + async peripherals.</b> This producer implements the <see cref="IAsyncProducer{TKey, TValue}"/>
/// surface — <c>Send</c> (the M11/P3 send path over the inline pull-pump, ffi §A7 Option C,
/// with the M11/P5 typed serialize skin above it) plus the M11/P2 peripherals <see cref="Flush"/> /
/// <see cref="Close(CancellationToken)"/> / <see cref="PartitionsFor"/>. Transactions remain deferred.
/// </para>
/// <para>
/// <b>Serialize above the bytes core (M11/P5, CLAUDE.md §11).</b> <c>Send</c> serializes the
/// record's key / value to bytes on the caller's thread (before the P/Invoke — no per-record
/// callback through the ABI), then forwards the internal bytes carrier to
/// <see cref="NativeProducer.SendViaPump"/>. A serializer throw is wrapped in a
/// <see cref="SerializationException"/> and raised <b>synchronously</b> (Java-faithful — the async
/// <c>Send</c> serializes inline before enqueuing to the pump, so the wrap surfaces before the
/// <see cref="Task"/> is returned).
/// </para>
/// <para>
/// <b>Both of Java's <c>send</c> signatures (M14/P1).</b>
/// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback, CancellationToken)"/> adds
/// Java's <c>send(record, Callback)</c>; the callback runs on the producer's send-completion pump
/// thread — .NET's analogue of Java's background I/O thread — before the returned
/// <see cref="Task{TResult}"/> is completed. See <see cref="IDeliveryCallback"/>.
/// </para>
/// <para>
/// <b>Disposal — thin forwarders over <see cref="NativeProducer"/> (ffi §A7; M11/P2.1).</b>
/// <see cref="DisposeAsync"/> / <see cref="Dispose"/> / <see cref="Close(CancellationToken)"/> each
/// forward straight to the matching <see cref="NativeProducer"/> teardown flavor, which owns the
/// whole graceful-close → destroy and the one-shot latch (mirroring the consumer's thin forwarders
/// over <c>NativeConsumer</c>). <see cref="DisposeAsync"/> is primary (async close via
/// <c>Producer_close_async</c>, swallows any close error); <see cref="Dispose"/> is the blocking
/// fallback (sync <c>Producer_close</c>, swallows); <see cref="Close(CancellationToken)"/> surfaces
/// a close failure. All are idempotent (the merged latch lives in <see cref="NativeProducer"/>);
/// a close error never prevents the destroy.
/// </para>
/// <para>
/// <b>Cancellation is best-effort (no native abort).</b> The producer has no <c>wakeup()</c>, so
/// a canceled token cancels the returned task's .NET-side wait but does not abort the in-flight
/// native op.
/// </para>
/// <para>
/// ⚠ <b>Do not mutate a record's key / value buffers after <c>Send</c> returns</b> (M11/P3.1
/// decision D6). The async send is deferred — the binding borrows the serialized bytes until a
/// background batch thread hands the record to the core — so a mutation in that window is visible
/// on the wire. See <see cref="IAsyncProducer{TKey, TValue}"/>'s <c>Send</c> for the full note.
/// The <b>synchronous</b> producer has no such window.
/// </para>
/// <para>
/// ⚠ <b><c>Send</c> BLOCKS the calling thread under sustained saturation</b> (M11/P3.3): once the
/// producer holds its bound of accepted-but-unsent records, a send waits for capacity for up to the
/// configured <c>max.block.ms</c>, and if the bound is still full then, the record is refused with
/// a <b>retriable</b> <see cref="KafkaException"/> that faults the send's <see cref="Task"/> and
/// fires its delivery callback — not thrown. Java's own <c>send()</c> contract on both halves, and
/// what bounds this producer's memory and latency —
/// <see cref="IAsyncProducer{TKey, TValue}"/> states it in full. The <b>synchronous</b> producer has
/// no such window either.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class AsyncKafkaProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
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
    public AsyncKafkaProducer(
        IReadOnlyDictionary<string, string> config,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.Create(config);
    }

    /// <inheritdoc/>
    public Task<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default)
    {
        // Precondition (ffi §A5): null record BEFORE any serialize / P-Invoke.
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        return SendValidated(record, callback: null, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<RecordMetadata> Send(
        ProducerRecord<TKey, TValue> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken = default)
    {
        // Preconditions (ffi §A5) in Java's own order: the RECORD first — Java's doSend is reached
        // through interceptors.onSend(record), which touches the record before the callback is ever
        // examined — then the CALLBACK (M14/P1 decision D8: rejected although Java accepts null,
        // because the two-arg overload already spells "no callback" and the parameter is non-nullable
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

        return SendValidated(record, callback, cancellationToken);
    }

    /// <summary>
    /// The shared send skin behind both <c>Send</c> overloads (M14/P1): closed-check → serialize →
    /// forward, building the delivery-callback carrier only when one was supplied. Each overload
    /// validates its own arguments first, so this deliberately does <b>not</b> re-check them (a
    /// second guard would shadow the real one). Not <c>async</c>, so a serializer throw stays
    /// synchronous rather than faulting the returned task.
    /// </summary>
    private Task<RecordMetadata> SendValidated(
        ProducerRecord<TKey, TValue> record,
        IDeliveryCallback? callback,
        CancellationToken cancellationToken)
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
        // the internal bytes carrier to the pump.
        SerializedProducerRecord serialized =
            SerializedProducerRecord.Serialize(record, _keySerializer, _valueSerializer);

        // The carrier holds the two fields Java's -1 placeholder metadata needs (D6): the topic, and
        // the record's EXPLICIT partition or -1 when it let the producer choose. Allocated only on
        // the callback-taking path (D11), so the plain Send stays allocation-identical.
        DeliveryRegistration? delivery = callback is null
            ? null
            : new DeliveryRegistration(callback, record.Topic, record.Partition ?? -1);

        return _native.SendViaPump(serialized, delivery, cancellationToken);
    }

    /// <inheritdoc/>
    public Task Flush(CancellationToken cancellationToken = default) =>
        _native.FlushWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
