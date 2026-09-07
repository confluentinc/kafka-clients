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
/// A broker-free, typed Kafka producer for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.MockProducer&lt;K, V&gt;</c>. A thin, Java-shaped forwarder
/// over the internal <see cref="NativeProducer"/> (like <see cref="AsyncKafkaProducer{TKey, TValue}"/>),
/// constructed over a <c>MockProducer</c> so <c>Send</c> / <see cref="Flush"/> /
/// <see cref="Close(CancellationToken)"/> / <see cref="PartitionsFor"/> resolve without a broker.
/// </summary>
/// <remarks>
/// <para>
/// <b>Mock-takes-serializers is Java-faithful (M11/P5, PLAN §7).</b> Java's <c>MockProducer</c>
/// takes <c>Serializer&lt;K&gt;</c> / <c>Serializer&lt;V&gt;</c> and serializes records into its
/// <c>history</c>. This mock does the same — <c>Send</c> serializes the key / value to bytes
/// exactly like the real client, then forwards to the bytes-based native mock (which stores bytes;
/// <see cref="HistoryCount()"/> reflects the count). Unlike the consumer, where mock-takes-
/// deserializers was a deviation, this is Java-faithful.
/// </para>
/// <para>
/// <b>Send + the send-control helpers.</b> Beyond the <see cref="IAsyncProducer{TKey, TValue}"/>
/// surface (<c>Send</c> + the peripherals), this mock exposes the Java <c>MockProducer</c>
/// send-driving helpers as inherent methods on the concrete type (not on the interface) —
/// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> / <see cref="HistoryCount()"/> /
/// <see cref="Clear"/> — mirroring the consumer's <c>AddRecord</c> / <c>SetPollError</c> mock-only
/// precedent. With <c>autoComplete: false</c> a send stays pending until
/// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> resolves it.
/// </para>
/// <para>
/// <b>Delivery callbacks come for free, and that is Java-parity (M14/P1 decision D10).</b>
/// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback, CancellationToken)"/> shares the
/// real client's plumbing, and Java's own <c>MockProducer</c> keeps the per-record callbacks and
/// fires them from <c>completeNext()</c> / <c>errorNext()</c> — which is behaviourally what
/// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> do here, through the core. No mock-specific
/// callback helper is added.
/// </para>
/// <para>
/// <b>Honest reachability caveat — <see cref="PartitionsFor"/> returns an EMPTY list.</b> The only
/// mock ctor (<c>MockProducer_new</c>) builds an empty cluster, so <see cref="PartitionsFor"/>
/// succeeds broker-free but returns an empty <see cref="IReadOnlyList{PartitionInfo}"/> for every
/// topic. A populated list is integration-only (a real <see cref="AsyncKafkaProducer{TKey, TValue}"/>
/// does live metadata). This is a success with an empty result, not a fault.
/// </para>
/// <para>
/// <b>Disposal</b> is identical to <see cref="AsyncKafkaProducer{TKey, TValue}"/> — thin forwarders
/// over <see cref="NativeProducer"/> (which owns the graceful close→destroy + the one-shot latch,
/// ffi §A7; M11/P2.1): the mock's <c>close_async</c> / <c>close</c> resolve broker-free.
/// </para>
/// <para>
/// ⚠ <b>Do not mutate a record's key / value buffers after <c>Send</c> returns</b> (M11/P3.1
/// decision D6). The async send is deferred — the binding borrows the serialized bytes until a
/// background batch thread hands the record to the core — so a mutation in that window is visible
/// on the wire. See <see cref="IAsyncProducer{TKey, TValue}"/>'s <c>Send</c> for the full note.
/// The <b>synchronous</b> producer has no such window.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class AsyncMockProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
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
    /// When <see langword="true"/> (the default), the mock resolves each <c>Send</c>
    /// automatically. When <see langword="false"/>, a send stays pending until
    /// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> resolves it. Flush / close on a mock
    /// resolve broker-free regardless of this flag.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null.
    /// </exception>
    public AsyncMockProducer(
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        bool autoComplete = true)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.CreateMock(autoComplete);
    }

    /// <inheritdoc/>
    public Task<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default)
    {
        // Serialize above the bytes core (identical to the real client — Java-faithful, PLAN §7):
        // null-record precondition first (ffi §A5).
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

        // Serialize on THIS thread (a serializer throw surfaces synchronously — and per D5 fires no
        // callback), then forward the bytes carrier to the pump.
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

    /// <summary>
    /// Completes the next pending send successfully (Java <c>MockProducer.completeNext()</c> /
    /// Python <c>complete_next()</c>) — for a mock created with <c>autoComplete: false</c>. Drives
    /// a manual send's <c>Send</c> <see cref="Task"/> to success. Inherent on the concrete
    /// mock (not on <see cref="IAsyncProducer{TKey, TValue}"/>), mirroring the consumer's mock-only helpers.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Completes the next pending send with an error (Java <c>MockProducer.errorNext(...)</c> /
    /// Python <c>error_next(code, message)</c>) — for a mock created with <c>autoComplete: false</c>.
    /// Faults a manual send's <c>Send</c> <see cref="Task"/> with a <see cref="KafkaException"/>
    /// carrying <paramref name="code"/> and <paramref name="message"/> (or the default message for
    /// the code when <paramref name="message"/> is <see langword="null"/>).
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

    /// <summary>
    /// <b>Internal test hook (M11/P3.1 §9), not public API.</b> Blocks until every <c>Send</c> made
    /// so far has been handed to the core by the send-batch thread — the deterministic replacement
    /// for a <c>Thread.Sleep</c> in the manual-completion tests.
    /// </summary>
    /// <remarks>
    /// Since M11/P3.1 the async <c>Send</c> is <b>deferred</b>: it appends to a binding-side
    /// accumulator and returns, and a batch thread hands the record to the core on a threshold or a
    /// free-running window. A manual-mode test that calls <see cref="CompleteNext"/> immediately
    /// after <c>Send</c> can therefore find no pending completion yet — a timing shift, not a defect.
    /// <see cref="Flush"/> is not the answer for those tests: on a mock it <em>completes</em> the
    /// pending sends, which is exactly what they are trying to drive by hand.
    /// </remarks>
    /// <param name="timeout">How long to wait for the accumulator to reach the core.</param>
    /// <exception cref="TimeoutException">The pending sends did not reach the core in time.</exception>
    internal void WaitForSendsToReachCore(TimeSpan timeout)
    {
        if (!_native.DrainPendingSends(timeout))
        {
            throw new TimeoutException(
                $"Pending sends did not reach the core within {timeout}.");
        }
    }

    /// <summary>
    /// <b>Internal test observation point (M11/P3.1 §3.8), not public API.</b> The number of sends
    /// the completion pump has taken off its queue — the witness for "the accumulator was drained
    /// into a still-OPEN pump gate". See <c>NativeProducer.DrainedSendCount</c> for why the send
    /// <see cref="Task"/>s cannot witness that ordering themselves.
    /// </summary>
    internal long DrainedSendCount => _native.DrainedSendCount;

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
