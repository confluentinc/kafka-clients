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
/// <b>Transactions (M17/P1).</b> The transaction members run against the core's mock, which
/// follows Java's <c>MockProducer</c>: a send inside a transaction reaches
/// <see cref="HistoryCount()"/> only when the transaction commits, and commit and abort flush
/// first. Three Java transaction helpers are here as inherent members:
/// <see cref="SetCommitTransactionError"/> / <see cref="ClearCommitTransactionError"/> (Java's
/// public <c>commitTransactionException</c> field), <see cref="SentOffsets"/> (Java
/// <c>sentOffsets()</c>) and <see cref="CommittedOffset"/> (a single-entry view of Java's
/// <c>consumerGroupOffsetsHistory()</c>). The others are absent because the C ABI does not export
/// them: <c>fenceProducer()</c>; <c>transactionInitialized()</c>, <c>transactionInFlight()</c>,
/// <c>transactionCommitted()</c> and <c>transactionAborted()</c>; <c>commitCount()</c>;
/// <c>flushed()</c>; the <c>initTransactionException</c>, <c>beginTransactionException</c>,
/// <c>sendOffsetsToTransactionException</c> and <c>abortTransactionException</c> fields; the
/// <c>history()</c> list and <c>uncommittedRecords()</c> (only <c>history()</c>'s count is
/// exported, as <see cref="HistoryCount()"/>); and the full <c>consumerGroupOffsetsHistory()</c>
/// list and <c>uncommittedOffsets()</c> (only a single-entry projection is exported, as
/// <see cref="CommittedOffset"/>).
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
/// <para>
/// ⚠ <b><c>Send</c> BLOCKS the calling thread under sustained saturation</b> (M11/P3.3), exactly as
/// on <see cref="AsyncKafkaProducer{TKey, TValue}"/> — the bound lives in the shared send
/// accumulator, so it is not a real-producer-only behaviour.
/// <see cref="IAsyncProducer{TKey, TValue}"/> states the contract in full. It is nonetheless hard to
/// reach on a mock: the bound counts records the binding has accepted but <em>not yet handed to its
/// send-batch thread</em>, and the mock's <c>send_batch</c> never blocks, so the batch thread keeps clearing the
/// accumulator within its linger window whether or not those sends have been completed. A
/// manual-completion mock (<c>autoComplete: false</c>) therefore accumulates unresolved sends
/// <em>past</em> this bound rather than against it — <see cref="CompleteNext"/> /
/// <see cref="ErrorNext"/> / <see cref="Flush"/> resolve those, and they are not what returns
/// admission capacity.
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

    /// <summary>
    /// <b>Internal test seam (M17/P1 D3), not public API.</b> As the public constructor, but the
    /// producer's send accumulator is built with <paramref name="settings"/> instead of the
    /// environment's — so a test can hold records in the accumulator (a long batch window and a slot
    /// threshold above its record count) without process-wide environment variables, which the
    /// parallel suite would share.
    /// </summary>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <param name="autoComplete">As for the public constructor.</param>
    /// <param name="settings">The accumulator settings.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null.
    /// </exception>
    internal AsyncMockProducer(
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        bool autoComplete,
        SendAccumulatorSettings settings)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.CreateMock(autoComplete, settings);
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
    public Task InitTransactions(CancellationToken cancellationToken = default) =>
        _native.InitTransactionsWithCallback(cancellationToken);

    /// <inheritdoc/>
    public void BeginTransaction() => _native.BeginTransaction();

    /// <inheritdoc/>
    public Task SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata,
        CancellationToken cancellationToken = default) =>
        _native.SendOffsetsToTransactionWithCallback(offsets, groupMetadata, cancellationToken);

    /// <inheritdoc/>
    public Task CommitTransaction(CancellationToken cancellationToken = default) =>
        _native.CommitTransactionWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task AbortTransaction(CancellationToken cancellationToken = default) =>
        _native.AbortTransactionWithCallback(cancellationToken);

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
    /// <remarks>
    /// ⚠ <b>On a manual (<c>autoComplete: false</c>) mock, complete or fail the pending sends
    /// before calling this.</b> Clearing drops their completions, so those sends never resolve, as
    /// in Java. Here that also holds up everything the producer resolves after them, in order: every
    /// later send, every later <see cref="CommitTransaction"/> or <see cref="AbortTransaction"/>
    /// (which waits for the earlier sends, residual R-a in the remarks on
    /// <see cref="IAsyncProducer{TKey, TValue}"/>), and <see cref="Dispose"/> /
    /// <see cref="DisposeAsync"/>, which never return.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public void Clear() => _native.MockClear();

    /// <summary>
    /// Makes every later <c>CommitTransaction</c> fail with <paramref name="code"/> and
    /// <paramref name="message"/> until <see cref="ClearCommitTransactionError"/> is called (Java's
    /// public <c>MockProducer.commitTransactionException</c> field, which stays set until it is
    /// cleared). The failed commit leaves the transaction open, so it can still be aborted.
    /// </summary>
    /// <remarks>
    /// <b>Setup only.</b> Call it between transaction members, never while one is running on the
    /// same producer: the C ABI does not say whether an overlapping transaction member sees the new
    /// value, and the binding adds no guard.
    /// </remarks>
    /// <param name="code">The Kafka error code: non-zero, and within the range of <see cref="short"/>.</param>
    /// <param name="message">The error message, or <see langword="null"/> for the code's default message.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="code"/> is 0, or outside the range of <see cref="short"/>.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public void SetCommitTransactionError(int code, string? message = null) =>
        _native.MockSetCommitTransactionError(code, message);

    /// <summary>
    /// Removes the error <see cref="SetCommitTransactionError"/> installed, so later commits behave
    /// normally again (Java: setting <c>commitTransactionException</c> back to
    /// <see langword="null"/>). Setup only, as <see cref="SetCommitTransactionError"/> is.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public void ClearCommitTransactionError() => _native.MockClearCommitTransactionError();

    /// <summary>
    /// Whether the current transaction has been given offsets by a
    /// <c>SendOffsetsToTransaction</c> call with a non-empty map (Java
    /// <c>MockProducer.sentOffsets()</c>). <c>BeginTransaction</c> and <see cref="Clear"/> reset it;
    /// a commit does not. A <b>method</b>, not a property, like <see cref="HistoryCount()"/>: it calls into the
    /// core and can throw.
    /// </summary>
    /// <returns><see langword="true"/> if offsets were sent since the last <c>BeginTransaction</c> or <see cref="Clear"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool SentOffsets() => _native.MockSentOffsets();

    /// <summary>
    /// The offset that committed transactions sent for <paramref name="groupId"/> and
    /// <paramref name="partition"/>, or <see langword="null"/> if none did — a single-entry view of
    /// Java's <c>MockProducer.consumerGroupOffsetsHistory()</c>. When several committed transactions
    /// sent one, the newest wins; offsets of an aborted or still-open transaction are not included.
    /// <see cref="Clear"/> forgets every committed offset, as Java's <c>clear()</c> does.
    /// </summary>
    /// <remarks>
    /// <see cref="OffsetAndMetadata.LeaderEpoch"/> is <see langword="null"/> when the offset was
    /// sent without one. The metadata is returned whole up to 1 MiB (1,048,576 bytes of UTF-8); a
    /// longer one throws rather than being returned cut short. The C ABI hands the metadata over
    /// NUL-terminated, so metadata containing a NUL character comes back ending before it. Setup
    /// only, as <see cref="SetCommitTransactionError"/> is.
    /// </remarks>
    /// <param name="groupId">The consumer group id.</param>
    /// <param name="partition">The topic-partition.</param>
    /// <returns>The committed offset, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="partition"/>'s topic is null (a <c>default</c> <see cref="TopicPartition"/>).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="InvalidOperationException">The metadata is longer than 1 MiB.</exception>
    public OffsetAndMetadata? CommittedOffset(string groupId, TopicPartition partition) =>
        _native.MockCommittedOffset(groupId, partition);

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
