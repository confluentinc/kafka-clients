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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Producer;

/// <summary>
/// A broker-free asynchronous producer for tests, Java's <c>org.apache.kafka.clients.producer.MockProducer&lt;K, V&gt;</c>:
/// it serializes the key and value like <see cref="AsyncKafkaProducer{TKey, TValue}"/> and counts the records it sends.
/// </summary>
/// <remarks>
/// With <c>autoComplete: false</c>, a send's future stays pending until <see cref="CompleteNext"/> or
/// <see cref="ErrorNext"/> resolves it, and its first stage does not wait for that. <see cref="Dispose"/> and
/// <see cref="DisposeAsync"/> first flush, completing the pending sends, unless a close has begun. <c>Close</c>
/// leaves a pending send pending.
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class AsyncMockProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
{
    private readonly NativeProducer _native;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;

    /// <summary>
    /// Creates a broker-free mock producer with the key and value serializers, as Java's <c>MockProducer</c> takes them.
    /// </summary>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <param name="autoComplete">
    /// When <see langword="true"/> (the default), each send completes at once. When <see langword="false"/>, a
    /// send's future stays pending until <see cref="CompleteNext"/> or <see cref="ErrorNext"/> resolves it;
    /// <see cref="Dispose"/> and <see cref="DisposeAsync"/> first flush, completing the pending sends, unless a
    /// close has begun, and <c>Close</c> leaves a pending send pending.
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
        _native = NativeProducer.CreateMock(autoComplete, NativeProducer.PumpThreadName);
    }

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        return SendValidated(record, callback: null, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<TKey, TValue> record,
        ICallback callback,
        CancellationToken cancellationToken = default)
    {
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

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<TKey, TValue> record,
        IAsyncCallback callback,
        CancellationToken cancellationToken = default)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        return SendValidated(record, null, cancellationToken, callback);
    }

    // Not async, so a serializer throw stays synchronous.
    private ValueTask<AsyncKafkaFuture<RecordMetadata>> SendValidated(
        ProducerRecord<TKey, TValue> record,
        ICallback? callback,
        CancellationToken cancellationToken,
        IAsyncCallback? asyncCallback = null)
    {
        // Java's doSend checks for a closed producer before it serializes.
        _native.ThrowIfClosed();

        // Java's MockProducer calls the serializers with no try, so every throw leaves Send unchanged.
        SerializedProducerRecord serialized =
            SerializedProducerRecord.Serialize(record, _keySerializer, _valueSerializer, wrapInvalidCast: false);

        return _native.SendViaPump(serialized, callback, cancellationToken, asyncCallback);
    }

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
    public Task Close(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(timeout, cancellationToken);

    /// <summary>
    /// Completes the earliest uncompleted send successfully (Java <c>MockProducer.completeNext()</c>), for a mock
    /// created with <c>autoComplete: false</c>.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Fails the earliest uncompleted send with <paramref name="e"/> (Java <c>MockProducer.errorNext</c>); its
    /// delivery callback receives the same instance, and its future's <see cref="AsyncKafkaFuture{T}.Get()"/> faults
    /// with an <see cref="Confluent.Kafka.ExecutionException"/> around it.
    /// </summary>
    /// <param name="e">
    /// The exception to fail the send with; <see langword="null"/> completes it successfully, as
    /// <see cref="CompleteNext"/> does.
    /// </param>
    /// <returns><see langword="true"/> if a pending send was completed; otherwise <see langword="false"/>.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool ErrorNext(Exception? e) => _native.MockErrorNext(e);

    /// <summary>
    /// The number of records in the sent history (Java <c>MockProducer.history().size()</c>); the native mock
    /// exposes the count only.
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public int HistoryCount() => _native.MockHistoryCount();

    /// <summary>
    /// Clears the sent history and any pending completions (Java <c>MockProducer.clear()</c>).
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void Clear() => _native.MockClear();

    /// <summary>Whether the producer has been closed (Java <c>MockProducer.closed()</c>).</summary>
    /// <returns><see langword="true"/> once the producer is closed or disposed.</returns>
    public bool Closed() => _native.MockClosed();

    /// <summary>
    /// Whether every sent record has completed, none still waiting for <c>CompleteNext</c> or <c>ErrorNext</c> (Java
    /// <c>MockProducer.flushed()</c>).
    /// </summary>
    /// <returns><see langword="true"/> when no completion is pending.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool Flushed() => _native.MockFlushed();

    /// <summary>
    /// Sets the exception that the task of <see cref="InitTransactions"/> fails with, as Java's
    /// <c>MockProducer.initTransactionException</c> field: every later call fails with this same instance, after the
    /// closed, fenced and state checks.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetInitTransactionException(Exception? exception) => _native.MockSetInitTransactionError(exception);

    /// <summary>
    /// Sets the exception that <see cref="BeginTransaction"/> throws, as Java's
    /// <c>MockProducer.beginTransactionException</c> field: every later call throws this same instance, after the
    /// closed, fenced and initialized checks, before the open-transaction check.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetBeginTransactionException(Exception? exception) => _native.MockSetBeginTransactionError(exception);

    /// <summary>
    /// Sets the exception that the task of <see cref="SendOffsetsToTransaction"/> fails with, as Java's
    /// <c>MockProducer.sendOffsetsToTransactionException</c> field: every later call fails with this same instance,
    /// after the closed, fenced and state checks.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetSendOffsetsToTransactionException(Exception? exception) =>
        _native.MockSetSendOffsetsToTransactionError(exception);

    /// <summary>
    /// Sets the exception that the task of <see cref="CommitTransaction"/> fails with, as Java's
    /// <c>MockProducer.commitTransactionException</c> field: every later call fails with this same instance, after the
    /// closed, fenced and state checks.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetCommitTransactionException(Exception? exception) => _native.MockSetCommitTransactionError(exception);

    /// <summary>
    /// Sets the exception that the task of <see cref="AbortTransaction"/> fails with, as Java's
    /// <c>MockProducer.abortTransactionException</c> field: every later call fails with this same instance, after the
    /// closed, fenced and state checks.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetAbortTransactionException(Exception? exception) => _native.MockSetAbortTransactionError(exception);

    /// <summary>
    /// Sets the exception that the first stage of <c>Send</c> fails with, as Java's <c>MockProducer.sendException</c> field: every later
    /// call fails with this same instance, after the closed and fenced checks. The key and value are serialized first,
    /// so a serializer's exception is thrown instead.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetSendException(Exception? exception) => _native.MockSetSendError(exception);

    /// <summary>
    /// Sets the exception that the task of <see cref="Flush"/> fails with, as Java's <c>MockProducer.flushException</c> field: every later
    /// call fails with this same instance after the closed check, and completes no pending send; <see cref="CommitTransaction"/> and <see cref="AbortTransaction"/>, which flush first, fail with it too.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetFlushException(Exception? exception) => _native.MockSetFlushError(exception);

    /// <summary>
    /// Sets the exception that the task of <see cref="PartitionsFor"/> fails with, as Java's <c>MockProducer.partitionsForException</c>
    /// field: every later call fails with this same instance.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetPartitionsForException(Exception? exception) => _native.MockSetPartitionsForError(exception);

    /// <summary>
    /// Sets the exception that the task of <c>Close</c> fails with, as Java's <c>MockProducer.closeException</c> field: every
    /// later call fails with this same instance and leaves the producer open (<see cref="Closed"/> stays
    /// <see langword="false"/>). A <c>Close</c> from inside a delivery callback returns at once and closes the producer
    /// without it, and <c>Dispose</c> releases the producer whatever it is set to.
    /// </summary>
    /// <param name="exception">The exception to throw; <see langword="null"/> clears it.</param>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void SetCloseException(Exception? exception) => _native.MockSetCloseError(exception);

    /// <summary>
    /// Fences the producer (Java <c>MockProducer.fenceProducer()</c>): the transaction operations then throw
    /// <see cref="Common.Errors.ProducerFencedException"/>, and <c>Send</c> a <see cref="KafkaException"/> caused by one.
    /// </summary>
    /// <exception cref="IllegalStateException">
    /// The producer is closed, or it has not been initialized for transactions.
    /// </exception>
    /// <exception cref="Common.Errors.ProducerFencedException">The producer is already fenced.</exception>
    public void FenceProducer() => _native.MockFenceProducer();

    /// <summary>Whether <see cref="InitTransactions"/> succeeded (Java <c>MockProducer.transactionInitialized()</c>).</summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool TransactionInitialized => _native.MockTransactionInitialized();

    /// <summary>Whether a transaction is open (Java <c>MockProducer.transactionInFlight()</c>).</summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool TransactionInFlight => _native.MockTransactionInFlight();

    /// <summary>
    /// Whether the last transaction was committed (Java <c>MockProducer.transactionCommitted()</c>); reset when a
    /// transaction begins.
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool TransactionCommitted => _native.MockTransactionCommitted();

    /// <summary>
    /// Whether the last transaction was aborted (Java <c>MockProducer.transactionAborted()</c>); reset when a
    /// transaction begins.
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool TransactionAborted => _native.MockTransactionAborted();

    /// <summary>
    /// Whether the open transaction, or the last one, sent offsets (Java <c>MockProducer.sentOffsets()</c>); reset when
    /// a transaction begins and by <see cref="Clear"/>. An empty <c>offsets</c> map sends none.
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool SentOffsets => _native.MockSentOffsets();

    /// <summary>The number of committed transactions (Java <c>MockProducer.commitCount()</c>).</summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public long CommitCount => _native.MockCommitCount();

    /// <summary>
    /// The consumer group offsets of each committed transaction that sent offsets, since the last <see cref="Clear"/>
    /// (Java <c>MockProducer.consumerGroupOffsetsHistory()</c>): one map per commit, from group id to its offsets.
    /// </summary>
    /// <returns>A copy, in commit order.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>
        ConsumerGroupOffsetsHistory() => _native.MockConsumerGroupOffsetsHistory();

    /// <summary>
    /// The consumer group offsets sent in the open transaction (Java <c>MockProducer.uncommittedOffsets()</c>), from
    /// group id to its offsets, the latest offset of each partition kept.
    /// </summary>
    /// <returns>A copy.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> UncommittedOffsets() =>
        _native.MockUncommittedOffsets();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
