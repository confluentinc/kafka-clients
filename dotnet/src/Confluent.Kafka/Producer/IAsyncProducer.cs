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
using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Producer;

/// <summary>
/// The asynchronous Kafka producer, Java's <c>org.apache.kafka.clients.producer.Producer&lt;K, V&gt;</c>,
/// implemented by <see cref="AsyncKafkaProducer{TKey, TValue}"/> and the broker-free
/// <see cref="AsyncMockProducer{TKey, TValue}"/>. <see cref="IProducer{TKey, TValue}"/> is its synchronous
/// sibling. Method names mirror Java's, with no <c>Async</c> suffix.
/// </summary>
/// <remarks>
/// <para>
/// <c>Send</c> has two stages, as Java's <c>send</c> is a call that may block and returns a <c>Future</c>.
/// The first stage, the <see cref="ValueTask{TResult}"/> of <see cref="AsyncKafkaFuture{T}"/>, completes
/// when the producer has the record. In the common case that is before <c>Send</c> returns. Otherwise the
/// producer waits up to <c>max.block.ms</c> for the topic's metadata or for buffer memory, without blocking
/// the calling thread. The <see cref="AsyncKafkaFuture{T}"/> completes when the cluster acknowledges the
/// record, or when it fails.
/// <code>
/// AsyncKafkaFuture&lt;RecordMetadata&gt; future = await producer.Send(record);   // the producer has it
/// RecordMetadata metadata = await future.Get();                                // delivered
/// </code>
/// </para>
/// <para>
/// On <see cref="AsyncKafkaProducer{TKey, TValue}"/>, a serializer that throws an <see cref="ApiException"/> fails
/// the record, as Java's <c>doSend</c> does: <c>Send</c> calls the delivery callback, if any, once on the caller's
/// thread, with metadata whose offset is <c>-1</c> and that exception, and completes the first stage with a future
/// that fails with it, after an <see cref="IAsyncDeliveryCallback"/>'s task has ended. An
/// <see cref="InvalidCastException"/> is wrapped in a <see cref="SerializationException"/>, and any other throw
/// leaves <c>Send</c> unchanged, as does a delivery callback's own throw on that path. On
/// <see cref="AsyncMockProducer{TKey, TValue}"/>, every serializer throw leaves <c>Send</c> unchanged, as Java's
/// <c>MockProducer</c> lets it.
/// </para>
/// <para>
/// Records reach the producer in <c>Send</c> order only when each first stage is awaited before the next
/// <c>Send</c>, as Java's blocking <c>send</c> makes its caller wait. Without that await, a record the
/// producer takes at once can overtake one that is still waiting.
/// </para>
/// <para>
/// <see cref="Flush"/>, <c>Close</c> and the transaction methods cover the records whose first stage had completed
/// when they began: a commit includes them and an abort discards them.
/// A record still waiting inside the producer is a send in progress, and they do not wait for it.
/// <c>Close</c> fails such a record with the <see cref="KafkaException"/> "Producer closed while send in
/// progress", and its delivery callback does not run.
/// </para>
/// <para>
/// The token passed to <c>Send</c> never cancels the record:
/// <list type="bullet">
/// <item><description>if it is already cancelled, <c>Send</c> throws <see cref="OperationCanceledException"/>
/// and nothing is sent;</description></item>
/// <item><description>if it fires while the first stage is pending, the first stage still waits until the
/// producer takes or refuses the record (up to <c>max.block.ms</c>), then ends with an
/// <see cref="OperationCanceledException"/> carrying the token, whatever the outcome. A record the producer
/// took is still sent, and its delivery callback still runs once, but you get no future for
/// it;</description></item>
/// <item><description>after the first stage has completed, it does nothing.</description></item>
/// </list>
/// To stop waiting for the delivery, pass a token to <see cref="AsyncKafkaFuture{T}.Get(CancellationToken)"/>.
/// A send's future cannot be cancelled: <see cref="AsyncKafkaFuture{T}.Cancel(bool)"/> returns
/// <see langword="false"/>.
/// </para>
/// <para>
/// <see cref="IAsyncDisposable.DisposeAsync"/> closes and then frees the producer;
/// <see cref="IDisposable.Dispose"/> is its blocking form. Both swallow a close error and are idempotent.
/// What a delivery callback may call on its own producer is in <see cref="IDeliveryCallback"/>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public interface IAsyncProducer<TKey, TValue> : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Needs to be called before any other methods when the <c>transactional.id</c> is set in the configuration
    /// (Java <c>initTransactions()</c>); <see cref="IProducer{TKey, TValue}.InitTransactions"/> describes what it
    /// does, its errors and its retries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The token ends only the wait: the task ends with <see cref="OperationCanceledException"/>, and the operation
    /// keeps running in the producer. A token already cancelled at the call throws before anything reaches the
    /// producer. Calling the same method again after a cancel has one of three outcomes, decided by the producer:
    /// while the cancelled operation is still running, the call throws <see cref="ConcurrentModificationException"/>
    /// ("Transactional methods of KafkaProducer are not safe for concurrent access."), where Java's retry would join
    /// the pending operation; after it timed out, the call joins it, as Java's retry does; after it succeeded, the
    /// call starts a new operation, and gets the producer's answer to it. The same holds for the other transaction
    /// methods that take a token.
    /// </para>
    /// <para>
    /// The transaction methods of one producer must not overlap: a call made while another one is still running on
    /// the same producer throws <see cref="ConcurrentModificationException"/>, where Java has no such check.
    /// </para>
    /// <para>
    /// Called from inside a delivery callback's synchronous part, it waits at most <c>max.block.ms</c>, then throws
    /// a <see cref="Common.Errors.TimeoutException"/>, because its completion is queued behind the callback that
    /// waits; the operation may still take effect. The same holds for the other four transaction methods. On an
    /// <see cref="AsyncMockProducer{TKey, TValue}"/>, called from a delivery callback, each of the five completes,
    /// as Java's mock does, with one exception: <see cref="CommitTransaction"/> and
    /// <see cref="AbortTransaction"/> wait for the calling callback's own report, so from a callback that ran
    /// queued behind an async callback, or from an async callback after an <c>await</c>, they end after
    /// <c>max.block.ms</c> (60000 on a mock) with that <see cref="Common.Errors.TimeoutException"/>.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">Ends the wait, never the operation.</param>
    /// <returns>A task that completes when the transactional state is initialized.</returns>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, or the producer is
    /// closed.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this
    /// producer.</exception>
    /// <exception cref="Common.Errors.TimeoutException">The time taken for initializing the transaction has
    /// surpassed <c>max.block.ms</c>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal error, or any other unexpected
    /// error.</exception>
    Task InitTransactions(CancellationToken cancellationToken = default);

    /// <summary>
    /// Should be called before the start of each new transaction (Java <c>beginTransaction()</c>). Note that prior
    /// to the first invocation of this method, you must invoke <see cref="InitTransactions"/> exactly one time.
    /// <see cref="IProducer{TKey, TValue}.BeginTransaction"/> describes its errors.
    /// </summary>
    /// <remarks>
    /// It is synchronous, as Java's, which never waits for the broker. The producer first hands over the records
    /// already sent, so it can wait for them; from inside a delivery callback's synchronous part it times out after
    /// <c>max.block.ms</c>, except on an <see cref="AsyncMockProducer{TKey, TValue}"/> (see
    /// <see cref="InitTransactions"/>).
    /// </remarks>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured,
    /// <see cref="InitTransactions"/> has not yet been invoked, or the producer is closed.</exception>
    /// <exception cref="ProducerFencedException">Another producer with the same <c>transactional.id</c> is
    /// active.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this
    /// producer.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal error, or any other unexpected
    /// error.</exception>
    void BeginTransaction();

    /// <summary>
    /// Sends a list of specified offsets to the consumer group coordinator, and also marks those offsets as part of
    /// the current transaction (Java <c>sendOffsetsToTransaction(offsets, groupMetadata)</c>). These offsets will be
    /// considered committed only if the transaction is committed successfully.
    /// <see cref="IProducer{TKey, TValue}.SendOffsetsToTransaction"/> describes its contract and errors.
    /// </summary>
    /// <remarks>
    /// The token ends only the wait, as for <see cref="InitTransactions"/>, whose remarks also give the outcomes of
    /// a call made again after a cancel, and what a call from inside a delivery callback does.
    /// </remarks>
    /// <param name="offsets">The offsets to commit with the transaction.</param>
    /// <param name="groupMetadata">The consumer group metadata, from the consumer's <c>GroupMetadata()</c>.</param>
    /// <param name="cancellationToken">Ends the wait, never the operation.</param>
    /// <returns>A task that completes when the coordinator has acknowledged the offsets.</returns>
    /// <exception cref="IllegalArgumentException"><paramref name="groupMetadata"/> is null ("Consumer group metadata
    /// could not be null"), or its generation id is above 0 while its member id is unknown.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, no transaction has
    /// been started, or the producer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this
    /// producer.</exception>
    /// <exception cref="Common.Errors.TimeoutException">The combined time taken for resolving topic metadata and
    /// sending the offsets has surpassed <c>max.block.ms</c>.</exception>
    /// <exception cref="BootstrapResolutionException">DNS resolution of the bootstrap servers fails within
    /// <c>bootstrap.resolve.timeout.ms</c>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal or abortable error, or any
    /// other unexpected error.</exception>
    Task SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, Consumer.OffsetAndMetadata> offsets,
        Consumer.ConsumerGroupMetadata groupMetadata,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the ongoing transaction (Java <c>commitTransaction()</c>). This method will flush any unsent records
    /// before actually committing the transaction. <see cref="IProducer{TKey, TValue}.CommitTransaction"/> describes
    /// its contract and errors.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If the transaction is committed successfully, the task completes after all delivery callbacks for records in
    /// the transaction have been invoked and completed, an <see cref="IAsyncDeliveryCallback"/>'s task included.
    /// </para>
    /// <para>
    /// The token ends only the wait, as for <see cref="InitTransactions"/>, whose remarks also give the outcomes of
    /// a call made again after a cancel. Called from inside a delivery callback, after an <c>await</c> too, it waits
    /// at most <c>max.block.ms</c> and then throws a <see cref="Common.Errors.TimeoutException"/>, because it waits
    /// for that callback's own report. On an <see cref="AsyncMockProducer{TKey, TValue}"/> it completes from a sync
    /// callback, or an async callback's synchronous part, that did not run queued behind an async callback (see
    /// <see cref="InitTransactions"/>).
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">Ends the wait, never the operation.</param>
    /// <returns>A task that completes when the transaction is committed.</returns>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, no transaction has
    /// been started, or the producer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this
    /// producer.</exception>
    /// <exception cref="Common.Errors.TimeoutException">The time taken for committing the transaction has surpassed
    /// <c>max.block.ms</c>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal or abortable error, or any
    /// other unexpected error.</exception>
    Task CommitTransaction(CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts the ongoing transaction (Java <c>abortTransaction()</c>). Any unflushed produce messages will be
    /// aborted when this call is made. <see cref="IProducer{TKey, TValue}.AbortTransaction"/> describes its contract
    /// and errors.
    /// </summary>
    /// <remarks>
    /// The token ends only the wait, as for <see cref="InitTransactions"/>, whose remarks also give the outcomes of
    /// a call made again after a cancel. Called from inside a delivery callback, after an <c>await</c> too, it waits
    /// at most <c>max.block.ms</c> and then throws a <see cref="Common.Errors.TimeoutException"/>, and completes on
    /// an <see cref="AsyncMockProducer{TKey, TValue}"/>, as <see cref="CommitTransaction"/> does.
    /// </remarks>
    /// <param name="cancellationToken">Ends the wait, never the operation.</param>
    /// <returns>A task that completes when the transaction is aborted.</returns>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, no transaction has
    /// been started, or the producer is closed.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this
    /// producer.</exception>
    /// <exception cref="Common.Errors.TimeoutException">The time taken for aborting the transaction has surpassed
    /// <c>max.block.ms</c>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal error, or any other unexpected
    /// error.</exception>
    Task AbortTransaction(CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes <paramref name="record"/> on the caller's thread and sends it (Java <c>send(record)</c>);
    /// the type's remarks describe the two stages.
    /// </summary>
    /// <remarks>
    /// Don't change or reuse the key and value buffers until <c>Send</c> throws or the
    /// <see cref="ValueTask{TResult}"/> it returns has completed.
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="cancellationToken">
    /// Never cancels the record. If it fires while the first stage is pending, that stage ends with
    /// <see cref="OperationCanceledException"/> once the producer has taken or refused the record. See the
    /// type's remarks.
    /// </param>
    /// <returns>
    /// The first stage, which completes once the producer has the record, yielding the record's
    /// <see cref="AsyncKafkaFuture{T}"/>; await its <see cref="AsyncKafkaFuture{T}.Get()"/> for the
    /// <see cref="RecordMetadata"/>, or for an <c>ExecutionException</c> around the delivery failure. Await the
    /// <see cref="ValueTask{TResult}"/> once; call <see cref="ValueTask{TResult}.AsTask"/> to store it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="ArgumentException">The topic or a header key contains a NUL character, which the native client
    /// cannot receive unchanged (thrown synchronously).</exception>
    /// <exception cref="SerializationException">On <c>AsyncKafkaProducer</c>, a serializer threw an
    /// <see cref="InvalidCastException"/> while encoding the key or value (thrown synchronously); any other
    /// serializer throw except an <see cref="ApiException"/>, and every serializer throw on
    /// <c>AsyncMockProducer</c>, is rethrown unchanged (see the type's remarks).</exception>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Java's <c>send(record, callback)</c>: <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/>,
    /// and <paramref name="callback"/> runs on the producer's completion thread when the send completes,
    /// before the record's future completes.
    /// </summary>
    /// <remarks>
    /// Don't change or reuse the key and value buffers until <c>Send</c> throws or the
    /// <see cref="ValueTask{TResult}"/> it returns has completed. A <see langword="null"/>
    /// <paramref name="callback"/> is rejected, where Java accepts it:
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/> already sends without one. The
    /// callback's contract is in <see cref="IDeliveryCallback"/>.
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="callback">The delivery callback (non-null).</param>
    /// <param name="cancellationToken">
    /// Never cancels the record. If it fires while the first stage is pending, that stage ends with
    /// <see cref="OperationCanceledException"/> once the producer has taken or refused the record. See the
    /// type's remarks.
    /// </param>
    /// <returns>
    /// The first stage, which completes once the producer has the record, yielding the record's
    /// <see cref="AsyncKafkaFuture{T}"/>; await its <see cref="AsyncKafkaFuture{T}.Get()"/> for the
    /// <see cref="RecordMetadata"/>, or for an <c>ExecutionException</c> around the delivery failure. Await the
    /// <see cref="ValueTask{TResult}"/> once; call <see cref="ValueTask{TResult}.AsTask"/> to store it.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="record"/> or <paramref name="callback"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">The topic or a header key contains a NUL character, which the native client
    /// cannot receive unchanged (thrown synchronously).</exception>
    /// <exception cref="SerializationException">On <c>AsyncKafkaProducer</c>, a serializer threw an
    /// <see cref="InvalidCastException"/> while encoding the key or value (thrown synchronously); any other
    /// serializer throw except an <see cref="ApiException"/>, and every serializer throw on
    /// <c>AsyncMockProducer</c>, is rethrown unchanged (see the type's remarks).</exception>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<TKey, TValue> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <paramref name="record"/> as <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/> does,
    /// and calls <paramref name="callback"/> on the pump thread when its delivery completes. The record's future, and
    /// every later completion of the producer, wait until the callback's task has ended.
    /// </summary>
    /// <remarks>
    /// Don't change or reuse the key and value buffers until <c>Send</c> throws or the
    /// <see cref="ValueTask{TResult}"/> it returns has completed.
    /// </remarks>
    /// <param name="record">The record to send.</param>
    /// <param name="callback">The callback; not <c>null</c>.</param>
    /// <param name="cancellationToken">
    /// Never cancels the record. If it fires while the first stage is pending, that stage ends with
    /// <see cref="OperationCanceledException"/> once the producer has taken or refused the record. See the
    /// type's remarks.
    /// </param>
    /// <returns>The first stage, which hands back the record's future.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="callback"/> is
    /// <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The topic or a header key contains a NUL character, which the native client
    /// cannot receive unchanged (thrown synchronously).</exception>
    /// <exception cref="SerializationException">On <c>AsyncKafkaProducer</c>, a serializer threw an
    /// <see cref="InvalidCastException"/> while encoding the key or value (thrown synchronously); any other
    /// serializer throw except an <see cref="ApiException"/>, and every serializer throw on
    /// <c>AsyncMockProducer</c>, is rethrown unchanged (see the type's remarks).</exception>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<TKey, TValue> record,
        IAsyncDeliveryCallback callback,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Java's <c>flush()</c>. Covers every record whose <c>Send</c> first stage had completed when
    /// <c>Flush</c> began, as Java's <c>flush()</c> covers the records whose <c>send</c> returned. It does
    /// not wait for a <c>Send</c> whose record is still waiting for metadata or buffer memory.
    /// </summary>
    /// <param name="cancellationToken">Best-effort cancellation of the .NET wait (no native abort).</param>
    /// <returns>A task that completes when the flush resolves, or faults with a <see cref="KafkaException"/>.</returns>
    Task Flush(CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the producer gracefully, then releases its resources (Java <c>close()</c>). Surfaces a close
    /// failure (unlike disposal, which swallows it). Idempotent.
    /// </summary>
    /// <remarks>
    /// A <c>Send</c> whose record is still waiting for metadata or buffer memory when the producer closes
    /// fails with the <see cref="KafkaException"/> "Producer closed while send in progress", or with
    /// <see cref="OperationCanceledException"/> if its token has fired. Its delivery callback does not run.
    /// </remarks>
    /// <param name="cancellationToken">
    /// Observed before the close is submitted (an already-canceled token throws
    /// <see cref="OperationCanceledException"/>); once the close is in flight the graceful join
    /// runs to completion.
    /// </param>
    /// <returns>A task that completes when the close resolves, or faults with a <see cref="KafkaException"/>.</returns>
    Task Close(CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits up to <paramref name="timeout"/> for the producer to complete the sending of all
    /// incomplete requests. If it cannot complete them before the timeout expires, it fails any unsent
    /// and unacknowledged records immediately, and aborts the ongoing transaction if it is not already
    /// completing. It then waits, without a bound, until the delivery callbacks of the records sent
    /// before it have run (Java <c>Producer.close(Duration)</c>).
    /// </summary>
    /// <param name="timeout">
    /// The maximum time to wait for the producer to complete any pending requests.
    /// <see cref="TimeSpan.Zero"/> means do not wait for pending send requests to complete.
    /// </param>
    /// <param name="cancellationToken">
    /// Observed before the close is submitted (an already-canceled token throws
    /// <see cref="OperationCanceledException"/>); once the close is in flight the graceful join
    /// runs to completion.
    /// </param>
    /// <remarks>
    /// Called from inside a delivery callback it does not block: it closes as for
    /// <see cref="TimeSpan.Zero"/>, and the producer is torn down after the callback returns.
    /// A <c>Send</c> whose record is still waiting for metadata or buffer memory when the producer
    /// closes fails with the <see cref="KafkaException"/> "Producer closed while send in progress", or
    /// with <see cref="OperationCanceledException"/> if its token has fired. A <c>Send</c> whose record
    /// the close has not yet handed to the producer when the timeout expires fails as a send after close
    /// does, with <see cref="IllegalStateException"/>. Neither record's delivery callback runs.
    /// </remarks>
    /// <returns>A task that completes when the close resolves, or faults with a <see cref="KafkaException"/>.</returns>
    /// <exception cref="IllegalArgumentException"><paramref name="timeout"/> is negative, on <c>AsyncKafkaProducer</c>.</exception>
    Task Close(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (Java <c>partitionsFor(String)</c>). An
    /// <see cref="AsyncMockProducer{TKey, TValue}"/> returns an empty list for every topic.
    /// </summary>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="cancellationToken">Best-effort cancellation of the .NET wait (no native abort).</param>
    /// <returns>
    /// A task resolving with the topic's partitions, or faulting with a
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a snapshot of the producer's metrics (Java <c>metrics()</c>). It does not block, so it is
    /// synchronous on both producer interfaces. An <see cref="AsyncMockProducer{TKey, TValue}"/> returns an
    /// empty snapshot.
    /// </summary>
    /// <returns>The producer's metrics, keyed by <see cref="MetricName"/> value identity.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();
}
