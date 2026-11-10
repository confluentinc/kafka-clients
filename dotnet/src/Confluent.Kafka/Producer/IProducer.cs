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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Producer;

/// <summary>
/// The synchronous Kafka producer, Java's <c>org.apache.kafka.clients.producer.Producer&lt;K, V&gt;</c>,
/// implemented by <see cref="KafkaProducer{TKey, TValue}"/> and the broker-free
/// <see cref="MockProducer{TKey, TValue}"/>. <see cref="IAsyncProducer{TKey, TValue}"/> is its asynchronous
/// sibling.
/// </summary>
/// <remarks>
/// <para>
/// <c>Send</c> serializes the key and value on the caller's thread and returns a
/// <see cref="KafkaFuture{T}"/> once the producer has the record, as Java's <c>send</c> does. It blocks only
/// while the producer waits for the topic's metadata or for buffer memory, up to <c>max.block.ms</c>, and
/// the key and value buffers can be reused when it returns. <see cref="KafkaFuture{T}.Get()"/> blocks until
/// the cluster acknowledges the record, or throws an <c>ExecutionException</c> around its failure. A discarded
/// future leaks nothing, and a callback passed to <c>Send</c> still runs.
/// </para>
/// <para>
/// On <see cref="KafkaProducer{TKey, TValue}"/>, a serializer that throws an <see cref="ApiException"/> fails the
/// record, as Java's <c>doSend</c> does: <c>Send</c> calls the delivery callback, if any, once on the caller's
/// thread, with metadata whose offset is <c>-1</c> and that exception, and returns a future failed with it. An
/// <see cref="InvalidCastException"/> is wrapped in a <see cref="SerializationException"/>, and any other throw
/// leaves <c>Send</c> unchanged, as does a delivery callback's own throw on that path. On
/// <see cref="MockProducer{TKey, TValue}"/>, every serializer throw leaves <c>Send</c> unchanged, as Java's
/// <c>MockProducer</c> lets it.
/// </para>
/// <para>
/// Concurrent <c>Send</c> from several threads is safe; add no lock of your own.
/// </para>
/// <para>
/// There is no <see cref="System.Threading.CancellationToken"/>: the producer has no <c>wakeup()</c>.
/// <see cref="KafkaFuture{T}.Get(System.TimeSpan)"/> and
/// <see cref="KafkaFuture{T}.Get(System.Threading.CancellationToken)"/> end only the caller's wait, never
/// the send.
/// </para>
/// <para>
/// <see cref="Close()"/> surfaces a close failure; <see cref="IDisposable.Dispose"/> swallows it. Both are
/// idempotent. What a delivery callback may call on its own producer is in <see cref="IDeliveryCallback"/>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public interface IProducer<TKey, TValue> : IDisposable
{
    /// <summary>
    /// Needs to be called before any other methods when the <c>transactional.id</c> is set in the configuration
    /// (Java <c>initTransactions()</c>). It ensures any transactions initiated by previous instances of the producer
    /// with the same <c>transactional.id</c> are completed: if the previous instance had failed with a transaction in
    /// progress, it will be aborted; if the last transaction had begun completion, but not yet finished, this method
    /// awaits its completion. It then gets the internal producer id and epoch, used in all future transactional
    /// messages issued by the producer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method will raise a <see cref="Common.Errors.TimeoutException"/> if the transactional state cannot be
    /// initialized before expiration of <c>max.block.ms</c>. It is safe to retry, but once the transactional state
    /// has been successfully initialized, this method should no longer be used.
    /// </para>
    /// <para>
    /// Called from inside a delivery callback, it waits at most <c>max.block.ms</c>, then throws a
    /// <see cref="Common.Errors.TimeoutException"/>, because its completion is queued behind the callback that
    /// waits; the operation may still take effect. The same holds for the other four transaction methods. On a
    /// <see cref="MockProducer{TKey, TValue}"/>, called from a delivery callback, each of the five completes, as
    /// Java's mock does.
    /// </para>
    /// </remarks>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, or the producer is
    /// closed.</exception>
    /// <exception cref="UnsupportedVersionException">Fatal error indicating the broker does not support transactions
    /// (i.e. if its version is lower than 0.11.0.0).</exception>
    /// <exception cref="AuthorizationException">The configured <c>transactional.id</c> is not authorized, or the
    /// idempotent producer id is unavailable. The call may be retried after fixing the permission.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this producer
    /// (see <see cref="CommitTransaction"/>).</exception>
    /// <exception cref="Common.Errors.TimeoutException">The time taken for initializing the transaction has
    /// surpassed <c>max.block.ms</c>.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal error, or any other unexpected
    /// error.</exception>
    void InitTransactions();

    /// <summary>
    /// Should be called before the start of each new transaction (Java <c>beginTransaction()</c>). Note that prior
    /// to the first invocation of this method, you must invoke <see cref="InitTransactions"/> exactly one time.
    /// </summary>
    /// <remarks>
    /// The core first hands over the records already sent, so this call can wait, where Java's returns at once;
    /// called from inside a delivery callback it times out after <c>max.block.ms</c>, except on a
    /// <see cref="MockProducer{TKey, TValue}"/> (see <see cref="InitTransactions"/>).
    /// </remarks>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured,
    /// <see cref="InitTransactions"/> has not yet been invoked, or the producer is closed.</exception>
    /// <exception cref="ProducerFencedException">Another producer with the same <c>transactional.id</c> is
    /// active.</exception>
    /// <exception cref="InvalidProducerEpochException">The producer has attempted to produce with an old epoch to the
    /// partition leader.</exception>
    /// <exception cref="UnsupportedVersionException">Fatal error indicating the broker does not support transactions
    /// (i.e. if its version is lower than 0.11.0.0).</exception>
    /// <exception cref="AuthorizationException">Fatal error indicating that the configured <c>transactional.id</c> is
    /// not authorized.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this producer
    /// (see <see cref="CommitTransaction"/>).</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal error, or any other unexpected
    /// error.</exception>
    void BeginTransaction();

    /// <summary>
    /// Sends a list of specified offsets to the consumer group coordinator, and also marks those offsets as part of
    /// the current transaction (Java <c>sendOffsetsToTransaction(offsets, groupMetadata)</c>). These offsets will be
    /// considered committed only if the transaction is committed successfully. The committed offset should be the
    /// next message your application will consume, i.e. <c>nextRecordToBeProcessed.Offset</c>. You should also add
    /// the leader epoch as commit metadata.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method should be used when you need to batch consumed and produced messages together, typically in a
    /// consume-transform-produce pattern. The specified <paramref name="groupMetadata"/> should be extracted from the
    /// used consumer via its <c>GroupMetadata()</c>.
    /// </para>
    /// <para>
    /// This method is a blocking call that waits until the request has been received and acknowledged by the
    /// consumer group coordinator; but the offsets are not considered as committed until the transaction itself is
    /// successfully committed later (via <see cref="CommitTransaction"/>). The consumer should have
    /// <c>enable.auto.commit=false</c> and should also not commit offsets manually.
    /// </para>
    /// <para>
    /// A null <paramref name="groupMetadata"/> is refused before <paramref name="offsets"/> is read, and a null
    /// <paramref name="offsets"/> before the state is checked. With an empty <paramref name="offsets"/>,
    /// <see cref="KafkaProducer{TKey, TValue}"/> and <see cref="AsyncKafkaProducer{TKey, TValue}"/> return without
    /// contacting the coordinator and without checking the transaction state, as Java's does: beyond those null
    /// checks they throw only on a closed producer, or on one with no transaction support
    /// (<c>enable.idempotence=false</c>). The mocks make their state checks first, as Java's mock does.
    /// </para>
    /// </remarks>
    /// <param name="offsets">The offsets to commit with the transaction.</param>
    /// <param name="groupMetadata">The consumer group metadata, from the consumer's <c>GroupMetadata()</c>.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="groupMetadata"/> is null ("Consumer group metadata
    /// could not be null"), or its generation id is above 0 while its member id is unknown.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key of <paramref name="offsets"/> has a null topic, or a value is
    /// null.</exception>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, no transaction has
    /// been started, or the producer is closed.</exception>
    /// <exception cref="ProducerFencedException">Fatal error indicating another producer with the same
    /// <c>transactional.id</c> is active.</exception>
    /// <exception cref="UnsupportedVersionException">Fatal error indicating the broker does not support transactions
    /// (i.e. if its version is lower than 0.11.0.0) or the broker doesn't support the latest version of the
    /// transactional API with all consumer group metadata (i.e. if its version is lower than 2.5.0).</exception>
    /// <exception cref="UnsupportedForMessageFormatException">Fatal error indicating the message format used for the
    /// offsets topic on the broker does not support transactions.</exception>
    /// <exception cref="AuthorizationException">Fatal error indicating that the configured <c>transactional.id</c> is
    /// not authorized, or the consumer group id is not authorized.</exception>
    /// <exception cref="Consumer.CommitFailedException">The commit failed and cannot be retried (e.g. if the
    /// consumer has been kicked out of the group). Users should handle this by aborting the transaction.</exception>
    /// <exception cref="FencedInstanceIdException">This producer instance gets fenced by the broker due to a
    /// mis-configured consumer instance id within the group metadata.</exception>
    /// <exception cref="InvalidProducerEpochException">The producer has attempted to produce with an old epoch to the
    /// partition leader.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this producer
    /// (see <see cref="CommitTransaction"/>).</exception>
    /// <exception cref="Common.Errors.TimeoutException">The combined time taken for resolving topic metadata and
    /// sending the offsets has surpassed <c>max.block.ms</c>.</exception>
    /// <exception cref="BootstrapResolutionException">DNS resolution of the bootstrap servers fails within
    /// <c>bootstrap.resolve.timeout.ms</c>.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal or abortable error, or any
    /// other unexpected error.</exception>
    void SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, Consumer.OffsetAndMetadata> offsets,
        Consumer.ConsumerGroupMetadata groupMetadata);

    /// <summary>
    /// Commits the ongoing transaction (Java <c>commitTransaction()</c>). This method will flush any unsent records
    /// before actually committing the transaction.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If any of the <c>Send</c> calls which were part of the transaction hit irrecoverable errors, this method will
    /// throw the last received error immediately and the transaction will not be committed. So all <c>Send</c> calls
    /// in a transaction must succeed in order for this method to succeed.
    /// </para>
    /// <para>
    /// If the transaction is committed successfully and this method returns without throwing, it is guaranteed
    /// that all delivery callbacks for records in the transaction will have been invoked and completed. Exceptions
    /// thrown by callbacks are ignored; the producer proceeds to commit the transaction in any case.
    /// </para>
    /// <para>
    /// This method will raise a <see cref="Common.Errors.TimeoutException"/> if the transaction cannot be committed
    /// before expiration of <c>max.block.ms</c>, but this does not mean the request did not actually reach the
    /// broker. It is safe to retry, but it is not possible to attempt a different operation (such as
    /// <see cref="AbortTransaction"/>) since the commit may already be in the progress of completing. If not
    /// retrying, the only option is to close the producer.
    /// </para>
    /// <para>
    /// The transaction methods of one producer must not overlap: a call made while the producer still runs another
    /// one's operation throws <see cref="ConcurrentModificationException"/> ("Transactional methods of
    /// KafkaProducer are not safe for concurrent access."), where Java has no such check. The check covers the
    /// operation only: a call made while a commit or an abort waits for the delivery callbacks is not refused.
    /// </para>
    /// </remarks>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, no transaction has
    /// been started, or the producer is closed.</exception>
    /// <exception cref="ProducerFencedException">Fatal error indicating another producer with the same
    /// <c>transactional.id</c> is active.</exception>
    /// <exception cref="UnsupportedVersionException">Fatal error indicating the broker does not support transactions
    /// (i.e. if its version is lower than 0.11.0.0).</exception>
    /// <exception cref="AuthorizationException">Fatal error indicating that the configured <c>transactional.id</c> is
    /// not authorized.</exception>
    /// <exception cref="InvalidProducerEpochException">The producer has attempted to produce with an old epoch to the
    /// partition leader.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this
    /// producer.</exception>
    /// <exception cref="Common.Errors.TimeoutException">The time taken for committing the transaction has surpassed
    /// <c>max.block.ms</c>.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal or abortable error, or any
    /// other unexpected error.</exception>
    void CommitTransaction();

    /// <summary>
    /// Aborts the ongoing transaction (Java <c>abortTransaction()</c>). Any unflushed produce messages will be
    /// aborted when this call is made. This call will throw immediately if any prior <c>Send</c> calls failed with a
    /// <see cref="ProducerFencedException"/> or an <see cref="AuthorizationException"/>.
    /// </summary>
    /// <remarks>
    /// This method will raise a <see cref="Common.Errors.TimeoutException"/> if the transaction cannot be aborted
    /// before expiration of <c>max.block.ms</c>, but this does not mean the request did not actually reach the
    /// broker. It is safe to retry, but it is not possible to attempt a different operation (such as
    /// <see cref="CommitTransaction"/>) since the abort may already be in the progress of completing. If not
    /// retrying, the only option is to close the producer.
    /// </remarks>
    /// <exception cref="IllegalStateException">No <c>transactional.id</c> has been configured, no transaction has
    /// been started, or the producer is closed.</exception>
    /// <exception cref="ProducerFencedException">Fatal error indicating another producer with the same
    /// <c>transactional.id</c> is active.</exception>
    /// <exception cref="InvalidProducerEpochException">The producer has attempted to produce with an old epoch to the
    /// partition leader.</exception>
    /// <exception cref="UnsupportedVersionException">Fatal error indicating the broker does not support transactions
    /// (i.e. if its version is lower than 0.11.0.0).</exception>
    /// <exception cref="AuthorizationException">Fatal error indicating that the configured <c>transactional.id</c> is
    /// not authorized.</exception>
    /// <exception cref="ConcurrentModificationException">Another transaction method is running on this producer
    /// (see <see cref="CommitTransaction"/>).</exception>
    /// <exception cref="Common.Errors.TimeoutException">The time taken for aborting the transaction has surpassed
    /// <c>max.block.ms</c>.</exception>
    /// <exception cref="KafkaException">The producer has encountered a previous fatal error, or any other unexpected
    /// error.</exception>
    void AbortTransaction();

    /// <summary>
    /// Serializes and sends <paramref name="record"/>, returning once the producer has it (Java
    /// <c>send(record)</c>). This blocks only while the producer waits for metadata or for buffer memory,
    /// up to <c>max.block.ms</c>.
    /// </summary>
    /// <param name="record">The record to publish.</param>
    /// <returns>The send's delivery handle; its <see cref="KafkaFuture{T}.Get()"/> returns the published
    /// record's <see cref="RecordMetadata"/> or throws an <c>ExecutionException</c> around the delivery
    /// failure.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="ArgumentException">The topic or a header key contains a NUL character, which the native client
    /// cannot receive unchanged.</exception>
    /// <exception cref="SerializationException">On <c>KafkaProducer</c>, a serializer threw an
    /// <see cref="InvalidCastException"/> while encoding the key or value; any other serializer throw except an
    /// <see cref="ApiException"/>, and every serializer throw on <c>MockProducer</c>, is rethrown unchanged (see the
    /// type's remarks).</exception>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record synchronously (a delivery failure
    /// surfaces from <see cref="KafkaFuture{T}.Get()"/> instead).</exception>
    KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record);

    /// <summary>
    /// Java's <c>send(record, callback)</c>: <see cref="Send(ProducerRecord{TKey, TValue})"/>, and
    /// <paramref name="callback"/> runs on the producer's completion thread when the send completes, before
    /// the returned future's <see cref="KafkaFuture{T}.Get()"/> returns or throws.
    /// </summary>
    /// <remarks>
    /// A <see langword="null"/> <paramref name="callback"/> is rejected, where Java accepts it:
    /// <see cref="Send(ProducerRecord{TKey, TValue})"/> already sends without one. The callback's contract is
    /// in <see cref="IDeliveryCallback"/>.
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="callback">The delivery callback (non-null).</param>
    /// <returns>The send's delivery handle; its <see cref="KafkaFuture{T}.Get()"/> returns the published
    /// record's <see cref="RecordMetadata"/> or throws an <c>ExecutionException</c> around the delivery
    /// failure.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="record"/> or <paramref name="callback"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">The topic or a header key contains a NUL character, which the native client
    /// cannot receive unchanged.</exception>
    /// <exception cref="SerializationException">On <c>KafkaProducer</c>, a serializer threw an
    /// <see cref="InvalidCastException"/> while encoding the key or value; any other serializer throw except an
    /// <see cref="ApiException"/>, and every serializer throw on <c>MockProducer</c>, is rethrown unchanged (see the
    /// type's remarks).</exception>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record synchronously (a delivery failure
    /// surfaces from <see cref="KafkaFuture{T}.Get()"/> instead).</exception>
    KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback);

    /// <summary>
    /// Blocks until every record whose <c>Send</c> returned before this call has completed, and its
    /// delivery callback has run (Java <c>flush()</c>). A <c>Send</c> still waiting for metadata or buffer
    /// memory on another thread is not waited for.
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a flush failure.</exception>
    void Flush();

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/>, blocking until the producer has it (Java
    /// <c>partitionsFor(String)</c>). A <see cref="MockProducer{TKey, TValue}"/> returns an empty list for
    /// every topic.
    /// </summary>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <returns>The topic's partitions (empty on a <see cref="MockProducer{TKey, TValue}"/>).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    /// <summary>
    /// Returns a snapshot of the producer's metrics (Java <c>metrics()</c>). It does not block, so it is
    /// synchronous on both producer interfaces. A <see cref="MockProducer{TKey, TValue}"/> returns an empty
    /// snapshot.
    /// </summary>
    /// <returns>The producer's metrics, keyed by <see cref="MetricName"/> value identity.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();

    /// <summary>
    /// Closes the producer, waiting until the delivery callbacks of the records sent before it have run,
    /// then releases its resources (Java <c>close()</c>). Surfaces a close failure, unlike
    /// <see cref="IDisposable.Dispose"/>, which swallows it. Idempotent.
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close();

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
    /// <remarks>
    /// Called from inside a delivery callback it does not block: it closes as for
    /// <see cref="TimeSpan.Zero"/>, and the producer is torn down after the callback returns.
    /// A <c>Send</c> whose record is still waiting for metadata or buffer memory when the producer
    /// closes fails with the <see cref="KafkaException"/> "Producer closed while send in progress".
    /// Its delivery callback does not run.
    /// </remarks>
    /// <exception cref="IllegalArgumentException"><paramref name="timeout"/> is negative, on <c>KafkaProducer</c>.</exception>
    void Close(TimeSpan timeout);
}
