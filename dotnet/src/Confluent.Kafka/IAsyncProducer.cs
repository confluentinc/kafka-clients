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

namespace Confluent.Kafka;

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
/// Records reach the producer in <c>Send</c> order only when each first stage is awaited before the next
/// <c>Send</c>, as Java's blocking <c>send</c> makes its caller wait. Without that await, a record the
/// producer takes at once can overtake one that is still waiting.
/// </para>
/// <para>
/// <see cref="Flush"/> and <c>Close</c> cover the records whose first stage had completed when they began.
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
    /// <see cref="RecordMetadata"/>, or for the delivery failure. Await the <see cref="ValueTask{TResult}"/>
    /// once; call <see cref="ValueTask{TResult}.AsTask"/> to store it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value (thrown synchronously).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
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
    /// <see cref="RecordMetadata"/>, or for the delivery failure. Await the <see cref="ValueTask{TResult}"/>
    /// once; call <see cref="ValueTask{TResult}.AsTask"/> to store it.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="record"/> or <paramref name="callback"/> is null.
    /// </exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value (thrown synchronously).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
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
    /// does, with <see cref="InvalidOperationException"/>. Neither record's delivery callback runs.
    /// </remarks>
    /// <returns>A task that completes when the close resolves, or faults with a <see cref="KafkaException"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative, on <c>AsyncKafkaProducer</c>.</exception>
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
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();
}
