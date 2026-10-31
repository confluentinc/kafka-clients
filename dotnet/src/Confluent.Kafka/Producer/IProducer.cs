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
/// the cluster acknowledges the record, or rethrows its failure unwrapped. A discarded future leaks nothing,
/// and a callback passed to <c>Send</c> still runs.
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
    /// Serializes and sends <paramref name="record"/>, returning once the producer has it (Java
    /// <c>send(record)</c>). This blocks only while the producer waits for metadata or for buffer memory,
    /// up to <c>max.block.ms</c>.
    /// </summary>
    /// <param name="record">The record to publish.</param>
    /// <returns>The send's delivery handle; its <see cref="KafkaFuture{T}.Get()"/> returns the published
    /// record's <see cref="RecordMetadata"/> or rethrows the delivery failure.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
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
    /// record's <see cref="RecordMetadata"/> or rethrows the delivery failure.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="record"/> or <paramref name="callback"/> is null.
    /// </exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record synchronously (a delivery failure
    /// surfaces from <see cref="KafkaFuture{T}.Get()"/> instead).</exception>
    KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback);

    /// <summary>
    /// Blocks until every record whose <c>Send</c> returned before this call has completed, and its
    /// delivery callback has run (Java <c>flush()</c>). A <c>Send</c> still waiting for metadata or buffer
    /// memory on another thread is not waited for.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
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
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    /// <summary>
    /// Returns a snapshot of the producer's metrics (Java <c>metrics()</c>). It does not block, so it is
    /// synchronous on both producer interfaces. A <see cref="MockProducer{TKey, TValue}"/> returns an empty
    /// snapshot.
    /// </summary>
    /// <returns>The producer's metrics, keyed by <see cref="MetricName"/> value identity.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
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
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative, on <c>KafkaProducer</c>.</exception>
    void Close(TimeSpan timeout);
}
