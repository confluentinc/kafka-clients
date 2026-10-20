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

namespace Confluent.Kafka;

/// <summary>
/// The callback of a record send, Java's <c>org.apache.kafka.clients.producer.Callback</c>. Pass it to
/// <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, IDeliveryCallback)"/> or
/// <see cref="IAsyncProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, IDeliveryCallback, System.Threading.CancellationToken)"/>,
/// which still return the record's future, as Java's <c>send(ProducerRecord, Callback)</c> does.
/// </summary>
/// <remarks>
/// <para>
/// <b>Thread.</b> It runs on the producer's completion thread, on both surfaces, as Java's runs on the
/// producer's I/O thread: one producer's callbacks never overlap, and a slow callback delays every other
/// completion of that producer. A callback still pending when the producer closes runs on the thread that
/// closes it. An instance shared across producers can run on several threads at once, so it must be
/// thread-safe.
/// </para>
/// <para>
/// <b>Order.</b> It runs before the record's future completes, as in Java.
/// </para>
/// <para>
/// <b>Which records fire it.</b> Every record the producer accepted fires it exactly once, on success or
/// failure, also when the wait on its future was abandoned. A record refused before it was appended fires
/// nothing: the synchronous <c>Send</c> throws, and the asynchronous <c>Send</c>'s first stage faults. An
/// error from <c>Send</c> does not prove that the callback will not fire: a record that reached the
/// accumulator before the send failed (a transactional producer's send outside a transaction) still fires
/// it once. A record that <c>Close</c> fails while it waits for metadata or buffer memory ("Producer closed
/// while send in progress") fires nothing, as in Java.
/// </para>
/// <para>
/// <b>Inside it.</b> Sending again is supported. <c>Get()</c> on the record's own future never returns, as
/// in Java; <c>Get(TimeSpan)</c> and <c>Get(CancellationToken)</c> end as usual. <c>Close</c> closes with a
/// zero timeout and returns at once, as Java's does. On <see cref="KafkaProducer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/>, <c>Flush</c> throws Java's "may lead to deadlock"
/// <see cref="KafkaException"/>, and <c>PartitionsFor</c>, whose wait is queued behind the callback, ends
/// after <c>max.block.ms</c> with a retriable <see cref="KafkaException"/>.
/// </para>
/// <para>
/// <b>Throwing.</b> An exception it throws is reported to the producer, which logs it, and is written once
/// to <see cref="System.Diagnostics.Trace"/>. It changes neither the record's outcome nor any other record.
/// </para>
/// </remarks>
public interface IDeliveryCallback
{
    /// <summary>
    /// Java's <c>onCompletion</c>: invoked once, when the record's send completes.
    /// </summary>
    /// <param name="metadata">
    /// The record's metadata. Never <see langword="null"/>: on failure it is Java's placeholder, the
    /// record's topic and partition (or <c>-1</c>), with <c>-1</c> for offset and timestamp.
    /// </param>
    /// <param name="exception">
    /// <see langword="null"/> on success; otherwise the exception the record's future fails with, the same
    /// instance.
    /// </param>
    void OnCompletion(RecordMetadata metadata, Exception? exception);
}
