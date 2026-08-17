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

namespace Confluent.Kafka;

/// <summary>
/// The <b>synchronous</b> Kafka producer surface — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.Producer</c> (which is synchronous), implemented by
/// <see cref="KafkaProducer"/> (the real client) and <see cref="MockProducer"/> (a broker-free
/// test helper). The blocking mirror of <see cref="IAsyncProducer"/>, exactly as the sync
/// <see cref="IConsumer{TKey, TValue}"/> is the blocking mirror of
/// <see cref="IAsyncConsumer{TKey, TValue}"/>. The C#-idiomatic <c>I</c> prefix marks the interface
/// (Framework Design Guidelines / analyzer CA1715); the sync-vs-async distinction is carried by the
/// interface/type (bare <c>IProducer</c> = sync; <c>IAsyncProducer</c> = async), not a method
/// suffix — method names mirror Java.
/// </summary>
/// <remarks>
/// <para>
/// <b>Blocking, direct sync C ABI (M11/P4).</b> Every operation calls the synchronous C ABI
/// directly; the core's blocking call parks the caller thread inside the Rust multi-thread runtime
/// (deadlock-free, ffi §A1) — this is <b>not</b> the forbidden managed sync-over-async, and is
/// never routed through the async binding API. It is the sync-consumer precedent
/// (<c>consumer-threading.md §1.1</c>) applied to the producer.
/// </para>
/// <para>
/// <b>Send blocks and returns the metadata directly</b> (= Java <c>send(record).get()</c>) — not a
/// <see cref="System.Threading.Tasks.Task{TResult}"/>. .NET has only one future type
/// (<see cref="System.Threading.Tasks.Task{TResult}"/>), which the async
/// <see cref="IAsyncProducer.Send"/> already returns; a <see cref="System.Threading.Tasks.Task"/>
/// here would clone the async surface and erase the sync/async split. Callers who want pipelined,
/// future-returning sends use <see cref="IAsyncProducer"/>. This deliberately diverges from Python's
/// sync producer (whose <c>send</c> returns a <c>concurrent.futures.Future</c>) — forced by .NET's
/// single future type (PLAN §3 decision #1).
/// </para>
/// <para>
/// <b>No <see cref="System.Threading.CancellationToken"/> anywhere</b> (decision #4): the producer
/// has no <c>wakeup()</c>, so there is no interruption primitive to expose — a blocked operation
/// runs to native completion, the single-owner model of the sync consumer.
/// </para>
/// <para>
/// <b>Single-owner / not thread-safe.</b> At most one operation in flight per instance; do not share
/// one instance across threads without external synchronization. (The manual-mock completion pattern
/// — one thread blocked in <see cref="Send"/>, another calling
/// <see cref="MockProducer.CompleteNext"/> — is the intended cross-thread use and is safe.)
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="Close"/> is the explicit graceful close that <em>surfaces</em> a
/// close failure; <see cref="IDisposable.Dispose"/> is the teardown that swallows it. Both are
/// idempotent. There is no <c>DisposeAsync</c> — this is the synchronous surface (<c>: IDisposable</c>
/// only).
/// </para>
/// </remarks>
public interface IProducer : IDisposable
{
    /// <summary>
    /// Publishes <paramref name="record"/> to its topic and <b>blocks</b> until the cluster
    /// acknowledges it, returning the published record's <see cref="RecordMetadata"/> directly (Java
    /// <c>Producer.send(record).get()</c> — decision #1). The record's key / value bytes are copied
    /// into the send buffer during the call, so the caller may reuse or mutate them the moment this
    /// returns (ffi §A4).
    /// </summary>
    /// <param name="record">The record to publish.</param>
    /// <returns>The published record's <see cref="RecordMetadata"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The send failed (synchronous validation or delivery).</exception>
    RecordMetadata Send(ProducerRecord record);

    /// <summary>
    /// Flushes all pending records and <b>blocks</b> until the core resolves the flush (Java
    /// <c>Producer.flush()</c>). Surfaces a flush failure as a <see cref="KafkaException"/>.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a flush failure.</exception>
    void Flush();

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/>, <b>blocking</b> until the core
    /// resolves it (Java <c>Producer.partitionsFor(String)</c>). On a <see cref="MockProducer"/> this
    /// succeeds broker-free but returns an <b>empty</b> list for every topic (the honest reachability
    /// caveat — a populated list is integration-only).
    /// </summary>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <returns>The topic's partitions (empty on a <see cref="MockProducer"/>).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    /// <summary>
    /// Closes the producer gracefully, then releases its resources (Java <c>Producer.close()</c>).
    /// Surfaces a close failure (unlike <see cref="IDisposable.Dispose"/>, which swallows it).
    /// Idempotent. There is no <c>Close(TimeSpan)</c> overload — the producer ABI exposes no timed
    /// close, and this matches Python's producer <c>close()</c> (decision #5).
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close();
}
