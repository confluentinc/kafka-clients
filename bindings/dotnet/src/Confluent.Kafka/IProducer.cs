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
/// The <b>synchronous</b>, typed Kafka producer surface — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.Producer&lt;K, V&gt;</c> (which is synchronous), implemented by
/// <see cref="KafkaProducer{TKey, TValue}"/> (the real client) and <see cref="MockProducer{TKey, TValue}"/>
/// (a broker-free test helper). The blocking mirror of <see cref="IAsyncProducer{TKey, TValue}"/>, exactly
/// as the sync <see cref="IConsumer{TKey, TValue}"/> is the blocking mirror of
/// <see cref="IAsyncConsumer{TKey, TValue}"/>. The C#-idiomatic <c>I</c> prefix marks the interface
/// (Framework Design Guidelines / analyzer CA1715); the sync-vs-async distinction is carried by the
/// interface/type (bare <c>IProducer</c> = sync; <c>IAsyncProducer</c> = async), not a method
/// suffix — method names mirror Java.
/// </summary>
/// <remarks>
/// <para>
/// <b>Generic-only (M11/P5, PLAN §3.1).</b> The producer surface is generic-only, mirroring the
/// consumer's M6/P1b conversion — there is no bytes-specialized sibling. Bytes users write
/// <c>IProducer&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>. Each generic interface
/// is flat and carries all its members — there is <b>no</b> <c>IProducerCommon</c> base (unlike the
/// consumer's <c>IConsumerCommon</c>): <see cref="Flush"/> / <see cref="Close"/> /
/// <see cref="PartitionsFor"/> have different signatures on the sync vs async interface, so there is
/// no sharing opportunity —
/// the one identical member, <see cref="Metrics"/>, is simply declared on both (M11/P8
/// decision D-6).
/// </para>
/// <para>
/// <b>Blocking, direct sync C ABI (M11/P4).</b> Every operation calls the synchronous C ABI
/// directly; the core's blocking call parks the caller thread inside the Rust multi-thread runtime
/// (deadlock-free, ffi §A1) — this is <b>not</b> the forbidden managed sync-over-async, and is
/// never routed through the async binding API. It is the sync-consumer precedent
/// (<c>consumer-threading.md §1.1</c>) applied to the producer.
/// </para>
/// <para>
/// <b>Send serializes then blocks and returns the metadata directly</b> (= Java
/// <c>send(record).get()</c>) — not a <see cref="System.Threading.Tasks.Task{TResult}"/>. The key /
/// value are serialized on the caller's thread before the blocking send; a serializer throw surfaces
/// synchronously as a <see cref="SerializationException"/>. .NET has only one future type
/// (<see cref="System.Threading.Tasks.Task{TResult}"/>), which the async
/// <see cref="IAsyncProducer{TKey, TValue}.Send"/> already returns; a <see cref="System.Threading.Tasks.Task"/>
/// here would clone the async surface and erase the sync/async split. Callers who want pipelined,
/// future-returning sends use <see cref="IAsyncProducer{TKey, TValue}"/>. This deliberately diverges from
/// Python's sync producer (whose <c>send</c> returns a <c>concurrent.futures.Future</c>) — forced by .NET's
/// single future type (PLAN §3 decision #1).
/// </para>
/// <para>
/// <b>No <see cref="System.Threading.CancellationToken"/> anywhere</b> (decision #4): the producer
/// has no <c>wakeup()</c>, so there is no interruption primitive to expose — a blocked operation
/// runs to native completion, the single-owner model of the sync consumer.
/// </para>
/// <para>
/// <b>Concurrent <see cref="Send"/> is supported — do NOT add your own lock.</b> The Rust core
/// serializes through the producer's internal <c>Mutex</c>, so calling <see cref="Send"/> from
/// multiple threads on one instance is correct (ffi §A1, which lists a binding-side send lock as an
/// anti-pattern). This is the opposite of the consumer, which is genuinely single-owner. (The
/// manual-mock completion pattern — one thread blocked in <see cref="Send"/>, another calling
/// <see cref="MockProducer{TKey, TValue}.CompleteNext"/> — is the intended cross-thread use and is
/// safe; it is an instance of the general rule, not an exception to it.)
/// </para>
/// <para>
/// <b>The single-owner constraint applies to TEARDOWN.</b> <see cref="Close"/> and
/// <see cref="IDisposable.Dispose"/> are one-shot and latched (the first caller wins and performs
/// the teardown; later or concurrent callers no-op), so do not race teardown against itself, and
/// treat "dispose while my own operations are still running" as misuse — it is memory-safe, but a
/// send racing teardown may fault rather than complete.
/// </para>
/// <para>
/// <b>Corrected in M11/P8 (Minor 13).</b> This paragraph previously read "Single-owner / not
/// thread-safe … do not share one instance across threads without external synchronization", which
/// contradicted ffi §A1 twice over — and contradicted the very next sentence here. A user who
/// believed it would add the lock the rules explicitly forbid and lose the concurrency the core is
/// built to provide.
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="Close"/> is the explicit graceful close that <em>surfaces</em> a
/// close failure; <see cref="IDisposable.Dispose"/> is the teardown that swallows it. Both are
/// idempotent. There is no <c>DisposeAsync</c> — this is the synchronous surface (<c>: IDisposable</c>
/// only).
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public interface IProducer<TKey, TValue> : IDisposable
{
    /// <summary>
    /// Serializes and publishes <paramref name="record"/> to its topic and <b>blocks</b> until the
    /// cluster acknowledges it, returning the published record's <see cref="RecordMetadata"/> directly
    /// (Java <c>Producer.send(record).get()</c> — decision #1). The key / value are serialized on the
    /// caller's thread before the send; the serialized bytes are copied into the send buffer during
    /// the call, so the caller may reuse or mutate them the moment this returns (ffi §A4).
    /// </summary>
    /// <param name="record">The record to publish.</param>
    /// <returns>The published record's <see cref="RecordMetadata"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The send failed (synchronous validation or delivery).</exception>
    RecordMetadata Send(ProducerRecord<TKey, TValue> record);

    /// <summary>
    /// Flushes all pending records and <b>blocks</b> until the core resolves the flush (Java
    /// <c>Producer.flush()</c>). Surfaces a flush failure as a <see cref="KafkaException"/>.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a flush failure.</exception>
    void Flush();

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/>, <b>blocking</b> until the core
    /// resolves it (Java <c>Producer.partitionsFor(String)</c>). On a <see cref="MockProducer{TKey, TValue}"/>
    /// this succeeds broker-free but returns an <b>empty</b> list for every topic (the honest reachability
    /// caveat — a populated list is integration-only).
    /// </summary>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <returns>The topic's partitions (empty on a <see cref="MockProducer{TKey, TValue}"/>).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    /// <summary>
    /// Returns a point-in-time snapshot of the producer's metrics (Java
    /// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c>). <b>Stays synchronous</b> on both
    /// producer interfaces — <c>metrics()</c> does not block in Java (CLAUDE.md §4 lists producer
    /// <c>Metrics</c> under "stays sync"), so it is not a <see cref="System.Threading.Tasks.Task"/>
    /// even on the async surface. A <b>method</b>, not a property, per the same FDG rule as the
    /// consumer's <c>Metrics()</c> / <c>Assignment()</c>: it does a P/Invoke, can throw, and
    /// returns a fresh owned snapshot per call.
    /// </summary>
    /// <remarks>
    /// Declared identically on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/> rather than on a shared base: the producer has no
    /// <c>IProducerCommon</c> (unlike the consumer's <c>IConsumerCommon</c>) and one shared member
    /// does not justify introducing one (M11/P8 decision D-6, DoD §7). On a
    /// <see cref="MockProducer{TKey, TValue}"/> the snapshot is <b>empty</b> (Java
    /// <c>MockProducer.metrics()</c> parity — its metric map is empty unless seeded, and the ABI
    /// exposes no seeding entry point).
    /// </remarks>
    /// <returns>The producer's metrics, keyed by <see cref="MetricName"/> value identity.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();

    /// <summary>
    /// Closes the producer gracefully, then releases its resources (Java <c>Producer.close()</c>).
    /// Surfaces a close failure (unlike <see cref="IDisposable.Dispose"/>, which swallows it).
    /// Idempotent. There is no <c>Close(TimeSpan)</c> overload — the producer ABI exposes no timed
    /// close, and this matches Python's producer <c>close()</c> (decision #5).
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close();
}
