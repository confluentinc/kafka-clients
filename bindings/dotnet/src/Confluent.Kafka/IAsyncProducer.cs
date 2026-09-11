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
/// The async, typed Kafka producer surface — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.Producer&lt;K, V&gt;</c>, implemented by
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> (the real client) and
/// <see cref="AsyncMockProducer{TKey, TValue}"/> (a broker-free test helper). The C#-idiomatic
/// <c>I</c> prefix marks the interface (Framework Design Guidelines / analyzer CA1715); the
/// <c>Async</c> prefix on the type marks this async surface (the sync mirror is
/// <see cref="IProducer{TKey, TValue}"/>). Method names carry <b>no <c>Async</c> suffix</b> — the
/// sync-vs-async distinction is carried by the interface/type, matching Java's method names and
/// the Python sibling.
/// </summary>
/// <remarks>
/// <para>
/// <b>Generic-only (M11/P5, PLAN §3.1).</b> The producer surface is generic-only, mirroring the
/// consumer's M6/P1b conversion — there is no bytes-specialized sibling. Bytes users write
/// <c>IAsyncProducer&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>. Each generic
/// interface is flat and carries all its members — there is <b>no</b> <c>IProducerCommon</c> base
/// (unlike the consumer's <c>IConsumerCommon</c>): <see cref="Flush"/> / <see cref="Close(CancellationToken)"/>
/// / <see cref="PartitionsFor"/> have different signatures on the sync vs async interface, so there
/// is no sharing opportunity —
/// the one identical member, <see cref="Metrics"/>, is simply declared on both (M11/P8
/// decision D-6).
/// </para>
/// <para>
/// <b>Subset of Java's <c>Producer</c>.</b> The send path (<c>Send</c> with
/// <see cref="ProducerRecord{TKey, TValue}"/> / <see cref="RecordMetadata"/>) plus the async
/// peripherals (<see cref="Flush"/> / <see cref="Close(CancellationToken)"/> /
/// <see cref="PartitionsFor"/>) and the synchronous <see cref="Metrics"/> state read (M11/P8).
/// Transactions remain deferred.
/// </para>
/// <para>
/// <b>Both of Java's <c>send</c> signatures are present (M14/P1).</b>
/// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback, CancellationToken)"/> mirrors
/// Java's <c>send(ProducerRecord, Callback)</c> (<c>Producer.java:86</c>): the callback is an
/// <b>additional</b> parameter — the overload still returns the
/// <see cref="Task{TResult}"/>. See <see cref="IDeliveryCallback"/> and the §4
/// <b>delivery-callback divergence</b> for the thread, ordering, non-null-metadata and throw-policy
/// contracts.
/// </para>
/// <para>
/// ⚠ <b><c>Send</c> can BLOCK the calling thread under sustained saturation (M11/P3.3).</b> This
/// producer accepts a bounded number of records that have not yet been handed to its send-batch
/// thread; once that bound is reached, <c>Send</c> waits for capacity for up to the configured
/// <c>max.block.ms</c> (default 60 s). Blocking that long is Java's own contract —
/// <c>KafkaProducer.send()</c> blocks up to <c>max.block.ms</c> once its accumulator is full — and
/// it is what keeps the client's memory and latency bounded: because
/// the <see cref="Task{TResult}"/> returned here is the <em>record's delivery</em> future rather
/// than an admission handle, a caller never awaits admission, so an asynchronous wait would
/// throttle nobody. The bound is sized so that reaching it is rare in practice; a producer that
/// hits it regularly is offering records faster than its cluster can accept them. The
/// <b>synchronous</b> <see cref="IProducer{TKey, TValue}"/> has no such window — it hands each
/// record to the core inside the call, where the core's own <c>buffer.memory</c> provides the
/// backpressure.
/// </para>
/// <para>
/// <b>If that wait expires, the record is refused the way Java refuses it: a failed future plus a
/// callback, NOT a throw.</b> <c>Send</c> returns normally and the returned
/// <see cref="Task{TResult}"/> faults with a <see cref="KafkaException"/> whose
/// <see cref="KafkaException.IsRetriable"/> is <see langword="true"/>; any
/// <see cref="IDeliveryCallback"/> supplied for that send fires first, with the placeholder
/// metadata. Java's accumulator-memory wait throws <c>BufferExhaustedException</c>, which extends
/// <c>TimeoutException</c> → <c>RetriableException</c> → <c>ApiException</c>, so it is caught by
/// <c>doSend</c>'s <c>catch (ApiException)</c> (<c>KafkaProducer.java:1049-1061</c>), which fires
/// the callback with the <c>-1</c> placeholder and returns a failed future without rethrowing. So
/// the idiomatic <c>catch (KafkaException e) when (e.IsRetriable)</c> retry works here exactly as
/// it does in Java. (⚠ Until this was corrected, the binding threw synchronously with
/// <see cref="KafkaException.Code"/> <c>0</c> and <see cref="KafkaException.IsRetriable"/>
/// <see langword="false"/>, and these remarks asserted that <em>was</em> Java's shape.)
/// </para>
/// <para>
/// <b>Cancellation is best-effort (no native abort).</b> Unlike the consumer, the producer has
/// no <c>wakeup()</c>: a canceled <see cref="CancellationToken"/> cancels the returned
/// <see cref="Task"/>'s .NET-side wait, but does not abort the in-flight native op — it
/// continues to completion (a host-idiom addition Java lacks; CLAUDE.md §4). A token that fires
/// while <c>Send</c> is blocked waiting for capacity <em>does</em> abort that wait, in which case
/// the record is not sent at all.
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="IAsyncDisposable.DisposeAsync"/> is the primary path (graceful
/// async close then destroy; swallows any close error); <see cref="IDisposable.Dispose"/> is
/// the blocking fallback (graceful sync close then destroy). Both are idempotent, and a close
/// error never prevents the underlying handle from being freed.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public interface IAsyncProducer<TKey, TValue> : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Serializes and publishes <paramref name="record"/> to its topic, completing when the cluster
    /// acknowledges it (Java <c>Producer.send(record)</c> — which returns a
    /// <c>Future&lt;RecordMetadata&gt;</c>, so a <see cref="Task{TResult}"/> here, CLAUDE.md §4). The
    /// key / value are serialized on the caller's thread <b>before</b> the send is enqueued.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Do not mutate the key / value buffers after this returns.</b> The send is deferred: the
    /// binding <b>borrows</b> the serialized bytes — it does not copy them — until a background
    /// batch thread hands the record to the core, which happens within milliseconds but not before
    /// this method returns. A mutation in that window <b>is</b> visible on the wire. If you need to
    /// reuse a buffer, either await this task first or hand each send its own array.
    /// <para>
    /// This is inherent to a zero-copy send path: the alternative is a per-record copy of every
    /// value, which is exactly the allocation this binding exists to avoid. It matches the Python
    /// binding, whose <c>send()</c> likewise borrows the buffer until its drain. The <b>synchronous</b>
    /// <c>IProducer.Send</c> has no such window — it hands the record to the core inside the call.
    /// </para>
    /// <para>
    /// ⚠ <b>Under sustained saturation this BLOCKS the calling thread, for up to the configured
    /// <c>max.block.ms</c>; if the bound is still full when that elapses, the returned
    /// <see cref="Task{TResult}"/> faults with a retriable <see cref="KafkaException"/> rather than
    /// this call throwing one</b> — Java's own <c>send()</c> contract on both halves, and the only
    /// thing that bounds an async producer whose caller never awaits admission. The type's remarks
    /// state the contract in full; it applies to the <b>async</b> surface only.
    /// </para>
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="cancellationToken">
    /// Best-effort cancellation of the .NET wait. An already-canceled token throws
    /// <see cref="OperationCanceledException"/> before the send is enqueued; a token that fires
    /// afterwards cancels the returned <see cref="Task{TResult}"/> but does <b>not</b> abort the
    /// in-flight native send (the producer has no <c>wakeup()</c>). A token that fires while this
    /// call is blocked waiting for accumulator capacity aborts that wait instead, and the record is
    /// then not sent at all.
    /// </param>
    /// <returns>
    /// A task resolving with the published record's <see cref="RecordMetadata"/>, or faulting with
    /// a <see cref="KafkaException"/> — including the <b>retriable</b> one that reports "the
    /// producer's accumulator stayed full for the whole of the configured <c>max.block.ms</c>, so
    /// this record was not accepted" (see the remarks).
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value (thrown synchronously).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    Task<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes and publishes <paramref name="record"/>, completing when the cluster acknowledges
    /// it, and additionally invokes <paramref name="callback"/> with the outcome — Java's second
    /// <c>send</c> signature, <c>Future&lt;RecordMetadata&gt; send(ProducerRecord, Callback)</c>
    /// (<c>Producer.java:86</c>). The callback is an <b>additional</b> parameter, not an
    /// alternative: this still returns the <see cref="Task{TResult}"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Declared identically on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/> rather than on a shared base — the producer has no
    /// <c>IProducerCommon</c> and the two signatures differ anyway. Same precedent as
    /// <see cref="Metrics"/> (M11/P8 decision D-6).
    /// </para>
    /// <para>
    /// <b>On this async surface the callback runs on the producer's send-completion pump thread</b>
    /// — the .NET analogue of the "background I/O thread" Java documents
    /// (<c>Callback.java:20-21</c>) — and <b>before</b> the returned
    /// <see cref="Task{TResult}"/> is completed (M14/P1 decision D3, Java's
    /// <c>ProducerBatch.java:303-323</c> ordering). It is one thread per producer, so a slow
    /// callback delays every other in-flight send's completion; keep it short.
    /// <see cref="IDeliveryCallback"/> documents the full contract: non-null metadata on every path,
    /// which outcomes fire it (a synchronous throw out of <c>Send</c> does not; a resolved-then-failed
    /// delivery does, and so does a send whose <see cref="Task{TResult}"/> was already canceled), and
    /// the swallow-and-trace policy for a throwing callback.
    /// </para>
    /// <para>
    /// ⚠ <b>Do not mutate the key / value buffers after this returns</b> — the same borrow window as
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/>, described in full there.
    /// </para>
    /// <para>
    /// ⚠ <b>And it BLOCKS the calling thread under sustained saturation</b>, for up to the
    /// configured <c>max.block.ms</c>, exactly as
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/> does — the bound is on
    /// the producer, not on the overload. When that wait expires <paramref name="callback"/>
    /// <b>does</b> fire, with the placeholder metadata and the retriable
    /// <see cref="KafkaException"/> that also faults the returned <see cref="Task{TResult}"/> —
    /// Java fires its <c>Callback</c> for exactly this condition
    /// (<c>KafkaProducer.java:1049-1061</c>). What fires <b>no</b> callback is an exception
    /// <em>thrown out of</em> this call, per <see cref="IDeliveryCallback"/>'s contract for a
    /// synchronous throw.
    /// </para>
    /// <para>
    /// <b>A null <paramref name="callback"/> is rejected (decision D8) — deliberately stricter than
    /// Java</b>, which accepts <c>send(record, null)</c> (<c>KafkaProducer.java:1058</c> guards
    /// <c>if (callback != null)</c>). The parameter is non-nullable under <c>#nullable enable</c>,
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/> already spells "no
    /// callback", and ffi §A5 mandates precondition validation before the FFI call.
    /// </para>
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="callback">The delivery callback (non-null).</param>
    /// <param name="cancellationToken">
    /// Best-effort cancellation of the .NET wait, exactly as on
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/>. Cancelling the wait does
    /// <b>not</b> cancel the callback obligation: <paramref name="callback"/> still fires when the
    /// core reports the send's completion.
    /// </param>
    /// <returns>
    /// A task resolving with the published record's <see cref="RecordMetadata"/>, or faulting with
    /// a <see cref="KafkaException"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="record"/> or <paramref name="callback"/> is null.
    /// </exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value (thrown synchronously).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    Task<RecordMetadata> Send(
        ProducerRecord<TKey, TValue> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes all pending records, completing when the core resolves the flush (Java
    /// <c>Producer.flush()</c>). Blocks in Java → a <see cref="Task"/> here (CLAUDE.md §4).
    /// </summary>
    /// <param name="cancellationToken">Best-effort cancellation of the .NET wait (no native abort).</param>
    /// <returns>A task that completes when the flush resolves, or faults with a <see cref="KafkaException"/>.</returns>
    Task Flush(CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the producer gracefully, then releases its resources (Java
    /// <c>Producer.close()</c>). Surfaces a close failure (unlike disposal, which swallows it).
    /// Idempotent.
    /// </summary>
    /// <param name="cancellationToken">
    /// Observed before the close is submitted (an already-canceled token throws
    /// <see cref="OperationCanceledException"/>); once the close is in flight the graceful join
    /// runs to completion.
    /// </param>
    /// <returns>A task that completes when the close resolves, or faults with a <see cref="KafkaException"/>.</returns>
    Task Close(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (Java
    /// <c>Producer.partitionsFor(String)</c>). Blocks in Java (a metadata fetch) → a
    /// <see cref="Task"/> here.
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
}
