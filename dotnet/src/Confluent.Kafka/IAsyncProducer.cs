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
/// <b>additional</b> parameter — the overload still yields the record's
/// <see cref="AsyncKafkaFuture{T}"/> (whose <see cref="AsyncKafkaFuture{T}.Get"/> is the delivery
/// <see cref="Task{TResult}"/>) from the same <see cref="ValueTask{TResult}"/> first stage. See
/// <see cref="IDeliveryCallback"/> and the §4 <b>delivery-callback divergence</b> for the thread,
/// ordering, non-null-metadata and throw-policy contracts.
/// </para>
/// <para>
/// ⚠ <b><c>Send</c> returns in two stages, and awaiting the first is what throttles you (M11/P3.5).</b>
/// <c>Send</c> appends the record to the producer's batch chain and returns a <see cref="ValueTask{TResult}"/>
/// that completes when the record is <b>accepted</b> under the producer's bound on records not yet handed to its
/// send-batch thread. Its result is the record's <see cref="AsyncKafkaFuture{T}"/> — Java's
/// <c>Future&lt;RecordMetadata&gt;</c>; its <see cref="AsyncKafkaFuture{T}.Get"/> is the <b>delivery</b>
/// <see cref="Task{TResult}"/>. The first stage is usually complete when <c>Send</c> returns; once the bound
/// is reached it stays pending until capacity frees. It never parks the calling thread. This is Java's blocking
/// <c>send()</c> on an async surface, in the Python binding's async <c>send</c> order: append, then wait for space.
/// <code>
/// AsyncKafkaFuture&lt;RecordMetadata&gt; future = await producer.Send(record);   // accepted
/// RecordMetadata metadata = await future.Get();                                // delivered
/// </code>
/// The bound is sized so that reaching it is rare in practice; a producer that hits it regularly is offering records
/// faster than its cluster can accept them. The <b>synchronous</b> <see cref="IProducer{TKey, TValue}"/> has no such
/// window — it hands each record to the core inside the call, where the core's own <c>buffer.memory</c> provides the
/// backpressure.
/// </para>
/// <para>
/// ⚠ <b>A caller that does not await the first stage is NOT throttled.</b> The record is appended before <c>Send</c>
/// returns, so dropping or deferring the returned <see cref="ValueTask{TResult}"/> still sends it — but then nothing
/// slows the caller: every such send is accepted, and its record, its buffers and a pending admission wait (about
/// 1.5 KB per send, measured) accumulate until the cluster drains them. Offered faster than the cluster accepts,
/// memory grows without bound, and <c>Flush</c> / <c>Close</c> take as long as that backlog takes to drain. This
/// matches the Python binding's async <c>send</c>. If you do not await acceptance, bound your own outstanding sends.
/// </para>
/// <para>
/// <b>The first stage has no timeout and never fails on its own</b> (only your own token can end it early — see
/// Cancellation). The record is accepted before the wait begins (M11/P3.4), so saturation delays acceptance rather
/// than refusing the record. The first stage completes when capacity frees, or when the producer is torn down — and
/// teardown does not discard the record: it is settled by the closing producer's drain (sent on a normal close;
/// faulted if the send-batch thread itself failed) and reported through the delivery task. Buffer exhaustion inside
/// the core (Java's <c>max.block.ms</c> case) is reported the same way, by faulting the delivery task.
/// </para>
/// <para>
/// <b>Cancellation is best-effort (no native abort), and it never un-sends a record.</b> Unlike the consumer, the
/// producer has no <c>wakeup()</c>: a canceled token cancels the .NET-side waits but does not abort the native send
/// (a host-idiom addition Java lacks; CLAUDE.md §4). ⚠ A token that is <b>already</b> canceled when you call
/// <c>Send</c> throws <see cref="OperationCanceledException"/> synchronously and nothing is appended. A token that
/// fires while the first stage is pending ends that stage with <see cref="OperationCanceledException"/> carrying the
/// token — but the record was appended before the wait began, so it is <b>still sent</b>, its delivery callback still
/// fires, and its delivery task is cancelled (as in the Python binding's async <c>send</c>). Its buffers stay borrowed
/// too; see <see cref="Send(ProducerRecord{TKey,TValue}, CancellationToken)"/>'s remarks. Consequences:
/// <list type="bullet">
/// <item><description>A canceled or timed-out send may still be delivered. Use the delivery callback
/// (<see cref="Send(ProducerRecord{TKey,TValue}, IDeliveryCallback, CancellationToken)"/>) to learn the
/// outcome.</description></item>
/// <item><description>Inside <c>await producer.Send(record, token)</c> the two cases look alike: an
/// <see cref="OperationCanceledException"/> means "not appended" only for an already-canceled token; otherwise the
/// record is still sent, so <b>retrying after an <see cref="OperationCanceledException"/> can duplicate it</b>. (The
/// same duplicate-on-retry risk exists at the delivery stage, and in Java's <c>future.get(timeout)</c>.)</description></item>
/// <item><description>A caller whose token ends the first stage is not throttled for that send: a short per-send
/// token while the producer is stuck lets records accumulate, one per token period — as with
/// <c>asyncio.wait_for</c> in Python, and like a caller that does not await the first stage at all
/// (above).</description></item>
/// </list>
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
    /// Serializes and publishes <paramref name="record"/> to its topic (Java
    /// <c>Producer.send(record)</c>, which may block and returns a
    /// <c>Future&lt;RecordMetadata&gt;</c> — so a <see cref="ValueTask{TResult}"/> yielding an
    /// <see cref="AsyncKafkaFuture{T}"/> here, CLAUDE.md §4). Awaiting the call once yields the record's
    /// <see cref="AsyncKafkaFuture{T}"/>; awaiting its <see cref="AsyncKafkaFuture{T}.Get"/> yields the
    /// <see cref="RecordMetadata"/> once the cluster
    /// acknowledges the record. The key / value are serialized on the caller's thread
    /// <b>before</b> the send is enqueued.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Do not mutate the key / value buffers until the delivery <see cref="Task{TResult}"/>
    /// (<see cref="AsyncKafkaFuture{T}.Get"/>) completes without being canceled.</b> The first stage
    /// completing does <b>not</b> end the borrow: an accepted record may not have reached the core yet.
    /// The send is deferred — the binding <b>borrows</b> the serialized
    /// bytes, it does not copy them, until a background batch thread hands the record to the core —
    /// so a mutation in that window <b>is</b> visible on the wire. If you need to reuse a buffer,
    /// either await the delivery task (<see cref="AsyncKafkaFuture{T}.Get"/>) first and see it complete
    /// without being canceled, or hand each
    /// send its own array.
    /// <para>
    /// ⚠ <b>A cancellation does not end the borrow either.</b> After a cancellation — an
    /// <see cref="OperationCanceledException"/> from either stage, or a delivery task that completes
    /// canceled — the record may still be inside the binding, not yet handed to the core, and it is
    /// still sent (see the type's remarks on cancellation). Its buffers stay borrowed until its
    /// delivery callback fires (the
    /// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback, CancellationToken)"/>
    /// overload) or a later <see cref="Flush"/> completes successfully.
    /// <see cref="Close(CancellationToken)"/>, <see cref="IDisposable.Dispose"/> or
    /// <see cref="IAsyncDisposable.DisposeAsync"/> returning is <b>not</b> that guarantee: teardown's
    /// wait for the send-batch thread is bounded, and past that bound it returns while the thread may
    /// still be handing records to the core. (An already-canceled token throws before anything is
    /// appended, so it leaves nothing borrowed.)
    /// </para>
    /// <para>
    /// This is inherent to a zero-copy send path: the alternative is a per-record copy of every
    /// value, which is exactly the allocation this binding exists to avoid. It matches the Python
    /// binding, whose <c>send()</c> likewise borrows the buffer until its drain. The <b>synchronous</b>
    /// <c>IProducer.Send</c> has no such window — it hands the record to the core inside the call.
    /// </para>
    /// <para>
    /// ⚠ <b>Under sustained saturation the first stage waits until capacity frees; the calling
    /// thread does not.</b> Await the first stage before the next send to be throttled by it; a
    /// caller that does not is not throttled (Python parity). The record is appended before the wait
    /// starts, so saturation delays the first stage rather than refusing the record. The type's
    /// remarks state the contract in full; it applies to the <b>async</b> surface only.
    /// </para>
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="cancellationToken">
    /// Best-effort cancellation of the .NET waits (no native abort — the producer has no
    /// <c>wakeup()</c>). An already-canceled token throws <see cref="OperationCanceledException"/>
    /// synchronously and nothing is appended. A token that fires afterwards cancels the record's
    /// delivery <see cref="Task{TResult}"/>, and ends a first stage that is still pending with an
    /// <see cref="OperationCanceledException"/> carrying the token — ⚠ but the record is <b>still
    /// sent</b> (see the type's remarks on cancellation: retrying can duplicate it).
    /// </param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that completes once the record is accepted, yielding the
    /// record's <see cref="AsyncKafkaFuture{T}"/>; await its <see cref="AsyncKafkaFuture{T}.Get"/> for
    /// the <see cref="RecordMetadata"/>, or for a <see cref="KafkaException"/> carrying the delivery
    /// failure. Await the <see cref="ValueTask{TResult}"/> once; call
    /// <see cref="ValueTask{TResult}.AsTask"/> to store it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value (thrown synchronously).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes and publishes <paramref name="record"/>, exactly as
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/> does, and additionally
    /// invokes <paramref name="callback"/> with the outcome — Java's second <c>send</c> signature,
    /// <c>Future&lt;RecordMetadata&gt; send(ProducerRecord, Callback)</c> (<c>Producer.java:86</c>).
    /// The callback is an <b>additional</b> parameter, not an alternative: this still returns the
    /// record's <see cref="AsyncKafkaFuture{T}"/>, inside the same <see cref="ValueTask{TResult}"/>.
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
    /// (<c>Callback.java:20-21</c>) — and <b>before</b> the record's delivery
    /// <see cref="Task{TResult}"/> is completed (M14/P1 decision D3, Java's
    /// <c>ProducerBatch.java:303-323</c> ordering). It is one thread per producer, so a slow
    /// callback delays every other in-flight send's completion; keep it short.
    /// <see cref="IDeliveryCallback"/> documents the full contract: non-null metadata on every path,
    /// which outcomes fire it (a synchronous throw out of <c>Send</c> does not; a resolved-then-failed
    /// delivery does, and so does a send whose delivery <see cref="Task{TResult}"/> was already canceled), and
    /// the swallow-and-trace policy for a throwing callback.
    /// </para>
    /// <para>
    /// ⚠ <b>Do not mutate the key / value buffers until the delivery <see cref="Task{TResult}"/>
    /// (<see cref="AsyncKafkaFuture{T}.Get"/>) completes without being canceled</b> — the first stage
    /// completing does <b>not</b> end the borrow (an accepted record may not have reached the core yet),
    /// and neither does a cancellation: after an
    /// <see cref="OperationCanceledException"/> from either stage, or a canceled delivery task, the
    /// record may still be inside the binding, and its buffers stay borrowed until
    /// <paramref name="callback"/> fires or a later <see cref="Flush"/> completes successfully.
    /// <see cref="Close(CancellationToken)"/> or disposal returning is <b>not</b> that guarantee. The
    /// same borrow window as <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/>,
    /// described in full there.
    /// </para>
    /// <para>
    /// ⚠ <b>And its first stage waits under sustained saturation</b>, exactly as
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/>'s does — the bound is on
    /// the producer, not on the overload. Saturation does not refuse the record, so it produces no
    /// <paramref name="callback"/> invocation of its own; the record's eventual delivery outcome
    /// fires it as usual. What fires <b>no</b> callback is an exception <em>thrown out of</em> this
    /// call, per <see cref="IDeliveryCallback"/>'s contract for a synchronous throw.
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
    /// Best-effort cancellation of the .NET waits, exactly as on
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/>. Cancelling a wait does
    /// <b>not</b> cancel the callback obligation: <paramref name="callback"/> still fires when the
    /// core reports the send's completion — including for a send whose first stage the token ended,
    /// whose record is still sent.
    /// </param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that completes once the record is accepted, yielding the
    /// record's <see cref="AsyncKafkaFuture{T}"/>; await its <see cref="AsyncKafkaFuture{T}.Get"/> for
    /// the <see cref="RecordMetadata"/>, or for a <see cref="KafkaException"/> carrying the delivery
    /// failure. Await the <see cref="ValueTask{TResult}"/> once; call
    /// <see cref="ValueTask{TResult}.AsTask"/> to store it.
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
