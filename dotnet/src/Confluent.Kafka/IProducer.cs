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
/// (a broker-free test helper). The synchronous mirror of <see cref="IAsyncProducer{TKey, TValue}"/>, exactly
/// as the sync <see cref="IConsumer{TKey, TValue}"/> is the synchronous mirror of
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
/// <b>Direct sync C ABI (M11/P4).</b> Every operation calls the synchronous C ABI directly; the
/// core's blocking call parks the calling thread inside the Rust multi-thread runtime (deadlock-free,
/// ffi §A1) — this is <b>not</b> the forbidden managed sync-over-async, and is never routed through
/// the async binding API. It is the sync-consumer precedent (<c>consumer-threading.md §1.1</c>) applied
/// to the producer. A sync producer starts exactly one background thread, its send-completion pump,
/// on its first <c>Send</c>, and joins it at teardown (M11/P4.2 decision D4). The pump reads each
/// send's outcome, and <see cref="KafkaFuture{T}.Get"/> waits on a managed latch that the pump
/// completes, not on a <see cref="System.Threading.Tasks.Task"/>.
/// </para>
/// <para>
/// <b><c>Send</c> returns a <see cref="KafkaFuture{T}"/>, as Java's <c>send</c> returns a
/// <c>Future</c> and Python's sync <c>send</c> returns a <c>concurrent.futures.Future</c> (M11/P4.2
/// decision D5).</b> <c>Send</c> serializes the key / value on the caller's thread (a serializer
/// throw surfaces synchronously as a <see cref="SerializationException"/>), hands the record to the
/// core, and returns once the core has accepted it. It blocks only while <c>buffer.memory</c> is
/// full, up to <c>max.block.ms</c> (Java's <c>send</c> blocking), and the key / value buffers are
/// reusable when it returns. <see cref="KafkaFuture{T}.Get"/> then blocks until the record is
/// acknowledged or fails (Java's <c>future.get()</c>), and rethrows a failure unwrapped. This
/// supersedes M11/P4 decision #1, under which <c>Send</c> blocked and returned the
/// <see cref="RecordMetadata"/> directly. That decision's premise — a future-returning sync
/// <c>Send</c> would clone the async surface, "forced by .NET's single Task type" — has been false
/// since M11/P3.5/P3.6 gave <see cref="IAsyncProducer{TKey, TValue}"/> its own two-stage shape: a
/// <see cref="System.Threading.Tasks.ValueTask{TResult}"/> admission yielding an
/// <see cref="AsyncKafkaFuture{T}"/>, whose <see cref="AsyncKafkaFuture{T}.Get"/> is a
/// <see cref="System.Threading.Tasks.Task{TResult}"/>.
/// </para>
/// <para>
/// <b>⚠ Migration (M11/P4.2 decision D12).</b> A statement <c>producer.Send(r);</c> that used to
/// block until delivery still compiles, and is now fire-and-forget: nothing flags a discarded
/// <see cref="KafkaFuture{T}"/>. Write <c>producer.Send(r).Get();</c> to keep the old meaning. Where
/// fire-and-forget is the intent, <c>_ = producer.Send(r);</c> says so; discarding the future leaks
/// nothing, and a callback passed to <c>Send</c> still fires.
/// </para>
/// <para>
/// <b>Rejected designs (M11/P4.2 S-3).</b> Two alternatives to the inline send plus pump were
/// rejected. Reusing the async engine with a blocking admission wait would break the sync guarantee
/// that the key / value buffers are reusable when <c>Send</c> returns, and would make the manual-mock
/// <c>Send</c> → <c>CompleteNext</c> → <c>Get</c> sequence racy. The core's callback-taking send
/// would need a reverse P/Invoke and a <c>GCHandle</c> per send, and would bring back the push
/// engine's teardown residuals.
/// </para>
/// <para>
/// <b>Completion order (M11/P4.2 decision D1).</b> One producer completes its sends in the order
/// <c>Send</c> returned. A slow partition therefore delays the <c>Get</c> returns and callbacks of
/// later sends on the same producer — not their sending — by milliseconds in steady state, and by up
/// to <c>delivery.timeout.ms</c> at worst.
/// </para>
/// <para>
/// <b><see cref="Flush"/> waits for the core, not for the callbacks (M11/P4.2 decision D8).</b> When
/// it returns, the core has resolved every earlier send, but their callbacks can trail it by the
/// pump's lag; Java's <c>flush()</c> also guarantees they have fired. The async surface and Python
/// behave the same way. A <c>Get()</c> after <c>Flush</c> does not wait on the cluster, only for the
/// pump to reach that send; when it returns, that send's callback has run.
/// </para>
/// <para>
/// <b>Both of Java's <c>send</c> signatures are present (M14/P1).</b>
/// <see cref="Send(ProducerRecord{TKey, TValue}, IDeliveryCallback)"/> mirrors Java's
/// <c>send(ProducerRecord, Callback)</c> (<c>Producer.java:86</c>): the callback is an
/// <b>additional</b> parameter — the overload still returns the <see cref="KafkaFuture{T}"/>. See
/// <see cref="IDeliveryCallback"/> and the §4 <b>delivery-callback divergence</b> for the thread,
/// ordering, non-null-metadata and throw-policy contracts.
/// </para>
/// <para>
/// <b>No <see cref="System.Threading.CancellationToken"/> anywhere</b> (decision #4): the producer
/// has no <c>wakeup()</c>, so there is no interruption primitive to expose — a blocked operation
/// runs to completion, the single-owner model of the sync consumer. That includes
/// <see cref="KafkaFuture{T}.Get"/>, which has no timed or cancellable form (M11/P4.2 decision D7).
/// </para>
/// <para>
/// <b>Concurrent <c>Send</c> is supported — do NOT add your own lock.</b> The Rust core
/// serializes through the producer's internal <c>Mutex</c>, so calling <c>Send</c> from
/// multiple threads on one instance is correct (ffi §A1, which lists a binding-side send lock as an
/// anti-pattern). This is the opposite of the consumer, which is genuinely single-owner.
/// </para>
/// <para>
/// <b>Manual mock completion takes one thread (M11/P4.2 decision D13).</b> On a
/// <see cref="MockProducer{TKey, TValue}"/> created with <c>autoComplete: false</c>, write
/// <c>var f = mock.Send(r); mock.CompleteNext(); f.Get();</c>. Unlike Java's synchronous
/// <c>completeNext</c>, the send's callback and its future's completion follow on the pump a moment
/// after <see cref="MockProducer{TKey, TValue}.CompleteNext"/> returns, so call <c>Get()</c> before
/// asserting on a callback: the callback has run by the time <c>Get()</c> returns.
/// </para>
/// <para>
/// <b>The single-owner constraint applies to TEARDOWN.</b> <see cref="Close"/> and
/// <see cref="IDisposable.Dispose"/> are one-shot and latched (the first caller wins and performs
/// the teardown; later or concurrent callers no-op), so do not race teardown against itself, and
/// treat "dispose while my own operations are still running" as misuse — it is memory-safe, but a
/// send racing teardown may fault rather than complete (its <c>Get()</c> then throws).
/// </para>
/// <para>
/// <b>Do not <see cref="Close"/> or <see cref="IDisposable.Dispose"/> the producer from inside a
/// delivery callback (M11/P4.2 §13 R6).</b> The callback runs on the producer's send-completion
/// pump, and teardown stops and <b>joins</b> that thread, so calling either from there asks the pump
/// to wait for itself. This holds on both producer surfaces (see <see cref="IDeliveryCallback"/>).
/// Java detects the case and closes with a zero timeout instead (<c>close(0)</c>, skipping the join);
/// this binding does not (follow-up FU-3). Close the producer from the thread that owns it.
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
    /// Serializes and publishes <paramref name="record"/> to its topic, returning once the core has
    /// accepted it (Java <c>Producer.send(record)</c> — M11/P4.2) with a <see cref="KafkaFuture{T}"/>
    /// whose <see cref="KafkaFuture{T}.Get"/> blocks until the cluster acknowledges the record and
    /// returns its <see cref="RecordMetadata"/> (Java <c>future.get()</c>). This call blocks only while
    /// <c>buffer.memory</c> is full, up to <c>max.block.ms</c>. The key / value are serialized on the
    /// caller's thread before the send; the serialized bytes are copied into the send buffer during
    /// the call, so the caller may reuse or mutate them the moment this returns (ffi §A4).
    /// </summary>
    /// <param name="record">The record to publish.</param>
    /// <returns>The send's delivery handle; its <see cref="KafkaFuture{T}.Get"/> returns the published
    /// record's <see cref="RecordMetadata"/> or rethrows the delivery failure.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record synchronously (a delivery failure
    /// surfaces from <see cref="KafkaFuture{T}.Get"/> instead).</exception>
    KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record);

    /// <summary>
    /// Serializes and publishes <paramref name="record"/>, returning a <see cref="KafkaFuture{T}"/> for
    /// its delivery, and additionally invokes <paramref name="callback"/> with the outcome —
    /// Java's second <c>send</c> signature, <c>Future&lt;RecordMetadata&gt; send(ProducerRecord,
    /// Callback)</c> (<c>Producer.java:86</c>). The callback is an <b>additional</b> parameter, not
    /// an alternative: this still returns the record's <see cref="KafkaFuture{T}"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Declared identically on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/> rather than on a shared base — the producer has no
    /// <c>IProducerCommon</c> and the two signatures differ anyway (the async one takes a
    /// <see cref="System.Threading.CancellationToken"/> and returns a
    /// <see cref="System.Threading.Tasks.ValueTask{TResult}"/> whose result is the record's
    /// <see cref="AsyncKafkaFuture{T}"/>). Same precedent as
    /// <see cref="Metrics"/> (M11/P8 decision D-6).
    /// </para>
    /// <para>
    /// <b>On this synchronous surface the callback runs on the producer's send-completion pump
    /// thread</b>, <b>before</b> the returned future's <see cref="KafkaFuture{T}.Get"/> returns or
    /// throws (M11/P4.2 decision D3, superseding M14/P1 decision D3, under which it ran inline on the
    /// calling thread before <c>Send</c> returned). <see cref="IDeliveryCallback"/> documents the full
    /// contract: non-null metadata on every path, which outcomes fire it (a throw out of <c>Send</c>
    /// does not; a resolved-then-failed delivery does), and the swallow-and-trace policy for a
    /// throwing callback.
    /// </para>
    /// <para>
    /// <b>One producer's callbacks are serialized on its pump, on both surfaces (M11/P4.2 decision
    /// D3).</b> On this surface every callback runs there, so concurrent <c>Send</c> calls on one
    /// producer do not enter a shared <paramref name="callback"/> at the same time. An instance shared
    /// across two producers is entered from each producer's pump, so it must be thread-safe;
    /// <see cref="IDeliveryCallback"/> has the full statement, including the async surface. A callback
    /// instance created per send needs no synchronization. ⚠ Until M11/P4.2 this paragraph made every
    /// shared instance a thread-safety obligation, because the callback then ran inline and N threads
    /// in <c>Send</c> entered it on N threads at once. It had also justified that by asserting Java
    /// never enters one callback from two threads, which is false: <c>Callback.java:20-21</c> says
    /// "<em>generally</em>", and <c>KafkaProducer.doSend</c>'s <c>catch (ApiException)</c> invokes the
    /// callback on the <b>application</b> thread concurrently with the Sender thread's own invocations
    /// (finding 72.17).
    /// </para>
    /// <para>
    /// <b>Inside the callback (M11/P4.2 decision D9)</b>, sending again is fine, but calling
    /// <c>Get()</c> on a not-yet-completed future of the same producer throws
    /// <see cref="InvalidOperationException"/> instead of deadlocking (Java's <c>get()</c> deadlocks
    /// there): the callback is running on the thread that must complete that send. Wait for it from
    /// another thread. Closing the producer from inside the callback is not supported either (see the
    /// remarks on <see cref="IProducer{TKey, TValue}"/>).
    /// </para>
    /// <para>
    /// <b>A null <paramref name="callback"/> is rejected (decision D8) — deliberately stricter than
    /// Java</b>, which accepts <c>send(record, null)</c> (<c>KafkaProducer.java:1058</c> guards
    /// <c>if (callback != null)</c>). The parameter is non-nullable under <c>#nullable enable</c>,
    /// <see cref="Send(ProducerRecord{TKey, TValue})"/> already spells "no callback", and ffi §A5
    /// mandates precondition validation before the FFI call. Same family as the
    /// <c>CommitAsync(callback)</c> guard (M9/P7) and the <c>Seek</c> negative-offset guard.
    /// </para>
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="callback">The delivery callback (non-null).</param>
    /// <returns>The send's delivery handle; its <see cref="KafkaFuture{T}.Get"/> returns the published
    /// record's <see cref="RecordMetadata"/> or rethrows the delivery failure.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="record"/> or <paramref name="callback"/> is null.
    /// </exception>
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record synchronously (a delivery failure
    /// surfaces from <see cref="KafkaFuture{T}.Get"/> instead).</exception>
    KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback);

    /// <summary>
    /// Flushes all pending records and <b>blocks</b> until the core resolves the flush (Java
    /// <c>Producer.flush()</c>). Surfaces a flush failure as a <see cref="KafkaException"/>. The
    /// delivery callbacks of earlier sends can still trail its return (M11/P4.2 decision D8; see the
    /// remarks on <see cref="IProducer{TKey, TValue}"/>).
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
