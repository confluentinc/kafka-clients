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
/// / <see cref="PartitionsFor"/> have different signatures on the sync vs async interface, and the
/// members whose signatures match are simply declared on both (M11/P8 decision D-6).
/// </para>
/// <para>
/// <b>Subset of Java's <c>Producer</c>.</b> The send path (<c>Send</c> with
/// <see cref="ProducerRecord{TKey, TValue}"/> / <see cref="RecordMetadata"/>) plus the async
/// peripherals (<see cref="Flush"/> / <see cref="Close(CancellationToken)"/> /
/// <see cref="PartitionsFor"/>) and the synchronous <see cref="Metrics"/> state read (M11/P8),
/// and the five transaction members (M17/P1).
/// </para>
/// <para>
/// <b>Transactions (M17/P1).</b> The five transaction members are Java's
/// (<c>Producer.java:45-66</c>) and need <c>transactional.id</c> in the producer's configuration.
/// Call <see cref="InitTransactions"/> once per producer, before any other transaction member; a
/// call that failed with a timeout (<see cref="KafkaException.Code"/> 7,
/// <see cref="KafkaException.IsRetriable"/>) may be called again (Java
/// <c>KafkaProducer.java:635</c>). Then, for each transaction: <see cref="BeginTransaction"/>, the
/// transaction's <c>Send</c>s and <see cref="SendOffsetsToTransaction"/> calls, and finally
/// <see cref="CommitTransaction"/> or <see cref="AbortTransaction"/>.
/// <see cref="BeginTransaction"/> stays synchronous here, because Java's does not block.
/// </para>
/// <para>
/// <b>A transaction member first hands the core every <c>Send</c> that has returned.</b>
/// <c>Send</c> on this surface is deferred: a returned send can still be buffered in the binding.
/// So each of the five transaction members first hands every buffered record to the core, and
/// only then acts. A commit therefore includes every send that had returned before it, an abort
/// discards them, and a <see cref="BeginTransaction"/> never sweeps a send that returned before
/// it into the transaction it opens — Java's guarantees, since Java's <c>send</c> registers the
/// record before it returns. Sending inside a transaction is the normal use. A <c>Send</c> that is
/// still running on another thread when a transaction member is called is not ordered against
/// it, as in Java.
/// </para>
/// <para>
/// <b>Cancellation abandons the wait, not the operation.</b> The core has no way to abort a
/// transaction member once it has started, so a <see cref="CancellationToken"/> passed to one
/// has four outcomes. Already canceled: <see cref="OperationCanceledException"/> is thrown once the
/// argument and closed checks pass, and nothing else is done. Fired while the buffered sends are handed to the core: the returned
/// <see cref="Task"/> is canceled and the operation is never started; the buffered sends still
/// reach the core. Fired after the operation started: the <see cref="Task"/> is canceled while
/// the core runs the operation to completion — calling another transaction member while it
/// still runs fails with <see cref="KafkaException.Code"/> -2, and one called after it finished
/// sees the state it left. Fired while <see cref="CommitTransaction"/> or
/// <see cref="AbortTransaction"/> waits for the earlier sends' completions: the
/// <see cref="Task"/> completes <b>successfully</b>, because the commit or abort did happen; only
/// that completion ordering is given up.
/// </para>
/// <para>
/// <b>Three cases the completion ordering of <see cref="CommitTransaction"/> and
/// <see cref="AbortTransaction"/> does not cover.</b> (R-a) On a manual
/// (<c>autoComplete: false</c>) <see cref="AsyncMockProducer{TKey, TValue}"/>,
/// <see cref="AsyncMockProducer{TKey, TValue}.Clear"/> with sends pending leaves those sends
/// unresolved, and a later commit or abort then waits for them until its token is canceled —
/// with no token, indefinitely. (R-b)
/// A delivery callback that blocks on <see cref="CommitTransaction"/> or
/// <see cref="AbortTransaction"/> deadlocks: it runs on a thread the producer owns, and the commit
/// or abort waits for that thread. Java's callbacks, which run on the thread that completes the
/// commit, have the same hazard. (R-c) The synchronous <see cref="IProducer{TKey, TValue}"/> has no
/// completion ordering to give: its <c>Send</c> has completed when it returns, and a
/// <c>Send</c> running on another thread is not ordered against a transaction member.
/// </para>
/// <para>
/// <b>Handling a transaction error.</b> Java's documented pattern
/// (<c>KafkaProducer.java:209-216</c>) translates literally. When
/// <c>ex.Code == 90 || ex.IsOutOfOrderSequenceError || ex.IsAuthorizationError</c> — Java's
/// <c>ProducerFencedException</c>, <c>OutOfOrderSequenceException</c> and
/// <c>AuthorizationException</c> — the producer cannot recover: close it. On any other
/// <see cref="KafkaException"/>, abort the transaction; when
/// <see cref="KafkaException.IsTransactionAbortableError"/> is <see langword="true"/>, abort and
/// then retry the transaction (KIP-1050). The KIP-1050 variant, which tests
/// <see cref="KafkaException.IsApplicationRecoverableError"/> in place of <c>Code == 90</c>, is a
/// <b>superset</b> of Java's example: five more codes (22, 25, 47, 49 and 82) route to closing the
/// producer. Code -2, the core's rejection of a transaction member that overlaps another one on
/// the same producer, is never a reason to abort: the other call is still running.
/// </para>
/// <para>
/// <b>Where <see cref="SendOffsetsToTransaction"/>'s checks differ from Java's.</b> The closed
/// check is the binding's and runs before the core's checks, so a closed producer reports
/// <see cref="ObjectDisposedException"/> where Java's <c>KafkaProducer</c> reports its
/// generation or no-transaction error first (<c>KafkaProducer.java:732-737</c>). A null
/// <c>groupMetadata</c> is an <see cref="ArgumentNullException"/> on the mock too, where Java's
/// <c>MockProducer</c> throws <c>NullPointerException</c>; and a null <c>offsets</c> map is
/// rejected before the core's checks, where Java reaches it only after its own.
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
/// thread; once that bound is reached, <c>Send</c> waits for capacity before returning. Blocking is
/// Java's own contract — <c>KafkaProducer.send()</c> blocks once its accumulator is full — and it is
/// what keeps the client's memory and latency bounded: because the <see cref="Task{TResult}"/>
/// returned here is the <em>record's delivery</em> future rather than an admission handle, a caller
/// never awaits admission, so an asynchronous wait would throttle nobody. The bound is sized so that
/// reaching it is rare in practice; a producer that hits it regularly is offering records faster than
/// its cluster can accept them. The <b>synchronous</b> <see cref="IProducer{TKey, TValue}"/> has no
/// such window — it hands each record to the core inside the call, where the core's own
/// <c>buffer.memory</c> provides the backpressure.
/// </para>
/// <para>
/// ⚠ <b>That wait has no timeout, and the record is ALREADY ACCEPTED before it starts (M11/P3.4).</b>
/// <c>Send</c> appends the record to the producer's batch chain first and waits afterwards — the
/// Python binding's order — so the wait throttles the <em>caller</em> and never decides the record's
/// fate. Consequently <c>Send</c> does not refuse a record for saturation at all: it returns when
/// capacity frees, or when the producer is torn down underneath it — and teardown does not discard
/// the record either, it is settled by the closing producer's own drain (sent by the final drain on
/// a normal close; faulted if the send-batch thread itself failed). The only thing that ends the
/// wait early is teardown.
/// </para>
/// <para>
/// <b>Cancellation is best-effort (no native abort).</b> Unlike the consumer, the producer has
/// no <c>wakeup()</c>: a canceled <see cref="CancellationToken"/> cancels the returned
/// <see cref="Task"/>'s .NET-side wait, but does not abort the in-flight native op — it
/// continues to completion (a host-idiom addition Java lacks; CLAUDE.md §4). ⚠ A token that fires
/// while <c>Send</c> is blocked waiting for capacity does <b>not</b> abort that wait (M11/P3.4):
/// the record is already accepted by then, so the token only cancels the returned
/// <see cref="Task{TResult}"/>.
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
    /// ⚠ <b>Under sustained saturation this BLOCKS the calling thread until capacity frees</b> —
    /// Java's own <c>send()</c> contract, and the only thing that bounds an async producer whose
    /// caller never awaits admission. The record is appended before the wait starts, so saturation
    /// delays the call rather than refusing the record. The type's remarks state the contract in
    /// full; it applies to the <b>async</b> surface only.
    /// </para>
    /// </remarks>
    /// <param name="record">The record to publish.</param>
    /// <param name="cancellationToken">
    /// Best-effort cancellation of the .NET wait. An already-canceled token throws
    /// <see cref="OperationCanceledException"/> before the send is enqueued; a token that fires
    /// afterwards cancels the returned <see cref="Task{TResult}"/> but does <b>not</b> abort the
    /// in-flight native send (the producer has no <c>wakeup()</c>). ⚠ A token that fires while this
    /// call is blocked waiting for accumulator capacity does <b>not</b> abort that wait — the record
    /// is already accepted by then (M11/P3.4).
    /// </param>
    /// <returns>
    /// A task resolving with the published record's <see cref="RecordMetadata"/>, or faulting with
    /// a <see cref="KafkaException"/> carrying the delivery failure.
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
    /// ⚠ <b>And it BLOCKS the calling thread under sustained saturation</b>, exactly as
    /// <see cref="Send(ProducerRecord{TKey, TValue}, CancellationToken)"/> does — the bound is on
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
    /// <c>IProducerCommon</c> (unlike the consumer's <c>IConsumerCommon</c>; M11/P8 decision D-6,
    /// DoD §7). On a
    /// <see cref="MockProducer{TKey, TValue}"/> the snapshot is <b>empty</b> (Java
    /// <c>MockProducer.metrics()</c> parity — its metric map is empty unless seeded, and the ABI
    /// exposes no seeding entry point).
    /// </remarks>
    /// <returns>The producer's metrics, keyed by <see cref="MetricName"/> value identity.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();

    /// <summary>
    /// Initializes the producer for transactions, completing when the core has done so (Java
    /// <c>Producer.initTransactions()</c>): the first transaction member to call, once per
    /// producer. The remarks on <see cref="IAsyncProducer{TKey, TValue}"/> give the lifecycle, and
    /// how buffered sends and cancellation are handled.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait, not the operation; the remarks on <see cref="IAsyncProducer{TKey, TValue}"/>
    /// give each case.
    /// </param>
    /// <returns>
    /// A task that completes when the producer is initialized, or faults with a
    /// <see cref="KafkaException"/> — for example a timeout (<see cref="KafkaException.Code"/> 7),
    /// after which the call may be made again.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task InitTransactions(CancellationToken cancellationToken = default);

    /// <summary>
    /// Begins a transaction (Java <c>Producer.beginTransaction()</c>). <b>Synchronous</b> on this
    /// async surface, because Java's <c>beginTransaction</c> does not block: it is a state change
    /// inside the core.
    /// </summary>
    /// <remarks>
    /// Before the state change it hands the core every <c>Send</c> that has returned (see the
    /// remarks on <see cref="IAsyncProducer{TKey, TValue}"/>), which normally takes at most one
    /// batch window. That wait is bounded at 30 seconds, because this member takes no token: if the
    /// buffered sends have not reached the core by then, it throws a <see cref="KafkaException"/>
    /// with <see cref="KafkaException.Code"/> 0 and the message "The producer's send accumulator
    /// did not drain within 30 seconds, so records buffered in the binding have not reached the
    /// core and beginTransaction() was not attempted.", and no transaction is begun.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a failure, or the buffered sends did not reach the core within 30 seconds
    /// (<see cref="KafkaException.Code"/> 0; no transaction is begun).
    /// </exception>
    void BeginTransaction();

    /// <summary>
    /// Adds consumer-group offsets to the current transaction, so they are committed or aborted
    /// with it, completing when the core has done so (Java
    /// <c>Producer.sendOffsetsToTransaction(Map, ConsumerGroupMetadata)</c>). The map is copied
    /// before this returns, so changing it afterwards — even while the call is still waiting —
    /// cannot change what is sent. An empty map adds no offsets. The remarks on
    /// <see cref="IAsyncProducer{TKey, TValue}"/> give the lifecycle, and how buffered sends and
    /// cancellation are handled.
    /// </summary>
    /// <param name="offsets">The offsets to add, keyed by topic-partition.</param>
    /// <param name="groupMetadata">
    /// The consumer group the offsets belong to — <see cref="IConsumerCommon.GroupMetadata"/> of the
    /// consumer that read the records.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the wait, not the operation; the remarks on <see cref="IAsyncProducer{TKey, TValue}"/>
    /// give each case.
    /// </param>
    /// <returns>
    /// A task that completes when the offsets are part of the transaction, or faults with a
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupMetadata"/> is null (checked first, as Java does), or
    /// <paramref name="offsets"/> is null. The remarks on <see cref="IAsyncProducer{TKey, TValue}"/>
    /// say where these checks differ from Java's.
    /// </exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current transaction, completing when the core has done so (Java
    /// <c>Producer.commitTransaction()</c>). The remarks on
    /// <see cref="IAsyncProducer{TKey, TValue}"/> give the lifecycle, how buffered sends and
    /// cancellation are handled, and how to handle a failure.
    /// </summary>
    /// <remarks>
    /// <b>When the returned <see cref="Task"/> completes successfully, every <c>Send</c> that had
    /// returned before this call — the transaction's records among them — has completed</b>: its
    /// <see cref="Task{TResult}"/> is completed and its delivery callback has returned (Java
    /// <c>KafkaProducer.java:754-755</c>). If <paramref name="cancellationToken"/> fires while this
    /// call waits for those completions, the <see cref="Task"/> completes successfully without them. When the <see cref="Task"/> faults,
    /// nothing is promised about them. The ordering holds for a producer that is not being torn
    /// down: nothing is promised for a call that races <see cref="Close(CancellationToken)"/> or
    /// disposal. The remarks on <see cref="IAsyncProducer{TKey, TValue}"/> list the cases it does
    /// not cover (R-a to R-c).
    /// </remarks>
    /// <param name="cancellationToken">
    /// Cancels the wait, not the operation; the remarks on <see cref="IAsyncProducer{TKey, TValue}"/>
    /// give each case.
    /// </param>
    /// <returns>A task that completes when the transaction is committed, or faults with a <see cref="KafkaException"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task CommitTransaction(CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts the current transaction, completing when the core has done so (Java
    /// <c>Producer.abortTransaction()</c>): the transaction's records and offsets are discarded.
    /// The remarks on <see cref="IAsyncProducer{TKey, TValue}"/> give the lifecycle, and how
    /// buffered sends and cancellation are handled.
    /// </summary>
    /// <remarks>
    /// <b>When the returned <see cref="Task"/> completes successfully, every <c>Send</c> that had
    /// returned before this call — the transaction's records among them — has completed</b>: its
    /// <see cref="Task{TResult}"/> is completed and its delivery callback has returned. If
    /// <paramref name="cancellationToken"/> fires while this call waits for those completions, the
    /// <see cref="Task"/> completes successfully without them. When the
    /// <see cref="Task"/> faults, nothing is promised about them. The ordering holds for a producer
    /// that is not being torn down: nothing is promised for a call that races
    /// <see cref="Close(CancellationToken)"/> or disposal. The remarks on
    /// <see cref="IAsyncProducer{TKey, TValue}"/> list the cases it does not cover (R-a to R-c).
    /// </remarks>
    /// <param name="cancellationToken">
    /// Cancels the wait, not the operation; the remarks on <see cref="IAsyncProducer{TKey, TValue}"/>
    /// give each case.
    /// </param>
    /// <returns>A task that completes when the transaction is aborted, or faults with a <see cref="KafkaException"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task AbortTransaction(CancellationToken cancellationToken = default);
}
