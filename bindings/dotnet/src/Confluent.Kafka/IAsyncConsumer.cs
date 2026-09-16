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
/// The async Kafka consumer surface — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.Consumer</c>, implemented by
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> (the real KIP-848 client) and
/// <see cref="AsyncMockConsumer{TKey, TValue}"/> (a broker-free test helper). The C#-idiomatic
/// <c>I</c> prefix marks the interface (Framework Design Guidelines / analyzer CA1715);
/// the <c>Async</c> prefix on the type marks this async surface (a future sync
/// <c>IConsumer</c> surface is reserved). Method names carry <b>no <c>Async</c>
/// suffix</b> — the sync-vs-async distinction is carried by the interface/type, matching
/// Java's method names and the Python sibling. Methods that <b>block</b> in Java's
/// <c>AsyncKafkaConsumer</c> implementation return a <see cref="Task"/> here; the
/// genuinely non-blocking members stay synchronous on <see cref="IConsumerCommon"/> (the
/// CLAUDE.md §4 idiom map, decided from the Java implementation — not the Javadoc /
/// method name).
/// </summary>
/// <remarks>
/// <para>
/// <b>Single-owner / not thread-safe.</b> A Kafka consumer is single-owner — at most one
/// operation in flight — mirroring Java's <c>ConcurrentModificationException</c> guard
/// and the in-repo Python sibling. Concurrency is serialized by the Rust core (there is
/// no managed guard): a concurrent <b>async op</b> faults its <see cref="Task"/> with a
/// <see cref="KafkaException"/> (ConcurrentModification); a concurrent <b>sync read</b>
/// (<see cref="IConsumerCommon.GroupMetadata"/>) throws
/// <see cref="InvalidOperationException"/>. <see cref="IConsumerCommon.Wakeup"/> is the
/// one method deliberately callable from another thread. Do not share one consumer
/// across threads without external synchronization, and do not race
/// <see cref="IConsumerCommon.Wakeup"/> against disposal (the canonical pattern — thread
/// A blocked in <see cref="Poll"/>, thread B wakes it, thread A then disposes — is
/// safe).
/// </para>
/// <para>
/// <b>Additive-growth surface.</b> This is a deliberate <em>subset</em> of Java's
/// <c>Consumer</c> — the operations proven and wired so far (subscribe → poll →
/// assign / seek / seekToBeginning / seekToEnd / pause / resume → position →
/// committed / beginningOffsets / endOffsets / offsetsForTimes → partitionsFor /
/// listTopics → commit / commitAsync → group metadata → close), enough for a complete
/// broker loop with confirming and fire-and-forget commits, manual partition management,
/// offset queries, and partition-metadata queries. The remaining Java members (pattern
/// subscribe, the rebalance listener, …) arrive in later phases as
/// <b>additive</b> members on this same interface and new public types — they do not
/// change this shape. No
/// throwing stubs for not-yet-wired ops. The surface is safe to grow additively because
/// the binding is pre-publish with no external implementers; a typed generic sibling
/// (<c>IAsyncConsumer&lt;TKey,TValue&gt;</c> with serializers) will arrive as a new type,
/// not a rename of this one.
/// </para>
/// <para>
/// <b>Generic-only (PLAN M6/P1b, decision B).</b> Java has a single generic
/// <c>Consumer&lt;K, V&gt;</c>; this binding matches — there is no non-generic
/// <c>IAsyncConsumer</c>. Bytes users write <c>IAsyncConsumer&lt;byte[], byte[]&gt;</c>
/// (paired with <see cref="Serdes.ByteArray"/>). Only <see cref="Poll"/> is
/// <typeparamref name="TKey"/> / <typeparamref name="TValue"/>-typed; every other member is
/// K/V-free and inherited unchanged from the non-generic <see cref="IConsumerCommon"/> or
/// restated here verbatim.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public interface IAsyncConsumer<TKey, TValue> : IConsumerCommon, IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>). Blocks in Java (an event
    /// round-trip), so it returns a <see cref="Task"/> here. Resolves with an owned
    /// <see cref="ConsumerRecords{TKey, TValue}"/> — a non-null result with <see cref="ConsumerRecords{TKey, TValue}.Count"/>
    /// <c>== 0</c> for an empty poll (success, not failure) — or faults with a
    /// <see cref="KafkaException"/> on failure. The record bytes are owned copies
    /// (ffi-marshalling.md §6.4); nothing native-backed escapes.
    /// </summary>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <returns>The records polled, as an owned <see cref="ConsumerRecords{TKey, TValue}"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (Java <c>subscribe(Collection)</c>).
    /// Blocks in Java (a cross-thread <c>addAndGet</c>), so it returns a
    /// <see cref="Task"/> here.
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> with a rebalance listener (Java
    /// <c>subscribe(Collection, ConsumerRebalanceListener)</c>). Blocks in Java, so it returns
    /// a <see cref="Task"/> here. The listener then fires on every subsequent rebalance until
    /// the registration is replaced.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java has two <c>subscribe</c> overloads and C#'s idiom is overloads, so this is an
    /// overload rather than an optional parameter (declared divergence D2; Python's
    /// <c>listener=None</c> is a Python idiom, not the Java shape).
    /// </para>
    /// <para>
    /// <b>Registration lifetime.</b> A <b>replacing</b> subscribe releases the previous
    /// listener — including the listener-less
    /// <see cref="Subscribe(IReadOnlyCollection{string}, CancellationToken)"/>, which is how
    /// you deregister — as does disposing the consumer. <see cref="Unsubscribe"/> and
    /// <c>Close</c> deliberately keep it, matching Java's
    /// <c>SubscriptionState.unsubscribe()</c>.
    /// </para>
    /// <para>
    /// See <see cref="IConsumerRebalanceListener"/> for the threading contract (the listener
    /// is <b>synchronous</b> and runs on the core's dispatcher thread), the consequence of
    /// throwing, and why a listener must not call back into its own consumer.
    /// </para>
    /// </remarks>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="listener">The rebalance listener to register.</param>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> or <paramref name="listener"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Subscribe(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Unsubscribes from all topics and partitions (Java <c>unsubscribe()</c>). Blocks
    /// in Java (a cross-thread <c>addAndGet</c>), so it returns a <see cref="Task"/> here.
    /// </summary>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Unsubscribe(CancellationToken cancellationToken = default);

    /// <summary>
    /// Manually assigns <paramref name="partitions"/> to this consumer (Java
    /// <c>assign(Collection)</c>). Blocks in Java (a cross-thread event round-trip), so it
    /// returns a <see cref="Task"/> here.
    /// </summary>
    /// <remarks>
    /// An <b>empty</b> collection <b>clears</b> the assignment (Java parity —
    /// <c>assign(emptyList)</c> assigns to the empty set), NOT a no-op error. A
    /// <see langword="null"/> collection is rejected (null ≠ empty, matching Java's
    /// NPE-on-null vs clear-on-empty).
    /// </remarks>
    /// <param name="partitions">The topic-partitions to assign (an empty collection clears the assignment).</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task Assign(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Suspends fetching for <paramref name="partitions"/> (Java <c>pause(Collection)</c>).
    /// Blocks in Java (a cross-thread event round-trip), so it returns a <see cref="Task"/>
    /// here. The paused set is observable via <see cref="IConsumerCommon.Paused"/>.
    /// </summary>
    /// <remarks>An <b>empty</b> collection is a no-op success (Java iterates an empty collection).</remarks>
    /// <param name="partitions">The topic-partitions to pause.</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task Pause(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> paused by <see cref="Pause"/>
    /// (Java <c>resume(Collection)</c>). Blocks in Java (a cross-thread event round-trip),
    /// so it returns a <see cref="Task"/> here.
    /// </summary>
    /// <remarks>An <b>empty</b> collection is a no-op success.</remarks>
    /// <param name="partitions">The topic-partitions to resume.</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task Resume(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their first available offset (Java
    /// <c>seekToBeginning(Collection)</c>). Blocks in Java (a cross-thread event round-trip),
    /// so it returns a <see cref="Task"/> here. The reset is applied on the next
    /// <see cref="Poll"/>.
    /// </summary>
    /// <remarks>
    /// Under KIP-848 this requests an <c>earliest</c> offset reset for the given partitions;
    /// the effective reset offset is resolved lazily on the next <see cref="Poll"/>. An
    /// <b>empty</b> collection is a no-op success.
    /// </remarks>
    /// <param name="partitions">The topic-partitions to seek to the beginning.</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their last offset (Java
    /// <c>seekToEnd(Collection)</c>). Blocks in Java (a cross-thread event round-trip), so it
    /// returns a <see cref="Task"/> here. The reset is applied on the next
    /// <see cref="Poll"/>.
    /// </summary>
    /// <remarks>
    /// Under KIP-848 this requests a <c>latest</c> offset reset for the given partitions;
    /// the effective reset offset is resolved lazily on the next <see cref="Poll"/>. An
    /// <b>empty</b> collection is a no-op success.
    /// </remarks>
    /// <param name="partitions">The topic-partitions to seek to the end.</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task SeekToEnd(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current offset position of <paramref name="partition"/> (Java
    /// <c>position(TopicPartition)</c>). Blocks in Java (a cross-thread event round-trip /
    /// <c>updateFetchPositions</c>), so it returns a <see cref="Task"/> here, resolving with
    /// the offset — or faulting with a <see cref="KafkaException"/> on failure (the
    /// canonical broker-free failure is a query for an <b>unassigned</b> partition).
    /// </summary>
    /// <remarks>
    /// <b>One method, no <c>TimeSpan</c> overload this phase.</b> Java's timed
    /// <c>position(TopicPartition, Duration)</c> overload is <b>deferred</b> until the C ABI
    /// exposes a timed <c>position_async</c> — at which point a faithful
    /// <c>Position(TopicPartition, TimeSpan)</c> lands as an additive overload (the shipped
    /// <see cref="Close"/> precedent for a missing timed ABI). We do not add a
    /// <c>TimeSpan</c> overload that silently ignores it, nor simulate the deadline
    /// binding-side.
    /// <para>
    /// <b>The <paramref name="cancellationToken"/> is user-initiated cancellation, not a
    /// timeout.</b> It maps to <c>wakeup()</c> (best-effort) so the caller can cancel the
    /// request when <em>they</em> decide to; the binding neither derives nor documents a
    /// deadline from it. A pre-canceled token throws
    /// <see cref="OperationCanceledException"/> synchronously (before any native call).
    /// </para>
    /// </remarks>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <param name="cancellationToken">
    /// User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.
    /// </param>
    /// <returns>The current offset position of <paramref name="partition"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current fetch positions for all assigned partitions (Java
    /// <c>commitSync()</c>) — the <b>confirming</b> commit. Blocks in Java, so it returns a
    /// <see cref="Task"/> here (async-bridged over the void completion callback, not a
    /// blocking-thread façade); the <see cref="Task"/> completes on success or faults with a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <remarks>
    /// Distinct from the fire-and-forget <see cref="IConsumerCommon.CommitAsync()"/> (Java
    /// <c>commitAsync()</c>): this one confirms — you can <c>await</c> it to know the commit
    /// landed. The <paramref name="cancellationToken"/> is user-initiated cancellation
    /// (mapped to <c>wakeup()</c>), <b>not</b> a timeout; a pre-canceled token throws
    /// <see cref="OperationCanceledException"/> synchronously. No <c>TimeSpan</c> overload
    /// this phase (Python-parity scope); Java's timed <c>commitSync(Duration)</c> is deferred.
    /// </remarks>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task Commit(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> (Java
    /// <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;)</c>) — the <b>confirming</b>
    /// commit with explicit offsets. Blocks in Java, so it returns a <see cref="Task"/> here
    /// (async-bridged); the <see cref="Task"/> completes on success or faults with a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <remarks>
    /// Construct each <see cref="OffsetAndMetadata"/> via its public constructor
    /// (<c>new OffsetAndMetadata(offset, metadata, leaderEpoch)</c>). An <b>empty</b> map
    /// commits nothing (a valid no-op, not a fault). The <paramref name="cancellationToken"/>
    /// is user-initiated cancellation (mapped to <c>wakeup()</c>), <b>not</b> a timeout; a
    /// pre-canceled token throws <see cref="OperationCanceledException"/> synchronously. No
    /// <c>TimeSpan</c> overload this phase (Python-parity scope).
    /// </remarks>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the last committed offset (and its metadata / leader epoch) for each of
    /// <paramref name="partitions"/> (Java <c>committed(Set&lt;TopicPartition&gt;)</c>).
    /// Blocks in Java (a cross-thread event round-trip), so it returns a
    /// <see cref="Task"/> here, resolving with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}"/> — partitions
    /// with no committed offset are <b>omitted</b> from the result (an empty dictionary
    /// when none are committed) — or faulting with a <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// <b>One method, no <c>TimeSpan</c> overload this phase.</b> Java's timed
    /// <c>committed(Set, Duration)</c> overload is <b>deferred</b> until the C ABI exposes
    /// a timed <c>committed_async</c> (the shipped <see cref="Position"/> / <see cref="Close"/>
    /// precedent). The <paramref name="cancellationToken"/> is user-initiated cancellation
    /// (mapped to <c>wakeup()</c>), <b>not</b> a timeout; a pre-canceled token throws
    /// <see cref="OperationCanceledException"/> synchronously.
    /// </remarks>
    /// <param name="partitions">The topic-partitions whose committed offsets to read.</param>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <returns>The committed offsets, keyed by topic-partition (absent partitions omitted).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> Committed(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Looks up the offset of the earliest record whose timestamp is greater than or equal
    /// to the given timestamp, for each entry in <paramref name="timestampsToSearch"/>
    /// (Java <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;)</c>). Blocks in Java, so it
    /// returns a <see cref="Task"/> here, resolving with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, OffsetAndTimestamp}"/> — or faulting
    /// with a <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// A <b>negative timestamp</b> is a Kafka-valid sentinel (the EARLIEST / LATEST special
    /// timestamps) and is passed through, <b>not</b> rejected. No <c>TimeSpan</c> overload
    /// this phase (the async ABI has no timeout); the <paramref name="cancellationToken"/>
    /// is user-initiated cancellation, not a timeout.
    /// </remarks>
    /// <param name="timestampsToSearch">The target timestamp (ms since epoch) per topic-partition.</param>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <returns>The resolved offset and timestamp per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/>
    /// (Java <c>beginningOffsets(Collection&lt;TopicPartition&gt;)</c>). Blocks in Java, so
    /// it returns a <see cref="Task"/> here, resolving with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, Int64}"/> — or faulting with a
    /// <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// No <c>TimeSpan</c> overload this phase (the async ABI has no timeout); the
    /// <paramref name="cancellationToken"/> is user-initiated cancellation, not a timeout.
    /// </remarks>
    /// <param name="partitions">The topic-partitions whose beginning offsets to read.</param>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <returns>The earliest offset per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the latest offset (log-end offset) for each of <paramref name="partitions"/>
    /// (Java <c>endOffsets(Collection&lt;TopicPartition&gt;)</c>). Blocks in Java, so it
    /// returns a <see cref="Task"/> here, resolving with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, Int64}"/> — or faulting with a
    /// <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// No <c>TimeSpan</c> overload this phase (the async ABI has no timeout); the
    /// <paramref name="cancellationToken"/> is user-initiated cancellation, not a timeout.
    /// </remarks>
    /// <param name="partitions">The topic-partitions whose end offsets to read.</param>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <returns>The latest offset per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (Java
    /// <c>partitionsFor(String)</c>). Blocks in Java (a metadata fetch), so it returns a
    /// <see cref="Task"/> here, resolving with an owned
    /// <see cref="IReadOnlyList{PartitionInfo}"/> — an <b>empty</b> list for a topic with no
    /// known partitions — or faulting with a <see cref="KafkaException"/>. The
    /// <see cref="PartitionInfo"/> / <see cref="Node"/> fields are owned copies; nothing
    /// native-backed escapes (ffi-marshalling.md §B2/§B4; consumer-threading.md §27).
    /// </summary>
    /// <remarks>
    /// An <b>empty</b> <paramref name="topic"/> is <b>forwarded</b> to the core, <b>not</b>
    /// rejected (Java/Python-faithful — the binding validates only <see langword="null"/>);
    /// only a <see langword="null"/> topic throws (before any native call). No
    /// <c>TimeSpan</c> overload this phase (the async ABI has no timeout — the
    /// <see cref="Position"/> / <see cref="Close"/> precedent); the
    /// <paramref name="cancellationToken"/> is user-initiated cancellation (mapped to
    /// <c>wakeup()</c>), <b>not</b> a timeout.
    /// </remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <returns>The partition metadata for <paramref name="topic"/>, as an owned list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view (Java
    /// <c>listTopics()</c>). Blocks in Java (a metadata fetch), so it returns a
    /// <see cref="Task"/> here, resolving with an owned
    /// <see cref="IReadOnlyDictionary{String, IReadOnlyList}"/> of topic name →
    /// <see cref="PartitionInfo"/> list — an <b>empty</b> dictionary when no topics are known
    /// — or faulting with a <see cref="KafkaException"/>. The values are owned copies;
    /// nothing native-backed escapes.
    /// </summary>
    /// <remarks>
    /// No <c>TimeSpan</c> overload this phase (the async ABI has no timeout); the
    /// <paramref name="cancellationToken"/> is user-initiated cancellation, not a timeout.
    /// </remarks>
    /// <param name="cancellationToken">User-initiated cancellation (mapped to <c>wakeup()</c>); <b>not</b> a timeout.</param>
    /// <returns>The partition metadata per topic, keyed by topic name.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the consumer gracefully (Java <c>close()</c>), joining the background task,
    /// then releases native resources. Unlike <see cref="IAsyncDisposable.DisposeAsync"/>,
    /// this <b>surfaces</b> a close failure. Idempotent with disposal — a subsequent
    /// <see cref="IDisposable.Dispose"/> / <see cref="IAsyncDisposable.DisposeAsync"/> is
    /// a no-op.
    /// </summary>
    /// <remarks>
    /// No timeout overload this phase: the C ABI has no async-close-with-timeout
    /// (<c>Consumer_close_async</c> takes only a callback). A faithful
    /// <c>Close(TimeSpan)</c> is deferred to an additive overload once the core
    /// exposes one (rather than silently ignoring a <see cref="TimeSpan"/>).
    /// </remarks>
    /// <param name="cancellationToken">
    /// Observed before the close is submitted (an already-canceled token throws);
    /// once close is in flight the graceful join runs to completion.
    /// </param>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    Task Close(CancellationToken cancellationToken = default);
}
