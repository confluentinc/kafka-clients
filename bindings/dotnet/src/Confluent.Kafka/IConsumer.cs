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
/// The <b>synchronous</b> Kafka consumer surface — the blocking mirror of
/// <see cref="IAsyncConsumer{TKey, TValue}"/> and the most Java-faithful shape (Java's
/// <c>org.apache.kafka.clients.consumer.Consumer</c> is synchronous). Implemented by
/// <see cref="KafkaConsumer{TKey, TValue}"/> (the real KIP-848 client) and <see cref="MockConsumer{TKey, TValue}"/>
/// (a broker-free test helper). The blocking-in-Java operations return their result
/// directly (or <see langword="void"/>) instead of a <see cref="System.Threading.Tasks.Task"/>;
/// the genuinely non-blocking members stay on the shared <see cref="IConsumerCommon"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Sibling of the async surface, not a wrapper.</b> Each method calls the synchronous
/// C ABI <b>directly</b>; the core's blocking call parks the caller's thread inside the
/// Rust multi-thread runtime (deadlock-free), so this is not sync-over-async and does not
/// route through <see cref="IAsyncConsumer{TKey, TValue}"/>. The two families are siblings over the same
/// native consumer. This is the shape Java users expect (a blocking <c>poll</c> /
/// <c>commitSync</c>), reconstructed in C#.
/// </para>
/// <para>
/// <b>No <see cref="System.Threading.CancellationToken"/>.</b> Interruption is via
/// <see cref="IConsumerCommon.Wakeup"/> only (Java-faithful): a <see cref="IConsumerCommon.Wakeup"/> from
/// another thread makes a blocked <see cref="Poll"/> throw a <see cref="KafkaException"/>
/// (Wakeup, one-shot — thrown once, then the consumer is reusable). <see cref="IConsumerCommon.Wakeup"/> is
/// the one member deliberately callable from another thread.
/// </para>
/// <para>
/// <b>Single-owner / not thread-safe.</b> At most one operation in flight; concurrency is
/// serialized by the Rust core (a concurrent op surfaces a <see cref="KafkaException"/>
/// with a ConcurrentModification code, a concurrent sync state read on
/// <see cref="IConsumerCommon"/> throws <see cref="InvalidOperationException"/>). Do not
/// share one instance across threads without external synchronization — except the
/// canonical <see cref="IConsumerCommon.Wakeup"/> pattern (thread A blocked in <see cref="Poll"/>, thread B
/// wakes it).
/// </para>
/// <para>
/// <b>Additive-growth surface.</b> Grown across two sub-phases: the core loop (subscribe /
/// assign / poll / seek / pause / resume / position / commit / close) shipped in M5/P8a, and
/// the query family (<see cref="Committed"/> / <see cref="OffsetsForTimes"/> /
/// <see cref="BeginningOffsets"/> / <see cref="EndOffsets"/> / <see cref="PartitionsFor"/> /
/// <see cref="ListTopics"/>) added <b>additively</b> in M5/P8b. Both mirror Java's
/// synchronous <c>Consumer</c>. The binding is pre-publish with no external implementers, so
/// the interface can grow additively without a breaking change.
/// </para>
/// <para>
/// <b>Generic-only (PLAN M6/P1b, decision B).</b> Java has a single generic
/// <c>Consumer&lt;K, V&gt;</c>; this binding matches — there is no non-generic
/// <c>IConsumer</c>. Bytes users write <c>IConsumer&lt;byte[], byte[]&gt;</c> (paired with
/// <see cref="Serdes.ByteArray"/>). Only <see cref="Poll"/> is <typeparamref name="TKey"/> /
/// <typeparamref name="TValue"/>-typed; every other member is K/V-free and inherited
/// unchanged from the non-generic <see cref="IConsumerCommon"/> or restated here verbatim.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public interface IConsumer<TKey, TValue> : IConsumerCommon, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>) — <b>blocks</b> and returns an owned
    /// <see cref="ConsumerRecords{TKey, TValue}"/> (a non-null result with <see cref="ConsumerRecords{TKey, TValue}.Count"/>
    /// <c>== 0</c> for an empty poll), or throws a <see cref="KafkaException"/> on failure. The
    /// record bytes are owned copies (ffi-marshalling.md §6.4); nothing native-backed escapes.
    /// A <see cref="IConsumerCommon.Wakeup"/> from another thread interrupts a blocked poll
    /// (throws a Wakeup <see cref="KafkaException"/> once, then the consumer is reusable).
    /// </summary>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <returns>The records polled, as an owned <see cref="ConsumerRecords{TKey, TValue}"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a poll failure (or a <see cref="IConsumerCommon.Wakeup"/> interrupted it).</exception>
    ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (Java <c>subscribe(Collection)</c>) — blocks
    /// until the subscription is applied.
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    void Subscribe(IReadOnlyCollection<string> topics);

    /// <summary>
    /// Unsubscribes from all topics and partitions (Java <c>unsubscribe()</c>) — blocks until
    /// applied.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    void Unsubscribe();

    /// <summary>
    /// Manually assigns <paramref name="partitions"/> to this consumer (Java
    /// <c>assign(Collection)</c>) — blocks until applied.
    /// </summary>
    /// <remarks>
    /// An <b>empty</b> collection <b>clears</b> the assignment (Java parity); a
    /// <see langword="null"/> collection is rejected (null ≠ empty).
    /// </remarks>
    /// <param name="partitions">The topic-partitions to assign (an empty collection clears the assignment).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an assignment failure.</exception>
    void Assign(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Suspends fetching for <paramref name="partitions"/> (Java <c>pause(Collection)</c>) —
    /// blocks until applied. The paused set is observable via <see cref="IConsumerCommon.Paused"/>.
    /// </summary>
    /// <remarks>An <b>empty</b> collection is a no-op success.</remarks>
    /// <param name="partitions">The topic-partitions to pause.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a pause failure (e.g. an unassigned partition).</exception>
    void Pause(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> paused by <see cref="Pause"/> (Java
    /// <c>resume(Collection)</c>) — blocks until applied.
    /// </summary>
    /// <remarks>An <b>empty</b> collection is a no-op success.</remarks>
    /// <param name="partitions">The topic-partitions to resume.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a resume failure (e.g. an unassigned partition).</exception>
    void Resume(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their first available offset (Java
    /// <c>seekToBeginning(Collection)</c>) — blocks until applied. Under KIP-848 this requests
    /// an <c>earliest</c> offset reset resolved lazily on the next <see cref="Poll"/>.
    /// </summary>
    /// <remarks>An <b>empty</b> collection is a no-op success.</remarks>
    /// <param name="partitions">The topic-partitions to seek to the beginning.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their last offset (Java <c>seekToEnd(Collection)</c>)
    /// — blocks until applied. Under KIP-848 this requests a <c>latest</c> offset reset resolved
    /// lazily on the next <see cref="Poll"/>.
    /// </summary>
    /// <remarks>An <b>empty</b> collection is a no-op success.</remarks>
    /// <param name="partitions">The topic-partitions to seek to the end.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the current offset position of <paramref name="partition"/> (Java
    /// <c>position(TopicPartition)</c>) — blocks and returns the offset, or throws a
    /// <see cref="KafkaException"/> on failure (the canonical broker-free failure is a query
    /// for an <b>unassigned</b> partition).
    /// </summary>
    /// <remarks>
    /// No <c>TimeSpan</c> overload this phase — Java's timed <c>position(TopicPartition, Duration)</c>
    /// is deferred until the C ABI exposes a timed variant (the <see cref="Close()"/> precedent).
    /// </remarks>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <returns>The current offset position of <paramref name="partition"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a position failure (e.g. an unassigned partition).</exception>
    long Position(TopicPartition partition);

    /// <summary>
    /// Commits the current fetch positions for all assigned partitions (Java <c>commitSync()</c>)
    /// — the <b>confirming</b> commit; blocks until the commit lands, or throws a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <remarks>
    /// Distinct from the fire-and-forget <see cref="IConsumerCommon.CommitAsync"/> (Java
    /// <c>commitAsync()</c>): this one confirms. No <c>TimeSpan</c> overload this phase
    /// (Python-parity scope); Java's timed <c>commitSync(Duration)</c> is deferred.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    void Commit();

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> (Java
    /// <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;)</c>) — the <b>confirming</b>
    /// commit with explicit offsets; blocks until the commit lands, or throws a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <remarks>
    /// Construct each <see cref="OffsetAndMetadata"/> via its public constructor. An
    /// <b>empty</b> map commits nothing (a valid no-op, not a fault).
    /// </remarks>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets);

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/> (Java
    /// <c>committed(Set&lt;TopicPartition&gt;)</c>) — <b>blocks</b> and returns an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}"/>, or throws a
    /// <see cref="KafkaException"/> on failure. Partitions with no committed offset are
    /// <b>omitted</b> from the result (an empty dictionary if none are committed).
    /// </summary>
    /// <remarks>
    /// No <c>TimeSpan</c> overload this phase — Java's timed
    /// <c>committed(Set, Duration)</c> is deferred until the C ABI exposes a timed variant.
    /// </remarks>
    /// <param name="partitions">The topic-partitions whose committed offsets to read.</param>
    /// <returns>The committed offset (and metadata) for each partition that has one.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a committed-query failure.</exception>
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Looks up the offset of the first record at or after each timestamp in
    /// <paramref name="timestampsToSearch"/> (Java
    /// <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;)</c>) — <b>blocks</b> and returns an
    /// owned <see cref="IReadOnlyDictionary{TopicPartition, OffsetAndTimestamp}"/>, or throws a
    /// <see cref="KafkaException"/> on failure. A <b>negative</b> timestamp is a Kafka-valid
    /// sentinel (EARLIEST/LATEST) and is passed through, not rejected.
    /// </summary>
    /// <param name="timestampsToSearch">The per-partition timestamps to search for.</param>
    /// <returns>The first offset at or after each timestamp, per partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an offsets-for-times failure.</exception>
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/> (Java
    /// <c>beginningOffsets(Collection&lt;TopicPartition&gt;)</c>) — <b>blocks</b> and returns an
    /// owned <see cref="IReadOnlyDictionary{TopicPartition, Int64}"/>, or throws a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <param name="partitions">The topic-partitions whose beginning offsets to read.</param>
    /// <returns>The earliest available offset for each partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a beginning-offsets failure.</exception>
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the latest offset (log-end offset) for each of <paramref name="partitions"/> (Java
    /// <c>endOffsets(Collection&lt;TopicPartition&gt;)</c>) — <b>blocks</b> and returns an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, Int64}"/>, or throws a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <param name="partitions">The topic-partitions whose end offsets to read.</param>
    /// <returns>The latest offset for each partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an end-offsets failure.</exception>
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (Java
    /// <c>partitionsFor(String)</c>) — <b>blocks</b> and returns an owned
    /// <see cref="IReadOnlyList{PartitionInfo}"/> (an empty list for a topic with no known
    /// partitions), or throws a <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <remarks>An <b>empty</b> topic is forwarded to the core (Java/Python-faithful), not rejected.</remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <returns>The partition metadata for the topic.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a partitions-for failure.</exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view (Java
    /// <c>listTopics()</c>) — <b>blocks</b> and returns an owned
    /// <see cref="IReadOnlyDictionary{String, IReadOnlyList}"/> of topic →
    /// <see cref="PartitionInfo"/> list (an empty dictionary when none), or throws a
    /// <see cref="KafkaException"/> on failure.
    /// </summary>
    /// <returns>The partition metadata for every visible topic, keyed by topic.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a list-topics failure.</exception>
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics();

    /// <summary>
    /// Closes the consumer gracefully with the default timeout (Java <c>close()</c>), joining
    /// the background task, then releases native resources. Unlike <see cref="IDisposable.Dispose"/>,
    /// this <b>surfaces</b> a close failure. Idempotent with disposal — a subsequent
    /// <see cref="Close(TimeSpan)"/> / <see cref="IDisposable.Dispose"/> is a no-op.
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close();

    /// <summary>
    /// Closes the consumer gracefully with a bounded <paramref name="timeout"/> (Java
    /// <c>close(Duration)</c>), joining the background task, then releases native resources.
    /// Unlike <see cref="IDisposable.Dispose"/>, this <b>surfaces</b> a close failure.
    /// Idempotent with disposal.
    /// </summary>
    /// <param name="timeout">The maximum time to wait for a graceful close (<see cref="TimeSpan.Zero"/> is valid).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close(TimeSpan timeout);
}
