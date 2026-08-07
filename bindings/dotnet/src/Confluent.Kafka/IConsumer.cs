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
/// <see cref="IAsyncConsumer"/> and the most Java-faithful shape (Java's
/// <c>org.apache.kafka.clients.consumer.Consumer</c> is synchronous). Implemented by
/// <see cref="KafkaConsumer"/> (the real KIP-848 client) and <see cref="MockConsumer"/>
/// (a broker-free test helper). The blocking-in-Java operations return their result
/// directly (or <see langword="void"/>) instead of a <see cref="System.Threading.Tasks.Task"/>;
/// the genuinely non-blocking members stay on the shared <see cref="IConsumerCommon"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Sibling of the async surface, not a wrapper.</b> Each method calls the synchronous
/// C ABI <b>directly</b>; the core's blocking call parks the caller's thread inside the
/// Rust multi-thread runtime (deadlock-free), so this is not sync-over-async and does not
/// route through <see cref="IAsyncConsumer"/>. The two families are siblings over the same
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
/// <b>Additive-growth surface.</b> This is a deliberate <em>subset</em> of Java's
/// <c>Consumer</c> — the core loop (subscribe / assign / poll / seek / pause / resume /
/// position / commit / close). The query family (<c>committed</c> / <c>offsetsForTimes</c> /
/// <c>beginningOffsets</c> / <c>endOffsets</c> / <c>partitionsFor</c> / <c>listTopics</c>)
/// arrives as <b>additive</b> members on this same interface in a later phase. The binding
/// is pre-publish with no external implementers, so the interface grows additively.
/// </para>
/// </remarks>
public interface IConsumer : IConsumerCommon, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>) — <b>blocks</b> and returns an owned
    /// <see cref="ConsumerRecords"/> (a non-null result with <see cref="ConsumerRecords.Count"/>
    /// <c>== 0</c> for an empty poll), or throws a <see cref="KafkaException"/> on failure. The
    /// record bytes are owned copies (ffi-marshalling.md §6.4); nothing native-backed escapes.
    /// A <see cref="IConsumerCommon.Wakeup"/> from another thread interrupts a blocked poll
    /// (throws a Wakeup <see cref="KafkaException"/> once, then the consumer is reusable).
    /// </summary>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <returns>The records polled, as an owned <see cref="ConsumerRecords"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a poll failure (or a <see cref="IConsumerCommon.Wakeup"/> interrupted it).</exception>
    ConsumerRecords Poll(TimeSpan timeout);

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
