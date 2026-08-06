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
/// <see cref="AsyncKafkaConsumer"/> (the real KIP-848 client) and
/// <see cref="AsyncMockConsumer"/> (a broker-free test helper). The C#-idiomatic
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
/// seek → position → group metadata → close), enough for a complete broker loop with auto-commit
/// (<c>enable.auto.commit</c>). The remaining Java members (commit family,
/// <c>committed</c>, the owned-handle query siblings, pattern
/// subscribe, pause / resume, …) arrive in later phases as <b>additive</b> members on
/// this same interface and new public types — they do not change this shape. No
/// throwing stubs for not-yet-wired ops. The surface is safe to grow additively because
/// the binding is pre-publish with no external implementers; a typed generic sibling
/// (<c>IAsyncConsumer&lt;TKey,TValue&gt;</c> with serializers) will arrive as a new type,
/// not a rename of this one.
/// </para>
/// </remarks>
public interface IAsyncConsumer : IConsumerCommon, IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>). Blocks in Java (an event
    /// round-trip), so it returns a <see cref="Task"/> here. Resolves with an owned
    /// <see cref="ConsumerRecords"/> — a non-null result with <see cref="ConsumerRecords.Count"/>
    /// <c>== 0</c> for an empty poll (success, not failure) — or faults with a
    /// <see cref="KafkaException"/> on failure. The record bytes are owned copies
    /// (ffi-marshalling.md §6.4); nothing native-backed escapes.
    /// </summary>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <param name="cancellationToken">Best-effort cancellation (mapped to <c>wakeup()</c>).</param>
    /// <returns>The records polled, as an owned <see cref="ConsumerRecords"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task<ConsumerRecords> Poll(TimeSpan timeout, CancellationToken cancellationToken = default);

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
    /// Unsubscribes from all topics and partitions (Java <c>unsubscribe()</c>). Blocks
    /// in Java (a cross-thread <c>addAndGet</c>), so it returns a <see cref="Task"/> here.
    /// </summary>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Unsubscribe(CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offset"/> (Java
    /// <c>seek(TopicPartition, long)</c>).
    /// </summary>
    /// <remarks>
    /// <b>Async, Java-faithful (deliberate divergence from Python's sync <c>seek</c>).</b>
    /// Java's <c>AsyncKafkaConsumer.seek()</c> returns <c>void</c> but performs a blocking
    /// cross-thread event round-trip (<c>addAndGet(new SeekUnvalidatedEvent(...))</c>), so
    /// the CLAUDE.md §4 idiom map maps it to a <see cref="Task"/>. Python exposes
    /// <c>seek</c> synchronously; we choose Java fidelity. A negative <paramref name="offset"/>
    /// is rejected with the exact Java message before any native call.
    /// </remarks>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offset">The offset to seek to (must be non-negative).</param>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="offset"/> is negative (Java: <c>"seek offset must not be a
    /// negative number"</c>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default);

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
