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
/// A Kafka consumer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.Consumer</c>, implemented by
/// <c>KafkaConsumer</c> (the real KIP-848 client) and <c>MockConsumer</c>
/// (a broker-free test helper). The C#-idiomatic <c>I</c> prefix marks the interface
/// (Framework Design Guidelines / analyzer CA1715); the <c>Async</c> suffix marks the
/// <see cref="Task"/>-returning members. Methods that <b>block</b> in Java's
/// <c>AsyncKafkaConsumer</c> implementation are async here; the genuinely non-blocking
/// members stay synchronous (the CLAUDE.md §4 idiom map, decided from the Java
/// implementation — not the Javadoc / method name).
/// </summary>
/// <remarks>
/// <para>
/// <b>Single-owner / not thread-safe.</b> A Kafka consumer is single-owner — at most one
/// operation in flight — mirroring Java's <c>ConcurrentModificationException</c> guard
/// and the in-repo Python sibling. Concurrency is serialized by the Rust core (there is
/// no managed guard): a concurrent <b>async op</b> faults its <see cref="Task"/> with a
/// <see cref="KafkaException"/> (ConcurrentModification); a concurrent <b>sync read</b>
/// (<see cref="GroupMetadata"/>) throws <see cref="InvalidOperationException"/>.
/// <see cref="Wakeup"/> is the one method deliberately callable from another thread. Do
/// not share one consumer across threads without external synchronization, and do not
/// race <see cref="Wakeup"/> against disposal (the canonical pattern — thread A blocked
/// in <see cref="PollAsync"/>, thread B wakes it, thread A then disposes — is safe).
/// </para>
/// <para>
/// <b>Additive-growth surface.</b> This is a deliberate <em>subset</em> of Java's
/// <c>Consumer</c> — the operations proven and wired this phase (subscribe → poll →
/// seek → group metadata → close), enough for a complete broker loop with auto-commit
/// (<c>enable.auto.commit</c>). The remaining Java members (commit family, position,
/// assignment / subscription getters, the owned-handle query siblings, pattern
/// subscribe, pause / resume, …) arrive in later phases as <b>additive</b> members on
/// this same interface and new public types — they do not change this shape. No
/// throwing stubs for not-yet-wired ops. The surface is safe to grow additively because
/// the binding is pre-publish with no external implementers; a typed generic sibling
/// (<c>IConsumer&lt;TKey,TValue&gt;</c> with serializers) will arrive as a new type, not
/// a rename of this one.
/// </para>
/// </remarks>
public interface IConsumer : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>). Blocks in Java (an event
    /// round-trip), so it is async here. Resolves with an owned
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
    Task<ConsumerRecords> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (Java <c>subscribe(Collection)</c>).
    /// Blocks in Java (a cross-thread <c>addAndGet</c>), so it is async here.
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task SubscribeAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);

    /// <summary>
    /// Unsubscribes from all topics and partitions (Java <c>unsubscribe()</c>). Blocks
    /// in Java (a cross-thread <c>addAndGet</c>), so it is async here.
    /// </summary>
    /// <param name="cancellationToken">Best-effort cancellation.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task UnsubscribeAsync(CancellationToken cancellationToken = default);

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
    Task SeekAsync(TopicPartition partition, long offset, CancellationToken cancellationToken = default);

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
    /// <c>CloseAsync(TimeSpan)</c> is deferred to an additive overload once the core
    /// exposes one (rather than silently ignoring a <see cref="TimeSpan"/>).
    /// </remarks>
    /// <param name="cancellationToken">
    /// Observed before the close is submitted (an already-canceled token throws);
    /// once close is in flight the graceful join runs to completion.
    /// </param>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    Task CloseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Interrupts a blocked operation on this consumer (Java <c>wakeup()</c>) — the
    /// in-flight <see cref="PollAsync"/> (etc.) faults with a <see cref="KafkaException"/>
    /// (Wakeup, one-shot). Non-blocking in Java, so it stays synchronous; it is the one
    /// member deliberately callable from another thread (the single-owner model's cross-
    /// thread escape). Best-effort: a no-op once the consumer is closing.
    /// </summary>
    void Wakeup();

    /// <summary>
    /// Returns the current consumer group metadata (Java <c>groupMetadata()</c>). A
    /// non-blocking getter in Java, so it stays synchronous (a method, matching Java's
    /// method-not-property shape).
    /// </summary>
    /// <returns>A snapshot of the group membership.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    ConsumerGroupMetadata GroupMetadata();
}
