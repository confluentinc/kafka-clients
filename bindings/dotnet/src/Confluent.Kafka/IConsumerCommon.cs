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
using System.Threading.Tasks;

namespace Confluent.Kafka;

/// <summary>
/// The non-blocking consumer surface shared by every consumer flavor — the members
/// that are non-blocking in Java's consumer implementation and therefore stay
/// synchronous regardless of the async/sync split. It is the common base of
/// <see cref="IAsyncConsumer{TKey, TValue}"/> (the async surface) and reserves the shape for a
/// future sync <c>IConsumer</c> surface, so both carry these members with the
/// same signatures.
/// </summary>
/// <remarks>
/// Mostly non-blocking <em>local</em> reads and actions (<see cref="Wakeup"/>,
/// <see cref="GroupMetadata"/>, <see cref="Assignment"/>, <see cref="Subscription"/>,
/// <see cref="Paused"/>, <see cref="EnforceRebalance"/>), plus the fire-and-forget
/// <see cref="CommitAsync"/> (M5/P6) and, from M5/P7, the two
/// <see cref="Seek(TopicPartition, long)"/> overloads and <see cref="CurrentLag"/>. The
/// latter three are <b>flavor-independent</b> (always synchronous, whether the consumer is
/// async or sync), so they live on this shared base — reachable through an
/// <see cref="IAsyncConsumer{TKey, TValue}"/> reference and inherited unchanged by a future sync
/// <c>IConsumer</c>. <see cref="CurrentLag"/> is a genuine non-blocking local read;
/// <c>Seek</c> blocks in Java yet is shipped <b>synchronous</b> here for Python parity
/// (a deliberate CLAUDE.md §4 divergence — see the member remarks), so this base's charter
/// widens from "non-blocking <em>local</em>" to "non-blocking regardless of network
/// semantics", accepted for the flavor-independence it buys.
/// </remarks>
/// <remarks>
/// The three state getters (<see cref="Assignment"/> / <see cref="Subscription"/> /
/// <see cref="Paused"/>) are plain <c>()</c> <b>methods, not properties</b> — matching
/// the shipped <see cref="GroupMetadata"/> precedent, Java's / the Python sibling's
/// method shape, and the .NET Framework Design Guidelines (a method, not a property, when
/// the accessor does non-trivial work — a P/Invoke + marshalling — can throw, and
/// returns a fresh owned snapshot each call). Each returns an
/// <see cref="IReadOnlyCollection{T}"/> (Java returns a <c>Set</c>, but
/// <c>IReadOnlySet</c> post-dates the netstandard2.0 floor) — an immutable owned copy,
/// matching Java's "returns a copy" contract.
/// </remarks>
public interface IConsumerCommon
{
    /// <summary>
    /// Interrupts a blocked operation on this consumer (Java <c>wakeup()</c>) — the
    /// in-flight <see cref="IAsyncConsumer{TKey, TValue}.Poll"/> (etc.) faults with a
    /// <see cref="KafkaException"/> (Wakeup, one-shot). Non-blocking in Java, so it stays
    /// synchronous; it is the one member deliberately callable from another thread (the
    /// single-owner model's cross-thread escape). Best-effort: a no-op once the consumer
    /// is closing.
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

    /// <summary>
    /// Returns the current partition assignment (Java <c>assignment()</c>). A non-blocking
    /// state read in Java, so it stays synchronous (a method — see the type remarks).
    /// Returns a fresh owned, immutable snapshot each call.
    /// </summary>
    /// <returns>The assigned topic-partitions (a copy; empty when nothing is assigned).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<TopicPartition> Assignment();

    /// <summary>
    /// Returns the current topic subscription (Java <c>subscription()</c>). A non-blocking
    /// state read in Java, so it stays synchronous (a method — see the type remarks).
    /// Returns a fresh owned, immutable snapshot each call.
    /// </summary>
    /// <returns>The subscribed topics (a copy; empty when not subscribed).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<string> Subscription();

    /// <summary>
    /// Returns the currently paused partitions (Java <c>paused()</c>). A non-blocking
    /// state read in Java, so it stays synchronous (a method — see the type remarks).
    /// Returns a fresh owned, immutable snapshot each call.
    /// </summary>
    /// <returns>The paused topic-partitions (a copy; empty when none are paused).</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    IReadOnlyCollection<TopicPartition> Paused();

    /// <summary>
    /// Triggers a rebalance (Java <c>enforceRebalance()</c> / <c>enforceRebalance(String
    /// reason)</c>, collapsed to one method with an optional <paramref name="reason"/>). A
    /// non-blocking action in Java, so it stays synchronous.
    /// </summary>
    /// <remarks>
    /// Under the KIP-848 group protocol this is a <b>logged no-op that returns
    /// successfully</b> — it never throws a <see cref="KafkaException"/> on that path,
    /// matching Java's <c>AsyncKafkaConsumer.enforceRebalance</c> (a pure logged no-op that
    /// throws nothing). The <see cref="KafkaException"/> below is reserved for a future
    /// classic-protocol arm and is not reachable under the current group protocol.
    /// </remarks>
    /// <param name="reason">An optional human-readable reason, or <see langword="null"/>.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a rebalance failure (not reachable under the current KIP-848
    /// no-op).
    /// </exception>
    void EnforceRebalance(string? reason = null);

    /// <summary>
    /// Commits the current fetch positions <b>fire-and-forget</b> (Java <c>commitAsync()</c>)
    /// — best-effort, no completion to await. Non-blocking in Java (it returns once the
    /// commit is <em>initiated</em>, not once it lands), so it stays synchronous with a
    /// <see langword="void"/> return, mirroring the Python sibling's <c>commit_async()</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The <b>fire-and-forget</b> commit is flavor-independent — it returns nothing to await
    /// in either an async or a sync consumer — so it lives on this shared non-blocking base
    /// (a future sync <c>IConsumer</c> inherits the identical member for free). The
    /// <b>confirming</b> commit is flavor-dependent (async <see cref="Task"/> vs a sync
    /// blocking mirror), so it lives on the flavor-specific surface
    /// (<see cref="IAsyncConsumer{TKey, TValue}.Commit(System.Threading.CancellationToken)"/>). Takes no
    /// <see cref="System.Threading.CancellationToken"/>: there is nothing to cancel once the
    /// commit has been handed off.
    /// </para>
    /// <para>
    /// A concurrent op surfaces the core's way — a thrown <see cref="KafkaException"/>
    /// (ConcurrentModification), the sync analog of the async ops' faulted <see cref="Task"/>
    /// (the consumer is single-owner; there is no managed guard).
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit-initiation failure.</exception>
    void CommitAsync();

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offset"/> (Java
    /// <c>seek(TopicPartition, long)</c>) — <b>synchronous</b>, matching the Python sibling.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Sync (deliberate §4 divergence).</b> Java's <c>AsyncKafkaConsumer.seek()</c> blocks
    /// (a cross-thread <c>addAndGet(new SeekUnvalidatedEvent(...))</c>), so CLAUDE.md §4 would
    /// map it to a <see cref="Task"/>; this phase ships it <b>synchronous</b> instead —
    /// Python exposes <c>seek</c> synchronously, and the sync ABI (<c>Consumer_seek</c>) is
    /// called directly (no <c>Task.Run</c>, so not sync-over-async). It therefore lives on
    /// this non-blocking shared base, reachable through an <see cref="IAsyncConsumer{TKey, TValue}"/>
    /// reference. Documented under the §4 idiom-map divergence.
    /// </para>
    /// <para>
    /// <b>Java-fidelity negative-offset guard (the one place stricter than Python).</b> A
    /// negative <paramref name="offset"/> is rejected with the exact Java message before any
    /// native call — even when the consumer is closed (the argument check precedes the
    /// disposed check).
    /// </para>
    /// </remarks>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offset">The offset to seek to (must be non-negative).</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="offset"/> is negative (Java: <c>"seek offset must not be a negative
    /// number"</c>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a seek failure (e.g. seeking an unassigned partition).
    /// </exception>
    void Seek(TopicPartition partition, long offset);

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offsetAndMetadata"/>'s offset,
    /// carrying its commit metadata and leader epoch (Java
    /// <c>seek(TopicPartition, OffsetAndMetadata)</c>) — <b>synchronous</b>, matching the
    /// Python sibling.
    /// </summary>
    /// <remarks>
    /// Same sync mapping as <see cref="Seek(TopicPartition, long)"/> (calls the sync ABI
    /// <c>Consumer_seek_with_metadata</c> directly). No offset guard is needed here — the
    /// <see cref="OffsetAndMetadata"/> constructor already rejects a negative offset (with
    /// <c>"Invalid negative offset"</c>).
    /// </remarks>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offsetAndMetadata">The offset (with metadata / leader epoch) to seek to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsetAndMetadata"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a seek failure (e.g. seeking an unassigned partition).
    /// </exception>
    void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /// <summary>
    /// Returns the consumer's current lag for <paramref name="partition"/> — the number of
    /// records between the consumer's position and the log-end offset (Java
    /// <c>currentLag(TopicPartition)</c>) — or <see langword="null"/> when the lag is not
    /// currently known (Java's <c>OptionalLong.empty</c>). <b>Synchronous</b>: a non-blocking
    /// local read (never blocks in the core), so it stays sync (a §4 divergence — the §4
    /// idiom map lists <c>currentLag</c> as blocking, corrected this phase).
    /// </summary>
    /// <remarks>
    /// A <see langword="null"/> result conflates "lag unknown" with a concurrent-access
    /// rejection (the ABI reports both as a bare "unknown") — matching the Python sibling,
    /// which returns the raw value / <c>None</c>. Unlike the concurrent sync <em>state
    /// reads</em> (<see cref="Assignment"/> etc.), <c>CurrentLag</c> does <b>not</b> throw
    /// <see cref="InvalidOperationException"/> on concurrent access — the ABI does not
    /// surface that split for lag.
    /// </remarks>
    /// <param name="partition">The topic-partition whose lag to read.</param>
    /// <returns>The current lag, or <see langword="null"/> when the lag is unknown.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    long? CurrentLag(TopicPartition partition);
}
