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
/// The non-blocking consumer surface shared by every consumer flavor — the members
/// that are non-blocking in Java's consumer implementation and therefore stay
/// synchronous regardless of the async/sync split. It is the common base of
/// <see cref="IAsyncConsumer"/> (the async surface) and reserves the shape for a
/// future sync <c>IConsumer</c> surface, so both carry these members with the
/// same signatures.
/// </summary>
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
    /// in-flight <see cref="IAsyncConsumer.Poll"/> (etc.) faults with a
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
}
