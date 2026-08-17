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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// A broker-free <b>synchronous</b> Kafka producer for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.MockProducer</c>. A thin, Java-shaped forwarder over the
/// internal <see cref="NativeProducer"/> (like <see cref="KafkaProducer"/>), the sync sibling of
/// <see cref="AsyncMockProducer"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Send + the send-control helpers.</b> Beyond the <see cref="IProducer"/> surface
/// (<see cref="Send"/> / <see cref="Flush"/> / <see cref="PartitionsFor"/> / <see cref="Close"/>),
/// this mock exposes the Java <c>MockProducer</c> send-driving helpers as inherent methods on the
/// concrete type (not on the interface) — <see cref="CompleteNext"/> / <see cref="ErrorNext"/> /
/// <see cref="HistoryCount"/> / <see cref="Clear"/> — mirroring <see cref="AsyncMockProducer"/> and
/// the consumer's mock-only precedent.
/// </para>
/// <para>
/// <b>Manual (<c>autoComplete: false</c>) sends need a second thread.</b> Because sync
/// <see cref="Send"/> <b>blocks</b> until the send resolves (decision #1), a manual mock's
/// <see cref="Send"/> blocks the calling thread until <see cref="CompleteNext"/> /
/// <see cref="ErrorNext"/> is called from <b>another</b> thread (the single-owner "another thread
/// completes" pattern — the sync analog of the async mock's pending-send drive). With
/// <c>autoComplete: true</c> (the default) each <see cref="Send"/> resolves without blocking.
/// </para>
/// <para>
/// <b>Honest reachability caveat — <see cref="PartitionsFor"/> returns an EMPTY list.</b> The only
/// mock ctor builds an empty cluster, so <see cref="PartitionsFor"/> succeeds broker-free but returns
/// an empty <see cref="IReadOnlyList{PartitionInfo}"/> for every topic. A populated list is
/// integration-only (a real <see cref="KafkaProducer"/> does live metadata). This is a success with
/// an empty result, not a fault.
/// </para>
/// </remarks>
public sealed class MockProducer : IProducer
{
    private readonly NativeProducer _native;

    /// <summary>
    /// Creates a broker-free mock producer.
    /// </summary>
    /// <param name="autoComplete">
    /// When <see langword="true"/> (the default), the mock resolves each <see cref="Send"/>
    /// automatically (it returns without blocking). When <see langword="false"/>, a
    /// <see cref="Send"/> blocks the calling thread until <see cref="CompleteNext"/> /
    /// <see cref="ErrorNext"/> resolves it from another thread. Flush / close on a mock resolve
    /// broker-free regardless of this flag.
    /// </param>
    public MockProducer(bool autoComplete = true)
    {
        _native = NativeProducer.CreateMock(autoComplete);
    }

    /// <inheritdoc/>
    public RecordMetadata Send(ProducerRecord record) => _native.SendSync(record);

    /// <inheritdoc/>
    public void Flush() => _native.FlushSync();

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _native.PartitionsForSync(topic);

    /// <inheritdoc/>
    public void Close() => _native.CloseSync();

    /// <summary>
    /// Completes the next pending send successfully (Java <c>MockProducer.completeNext()</c> /
    /// Python <c>complete_next()</c>) — for a mock created with <c>autoComplete: false</c>. Unblocks a
    /// manual send's blocking <see cref="Send"/> (call it from a different thread than the one blocked
    /// in <see cref="Send"/>). Inherent on the concrete mock (not on <see cref="IProducer"/>),
    /// mirroring the consumer's mock-only helpers.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Completes the next pending send with an error (Java <c>MockProducer.errorNext(...)</c> /
    /// Python <c>error_next(code, message)</c>) — for a mock created with <c>autoComplete: false</c>.
    /// Faults a manual send's blocking <see cref="Send"/> with a <see cref="KafkaException"/> carrying
    /// <paramref name="code"/> and <paramref name="message"/> (or the default message for the code
    /// when <paramref name="message"/> is <see langword="null"/>). Call it from a different thread
    /// than the one blocked in <see cref="Send"/>.
    /// </summary>
    /// <param name="code">The Kafka error code to complete the send with.</param>
    /// <param name="message">The error message, or <see langword="null"/> for the code's default message.</param>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool ErrorNext(int code, string? message = null) => _native.MockErrorNext(code, message);

    /// <summary>
    /// The number of records in the sent history (Java <c>MockProducer.history().size()</c> /
    /// Python <c>history_count()</c>). A property because the ABI exposes only a count — Java's
    /// <c>history()</c> record list is not surfaced (CLAUDE.md §3).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public int HistoryCount => _native.MockHistoryCount();

    /// <summary>
    /// Clears the sent history and any pending completions (Java <c>MockProducer.clear()</c> /
    /// Python <c>clear()</c>).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public void Clear() => _native.MockClear();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
