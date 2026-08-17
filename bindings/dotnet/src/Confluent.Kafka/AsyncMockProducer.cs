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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// A broker-free Kafka producer for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.MockProducer</c>, for the M11/P2 peripherals. A thin,
/// Java-shaped forwarder over the internal <see cref="NativeProducer"/> (like
/// <see cref="AsyncKafkaProducer"/>), constructed over a <c>MockProducer</c> so
/// <see cref="Flush"/> / <see cref="Close(CancellationToken)"/> / <see cref="PartitionsFor"/>
/// resolve without a broker.
/// </summary>
/// <remarks>
/// <para>
/// <b>Send + the send-control helpers (M11/P3).</b> Beyond the <see cref="IAsyncProducer"/> surface
/// (<see cref="Send"/> + the peripherals), this mock exposes the Java <c>MockProducer</c>
/// send-driving helpers as inherent methods on the concrete type (not on the interface) —
/// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> / <see cref="HistoryCount"/> /
/// <see cref="Clear"/> — mirroring the consumer's <c>AddRecord</c> / <c>SetPollError</c> mock-only
/// precedent. With <c>autoComplete: false</c> a send stays pending until
/// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> resolves it.
/// </para>
/// <para>
/// <b>Honest reachability caveat — <see cref="PartitionsFor"/> returns an EMPTY list.</b> The only
/// mock ctor (<c>MockProducer_new</c>) builds an empty cluster, so <see cref="PartitionsFor"/>
/// succeeds broker-free but returns an empty <see cref="IReadOnlyList{PartitionInfo}"/> for every
/// topic. A populated list is integration-only (a real <see cref="AsyncKafkaProducer"/> does live
/// metadata). This is a success with an empty result, not a fault.
/// </para>
/// <para>
/// <b>Disposal</b> is identical to <see cref="AsyncKafkaProducer"/> — thin forwarders over
/// <see cref="NativeProducer"/> (which owns the graceful close→destroy + the one-shot latch,
/// ffi §A7; M11/P2.1): the mock's <c>close_async</c> / <c>close</c> resolve broker-free.
/// </para>
/// </remarks>
public sealed class AsyncMockProducer : IAsyncProducer
{
    private readonly NativeProducer _native;

    /// <summary>
    /// Creates a broker-free mock producer.
    /// </summary>
    /// <param name="autoComplete">
    /// When <see langword="true"/> (the default), the mock resolves each <see cref="Send"/>
    /// automatically. When <see langword="false"/>, a send stays pending until
    /// <see cref="CompleteNext"/> / <see cref="ErrorNext"/> resolves it. Flush / close on a mock
    /// resolve broker-free regardless of this flag.
    /// </param>
    public AsyncMockProducer(bool autoComplete = true)
    {
        _native = NativeProducer.CreateMock(autoComplete);
    }

    /// <inheritdoc/>
    public Task<RecordMetadata> Send(ProducerRecord record, CancellationToken cancellationToken = default) =>
        _native.SendViaPump(record, cancellationToken);

    /// <inheritdoc/>
    public Task Flush(CancellationToken cancellationToken = default) =>
        _native.FlushWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(cancellationToken);

    /// <summary>
    /// Completes the next pending send successfully (Java <c>MockProducer.completeNext()</c> /
    /// Python <c>complete_next()</c>) — for a mock created with <c>autoComplete: false</c>. Drives
    /// a manual send's <see cref="Send"/> <see cref="Task"/> to success. Inherent on the concrete
    /// mock (not on <see cref="IAsyncProducer"/>), mirroring the consumer's mock-only helpers.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Completes the next pending send with an error (Java <c>MockProducer.errorNext(...)</c> /
    /// Python <c>error_next(code, message)</c>) — for a mock created with <c>autoComplete: false</c>.
    /// Faults a manual send's <see cref="Send"/> <see cref="Task"/> with a <see cref="KafkaException"/>
    /// carrying <paramref name="code"/> and <paramref name="message"/> (or the default message for
    /// the code when <paramref name="message"/> is <see langword="null"/>).
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

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
