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
/// The async Kafka producer surface — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.Producer</c>, implemented by
/// <see cref="AsyncKafkaProducer"/> (the real client) and <see cref="AsyncMockProducer"/> (a
/// broker-free test helper). The C#-idiomatic <c>I</c> prefix marks the interface (Framework
/// Design Guidelines / analyzer CA1715); the <c>Async</c> prefix on the type marks this async
/// surface (a future sync <c>IProducer</c> surface is reserved). Method names carry <b>no
/// <c>Async</c> suffix</b> — the sync-vs-async distinction is carried by the interface/type,
/// matching Java's method names and the Python sibling.
/// </summary>
/// <remarks>
/// <para>
/// <b>Additive-growth surface — M11/P2 peripherals only, no <c>Send</c> yet.</b> This is a
/// deliberate <em>subset</em> of Java's <c>Producer</c> — the async peripherals
/// (<see cref="Flush"/> / <see cref="Close(CancellationToken)"/> / <see cref="PartitionsFor"/>).
/// The <c>Send</c> method (and <c>ProducerRecord</c> / <c>RecordMetadata</c>) arrives in a later
/// phase as an <b>additive</b> member on this same interface — exactly as the sync
/// <c>IConsumer</c> grew across sub-phases. An <see cref="IAsyncProducer"/> without <c>Send</c>
/// is intentional this phase. The surface is safe to grow additively because the binding is
/// pre-publish with no external implementers.
/// </para>
/// <para>
/// <b>Cancellation is best-effort (no native abort).</b> Unlike the consumer, the producer has
/// no <c>wakeup()</c>: a canceled <see cref="CancellationToken"/> cancels the returned
/// <see cref="Task"/>'s .NET-side wait, but does not abort the in-flight native op — it
/// continues to completion (a host-idiom addition Java lacks; CLAUDE.md §4).
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="IAsyncDisposable.DisposeAsync"/> is the primary path (graceful
/// async close then destroy; swallows any close error); <see cref="IDisposable.Dispose"/> is
/// the blocking fallback (graceful sync close then destroy). Both are idempotent, and a close
/// error never prevents the underlying handle from being freed.
/// </para>
/// </remarks>
public interface IAsyncProducer : IAsyncDisposable, IDisposable
{
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
}
