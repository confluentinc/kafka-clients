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
/// <b>Additive-growth surface — the send path landed in M11/P3.</b> This is still a deliberate
/// <em>subset</em> of Java's <c>Producer</c>: the send path (<see cref="Send"/> with
/// <see cref="ProducerRecord"/> / <see cref="RecordMetadata"/>, M11/P3) plus the async peripherals
/// (<see cref="Flush"/> / <see cref="Close(CancellationToken)"/> / <see cref="PartitionsFor"/>,
/// M11/P2). <see cref="Send"/> was added <b>additively</b> to this interface — exactly as the sync
/// <c>IConsumer</c> grew across sub-phases — safe because the binding is pre-publish with no
/// external implementers. Transactions / metrics and the typed generic producer remain deferred.
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
    /// Publishes <paramref name="record"/> to its topic, completing when the cluster acknowledges
    /// it (Java <c>Producer.send(record)</c> — which returns a <c>Future&lt;RecordMetadata&gt;</c>,
    /// so a <see cref="Task{TResult}"/> here, CLAUDE.md §4). The record's key / value bytes are
    /// copied into the send buffer during the call, so the caller may reuse or mutate them the
    /// moment this returns (ffi §A4).
    /// </summary>
    /// <param name="record">The record to publish.</param>
    /// <param name="cancellationToken">
    /// Best-effort cancellation of the .NET wait. An already-canceled token throws
    /// <see cref="OperationCanceledException"/> before the send is enqueued; a token that fires
    /// afterwards cancels the returned <see cref="Task{TResult}"/> but does <b>not</b> abort the
    /// in-flight native send (the producer has no <c>wakeup()</c>).
    /// </param>
    /// <returns>
    /// A task resolving with the published record's <see cref="RecordMetadata"/>, or faulting with
    /// a <see cref="KafkaException"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    Task<RecordMetadata> Send(ProducerRecord record, CancellationToken cancellationToken = default);

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
