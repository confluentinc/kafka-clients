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
/// The async, typed Kafka producer surface — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.Producer&lt;K, V&gt;</c>, implemented by
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> (the real client) and
/// <see cref="AsyncMockProducer{TKey, TValue}"/> (a broker-free test helper). The C#-idiomatic
/// <c>I</c> prefix marks the interface (Framework Design Guidelines / analyzer CA1715); the
/// <c>Async</c> prefix on the type marks this async surface (the sync mirror is
/// <see cref="IProducer{TKey, TValue}"/>). Method names carry <b>no <c>Async</c> suffix</b> — the
/// sync-vs-async distinction is carried by the interface/type, matching Java's method names and
/// the Python sibling.
/// </summary>
/// <remarks>
/// <para>
/// <b>Generic-only (M11/P5, PLAN §3.1).</b> The producer surface is generic-only, mirroring the
/// consumer's M6/P1b conversion — there is no bytes-specialized sibling. Bytes users write
/// <c>IAsyncProducer&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>. Each generic
/// interface is flat and carries all its members — there is <b>no</b> <c>IProducerCommon</c> base
/// (unlike the consumer's <c>IConsumerCommon</c>): <see cref="Flush"/> / <see cref="Close(CancellationToken)"/>
/// / <see cref="PartitionsFor"/> have different signatures on the sync vs async interface, so there
/// is no sharing opportunity —
/// the one identical member, <see cref="Metrics"/>, is simply declared on both (M11/P8
/// decision D-6).
/// </para>
/// <para>
/// <b>Subset of Java's <c>Producer</c>.</b> The send path (<see cref="Send"/> with
/// <see cref="ProducerRecord{TKey, TValue}"/> / <see cref="RecordMetadata"/>) plus the async
/// peripherals (<see cref="Flush"/> / <see cref="Close(CancellationToken)"/> /
/// <see cref="PartitionsFor"/>) and the synchronous <see cref="Metrics"/> state read (M11/P8).
/// Transactions remain deferred.
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
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public interface IAsyncProducer<TKey, TValue> : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Serializes and publishes <paramref name="record"/> to its topic, completing when the cluster
    /// acknowledges it (Java <c>Producer.send(record)</c> — which returns a
    /// <c>Future&lt;RecordMetadata&gt;</c>, so a <see cref="Task{TResult}"/> here, CLAUDE.md §4). The
    /// key / value are serialized on the caller's thread <b>before</b> the send is enqueued; the
    /// serialized bytes are copied into the send buffer during the native call, so the caller may
    /// reuse or mutate them the moment this returns (ffi §A4).
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
    /// <exception cref="SerializationException">A serializer threw while encoding the key or value (thrown synchronously).</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    Task<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);

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

    /// <summary>
    /// Returns a point-in-time snapshot of the producer's metrics (Java
    /// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c>). <b>Stays synchronous</b> on both
    /// producer interfaces — <c>metrics()</c> does not block in Java (CLAUDE.md §4 lists producer
    /// <c>Metrics</c> under "stays sync"), so it is not a <see cref="System.Threading.Tasks.Task"/>
    /// even on the async surface. A <b>method</b>, not a property, per the same FDG rule as the
    /// consumer's <c>Metrics()</c> / <c>Assignment()</c>: it does a P/Invoke, can throw, and
    /// returns a fresh owned snapshot per call.
    /// </summary>
    /// <remarks>
    /// Declared identically on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/> rather than on a shared base: the producer has no
    /// <c>IProducerCommon</c> (unlike the consumer's <c>IConsumerCommon</c>) and one shared member
    /// does not justify introducing one (M11/P8 decision D-6, DoD §7). On a
    /// <see cref="MockProducer{TKey, TValue}"/> the snapshot is <b>empty</b> (Java
    /// <c>MockProducer.metrics()</c> parity — its metric map is empty unless seeded, and the ABI
    /// exposes no seeding entry point).
    /// </remarks>
    /// <returns>The producer's metrics, keyed by <see cref="MetricName"/> value identity.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    IReadOnlyDictionary<MetricName, IMetric> Metrics();
}
