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
/// The real (KIP-848) Kafka consumer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.KafkaConsumer</c>. A thin, Java-shaped forwarder
/// over the internal <see cref="NativeConsumer"/> lifecycle wrapper, which owns the
/// native handle, the completion bridge, and the receive-path copy-out (ffi-marshalling.md
/// §B). All Kafka logic lives in the Rust core; this type only restores the Java shape.
/// </summary>
/// <remarks>
/// <para>
/// <b>Single-owner / not thread-safe (inherited from <see cref="NativeConsumer"/>).</b>
/// At most one operation in flight; concurrency is serialized by the Rust core (a
/// concurrent async op faults its <see cref="Task"/>, a concurrent
/// <see cref="GroupMetadata"/> throws <see cref="InvalidOperationException"/>). Do not
/// share one instance across threads without external synchronization.
/// <see cref="Wakeup"/> is the one deliberately cross-thread member; the canonical
/// pattern (thread A blocked in <see cref="PollAsync"/>, thread B wakes it, thread A
/// then disposes) is safe. Racing <see cref="Wakeup"/> against disposal from a second
/// thread is a misuse that the not-thread-safe contract does not defend against (an
/// accepted single-owner residual; a candidate future hardening).
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="DisposeAsync"/> is the primary path (graceful async close
/// then destroy; swallows any close error); <see cref="Dispose"/> is the blocking
/// fallback. <see cref="CloseAsync"/> is the explicit graceful close that <em>surfaces</em>
/// a close failure. All are idempotent and gated by a single atomic closed flag.
/// </para>
/// </remarks>
public sealed class KafkaConsumer : IConsumer
{
    private readonly NativeConsumer _native;

    /// <summary>
    /// Creates a real KIP-848 consumer from a configuration map. Keys are the Java
    /// dotted names (e.g. <c>bootstrap.servers</c>, <c>group.id</c>,
    /// <c>group.protocol</c>); values are strings.
    /// </summary>
    /// <param name="config">The consumer configuration.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    public KafkaConsumer(IReadOnlyDictionary<string, string> config)
    {
        _native = NativeConsumer.Create(config);
    }

    /// <inheritdoc/>
    public Task<ConsumerRecords> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PollAsync(timeout, cancellationToken);

    /// <inheritdoc/>
    public Task SubscribeAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default) =>
        _native.SubscribeAsync(topics, cancellationToken);

    /// <inheritdoc/>
    public Task UnsubscribeAsync(CancellationToken cancellationToken = default) =>
        _native.UnsubscribeAsync(cancellationToken);

    /// <inheritdoc/>
    public Task SeekAsync(TopicPartition partition, long offset, CancellationToken cancellationToken = default) =>
        _native.SeekAsync(partition.Topic, partition.Partition, offset, cancellationToken);

    /// <inheritdoc/>
    public Task CloseAsync(CancellationToken cancellationToken = default) =>
        _native.CloseAsync(cancellationToken).AsTask();

    /// <inheritdoc/>
    public void Wakeup() => _native.Wakeup();

    /// <inheritdoc/>
    public ConsumerGroupMetadata GroupMetadata() => _native.GroupMetadata();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
