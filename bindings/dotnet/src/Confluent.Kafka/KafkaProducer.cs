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
/// The real <b>synchronous</b> Kafka producer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.KafkaProducer</c> (which is synchronous). A thin,
/// Java-shaped forwarder over the internal <see cref="NativeProducer"/> lifecycle wrapper (the same
/// wrapper the async <see cref="AsyncKafkaProducer"/> composes — the two client families are
/// siblings over one native producer, not one wrapping the other). All Kafka logic lives in the Rust
/// core; this type only restores the Java (blocking) shape.
/// </summary>
/// <remarks>
/// <para>
/// <b>Blocking, direct sync C ABI (inherited from <see cref="NativeProducer"/>).</b> Each operation
/// calls the synchronous C ABI directly; the core's blocking call parks the caller thread inside the
/// Rust multi-thread runtime (deadlock-free, ffi §A1) — not sync-over-async. The sync producer starts
/// <b>no</b> completion pump (only the async <see cref="AsyncKafkaProducer.Send"/> does), so a
/// sync-only producer spins no background thread.
/// </para>
/// <para>
/// <b>Single-owner / not thread-safe.</b> At most one operation in flight; concurrency is serialized
/// by the Rust core. Do not share one instance across threads without external synchronization.
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="Close"/> is the explicit graceful close that <em>surfaces</em> a close
/// failure; <see cref="Dispose"/> is the teardown that swallows it. Both are idempotent and gated by
/// a single atomic closed flag. There is no <c>DisposeAsync</c> — this is the synchronous surface.
/// </para>
/// </remarks>
public sealed class KafkaProducer : IProducer
{
    private readonly NativeProducer _native;

    /// <summary>
    /// Creates a real producer from a configuration map. Keys are the Java dotted names (e.g.
    /// <c>bootstrap.servers</c>); values are strings.
    /// </summary>
    /// <param name="config">The producer configuration.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    public KafkaProducer(IReadOnlyDictionary<string, string> config)
    {
        _native = NativeProducer.Create(config);
    }

    /// <inheritdoc/>
    public RecordMetadata Send(ProducerRecord record) => _native.SendSync(record);

    /// <inheritdoc/>
    public void Flush() => _native.FlushSync();

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _native.PartitionsForSync(topic);

    /// <inheritdoc/>
    public void Close() => _native.CloseSync();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
