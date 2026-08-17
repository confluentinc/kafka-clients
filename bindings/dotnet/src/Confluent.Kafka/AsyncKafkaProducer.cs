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
/// The real Kafka producer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.producer.KafkaProducer</c>. A thin, Java-shaped forwarder over
/// the internal <see cref="NativeProducer"/> lifecycle wrapper, which owns the native handle
/// and the completion bridge (ffi-marshalling.md §A). All Kafka logic lives in the Rust core;
/// this type only restores the Java shape.
/// </summary>
/// <remarks>
/// <para>
/// <b>Send + async peripherals.</b> This producer implements the <see cref="IAsyncProducer"/>
/// surface — <see cref="Send"/> (the M11/P3 send path over the inline pull-pump, ffi §A7 Option C)
/// plus the M11/P2 peripherals <see cref="Flush"/> / <see cref="Close(CancellationToken)"/> /
/// <see cref="PartitionsFor"/>. The typed generic producer and transactions remain deferred.
/// </para>
/// <para>
/// <b>Disposal — thin forwarders over <see cref="NativeProducer"/> (ffi §A7; M11/P2.1).</b>
/// <see cref="DisposeAsync"/> / <see cref="Dispose"/> / <see cref="Close(CancellationToken)"/> each
/// forward straight to the matching <see cref="NativeProducer"/> teardown flavor, which owns the
/// whole graceful-close → destroy and the one-shot latch (mirroring the consumer's thin forwarders
/// over <c>NativeConsumer</c>). <see cref="DisposeAsync"/> is primary (async close via
/// <c>Producer_close_async</c>, swallows any close error); <see cref="Dispose"/> is the blocking
/// fallback (sync <c>Producer_close</c>, swallows); <see cref="Close(CancellationToken)"/> surfaces
/// a close failure. All are idempotent (the merged latch lives in <see cref="NativeProducer"/>);
/// a close error never prevents the destroy.
/// </para>
/// <para>
/// <b>Cancellation is best-effort (no native abort).</b> The producer has no <c>wakeup()</c>, so
/// a canceled token cancels the returned task's .NET-side wait but does not abort the in-flight
/// native op.
/// </para>
/// </remarks>
public sealed class AsyncKafkaProducer : IAsyncProducer
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
    public AsyncKafkaProducer(IReadOnlyDictionary<string, string> config)
    {
        _native = NativeProducer.Create(config);
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

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
