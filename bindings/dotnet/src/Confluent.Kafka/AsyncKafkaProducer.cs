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
/// <b>M11/P2 — the async PERIPHERALS only (no <c>Send</c> yet).</b> This first public producer
/// implements the <see cref="IAsyncProducer"/> peripheral surface — <see cref="Flush"/> /
/// <see cref="Close(CancellationToken)"/> / <see cref="Close(TimeSpan, CancellationToken)"/> /
/// <see cref="PartitionsFor"/>. <c>Send</c> (and <c>ProducerRecord</c> / <c>RecordMetadata</c>)
/// arrives additively in a later phase.
/// </para>
/// <para>
/// <b>Disposal — the graceful upgrade (ffi §A7).</b> <see cref="DisposeAsync"/> is primary
/// (graceful async close via <c>Producer_close_async</c>, then <c>Producer_destroy</c>; swallows
/// any close error); <see cref="Dispose"/> is the blocking fallback (graceful sync
/// <c>Producer_close</c>, then destroy). <see cref="Close(CancellationToken)"/> is the explicit
/// graceful close that <em>surfaces</em> a close failure. All are idempotent (one-shot latch);
/// a close error never prevents the destroy. The graceful close→destroy orchestration is
/// layered here, above <see cref="NativeProducer"/> (whose teardown stays the pinned
/// <c>Producer_destroy</c>-only sequence) — via the shared <see cref="ProducerTeardown"/>.
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

    // One-shot close latch (0 = open, 1 = closing/closed): makes Close / Dispose / DisposeAsync
    // idempotent and mutually exclusive so exactly one graceful-close → destroy runs. Atomic (not
    // a plain bool) to avoid a torn read/write — the same discipline as the consumer's flag.
    private int _closed;

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
    public Task Flush(CancellationToken cancellationToken = default) =>
        _native.FlushWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return TryBeginClose()
            ? ProducerTeardown.CloseGracefulThenDestroyAsync(_native)
            : Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must not be negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        return TryBeginClose()
            ? ProducerTeardown.CloseWithDeadlineThenDestroyAsync(_native, timeout, cancellationToken)
            : Task.CompletedTask;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (TryBeginClose())
        {
            ProducerTeardown.CloseSyncThenDestroy(_native);
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() =>
        TryBeginClose()
            ? ProducerTeardown.CloseBestEffortThenDestroyAsync(_native)
            : default;

    private bool TryBeginClose() => Interlocked.CompareExchange(ref _closed, 1, 0) == 0;
}
