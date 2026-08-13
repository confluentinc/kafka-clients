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
/// <b>Peripherals only — no send-control helpers this phase.</b> The Java <c>MockProducer</c>
/// send-driving helpers (<c>completeNext</c> / <c>errorNext</c> / <c>history</c> / <c>clear</c>)
/// are part of the deferred send surface, so they are <b>not</b> here yet — this is a functional
/// mock for the peripherals only. They arrive with <c>Send</c> in a later phase, as inherent
/// methods on this concrete type (the consumer's <c>AddRecord</c> / <c>SetPollError</c> mock-only
/// precedent).
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
    /// When <see langword="true"/> (the default), the mock resolves sends automatically. It has
    /// no effect on the M11/P2 peripherals (flush / close on a mock resolve broker-free
    /// regardless); it is carried for parity with the later send phase.
    /// </param>
    public AsyncMockProducer(bool autoComplete = true)
    {
        _native = NativeProducer.CreateMock(autoComplete);
    }

    /// <inheritdoc/>
    public Task Flush(CancellationToken cancellationToken = default) =>
        _native.FlushWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) =>
        _native.Close(cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
