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
/// A broker-free Kafka consumer for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.MockConsumer</c>. A thin, Java-shaped forwarder
/// over the internal <see cref="NativeConsumer"/> (like <see cref="AsyncKafkaConsumer"/>),
/// plus mock-only helpers to drive records and errors without a broker.
/// </summary>
/// <remarks>
/// The mock-only helpers (<see cref="Assign"/>, <see cref="AddRecord"/>,
/// <see cref="SetPollError"/>) are <b>inherent methods on this concrete type, not on
/// <see cref="IAsyncConsumer"/></b> (consumer-threading §2; Python's
/// <c>_MockConsumerMixin</c> parity) — tests hold an <see cref="AsyncMockConsumer"/>
/// directly and pass it as an <see cref="IAsyncConsumer"/> where the interface is
/// expected. Single-owner / not thread-safe, same as <see cref="AsyncKafkaConsumer"/>.
/// </remarks>
public sealed class AsyncMockConsumer : IAsyncConsumer
{
    private readonly NativeConsumer _native;

    /// <summary>
    /// Creates a broker-free mock consumer.
    /// </summary>
    /// <param name="autoOffsetReset">
    /// The reset-strategy name (<c>"earliest"</c> / <c>"latest"</c> / <c>"none"</c> /
    /// <c>"by_duration:&lt;ISO-8601&gt;"</c>), or <see langword="null"/> for the default
    /// (<c>"latest"</c>).
    /// </param>
    public AsyncMockConsumer(string? autoOffsetReset = null)
    {
        _native = NativeConsumer.CreateMock(autoOffsetReset);
    }

    /// <inheritdoc/>
    public Task<ConsumerRecords> Poll(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PollWithCallback(timeout, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(topics, cancellationToken);

    /// <inheritdoc/>
    public Task Unsubscribe(CancellationToken cancellationToken = default) =>
        _native.UnsubscribeWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default) =>
        _native.SeekWithCallback(partition.Topic, partition.Partition, offset, cancellationToken);

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(cancellationToken).AsTask();

    /// <inheritdoc/>
    public void Wakeup() => _native.Wakeup();

    /// <inheritdoc/>
    public ConsumerGroupMetadata GroupMetadata() => _native.GroupMetadata();

    /// <inheritdoc/>
    public IReadOnlyCollection<TopicPartition> Assignment() => _native.Assignment();

    /// <inheritdoc/>
    public IReadOnlyCollection<string> Subscription() => _native.Subscription();

    /// <inheritdoc/>
    public IReadOnlyCollection<TopicPartition> Paused() => _native.Paused();

    /// <inheritdoc/>
    public void EnforceRebalance(string? reason = null) => _native.EnforceRebalance(reason);

    /// <summary>
    /// Assigns the mock consumer to the given topic-partitions (mock-only helper). A
    /// partition must be assigned before <see cref="AddRecord"/> can queue a record on
    /// it.
    /// </summary>
    /// <param name="partitions">The topic-partitions to assign.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the assignment.</exception>
    public void Assign(IReadOnlyList<TopicPartition> partitions)
    {
        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        var pairs = new (string Topic, int Partition)[partitions.Count];
        for (int i = 0; i < partitions.Count; i++)
        {
            pairs[i] = (partitions[i].Topic, partitions[i].Partition);
        }

        _native.Assign(pairs);
    }

    /// <summary>
    /// Queues a record to be returned by the next <see cref="Poll"/> (mock-only
    /// helper). The partition must already be assigned (via <see cref="Assign"/>). The
    /// component-tuple form (topic / partition / offset / key / value) needs no public
    /// <see cref="ConsumerRecord"/> constructor this phase (PLAN decision 4); a
    /// Java-style <c>AddRecord(ConsumerRecord)</c> can be added additively later.
    /// </summary>
    /// <param name="topic">The record topic.</param>
    /// <param name="partition">The record partition (must be assigned; non-negative).</param>
    /// <param name="offset">The record offset.</param>
    /// <param name="key">The key bytes, or <see langword="null"/> for an absent key.</param>
    /// <param name="value">The value bytes, or <see langword="null"/> for a tombstone.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record (e.g. unassigned partition).</exception>
    public void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _native.AddRecord(topic, partition, offset, key, value);

    /// <summary>
    /// Injects an error to be returned by the next <see cref="Poll"/> (mock-only
    /// helper; mirrors Java <c>setPollException</c>). The awaiting <see cref="Poll"/>
    /// faults with a <see cref="KafkaException"/> carrying <paramref name="message"/>.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void SetPollError(string message) => _native.SetPollError(message);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
