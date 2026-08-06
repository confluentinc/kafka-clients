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
/// The mock-only helpers (<see cref="AddRecord"/>, <see cref="SetPollError"/>,
/// <see cref="UpdateBeginningOffset"/>, <see cref="UpdateEndOffset"/>,
/// <see cref="UpdatePartitions"/>) are <b>inherent methods on this concrete type, not on
/// <see cref="IAsyncConsumer"/></b>
/// (consumer-threading §2; Python's <c>_MockConsumerMixin</c> parity) — tests hold an
/// <see cref="AsyncMockConsumer"/> directly and pass it as an <see cref="IAsyncConsumer"/>
/// where the interface is expected. Partition management (<see cref="Assign"/> /
/// <see cref="Pause"/> / <see cref="SeekToBeginning"/> / …) is <b>not</b> mock-only — it
/// lives on <see cref="IAsyncConsumer"/> (Java parity; broker-free on the mock), so test
/// setup uses the public async <see cref="Assign"/>. Single-owner / not thread-safe, same
/// as <see cref="AsyncKafkaConsumer"/>.
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
    public Task Assign(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.AssignWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task Pause(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.PauseWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task Resume(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.ResumeWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.SeekToBeginningWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task SeekToEnd(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.SeekToEndWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default) =>
        _native.PositionWithCallback(partition, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> Committed(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.CommittedWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default) =>
        _native.OffsetsForTimesWithCallback(timestampsToSearch, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.BeginningOffsetsWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.EndOffsetsWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken cancellationToken = default) =>
        _native.ListTopicsWithCallback(cancellationToken);

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
    /// Sets the beginning (earliest) offset for a <c>(topic, partition)</c> used by a
    /// subsequent <see cref="SeekToBeginning"/> reset (mock-only helper; mirrors Java
    /// <c>updateBeginningOffsets</c>, one entry). Lets a <see cref="SeekToBeginning"/> be
    /// observed end-to-end via a follow-up <see cref="Poll"/>.
    /// </summary>
    /// <param name="topic">The record topic.</param>
    /// <param name="partition">The partition (non-negative).</param>
    /// <param name="offset">The beginning offset to reset to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void UpdateBeginningOffset(string topic, int partition, long offset) =>
        _native.UpdateBeginningOffset(topic, partition, offset);

    /// <summary>
    /// Sets the end (latest) offset for a <c>(topic, partition)</c> used by a subsequent
    /// <see cref="SeekToEnd"/> reset (mock-only helper; mirrors Java <c>updateEndOffsets</c>,
    /// one entry). The <see cref="SeekToEnd"/> analog of <see cref="UpdateBeginningOffset"/>.
    /// </summary>
    /// <param name="topic">The record topic.</param>
    /// <param name="partition">The partition (non-negative).</param>
    /// <param name="offset">The end offset to reset to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void UpdateEndOffset(string topic, int partition, long offset) =>
        _native.UpdateEndOffset(topic, partition, offset);

    /// <summary>
    /// Registers partition metadata for <paramref name="topic"/> (mock-only helper; mirrors
    /// Java <c>updatePartitions(String, List&lt;PartitionInfo&gt;)</c>) so
    /// <see cref="PartitionsFor"/> and <see cref="ListTopics"/> return data broker-free. Each
    /// of <paramref name="partitionCount"/> partitions is built with a single leader node
    /// <c>(leaderId, leaderHost, leaderPort)</c> that is also its sole replica and in-sync
    /// replica; offline replicas are empty and the node has no rack (a reachable-slice limit
    /// of the mock — the marshaller's offline-replica and rack paths are still exercised
    /// structurally, as an empty list / null).
    /// </summary>
    /// <param name="topic">The topic to register partition metadata for.</param>
    /// <param name="partitionCount">The number of partitions to register (non-negative).</param>
    /// <param name="leaderId">The leader node id for every partition.</param>
    /// <param name="leaderHost">The leader host for every partition.</param>
    /// <param name="leaderPort">The leader port for every partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> or <paramref name="leaderHost"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void UpdatePartitions(string topic, int partitionCount, int leaderId, string leaderHost, int leaderPort) =>
        _native.UpdatePartitions(topic, partitionCount, leaderId, leaderHost, leaderPort);

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
