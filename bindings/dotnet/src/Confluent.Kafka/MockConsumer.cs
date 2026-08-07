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
/// A broker-free <b>synchronous</b> Kafka consumer for tests — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.MockConsumer</c>. A thin, Java-shaped forwarder over the
/// internal <see cref="NativeConsumer"/> (like <see cref="KafkaConsumer"/>), plus mock-only
/// helpers to drive records and errors without a broker. The synchronous sibling of
/// <see cref="AsyncMockConsumer"/> — both are forwarders over the same
/// <see cref="NativeConsumer"/>.
/// </summary>
/// <remarks>
/// The mock-only helpers (<see cref="AddRecord"/>, <see cref="SetPollError"/>,
/// <see cref="UpdateBeginningOffset"/>, <see cref="UpdateEndOffset"/>,
/// <see cref="UpdatePartitions"/>) are <b>inherent methods on this concrete type, not on
/// <see cref="IConsumer"/></b> (consumer-threading §2; Python's <c>_MockConsumerMixin</c>
/// parity) — tests hold a <see cref="MockConsumer"/> directly and pass it as an
/// <see cref="IConsumer"/> where the interface is expected. Partition management
/// (<see cref="Assign"/> / <see cref="Pause"/> / <see cref="SeekToBeginning"/> / …) is
/// <b>not</b> mock-only — it lives on <see cref="IConsumer"/> (Java parity; broker-free on the
/// mock). Single-owner / not thread-safe, same as <see cref="KafkaConsumer"/>.
/// </remarks>
public sealed class MockConsumer : IConsumer
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
    public MockConsumer(string? autoOffsetReset = null)
    {
        _native = NativeConsumer.CreateMock(autoOffsetReset);
    }

    /// <inheritdoc/>
    public ConsumerRecords Poll(TimeSpan timeout) => _native.Poll(timeout);

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics) => _native.Subscribe(topics);

    /// <inheritdoc/>
    public void Unsubscribe() => _native.Unsubscribe();

    /// <inheritdoc/>
    public void Assign(IReadOnlyCollection<TopicPartition> partitions) => _native.Assign(partitions);

    /// <inheritdoc/>
    public void Pause(IReadOnlyCollection<TopicPartition> partitions) => _native.Pause(partitions);

    /// <inheritdoc/>
    public void Resume(IReadOnlyCollection<TopicPartition> partitions) => _native.Resume(partitions);

    /// <inheritdoc/>
    public void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) => _native.SeekToBeginning(partitions);

    /// <inheritdoc/>
    public void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) => _native.SeekToEnd(partitions);

    /// <inheritdoc/>
    public long Position(TopicPartition partition) => _native.Position(partition);

    /// <inheritdoc/>
    public void Commit() => _native.CommitSync();

    /// <inheritdoc/>
    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        _native.CommitSyncOffsets(offsets);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(
        IReadOnlyCollection<TopicPartition> partitions) => _native.Committed(partitions);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch) =>
        _native.OffsetsForTimes(timestampsToSearch);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions) => _native.BeginningOffsets(partitions);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions) => _native.EndOffsets(partitions);

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _native.PartitionsFor(topic);

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics() => _native.ListTopics();

    /// <inheritdoc/>
    public void Close() => _native.CloseSync();

    /// <inheritdoc/>
    public void Close(TimeSpan timeout)
    {
        // Precondition BEFORE any native call (ffi §B5), thrown even when closed: a negative
        // timeout is a programmer error. TimeSpan.Zero is valid.
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "Timeout must not be negative.");
        }

        _native.CloseSyncWithTimeout((long)timeout.TotalMilliseconds);
    }

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, long offset) =>
        _native.Seek(partition.Topic, partition.Partition, offset);

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) =>
        _native.SeekWithMetadata(partition.Topic, partition.Partition, offsetAndMetadata);

    /// <inheritdoc/>
    public long? CurrentLag(TopicPartition partition) =>
        _native.CurrentLag(partition.Topic, partition.Partition);

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

    /// <inheritdoc/>
    public void CommitAsync() => _native.CommitAsync();

    /// <summary>
    /// Sets the beginning (earliest) offset for a <c>(topic, partition)</c> used by a
    /// subsequent <see cref="SeekToBeginning"/> reset (mock-only helper; mirrors Java
    /// <c>updateBeginningOffsets</c>, one entry). Lets a <see cref="SeekToBeginning"/> be
    /// observed via a follow-up <see cref="Poll"/>.
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
    /// Registers partition metadata for <paramref name="topic"/> (mock-only helper; mirrors Java
    /// <c>updatePartitions(String, List&lt;PartitionInfo&gt;)</c>). Each of
    /// <paramref name="partitionCount"/> partitions is built with a single leader node
    /// <c>(leaderId, leaderHost, leaderPort)</c> that is also its sole replica and in-sync
    /// replica; offline replicas are empty and the node has no rack.
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
    /// Queues a record to be returned by the next <see cref="Poll"/> (mock-only helper). The
    /// partition must already be assigned (via <see cref="Assign"/>).
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
    /// Injects an error to be returned by the next <see cref="Poll"/> (mock-only helper;
    /// mirrors Java <c>setPollException</c>). The next <see cref="Poll"/> throws a
    /// <see cref="KafkaException"/> carrying <paramref name="message"/>.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void SetPollError(string message) => _native.SetPollError(message);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
