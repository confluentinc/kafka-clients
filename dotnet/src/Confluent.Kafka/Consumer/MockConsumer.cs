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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A broker-free synchronous consumer for tests, Java's <c>org.apache.kafka.clients.consumer.MockConsumer</c>.
/// </summary>
/// <remarks>
/// <para>
/// The mock-only helpers (<see cref="AddRecord"/>, <see cref="SetPollException"/>,
/// <see cref="UpdateBeginningOffset"/>, <see cref="UpdateEndOffset"/>, <see cref="UpdatePartitions"/>,
/// <see cref="Rebalance"/>, <see cref="LosePartitions"/>) are on this type only; hold a
/// <see cref="MockConsumer{TKey, TValue}"/> and pass it where an <see cref="IConsumer{TKey, TValue}"/> is expected.
/// </para>
/// <para>
/// Two deviations from Java's <c>MockConsumer</c>, shared with <see cref="AsyncMockConsumer{TKey, TValue}"/>: the
/// constructor takes the deserializers, because <see cref="Poll"/> decodes bytes as
/// <see cref="KafkaConsumer{TKey, TValue}"/> does; and <see cref="AddRecord"/> takes bytes, not a typed record.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type produced by the key deserializer.</typeparam>
/// <typeparam name="TValue">The value type produced by the value deserializer.</typeparam>
public sealed class MockConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly NativeConsumer _native;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;

    /// <summary>
    /// Creates a broker-free mock consumer whose <see cref="Poll"/> decodes the records queued by
    /// <see cref="AddRecord"/> with the two deserializers.
    /// </summary>
    /// <param name="keyDeserializer">Deserializer for record keys.</param>
    /// <param name="valueDeserializer">Deserializer for record values.</param>
    /// <param name="autoOffsetReset">
    /// The reset-strategy name (<c>"earliest"</c> / <c>"latest"</c> / <c>"none"</c> /
    /// <c>"by_duration:&lt;ISO-8601&gt;"</c>), or <see langword="null"/> for the default
    /// (<c>"latest"</c>).
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="keyDeserializer"/> or <paramref name="valueDeserializer"/> is null.
    /// </exception>
    public MockConsumer(
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        string? autoOffsetReset = null)
    {
        _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
        _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
        _native = NativeConsumer.CreateMock(autoOffsetReset, withPump: false);
    }

    /// <inheritdoc/>
    public ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout) =>
        _native.PollTyped(timeout, _keyDeserializer, _valueDeserializer);

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics) => _native.Subscribe(topics);

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener) =>
        _native.Subscribe(topics, listener);

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
    public long Position(TopicPartition partition, TimeSpan timeout) => _native.Position(partition, timeout);

    /// <inheritdoc/>
    public void Commit() => _native.CommitSync();

    /// <inheritdoc/>
    public void Commit(TimeSpan timeout) => _native.CommitSync(timeout);

    /// <inheritdoc/>
    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        _native.CommitSyncOffsets(offsets);

    /// <inheritdoc/>
    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout) =>
        _native.CommitSyncOffsets(offsets, timeout);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions) => _native.Committed(partitions);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => _native.Committed(partitions, timeout);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch) =>
        _native.OffsetsForTimes(timestampsToSearch);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, TimeSpan timeout) =>
        _native.OffsetsForTimes(timestampsToSearch, timeout);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions) => _native.BeginningOffsets(partitions);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => _native.BeginningOffsets(partitions, timeout);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions) => _native.EndOffsets(partitions);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => _native.EndOffsets(partitions, timeout);

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _native.PartitionsFor(topic);

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic, TimeSpan timeout) =>
        _native.PartitionsFor(topic, timeout);

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics() => _native.ListTopics();

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(TimeSpan timeout) =>
        _native.ListTopics(timeout);

    /// <inheritdoc/>
    public void Close() => _native.CloseSync();

    /// <inheritdoc/>
    public void Close(CloseOptions option) => _native.CloseSync(option);

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

    /// <inheritdoc/>
    public void CommitAsync(IOffsetCommitCallback callback) => _native.CommitAsync(callback);

    /// <inheritdoc/>
    public void CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null) =>
        _native.CommitAsync(offsets, callback);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <inheritdoc/>
    public ConsumerHandle Handle() => _native.CreateReentrancyHandle();

    /// <summary>
    /// Sets the beginning offset of a partition, used when <see cref="SeekToBeginning"/> resets it (Java
    /// <c>updateBeginningOffsets</c>, one entry).
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
    /// Sets the end offset of a partition, used when <see cref="SeekToEnd"/> resets it (Java
    /// <c>updateEndOffsets</c>, one entry).
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
    /// Registers <paramref name="partitionCount"/> partitions of <paramref name="topic"/> for
    /// <see cref="PartitionsFor(string)"/> and <see cref="ListTopics()"/> (Java <c>updatePartitions</c>). Each
    /// partition's leader, sole replica and sole in-sync replica is the node
    /// <c>(leaderId, leaderHost, leaderPort)</c>, with no rack and no offline replicas.
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
    /// Queues a record for the next <see cref="Poll"/> (Java <c>addRecord</c>). The partition must be assigned.
    /// </summary>
    /// <param name="topic">The record topic.</param>
    /// <param name="partition">The record partition (must be assigned; non-negative).</param>
    /// <param name="offset">The record offset.</param>
    /// <param name="key">The key bytes, or <see langword="null"/> for an absent key.</param>
    /// <param name="value">The value bytes, or <see langword="null"/> for a tombstone.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">The partition is not assigned.</exception>
    public void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _native.AddRecord(topic, partition, offset, key, value);

    /// <summary>
    /// Makes the next <see cref="Poll"/> throw <paramref name="exception"/>, the same instance (Java
    /// <c>MockConsumer.setPollException</c>); <see langword="null"/> clears a pending one.
    /// </summary>
    /// <param name="exception">The exception for the next poll, or <see langword="null"/> to clear a pending one.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void SetPollException(KafkaException? exception) => _native.SetPollException(exception);

    /// <summary>
    /// Simulates a rebalance to the new full assignment <paramref name="partitions"/> (Java
    /// <c>MockConsumer.rebalance(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// While a listener is registered, <c>OnPartitionsRevoked</c> fires with the removed partitions, only if there
    /// are any, then <c>OnPartitionsAssigned</c> with the added ones, possibly none, on the calling thread. It
    /// returns after they have returned; an exception from either is thrown here as the same instance. Buffered
    /// records are cleared, as in Java.
    /// </remarks>
    /// <param name="partitions">The new full assignment.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// An element topic is null, or the consumer is manually assigned (Java's <c>IllegalArgumentException</c>).
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public void Rebalance(IReadOnlyCollection<TopicPartition> partitions) => _native.Rebalance(partitions);

    /// <summary>
    /// Simulates losing <paramref name="partitions"/> without a clean revocation (Java
    /// <c>MockConsumer.losePartitions(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// The buffered records of the lost partitions are cleared; then, while a listener is registered,
    /// <c>OnPartitionsLost</c> fires with them on the calling thread; then they leave the <see cref="Assignment"/>.
    /// An exception from the listener is thrown here as the same instance, and the assignment is left unchanged.
    /// </remarks>
    /// <param name="partitions">The partitions to lose; every one must be currently assigned.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// A partition is not currently assigned (Java's <c>IllegalStateException</c>).
    /// </exception>
    public void LosePartitions(IReadOnlyCollection<TopicPartition> partitions) => _native.LosePartitions(partitions);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
