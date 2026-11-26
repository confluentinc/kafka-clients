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
using Confluent.Kafka.Common.Config;
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
/// </remarks>
/// <typeparam name="TKey">The record key type.</typeparam>
/// <typeparam name="TValue">The record value type.</typeparam>
public sealed class MockConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly NativeConsumer _native;
    private readonly MockRecordSlots<TKey, TValue> _slots = new MockRecordSlots<TKey, TValue>();

    /// <summary>
    /// Creates a broker-free mock consumer (Java <c>MockConsumer(String offsetResetStrategy)</c>).
    /// </summary>
    /// <param name="offsetResetStrategy">
    /// The reset-strategy name (<c>"earliest"</c> / <c>"latest"</c> / <c>"none"</c> /
    /// <c>"by_duration:&lt;ISO-8601&gt;"</c>).
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="offsetResetStrategy"/> is <see langword="null"/> ("Auto offset reset strategy is null", as
    /// Java's <c>MockConsumer(String)</c>) or not a valid strategy.
    /// </exception>
    public MockConsumer(string offsetResetStrategy)
    {
        _native = NativeConsumer.CreateMock(offsetResetStrategy, withPump: false);
        _native.OnTornDown(_slots.Clear);
    }

    /// <inheritdoc/>
    public ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout) =>
        _native.PollResolved(timeout, _slots);

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics) => _native.Subscribe(topics);

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener callback) =>
        _native.Subscribe(topics, callback);

    /// <inheritdoc/>
    public void Subscribe(SubscriptionPattern pattern) => _native.Subscribe(pattern);

    /// <inheritdoc/>
    public void Subscribe(SubscriptionPattern pattern, IConsumerRebalanceListener callback) =>
        _native.Subscribe(pattern, callback);

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
    public void Close()
    {
        _native.CloseSync();
    }

    /// <inheritdoc/>
    public void Close(CloseOptions option)
    {
        _native.CloseSync(option);
    }

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, long offset) =>
        _native.Seek(partition, offset);

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) =>
        _native.SeekWithMetadata(partition, offsetAndMetadata);

    /// <inheritdoc/>
    public long? CurrentLag(TopicPartition topicPartition) =>
        _native.CurrentLag(topicPartition);

    /// <inheritdoc/>
    public void Wakeup() => _native.Wakeup();

    /// <inheritdoc/>
    public IConsumerGroupMetadata GroupMetadata() => _native.GroupMetadata();

    /// <inheritdoc/>
    public IReadOnlyCollection<TopicPartition> Assignment() => _native.Assignment();

    /// <inheritdoc/>
    public IReadOnlyCollection<string> Subscription() => _native.Subscription();

    /// <inheritdoc/>
    public IReadOnlyCollection<TopicPartition> Paused() => _native.Paused();

    /// <inheritdoc/>
    public void EnforceRebalance() => _native.EnforceRebalance(null);

    /// <inheritdoc/>
    public void EnforceRebalance(string? reason) => _native.EnforceRebalance(reason);

    /// <inheritdoc/>
    public void CommitAsync() => _native.CommitAsync();

    /// <inheritdoc/>
    public void CommitAsync(IOffsetCommitCallback callback) => _native.CommitAsync(callback);

    /// <inheritdoc/>
    public void CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback) =>
        _native.CommitAsync(offsets, callback);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <summary>
    /// Sets the beginning offset of a partition, used when <see cref="SeekToBeginning"/> resets it (Java
    /// <c>updateBeginningOffsets</c>, one entry).
    /// </summary>
    /// <param name="topic">The record topic.</param>
    /// <param name="partition">The partition (non-negative).</param>
    /// <param name="offset">The beginning offset to reset to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> contains a NUL character, which the native client cannot receive unchanged.
    /// </exception>
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
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> contains a NUL character, which the native client cannot receive unchanged.
    /// </exception>
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
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> or <paramref name="leaderHost"/> contains a NUL character, which the native client
    /// cannot receive unchanged.
    /// </exception>
    public void UpdatePartitions(string topic, int partitionCount, int leaderId, string leaderHost, int leaderPort) =>
        _native.UpdatePartitions(topic, partitionCount, leaderId, leaderHost, leaderPort);

    /// <summary>
    /// Queues <paramref name="record"/> for the next <see cref="Poll"/> (Java <c>MockConsumer.addRecord</c>). The
    /// record's partition must be assigned; <see cref="Poll"/> returns its key and value as the same objects.
    /// A record that is never returned (below the position, or for a partition no longer assigned) is held until
    /// <c>Rebalance</c>, or <c>LosePartitions</c> of its partition, discards it, or the consumer is closed.
    /// </summary>
    /// <param name="record">The record to queue.</param>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is null, or its headers hold a null
    /// header.</exception>
    /// <exception cref="IllegalArgumentException">A header key is null.</exception>
    /// <exception cref="ArgumentException">A header key contains a NUL character, which the native client cannot
    /// receive unchanged.</exception>
    /// <exception cref="IllegalStateException">The consumer is closed, or the partition is not assigned; one whose
    /// topic holds a NUL never is.</exception>
    /// <remarks>Java's <c>addRecord</c> refuses none of these headers; the binding refuses them as a producer
    /// does, since the record crosses the native client. A refused record is not queued. A negative
    /// <c>LeaderEpoch</c> or <c>DeliveryCount</c> polls back as null, which the native record stores as absent;
    /// Java's mock returns the instance added, keeping the value.</remarks>
    public void AddRecord(ConsumerRecord<TKey, TValue> record) => _native.AddRecord(record, _slots);

    /// <summary>
    /// Makes the next <see cref="Poll"/> throw <paramref name="exception"/>, the same instance (Java
    /// <c>MockConsumer.setPollException</c>); <see langword="null"/> clears a pending one.
    /// </summary>
    /// <param name="exception">The exception for the next poll, or <see langword="null"/> to clear a pending one.</param>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    public void SetPollException(KafkaException? exception) => _native.SetPollException(exception);

    /// <summary>
    /// Makes the offset queries throw <paramref name="exception"/>, the same instance (Java
    /// <c>MockConsumer.setOffsetsException</c>); <see langword="null"/> clears a pending one.
    /// </summary>
    /// <param name="exception">The exception for the offset queries, or <see langword="null"/> to clear a pending one.</param>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    public void SetOffsetsException(KafkaException? exception) => _native.SetOffsetsException(exception);

    /// <summary>
    /// Sets the offsets the <c>by_duration</c> reset strategy resets to (Java
    /// <c>MockConsumer.updateDurationOffsets(Map)</c>).
    /// </summary>
    /// <param name="newOffsets">The offset of each partition.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="newOffsets"/> or a partition in it is null, or a partition in it has a null topic.
    /// </exception>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="ArgumentException">
    /// A topic in <paramref name="newOffsets"/> contains a NUL character, which the native client cannot receive
    /// unchanged.
    /// </exception>
    public void UpdateDurationOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        _native.UpdateDurationOffsets(newOffsets);

    /// <summary>Schedules a poll task that does nothing (Java <c>MockConsumer.scheduleNopPollTask()</c>).</summary>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    public void ScheduleNopPollTask() => _native.ScheduleNopPollTask();

    /// <summary>
    /// Sets the most records one poll returns (Java <c>MockConsumer.setMaxPollRecords(long)</c>).
    /// </summary>
    /// <param name="maxPollRecords">The most records one poll returns, at least <c>1</c>.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="maxPollRecords"/> is less than <c>1</c>.</exception>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    public void SetMaxPollRecords(long maxPollRecords) => _native.SetMaxPollRecords(maxPollRecords);

    /// <summary>The timeout of the last poll (Java <c>MockConsumer.lastPollTimeout()</c>).</summary>
    /// <returns>The last poll's timeout, or <see langword="null"/> before the first poll.</returns>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    public TimeSpan? LastPollTimeout() => _native.LastPollTimeout();

    /// <summary>Whether the consumer has been closed (Java <c>MockConsumer.closed()</c>).</summary>
    /// <returns><see langword="true"/> once the consumer is closed or disposed.</returns>
    public bool Closed() => _native.MockClosed();

    /// <summary>
    /// Simulates a rebalance to the new full assignment <paramref name="newAssignment"/> (Java
    /// <c>MockConsumer.rebalance(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// While a listener is registered, <c>OnPartitionsRevoked</c> fires with the removed partitions, only if there
    /// are any, then <c>OnPartitionsAssigned</c> with the added ones, possibly none, on the calling thread. It
    /// returns after they have returned; an exception from either is thrown here as the same instance. Buffered
    /// records are cleared, as in Java.
    /// </remarks>
    /// <param name="newAssignment">The new full assignment.</param>
    /// <exception cref="ArgumentNullException"><paramref name="newAssignment"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// An element or an element topic is null, or the consumer is manually assigned (Java's
    /// <c>IllegalArgumentException</c>).
    /// The topic of a partition in <paramref name="newAssignment"/> contains a NUL character, which the native client
    /// cannot receive unchanged.
    /// </exception>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    public void Rebalance(IReadOnlyCollection<TopicPartition> newAssignment) => _native.Rebalance(newAssignment, _slots);

    /// <summary>
    /// Simulates losing <paramref name="partitionsLost"/> without a clean revocation (Java
    /// <c>MockConsumer.losePartitions(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// The buffered records of the lost partitions are cleared; then, while a listener is registered,
    /// <see cref="ConsumerRebalanceListenerExtensions.OnPartitionsLost(IConsumerRebalanceListener, IReadOnlyCollection{TopicPartition})"/>
    /// runs with them on the calling thread; then they leave the <see cref="Assignment"/>.
    /// An exception from the listener is thrown here as the same instance, and the assignment is left unchanged.
    /// </remarks>
    /// <param name="partitionsLost">The partitions to lose; every one must be currently assigned.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitionsLost"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// An element topic is null.
    /// The topic of a partition in <paramref name="partitionsLost"/> contains a NUL character, which the native client
    /// cannot receive unchanged.
    /// </exception>
    /// <exception cref="IllegalStateException">
    /// The consumer is closed, or a partition, a null one included, is not currently assigned.
    /// </exception>
    public void LosePartitions(IReadOnlyCollection<TopicPartition> partitionsLost) => _native.LosePartitions(partitionsLost, _slots);

    /// <inheritdoc/>
    public void Dispose()
    {
        _native.Dispose();
    }
}
