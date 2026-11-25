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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A broker-free asynchronous consumer for tests, Java's <c>org.apache.kafka.clients.consumer.MockConsumer</c>.
/// </summary>
/// <remarks>
/// <para>
/// The mock-only helpers (<see cref="AddRecord"/>, <see cref="SetPollException"/>,
/// <see cref="UpdateBeginningOffset"/>, <see cref="UpdateEndOffset"/>, <see cref="UpdatePartitions"/>,
/// <see cref="Rebalance"/>, <see cref="LosePartitions"/>) are on this type only; hold an
/// <see cref="AsyncMockConsumer{TKey, TValue}"/> and pass it where an <see cref="IAsyncConsumer{TKey, TValue}"/>
/// is expected.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The record key type.</typeparam>
/// <typeparam name="TValue">The record value type.</typeparam>
public sealed class AsyncMockConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue>
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
    public AsyncMockConsumer(string offsetResetStrategy)
    {
        _native = NativeConsumer.CreateMock(offsetResetStrategy);
        _native.OnTornDown(_slots.Clear);
    }

    /// <inheritdoc/>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> when the poll ended before any record was delivered. A poll that
    /// already has its records returns them, whether the token fired or not.
    /// </param>
    /// <remarks>
    /// The mock removes the records a poll delivers, as Java's <c>MockConsumer.poll</c> does, so it cannot deliver
    /// them again after a seek back. Unlike <see cref="AsyncKafkaConsumer{TKey, TValue}"/>, which drops a cancelled
    /// poll's records and seeks back to them, this poll never drops a batch: a cancellation that comes after the
    /// records were delivered is ignored, and one that ends the poll before any record was delivered leaves the
    /// records added for the next <see cref="Poll"/>.
    /// </remarks>
    public Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PollWithCallback(timeout, _slots, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(topics, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener callback,
        CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(topics, callback, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(
        IReadOnlyCollection<string> topics,
        IAsyncConsumerRebalanceListener callback,
        CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(topics, callback, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(SubscriptionPattern pattern, CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(pattern, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(
        SubscriptionPattern pattern,
        IConsumerRebalanceListener callback,
        CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(pattern, callback, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(
        SubscriptionPattern pattern,
        IAsyncConsumerRebalanceListener callback,
        CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(pattern, callback, cancellationToken);

    /// <inheritdoc/>
    public Task Unsubscribe(CancellationToken cancellationToken = default) =>
        _native.UnsubscribeWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default) =>
        _native.SeekWithCallback(partition, offset, cancellationToken);

    /// <inheritdoc/>
    public Task Seek(
        TopicPartition partition,
        OffsetAndMetadata offsetAndMetadata,
        CancellationToken cancellationToken = default) =>
        _native.SeekWithCallback(partition, offsetAndMetadata, cancellationToken);

    /// <inheritdoc/>
    public Task<long?> CurrentLag(TopicPartition topicPartition, CancellationToken cancellationToken = default) =>
        _native.CurrentLagWithCallback(topicPartition, cancellationToken);

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
    public Task<long> Position(TopicPartition partition, TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PositionWithCallback(partition, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task Commit(CancellationToken cancellationToken = default) =>
        _native.CommitWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task Commit(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.CommitWithCallback(timeout, cancellationToken);

    /// <inheritdoc/>
    public Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, CancellationToken cancellationToken = default) =>
        _native.CommitWithCallback(offsets, cancellationToken);

    /// <inheritdoc/>
    public Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.CommitWithCallback(offsets, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.CommittedWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.CommittedWithCallback(partitions, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default) =>
        _native.OffsetsForTimesWithCallback(timestampsToSearch, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.OffsetsForTimesWithCallback(timestampsToSearch, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.BeginningOffsetsWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.BeginningOffsetsWithCallback(partitions, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default) =>
        _native.EndOffsetsWithCallback(partitions, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.EndOffsetsWithCallback(partitions, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(
        string topic,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, timeout, cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken cancellationToken = default) =>
        _native.ListTopicsWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        _native.ListTopicsWithCallback(timeout, cancellationToken);

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(cancellationToken).AsTask();

    /// <inheritdoc/>
    public Task Close(CloseOptions option, CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(option, cancellationToken).AsTask();

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
    public void EnforceRebalance() => _native.EnforceRebalance(null);

    /// <inheritdoc/>
    public void EnforceRebalance(string? reason) => _native.EnforceRebalance(reason);

    /// <inheritdoc/>
    public Task CommitAsync(CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task CommitAsync(IOffsetCommitCallback callback, CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(callback, cancellationToken);

    /// <inheritdoc/>
    public Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback,
        CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(offsets, callback, cancellationToken);

    /// <inheritdoc/>
    public Task CommitAsync(IAsyncOffsetCommitCallback callback, CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(callback, cancellationToken);

    /// <inheritdoc/>
    public Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IAsyncOffsetCommitCallback callback,
        CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(offsets, callback, cancellationToken);

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
    /// <see cref="PartitionsFor(string, CancellationToken)"/> and <see cref="ListTopics(CancellationToken)"/> (Java
    /// <c>updatePartitions</c>). Each partition's leader, sole replica and sole in-sync replica is the node
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
    /// <c>Rebalance</c>, or <c>LosePartitions</c> of its partition, discards it, or the consumer is closed. A
    /// cancelled <see cref="Poll"/> drops no record: it returns the records it delivered, or leaves them for the
    /// next <see cref="Poll"/>.
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
    /// Makes the next <see cref="Poll"/> fault with <paramref name="exception"/>, the same instance (Java
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
    /// are any, then <c>OnPartitionsAssigned</c> with the added ones, possibly none, on the consumer's drain thread.
    /// The <see cref="Task"/> completes after they have ended; an exception from either faults it with the same
    /// instance. Buffered records are cleared, as in Java. On a manually assigned consumer the <see cref="Task"/>
    /// faults with <see cref="ArgumentException"/>, as Java's <c>IllegalArgumentException</c>.
    /// </remarks>
    /// <param name="newAssignment">The new full assignment.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>A <see cref="Task"/> that completes when the rebalance is done.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="newAssignment"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// An element or an element topic is null.
    /// The topic of a partition in <paramref name="newAssignment"/> contains a NUL character, which the native client
    /// cannot receive unchanged.
    /// </exception>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    public Task Rebalance(
        IReadOnlyCollection<TopicPartition> newAssignment,
        CancellationToken cancellationToken = default) =>
        _native.RebalanceWithCallback(newAssignment, _slots, cancellationToken);

    /// <summary>
    /// Simulates losing <paramref name="partitionsLost"/> without a clean revocation (Java
    /// <c>MockConsumer.losePartitions(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// The buffered records of the lost partitions are cleared; then, while a listener is registered, its
    /// <c>OnPartitionsLost</c> extension method (<see cref="ConsumerRebalanceListenerExtensions"/> or
    /// <see cref="AsyncConsumerRebalanceListenerExtensions"/>) runs with them on the consumer's drain thread; then they
    /// leave the <see cref="Assignment"/>. The <see cref="Task"/> completes after the listener has ended; an exception from
    /// it faults the <see cref="Task"/> with the same instance. A partition not currently assigned, a null one included,
    /// faults it with <see cref="IllegalStateException"/>, as Java's.
    /// </remarks>
    /// <param name="partitionsLost">The partitions to lose; every one must be currently assigned.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>A <see cref="Task"/> that completes when the partitions are lost.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitionsLost"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// An element topic is null.
    /// The topic of a partition in <paramref name="partitionsLost"/> contains a NUL character, which the native client
    /// cannot receive unchanged.
    /// </exception>
    /// <exception cref="IllegalStateException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    public Task LosePartitions(
        IReadOnlyCollection<TopicPartition> partitionsLost,
        CancellationToken cancellationToken = default) =>
        _native.LosePartitionsWithCallback(partitionsLost, _slots, cancellationToken);

    /// <inheritdoc/>
    public void Dispose()
    {
        _native.Dispose();
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
