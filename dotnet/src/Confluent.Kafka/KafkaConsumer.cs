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
/// The Kafka consumer with a synchronous surface, Java's <c>org.apache.kafka.clients.consumer.KafkaConsumer</c>
/// under the KIP-848 group protocol. <see cref="IConsumer{TKey, TValue}"/> describes its threads and disposal;
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> is its asynchronous sibling.
/// </summary>
/// <typeparam name="TKey">The key type produced by the key deserializer.</typeparam>
/// <typeparam name="TValue">The value type produced by the value deserializer.</typeparam>
public sealed class KafkaConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly NativeConsumer _native;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;

    /// <summary>
    /// Creates a real KIP-848 consumer from a configuration map and the two deserializers
    /// (Java <c>KafkaConsumer(Map, Deserializer&lt;K&gt;, Deserializer&lt;V&gt;)</c>). Keys
    /// are the Java dotted names (e.g. <c>bootstrap.servers</c>, <c>group.id</c>,
    /// <c>group.protocol</c>); values are strings.
    /// </summary>
    /// <param name="config">The consumer configuration.</param>
    /// <param name="keyDeserializer">Deserializer for record keys.</param>
    /// <param name="valueDeserializer">Deserializer for record values.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="config"/>, <paramref name="keyDeserializer"/>, or
    /// <paramref name="valueDeserializer"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    /// <exception cref="NotSupportedException">
    /// <c>group.protocol</c> is <c>classic</c>, which this client does not support; <c>classic</c> is
    /// also the default when the key is unset.
    /// </exception>
    public KafkaConsumer(
        IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
        _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
        _native = NativeConsumer.Create(config, withPump: false);
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

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
