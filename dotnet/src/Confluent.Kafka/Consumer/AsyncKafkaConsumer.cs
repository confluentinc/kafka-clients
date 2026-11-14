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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The Kafka consumer with an asynchronous surface, Java's <c>org.apache.kafka.clients.consumer.KafkaConsumer</c>
/// under the KIP-848 group protocol. <see cref="IAsyncConsumer{TKey, TValue}"/> describes its threads,
/// cancellation and disposal; <see cref="KafkaConsumer{TKey, TValue}"/> is its synchronous sibling.
/// </summary>
/// <typeparam name="TKey">The key type produced by the key deserializer.</typeparam>
/// <typeparam name="TValue">The value type produced by the value deserializer.</typeparam>
public sealed class AsyncKafkaConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue>
{
    private readonly NativeConsumer _native;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;
    private readonly IDeserializerWithHeaders<TKey>? _keyWithHeaders;
    private readonly IDeserializerWithHeaders<TValue>? _valueWithHeaders;

    /// <summary>
    /// Creates a real KIP-848 consumer from a configuration map and the two deserializers
    /// (Java <c>KafkaConsumer(Map, Deserializer&lt;K&gt;, Deserializer&lt;V&gt;)</c>). Keys
    /// are the Java dotted names (e.g. <c>bootstrap.servers</c>, <c>group.id</c>,
    /// <c>group.protocol</c>).
    /// </summary>
    /// <param name="configs">
    /// The consumer configuration, keyed by Java's dotted names: each value of the type Java's <c>ConfigDef</c>
    /// accepts for its key, or the string Java would parse. A <c>null</c> value leaves its key unset, unless the
    /// key's validator refuses <c>null</c>. The deserializer keys (<c>key.deserializer</c>,
    /// <c>value.deserializer</c>, <c>deserializer.encoding</c>, <c>key.deserializer.encoding</c>,
    /// <c>value.deserializer.encoding</c>) are ignored: the deserializers are the arguments, which the consumer never
    /// configures, as Java never configures a deserializer it is given.
    /// </param>
    /// <param name="keyDeserializer">Deserializer for record keys.</param>
    /// <param name="valueDeserializer">Deserializer for record values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <paramref name="keyDeserializer"/> or <paramref name="valueDeserializer"/> is null: Java then reads the
    /// <c>key.deserializer</c> / <c>value.deserializer</c> config, which .NET config cannot set.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// A deserializer declares a public <c>Deserialize(string, Headers, ReadOnlySpan&lt;byte&gt;)</c> but does not
    /// implement <see cref="IDeserializerWithHeaders{T}"/>, or a public <c>Close()</c> but is not
    /// <see cref="IDisposable"/>: the consumer would never call that method.
    /// </exception>
    /// <exception cref="ConfigException">
    /// A value is not of a type its key accepts, a <c>null</c> value is one its key's validator refuses, a key is
    /// <c>null</c>, or the client refuses a value.
    /// </exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    /// <exception cref="UnsupportedOperationException">
    /// <c>group.protocol</c> is <c>classic</c>, which this client does not support; <c>classic</c> is
    /// also the default when the key is unset.
    /// </exception>
    /// <remarks>
    /// Each deserializer that is <see cref="IDisposable"/> is disposed once, key then value, when the consumer is
    /// closed or disposed, or when the core fails to build it with the <see cref="KafkaException"/> "Failed to
    /// construct kafka consumer", but not for a configuration the core refuses with another error, nor when this
    /// constructor throws
    /// <see cref="ArgumentNullException"/> or refuses a deserializer, a value's type or a <c>null</c> with
    /// <see cref="ConfigException"/> or a deserializer with <see cref="IllegalArgumentException"/>, nor by a close
    /// refused with <see cref="ConcurrentModificationException"/>, which
    /// leaves the consumer open. So a deserializer shared with another client, or used after the close, sees it
    /// disposed. A <c>Dispose</c> that throws is written to <see cref="System.Diagnostics.Trace"/> and not rethrown.
    /// A deserializer that is only <see cref="IAsyncDisposable"/> is not disposed. When a <c>Poll</c> is still
    /// deserializing, <c>Close</c> or <c>Dispose</c> returns first, and that <c>Poll</c> disposes the deserializers
    /// after it ends.
    /// </remarks>
    public AsyncKafkaConsumer(
        IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        // Java copies the config map first, so a null one fails before a missing deserializer.
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        _keyDeserializer = NativeConsumer.RequireDeserializer(keyDeserializer, "key.deserializer");
        _valueDeserializer = NativeConsumer.RequireDeserializer(valueDeserializer, "value.deserializer");
        SerdeGuard.Check(_keyDeserializer, DefaultMethodGuard.Consumer, disposes: true);
        SerdeGuard.Check(_valueDeserializer, DefaultMethodGuard.Consumer, disposes: true);
        _keyWithHeaders = _keyDeserializer as IDeserializerWithHeaders<TKey>;
        _valueWithHeaders = _valueDeserializer as IDeserializerWithHeaders<TValue>;
        _native = NativeConsumer.Create(configs, serdes: new SerdeDisposal(keyDeserializer, valueDeserializer));
    }

    /// <inheritdoc/>
    public Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PollWithCallback(
            timeout, _keyDeserializer, _keyWithHeaders, _valueDeserializer, _valueWithHeaders, cancellationToken);

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

    internal ConsumerHandle Handle() => _native.CreateReentrancyHandle();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
