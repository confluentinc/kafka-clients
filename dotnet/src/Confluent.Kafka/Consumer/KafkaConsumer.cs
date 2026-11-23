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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

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
    private readonly IDeserializerWithHeaders<TKey>? _keyWithHeaders;
    private readonly IDeserializerWithHeaders<TValue>? _valueWithHeaders;
    private readonly IDeserializerWithReadOnlyMemory<TKey>? _keyWithMemory;
    private readonly IDeserializerWithReadOnlyMemory<TValue>? _valueWithMemory;

    private KafkaConsumer(
        NativeConsumer native, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
    {
        _native = native;
        _keyDeserializer = keyDeserializer;
        _valueDeserializer = valueDeserializer;
        _keyWithHeaders = keyDeserializer as IDeserializerWithHeaders<TKey>;
        _valueWithHeaders = valueDeserializer as IDeserializerWithHeaders<TValue>;
        _keyWithMemory = keyDeserializer as IDeserializerWithReadOnlyMemory<TKey>;
        _valueWithMemory = valueDeserializer as IDeserializerWithReadOnlyMemory<TValue>;
    }

    /// <summary>
    /// Creates a real KIP-848 consumer from a configuration map of Java's dotted keys that also holds the key and
    /// value deserializers (Java <c>KafkaConsumer(Map)</c>): the same as
    /// <see cref="Create(IReadOnlyDictionary{string, object}, IDeserializer{TKey}, IDeserializer{TValue})"/> with two
    /// null deserializers.
    /// </summary>
    /// <param name="configs">
    /// The consumer configuration, in which <c>key.deserializer</c> and <c>value.deserializer</c> are required: each
    /// holds a function that returns the deserializer, such as <c>() =&gt; new StringDeserializer()</c>.
    /// </param>
    /// <returns>The consumer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <c>key.deserializer</c> or <c>value.deserializer</c> is missing, <c>null</c> or not a function that returns
    /// the deserializer; or the configuration is refused as the other overload refuses it.
    /// </exception>
    /// <exception cref="KafkaException">
    /// "Failed to construct kafka consumer": a deserializer could not be built, or the core could not build the
    /// consumer.
    /// </exception>
    /// <exception cref="UnsupportedOperationException">
    /// <c>group.protocol</c> is <c>classic</c>, which this client does not support; <c>classic</c> is
    /// also the default when the key is unset.
    /// </exception>
    public static KafkaConsumer<TKey, TValue> Create(IReadOnlyDictionary<string, object?> configs) =>
        Create(configs, null, null);

    /// <summary>
    /// Creates a real KIP-848 consumer from a configuration map and the two deserializers
    /// (Java <c>KafkaConsumer(Map, Deserializer&lt;K&gt;, Deserializer&lt;V&gt;)</c>). Keys
    /// are the Java dotted names (e.g. <c>bootstrap.servers</c>, <c>group.id</c>,
    /// <c>group.protocol</c>).
    /// </summary>
    /// <param name="configs">
    /// The consumer configuration, keyed by Java's dotted names: each value of the type Java's <c>ConfigDef</c>
    /// accepts for its key, or the string Java would parse. A <c>null</c> value leaves its key unset, unless the
    /// key's validator refuses <c>null</c>. <c>key.deserializer</c> and <c>value.deserializer</c> are read only for a
    /// null deserializer argument: each then holds a function that returns the deserializer, a
    /// <c>Func&lt;IDeserializer&lt;TKey&gt;&gt;</c> or <c>Func&lt;IDeserializer&lt;TValue&gt;&gt;</c> (a function
    /// declared to return a class that implements the interface is one), where Java's holds a class. The other
    /// deserializer keys (<c>deserializer.encoding</c>, <c>key.deserializer.encoding</c>,
    /// <c>value.deserializer.encoding</c>) are for the deserializers the consumer configures. None of the five
    /// reaches the client.
    /// </param>
    /// <param name="keyDeserializer">
    /// The deserializer for record keys, which the consumer never configures, as Java never configures a deserializer
    /// it is given; or <c>null</c> to have the consumer call the function under <c>key.deserializer</c> once and
    /// then call <c>Configure(configs, true)</c> on a deserializer that implements
    /// <see cref="IDeserializerWithConfigure{T}"/>, as Java configures a deserializer it builds.
    /// </param>
    /// <param name="valueDeserializer">
    /// The deserializer for record values; or <c>null</c> for the one the function under <c>value.deserializer</c>
    /// returns, built after the key's and configured with <c>Configure(configs, false)</c>.
    /// </param>
    /// <returns>The consumer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <paramref name="keyDeserializer"/> or <paramref name="valueDeserializer"/> is null and
    /// <c>key.deserializer</c> or <c>value.deserializer</c> is missing or <c>null</c> (Java's "Invalid value null
    /// for configuration key.deserializer: must be non-null.", the key's before the value's), or holds anything but
    /// a function that returns the deserializer.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// A given deserializer declares a public <c>Deserialize(string, Headers, ReadOnlySpan&lt;byte&gt;)</c> but does
    /// not implement <see cref="IDeserializerWithHeaders{T}"/>, a public
    /// <c>Deserialize(string, Headers, ReadOnlyMemory&lt;byte&gt;)</c> but not
    /// <see cref="IDeserializerWithReadOnlyMemory{T}"/>, or a public <c>Close()</c> but is not
    /// <see cref="IDisposable"/>: the consumer would never call that method.
    /// </exception>
    /// <exception cref="ConfigException">
    /// A value is not of a type its key accepts, a <c>null</c> value is one its key's validator refuses, a key is
    /// <c>null</c>, or the client refuses a value. No deserializer function has run then.
    /// </exception>
    /// <exception cref="KafkaException">
    /// "Failed to construct kafka consumer": the core could not build the consumer; or a deserializer function
    /// returned <c>null</c> or threw, or the deserializer it returned was refused as a given one is or its
    /// <c>Configure</c> threw, with that failure as the <see cref="Exception.InnerException"/>.
    /// </exception>
    /// <exception cref="UnsupportedOperationException">
    /// <c>group.protocol</c> is <c>classic</c>, which this client does not support; <c>classic</c> is
    /// also the default when the key is unset. A deserializer built from the configuration is disposed then, and a
    /// given one is not.
    /// </exception>
    /// <remarks>
    /// Each deserializer that is <see cref="IDisposable"/>, given or built, is disposed once, key then value, when
    /// the consumer is closed or disposed, or when this method fails with the <see cref="KafkaException"/> "Failed
    /// to construct kafka consumer" after taking it, but a given one not for a configuration the core refuses with
    /// another error, nor when this method throws <see cref="ArgumentNullException"/> or refuses a deserializer, a
    /// value's type or a <c>null</c> with <see cref="ConfigException"/> or a deserializer with
    /// <see cref="IllegalArgumentException"/>, nor a given key deserializer when the value deserializer could not
    /// be built from <paramref name="configs"/>, as Java's consumer holds neither yet, nor by a close refused with
    /// <see cref="ConcurrentModificationException"/>, which leaves the consumer open. So a deserializer shared with
    /// another client, or used after the close, sees it disposed. A <c>Dispose</c> that throws is written to
    /// <see cref="System.Diagnostics.Trace"/> and not rethrown. A deserializer that is only
    /// <see cref="IAsyncDisposable"/> is not disposed. When a <c>Poll</c> is still deserializing, <c>Close</c> or
    /// <c>Dispose</c> returns first, and that <c>Poll</c> disposes the deserializers after it ends. This method
    /// copies <paramref name="configs"/> once, before it reads it, and uses only that copy, so a later change to
    /// <paramref name="configs"/>, a function's included, changes nothing. The <c>configs</c> a built
    /// deserializer's <c>Configure</c> receives is its own copy of that one; it does not have the <c>client.id</c>
    /// the consumer chooses when none is set, which Java's has.
    /// </remarks>
    public static KafkaConsumer<TKey, TValue> Create(
        IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TValue>? valueDeserializer)
    {
        ConfiguredSerdes<IDeserializer<TKey>, IDeserializer<TValue>> deserializers =
            ConfiguredSerdes.Deserializers(configs, keyDeserializer, valueDeserializer);
        // The copy of configs the call took: the checks, the core and each Configure read the same entries.
        NativeConsumer native = NativeConsumer.Create(deserializers.Configs, withPump: false, serdes: deserializers);
        return new KafkaConsumer<TKey, TValue>(native, deserializers.Key, deserializers.Value);
    }

    /// <inheritdoc/>
    public ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout) =>
        _native.PollTyped(
            timeout, _keyDeserializer, _keyWithHeaders, _keyWithMemory, _valueDeserializer, _valueWithHeaders,
            _valueWithMemory);

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
    public void Close() => _native.CloseSync();

    /// <inheritdoc/>
    public void Close(CloseOptions option) => _native.CloseSync(option);

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, long offset) =>
        _native.Seek(partition.Topic(), partition.Partition(), offset);

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) =>
        _native.SeekWithMetadata(partition.Topic(), partition.Partition(), offsetAndMetadata);

    /// <inheritdoc/>
    public long? CurrentLag(TopicPartition topicPartition) =>
        _native.CurrentLag(topicPartition.Topic(), topicPartition.Partition());

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

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
