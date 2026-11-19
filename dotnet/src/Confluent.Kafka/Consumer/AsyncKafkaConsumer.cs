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
    private readonly AsyncDeserializerCall<TKey> _key;
    private readonly AsyncDeserializerCall<TValue> _value;

    /// <summary>
    /// Creates a real KIP-848 consumer from a configuration map and the two asynchronous deserializers
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
    /// <param name="keyDeserializer">
    /// Deserializer for record keys: a built-in <c>Async…Deserializer</c> for the built-in types, which <c>Poll</c>
    /// calls without awaiting, or a class of the caller's, which <c>Poll</c> awaits for each non-null key.
    /// </param>
    /// <param name="valueDeserializer">Deserializer for record values, as for keys.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <paramref name="keyDeserializer"/> or <paramref name="valueDeserializer"/> is null: Java then reads the
    /// <c>key.deserializer</c> / <c>value.deserializer</c> config, which .NET config cannot set.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// A deserializer is <see cref="IDisposable"/>, or declares a public <c>DisposeAsync()</c>, <c>Dispose()</c> or
    /// <c>Close()</c>, but is not <see cref="IAsyncDisposable"/>: the consumer would never dispose it.
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
    /// Each deserializer that is <see cref="IAsyncDisposable"/> is disposed once, key then value, through its
    /// <c>DisposeAsync()</c> and never through <see cref="IDisposable"/>, when the consumer is
    /// closed or disposed, or when the core fails to build it with the <see cref="KafkaException"/> "Failed to
    /// construct kafka consumer", but not for a configuration the core refuses with another error, nor when this
    /// constructor throws
    /// <see cref="ArgumentNullException"/> or refuses a deserializer, a value's type or a <c>null</c> with
    /// <see cref="ConfigException"/> or a deserializer with <see cref="IllegalArgumentException"/>, nor by a close
    /// refused with <see cref="ConcurrentModificationException"/>, which
    /// leaves the consumer open. So a deserializer shared with another client, or used after the close, sees it
    /// disposed. A disposal that throws is written to <see cref="System.Diagnostics.Trace"/> and not rethrown.
    /// <c>Close</c> and <c>DisposeAsync</c> await a <c>DisposeAsync()</c>; the synchronous <c>Dispose()</c>, and this
    /// constructor when the core fails to build the consumer, call it and wait for it on their own thread, so
    /// <c>await using</c> or <c>DisposeAsync()</c> is the form to prefer. A deserializer's <c>DisposeAsync()</c> is
    /// always called with no synchronization context and with the default task scheduler current, so an
    /// <c>await</c> in it never needs the thread that waits. When a <c>Poll</c> is still
    /// deserializing, <c>Close</c> or <c>Dispose</c> returns first, and that <c>Poll</c> disposes the deserializers
    /// after it ends.
    /// </remarks>
    public AsyncKafkaConsumer(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncDeserializer<TKey> keyDeserializer,
        IAsyncDeserializer<TValue> valueDeserializer)
    {
        // Java copies the config map first, so a null one fails before a missing deserializer.
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        IAsyncDeserializer<TKey> key = NativeConsumer.RequireDeserializer(keyDeserializer, "key.deserializer");
        IAsyncDeserializer<TValue> value = NativeConsumer.RequireDeserializer(valueDeserializer, "value.deserializer");
        SerdeGuard.Check(key, DefaultMethodGuard.Consumer, disposes: true);
        SerdeGuard.Check(value, DefaultMethodGuard.Consumer, disposes: true);
        _key = new AsyncDeserializerCall<TKey>(key);
        _value = new AsyncDeserializerCall<TValue>(value);
        _native = NativeConsumer.Create(configs, serdes: ConfiguredSerdes.AsyncDeserializers(configs, key, value));
    }

    private AsyncKafkaConsumer(
        NativeConsumer native, IAsyncDeserializer<TKey> keyDeserializer, IAsyncDeserializer<TValue> valueDeserializer)
    {
        _native = native;
        _key = new AsyncDeserializerCall<TKey>(keyDeserializer);
        _value = new AsyncDeserializerCall<TValue>(valueDeserializer);
    }

    /// <summary>
    /// Creates a consumer from a configuration map of Java's dotted keys that also holds the key and value
    /// deserializers (Java <c>KafkaConsumer(Map)</c>): the same as
    /// <see cref="Create(IReadOnlyDictionary{string, object}, IAsyncDeserializer{TKey}, IAsyncDeserializer{TValue}, CancellationToken)"/>
    /// with two null deserializers.
    /// </summary>
    /// <param name="configs">
    /// The consumer configuration, in which <c>key.deserializer</c> and <c>value.deserializer</c> are required: each
    /// holds a function that returns the deserializer, such as <c>() =&gt; new AsyncStringDeserializer()</c>.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the creation while the deserializers are built and configured, as the other overload says.
    /// </param>
    /// <returns>A task that completes with the consumer once its deserializers are configured.</returns>
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
    /// <c>group.protocol</c> is <c>classic</c>, which this client does not support; <c>classic</c> is also the
    /// default when the key is unset.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static Task<AsyncKafkaConsumer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken = default) =>
        Create(configs, null, null, cancellationToken);

    /// <summary>
    /// Creates a consumer from a configuration map of Java's dotted keys and the key and value deserializers
    /// (Java <c>KafkaConsumer(Map, Deserializer&lt;K&gt;, Deserializer&lt;V&gt;)</c>), awaiting the <c>Configure</c>
    /// of a deserializer it builds. The consumer is not subscribed to any topic until <c>Subscribe</c> or
    /// <c>Assign</c> is called.
    /// </summary>
    /// <param name="configs">
    /// The consumer configuration, keyed by Java's dotted names: each value of the type Java's <c>ConfigDef</c>
    /// accepts for its key, or the string Java would parse. A <c>null</c> value leaves its key unset, unless the
    /// key's validator refuses <c>null</c>. <c>key.deserializer</c> and <c>value.deserializer</c> are read only for
    /// a null deserializer argument: each then holds a function that returns the deserializer, a
    /// <c>Func&lt;IAsyncDeserializer&lt;TKey&gt;&gt;</c> or <c>Func&lt;IAsyncDeserializer&lt;TValue&gt;&gt;</c> (a
    /// function declared to return a class that implements the interface is one), where Java's holds a class. The
    /// other deserializer keys (<c>deserializer.encoding</c>, <c>key.deserializer.encoding</c>,
    /// <c>value.deserializer.encoding</c>) are for the deserializers the consumer configures. None of the five
    /// reaches the client.
    /// </param>
    /// <param name="keyDeserializer">
    /// Deserializer for record keys: a built-in <c>Async…Deserializer</c> for the built-in types, which <c>Poll</c>
    /// calls without awaiting, or a class of the caller's, which <c>Poll</c> awaits for each non-null key. The
    /// consumer never configures it, as Java never configures a deserializer it is given. Or <c>null</c> to have
    /// the consumer call the function under <c>key.deserializer</c> once and then await
    /// <c>Configure(configs, true, cancellationToken)</c> on a deserializer that implements
    /// <see cref="IAsyncDeserializerWithConfigure{T}"/>, as Java configures a deserializer it builds.
    /// </param>
    /// <param name="valueDeserializer">
    /// Deserializer for record values, as for keys; or <c>null</c> for the one the function under
    /// <c>value.deserializer</c> returns, built once the key's is configured and configured with
    /// <c>Configure(configs, false, cancellationToken)</c>.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the creation until the core builds the consumer: it is read before each deserializer is built,
    /// passed to each <c>Configure</c>, and read once more when both are configured. A cancelled creation disposes
    /// the deserializers it built and leaves the given ones to the caller.
    /// </param>
    /// <returns>
    /// A task that completes with the consumer once its deserializers are configured. The checks of the arguments
    /// and of the configuration throw from the call, before any function runs; the task carries what happens from
    /// the first function on.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <paramref name="keyDeserializer"/> or <paramref name="valueDeserializer"/> is null and
    /// <c>key.deserializer</c> or <c>value.deserializer</c> is missing or <c>null</c> (Java's "Invalid value null
    /// for configuration key.deserializer: must be non-null.", the key's before the value's), or holds anything but
    /// a function that returns the deserializer.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// A given deserializer is <see cref="IDisposable"/>, or declares a public <c>DisposeAsync()</c>,
    /// <c>Dispose()</c> or <c>Close()</c>, but is not <see cref="IAsyncDisposable"/>: the consumer would never
    /// dispose it.
    /// </exception>
    /// <exception cref="ConfigException">
    /// A value is not of a type its key accepts, a <c>null</c> value is one its key's validator refuses, a key is
    /// <c>null</c>, or the client refuses a value. No deserializer function has run then.
    /// </exception>
    /// <exception cref="KafkaException">
    /// "Failed to construct kafka consumer": the core could not build the consumer; or a deserializer function
    /// returned <c>null</c> or threw, or the deserializer it returned was refused as a given one is or its
    /// <c>Configure</c> failed, with that failure as the <see cref="Exception.InnerException"/>.
    /// </exception>
    /// <exception cref="UnsupportedOperationException">
    /// <c>group.protocol</c> is <c>classic</c>, which this client does not support; <c>classic</c> is also the
    /// default when the key is unset. A deserializer built from the configuration is disposed then, and a given one
    /// is not.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// Each deserializer that is <see cref="IAsyncDisposable"/>, given or built, is disposed once, key then value,
    /// through its <c>DisposeAsync()</c> and never through <see cref="IDisposable"/>, when the consumer is closed or
    /// disposed, or when this method fails with the <see cref="KafkaException"/> "Failed to construct kafka
    /// consumer" after taking it, but a given one not for a configuration the core refuses with another error, nor
    /// for a cancellation, nor when this method throws <see cref="ArgumentNullException"/> or refuses a
    /// deserializer, a value's type or a <c>null</c> with <see cref="ConfigException"/> or a deserializer with
    /// <see cref="IllegalArgumentException"/>, nor by a close refused with
    /// <see cref="ConcurrentModificationException"/>, which leaves the consumer open. So a deserializer shared with
    /// another client, or used after the close, sees it disposed. A disposal that throws is written to
    /// <see cref="System.Diagnostics.Trace"/> and not rethrown. This method, <c>Close</c> and <c>DisposeAsync</c>
    /// await a <c>DisposeAsync()</c>; the synchronous <c>Dispose()</c> calls it and waits for it on its own thread,
    /// so <c>await using</c> or <c>DisposeAsync()</c> is the form to prefer. A deserializer's <c>DisposeAsync()</c>
    /// is always called with no synchronization context and with the default task scheduler current, so an
    /// <c>await</c> in it never needs the thread that waits. When a <c>Poll</c> is still deserializing, <c>Close</c>
    /// or <c>Dispose</c> returns first, and that <c>Poll</c> disposes the deserializers after it ends. A function
    /// and the <c>Configure</c> of the deserializer it returns start on the flow that reaches them: the key's on the
    /// caller's, with its synchronization context, and the value's wherever the key's <c>Configure</c> completed.
    /// The <c>configs</c> a built deserializer's <c>Configure</c> receives is a copy of <paramref name="configs"/>;
    /// it does not have the <c>client.id</c> the consumer chooses when none is set, which Java's has.
    /// </remarks>
    public static Task<AsyncKafkaConsumer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncDeserializer<TKey>? keyDeserializer,
        IAsyncDeserializer<TValue>? valueDeserializer,
        CancellationToken cancellationToken = default)
    {
        ConfiguredSerdes<IAsyncDeserializer<TKey>, IAsyncDeserializer<TValue>> deserializers =
            ConfiguredSerdes.AsyncDeserializers(configs, keyDeserializer, valueDeserializer);
        cancellationToken.ThrowIfCancellationRequested();
        return Created(NativeConsumer.CreateAwaited(configs, deserializers, cancellationToken), deserializers);
    }

    private static async Task<AsyncKafkaConsumer<TKey, TValue>> Created(
        Task<NativeConsumer> native,
        ConfiguredSerdes<IAsyncDeserializer<TKey>, IAsyncDeserializer<TValue>> deserializers) =>
        new AsyncKafkaConsumer<TKey, TValue>(await native.ConfigureAwait(false), deserializers.Key, deserializers.Value);

    /// <inheritdoc/>
    public Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PollWithCallback(
            timeout, _key, _value, cancellationToken);

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
    public void EnforceRebalance(string? reason = null) => _native.EnforceRebalance(reason);

    /// <inheritdoc/>
    public Task CommitAsync(CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task CommitAsync(IOffsetCommitCallback callback, CancellationToken cancellationToken = default) =>
        _native.CommitAsyncWithCallback(callback, cancellationToken);

    /// <inheritdoc/>
    public Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null,
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

    /// <inheritdoc/>
    public ConsumerHandle Handle() => _native.CreateReentrancyHandle();

    /// <inheritdoc/>
    /// <remarks>
    /// Prefer <c>await using</c> or <see cref="DisposeAsync"/>: this call blocks its thread until the consumer is
    /// closed and its deserializers are disposed. A deserializer that is <see cref="IAsyncDisposable"/> is disposed
    /// through its <c>DisposeAsync()</c>, called with no synchronization context and with the default task scheduler
    /// current, which this call then waits for.
    /// </remarks>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
