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
/// The real (KIP-848) Kafka consumer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.KafkaConsumer</c>. A thin, Java-shaped forwarder
/// over the internal <see cref="NativeConsumer"/> lifecycle wrapper, which owns the
/// native handle, the completion bridge, and the receive-path copy-out (ffi-marshalling.md
/// §B). All Kafka logic lives in the Rust core; this type only restores the Java shape.
/// </summary>
/// <remarks>
/// <para>
/// <b>Single-owner / not thread-safe (inherited from <see cref="NativeConsumer"/>).</b>
/// At most one operation in flight; concurrency is serialized by the Rust core (a
/// concurrent async op faults its <see cref="Task"/>, a concurrent
/// <see cref="GroupMetadata"/> throws <see cref="InvalidOperationException"/>). Do not
/// share one instance across threads without external synchronization.
/// <see cref="Wakeup"/> is the one deliberately cross-thread member; the canonical
/// pattern (thread A blocked in <see cref="Poll"/>, thread B wakes it, thread A
/// then disposes) is safe. Racing <see cref="Wakeup"/> against disposal from a second
/// thread is <b>also</b> safe as of M9/P4 H1d — it used to be an accepted residual, but the
/// underlying handle race is now closed (the call holds a marshaller reference for its
/// duration, and <see cref="Wakeup"/> stays a no-op once closing/closed). Concurrent
/// <em>operations</em> are still the caller's responsibility, as above.
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="DisposeAsync"/> is the primary path (graceful async close
/// then destroy; swallows any close error); <see cref="Dispose"/> is the blocking
/// fallback. <see cref="Close"/> is the explicit graceful close that <em>surfaces</em>
/// a close failure. All are idempotent and gated by a single atomic closed flag.
/// <b>Deterministic native release requires that no operation is in flight — await your
/// operations before disposing.</b> Disposing while an unawaited operation is still
/// running defers the native release until that operation completes (bounded by its own
/// timeout); it is accepted and documented, not a leak (M9/P4 decision Q1).
/// </para>
/// <para>
/// <b>Generic-only, 3-param ctor (PLAN M6/P1b, decisions A/B).</b> Mirrors Java's
/// <c>KafkaConsumer(Map, Deserializer&lt;K&gt;, Deserializer&lt;V&gt;)</c>. The key/value
/// deserializers decode each polled record's raw bytes (the typed zero-copy poll path,
/// ffi §B4) — here on the core's foreign dispatcher thread inside the poll callback, so a
/// throwing deserializer faults the <see cref="Task"/> with a
/// <see cref="SerializationException"/> and never unwinds into native (PLAN §5/§6). Bytes
/// users pass <see cref="Serdes.ByteArray"/> for both.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type produced by the key deserializer.</typeparam>
/// <typeparam name="TValue">The value type produced by the value deserializer.</typeparam>
public sealed class AsyncKafkaConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue>
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
    public AsyncKafkaConsumer(
        IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
        _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
        _native = NativeConsumer.Create(config);
    }

    /// <inheritdoc/>
    public Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.PollWithCallback(timeout, _keyDeserializer, _valueDeserializer, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(topics, cancellationToken);

    /// <inheritdoc/>
    public Task Subscribe(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default) =>
        _native.SubscribeWithCallback(topics, listener, cancellationToken);

    /// <inheritdoc/>
    public Task Unsubscribe(CancellationToken cancellationToken = default) =>
        _native.UnsubscribeWithCallback(cancellationToken);

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
    public Task Commit(CancellationToken cancellationToken = default) =>
        _native.CommitWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, CancellationToken cancellationToken = default) =>
        _native.CommitWithCallback(offsets, cancellationToken);

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

    /// <inheritdoc/>
    public void CommitAsync() => _native.CommitAsync();

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <inheritdoc/>
    public string ClientId() => _native.ClientId();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
