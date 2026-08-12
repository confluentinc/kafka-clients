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
/// The real (KIP-848) <b>synchronous</b> Kafka consumer — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.KafkaConsumer</c>. A thin, Java-shaped forwarder over
/// the internal <see cref="NativeConsumer"/> lifecycle wrapper (the same wrapper the async
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> composes — the two client families are siblings over one
/// native consumer, not one wrapping the other). All Kafka logic lives in the Rust core; this
/// type only restores the Java (blocking) shape.
/// </summary>
/// <remarks>
/// <para>
/// <b>Blocking, direct sync C ABI (inherited from <see cref="NativeConsumer"/>).</b> Every
/// operation calls the synchronous C ABI directly; the core's blocking call parks the caller
/// thread inside the Rust multi-thread runtime (deadlock-free) — not sync-over-async, and never
/// routed through the async binding API.
/// </para>
/// <para>
/// <b>Single-owner / not thread-safe.</b> At most one operation in flight; concurrency is
/// serialized by the Rust core. <see cref="Wakeup"/> is the one deliberately cross-thread
/// member; the canonical pattern (thread A blocked in <see cref="Poll"/>, thread B wakes it) is
/// safe. Do not share one instance across threads without external synchronization.
/// </para>
/// <para>
/// <b>Disposal.</b> <see cref="Close()"/> / <see cref="Close(TimeSpan)"/> are the explicit
/// graceful closes that <em>surface</em> a close failure; <see cref="Dispose"/> is the
/// blocking teardown that swallows it. All are idempotent and gated by a single atomic closed
/// flag. There is no <c>DisposeAsync</c> — this is the synchronous surface.
/// </para>
/// <para>
/// <b>Generic-only, 3-param ctor (PLAN M6/P1b, decisions A/B).</b> Mirrors Java's
/// <c>KafkaConsumer(Map, Deserializer&lt;K&gt;, Deserializer&lt;V&gt;)</c>. The key/value
/// deserializers decode each polled record's raw bytes (the typed zero-copy poll path,
/// ffi §B4). Bytes users pass <see cref="Serdes.ByteArray"/> for both.
/// </para>
/// </remarks>
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
    public KafkaConsumer(
        IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        _keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
        _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
        _native = NativeConsumer.Create(config);
    }

    /// <inheritdoc/>
    public ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout) =>
        _native.PollTyped(timeout, _keyDeserializer, _valueDeserializer);

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

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
