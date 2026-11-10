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
using System.Globalization;

using Confluent.Kafka.Common;

namespace Confluent.Kafka.Producer;

/// <summary>
/// The metadata for a record that has been acknowledged by the server, Java's
/// <c>org.apache.kafka.clients.producer.RecordMetadata</c>. On a failed delivery an <see cref="ICallback"/>
/// receives Java's placeholder instead.
/// </summary>
public sealed class RecordMetadata
{
    /// <summary>Partition value for record without partition assigned.</summary>
    public const int UNKNOWN_PARTITION = -1;

    private const long InvalidOffset = -1L;
    private const long NoTimestamp = -1L;

    private readonly TopicPartition? _topicPartition;
    private readonly long _offset;
    private readonly long _timestamp;
    private readonly int _serializedKeySize;
    private readonly int _serializedValueSize;

    /// <summary>Creates a new instance with the provided parameters.</summary>
    /// <param name="topicPartition">
    /// The topic and partition the record was sent to, stored as given. When it is <see langword="null"/>,
    /// <see cref="Topic()"/>, <see cref="Partition()"/> and <see cref="ToString"/> throw.
    /// </param>
    /// <param name="baseOffset">The offset of the record's batch, or -1 if it is unknown.</param>
    /// <param name="batchIndex">The record's index in its batch; ignored when <paramref name="baseOffset"/> is -1.</param>
    /// <param name="timestamp">The record's timestamp, or -1 if it has none.</param>
    /// <param name="serializedKeySize">The size of the serialized key in bytes, or -1 if the key is null.</param>
    /// <param name="serializedValueSize">The size of the serialized value in bytes, or -1 if the value is null.</param>
    public RecordMetadata(
        TopicPartition? topicPartition,
        long baseOffset,
        int batchIndex,
        long timestamp,
        int serializedKeySize,
        int serializedValueSize)
    {
        _topicPartition = topicPartition;
        _offset = baseOffset == InvalidOffset ? baseOffset : baseOffset + batchIndex;
        _timestamp = timestamp;
        _serializedKeySize = serializedKeySize;
        _serializedValueSize = serializedValueSize;
    }

    internal RecordMetadata(
        TopicPartition topicPartition,
        long offset,
        long timestamp,
        int serializedKeySize,
        int serializedValueSize)
    {
        _topicPartition = topicPartition;
        _offset = offset;
        _timestamp = timestamp;
        _serializedKeySize = serializedKeySize;
        _serializedValueSize = serializedValueSize;
    }

    /// <summary>Indicates whether the record metadata includes the offset.</summary>
    /// <returns>Whether the record metadata includes the offset.</returns>
    public bool HasOffset() => Offset() != InvalidOffset;

    /// <summary>The offset of the record in the topic/partition, or -1 if <see cref="HasOffset()"/> is false.</summary>
    /// <returns>The offset of the record in the topic/partition, or -1 if <see cref="HasOffset()"/> is
    /// false.</returns>
    public long Offset() => _offset;

    /// <summary>Indicates whether the record metadata includes the timestamp.</summary>
    /// <returns>Whether the record metadata includes the timestamp.</returns>
    public bool HasTimestamp() => Timestamp() != NoTimestamp;

    /// <summary>
    /// The timestamp of the record in the topic/partition, or -1 if <see cref="HasTimestamp()"/> is false.
    /// </summary>
    /// <returns>The timestamp of the record in the topic/partition, or -1 if <see cref="HasTimestamp()"/> is
    /// false.</returns>
    public long Timestamp() => _timestamp;

    /// <summary>The size of the serialized, uncompressed key in bytes. If key is null, the size is -1.</summary>
    /// <returns>The size of the serialized, uncompressed key in bytes.</returns>
    public int SerializedKeySize() => _serializedKeySize;

    /// <summary>The size of the serialized, uncompressed value in bytes. If value is null, the size is -1.</summary>
    /// <returns>The size of the serialized, uncompressed value in bytes.</returns>
    public int SerializedValueSize() => _serializedValueSize;

    /// <summary>The topic the record was appended to.</summary>
    /// <returns>The topic the record was appended to.</returns>
    /// <exception cref="InvalidOperationException">The instance was constructed with a null topic partition.</exception>
    public string Topic() => StoredTopicPartition().Topic();

    /// <summary>The partition the record was sent to.</summary>
    /// <returns>The partition the record was sent to.</returns>
    /// <exception cref="InvalidOperationException">The instance was constructed with a null topic partition.</exception>
    public int Partition() => StoredTopicPartition().Partition();

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">The instance was constructed with a null topic partition.</exception>
    public override string ToString() =>
        StoredTopicPartition().ToString() + "@" + Offset().ToString(CultureInfo.InvariantCulture);

    private TopicPartition StoredTopicPartition() =>
        _topicPartition ?? throw new InvalidOperationException("topicPartition must not be null");
}
