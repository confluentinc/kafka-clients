// Copyright 2026 Confluent Inc.
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

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.

using System;
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// No reset policy has been defined, and the offsets for these partitions are either larger or smaller than the range
/// of offsets the server has for the given partition. Java's
/// <c>org.apache.kafka.clients.consumer.OffsetOutOfRangeException</c>.
/// </summary>
public class OffsetOutOfRangeException : global::Confluent.Kafka.Consumer.InvalidOffsetException
{
    private readonly IReadOnlyDictionary<TopicPartition, long> _offsetOutOfRangePartitions;

    /// <summary>
    /// Initializes the exception, as Java's <c>OffsetOutOfRangeException(Map&lt;TopicPartition, Long&gt;
    /// offsetOutOfRangePartitions)</c>.
    /// </summary>
    public OffsetOutOfRangeException(IReadOnlyDictionary<TopicPartition, long> offsetOutOfRangePartitions)
        : this("Offsets out of range with no configured reset policy for partitions: " + JavaText.ValueOf(offsetOutOfRangePartitions), offsetOutOfRangePartitions)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>OffsetOutOfRangeException(String message, Map&lt;TopicPartition,
    /// Long&gt; offsetOutOfRangePartitions)</c>.
    /// </summary>
    public OffsetOutOfRangeException(string? message, IReadOnlyDictionary<TopicPartition, long> offsetOutOfRangePartitions)
        : base(message)
    {
        _offsetOutOfRangePartitions = offsetOutOfRangePartitions;
    }

    internal OffsetOutOfRangeException(CoreError _, string? message, Exception? innerException, IReadOnlyDictionary<TopicPartition, long> offsetOutOfRangePartitions)
        : base(_, message, innerException)
    {
        _offsetOutOfRangePartitions = offsetOutOfRangePartitions;
    }

    /// <summary>Java's <c>offsetOutOfRangePartitions()</c>.</summary>
    /// <returns>The offset out of range partitions.</returns>
    public IReadOnlyDictionary<TopicPartition, long> OffsetOutOfRangePartitions() => _offsetOutOfRangePartitions;

    /// <inheritdoc/>
    public override IReadOnlyCollection<TopicPartition> Partitions() => new KeySetView<TopicPartition, long>(_offsetOutOfRangePartitions);

    internal override int FfiId => -22;
}
