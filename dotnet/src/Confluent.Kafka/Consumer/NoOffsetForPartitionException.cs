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
/// Indicates that there is no stored offset for a partition and no defined offset reset policy. Java's
/// <c>org.apache.kafka.clients.consumer.NoOffsetForPartitionException</c>.
/// </summary>
public class NoOffsetForPartitionException : global::Confluent.Kafka.Consumer.InvalidOffsetException
{
    private readonly IReadOnlyCollection<TopicPartition> _partitions;

    /// <summary>
    /// Initializes the exception, as Java's <c>NoOffsetForPartitionException(TopicPartition partition)</c>.
    /// </summary>
    public NoOffsetForPartitionException(TopicPartition partition)
        : base("Undefined offset with no reset policy for partition: " + JavaText.ValueOf(partition))
    {
        _partitions = new[] { partition };
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>NoOffsetForPartitionException(Collection&lt;TopicPartition&gt;
    /// partitions)</c>.
    /// </summary>
    public NoOffsetForPartitionException(IReadOnlyCollection<TopicPartition> partitions)
        : base("Undefined offset with no reset policy for partitions: " + JavaText.ValueOf(partitions))
    {
        _partitions = new HashSet<TopicPartition>(partitions ?? throw new ArgumentNullException(nameof(partitions)));
    }

    internal NoOffsetForPartitionException(CoreError _, string? message, Exception? innerException, IReadOnlyCollection<TopicPartition> partitions)
        : base(_, message, innerException)
    {
        _partitions = partitions;
    }

    /// <inheritdoc/>
    public override IReadOnlyCollection<TopicPartition> Partitions() => _partitions;

    internal override int FfiId => -21;
}
