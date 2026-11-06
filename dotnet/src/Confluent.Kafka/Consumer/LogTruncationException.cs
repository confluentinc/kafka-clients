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
/// In the event of an unclean leader election, the log will be truncated, previously committed data will be lost, and
/// new data will be written over these offsets. When this happens, the consumer will detect the truncation and raise
/// this exception (if no automatic reset policy has been defined) with the first offset known to diverge from what the
/// consumer previously read. Java's <c>org.apache.kafka.clients.consumer.LogTruncationException</c>.
/// </summary>
public class LogTruncationException : global::Confluent.Kafka.Consumer.OffsetOutOfRangeException
{
    private readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> _divergentOffsets;

    /// <summary>
    /// Initializes the exception, as Java's <c>LogTruncationException(Map&lt;TopicPartition, Long&gt; fetchOffsets,
    /// Map&lt;TopicPartition, OffsetAndMetadata&gt; divergentOffsets)</c>.
    /// </summary>
    public LogTruncationException(IReadOnlyDictionary<TopicPartition, long> fetchOffsets, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> divergentOffsets)
        : this("Truncated partitions detected with divergent offsets " + JavaText.ValueOf(divergentOffsets), fetchOffsets, divergentOffsets)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>LogTruncationException(String message, Map&lt;TopicPartition, Long&gt;
    /// fetchOffsets, Map&lt;TopicPartition, OffsetAndMetadata&gt; divergentOffsets)</c>.
    /// </summary>
    public LogTruncationException(string? message, IReadOnlyDictionary<TopicPartition, long> fetchOffsets, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> divergentOffsets)
        : base(message, fetchOffsets)
    {
        _divergentOffsets = divergentOffsets ?? throw new ArgumentNullException(nameof(divergentOffsets));
    }

    internal LogTruncationException(CoreError _, string? message, Exception? innerException, IReadOnlyDictionary<TopicPartition, long> offsetOutOfRangePartitions, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> divergentOffsets)
        : base(_, message, innerException, offsetOutOfRangePartitions)
    {
        _divergentOffsets = divergentOffsets;
    }

    /// <summary>Java's <c>divergentOffsets()</c>.</summary>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> DivergentOffsets() => _divergentOffsets;

    internal override int FfiId => -20;
}
