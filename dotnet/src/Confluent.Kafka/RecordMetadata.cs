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

using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// The metadata of a record the cluster acknowledged, Java's <c>org.apache.kafka.clients.producer.RecordMetadata</c>.
/// On a failed delivery an <see cref="IDeliveryCallback"/> receives Java's placeholder instead.
/// </summary>
public sealed class RecordMetadata
{
    internal RecordMetadata(string topic, int partition, long offset, long timestamp)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
    }

    /// <summary>The topic the record was published to.</summary>
    public string Topic { get; }

    /// <summary>The partition the record was published to.</summary>
    public int Partition { get; }

    /// <summary>The offset the record was assigned in its partition.</summary>
    public long Offset { get; }

    /// <summary>The record's timestamp in milliseconds since epoch.</summary>
    public long Timestamp { get; }

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{0}-{1}@{2}",
            Topic,
            Partition,
            Offset);
}
