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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Identifies the hanging transaction to abort — Java's
/// <c>org.apache.kafka.clients.admin.AbortTransactionSpec</c>.
/// </summary>
/// <remarks>
/// <see cref="ProducerEpoch"/> is a <see langword="short"/>, as Java's is, although the same concept
/// is an <see langword="int"/> on <see cref="ProducerState"/> and <see cref="TransactionDescription"/>.
/// </remarks>
public sealed class AbortTransactionSpec
{
    /// <summary>Creates an abort specification — Java's constructor.</summary>
    /// <param name="topicPartition">The partition holding the hanging transaction.</param>
    /// <param name="producerId">The producer id of the hanging transaction.</param>
    /// <param name="producerEpoch">The producer epoch of the hanging transaction.</param>
    /// <param name="coordinatorEpoch">The epoch of the transaction coordinator.</param>
    public AbortTransactionSpec(
        TopicPartition topicPartition,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch)
    {
        TopicPartition = topicPartition;
        ProducerId = producerId;
        ProducerEpoch = producerEpoch;
        CoordinatorEpoch = coordinatorEpoch;
    }

    /// <summary>The partition — Java's <c>topicPartition()</c>.</summary>
    public TopicPartition TopicPartition { get; }

    /// <summary>The producer id — Java's <c>producerId()</c>.</summary>
    public long ProducerId { get; }

    /// <summary>The producer epoch — Java's <c>producerEpoch()</c>.</summary>
    public short ProducerEpoch { get; }

    /// <summary>The coordinator epoch — Java's <c>coordinatorEpoch()</c>.</summary>
    public int CoordinatorEpoch { get; }

    /// <summary>Value equality over all four fields — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two identify the same transaction.</returns>
    public override bool Equals(object? obj) =>
        obj is AbortTransactionSpec other
        && ProducerId == other.ProducerId
        && ProducerEpoch == other.ProducerEpoch
        && CoordinatorEpoch == other.CoordinatorEpoch
        && TopicPartition == other.TopicPartition;

    /// <summary>The hash of all four fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = TopicPartition.GetHashCode();
            hash = (hash * 31) + ProducerId.GetHashCode();
            hash = (hash * 31) + ProducerEpoch;
            return (hash * 31) + CoordinatorEpoch;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "AbortTransactionSpec(topicPartition={0}, producerId={1}, producerEpoch={2}, coordinatorEpoch={3})",
            TopicPartition,
            ProducerId,
            ProducerEpoch,
            CoordinatorEpoch);
}
