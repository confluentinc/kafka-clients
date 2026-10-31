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

using System.Collections.Generic;
using System.Globalization;
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The description of one ongoing transaction — Java's
/// <c>org.apache.kafka.clients.admin.TransactionDescription</c>.
/// </summary>
/// <remarks>
/// <see cref="TransactionStartTimeMs"/> is Java's <c>OptionalLong</c>: absent is
/// <see langword="null"/>, never <c>0</c> or <c>-1</c>. <see cref="TransactionTimeoutMs"/> is a
/// <see langword="long"/> because Java's is, although the wire field is an int.
/// </remarks>
public sealed class TransactionDescription
{
    private readonly List<TopicPartition> _topicPartitions;

    /// <summary>Creates a transaction description — Java's constructor.</summary>
    /// <param name="coordinatorId">The id of the transaction coordinator broker.</param>
    /// <param name="state">The transaction state.</param>
    /// <param name="producerId">The producer id.</param>
    /// <param name="producerEpoch">The producer epoch.</param>
    /// <param name="transactionTimeoutMs">The configured transaction timeout, in milliseconds.</param>
    /// <param name="transactionStartTimeMs">
    /// When the transaction started, in milliseconds, or <see langword="null"/> when absent.
    /// </param>
    /// <param name="topicPartitions">The partitions enlisted in the transaction; null means none.</param>
    /// <remarks>
    /// A null <paramref name="topicPartitions"/> becomes an empty <see cref="TopicPartitions"/>,
    /// where Java's <c>topicPartitions()</c> would return null: a recorded deviation. Repeated
    /// partitions are kept once, as Java's <c>Set</c> field keeps them.
    /// </remarks>
    public TransactionDescription(
        int coordinatorId,
        TransactionState state,
        long producerId,
        int producerEpoch,
        long transactionTimeoutMs,
        long? transactionStartTimeMs,
        IEnumerable<TopicPartition>? topicPartitions)
    {
        CoordinatorId = coordinatorId;
        State = state;
        ProducerId = producerId;
        ProducerEpoch = producerEpoch;
        TransactionTimeoutMs = transactionTimeoutMs;
        TransactionStartTimeMs = transactionStartTimeMs;

        _topicPartitions = new List<TopicPartition>();
        if (topicPartitions is not null)
        {
            HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
            foreach (TopicPartition topicPartition in topicPartitions)
            {
                if (seen.Add(topicPartition))
                {
                    _topicPartitions.Add(topicPartition);
                }
            }
        }
    }

    /// <summary>The coordinator broker id — Java's <c>coordinatorId()</c>.</summary>
    public int CoordinatorId { get; }

    /// <summary>The transaction state — Java's <c>state()</c>.</summary>
    public TransactionState State { get; }

    /// <summary>The producer id — Java's <c>producerId()</c>.</summary>
    public long ProducerId { get; }

    /// <summary>The producer epoch — Java's <c>producerEpoch()</c>.</summary>
    public int ProducerEpoch { get; }

    /// <summary>The transaction timeout in ms — Java's <c>transactionTimeoutMs()</c>.</summary>
    public long TransactionTimeoutMs { get; }

    /// <summary>
    /// When the transaction started in ms, or <see langword="null"/> when absent — Java's
    /// <c>transactionStartTimeMs()</c>.
    /// </summary>
    public long? TransactionStartTimeMs { get; }

    /// <summary>
    /// The partitions enlisted in the transaction — Java's <c>topicPartitions()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    public IReadOnlyCollection<TopicPartition> TopicPartitions => _topicPartitions;

    /// <summary>Value equality over all seven fields — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same transaction.</returns>
    public override bool Equals(object? obj) =>
        obj is TransactionDescription other
        && CoordinatorId == other.CoordinatorId
        && ProducerId == other.ProducerId
        && ProducerEpoch == other.ProducerEpoch
        && TransactionTimeoutMs == other.TransactionTimeoutMs
        && State == other.State
        && TransactionStartTimeMs == other.TransactionStartTimeMs
        && _topicPartitions.Count == other._topicPartitions.Count
        && new HashSet<TopicPartition>(_topicPartitions).SetEquals(other._topicPartitions);

    /// <summary>The hash of all seven fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = CoordinatorId;
            hash = (hash * 31) + (int)State;
            hash = (hash * 31) + ProducerId.GetHashCode();
            hash = (hash * 31) + ProducerEpoch;
            hash = (hash * 31) + TransactionTimeoutMs.GetHashCode();
            hash = (hash * 31) + (TransactionStartTimeMs?.GetHashCode() ?? 0);

            int partitions = 0;
            foreach (TopicPartition topicPartition in _topicPartitions)
            {
                partitions += topicPartition.GetHashCode();
            }

            return (hash * 31) + partitions;
        }
    }

    /// <summary>
    /// A diagnostic rendering — Java's <c>toString()</c>, where <c>transactionStartTimeMs</c>
    /// prints as Java's <c>OptionalLong[5]</c> or <c>OptionalLong.empty</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "TransactionDescription(coordinatorId={0}, state={1}, producerId={2}, producerEpoch={3}"
                + ", transactionTimeoutMs={4}, transactionStartTimeMs={5}, topicPartitions=[{6}])",
            CoordinatorId,
            State,
            ProducerId,
            ProducerEpoch,
            TransactionTimeoutMs,
            TransactionStartTimeMs.HasValue
                ? "OptionalLong[" + TransactionStartTimeMs.Value.ToString(CultureInfo.InvariantCulture) + "]"
                : "OptionalLong.empty",
            string.Join(", ", _topicPartitions));
}
