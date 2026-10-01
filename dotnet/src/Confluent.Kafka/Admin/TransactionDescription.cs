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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The description of one ongoing transaction — Java's
/// <c>org.apache.kafka.clients.admin.TransactionDescription</c> (<c>:25</c>).
/// </summary>
/// <remarks>
/// ⚠ <see cref="TransactionStartTimeMs"/> is Java's <c>OptionalLong</c> (<c>:31</c>): absent is
/// <see langword="null"/>, never <c>0</c> or <c>-1</c>. <see cref="TransactionTimeoutMs"/> is a
/// <see langword="long"/> because Java's is (<c>:30</c>), although the wire field is an int.
/// </remarks>
public sealed class TransactionDescription
{
    private readonly List<TopicPartition> _topicPartitions;

    /// <summary>Creates a transaction description — Java's constructor (<c>:34-50</c>).</summary>
    /// <param name="coordinatorId">The id of the transaction coordinator broker.</param>
    /// <param name="state">The transaction state.</param>
    /// <param name="producerId">The producer id.</param>
    /// <param name="producerEpoch">The producer epoch.</param>
    /// <param name="transactionTimeoutMs">The configured transaction timeout, in milliseconds.</param>
    /// <param name="transactionStartTimeMs">
    /// When the transaction started, in milliseconds, or <see langword="null"/> when absent.
    /// </param>
    /// <param name="topicPartitions">The partitions enlisted in the transaction; null means none.</param>
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

        // Java's field is a Set (:32); de-duplicate into an owned list so the published
        // collection keeps set semantics (the MemberAssignment precedent).
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

    /// <summary>The coordinator broker id — Java's <c>coordinatorId()</c> (<c>:52</c>).</summary>
    public int CoordinatorId { get; }

    /// <summary>The transaction state — Java's <c>state()</c> (<c>:56</c>).</summary>
    public TransactionState State { get; }

    /// <summary>The producer id — Java's <c>producerId()</c> (<c>:60</c>).</summary>
    public long ProducerId { get; }

    /// <summary>The producer epoch — Java's <c>producerEpoch()</c> (<c>:64</c>).</summary>
    public int ProducerEpoch { get; }

    /// <summary>The transaction timeout in ms — Java's <c>transactionTimeoutMs()</c> (<c>:68</c>).</summary>
    public long TransactionTimeoutMs { get; }

    /// <summary>
    /// When the transaction started in ms, or <see langword="null"/> when absent — Java's
    /// <c>transactionStartTimeMs()</c> (<c>:72</c>, <c>OptionalLong</c>).
    /// </summary>
    public long? TransactionStartTimeMs { get; }

    /// <summary>
    /// The partitions enlisted in the transaction — Java's <c>topicPartitions()</c> (<c>:76</c>).
    /// Never <see langword="null"/>.
    /// </summary>
    public IReadOnlyCollection<TopicPartition> TopicPartitions => _topicPartitions;

    /// <summary>Value equality over all seven fields — Java's <c>equals</c> (<c>:81</c>).</summary>
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

    /// <summary>The hash of all seven fields — Java's <c>hashCode</c> (<c>:95</c>).</summary>
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

            // Order-independent, matching the Set the hash mirrors.
            int partitions = 0;
            foreach (TopicPartition topicPartition in _topicPartitions)
            {
                partitions += topicPartition.GetHashCode();
            }

            return (hash * 31) + partitions;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:100</c>).</summary>
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
                ? TransactionStartTimeMs.Value.ToString(CultureInfo.InvariantCulture)
                : "null",
            string.Join(", ", _topicPartitions));
}
