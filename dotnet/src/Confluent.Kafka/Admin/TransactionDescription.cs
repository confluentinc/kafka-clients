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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The description of one ongoing transaction — Java's
/// <c>org.apache.kafka.clients.admin.TransactionDescription</c>.
/// </summary>
/// <remarks>
/// <see cref="TransactionStartTimeMs()"/> is Java's <c>OptionalLong</c>: absent is
/// <see langword="null"/>, never <c>0</c> or <c>-1</c>. <see cref="TransactionTimeoutMs()"/> is a
/// <see langword="long"/> because Java's is, although the wire field is an int.
/// </remarks>
public sealed class TransactionDescription
{
    private readonly List<TopicPartition> _topicPartitions;
    private readonly int _coordinatorId;
    private readonly TransactionState _state;
    private readonly long _producerId;
    private readonly int _producerEpoch;
    private readonly long _transactionTimeoutMs;
    private readonly long? _transactionStartTimeMs;

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
    /// A null <paramref name="topicPartitions"/> becomes an empty <see cref="TopicPartitions()"/>,
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
        _coordinatorId = coordinatorId;
        _state = state;
        _producerId = producerId;
        _producerEpoch = producerEpoch;
        _transactionTimeoutMs = transactionTimeoutMs;
        _transactionStartTimeMs = transactionStartTimeMs;

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
    /// <returns>The coordinator broker id.</returns>
    public int CoordinatorId() => _coordinatorId;

    /// <summary>The transaction state — Java's <c>state()</c>.</summary>
    /// <returns>The transaction state.</returns>
    public TransactionState State() => _state;

    /// <summary>The producer id — Java's <c>producerId()</c>.</summary>
    /// <returns>The producer id.</returns>
    public long ProducerId() => _producerId;

    /// <summary>The producer epoch — Java's <c>producerEpoch()</c>.</summary>
    /// <returns>The producer epoch.</returns>
    public int ProducerEpoch() => _producerEpoch;

    /// <summary>The transaction timeout in ms — Java's <c>transactionTimeoutMs()</c>.</summary>
    /// <returns>The transaction timeout in ms.</returns>
    public long TransactionTimeoutMs() => _transactionTimeoutMs;

    /// <summary>
    /// When the transaction started in ms, or <see langword="null"/> when absent — Java's
    /// <c>transactionStartTimeMs()</c>.
    /// </summary>
    /// <returns>When the transaction started in ms, or <see langword="null"/> when absent.</returns>
    public long? TransactionStartTimeMs() => _transactionStartTimeMs;

    /// <summary>
    /// The partitions enlisted in the transaction — Java's <c>topicPartitions()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    /// <returns>The partitions enlisted in the transaction.</returns>
    public IReadOnlyCollection<TopicPartition> TopicPartitions() => _topicPartitions;

    /// <summary>Value equality over all seven fields — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same transaction.</returns>
    public override bool Equals(object? obj) =>
        obj is TransactionDescription other
        && _coordinatorId == other._coordinatorId
        && _producerId == other._producerId
        && _producerEpoch == other._producerEpoch
        && _transactionTimeoutMs == other._transactionTimeoutMs
        && _state == other._state
        && _transactionStartTimeMs == other._transactionStartTimeMs
        && _topicPartitions.Count == other._topicPartitions.Count
        && new HashSet<TopicPartition>(_topicPartitions).SetEquals(other._topicPartitions);

    /// <summary>The hash of all seven fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = _coordinatorId;
            hash = (hash * 31) + (int)_state;
            hash = (hash * 31) + _producerId.GetHashCode();
            hash = (hash * 31) + _producerEpoch;
            hash = (hash * 31) + _transactionTimeoutMs.GetHashCode();
            hash = (hash * 31) + (_transactionStartTimeMs?.GetHashCode() ?? 0);

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
            _coordinatorId,
            TransactionMarshal.NameFromState(_state) ?? _state.ToString(),
            _producerId,
            _producerEpoch,
            _transactionTimeoutMs,
            _transactionStartTimeMs.HasValue
                ? "OptionalLong[" + _transactionStartTimeMs.Value.ToString(CultureInfo.InvariantCulture) + "]"
                : "OptionalLong.empty",
            string.Join(", ", _topicPartitions));
}
