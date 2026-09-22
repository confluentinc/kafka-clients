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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Reads one row of the <c>describeTransactions</c> table — six inline scalars and the
/// nested <c>(i, j)</c> walk over that transaction's topic partitions.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The inner walk is bounded by <c>get_topic_partition_count(i)</c>, never by the
/// outer <c>count</c>.</b> The two are unrelated, and the inner count is <c>0</c> for a row
/// the call failed to describe (<c>confluent_kafka.h:10062-10065</c>).
/// </para>
/// <para>
/// ⚠ <c>transaction_start_time_ms</c> is Java's <c>OptionalLong</c> and the ABI spells the
/// discriminant as the <b>return value</b> (<c>h:10046-10060</c>): when it returns false the
/// out-param is left untouched, so the value must not be read on that branch.
/// </para>
/// <para>
/// The accessor set is a <b>parameter</b> for the <see cref="UserScramCredentialMarshal"/>
/// reason: both mocks mirror Java's own <c>UnsupportedOperationException</c> for this RPC, so
/// the ABI offers no way to construct a populated result to drive the walk on.
/// </para>
/// </remarks>
internal static class TransactionDescriptionMarshal
{
    /// <summary>The production <c>kafka_admin_DescribeTransactionsResult_*</c> set.</summary>
    internal static readonly Accessors NativeAccessors = new Accessors(
        NativeMethods.DescribeTransactionsResultGetCoordinatorId,
        NativeMethods.DescribeTransactionsResultGetState,
        NativeMethods.DescribeTransactionsResultGetProducerId,
        NativeMethods.DescribeTransactionsResultGetProducerEpoch,
        NativeMethods.DescribeTransactionsResultGetTransactionTimeoutMs,
        NativeMethods.DescribeTransactionsResultGetTransactionStartTimeMs,
        NativeMethods.DescribeTransactionsResultGetTopicPartitionCount,
        NativeMethods.DescribeTransactionsResultGetTopicPartitionTopic,
        NativeMethods.DescribeTransactionsResultGetTopicPartitionPartition);

    /// <summary>Reads row <paramref name="index"/> through the production accessors.</summary>
    /// <param name="result">The owned result root.</param>
    /// <param name="index">The row index, inside the result's own count.</param>
    /// <returns>The copied-out description.</returns>
    internal static TransactionDescription ReadDescription(IntPtr result, int index) =>
        ReadDescription(result, index, NativeAccessors);

    /// <summary>Reads row <paramref name="index"/> through an injected accessor set.</summary>
    /// <param name="result">The result root, or a stand-in under an injected set.</param>
    /// <param name="index">The row index.</param>
    /// <param name="accessors">The nine accessors to decode it with.</param>
    /// <returns>The copied-out description.</returns>
    /// <exception cref="KafkaException">
    /// A partition inside the row's own count carried no topic name.
    /// </exception>
    internal static TransactionDescription ReadDescription(
        IntPtr result, int index, Accessors accessors)
    {
        // ⚠ Its own count — never the outer one. See the type remarks.
        int partitionCount = accessors.GetTopicPartitionCount(result, index);
        List<TopicPartition> partitions = new List<TopicPartition>(Math.Max(partitionCount, 0));
        for (int partition = 0; partition < partitionCount; partition++)
        {
            string topic = Utf8Marshal.PtrToString(
                    accessors.GetTopicPartitionTopic(result, index, partition))
                ?? throw new KafkaException(
                    "The describeTransactions result produced no topic name for a partition "
                    + "within its own count.");

            partitions.Add(
                new TopicPartition(
                    topic, accessors.GetTopicPartitionPartition(result, index, partition)));
        }

        // ⚠ The out-param is only meaningful when the accessor returns true.
        long? startTimeMs =
            accessors.TryGetTransactionStartTimeMs(result, index, out long startTime)
                ? startTime
                : (long?)null;

        return new TransactionDescription(
            accessors.GetCoordinatorId(result, index),
            TransactionMarshal.ReadState(accessors.GetState(result, index)),
            accessors.GetProducerId(result, index),
            accessors.GetProducerEpoch(result, index),
            accessors.GetTransactionTimeoutMs(result, index),
            startTimeMs,
            partitions);
    }

    /// <summary>Java's <c>OptionalLong</c> at the ABI: false means empty, and leaves the out-param.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="value">The value, written only when this returns true.</param>
    /// <returns>Whether the row carries a value.</returns>
    internal delegate bool OptionalInt64Accessor(IntPtr result, int index, out long value);

    /// <summary>Reads a borrowed string indexed by <c>(row, partition)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="partitionIndex">The partition index within that row.</param>
    /// <returns>The borrowed pointer, or <c>IntPtr.Zero</c> when either index is out of range.</returns>
    internal delegate IntPtr NestedStringAccessor(IntPtr result, int index, int partitionIndex);

    /// <summary>Reads an <c>int32_t</c> indexed by <c>(row, partition)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="partitionIndex">The partition index within that row.</param>
    /// <returns>The value, or <c>-1</c> when either index is out of range.</returns>
    internal delegate int NestedInt32Accessor(IntPtr result, int index, int partitionIndex);

    /// <summary>The nine per-row accessors, as one set.</summary>
    internal sealed class Accessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        /// <param name="getCoordinatorId">The row's coordinator broker id.</param>
        /// <param name="getState">The row's state, borrowed, as Java's <c>toString()</c> spelling.</param>
        /// <param name="getProducerId">The row's producer id.</param>
        /// <param name="getProducerEpoch">The row's producer epoch.</param>
        /// <param name="getTransactionTimeoutMs">The row's configured transaction timeout.</param>
        /// <param name="tryGetTransactionStartTimeMs">The row's optional start time.</param>
        /// <param name="getTopicPartitionCount">The row's partition count — the inner bound.</param>
        /// <param name="getTopicPartitionTopic">One partition's topic name, borrowed.</param>
        /// <param name="getTopicPartitionPartition">One partition's id.</param>
        internal Accessors(
            Func<IntPtr, int, int> getCoordinatorId,
            KeyedResultMarshal.IndexedAccessor getState,
            Func<IntPtr, int, long> getProducerId,
            Func<IntPtr, int, int> getProducerEpoch,
            Func<IntPtr, int, long> getTransactionTimeoutMs,
            OptionalInt64Accessor tryGetTransactionStartTimeMs,
            Func<IntPtr, int, int> getTopicPartitionCount,
            NestedStringAccessor getTopicPartitionTopic,
            NestedInt32Accessor getTopicPartitionPartition)
        {
            GetCoordinatorId = getCoordinatorId;
            GetState = getState;
            GetProducerId = getProducerId;
            GetProducerEpoch = getProducerEpoch;
            GetTransactionTimeoutMs = getTransactionTimeoutMs;
            TryGetTransactionStartTimeMs = tryGetTransactionStartTimeMs;
            GetTopicPartitionCount = getTopicPartitionCount;
            GetTopicPartitionTopic = getTopicPartitionTopic;
            GetTopicPartitionPartition = getTopicPartitionPartition;
        }

        /// <summary><c>get_coordinator_id(i)</c>.</summary>
        internal Func<IntPtr, int, int> GetCoordinatorId { get; }

        /// <summary><c>get_state(i)</c> — borrowed.</summary>
        internal KeyedResultMarshal.IndexedAccessor GetState { get; }

        /// <summary><c>get_producer_id(i)</c>.</summary>
        internal Func<IntPtr, int, long> GetProducerId { get; }

        /// <summary><c>get_producer_epoch(i)</c>.</summary>
        internal Func<IntPtr, int, int> GetProducerEpoch { get; }

        /// <summary><c>get_transaction_timeout_ms(i)</c>.</summary>
        internal Func<IntPtr, int, long> GetTransactionTimeoutMs { get; }

        /// <summary><c>get_transaction_start_time_ms(i, &amp;out)</c>.</summary>
        internal OptionalInt64Accessor TryGetTransactionStartTimeMs { get; }

        /// <summary><c>get_topic_partition_count(i)</c> — the inner walk's bound.</summary>
        internal Func<IntPtr, int, int> GetTopicPartitionCount { get; }

        /// <summary><c>get_topic_partition_topic(i, j)</c> — borrowed.</summary>
        internal NestedStringAccessor GetTopicPartitionTopic { get; }

        /// <summary><c>get_topic_partition_partition(i, j)</c>.</summary>
        internal NestedInt32Accessor GetTopicPartitionPartition { get; }
    }
}
