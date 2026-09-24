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
/// Reads one row of the <c>describeProducers</c> table — the nested <c>(i, j)</c> walk over
/// that partition's active producers.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The inner walk is bounded by <c>get_producer_count(i)</c>, never by the outer
/// <c>count</c>.</b> The two are unrelated, and the inner count is <c>0</c> for a partition
/// the call failed to describe (<c>confluent_kafka.h:9828-9831</c>).
/// </para>
/// <para>
/// ⚠ Both optionals spell their discriminant as the <b>return value</b>
/// (<c>h:9896-9911</c>, <c>:9915-9928</c>) rather than as a sentinel, because every
/// <c>long</c> is a legal start offset — so the out-param must not be read when the accessor
/// returns false. The four plain scalars beside them <em>do</em> use <c>-1</c>, as Java's own
/// <c>RecordBatch.NO_PRODUCER_ID</c> / <c>NO_SEQUENCE</c> / <c>NO_TIMESTAMP</c>; the two
/// encodings must not be conflated.
/// </para>
/// <para>
/// The accessor set is a <b>parameter</b> for the <see cref="UserScramCredentialMarshal"/>
/// reason: both mocks mirror Java's own <c>UnsupportedOperationException</c> for this RPC, so
/// the ABI offers no way to construct a populated result to drive the walk on.
/// </para>
/// </remarks>
internal static class PartitionProducerStateMarshal
{
    /// <summary>The production <c>kafka_admin_DescribeProducersResult_*</c> set.</summary>
    internal static readonly Accessors NativeAccessors = new Accessors(
        NativeMethods.DescribeProducersResultGetProducerCount,
        NativeMethods.DescribeProducersResultGetProducerId,
        NativeMethods.DescribeProducersResultGetProducerEpoch,
        NativeMethods.DescribeProducersResultGetLastSequence,
        NativeMethods.DescribeProducersResultGetLastTimestamp,
        NativeMethods.DescribeProducersResultGetCurrentTransactionStartOffset,
        NativeMethods.DescribeProducersResultGetCoordinatorEpoch);

    /// <summary>Reads row <paramref name="index"/> through the production accessors.</summary>
    /// <param name="result">The owned result root.</param>
    /// <param name="index">The row index, inside the result's own count.</param>
    /// <returns>The copied-out partition state.</returns>
    internal static DescribeProducersResult.PartitionProducerState ReadPartitionProducerState(
        IntPtr result, int index) =>
        ReadPartitionProducerState(result, index, NativeAccessors);

    /// <summary>Reads row <paramref name="index"/> through an injected accessor set.</summary>
    /// <param name="result">The result root, or a stand-in under an injected set.</param>
    /// <param name="index">The row index.</param>
    /// <param name="accessors">The seven accessors to decode it with.</param>
    /// <returns>The copied-out partition state.</returns>
    internal static DescribeProducersResult.PartitionProducerState ReadPartitionProducerState(
        IntPtr result, int index, Accessors accessors)
    {
        // ⚠ Its own count — never the outer one. See the type remarks.
        int producerCount = accessors.GetProducerCount(result, index);
        List<ProducerState> producers = new List<ProducerState>(Math.Max(producerCount, 0));
        for (int producer = 0; producer < producerCount; producer++)
        {
            // ⚠ Each out-param is only meaningful when its accessor returns true.
            int? coordinatorEpoch =
                accessors.TryGetCoordinatorEpoch(result, index, producer, out int epoch)
                    ? epoch
                    : (int?)null;
            long? startOffset =
                accessors.TryGetCurrentTransactionStartOffset(result, index, producer, out long offset)
                    ? offset
                    : (long?)null;

            producers.Add(
                new ProducerState(
                    accessors.GetProducerId(result, index, producer),
                    accessors.GetProducerEpoch(result, index, producer),
                    accessors.GetLastSequence(result, index, producer),
                    accessors.GetLastTimestamp(result, index, producer),
                    coordinatorEpoch,
                    startOffset));
        }

        return new DescribeProducersResult.PartitionProducerState(producers);
    }

    /// <summary>Reads an <c>int64_t</c> indexed by <c>(row, producer)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="producerIndex">The producer index within that row.</param>
    /// <returns>The value, or <c>-1</c> when either index is out of range.</returns>
    internal delegate long NestedInt64Accessor(IntPtr result, int index, int producerIndex);

    /// <summary>Reads an <c>int32_t</c> indexed by <c>(row, producer)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="producerIndex">The producer index within that row.</param>
    /// <returns>The value, or <c>-1</c> when either index is out of range.</returns>
    internal delegate int NestedInt32Accessor(IntPtr result, int index, int producerIndex);

    /// <summary>Java's <c>OptionalLong</c> at the ABI: false means empty, and leaves the out-param.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="producerIndex">The producer index within that row.</param>
    /// <param name="value">The value, written only when this returns true.</param>
    /// <returns>Whether the producer carries a value.</returns>
    internal delegate bool NestedOptionalInt64Accessor(
        IntPtr result, int index, int producerIndex, out long value);

    /// <summary>Java's <c>OptionalInt</c> at the ABI: false means empty, and leaves the out-param.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="producerIndex">The producer index within that row.</param>
    /// <param name="value">The value, written only when this returns true.</param>
    /// <returns>Whether the producer carries a value.</returns>
    internal delegate bool NestedOptionalInt32Accessor(
        IntPtr result, int index, int producerIndex, out int value);

    /// <summary>The seven per-row accessors, as one set.</summary>
    internal sealed class Accessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        /// <param name="getProducerCount">The row's producer count — the inner bound.</param>
        /// <param name="getProducerId">One producer's id.</param>
        /// <param name="getProducerEpoch">One producer's epoch.</param>
        /// <param name="getLastSequence">One producer's last written sequence.</param>
        /// <param name="getLastTimestamp">One producer's last written timestamp.</param>
        /// <param name="tryGetCurrentTransactionStartOffset">One producer's optional start offset.</param>
        /// <param name="tryGetCoordinatorEpoch">One producer's optional coordinator epoch.</param>
        internal Accessors(
            Func<IntPtr, int, int> getProducerCount,
            NestedInt64Accessor getProducerId,
            NestedInt32Accessor getProducerEpoch,
            NestedInt32Accessor getLastSequence,
            NestedInt64Accessor getLastTimestamp,
            NestedOptionalInt64Accessor tryGetCurrentTransactionStartOffset,
            NestedOptionalInt32Accessor tryGetCoordinatorEpoch)
        {
            GetProducerCount = getProducerCount;
            GetProducerId = getProducerId;
            GetProducerEpoch = getProducerEpoch;
            GetLastSequence = getLastSequence;
            GetLastTimestamp = getLastTimestamp;
            TryGetCurrentTransactionStartOffset = tryGetCurrentTransactionStartOffset;
            TryGetCoordinatorEpoch = tryGetCoordinatorEpoch;
        }

        /// <summary><c>get_producer_count(i)</c> — the inner walk's bound.</summary>
        internal Func<IntPtr, int, int> GetProducerCount { get; }

        /// <summary><c>get_producer_id(i, j)</c>.</summary>
        internal NestedInt64Accessor GetProducerId { get; }

        /// <summary><c>get_producer_epoch(i, j)</c>.</summary>
        internal NestedInt32Accessor GetProducerEpoch { get; }

        /// <summary><c>get_last_sequence(i, j)</c>.</summary>
        internal NestedInt32Accessor GetLastSequence { get; }

        /// <summary><c>get_last_timestamp(i, j)</c>.</summary>
        internal NestedInt64Accessor GetLastTimestamp { get; }

        /// <summary><c>get_current_transaction_start_offset(i, j, &amp;out)</c>.</summary>
        internal NestedOptionalInt64Accessor TryGetCurrentTransactionStartOffset { get; }

        /// <summary><c>get_coordinator_epoch(i, j, &amp;out)</c>.</summary>
        internal NestedOptionalInt32Accessor TryGetCoordinatorEpoch { get; }
    }
}
