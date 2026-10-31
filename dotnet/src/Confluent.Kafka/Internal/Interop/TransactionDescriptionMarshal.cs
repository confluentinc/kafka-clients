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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the transaction description a describe-transactions future delivers into a managed value, and maps a
/// transaction-state singleton to its <see cref="TransactionState"/>.
/// </summary>
internal static class TransactionDescriptionMarshal
{
    internal static TransactionDescription CopyOut(IntPtr description)
    {
        if (description == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no transaction description.");
        }

        IntPtr outErr = IntPtr.Zero;
        int coordinatorId = NativeMethods.TransactionDescriptionCoordinatorId(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr state = NativeMethods.TransactionDescriptionState(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        TransactionState mappedState = StateFrom(state);
        long producerId = NativeMethods.TransactionDescriptionProducerId(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int producerEpoch = NativeMethods.TransactionDescriptionProducerEpoch(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long transactionTimeoutMs = NativeMethods.TransactionDescriptionTransactionTimeoutMs(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long transactionStartTimeMs = NativeMethods.TransactionDescriptionTransactionStartTimeMs(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr topicPartitions = NativeMethods.TransactionDescriptionTopicPartitions(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new TransactionDescription(
            coordinatorId,
            mappedState,
            producerId,
            producerEpoch,
            transactionTimeoutMs,
            transactionStartTimeMs < 0 ? null : transactionStartTimeMs,
            topicPartitions == IntPtr.Zero
                ? Array.Empty<TopicPartition>()
                : TopicPartitionListMarshal.CopyOutAndDestroy(topicPartitions));
    }

    internal static TransactionState StateFrom(IntPtr state)
    {
        if (state == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no transaction state.");
        }

        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.TransactionStateEnum(state, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return id switch
        {
            0 => TransactionState.Ongoing,
            1 => TransactionState.PrepareAbort,
            2 => TransactionState.PrepareCommit,
            3 => TransactionState.CompleteAbort,
            4 => TransactionState.CompleteCommit,
            5 => TransactionState.Empty,
            6 => TransactionState.PrepareEpochFence,
            _ => TransactionState.Unknown,
        };
    }
}
