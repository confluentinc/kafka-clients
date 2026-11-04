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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the partition producer state a describe-producers future delivers into a managed value.
/// </summary>
internal static class PartitionProducerStateMarshal
{
    internal static DescribeProducersResult.PartitionProducerState CopyOut(IntPtr state)
    {
        if (state == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no partition producer state.");
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr producers = NativeMethods.PartitionProducerStateActiveProducers(state, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            List<ProducerState> copied = new List<ProducerState>();
            if (producers != IntPtr.Zero)
            {
                int count = NativeMethods.ListSize(producers, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                copied.Capacity = Math.Max(count, 0);
                for (int i = 0; i < count; i++)
                {
                    IntPtr producer = NativeMethods.ListGet(producers, i, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    copied.Add(CopyOutProducer(producer));
                }
            }

            return new DescribeProducersResult.PartitionProducerState(copied);
        }
        finally
        {
            DestroyList(producers);
        }
    }

    internal static ProducerState CopyOutProducer(IntPtr producer)
    {
        if (producer == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no producer state.");
        }

        IntPtr outErr = IntPtr.Zero;
        long producerId = NativeMethods.ProducerStateProducerId(producer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int producerEpoch = NativeMethods.ProducerStateProducerEpoch(producer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int lastSequence = NativeMethods.ProducerStateLastSequence(producer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long lastTimestamp = NativeMethods.ProducerStateLastTimestamp(producer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int coordinatorEpoch = NativeMethods.ProducerStateCoordinatorEpoch(producer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long currentTransactionStartOffset = NativeMethods.ProducerStateCurrentTransactionStartOffset(producer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ProducerState(
            producerId,
            producerEpoch,
            lastSequence,
            lastTimestamp,
            coordinatorEpoch < 0 ? null : coordinatorEpoch,
            currentTransactionStartOffset < 0 ? null : currentTransactionStartOffset);
    }

    private static void DestroyList(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
