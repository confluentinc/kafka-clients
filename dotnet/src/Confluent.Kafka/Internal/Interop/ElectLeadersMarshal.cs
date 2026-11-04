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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the borrowed per-partition election outcomes an <c>electLeaders</c> future delivers into an owned map.
/// </summary>
internal static class ElectLeadersMarshal
{
    internal static IReadOnlyDictionary<TopicPartition, KafkaException?> CopyOutPartitions(IntPtr partitions)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(partitions, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<TopicPartition, KafkaException?> copied = new Dictionary<TopicPartition, KafkaException?>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr partition = NativeMethods.MapKey(partitions, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr borrowedError = NativeMethods.MapValue(partitions, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            // The map holds KafkaException values, so an election error of another type fails the future.
            Exception? error = KafkaException.FromFfiError(borrowedError, owned: false);
            if (error is not null and not KafkaException)
            {
                throw error;
            }

            copied[TopicPartitionListMarshal.CopyOutElement(partition)] = (KafkaException?)error;
        }

        return copied;
    }
}
