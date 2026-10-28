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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a <c>kafka_List_t</c> of <c>TopicPartition_t</c>, or one borrowed <c>TopicPartition_t</c>, into owned
/// <see cref="TopicPartition"/>s; only <see cref="CopyOutAndDestroy"/> frees the list.
/// </summary>
internal static class TopicPartitionListMarshal
{
    internal static IReadOnlyCollection<TopicPartition> CopyOut(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (count <= 0)
        {
            return Array.Empty<TopicPartition>();
        }

        List<TopicPartition> result = new List<TopicPartition>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr element = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (element != IntPtr.Zero)
            {
                result.Add(CopyOutElement(element));
            }
        }

        return result;
    }

    internal static IReadOnlyCollection<TopicPartition> CopyOutAndDestroy(IntPtr list)
    {
        try
        {
            return CopyOut(list);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    internal static TopicPartition CopyOutElement(IntPtr topicPartition)
    {
        IntPtr outErr = IntPtr.Zero;
        string topic = Utf8Marshal.PtrToString(NativeMethods.TopicPartitionTopic(topicPartition, ref outErr)) ?? string.Empty;
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int partition = NativeMethods.TopicPartitionPartition(topicPartition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new TopicPartition(topic, partition);
    }
}
