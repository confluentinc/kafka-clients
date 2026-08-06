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
/// Marshals an owned (Category-3) <c>TopicPartitionList_t</c> borrow-root into an owned
/// snapshot of <see cref="TopicPartition"/> and then destroys it (ffi-marshalling.md
/// §B2/§B3). The list is the borrow-root; each element from
/// <c>TopicPartitionList_get</c> is a Category-4 <b>borrowed view</b> (never freed on its
/// own — it dies with the root), so every field is copied out <b>before</b> the root is
/// destroyed. The sibling of <see cref="ConsumerGroupMetadataMarshal"/> /
/// <see cref="StringListMarshal"/>; it needs no <c>unsafe</c> (the NUL-terminated
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> reads are safe managed API).
/// </summary>
internal static class TopicPartitionListMarshal
{
    /// <summary>
    /// Reads every element off the owned <paramref name="list"/> borrow-root — each
    /// element's NUL-terminated topic (§B3, NOT the length-delimited receive-path form)
    /// and its partition — into an owned <see cref="TopicPartition"/> array, then frees
    /// the root exactly once in a <c>finally</c> (even if a read throws). The borrowed
    /// element / string pointers are copied out before the destroy — they die with the
    /// root (§B2 Category 4). The result is an immutable owned snapshot, matching Java's
    /// "returns a copy" contract.
    /// </summary>
    /// <param name="list">The owned, non-null topic-partition-list handle.</param>
    internal static IReadOnlyCollection<TopicPartition> CopyOutAndDestroy(IntPtr list)
    {
        try
        {
            int count = NativeMethods.TopicPartitionListCount(list);
            TopicPartition[] result = new TopicPartition[count];
            for (int i = 0; i < count; i++)
            {
                // Borrowed element (Category 4) — never freed on its own; dies with the
                // root. Copy the topic + partition out BEFORE _destroy (§B2/§B3).
                IntPtr element = NativeMethods.TopicPartitionListGet(list, i);
                string topic = Utf8Marshal.PtrToString(NativeMethods.TopicPartitionTopic(element)) ?? string.Empty;
                int partition = NativeMethods.TopicPartitionPartition(element);
                result[i] = new TopicPartition(topic, partition);
            }

            return result;
        }
        finally
        {
            // Owned Category-3 borrow-root — free it exactly once after copy-out (§B2).
            NativeMethods.TopicPartitionListDestroy(list);
        }
    }
}
