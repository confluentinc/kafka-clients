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
/// Builds the ABI's <c>kafka_admin_NewTopic_t</c> input handle from the public
/// <see cref="NewTopic"/>. One of only two <b>input</b> handle types in the whole admin
/// ABI (every other input crosses as parallel arrays).
/// </summary>
/// <remarks>
/// <para>
/// <b>Ownership: the caller retains it.</b> The ABI copies out of these entries during
/// the submit, and the header states the caller keeps ownership — so every handle built
/// here must be destroyed after the submit returns, or each call leaks one per topic.
/// </para>
/// <para>
/// <b>The two request forms.</b> A negative <c>num_partitions</c> /
/// <c>replication_factor</c> means "unset" (Java's <c>Optional.empty()</c>), and setting
/// any replica assignment switches the entry to Java's replica-assignment constructor,
/// in which those two are not sent at all. <see cref="NewTopic"/> makes the two forms
/// mutually exclusive by construction, so this method simply applies whichever is
/// present.
/// </para>
/// </remarks>
internal static class NewTopicMarshal
{
    /// <summary>
    /// Builds one native entry. On any failure after allocation the handle is destroyed
    /// before rethrowing, so a partially built entry never leaks.
    /// </summary>
    /// <param name="topic">The topic to create. Already validated by the caller.</param>
    /// <returns>An owned <c>kafka_admin_NewTopic_t</c> the caller must destroy.</returns>
    /// <exception cref="KafkaException">The ABI refused to allocate the entry.</exception>
    internal static IntPtr Build(NewTopic topic)
    {
        IntPtr handle;
        using (Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(topic.Name))
        {
            handle = NativeMethods.NewTopicNew(
                name.Pointer,
                topic.NumPartitions ?? -1,
                topic.ReplicationFactor ?? (short)-1);
        }

        if (handle == IntPtr.Zero)
        {
            // The ABI returns null only for a null name, which NewTopic's constructor
            // already rejects — so reaching here is a core contract violation, surfaced
            // as an operational error rather than a null dereference.
            throw new KafkaException(
                $"kafka_admin_NewTopic_new returned a null handle for topic '{topic.Name}'.");
        }

        try
        {
            if (topic.Configs is not null)
            {
                foreach (KeyValuePair<string, string> entry in topic.Configs)
                {
                    using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                    using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                    NativeMethods.NewTopicPutConfig(handle, key.Pointer, value.Pointer);
                }
            }

            if (topic.ReplicasAssignments is not null)
            {
                foreach (KeyValuePair<int, IReadOnlyList<int>> assignment in topic.ReplicasAssignments)
                {
                    int[] brokerIds = new int[assignment.Value.Count];
                    for (int i = 0; i < brokerIds.Length; i++)
                    {
                        brokerIds[i] = assignment.Value[i];
                    }

                    // The blittable int[] is pinned by the marshaller for the duration of
                    // the call; the ABI copies out during it (ffi §A4 call-scoped).
                    NativeMethods.NewTopicSetReplicasAssignment(
                        handle, assignment.Key, brokerIds, brokerIds.Length);
                }
            }
        }
        catch
        {
            NativeMethods.NewTopicDestroy(handle);
            throw;
        }

        return handle;
    }
}
