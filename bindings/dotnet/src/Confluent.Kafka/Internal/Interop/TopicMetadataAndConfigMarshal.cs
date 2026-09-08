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
/// The <b>copy-out</b> marshaller for one <c>createTopics</c> per-key value: turns a
/// borrowed <c>kafka_admin_TopicMetadataAndConfig_t</c> into a fully owned managed
/// <see cref="TopicMetadataAndConfig"/>, so nothing native-backed survives the result
/// root's destroy (ffi §B4 / CLAUDE.md §6.4).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ Both the metadata pointer and everything reachable from it — the inner error, the
/// topic-id string, every config name and value — are <b>borrowed from the result
/// root</b> and die with it. They are read and copied here, on whichever thread the
/// completion callback fired on, strictly before the trampoline destroys that root.
/// The inner error therefore uses
/// <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/>; destroying it would be a
/// double free.
/// </para>
/// <para>
/// The inner error is <b>not</b> the per-key error. A non-null one means the topic
/// <em>was</em> created but the broker returned no metadata — Java's
/// <c>TopicMetadataAndConfig.ensureSuccess()</c> condition, where the per-topic future
/// still completes successfully and only the accessors throw.
/// </para>
/// </remarks>
internal static class TopicMetadataAndConfigMarshal
{
    /// <summary>
    /// Copies the borrowed metadata into an owned managed object.
    /// </summary>
    /// <param name="metadataAndConfig">
    /// A borrowed <c>kafka_admin_TopicMetadataAndConfig_t</c> from
    /// <c>CreateTopicsResult_get_value</c>. Non-null on the branch that reaches here
    /// (its complement, <c>get_error</c>, was null).
    /// </param>
    /// <returns>The owned metadata.</returns>
    internal static TopicMetadataAndConfig CopyOut(IntPtr metadataAndConfig)
    {
        // ⚠ BORROWED — read, never destroy (it dies with the result root).
        KafkaException? metadataError =
            KafkaException.FromBorrowedHandle(NativeMethods.TopicMetadataAndConfigError(metadataAndConfig));
        if (metadataError is not null)
        {
            return new TopicMetadataAndConfig(metadataError);
        }

        // NUL-terminated, borrowed (ffi §B3 row 2). The header documents an empty string
        // only for the metadata-unavailable case, already handled above; treating an
        // empty id as Uuid.Zero keeps that defensive, while a genuinely malformed id
        // throws out of Parse and faults just this key (KeyedResultMarshal's per-key
        // catch) rather than the whole batch.
        string topicIdText =
            Utf8Marshal.PtrToString(NativeMethods.TopicMetadataAndConfigTopicId(metadataAndConfig)) ?? string.Empty;
        Uuid topicId = topicIdText.Length == 0 ? Uuid.Zero : Uuid.Parse(topicIdText);

        int numPartitions = NativeMethods.TopicMetadataAndConfigNumPartitions(metadataAndConfig);

        // The ABI widens Java's `short` replication factor to int32_t; narrow it back.
        short replicationFactor =
            (short)NativeMethods.TopicMetadataAndConfigReplicationFactor(metadataAndConfig);

        return new TopicMetadataAndConfig(topicId, numPartitions, replicationFactor, CopyOutConfig(metadataAndConfig));
    }

    /// <summary>
    /// Copies the flattened config accessors into an owned <see cref="Config"/>.
    /// </summary>
    private static Config CopyOutConfig(IntPtr metadataAndConfig)
    {
        int count = NativeMethods.TopicMetadataAndConfigConfigCount(metadataAndConfig);
        if (count <= 0)
        {
            return new Config(Array.Empty<ConfigEntry>());
        }

        List<ConfigEntry> entries = new List<ConfigEntry>(count);
        for (int index = 0; index < count; index++)
        {
            string? name = Utf8Marshal.PtrToString(
                NativeMethods.TopicMetadataAndConfigConfigName(metadataAndConfig, index));
            if (name is null)
            {
                // Guarded by `count`, so unreachable; skipping is the safe reading.
                continue;
            }

            // A null value pointer is a genuinely null value (Java's ConfigEntry.value()
            // is nullable), NOT an out-of-range marker — `name` already proved the index.
            string? value = Utf8Marshal.PtrToString(
                NativeMethods.TopicMetadataAndConfigConfigValue(metadataAndConfig, index));

            entries.Add(new ConfigEntry(
                name,
                value,
                NativeMethods.TopicMetadataAndConfigConfigIsDefault(metadataAndConfig, index),
                NativeMethods.TopicMetadataAndConfigConfigIsSensitive(metadataAndConfig, index),
                NativeMethods.TopicMetadataAndConfigConfigIsReadOnly(metadataAndConfig, index)));
        }

        return new Config(entries);
    }
}
