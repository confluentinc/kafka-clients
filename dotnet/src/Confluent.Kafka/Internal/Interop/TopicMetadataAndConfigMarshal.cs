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
/// topic-id string, the <c>kafka_admin_Config_t</c> and every entry and string it hands
/// back — are <b>borrowed from the result root</b> and die with it. They are read and copied here, on whichever thread the
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

        // `int` end to end: the ABI reports `int32_t` and Java's result-side accessor is
        // `int replicationFactor()` (CreateTopicsResult.java:141), so nothing is narrowed
        // here. (`short` belongs to the request side, NewTopic.replicationFactor().)
        int replicationFactor = NativeMethods.TopicMetadataAndConfigReplicationFactor(metadataAndConfig);

        return new TopicMetadataAndConfig(topicId, numPartitions, replicationFactor, CopyOutConfig(metadataAndConfig));
    }

    /// <summary>
    /// Copies the topic's borrowed <c>kafka_admin_Config_t</c> into an owned
    /// <see cref="Config"/> with the <b>same reader <c>describeConfigs</c> uses</b>, so every
    /// entry's <see cref="ConfigEntry.Source"/>, <see cref="ConfigEntry.IsSensitive"/> and
    /// <see cref="ConfigEntry.IsReadOnly"/> come from the core (M15/P13.3, D13) — and
    /// <see cref="ConfigEntry.IsDefault"/> derives from that source, as in Java, instead of
    /// the source being guessed from an <c>is_default</c> flag.
    /// </summary>
    /// <remarks>
    /// ⚠ The <c>Config_t</c> is <b>borrowed</b> from <paramref name="metadataAndConfig"/> and is
    /// never destroyed here: <see cref="ConfigMarshal.CopyOut(IntPtr)"/> only reads it. As in
    /// Java, a <c>createTopics</c> entry has no synonyms and a null type and documentation;
    /// <c>ConfigMarshal</c> maps the null type to <see cref="ConfigEntry.ConfigType.Unknown"/>,
    /// because <see cref="ConfigEntry.Type"/> is not nullable here.
    /// </remarks>
    private static Config CopyOutConfig(IntPtr metadataAndConfig)
    {
        IntPtr config = NativeMethods.TopicMetadataAndConfigConfig(metadataAndConfig);
        if (config == IntPtr.Zero)
        {
            // The header returns null only when the metadata is unavailable, which CopyOut
            // handled before calling here (the inner error was non-null), so this is a core
            // contract violation. Fault just this key (KeyedResultMarshal's per-key catch)
            // rather than inventing an empty configuration.
            throw new KafkaException(
                "The createTopics result carried neither a topic configuration nor a metadata error for a topic.");
        }

        return ConfigMarshal.CopyOut(config);
    }
}
