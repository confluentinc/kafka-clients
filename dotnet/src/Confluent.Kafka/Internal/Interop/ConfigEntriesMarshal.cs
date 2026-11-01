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
/// Copies a <c>kafka_admin_Config_t</c> into a <see cref="Config"/> through its owned <c>entries()</c> and
/// <c>synonyms()</c> lists, which it frees; the <c>Config_t</c> itself stays with its owner.
/// </summary>
internal static class ConfigEntriesMarshal
{
    internal static Config CopyOut(IntPtr config)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ConfigEntries(config, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<ConfigEntry> entries = new List<ConfigEntry>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr entry = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                entries.Add(CopyOutEntry(entry));
            }

            return new Config(entries);
        }
        finally
        {
            DestroyList(list);
        }
    }

    private static ConfigEntry CopyOutEntry(IntPtr entry)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr name = NativeMethods.ConfigEntryName(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr value = NativeMethods.ConfigEntryValue(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr source = NativeMethods.ConfigEntrySource(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte isSensitive = NativeMethods.ConfigEntryIsSensitive(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte isReadOnly = NativeMethods.ConfigEntryIsReadOnly(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr type = NativeMethods.ConfigEntryType(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr documentation = NativeMethods.ConfigEntryDocumentation(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ConfigEntry(
            Utf8Marshal.PtrToString(name) ?? string.Empty,
            Utf8Marshal.PtrToString(value),
            SourceOf(source),
            isSensitive != 0,
            isReadOnly != 0,
            CopyOutSynonyms(entry),
            TypeOf(type),
            Utf8Marshal.PtrToString(documentation));
    }

    private static IReadOnlyList<ConfigEntry.ConfigSynonym> CopyOutSynonyms(IntPtr entry)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ConfigEntrySynonyms(entry, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<ConfigEntry.ConfigSynonym> synonyms = new List<ConfigEntry.ConfigSynonym>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr synonym = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr name = NativeMethods.ConfigSynonymName(synonym, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr value = NativeMethods.ConfigSynonymValue(synonym, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr source = NativeMethods.ConfigSynonymSource(synonym, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                synonyms.Add(new ConfigEntry.ConfigSynonym(
                    Utf8Marshal.PtrToString(name) ?? string.Empty, Utf8Marshal.PtrToString(value), SourceOf(source)));
            }

            return synonyms;
        }
        finally
        {
            DestroyList(list);
        }
    }

    private static ConfigEntry.ConfigSource SourceOf(IntPtr source)
    {
        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.ConfigSourceEnum(source, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return id switch
        {
            0 => ConfigEntry.ConfigSource.DynamicTopicConfig,
            1 => ConfigEntry.ConfigSource.DynamicBrokerLoggerConfig,
            2 => ConfigEntry.ConfigSource.DynamicBrokerConfig,
            3 => ConfigEntry.ConfigSource.DynamicDefaultBrokerConfig,
            4 => ConfigEntry.ConfigSource.DynamicClientMetricsConfig,
            5 => ConfigEntry.ConfigSource.DynamicGroupConfig,
            6 => ConfigEntry.ConfigSource.StaticBrokerConfig,
            7 => ConfigEntry.ConfigSource.DefaultConfig,
            _ => ConfigEntry.ConfigSource.Unknown,
        };
    }

    private static ConfigEntry.ConfigType? TypeOf(IntPtr type)
    {
        if (type == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.ConfigTypeEnum(type, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return id switch
        {
            1 => ConfigEntry.ConfigType.Boolean,
            2 => ConfigEntry.ConfigType.String,
            3 => ConfigEntry.ConfigType.Int,
            4 => ConfigEntry.ConfigType.Short,
            5 => ConfigEntry.ConfigType.Long,
            6 => ConfigEntry.ConfigType.Double,
            7 => ConfigEntry.ConfigType.List,
            8 => ConfigEntry.ConfigType.Class,
            9 => ConfigEntry.ConfigType.Password,
            _ => ConfigEntry.ConfigType.Unknown,
        };
    }

    private static void DestroyList(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
