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
/// Copies a borrowed <c>kafka_admin_Config_t</c> — and the <c>ConfigEntry</c> / synonym
/// tree hanging off it — into fully owned managed objects, so nothing survives the
/// <c>DescribeConfigsResult_destroy</c> that follows the walk (ffi §B2 Category 4 / §B4).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Three levels of borrowed pointers under one root, all invalidated together.</b>
/// The result root owns the <c>Config_t</c>, which owns each <c>ConfigEntry_t</c>, which
/// owns its synonym strings; destroying the root invalidates every one of them. So the
/// copy-out is eager and total: each string is materialised with
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> at the moment it is read, and the
/// managed types carry no <see cref="IntPtr"/>, no handle and no deferred accessor.
/// Everything here is the NUL-terminated, callee-owned string form (ffi §B3 row 2).
/// </para>
/// <para>
/// ⚠ <b>Two nullable strings must round-trip as <see langword="null"/>, not as
/// <c>""</c>.</b> <c>ConfigEntry_value</c> is null for a sensitive config, and
/// <c>ConfigEntry_documentation</c> is null when the broker did not report it — the header
/// says both. <c>synonym_value</c> is null for the same reason.
/// </para>
/// <para>
/// ⚠ <b>The enum decode here is by NAME, which is the opposite of every other enum in this
/// phase.</b> <c>ConfigEntry_source</c> and <c>ConfigEntry_type</c> return Java's enum
/// constant name — the header: "<c>ConfigEntry.ConfigSource</c> has no numeric id in Java,
/// so the name is the contract" — while <see cref="ConfigResourceType"/> and
/// <see cref="AlterConfigOpType"/> cross as <c>int32_t</c> ids. See
/// <see cref="SourceFromName"/>.
/// </para>
/// </remarks>
internal static class ConfigMarshal
{
    /// <summary>
    /// Copies one borrowed <c>Config_t</c> out, entries and all.
    /// </summary>
    /// <param name="config">
    /// The borrowed <c>get_value(i)</c> pointer. Valid only until the result root is
    /// destroyed.
    /// </param>
    /// <returns>The owned configuration.</returns>
    /// <exception cref="KafkaException">
    /// The ABI produced no config for a key whose <c>get_error</c> was null — unreachable
    /// in practice, since the header ties a null value to a non-null error, and the walker
    /// checks the error first.
    /// </exception>
    internal static Config CopyOut(IntPtr config)
    {
        if (config == IntPtr.Zero)
        {
            throw new KafkaException(
                "The describeConfigs result carried neither a configuration nor an error for a resource.");
        }

        int count = NativeMethods.ConfigEntryCount(config);
        List<ConfigEntry> entries = new List<ConfigEntry>(Math.Max(count, 0));
        for (int index = 0; index < count; index++)
        {
            IntPtr entry = NativeMethods.ConfigGetEntry(config, index);
            if (entry == IntPtr.Zero)
            {
                // Guarded by `entry_count`, so unreachable; skipping is the safe reading.
                continue;
            }

            entries.Add(CopyOutEntry(entry));
        }

        return new Config(entries);
    }

    /// <summary>
    /// Copies one borrowed <c>ConfigEntry_t</c> out, synonyms and all.
    /// </summary>
    /// <param name="entry">The borrowed entry pointer.</param>
    /// <returns>The owned entry.</returns>
    internal static ConfigEntry CopyOutEntry(IntPtr entry)
    {
        // Name is always present; a defensive null normalizes to empty so the
        // non-nullable annotation on ConfigEntry.Name holds.
        string name = Utf8Marshal.PtrToString(NativeMethods.ConfigEntryName(entry)) ?? string.Empty;

        // ⚠ Nullable, and the null is meaningful: a sensitive config's value is null.
        string? value = Utf8Marshal.PtrToString(NativeMethods.ConfigEntryValue(entry));

        ConfigEntry.ConfigSource source = SourceFromName(
            Utf8Marshal.PtrToString(NativeMethods.ConfigEntrySource(entry)));

        bool isSensitive = NativeMethods.ConfigEntryIsSensitive(entry);
        bool isReadOnly = NativeMethods.ConfigEntryIsReadOnly(entry);

        ConfigEntry.ConfigType type = TypeFromName(
            Utf8Marshal.PtrToString(NativeMethods.ConfigEntryType(entry)));

        // ⚠ Nullable, and the null is meaningful: the broker did not report documentation.
        string? documentation = Utf8Marshal.PtrToString(NativeMethods.ConfigEntryDocumentation(entry));

        int synonymCount = NativeMethods.ConfigEntrySynonymCount(entry);
        List<ConfigEntry.ConfigSynonym> synonyms =
            new List<ConfigEntry.ConfigSynonym>(Math.Max(synonymCount, 0));
        for (int index = 0; index < synonymCount; index++)
        {
            // ⚠ Appended in the ABI's delivery order and never sorted: the header states
            // that synonyms "keep Java's precedence order", which is why Java's accessor
            // returns a List. A null synonym value inside the bound is a genuine null.
            synonyms.Add(new ConfigEntry.ConfigSynonym(
                Utf8Marshal.PtrToString(NativeMethods.ConfigEntrySynonymName(entry, index)) ?? string.Empty,
                Utf8Marshal.PtrToString(NativeMethods.ConfigEntrySynonymValue(entry, index)),
                SourceFromName(Utf8Marshal.PtrToString(NativeMethods.ConfigEntrySynonymSource(entry, index)))));
        }

        // ⚠ NOT the flag-taking constructor: `is_default` is derived from the source, so
        // reading `ConfigEntry_is_default` here would introduce a second, independent
        // answer to the same question. The ABI's own flag is left unread for exactly that
        // reason — Java derives it too (ConfigEntry.java:102-104).
        return new ConfigEntry(name, value, source, isSensitive, isReadOnly, synonyms, type, documentation);
    }

    /// <summary>
    /// Java's <c>ConfigEntry.ConfigSource.valueOf</c>, degrading rather than throwing.
    /// </summary>
    /// <param name="name">The enum constant name the ABI reported, or <see langword="null"/>.</param>
    /// <returns>
    /// The matching member, or <see cref="ConfigEntry.ConfigSource.Unknown"/> for a name
    /// this client does not recognise.
    /// </returns>
    /// <remarks>
    /// ⚠ <b>An unrecognised name degrades to <c>UNKNOWN</c>; it must not throw.</b> A
    /// broker introducing a new source would otherwise fail the entire describe rather than
    /// reporting the configs it did understand — and Java's own <c>UNKNOWN</c> member
    /// (<c>ConfigEntry.java:224</c>) exists for exactly this. The mapping is written out
    /// rather than delegated to <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/>
    /// because the C# member names are PascalCase and Java's are SCREAMING_SNAKE: a parse
    /// would match by accident at best, and silently differently as members are added.
    /// </remarks>
    internal static ConfigEntry.ConfigSource SourceFromName(string? name) => name switch
    {
        "DYNAMIC_TOPIC_CONFIG" => ConfigEntry.ConfigSource.DynamicTopicConfig,
        "DYNAMIC_BROKER_LOGGER_CONFIG" => ConfigEntry.ConfigSource.DynamicBrokerLoggerConfig,
        "DYNAMIC_BROKER_CONFIG" => ConfigEntry.ConfigSource.DynamicBrokerConfig,
        "DYNAMIC_DEFAULT_BROKER_CONFIG" => ConfigEntry.ConfigSource.DynamicDefaultBrokerConfig,
        "DYNAMIC_CLIENT_METRICS_CONFIG" => ConfigEntry.ConfigSource.DynamicClientMetricsConfig,
        "DYNAMIC_GROUP_CONFIG" => ConfigEntry.ConfigSource.DynamicGroupConfig,
        "STATIC_BROKER_CONFIG" => ConfigEntry.ConfigSource.StaticBrokerConfig,
        "DEFAULT_CONFIG" => ConfigEntry.ConfigSource.DefaultConfig,
        _ => ConfigEntry.ConfigSource.Unknown,
    };

    /// <summary>
    /// Java's <c>ConfigEntry.ConfigType.valueOf</c>, degrading rather than throwing.
    /// </summary>
    /// <param name="name">The enum constant name the ABI reported, or <see langword="null"/>.</param>
    /// <returns>
    /// The matching member, or <see cref="ConfigEntry.ConfigType.Unknown"/> for a name this
    /// client does not recognise.
    /// </returns>
    /// <remarks>
    /// <inheritdoc cref="SourceFromName" path="/remarks"/>
    /// </remarks>
    internal static ConfigEntry.ConfigType TypeFromName(string? name) => name switch
    {
        "BOOLEAN" => ConfigEntry.ConfigType.Boolean,
        "STRING" => ConfigEntry.ConfigType.String,
        "INT" => ConfigEntry.ConfigType.Int,
        "SHORT" => ConfigEntry.ConfigType.Short,
        "LONG" => ConfigEntry.ConfigType.Long,
        "DOUBLE" => ConfigEntry.ConfigType.Double,
        "LIST" => ConfigEntry.ConfigType.List,
        "CLASS" => ConfigEntry.ConfigType.Class,
        "PASSWORD" => ConfigEntry.ConfigType.Password,
        _ => ConfigEntry.ConfigType.Unknown,
    };
}
