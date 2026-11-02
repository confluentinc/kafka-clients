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
using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A single configuration entry — Java's <c>org.apache.kafka.clients.admin.ConfigEntry</c>.
/// </summary>
/// <remarks>
/// <see cref="ConfigSynonym"/>'s constructor is not public, as Java's is package-private, so a
/// caller passes an empty synonym list, as a Java caller outside the package must.
/// </remarks>
public sealed class ConfigEntry
{
    private readonly IReadOnlyList<ConfigSynonym> _synonyms;

    /// <summary>
    /// Initializes a configuration entry with a name and value — Java's public
    /// <c>ConfigEntry(String name, String value)</c>, which likewise defaults the source and type
    /// to <c>UNKNOWN</c>, the synonyms to empty and the documentation to null.
    /// </summary>
    /// <param name="name">The configuration key.</param>
    /// <param name="value">The configuration value, or <see langword="null"/> (Java's <c>value()</c> is nullable).</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public ConfigEntry(string name, string? value)
        : this(
            name,
            value,
            ConfigSource.Unknown,
            isSensitive: false,
            isReadOnly: false,
            synonyms: Array.Empty<ConfigSynonym>(),
            type: ConfigType.Unknown,
            documentation: null)
    {
    }

    /// <summary>
    /// Initializes a complete entry — Java's public 8-argument
    /// <c>ConfigEntry(String name, String value, ConfigSource source, boolean isSensitive,
    /// boolean isReadOnly, List&lt;ConfigSynonym&gt; synonyms, ConfigType type,
    /// String documentation)</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="IsDefault"/> is not a parameter, as in Java: it derives from
    /// <paramref name="source"/> (<see cref="ConfigSource.DefaultConfig"/> → true). Recorded
    /// deviations: a null <paramref name="synonyms"/> or a null synonym element is rejected, where
    /// Java stores it, because <see cref="Synonyms"/> is non-nullable and
    /// <see cref="Equals(object?)"/>, <see cref="GetHashCode"/> and <see cref="ToString"/> read every
    /// element; and the list is copied, where Java stores the caller's list, so the entry's equality
    /// and hash cannot change after it is built.
    /// </remarks>
    /// <param name="name">The configuration key.</param>
    /// <param name="value">The configuration value, or <see langword="null"/>.</param>
    /// <param name="source">Where the value came from.</param>
    /// <param name="isSensitive">
    /// Whether the value is sensitive, in which case the broker never returns it.
    /// </param>
    /// <param name="isReadOnly">Whether the entry is read-only and cannot be updated.</param>
    /// <param name="synonyms">The synonyms, in Java's order of precedence.</param>
    /// <param name="type">
    /// The value's data type, or <see langword="null"/> for Java's null type, which the entries of a
    /// <c>createTopics</c> result carry.
    /// </param>
    /// <param name="documentation">The documentation, or <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="synonyms"/> is null, or
    /// <paramref name="synonyms"/> contains a null element.
    /// </exception>
    public ConfigEntry(
        string name,
        string? value,
        ConfigSource source,
        bool isSensitive,
        bool isReadOnly,
        IReadOnlyList<ConfigSynonym> synonyms,
        ConfigType? type,
        string? documentation)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Value = value;
        Source = source;
        IsSensitive = isSensitive;
        IsReadOnly = isReadOnly;
        _synonyms = CopySynonyms(synonyms);
        Type = type;
        Documentation = documentation;
    }

    /// <summary>The configuration key (Java's <c>name()</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// The configuration value (Java's <c>value()</c>), or <see langword="null"/>.
    /// </summary>
    public string? Value { get; }

    /// <summary>Where this value came from (Java's <c>source()</c>).</summary>
    public ConfigSource Source { get; }

    /// <summary>
    /// Whether the value is the broker default (Java's <c>isDefault()</c>), derived from
    /// <see cref="Source"/> as Java derives it.
    /// </summary>
    public bool IsDefault => Source == ConfigSource.DefaultConfig;

    /// <summary>
    /// Whether the value is sensitive, in which case the broker redacts it (Java's
    /// <c>isSensitive()</c>).
    /// </summary>
    public bool IsSensitive { get; }

    /// <summary>Whether the entry cannot be changed (Java's <c>isReadOnly()</c>).</summary>
    public bool IsReadOnly { get; }

    /// <summary>
    /// Every value that may be used for this config, with its source, in order of precedence —
    /// Java's <c>synonyms()</c>. Empty when synonyms were not requested through
    /// <see cref="DescribeConfigsOptions.IncludeSynonyms"/>.
    /// </summary>
    public IReadOnlyList<ConfigSynonym> Synonyms => _synonyms;

    /// <summary>
    /// The value's data type (Java's <c>type()</c>), or <see langword="null"/> for the entries of a
    /// <c>createTopics</c> result, which Java builds with a null type.
    /// </summary>
    public ConfigType? Type { get; }

    /// <summary>
    /// The documentation for this config, or <see langword="null"/> when the broker did
    /// not report it (Java's <c>documentation()</c>).
    /// </summary>
    public string? Documentation { get; }

    /// <summary>
    /// Value equality over every field Java compares — Java's <c>equals</c>, which leaves out
    /// <see cref="IsDefault"/>, derived from the compared <see cref="Source"/>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two entries carry the same configuration.</returns>
    public override bool Equals(object? obj)
    {
        if (obj is not ConfigEntry other)
        {
            return false;
        }

        return string.Equals(Name, other.Name, StringComparison.Ordinal)
            && string.Equals(Value, other.Value, StringComparison.Ordinal)
            && IsSensitive == other.IsSensitive
            && IsReadOnly == other.IsReadOnly
            && Source == other.Source
            && Type == other.Type
            && string.Equals(Documentation, other.Documentation, StringComparison.Ordinal)
            && SynonymsEqual(_synonyms, other._synonyms);
    }

    /// <summary>
    /// The hash of every field <see cref="Equals(object?)"/> compares — Java's <c>hashCode</c>, same
    /// fields and same <c>31 *</c> chain.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Name);
            hash = (hash * 31) + (Value is null ? 0 : StringComparer.Ordinal.GetHashCode(Value));
            hash = (hash * 31) + (IsSensitive ? 1 : 0);
            hash = (hash * 31) + (IsReadOnly ? 1 : 0);
            hash = (hash * 31) + Source.GetHashCode();
            hash = (hash * 31) + Type.GetHashCode();
            hash = (hash * 31) + (Documentation is null ? 0 : StringComparer.Ordinal.GetHashCode(Documentation));
            foreach (ConfigSynonym synonym in _synonyms)
            {
                hash = (hash * 31) + synonym.GetHashCode();
            }

            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>, which redacts a sensitive value
    /// rather than printing it.
    /// </summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// <see cref="Source"/> and <see cref="Type"/> print as Java's constant names
    /// (<c>DEFAULT_CONFIG</c>, <c>STRING</c>), the two booleans lowercase, and a
    /// <see langword="null"/> value, type or documentation as <c>null</c>. A source or type that is not a
    /// defined member prints as its numeric value.
    /// </remarks>
    public override string ToString()
    {
        StringBuilder synonyms = new StringBuilder("[");
        for (int index = 0; index < _synonyms.Count; index++)
        {
            if (index > 0)
            {
                synonyms.Append(", ");
            }

            synonyms.Append(_synonyms[index].ToString());
        }

        synonyms.Append(']');

        return string.Format(
            CultureInfo.InvariantCulture,
            "ConfigEntry(name={0}, value={1}, source={2}, isSensitive={3}, isReadOnly={4}, synonyms={5}, "
                + "type={6}, documentation={7})",
            Name,
            IsSensitive ? "Redacted" : (Value ?? "null"),
            JavaName(Source),
            IsSensitive ? "true" : "false",
            IsReadOnly ? "true" : "false",
            synonyms,
            Type is null ? "null" : JavaName(Type.Value),
            Documentation ?? "null");
    }

    private static string JavaName(ConfigSource source) => source switch
    {
        ConfigSource.DynamicTopicConfig => "DYNAMIC_TOPIC_CONFIG",
        ConfigSource.DynamicBrokerLoggerConfig => "DYNAMIC_BROKER_LOGGER_CONFIG",
        ConfigSource.DynamicBrokerConfig => "DYNAMIC_BROKER_CONFIG",
        ConfigSource.DynamicDefaultBrokerConfig => "DYNAMIC_DEFAULT_BROKER_CONFIG",
        ConfigSource.DynamicClientMetricsConfig => "DYNAMIC_CLIENT_METRICS_CONFIG",
        ConfigSource.DynamicGroupConfig => "DYNAMIC_GROUP_CONFIG",
        ConfigSource.StaticBrokerConfig => "STATIC_BROKER_CONFIG",
        ConfigSource.DefaultConfig => "DEFAULT_CONFIG",
        ConfigSource.Unknown => "UNKNOWN",
        _ => ((int)source).ToString(CultureInfo.InvariantCulture),
    };

    private static string JavaName(ConfigType type) => type switch
    {
        ConfigType.Unknown => "UNKNOWN",
        ConfigType.Boolean => "BOOLEAN",
        ConfigType.String => "STRING",
        ConfigType.Int => "INT",
        ConfigType.Short => "SHORT",
        ConfigType.Long => "LONG",
        ConfigType.Double => "DOUBLE",
        ConfigType.List => "LIST",
        ConfigType.Class => "CLASS",
        ConfigType.Password => "PASSWORD",
        _ => ((int)type).ToString(CultureInfo.InvariantCulture),
    };

    private static IReadOnlyList<ConfigSynonym> CopySynonyms(IReadOnlyList<ConfigSynonym> synonyms)
    {
        if (synonyms is null)
        {
            throw new ArgumentNullException(nameof(synonyms));
        }

        int count = synonyms.Count;
        if (count == 0)
        {
            return Array.Empty<ConfigSynonym>();
        }

        ConfigSynonym[] copy = new ConfigSynonym[count];
        for (int index = 0; index < count; index++)
        {
            copy[index] = synonyms[index]
                ?? throw new ArgumentNullException(
                    nameof(synonyms), "Config synonyms must not contain a null element.");
        }

        return new ReadOnlyCollection<ConfigSynonym>(copy);
    }

    private static bool SynonymsEqual(IReadOnlyList<ConfigSynonym> left, IReadOnlyList<ConfigSynonym> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        for (int index = 0; index < left.Count; index++)
        {
            if (!left[index].Equals(right[index]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The data type of a configuration entry — Java's nested <c>ConfigEntry.ConfigType</c>.
    /// </summary>
    /// <remarks>
    /// Java's enum has no numeric id, so the member values are C# ordinals with no wire meaning.
    /// </remarks>
    public enum ConfigType
    {
        /// <summary>Java's <c>UNKNOWN</c> — also where an unrecognised name lands.</summary>
        Unknown,

        /// <summary>Java's <c>BOOLEAN</c>.</summary>
        Boolean,

        /// <summary>Java's <c>STRING</c>.</summary>
        String,

        /// <summary>Java's <c>INT</c>.</summary>
        Int,

        /// <summary>Java's <c>SHORT</c>.</summary>
        Short,

        /// <summary>Java's <c>LONG</c>.</summary>
        Long,

        /// <summary>Java's <c>DOUBLE</c>.</summary>
        Double,

        /// <summary>Java's <c>LIST</c>.</summary>
        List,

        /// <summary>Java's <c>CLASS</c>.</summary>
        Class,

        /// <summary>Java's <c>PASSWORD</c>.</summary>
        Password,
    }

    /// <summary>
    /// Where a configuration entry's value came from — Java's nested
    /// <c>ConfigEntry.ConfigSource</c>.
    /// </summary>
    /// <remarks>
    /// <inheritdoc cref="ConfigType" path="/remarks"/>
    /// </remarks>
    public enum ConfigSource
    {
        /// <summary>Java's <c>DYNAMIC_TOPIC_CONFIG</c> — set for a specific topic.</summary>
        DynamicTopicConfig,

        /// <summary>Java's <c>DYNAMIC_BROKER_LOGGER_CONFIG</c> — set for a specific broker's logger.</summary>
        DynamicBrokerLoggerConfig,

        /// <summary>Java's <c>DYNAMIC_BROKER_CONFIG</c> — set for a specific broker.</summary>
        DynamicBrokerConfig,

        /// <summary>Java's <c>DYNAMIC_DEFAULT_BROKER_CONFIG</c> — the cluster-wide broker default.</summary>
        DynamicDefaultBrokerConfig,

        /// <summary>Java's <c>DYNAMIC_CLIENT_METRICS_CONFIG</c> — a client-metrics subscription config.</summary>
        DynamicClientMetricsConfig,

        /// <summary>Java's <c>DYNAMIC_GROUP_CONFIG</c> — set for a specific group.</summary>
        DynamicGroupConfig,

        /// <summary>Java's <c>STATIC_BROKER_CONFIG</c> — supplied as broker properties at start-up.</summary>
        StaticBrokerConfig,

        /// <summary>
        /// Java's <c>DEFAULT_CONFIG</c> — the built-in default. This is the one member
        /// <see cref="ConfigEntry.IsDefault"/> tests for.
        /// </summary>
        DefaultConfig,

        /// <summary>Java's <c>UNKNOWN</c> — also where an unrecognised name lands.</summary>
        Unknown,
    }

    /// <summary>
    /// One alternative value that may be used for a configuration, with its source — Java's nested
    /// <c>ConfigEntry.ConfigSynonym</c>.
    /// </summary>
    public sealed class ConfigSynonym
    {
        internal ConfigSynonym(string name, string? value, ConfigSource source)
        {
            Name = name;
            Value = value;
            Source = source;
        }

        /// <summary>The configuration name (Java's <c>name()</c>).</summary>
        public string Name { get; }

        /// <summary>
        /// The value, or <see langword="null"/> when the configuration is sensitive — Java's
        /// <c>value()</c>.
        /// </summary>
        public string? Value { get; }

        /// <summary>Where this value came from (Java's <c>source()</c>).</summary>
        public ConfigSource Source { get; }

        /// <summary>Value equality — Java's <c>equals</c>.</summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns>Whether the two synonyms are the same.</returns>
        public override bool Equals(object? obj) =>
            obj is ConfigSynonym other
            && string.Equals(Name, other.Name, StringComparison.Ordinal)
            && string.Equals(Value, other.Value, StringComparison.Ordinal)
            && Source == other.Source;

        /// <summary>The hash of the three fields — Java's <c>hashCode</c>.</summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = (hash * 31) + (Name is null ? 0 : StringComparer.Ordinal.GetHashCode(Name));
                hash = (hash * 31) + (Value is null ? 0 : StringComparer.Ordinal.GetHashCode(Value));
                hash = (hash * 31) + Source.GetHashCode();
                return hash;
            }
        }

        /// <summary>
        /// A diagnostic rendering matching Java's <c>toString()</c>: the source prints as Java's
        /// constant name (<c>STATIC_BROKER_CONFIG</c>) and a <see langword="null"/> value as
        /// <c>null</c>, as <see cref="ConfigEntry.ToString"/> does; an undefined source prints as its
        /// number.
        /// </summary>
        /// <returns>The rendering.</returns>
        public override string ToString() =>
            string.Format(
                CultureInfo.InvariantCulture,
                "ConfigSynonym(name={0}, value={1}, source={2})",
                Name,
                Value ?? "null",
                JavaName(Source));
    }
}
