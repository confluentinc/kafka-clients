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
using System.Globalization;
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A single configuration entry — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ConfigEntry</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>M15/P1 shipped a flattened subset; M15/P3 Stage 2 completed the accessors.</b> The
/// <c>createTopics</c> result exposes its topic configuration through a flattened accessor
/// family (<c>config_name</c> / <c>config_value</c> / <c>config_is_default</c> /
/// <c>config_is_sensitive</c> / <c>config_is_read_only</c>), which is why P1 could carry
/// only those five. <c>describeConfigs</c>' <c>kafka_admin_ConfigEntry_t</c> exposes the
/// rest, so <see cref="Source"/>, <see cref="Type"/>, <see cref="Documentation"/> and
/// <see cref="Synonyms"/> now land, matching Java's <c>source()</c> (<c>:95</c>),
/// <c>type()</c> (<c>:133</c>), <c>documentation()</c> (<c>:140</c>) and
/// <c>synonyms()</c> (<c>:126</c>).
/// </para>
/// <para>
/// ⚠ <b><see cref="IsDefault"/> is where that growth was not merely additive — and P1's
/// recorded plan for it has now been carried out.</b> In Java <c>isDefault()</c> is
/// <em>derived</em> — <c>return source == ConfigSource.DEFAULT_CONFIG</c>
/// (<c>ConfigEntry.java:102-104</c>) — and neither public Java constructor takes it. Over
/// P1's flattened ABI there was no source to derive from, so the flag was stored and the
/// flag-taking constructor was made <see langword="internal"/> to stop a caller building
/// an entry whose <see cref="IsDefault"/> and <see cref="Source"/> disagree, a state Java
/// cannot represent. P1 recorded that "when <c>Source</c> lands it becomes the source of
/// truth and <see cref="IsDefault"/> derives from it, exactly as in Java" — that is what
/// happens here. The internal flag-taking constructor survives for the flattened
/// <c>createTopics</c> path and now translates its flag into a
/// <see cref="ConfigSource"/>, so the two can no longer disagree by construction.
/// </para>
/// <para>
/// ⚠ <b>Recorded gap (<c>definition-of-done.md</c> §2), raised rather than decided.</b>
/// Java has a second <b>public</b> constructor — the 8-argument
/// <c>ConfigEntry(name, value, source, isSensitive, isReadOnly, synonyms, type,
/// documentation)</c> (<c>ConfigEntry.java:59</c>) — which today's ABI would finally make
/// expressible. It is <b>not</b> published here, because
/// <c>PublicAdminShapeParityTests.ConfigEntry_PublishesOnlyTheConstructorJavaHas</c>
/// asserts exactly one public constructor and that assertion is a ratified P1 boundary
/// condition. Publishing the overload is a public-surface decision for the maintainer, not
/// an Actor's to take mid-stage; the equivalent constructor is
/// <see langword="internal"/> so the result marshaller can build a complete entry. Nothing
/// in Stage 2's input path needs the public form — <c>incrementalAlterConfigs</c> sends
/// only <c>(name, value, opType)</c>, which the shipped 2-argument constructor covers, as
/// Java's own <c>AlterConfigOp</c> javadoc example shows.
/// </para>
/// <para>
/// <b>Value equality mirrors Java's</b> (<c>:144</c>, <c>:163</c>) and is <em>not</em>
/// decoration: <see cref="AlterConfigOp"/>'s equality compares its entry with Java's
/// <c>Objects.equals</c>, so without these overrides two operations Java calls equal
/// would differ here by reference. ⚠ Java's <c>equals</c> deliberately does <b>not</b>
/// include <c>isDefault</c> — it is derived from <see cref="Source"/>, which is compared.
/// </para>
/// </remarks>
public sealed class ConfigEntry
{
    private readonly IReadOnlyList<ConfigSynonym> _synonyms;

    /// <summary>
    /// Initializes a configuration entry with a name and value — Java's public
    /// <c>ConfigEntry(String name, String value)</c> (<c>:44</c>), which likewise defaults
    /// the source and type to <c>UNKNOWN</c>, the synonyms to empty and the documentation
    /// to null.
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
    /// Initializes an entry from the <b>flattened</b> <c>createTopics</c> accessors, whose
    /// only source signal is an <c>is_default</c> flag.
    /// </summary>
    /// <remarks>
    /// <b>Deliberately <see langword="internal"/>, and it has no Java counterpart</b>
    /// (<c>definition-of-done.md</c> §7): Java's public constructors take <c>source</c>,
    /// never <c>isDefault</c> — see the type remarks for why publishing this one would
    /// create a state Java cannot represent. It now translates the flag into a
    /// <see cref="ConfigSource"/> rather than storing it, so
    /// <see cref="IsDefault"/> derives from <see cref="Source"/> on every path.
    /// <c>false</c> becomes <see cref="ConfigSource.Unknown"/> — Java's own default for
    /// "source not set" (<c>:45</c>) — because the flattened accessors report no source.
    /// </remarks>
    /// <param name="name">The configuration key.</param>
    /// <param name="value">The configuration value, or <see langword="null"/>.</param>
    /// <param name="isDefault">Whether the value is the broker default (Java's <c>isDefault()</c>).</param>
    /// <param name="isSensitive">Whether the value is sensitive and therefore redacted (Java's <c>isSensitive()</c>).</param>
    /// <param name="isReadOnly">Whether the entry cannot be changed (Java's <c>isReadOnly()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    internal ConfigEntry(string name, string? value, bool isDefault, bool isSensitive, bool isReadOnly)
        : this(
            name,
            value,
            isDefault ? ConfigSource.DefaultConfig : ConfigSource.Unknown,
            isSensitive,
            isReadOnly,
            synonyms: Array.Empty<ConfigSynonym>(),
            type: ConfigType.Unknown,
            documentation: null)
    {
    }

    /// <summary>
    /// Initializes a complete entry — the shape of Java's 8-argument constructor
    /// (<c>:59</c>), <see langword="internal"/> here per the recorded gap in the type
    /// remarks.
    /// </summary>
    /// <param name="name">The configuration key.</param>
    /// <param name="value">The configuration value, or <see langword="null"/>.</param>
    /// <param name="source">Where the value came from.</param>
    /// <param name="isSensitive">Whether the value is sensitive and therefore redacted.</param>
    /// <param name="isReadOnly">Whether the entry cannot be changed.</param>
    /// <param name="synonyms">The synonyms, in Java's precedence order.</param>
    /// <param name="type">The value's data type.</param>
    /// <param name="documentation">The documentation, or <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="synonyms"/> is null.
    /// </exception>
    internal ConfigEntry(
        string name,
        string? value,
        ConfigSource source,
        bool isSensitive,
        bool isReadOnly,
        IReadOnlyList<ConfigSynonym> synonyms,
        ConfigType type,
        string? documentation)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Value = value;
        Source = source;
        IsSensitive = isSensitive;
        IsReadOnly = isReadOnly;
        _synonyms = synonyms ?? throw new ArgumentNullException(nameof(synonyms));
        Type = type;
        Documentation = documentation;
    }

    /// <summary>The configuration key (Java's <c>name()</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// The configuration value (Java's <c>value()</c>), or <see langword="null"/> —
    /// Java's accessor is nullable and the ABI reports a null value as a null pointer.
    /// </summary>
    public string? Value { get; }

    /// <summary>Where this value came from (Java's <c>source()</c>, <c>:95</c>).</summary>
    public ConfigSource Source { get; }

    /// <summary>
    /// Whether the value is the broker default (Java's <c>isDefault()</c>) — <b>derived</b>
    /// from <see cref="Source"/> exactly as Java derives it (<c>:102-104</c>), never
    /// stored.
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
    /// Every value that may be used for this config, with its source, <b>in order of
    /// precedence</b> — Java's <c>synonyms()</c> (<c>:126</c>). Empty when synonyms were
    /// not requested through <see cref="DescribeConfigsOptions.IncludeSynonyms"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ The order is the contract — it is why Java returns a <c>List</c> rather than a
    /// <c>Set</c>, and why the ABI states that synonyms "keep Java's precedence order and
    /// are not sorted". Nothing on this path sorts or de-duplicates them.
    /// </remarks>
    public IReadOnlyList<ConfigSynonym> Synonyms => _synonyms;

    /// <summary>The value's data type (Java's <c>type()</c>, <c>:133</c>).</summary>
    public ConfigType Type { get; }

    /// <summary>
    /// The documentation for this config, or <see langword="null"/> when the broker did
    /// not report it (Java's <c>documentation()</c>, <c>:140</c>).
    /// </summary>
    public string? Documentation { get; }

    /// <summary>
    /// Value equality over every field Java compares — Java's <c>equals</c> (<c>:144</c>).
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
    /// The hash of every field <see cref="Equals(object?)"/> compares — Java's
    /// <c>hashCode</c> (<c>:163</c>), same fields and same <c>31 *</c> chain.
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
    /// A diagnostic rendering matching Java's <c>toString()</c> (<c>:182</c>), which
    /// <b>redacts a sensitive value</b> rather than printing it.
    /// </summary>
    /// <returns>The rendering.</returns>
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
            IsSensitive ? "Redacted" : Value,
            Source,
            IsSensitive,
            IsReadOnly,
            synonyms,
            Type,
            Documentation);
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
    /// The data type of a configuration entry — the .NET realization of Java's nested
    /// <c>ConfigEntry.ConfigType</c> (<c>ConfigEntry.java:199-210</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This enum crosses the ABI as Java's constant NAME, not as a numeric id</b> —
    /// the header says so outright: "<c>ConfigEntry.ConfigType</c> has no numeric id in
    /// Java, so the name is the contract". That is the opposite convention from
    /// <see cref="ConfigResourceType"/>, <see cref="AlterConfigOpType"/> and
    /// <see cref="AclOperation"/>, which all cross as <c>int32_t</c> wire codes. The member
    /// values here are therefore ordinary C# ordinals with no wire meaning; the decode is
    /// by name, in <c>ConfigMarshal</c>.
    /// <para>
    /// <b>Stays nested, matching Java (M15/P3 decision D16).</b> Unlike
    /// <c>ConfigResource.Type</c>, which had to be flattened because <c>Type</c> collides
    /// with <see cref="System.Type"/>, this name nests cleanly.
    /// </para>
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
    /// Where a configuration entry's value came from — the .NET realization of Java's
    /// nested <c>ConfigEntry.ConfigSource</c> (<c>ConfigEntry.java:215-225</c>).
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
    /// One alternative value that may be used for a configuration, with its source — the
    /// .NET realization of Java's nested <c>ConfigEntry.ConfigSynonym</c>
    /// (<c>ConfigEntry.java:230</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>There is no <c>ConfigSynonym_t</c> in the ABI.</b> Synonyms are read through
    /// three parallel indexed accessors on the parent entry
    /// (<c>synonym_name</c> / <c>synonym_value</c> / <c>synonym_source</c>), bounded by
    /// <c>synonym_count</c>, so this is a plain managed value assembled from them.
    /// </remarks>
    public sealed class ConfigSynonym
    {
        /// <summary>
        /// Initializes a synonym — Java's package-private
        /// <c>ConfigSynonym(String, String, ConfigSource)</c> (<c>:243</c>), so likewise
        /// not public here.
        /// </summary>
        /// <param name="name">The configuration name, which may differ from the parent entry's.</param>
        /// <param name="value">The value, or <see langword="null"/> when the configuration is sensitive.</param>
        /// <param name="source">Where this value came from.</param>
        internal ConfigSynonym(string name, string? value, ConfigSource source)
        {
            Name = name;
            Value = value;
            Source = source;
        }

        /// <summary>The configuration name (Java's <c>name()</c>, <c>:252</c>).</summary>
        public string Name { get; }

        /// <summary>
        /// The value, or <see langword="null"/> when the configuration is sensitive —
        /// Java's <c>value()</c> (<c>:259</c>), whose javadoc says so.
        /// </summary>
        public string? Value { get; }

        /// <summary>Where this value came from (Java's <c>source()</c>, <c>:266</c>).</summary>
        public ConfigSource Source { get; }

        /// <summary>Value equality — Java's <c>equals</c> (<c>:270</c>).</summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns>Whether the two synonyms are the same.</returns>
        public override bool Equals(object? obj) =>
            obj is ConfigSynonym other
            && string.Equals(Name, other.Name, StringComparison.Ordinal)
            && string.Equals(Value, other.Value, StringComparison.Ordinal)
            && Source == other.Source;

        /// <summary>The hash of the three fields — Java's <c>hashCode</c> (<c>:279</c>).</summary>
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

        /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:284</c>).</summary>
        /// <returns>The rendering.</returns>
        public override string ToString() =>
            string.Format(
                CultureInfo.InvariantCulture, "ConfigSynonym(name={0}, value={1}, source={2})", Name, Value, Source);
    }
}
