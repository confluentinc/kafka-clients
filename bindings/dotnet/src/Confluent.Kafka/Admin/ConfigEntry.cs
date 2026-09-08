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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A single configuration entry — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ConfigEntry</c>.
/// </summary>
/// <remarks>
/// <b>Clipped to today's ABI.</b> The <c>createTopics</c> result exposes its topic
/// configuration through a flattened accessor family
/// (<c>config_name</c> / <c>config_value</c> / <c>config_is_default</c> /
/// <c>config_is_sensitive</c> / <c>config_is_read_only</c>), so those five are what a
/// <see cref="ConfigEntry"/> can carry here. Java's <c>source()</c>, <c>type()</c>,
/// <c>documentation()</c> and <c>synonyms()</c> arrive with <c>describeConfigs</c>,
/// whose <c>kafka_admin_ConfigEntry_t</c> accessors do expose them; adding them later
/// is purely additive.
/// </remarks>
public sealed class ConfigEntry
{
    /// <summary>
    /// Initializes a configuration entry with a name and value — Java's public
    /// <c>ConfigEntry(String name, String value)</c>. The three flags default to
    /// <see langword="false"/>.
    /// </summary>
    /// <param name="name">The configuration key.</param>
    /// <param name="value">The configuration value, or <see langword="null"/> (Java's <c>value()</c> is nullable).</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public ConfigEntry(string name, string? value)
        : this(name, value, isDefault: false, isSensitive: false, isReadOnly: false)
    {
    }

    /// <summary>
    /// Initializes a configuration entry with its flags. Used by the result marshaller
    /// to reproduce what the broker reported.
    /// </summary>
    /// <param name="name">The configuration key.</param>
    /// <param name="value">The configuration value, or <see langword="null"/>.</param>
    /// <param name="isDefault">Whether the value is the broker default (Java's <c>isDefault()</c>).</param>
    /// <param name="isSensitive">Whether the value is sensitive and therefore redacted (Java's <c>isSensitive()</c>).</param>
    /// <param name="isReadOnly">Whether the entry cannot be changed (Java's <c>isReadOnly()</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public ConfigEntry(string name, string? value, bool isDefault, bool isSensitive, bool isReadOnly)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Value = value;
        IsDefault = isDefault;
        IsSensitive = isSensitive;
        IsReadOnly = isReadOnly;
    }

    /// <summary>The configuration key (Java's <c>name()</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// The configuration value (Java's <c>value()</c>), or <see langword="null"/> —
    /// Java's accessor is nullable and the ABI reports a null value as a null pointer.
    /// </summary>
    public string? Value { get; }

    /// <summary>Whether the value is the broker default (Java's <c>isDefault()</c>).</summary>
    public bool IsDefault { get; }

    /// <summary>
    /// Whether the value is sensitive, in which case the broker redacts it (Java's
    /// <c>isSensitive()</c>).
    /// </summary>
    public bool IsSensitive { get; }

    /// <summary>Whether the entry cannot be changed (Java's <c>isReadOnly()</c>).</summary>
    public bool IsReadOnly { get; }
}
