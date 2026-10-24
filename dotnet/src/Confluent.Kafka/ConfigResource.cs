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
using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// A resource that has configuration — Java's
/// <c>org.apache.kafka.common.config.ConfigResource</c>.
/// </summary>
/// <remarks>
/// This type keys the config RPCs' results, so its equality decides every lookup in them.
/// </remarks>
public sealed class ConfigResource
{
    /// <summary>Creates a config resource — Java's constructor.</summary>
    /// <param name="type">
    /// The resource type. A value that is not a defined <see cref="ConfigResourceType"/>
    /// member is stored as <see cref="ConfigResourceType.Unknown"/>, as Java's
    /// <c>Type.forId</c> maps an id it has no member for, so a request and its answer key the
    /// same resource.
    /// </param>
    /// <param name="name">
    /// The resource name; empty means "the default resource of this type"
    /// (see <see cref="IsDefault"/>).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public ConfigResource(ConfigResourceType type, string name)
    {
        Type = DefinedOrUnknown(type);
        Name = name ?? throw new ArgumentNullException(nameof(name));
    }

    /// <summary>The resource type — Java's <c>type()</c>.</summary>
    public ConfigResourceType Type { get; }

    /// <summary>The resource name — Java's <c>name()</c>.</summary>
    public string Name { get; }

    /// <summary>
    /// Whether this is the default resource of its type, that is whether <see cref="Name"/> is
    /// empty — Java's <c>isDefault()</c>.
    /// </summary>
    public bool IsDefault => Name.Length == 0;

    /// <summary>
    /// Value equality over <see cref="Type"/> and <see cref="Name"/>, the name compared
    /// ordinally — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same resource.</returns>
    public override bool Equals(object? obj) =>
        obj is ConfigResource other
        && Type == other.Type
        && string.Equals(Name, other.Name, StringComparison.Ordinal);

    /// <summary>The hash of <see cref="Type"/> and <see cref="Name"/> — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (((int)Type).GetHashCode() * 31) + StringComparer.Ordinal.GetHashCode(Name);
        }
    }

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> format, the type printed as Java's
    /// constant name: <c>ConfigResource(type=TOPIC, name='t')</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "ConfigResource(type={0}, name='{1}')", JavaName(Type), Name);

    private static ConfigResourceType DefinedOrUnknown(ConfigResourceType type) =>
        Enum.IsDefined(typeof(ConfigResourceType), type) ? type : ConfigResourceType.Unknown;

    private static string JavaName(ConfigResourceType type) => type switch
    {
        ConfigResourceType.Unknown => "UNKNOWN",
        ConfigResourceType.Topic => "TOPIC",
        ConfigResourceType.Broker => "BROKER",
        ConfigResourceType.BrokerLogger => "BROKER_LOGGER",
        ConfigResourceType.ClientMetrics => "CLIENT_METRICS",
        ConfigResourceType.Group => "GROUP",
        _ => ((int)type).ToString(CultureInfo.InvariantCulture),
    };
}
