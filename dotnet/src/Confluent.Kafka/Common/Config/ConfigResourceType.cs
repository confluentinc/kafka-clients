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

namespace Confluent.Kafka.Common.Config;

/// <summary>
/// The kind of resource a <see cref="ConfigResource"/> names — Java's nested
/// <c>org.apache.kafka.common.config.ConfigResource.Type</c>.
/// </summary>
/// <remarks>
/// <para>
/// Each member's value is Java's <c>Type.id()</c>, which crosses the C ABI in both
/// directions, not an ordinal.
/// </para>
/// <para>
/// A recorded deviation: the type is flattened out of Java's nesting, since a nested
/// <c>Type</c> would collide with <see cref="System.Type"/> at every unqualified use.
/// </para>
/// <para>
/// The members are declared in Java's order, but <see cref="System.Enum.GetValues(System.Type)"/> orders them by
/// value (<c>UNKNOWN</c> first), not as Java's <c>values()</c> does (<c>GROUP</c> first), and
/// <see cref="System.Enum.CompareTo(object)"/> compares values, not Java's ordinals.
/// </para>
/// </remarks>
public enum ConfigResourceType
{
    /// <summary>A consumer group's configuration — Java's <c>GROUP</c> (id 32).</summary>
    GROUP = 32,

    /// <summary>
    /// A client-metrics subscription — Java's <c>CLIENT_METRICS</c> (id 16).
    /// </summary>
    CLIENT_METRICS = 16,

    /// <summary>A broker's logger levels — Java's <c>BROKER_LOGGER</c> (id 8).</summary>
    BROKER_LOGGER = 8,

    /// <summary>A broker's configuration — Java's <c>BROKER</c> (id 4).</summary>
    BROKER = 4,

    /// <summary>A topic's configuration — Java's <c>TOPIC</c> (id 2).</summary>
    TOPIC = 2,

    /// <summary>
    /// A resource type this client does not recognize — Java's <c>UNKNOWN</c> (id 0).
    /// </summary>
    UNKNOWN = 0,
}

/// <summary>
/// The members Java declares on the <see cref="ConfigResourceType"/> enum itself, which a C# enum
/// cannot carry. From C# 14 the static one is called on the enum
/// (<c>ConfigResourceType.ForId(…)</c>); older C# calls it on this class.
/// </summary>
public static class ConfigResourceTypeMethods
{
    extension(ConfigResourceType)
    {
        /// <summary>
        /// Return the type with the given id, or <see cref="ConfigResourceType.UNKNOWN"/> if no type has it —
        /// Java's <c>forId(byte)</c>.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>The type; never throws.</returns>
        public static ConfigResourceType ForId(byte id) => id switch
        {
            (byte)ConfigResourceType.GROUP => ConfigResourceType.GROUP,
            (byte)ConfigResourceType.CLIENT_METRICS => ConfigResourceType.CLIENT_METRICS,
            (byte)ConfigResourceType.BROKER_LOGGER => ConfigResourceType.BROKER_LOGGER,
            (byte)ConfigResourceType.BROKER => ConfigResourceType.BROKER,
            (byte)ConfigResourceType.TOPIC => ConfigResourceType.TOPIC,
            _ => ConfigResourceType.UNKNOWN,
        };
    }

    extension(ConfigResourceType type)
    {
        /// <summary>Return the id of this type — Java's <c>id()</c>.</summary>
        /// <returns>The id, which is the member's value.</returns>
        public byte Id() => unchecked((byte)type);
    }
}
