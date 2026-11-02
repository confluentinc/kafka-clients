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
/// </remarks>
public enum ConfigResourceType
{
    /// <summary>
    /// A resource type this client does not recognize — Java's <c>UNKNOWN</c> (id 0).
    /// </summary>
    Unknown = 0,

    /// <summary>A topic's configuration — Java's <c>TOPIC</c> (id 2).</summary>
    Topic = 2,

    /// <summary>A broker's configuration — Java's <c>BROKER</c> (id 4).</summary>
    Broker = 4,

    /// <summary>A broker's logger levels — Java's <c>BROKER_LOGGER</c> (id 8).</summary>
    BrokerLogger = 8,

    /// <summary>
    /// A client-metrics subscription — Java's <c>CLIENT_METRICS</c> (id 16).
    /// </summary>
    ClientMetrics = 16,

    /// <summary>A consumer group's configuration — Java's <c>GROUP</c> (id 32).</summary>
    Group = 32,
}
