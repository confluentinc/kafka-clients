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

namespace Confluent.Kafka;

/// <summary>
/// The kind of resource a <see cref="ConfigResource"/> names — the .NET realization of
/// Java's nested <c>org.apache.kafka.common.config.ConfigResource.Type</c>
/// (<c>ConfigResource.java:35-60</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Every member's numeric value is Java's <c>Type.id()</c></b>
/// (<c>ConfigResource.java:36-41</c>), <b>not</b> a C#-assigned ordinal. The ids cross the
/// C ABI as bare <c>int32_t</c> in <b>both</b> directions — out of
/// <c>kafka_admin_ListConfigResourcesResult_get_type</c> ("the
/// <c>ConfigResource.Type.id()</c> of the resource at <c>index</c>"), and into
/// <c>kafka_admin_AdminClient_list_config_resources</c> ("<c>resource_types</c> hold
/// <c>ConfigResource.Type.id()</c> codes") — so an auto-assigned value would mislabel
/// every resource in each direction. The codes are asserted member-by-member in the unit
/// tests, as <see cref="AclOperation"/>'s wire codes are.
/// </para>
/// <para>
/// <b>Flattened out of Java's nesting, and named accordingly (M15/P3 decision D16).</b>
/// Java nests this as <c>ConfigResource.Type</c>; C# cannot keep that name nested without
/// colliding with <see cref="System.Type"/> at every unqualified use site, so it is
/// flattened to <c>ConfigResourceType</c> beside <see cref="ConfigResource"/>. Sibling
/// enums that <em>can</em> nest cleanly stay nested, matching Java.
/// </para>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common.config</c> — under
/// <c>common</c>, not <c>clients.admin</c> — so it lives at the root
/// <c>Confluent.Kafka</c> namespace beside <see cref="ConfigResource"/>, following the
/// same rule that puts <see cref="Uuid"/>, <see cref="Node"/>,
/// <see cref="AclOperation"/> and <see cref="TopicCollection"/> there. ⚠ This diverges
/// from <c>confluent-kafka-dotnet</c>, which places its <c>ConfigResource</c> under
/// <c>Confluent.Kafka.Admin</c>; the divergence is deliberate, since this binding targets
/// the <b>Java</b> shape rather than the ecosystem client's
/// (<c>bindings/CLAUDE.md</c> §2).
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
