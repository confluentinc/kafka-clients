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
/// Options for <see cref="IAdmin.ListClientMetricsResources"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.ListClientMetricsResourcesOptions</c>
/// (<c>ListClientMetricsResourcesOptions.java:25</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// ⚠ Java deprecates the options class itself —
/// <c>@Deprecated(since = "4.1")</c> at <c>ListClientMetricsResourcesOptions.java:24</c>
/// — so it carries <see cref="ObsoleteAttribute"/> here too. It declares no members of its
/// own, so <see cref="TimeoutMs"/> is the whole surface.
/// </para>
/// </remarks>
[Obsolete(
    "Deprecated in Kafka since 4.1. Use ListConfigResourcesOptions with "
    + "IAdmin.ListConfigResources filtered to ConfigResourceType.ClientMetrics instead.")]
public sealed class ListClientMetricsResourcesOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }
}
