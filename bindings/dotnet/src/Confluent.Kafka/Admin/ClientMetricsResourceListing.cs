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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A listing of one client-metrics resource — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ClientMetricsResourceListing</c>
/// (<c>ClientMetricsResourceListing.java:22, :25, :29, :34, :42, :47</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The type itself is deprecated in Java</b>, not merely the RPC that produces it:
/// <c>@Deprecated(since = "4.1")</c> at <c>ClientMetricsResourceListing.java:21</c>. So it
/// carries <see cref="ObsoleteAttribute"/> here, as do
/// <see cref="ListClientMetricsResourcesResult"/> and
/// <see cref="ListClientMetricsResourcesOptions"/>, which Java deprecates on the same
/// line of reasoning.
/// </para>
/// <para>
/// <b><see cref="Name"/> is a property although Java's <c>name()</c> is a method</b> — a
/// pure managed field read that does no P/Invoke and cannot throw, CLAUDE.md §3's
/// "non-blocking getter → sync property" row. Same reading as
/// <see cref="TopicListing.Name"/> and <see cref="ConfigResource.Name"/>.
/// </para>
/// </remarks>
[Obsolete(
    "Deprecated in Kafka since 4.1. Use IAdmin.ListConfigResources filtered to "
    + "ConfigResourceType.ClientMetrics instead.")]
public sealed class ClientMetricsResourceListing
{
    /// <summary>
    /// Creates a listing — Java's <c>ClientMetricsResourceListing(String name)</c>
    /// (<c>:25</c>).
    /// </summary>
    /// <param name="name">The client-metrics resource name.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <remarks>
    /// ⚠ <b>Deliberately stricter than Java</b>, which does not null-check here (its
    /// <c>equals</c>/<c>hashCode</c>/<c>toString</c> all go through <c>Objects</c> and
    /// tolerate a null name). <see cref="Name"/> is a non-nullable <c>string</c> under
    /// <c>#nullable enable</c>, so accepting null would falsify the annotation — the same
    /// call, for the same reason, that <see cref="TopicListing"/> already makes.
    /// </remarks>
    public ClientMetricsResourceListing(string name)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
    }

    /// <summary>The resource name — Java's <c>name()</c> (<c>:29</c>).</summary>
    public string Name { get; }

    /// <summary>Value equality over <see cref="Name"/> — Java's <c>equals</c> (<c>:34</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same resource.</returns>
    public override bool Equals(object? obj) =>
        obj is ClientMetricsResourceListing other
        && string.Equals(Name, other.Name, StringComparison.Ordinal);

    /// <summary>The hash of <see cref="Name"/> — Java's <c>hashCode</c> (<c>:42</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(Name);

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:47</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// ⚠ The unbalanced quote is <b>Java's own</b>: <c>:48-50</c> concatenates
    /// <c>"name='" + name + ')'</c>, so the opening quote has no closing partner. Mirrored
    /// rather than corrected — <c>toString()</c> is the rendering Java specifies.
    /// </remarks>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "ClientMetricsResourceListing(name='{0})", Name);
}
