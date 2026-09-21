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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.ListClientMetricsResources"/> — the .NET realization of
/// Java's <c>ListClientMetricsResourcesResult</c>: <b>one</b> awaitable over the whole
/// listing, handed back the moment the request is submitted
/// (<c>ListClientMetricsResourcesResult.java:45</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ Java deprecates this class itself —
/// <c>@Deprecated(since = "4.1")</c> at <c>ListClientMetricsResourcesResult.java:30</c> —
/// so it carries <see cref="ObsoleteAttribute"/>, as do
/// <see cref="ClientMetricsResourceListing"/> and
/// <see cref="ListClientMetricsResourcesOptions"/>.
/// </para>
/// <para>
/// ⚠ <b>Exactly one accessor, because Java declares exactly one</b>, and a
/// <em>collection</em> rather than a map: <c>kafka_admin_ListClientMetricsResourcesResult_t</c>
/// exposes only <c>count</c> / <c>get_name</c> / <c>destroy</c> — the listing <em>is</em>
/// the name — with no key and no <c>get_error</c>. See
/// <c>KeyedResultMarshal.CompleteList</c>.
/// </para>
/// </remarks>
[Obsolete(
    "Deprecated in Kafka since 4.1. Use IAdmin.ListConfigResources filtered to "
    + "ConfigResourceType.ClientMetrics instead.")]
public sealed class ListClientMetricsResourcesResult
{
    private readonly Task<IReadOnlyCollection<ClientMetricsResourceListing>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>ListClientMetricsResourcesResult(KafkaFuture&lt;Collection&lt;ClientMetricsResourceListing&gt;&gt;)</c>
    /// (<c>:34</c>).
    /// </summary>
    internal ListClientMetricsResourcesResult(Task<IReadOnlyCollection<ClientMetricsResourceListing>> future)
    {
        _future = future;
    }

    /// <summary>
    /// The full set of client-metrics listings — Java's <c>all()</c> (<c>:45</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the listings, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// <inheritdoc cref="ListConfigResourcesResult.All" path="/remarks"/>
    /// </remarks>
    public Task<IReadOnlyCollection<ClientMetricsResourceListing>> All() => _future;
}
