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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeCluster"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeClusterOptions</c>
/// (<c>DescribeClusterOptions.java:23-64</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java declares exactly two fields of its own — <c>includeAuthorizedOperations</c>
/// (<c>:25, :54</c>) and <c>includeFencedBrokers</c> (<c>:27, :62</c>) — beside the
/// inherited timeout, and the ABI's <c>describe_cluster_async</c> takes exactly those two
/// booleans plus <c>timeout_ms</c>.
/// </para>
/// </remarks>
public sealed class DescribeClusterOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Ask the broker to report the cluster's authorized operations — Java's
    /// <c>includeAuthorizedOperations(boolean)</c> / <c>includeAuthorizedOperations()</c>.
    /// Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    /// <remarks>
    /// ⚠ Even when this is set, an older broker may not supply the information (Java says
    /// so at <c>:51-53</c>) — which is exactly why
    /// <see cref="DescribeClusterResult.AuthorizedOperations"/> yields
    /// <see langword="null"/> rather than an empty collection in that case.
    /// </remarks>
    public bool IncludeAuthorizedOperations { get; set; }

    /// <summary>
    /// Include fenced brokers in the reported node set — Java's
    /// <c>includeFencedBrokers(boolean)</c> / <c>includeFencedBrokers()</c>. Defaults to
    /// <see langword="false"/>, as Java's does.
    /// </summary>
    public bool IncludeFencedBrokers { get; set; }
}
