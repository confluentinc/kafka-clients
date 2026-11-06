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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeCluster"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeClusterOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DescribeClusterOptions
{
    private int? _timeoutMs;

    private bool _includeAuthorizedOperations;

    private bool _includeFencedBrokers;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public DescribeClusterOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Ask the broker to report the cluster's authorized operations — Java's
    /// <c>includeAuthorizedOperations(boolean)</c> / <c>includeAuthorizedOperations()</c>.
    /// Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    /// <returns>Ask the broker to report the cluster's authorized operations — Java's
    /// <c>includeAuthorizedOperations(boolean)</c> / <c>includeAuthorizedOperations()</c>. Defaults to
    /// <see langword="false"/>, as Java's does.</returns>
    /// <remarks>
    /// Even when this is set, an older broker may not supply the information; then
    /// <see cref="DescribeClusterResult.AuthorizedOperations"/> yields <see langword="null"/>.
    /// </remarks>
    public bool IncludeAuthorizedOperations() => _includeAuthorizedOperations;

    /// <summary>Sets whether the cluster's authorized operations are included in the response.</summary>
    /// <param name="includeAuthorizedOperations">Whether to include them. Defaults to <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public DescribeClusterOptions IncludeAuthorizedOperations(bool includeAuthorizedOperations)
    {
        _includeAuthorizedOperations = includeAuthorizedOperations;
        Setters.Ran(nameof(IncludeAuthorizedOperations));
        return this;
    }

    /// <summary>
    /// Include fenced brokers in the reported node set — Java's
    /// <c>includeFencedBrokers(boolean)</c> / <c>includeFencedBrokers()</c>. Defaults to
    /// <see langword="false"/>, as Java's does.
    /// </summary>
    /// <returns>Include fenced brokers in the reported node set — Java's <c>includeFencedBrokers(boolean)</c> /
    /// <c>includeFencedBrokers()</c>. Defaults to <see langword="false"/>, as Java's does.</returns>
    public bool IncludeFencedBrokers() => _includeFencedBrokers;

    /// <summary>Sets whether fenced brokers are included in the response.</summary>
    /// <param name="includeFencedBrokers">Whether to include them. Defaults to <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public DescribeClusterOptions IncludeFencedBrokers(bool includeFencedBrokers)
    {
        _includeFencedBrokers = includeFencedBrokers;
        Setters.Ran(nameof(IncludeFencedBrokers));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
