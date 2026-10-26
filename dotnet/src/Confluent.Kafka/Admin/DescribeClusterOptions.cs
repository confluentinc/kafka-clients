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

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs
    {
        get => _timeoutMs;
        set
        {
            _timeoutMs = value;
            Setters.Ran(nameof(TimeoutMs));
        }
    }

    /// <summary>
    /// Ask the broker to report the cluster's authorized operations — Java's
    /// <c>includeAuthorizedOperations(boolean)</c> / <c>includeAuthorizedOperations()</c>.
    /// Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    /// <remarks>
    /// Even when this is set, an older broker may not supply the information; then
    /// <see cref="DescribeClusterResult.AuthorizedOperations"/> yields <see langword="null"/>.
    /// </remarks>
    public bool IncludeAuthorizedOperations
    {
        get => _includeAuthorizedOperations;
        set
        {
            _includeAuthorizedOperations = value;
            Setters.Ran(nameof(IncludeAuthorizedOperations));
        }
    }

    /// <summary>
    /// Include fenced brokers in the reported node set — Java's
    /// <c>includeFencedBrokers(boolean)</c> / <c>includeFencedBrokers()</c>. Defaults to
    /// <see langword="false"/>, as Java's does.
    /// </summary>
    public bool IncludeFencedBrokers
    {
        get => _includeFencedBrokers;
        set
        {
            _includeFencedBrokers = value;
            Setters.Ran(nameof(IncludeFencedBrokers));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
