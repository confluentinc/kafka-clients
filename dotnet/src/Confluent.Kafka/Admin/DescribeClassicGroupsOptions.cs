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
/// Options for describing classic groups — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeClassicGroupsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DescribeClassicGroupsOptions
{
    private int? _timeoutMs;
    private bool _includeAuthorizedOperations;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public DescribeClassicGroupsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Ask the broker to report each group's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to
    /// <see langword="false"/>, as Java's uninitialized <c>boolean</c> field does.
    /// </summary>
    /// <returns>Ask the broker to report each group's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to <see langword="false"/>, as Java's uninitialized
    /// <c>boolean</c> field does.</returns>
    /// <remarks>
    /// While this is <see langword="false"/> the broker reports nothing, which is exactly
    /// the case <see cref="ClassicGroupDescription.AuthorizedOperations()"/> renders as
    /// <see langword="null"/> rather than as an empty collection — the same distinction
    /// <see cref="DescribeConsumerGroupsOptions.IncludeAuthorizedOperations()"/> draws for
    /// the newer group generation.
    /// </remarks>
    public bool IncludeAuthorizedOperations() => _includeAuthorizedOperations;

    /// <summary>Sets whether each group's authorized operations are included in the response.</summary>
    /// <param name="includeAuthorizedOperations">Whether to include them. Defaults to <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public DescribeClassicGroupsOptions IncludeAuthorizedOperations(bool includeAuthorizedOperations)
    {
        _includeAuthorizedOperations = includeAuthorizedOperations;
        Setters.Ran(nameof(IncludeAuthorizedOperations));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
