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
/// Options for describing consumer groups — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DescribeConsumerGroupsOptions
{
    private int? _timeoutMs;
    private bool _includeAuthorizedOperations;

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
    /// Ask the broker to report each group's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to
    /// <see langword="false"/>, as Java's uninitialized <c>boolean</c> field does.
    /// </summary>
    /// <remarks>
    /// While this is <see langword="false"/>,
    /// <see cref="ConsumerGroupDescription.AuthorizedOperations"/> is <see langword="null"/>,
    /// not an empty collection.
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

    internal OptionSetters Setters { get; } = new OptionSetters();
}
