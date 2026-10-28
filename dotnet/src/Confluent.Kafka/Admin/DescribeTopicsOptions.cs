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
/// Options for <see cref="IAdmin.DescribeTopics(TopicCollection, DescribeTopicsOptions?)"/> —
/// the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeTopicsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DescribeTopicsOptions
{
    private int? _timeoutMs;

    private bool _includeAuthorizedOperations;

    private int _partitionSizeLimitPerResponse = 2000;

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
    /// Ask the broker to report each topic's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to <see langword="false"/>, as
    /// Java's does.
    /// </summary>
    /// <remarks>
    /// While this is <see langword="false"/>, <see cref="TopicDescription.AuthorizedOperations"/>
    /// is <see langword="null"/>, not an empty collection.
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
    /// The maximum number of partitions the broker may return in a single response —
    /// Java's <c>partitionSizeLimitPerResponse()</c>. Defaults to 2000, as Java's does.
    /// </summary>
    /// <remarks>
    /// Only a by-name describe uses it, as the DescribeTopicPartitions request's
    /// <c>ResponsePartitionLimit</c>, capped by the broker's
    /// <c>max.request.partition.size.limit</c>. A by-id describe, and a by-name one on a
    /// broker without DescribeTopicPartitions, go through Metadata, which does not use it.
    /// </remarks>
    public int PartitionSizeLimitPerResponse
    {
        get => _partitionSizeLimitPerResponse;
        set
        {
            _partitionSizeLimitPerResponse = value;
            Setters.Ran(nameof(PartitionSizeLimitPerResponse));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
