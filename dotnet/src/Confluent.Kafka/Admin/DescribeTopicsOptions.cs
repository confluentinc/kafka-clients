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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeTopics(TopicCollection, DescribeTopicsOptions)"/> —
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

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public DescribeTopicsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Ask the broker to report each topic's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to <see langword="false"/>, as
    /// Java's does.
    /// </summary>
    /// <returns>Ask the broker to report each topic's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to <see langword="false"/>, as Java's does.</returns>
    /// <remarks>
    /// While this is <see langword="false"/>, <see cref="TopicDescription.AuthorizedOperations()"/>
    /// is <see langword="null"/>, not an empty collection.
    /// </remarks>
    public bool IncludeAuthorizedOperations() => _includeAuthorizedOperations;

    /// <summary>Sets whether each topic's authorized operations are included in the response.</summary>
    /// <param name="includeAuthorizedOperations">Whether to include them. Defaults to <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public DescribeTopicsOptions IncludeAuthorizedOperations(bool includeAuthorizedOperations)
    {
        _includeAuthorizedOperations = includeAuthorizedOperations;
        Setters.Ran(nameof(IncludeAuthorizedOperations));
        return this;
    }

    /// <summary>
    /// The maximum number of partitions the broker may return in a single response —
    /// Java's <c>partitionSizeLimitPerResponse()</c>. Defaults to 2000, as Java's does.
    /// </summary>
    /// <returns>The maximum number of partitions the broker may return in a single response — Java's
    /// <c>partitionSizeLimitPerResponse()</c>. Defaults to 2000, as Java's does.</returns>
    /// <remarks>
    /// Only a by-name describe uses it, as the DescribeTopicPartitions request's
    /// <c>ResponsePartitionLimit</c>, capped by the broker's
    /// <c>max.request.partition.size.limit</c>. A by-id describe, and a by-name one on a
    /// broker without DescribeTopicPartitions, go through Metadata, which does not use it.
    /// </remarks>
    public int PartitionSizeLimitPerResponse() => _partitionSizeLimitPerResponse;

    /// <summary>
    /// Sets the maximum number of partitions to be returned in a single response. It is only effective when
    /// using topic names (not topic IDs), and not if it is larger than the server-side configuration
    /// <c>max.request.partition.size.limit</c>.
    /// </summary>
    /// <param name="partitionSizeLimitPerResponse">The maximum number of partitions per response. Defaults to
    /// 2000.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public DescribeTopicsOptions PartitionSizeLimitPerResponse(int partitionSizeLimitPerResponse)
    {
        _partitionSizeLimitPerResponse = partitionSizeLimitPerResponse;
        Setters.Ran(nameof(PartitionSizeLimitPerResponse));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
