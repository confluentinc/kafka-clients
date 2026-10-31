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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A snapshot of the consumer's group membership, Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerGroupMetadata</c>, returned by <c>GroupMetadata()</c>.
/// </summary>
/// <remarks>
/// Before the consumer has joined a group, <see cref="GroupId"/> is the configured <c>group.id</c>,
/// <see cref="GenerationId"/> is <c>-1</c>, <see cref="MemberId"/> is empty, and <see cref="GroupInstanceId"/> is
/// the configured <c>group.instance.id</c>, or <see langword="null"/>. A mock consumer returns Java's mock values.
/// </remarks>
public sealed class ConsumerGroupMetadata
{
    internal ConsumerGroupMetadata(string groupId, int generationId, string memberId, string? groupInstanceId)
    {
        GroupId = groupId;
        GenerationId = generationId;
        MemberId = memberId;
        GroupInstanceId = groupInstanceId;
    }

    /// <summary>The consumer group id (the configured <c>group.id</c>).</summary>
    public string GroupId { get; }

    /// <summary>
    /// The group generation id, or <c>-1</c> when unknown (before the consumer has
    /// joined a group).
    /// </summary>
    public int GenerationId { get; }

    /// <summary>
    /// The member id assigned by the coordinator, or the empty string before the
    /// consumer has joined a group.
    /// </summary>
    public string MemberId { get; }

    /// <summary>
    /// The static group instance id (the configured <c>group.instance.id</c>), or
    /// <see langword="null"/> when the consumer is not a static member.
    /// </summary>
    public string? GroupInstanceId { get; }
}
