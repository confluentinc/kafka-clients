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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A snapshot of the consumer's group membership, Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerGroupMetadata</c>, returned by <c>GroupMetadata()</c>.
/// </summary>
/// <remarks>
/// Before the consumer has joined a group, <see cref="GroupId()"/> is the configured <c>group.id</c>,
/// <see cref="GenerationId()"/> is <c>-1</c>, <see cref="MemberId()"/> is empty, and <see cref="GroupInstanceId()"/> is
/// the configured <c>group.instance.id</c>, or <see langword="null"/>. A mock consumer returns Java's mock values.
/// An instance can be passed to a producer's <c>SendOffsetsToTransaction</c>, also after its consumer is closed: it
/// keeps the client's own copy of the metadata, released when the instance is garbage-collected.
/// </remarks>
public sealed class ConsumerGroupMetadata
{
    private readonly string _groupId;
    private readonly int _generationId;
    private readonly string _memberId;
    private readonly string? _groupInstanceId;

    internal ConsumerGroupMetadata(
        string groupId,
        int generationId,
        string memberId,
        string? groupInstanceId,
        SafeConsumerGroupMetadataHandle handle)
    {
        Handle = handle;
        _groupId = groupId;
        _generationId = generationId;
        _memberId = memberId;
        _groupInstanceId = groupInstanceId;
    }

    /// <summary>The consumer group id (the configured <c>group.id</c>).</summary>
    /// <returns>The consumer group id (the configured <c>group.id</c>).</returns>
    public string GroupId() => _groupId;

    /// <summary>
    /// The group generation id, or <c>-1</c> when unknown (before the consumer has
    /// joined a group).
    /// </summary>
    /// <returns>The group generation id, or <c>-1</c> when unknown (before the consumer has joined a
    /// group).</returns>
    public int GenerationId() => _generationId;

    /// <summary>
    /// The member id assigned by the coordinator, or the empty string before the
    /// consumer has joined a group.
    /// </summary>
    /// <returns>The member id assigned by the coordinator, or the empty string before the consumer has joined a
    /// group.</returns>
    public string MemberId() => _memberId;

    /// <summary>
    /// The static group instance id (the configured <c>group.instance.id</c>), or
    /// <see langword="null"/> when the consumer is not a static member.
    /// </summary>
    /// <returns>The static group instance id (the configured <c>group.instance.id</c>), or <see langword="null"/>
    /// when the consumer is not a static member.</returns>
    public string? GroupInstanceId() => _groupInstanceId;

    /// <summary>
    /// Value equality over the group id, the generation id, the member id and the group instance id, Java's
    /// <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same group membership.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is ConsumerGroupMetadata other
            && _generationId == other._generationId
            && string.Equals(_groupId, other._groupId, StringComparison.Ordinal)
            && string.Equals(_memberId, other._memberId, StringComparison.Ordinal)
            && string.Equals(_groupInstanceId, other._groupInstanceId, StringComparison.Ordinal));

    /// <summary>The hash of the members <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + (_groupId is null ? 0 : StringComparer.Ordinal.GetHashCode(_groupId));
            hash = (hash * 31) + _generationId;
            hash = (hash * 31) + (_memberId is null ? 0 : StringComparer.Ordinal.GetHashCode(_memberId));
            hash = (hash * 31) + (_groupInstanceId is null ? 0 : StringComparer.Ordinal.GetHashCode(_groupInstanceId));
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> form, an absent group instance id printed empty.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "GroupMetadata(groupId = {0}, generationId = {1}, memberId = {2}, groupInstanceId = {3})",
            _groupId,
            _generationId,
            _memberId,
            _groupInstanceId ?? string.Empty);

    // The core's metadata, which a producer's SendOffsetsToTransaction passes on. Not disposable: the finalizer frees it.
    internal SafeConsumerGroupMetadataHandle Handle { get; }
}
