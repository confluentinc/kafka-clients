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
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The <see cref="IConsumerGroupMetadata"/> a consumer returns: a copy of the core's metadata, which keeps the core's
/// handle for a producer's <c>SendOffsetsToTransaction</c>, with Java's value equality and <c>toString()</c>.
/// </summary>
internal sealed class ConsumerGroupMetadataImpl : IConsumerGroupMetadata
{
    private readonly string _groupId;
    private readonly int _generationId;
    private readonly string _memberId;
    private readonly string? _groupInstanceId;

    internal ConsumerGroupMetadataImpl(
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

    // The core's metadata, which a producer's SendOffsetsToTransaction passes on. Not disposable: the finalizer frees it.
    internal SafeConsumerGroupMetadataHandle Handle { get; }

    /// <inheritdoc/>
    public string GroupId() => _groupId;

    /// <inheritdoc/>
    public int GenerationId() => _generationId;

    /// <inheritdoc/>
    public string MemberId() => _memberId;

    /// <inheritdoc/>
    public string? GroupInstanceId() => _groupInstanceId;

    /// <summary>
    /// Value equality over the group id, the generation id, the member id and the group instance id, Java's
    /// <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same group membership.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is ConsumerGroupMetadataImpl other
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
}
