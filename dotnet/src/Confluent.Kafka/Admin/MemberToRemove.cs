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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A struct containing information about the member to be removed — Java's
/// <c>org.apache.kafka.clients.admin.MemberToRemove</c>.
/// </summary>
/// <remarks>
/// Equality is by <see cref="GroupInstanceId"/> alone, ordinal, as Java's.
/// </remarks>
public sealed class MemberToRemove : IEquatable<MemberToRemove>
{
    /// <summary>
    /// Creates a member to remove, identified by its <c>group.instance.id</c> — Java's
    /// constructor.
    /// </summary>
    /// <param name="groupInstanceId">The static member's <c>group.instance.id</c>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupInstanceId"/> is null: stricter than Java, whose constructor accepts
    /// it, a recorded deviation.
    /// </exception>
    public MemberToRemove(string groupInstanceId)
    {
        if (groupInstanceId is null)
        {
            throw new ArgumentNullException(nameof(groupInstanceId));
        }

        GroupInstanceId = groupInstanceId;
    }

    /// <summary>
    /// The static member's <c>group.instance.id</c> — Java's <c>groupInstanceId()</c>.
    /// </summary>
    public string GroupInstanceId { get; }

    /// <inheritdoc/>
    public bool Equals(MemberToRemove? other) =>
        other is not null && string.Equals(GroupInstanceId, other.GroupInstanceId, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as MemberToRemove);

    /// <inheritdoc/>
    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(GroupInstanceId);
}
