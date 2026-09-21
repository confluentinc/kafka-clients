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
/// A struct containing information about the member to be removed — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.MemberToRemove</c> (<c>MemberToRemove.java:27</c>).
/// </summary>
/// <remarks>
/// <para>
/// Equality is by <see cref="GroupInstanceId"/> alone, ordinal — Java's <c>equals</c>/
/// <c>hashCode</c> (<c>:34-47</c>) compare only the <c>groupInstanceId</c> field. This is
/// what lets <see cref="RemoveMembersFromConsumerGroupOptions"/> de-duplicate a
/// <see cref="System.Collections.Generic.HashSet{T}"/> of these the same way Java's
/// constructor collapses its <c>Collection</c> into a <c>Set</c> (<c>:37</c>), and what lets
/// <see cref="RemoveMembersFromConsumerGroupResult.MemberResult"/> look a member up by value.
/// </para>
/// <para>
/// ⚠ Java's <c>toString()</c> is not overridden — the default <c>Object.toString()</c>
/// (class name + identity hash) appears in Java's own exception messages
/// (<c>RemoveMembersFromConsumerGroupResult.java:83-87</c>), which makes those messages
/// non-deterministic even in Java. This binding substitutes <see cref="GroupInstanceId"/>
/// directly where Java would have interpolated <c>member.toString()</c> — a strictly more
/// useful message, and a deviation that does not chase a moving target.
/// </para>
/// </remarks>
public sealed class MemberToRemove : IEquatable<MemberToRemove>
{
    /// <summary>
    /// Creates a member to remove, identified by its <c>group.instance.id</c> — Java's
    /// <c>MemberToRemove(String groupInstanceId)</c> (<c>:30-32</c>).
    /// </summary>
    /// <param name="groupInstanceId">The static member's <c>group.instance.id</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="groupInstanceId"/> is null.</exception>
    public MemberToRemove(string groupInstanceId)
    {
        if (groupInstanceId is null)
        {
            throw new ArgumentNullException(nameof(groupInstanceId));
        }

        GroupInstanceId = groupInstanceId;
    }

    /// <summary>
    /// The static member's <c>group.instance.id</c> — Java's <c>groupInstanceId()</c>
    /// (<c>:55-57</c>).
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
