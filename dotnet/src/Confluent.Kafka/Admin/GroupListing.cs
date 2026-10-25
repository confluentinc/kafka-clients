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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A listing of one group in the cluster — Java's
/// <c>org.apache.kafka.clients.admin.GroupListing</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's <c>Optional</c> becomes a C# nullable: <see langword="null"/> in <see cref="Type"/> or
/// <see cref="GroupState"/> is Java's <c>Optional.empty()</c>, for a broker that did not report
/// it. It differs from <see cref="GroupType.Unknown"/> and <see cref="GroupState.Unknown"/>, which
/// mean the broker named a value this client does not recognise, as in Java.
/// </para>
/// <para>
/// Java's <c>Objects.requireNonNull(type)</c> has no counterpart: it rejects a null
/// <c>Optional</c> reference, and <see langword="null"/> here is the empty case, which Java
/// accepts.
/// </para>
/// </remarks>
public sealed class GroupListing
{
    /// <summary>
    /// Creates a listing — Java's constructor.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="type">The group type, or <see langword="null"/> for Java's <c>Optional.empty()</c>.</param>
    /// <param name="protocol">
    /// The group protocol — the empty string for a classic group that is not using one.
    /// </param>
    /// <param name="groupState">The group state, or <see langword="null"/> for Java's <c>Optional.empty()</c>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="protocol"/> is null.
    /// </exception>
    /// <remarks>
    /// Both null checks are stricter than Java, which has neither: a recorded deviation that keeps
    /// <see cref="GroupId"/> and <see cref="Protocol"/> non-nullable, as on
    /// <see cref="TopicListing"/>.
    /// </remarks>
    public GroupListing(string groupId, GroupType? type, string protocol, GroupState? groupState)
    {
        GroupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
        Type = type;
        Protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        GroupState = groupState;
    }

    /// <summary>The group id — Java's <c>groupId()</c>.</summary>
    public string GroupId { get; }

    /// <summary>
    /// The group type, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>type()</c>.
    /// </summary>
    public GroupType? Type { get; }

    /// <summary>The group protocol — Java's <c>protocol()</c>.</summary>
    public string Protocol { get; }

    /// <summary>
    /// The group state, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>groupState()</c>.
    /// </summary>
    public GroupState? GroupState { get; }

    /// <summary>
    /// Whether this is a simple consumer group — Java's <c>isSimpleConsumerGroup()</c>: a classic
    /// group with an empty protocol.
    /// </summary>
    /// <remarks>
    /// Derived from <see cref="Type"/> and <see cref="Protocol"/>, as Java derives it, so a listing
    /// the caller builds answers as one the client returns; the core's
    /// <c>kafka_admin_GroupListing_is_simple_consumer_group</c> is not used.
    /// </remarks>
    public bool IsSimpleConsumerGroup => Type == GroupType.Classic && Protocol.Length == 0;

    /// <summary>
    /// Value equality over all four fields — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same listing.</returns>
    /// <remarks>
    /// The strings are compared ordinally, as Java's <c>Objects.equals</c> compares a
    /// <c>String</c>.
    /// </remarks>
    public override bool Equals(object? obj) =>
        obj is GroupListing other
        && string.Equals(GroupId, other.GroupId, StringComparison.Ordinal)
        && Type == other.Type
        && string.Equals(Protocol, other.Protocol, StringComparison.Ordinal)
        && GroupState == other.GroupState;

    /// <summary>The hash of all four fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(GroupId);
            hash = (hash * 31) + (Type is null ? 0 : ((int)Type.Value).GetHashCode());
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Protocol);
            hash = (hash * 31) + (GroupState is null ? 0 : ((int)GroupState.Value).GetHashCode());
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// The unbalanced quotes around <c>groupId</c> and <c>protocol</c> are Java's, kept as they
    /// are. An absent type or state renders as <c>none</c>, as Java's <c>orElse("none")</c>.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(groupId='{0}, type={1}, protocol='{2}, groupState={3})",
            GroupId,
            Type?.ToString() ?? "none",
            Protocol,
            GroupState?.ToString() ?? "none");
}
