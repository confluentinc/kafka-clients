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
/// A detailed description of one group member — Java's
/// <c>org.apache.kafka.clients.admin.MemberDescription</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's four deprecated constructors are not translated: they pass <c>Optional.empty()</c>
/// for the arguments they omit, which a caller passes as <see langword="null"/> to the current
/// one.
/// </para>
/// <para>
/// Java's <c>Optional</c> becomes a C# nullable: <see cref="GroupInstanceId"/>,
/// <see cref="RackId"/>, <see cref="TargetAssignment"/>, <see cref="MemberEpoch"/> and
/// <see cref="Upgraded"/> are <see langword="null"/> for Java's <c>Optional.empty()</c>.
/// </para>
/// <para>
/// Null arguments are coalesced as in Java: <see cref="ConsumerId"/>, <see cref="ClientId"/>
/// and <see cref="Host"/> become the empty string and <see cref="Assignment"/> an empty
/// <see cref="MemberAssignment"/>.
/// </para>
/// </remarks>
public sealed class MemberDescription
{
    /// <summary>
    /// Creates a description — Java's current constructor.
    /// </summary>
    /// <param name="memberId">
    /// The member id, read back through <see cref="ConsumerId"/>. Null becomes the empty
    /// string.
    /// </param>
    /// <param name="groupInstanceId">
    /// The static group instance id, or <see langword="null"/> if the member is not a
    /// static member.
    /// </param>
    /// <param name="rackId">
    /// The member's rack id, or <see langword="null"/> if not reported. Only consumer
    /// groups on the new group protocol report it.
    /// </param>
    /// <param name="clientId">The client id. Null becomes the empty string.</param>
    /// <param name="host">The host the member runs on. Null becomes the empty string.</param>
    /// <param name="assignment">
    /// The member's current assignment. Null becomes an empty assignment.
    /// </param>
    /// <param name="targetAssignment">
    /// The member's target assignment, or <see langword="null"/> if not reported.
    /// </param>
    /// <param name="memberEpoch">
    /// The member epoch, or <see langword="null"/> for a classic-group member.
    /// </param>
    /// <param name="upgraded">
    /// Whether the member uses the consumer protocol, or <see langword="null"/> if unknown.
    /// </param>
    public MemberDescription(
        string? memberId,
        string? groupInstanceId,
        string? rackId,
        string? clientId,
        string? host,
        MemberAssignment? assignment,
        MemberAssignment? targetAssignment,
        int? memberEpoch,
        bool? upgraded)
    {
        ConsumerId = memberId ?? string.Empty;
        GroupInstanceId = groupInstanceId;
        RackId = rackId;
        ClientId = clientId ?? string.Empty;
        Host = host ?? string.Empty;
        Assignment = assignment ?? new MemberAssignment(null);
        TargetAssignment = targetAssignment;
        MemberEpoch = memberEpoch;
        Upgraded = upgraded;
    }

    /// <summary>
    /// The consumer id of the group member — Java's <c>consumerId()</c>, over the field Java
    /// calls <c>memberId</c>. Never <see langword="null"/>.
    /// </summary>
    public string ConsumerId { get; }

    /// <summary>
    /// The static group instance id of the member, or <see langword="null"/> if it is not a
    /// static member — Java's <c>groupInstanceId()</c>.
    /// </summary>
    public string? GroupInstanceId { get; }

    /// <summary>
    /// The rack id of the member, or <see langword="null"/> if not reported — Java's
    /// <c>rackId()</c>. Only available for consumer groups using the new
    /// consumer group protocol (<c>group.protocol=consumer</c>).
    /// </summary>
    public string? RackId { get; }

    /// <summary>
    /// The client id of the group member — Java's <c>clientId()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// The host the group member is running on — Java's <c>host()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    public string Host { get; }

    /// <summary>
    /// The member's current assignment — Java's <c>assignment()</c>. Never
    /// <see langword="null"/>; provided for both classic groups and consumer groups.
    /// </summary>
    public MemberAssignment Assignment { get; }

    /// <summary>
    /// The member's target assignment, or <see langword="null"/> if not reported — Java's
    /// <c>targetAssignment()</c>. Provided only for consumer groups.
    /// </summary>
    public MemberAssignment? TargetAssignment { get; }

    /// <summary>
    /// The epoch of the group member, or <see langword="null"/> for a classic-group member
    /// — Java's <c>memberEpoch()</c>.
    /// </summary>
    public int? MemberEpoch { get; }

    /// <summary>
    /// Whether a consumer-group member uses the consumer protocol — Java's
    /// <c>upgraded()</c>. <see langword="true"/> if it does,
    /// <see langword="false"/> if it does not, and <see langword="null"/> if unknown or if
    /// the group is a classic group.
    /// </summary>
    public bool? Upgraded { get; }

    /// <summary>
    /// Compares all nine members — Java's <c>equals</c>. Strings compare ordinally, as Java's
    /// <c>String.equals</c> does.
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when every member matches.</returns>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is MemberDescription other
            && string.Equals(ConsumerId, other.ConsumerId, StringComparison.Ordinal)
            && string.Equals(GroupInstanceId, other.GroupInstanceId, StringComparison.Ordinal)
            && string.Equals(RackId, other.RackId, StringComparison.Ordinal)
            && string.Equals(ClientId, other.ClientId, StringComparison.Ordinal)
            && string.Equals(Host, other.Host, StringComparison.Ordinal)
            && Assignment.Equals(other.Assignment)
            && AssignmentsMatch(TargetAssignment, other.TargetAssignment)
            && MemberEpoch == other.MemberEpoch
            && Upgraded == other.Upgraded;
    }

    /// <summary>
    /// The hash of the same nine members — Java's <c>hashCode</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(ConsumerId);
            hash = (hash * 31) + (GroupInstanceId is null ? 0 : StringComparer.Ordinal.GetHashCode(GroupInstanceId));
            hash = (hash * 31) + (RackId is null ? 0 : StringComparer.Ordinal.GetHashCode(RackId));
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(ClientId);
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Host);
            hash = (hash * 31) + Assignment.GetHashCode();
            hash = (hash * 31) + (TargetAssignment is null ? 0 : TargetAssignment.GetHashCode());
            hash = (hash * 31) + MemberEpoch.GetHashCode();
            hash = (hash * 31) + Upgraded.GetHashCode();
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// As in Java, an absent <c>groupInstanceId</c>, <c>rackId</c>, <c>memberEpoch</c> or
    /// <c>upgraded</c> renders as <c>null</c>, the target assignment as <c>Optional[...]</c> or
    /// <c>Optional.empty</c>, and the member id under the label <c>memberId</c>.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(memberId={0}, groupInstanceId={1}, rackId={2}, clientId={3}, host={4}, " +
            "assignment={5}, targetAssignment={6}, memberEpoch={7}, upgraded={8})",
            ConsumerId,
            GroupInstanceId ?? "null",
            RackId ?? "null",
            ClientId,
            Host,
            Assignment.ToString(),
            RenderOptional(TargetAssignment?.ToString()),
            MemberEpoch is null ? "null" : MemberEpoch.Value.ToString(CultureInfo.InvariantCulture),
            Upgraded is null ? "null" : (Upgraded.Value ? "true" : "false"));

    private static bool AssignmentsMatch(MemberAssignment? left, MemberAssignment? right) =>
        left is null ? right is null : left.Equals(right);

    private static string RenderOptional(string? value) =>
        value is null
            ? "Optional.empty"
            : string.Format(CultureInfo.InvariantCulture, "Optional[{0}]", value);
}
