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
/// Java's <c>Optional</c> becomes a C# nullable: <see cref="GroupInstanceId()"/>,
/// <see cref="RackId()"/>, <see cref="TargetAssignment()"/>, <see cref="MemberEpoch()"/> and
/// <see cref="Upgraded()"/> are <see langword="null"/> for Java's <c>Optional.empty()</c>.
/// </para>
/// <para>
/// Null arguments are coalesced as in Java: <see cref="ConsumerId()"/>, <see cref="ClientId()"/>
/// and <see cref="Host()"/> become the empty string and <see cref="Assignment()"/> an empty
/// <see cref="MemberAssignment"/>.
/// </para>
/// </remarks>
public sealed class MemberDescription
{
    private readonly string _consumerId;
    private readonly string? _groupInstanceId;
    private readonly string? _rackId;
    private readonly string _clientId;
    private readonly string _host;
    private readonly MemberAssignment _assignment;
    private readonly MemberAssignment? _targetAssignment;
    private readonly int? _memberEpoch;
    private readonly bool? _upgraded;

    /// <summary>
    /// Creates a description — Java's current constructor.
    /// </summary>
    /// <param name="memberId">
    /// The member id, read back through <see cref="ConsumerId()"/>. Null becomes the empty
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
        _consumerId = memberId ?? string.Empty;
        _groupInstanceId = groupInstanceId;
        _rackId = rackId;
        _clientId = clientId ?? string.Empty;
        _host = host ?? string.Empty;
        _assignment = assignment ?? new MemberAssignment(null);
        _targetAssignment = targetAssignment;
        _memberEpoch = memberEpoch;
        _upgraded = upgraded;
    }

    /// <summary>
    /// The consumer id of the group member — Java's <c>consumerId()</c>, over the field Java
    /// calls <c>memberId</c>. Never <see langword="null"/>.
    /// </summary>
    /// <returns>The consumer id of the group member.</returns>
    public string ConsumerId() => _consumerId;

    /// <summary>
    /// The static group instance id of the member, or <see langword="null"/> if it is not a
    /// static member — Java's <c>groupInstanceId()</c>.
    /// </summary>
    /// <returns>The static group instance id of the member, or <see langword="null"/> if it is not a static
    /// member.</returns>
    public string? GroupInstanceId() => _groupInstanceId;

    /// <summary>
    /// The rack id of the member, or <see langword="null"/> if not reported — Java's
    /// <c>rackId()</c>. Only available for consumer groups using the new
    /// consumer group protocol (<c>group.protocol=consumer</c>).
    /// </summary>
    /// <returns>The rack id of the member, or <see langword="null"/> if not reported.</returns>
    public string? RackId() => _rackId;

    /// <summary>
    /// The client id of the group member — Java's <c>clientId()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    /// <returns>The client id of the group member.</returns>
    public string ClientId() => _clientId;

    /// <summary>
    /// The host the group member is running on — Java's <c>host()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    /// <returns>The host the group member is running on.</returns>
    public string Host() => _host;

    /// <summary>
    /// The member's current assignment — Java's <c>assignment()</c>. Never
    /// <see langword="null"/>; provided for both classic groups and consumer groups.
    /// </summary>
    /// <returns>The member's current assignment.</returns>
    public MemberAssignment Assignment() => _assignment;

    /// <summary>
    /// The member's target assignment, or <see langword="null"/> if not reported — Java's
    /// <c>targetAssignment()</c>. Provided only for consumer groups.
    /// </summary>
    /// <returns>The member's target assignment, or <see langword="null"/> if not reported.</returns>
    public MemberAssignment? TargetAssignment() => _targetAssignment;

    /// <summary>
    /// The epoch of the group member, or <see langword="null"/> for a classic-group member
    /// — Java's <c>memberEpoch()</c>.
    /// </summary>
    /// <returns>The epoch of the group member, or <see langword="null"/> for a classic-group member.</returns>
    public int? MemberEpoch() => _memberEpoch;

    /// <summary>
    /// Whether a consumer-group member uses the consumer protocol — Java's
    /// <c>upgraded()</c>. <see langword="true"/> if it does,
    /// <see langword="false"/> if it does not, and <see langword="null"/> if unknown or if
    /// the group is a classic group.
    /// </summary>
    /// <returns>Whether a consumer-group member uses the consumer protocol.</returns>
    public bool? Upgraded() => _upgraded;

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
            && string.Equals(ConsumerId(), other.ConsumerId(), StringComparison.Ordinal)
            && string.Equals(GroupInstanceId(), other.GroupInstanceId(), StringComparison.Ordinal)
            && string.Equals(RackId(), other.RackId(), StringComparison.Ordinal)
            && string.Equals(ClientId(), other.ClientId(), StringComparison.Ordinal)
            && string.Equals(Host(), other.Host(), StringComparison.Ordinal)
            && Assignment().Equals(other.Assignment())
            && AssignmentsMatch(TargetAssignment(), other.TargetAssignment())
            && MemberEpoch() == other.MemberEpoch()
            && Upgraded() == other.Upgraded();
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
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(_consumerId);
            hash = (hash * 31) + (_groupInstanceId is null ? 0 : StringComparer.Ordinal.GetHashCode(_groupInstanceId));
            hash = (hash * 31) + (_rackId is null ? 0 : StringComparer.Ordinal.GetHashCode(_rackId));
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(_clientId);
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(_host);
            hash = (hash * 31) + _assignment.GetHashCode();
            hash = (hash * 31) + (_targetAssignment is null ? 0 : _targetAssignment.GetHashCode());
            hash = (hash * 31) + _memberEpoch.GetHashCode();
            hash = (hash * 31) + _upgraded.GetHashCode();
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
            _consumerId,
            _groupInstanceId ?? "null",
            _rackId ?? "null",
            _clientId,
            _host,
            _assignment.ToString(),
            RenderOptional(_targetAssignment?.ToString()),
            _memberEpoch is null ? "null" : _memberEpoch.Value.ToString(CultureInfo.InvariantCulture),
            _upgraded is null ? "null" : (_upgraded.Value ? "true" : "false"));

    private static bool AssignmentsMatch(MemberAssignment? left, MemberAssignment? right) =>
        left is null ? right is null : left.Equals(right);

    private static string RenderOptional(string? value) =>
        value is null
            ? "Optional.empty"
            : string.Format(CultureInfo.InvariantCulture, "Optional[{0}]", value);
}
