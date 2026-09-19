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
/// A detailed description of one group member — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.MemberDescription</c>
/// (<c>MemberDescription.java:28, :39, :146, :162, :169, :176, :187, :194, :201, :208,
/// :215, :224, :234, :239</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>The type is not deprecated</b>, although the RPC family around it has deprecated
/// members: Java's class carries no <c>@Deprecated</c>, so no
/// <see cref="ObsoleteAttribute"/> appears here. Contrast
/// <see cref="ConsumerGroupListing"/>, whose Java class <em>is</em> deprecated and which
/// mirrors that.
/// </para>
/// <para>
/// ⚠ <b>Java's four deprecated constructors are deliberately not translated</b> — the
/// standing ruling this phase applied to <see cref="ConsumerGroupListing"/>'s two, for the
/// same reason. All four (<c>:66</c> since 4.2, <c>:93</c>, <c>:117</c> and <c>:138</c>
/// since 4.0, every one <c>forRemoval = true</c>) are pure forwarders that pass
/// <c>Optional.empty()</c> for the arguments they omit; a caller can pass
/// <see langword="null"/> for those same arguments to the current constructor and get the
/// identical object. Nothing expressible is lost. The current constructor ships in full,
/// and every accessor ships — none of them is deprecated in Java.
/// </para>
/// <para>
/// ⚠ <b><see cref="ConsumerId"/> is Java's accessor name for a field Java calls
/// <c>memberId</c>.</b> Java's <c>consumerId()</c> (<c>:169</c>) returns <c>memberId</c>,
/// and its <c>toString()</c> labels the value <c>memberId=</c> (<c>:240</c>). Both
/// spellings are mirrored where each occurs: the property takes the accessor's name, the
/// rendering takes the label's.
/// </para>
/// <para>
/// <b>Java's <c>Optional&lt;T&gt;</c> becomes a C# nullable.</b>
/// <see cref="GroupInstanceId"/>, <see cref="RackId"/>, <see cref="TargetAssignment"/>,
/// <see cref="MemberEpoch"/> and <see cref="Upgraded"/> are all <c>Optional</c> in Java;
/// <see langword="null"/> here is that <c>Optional.empty()</c>. The two value-typed ones
/// matter most: the C ABI reports each as a presence flag beside the value, so
/// <see cref="MemberEpoch"/> and <see cref="Upgraded"/> are nullable rather than carrying
/// a sentinel — there is no epoch and no boolean that could stand in for "absent".
/// </para>
/// <para>
/// ⚠ <b>Null arguments are accepted and coalesced, not rejected.</b>
/// <see cref="ConsumerId"/>, <see cref="ClientId"/> and <see cref="Host"/> become the empty
/// string and <see cref="Assignment"/> becomes an empty <see cref="MemberAssignment"/>,
/// mirroring Java <c>:50-56</c>. As on <see cref="MemberAssignment"/>, this differs from
/// the <see cref="ArgumentNullException"/> guard <see cref="ConsumerGroupListing"/> applies
/// because Java itself differs — coalescing here, rejecting there.
/// </para>
/// </remarks>
public sealed class MemberDescription
{
    /// <summary>
    /// Creates a description — Java's current constructor
    /// (<c>MemberDescription.java:39</c>).
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
        // Java :50-56 — the three plain strings and the assignment coalesce; the five
        // Optionals are stored as given.
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
    /// The consumer id of the group member — Java's <c>consumerId()</c> (<c>:169</c>).
    /// Never <see langword="null"/>; see the naming note in the type remarks.
    /// </summary>
    public string ConsumerId { get; }

    /// <summary>
    /// The static group instance id of the member, or <see langword="null"/> if it is not a
    /// static member — Java's <c>groupInstanceId()</c> (<c>:176</c>).
    /// </summary>
    public string? GroupInstanceId { get; }

    /// <summary>
    /// The rack id of the member, or <see langword="null"/> if not reported — Java's
    /// <c>rackId()</c> (<c>:187</c>). Only available for consumer groups using the new
    /// consumer group protocol (<c>group.protocol=consumer</c>).
    /// </summary>
    public string? RackId { get; }

    /// <summary>
    /// The client id of the group member — Java's <c>clientId()</c> (<c>:194</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// The host the group member is running on — Java's <c>host()</c> (<c>:201</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    public string Host { get; }

    /// <summary>
    /// The member's current assignment — Java's <c>assignment()</c> (<c>:208</c>). Never
    /// <see langword="null"/>; provided for both classic groups and consumer groups.
    /// </summary>
    public MemberAssignment Assignment { get; }

    /// <summary>
    /// The member's target assignment, or <see langword="null"/> if not reported — Java's
    /// <c>targetAssignment()</c> (<c>:215</c>). Provided only for consumer groups.
    /// </summary>
    public MemberAssignment? TargetAssignment { get; }

    /// <summary>
    /// The epoch of the group member, or <see langword="null"/> for a classic-group member
    /// — Java's <c>memberEpoch()</c> (<c>:224</c>).
    /// </summary>
    public int? MemberEpoch { get; }

    /// <summary>
    /// Whether a consumer-group member uses the consumer protocol — Java's
    /// <c>upgraded()</c> (<c>:234</c>). <see langword="true"/> if it does,
    /// <see langword="false"/> if it does not, and <see langword="null"/> if unknown or if
    /// the group is a classic group.
    /// </summary>
    public bool? Upgraded { get; }

    /// <summary>
    /// Compares all nine members — Java's <c>equals</c> (<c>:146</c>).
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when every member matches.</returns>
    /// <remarks>
    /// Java's <c>getClass() != o.getClass()</c> check (<c>:148</c>) is met by the type being
    /// <see langword="sealed"/>. Strings compare ordinally, as Java's <c>String.equals</c>
    /// does.
    /// </remarks>
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
    /// The hash of the same nine members — Java's <c>Objects.hash(...)</c> (<c>:162</c>),
    /// whose 31-fold this reproduces in Java's argument order.
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

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:239</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Java renders its five <c>Optional</c> members three different ways in this one
    /// method, and each is mirrored.</b> <c>groupInstanceId</c> and <c>rackId</c> go
    /// through <c>orElse("null")</c> (<c>:241-242</c>), so an absent one prints the bare
    /// text <c>null</c>. <c>targetAssignment</c> is concatenated raw (<c>:246</c>) and so
    /// inherits <c>Optional.toString()</c> — <c>Optional[...]</c> or <c>Optional.empty</c>.
    /// <c>memberEpoch</c> and <c>upgraded</c> go through <c>orElse(null)</c>
    /// (<c>:247-248</c>), whose null result concatenates as the text <c>null</c> — the same
    /// spelling as the first pair, reached by a different route. Normalizing the three
    /// would produce a diagnostic string that no longer matches the Java client's.
    /// </para>
    /// <para>
    /// The value label is <c>memberId</c>, not <c>consumerId</c> — see the naming note in
    /// the type remarks. A present <see cref="Upgraded"/> renders lowercase
    /// <c>true</c>/<c>false</c>, as Java's <c>Boolean.toString()</c> does and as
    /// <see cref="ConsumerGroupListing"/> already spells it.
    /// </para>
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

    /// <summary>
    /// Compares two possibly-absent assignments the way Java's
    /// <c>Optional&lt;MemberAssignment&gt;.equals</c> does — absent equals absent, and two
    /// present ones compare by value.
    /// </summary>
    /// <param name="left">The first assignment, or null for absent.</param>
    /// <param name="right">The second assignment, or null for absent.</param>
    /// <returns>True when both are absent or both are present and equal.</returns>
    private static bool AssignmentsMatch(MemberAssignment? left, MemberAssignment? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>
    /// Renders one value the way Java's <c>Optional.toString()</c> does —
    /// <c>Optional[X]</c> for a present value, <c>Optional.empty</c> for an absent one.
    /// </summary>
    /// <param name="value">The present value's <c>toString()</c> spelling, or null.</param>
    /// <returns>The rendering.</returns>
    private static string RenderOptional(string? value) =>
        value is null
            ? "Optional.empty"
            : string.Format(CultureInfo.InvariantCulture, "Optional[{0}]", value);
}
