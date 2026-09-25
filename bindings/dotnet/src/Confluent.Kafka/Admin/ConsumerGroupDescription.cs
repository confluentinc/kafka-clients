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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A detailed description of a single consumer group in the cluster — the .NET realization
/// of Java's <c>org.apache.kafka.clients.admin.ConsumerGroupDescription</c>
/// (<c>ConsumerGroupDescription.java:37, :103, :126, :143, :151, :158, :165, :172, :180,
/// :190, :197, :204, :211, :220, :229, :234</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>The type is not deprecated</b>, although one of its members is: Java's class carries
/// no <c>@Deprecated</c>, so no <see cref="ObsoleteAttribute"/> appears on it — only on
/// <see cref="State"/>, which Java deprecates at <c>:189</c>. Contrast
/// <see cref="ConsumerGroupListing"/>, whose Java class <em>is</em> deprecated and which
/// mirrors that.
/// </para>
/// <para>
/// ⚠ <b>Java's three deprecated constructors are deliberately not translated</b> — the
/// standing ruling this phase applied to <see cref="ConsumerGroupListing"/>'s two and
/// <see cref="MemberDescription"/>'s four, for the same reason. All three (<c>:54</c>,
/// <c>:68</c> and <c>:83</c>, every one <c>since = "4.0", forRemoval = true</c>) take the
/// superseded <see cref="Confluent.Kafka.ConsumerGroupState"/> and fill the arguments they
/// omit with fixed values — <c>Collections.emptySet()</c>, <c>GroupType.CLASSIC</c> and two
/// <c>Optional.empty()</c>. A caller can pass those same values to the current constructor
/// and get the identical object. Nothing expressible is lost.
/// </para>
/// <para>
/// ⚠ <b><see cref="Type"/> and <see cref="GroupState"/> are <em>not</em> optional here,
/// although the same two members are optional on <see cref="ConsumerGroupListing"/>.</b>
/// Java's accessors return a bare <c>GroupType</c> / <c>GroupState</c> (<c>:180</c>,
/// <c>:197</c>) where that class returns <c>Optional</c>, so these are non-nullable enums.
/// The asymmetry is Java's and is mirrored rather than normalized. Java's field could still
/// hold <see langword="null"/> — the constructor stores it raw — but a C# enum is a value
/// type with no null to store, so the difference is unreachable from this surface. Java's
/// own "defaults to Classic if not provided by the server" (<c>:177-178</c>) happens on the
/// decode path, not in the constructor.
/// </para>
/// <para>
/// ⚠ <b><see cref="AuthorizedOperations"/> distinguishes <em>absent</em> from
/// <em>empty</em>.</b> Java's accessor documents "or <b>null</b> if that information is not
/// known" (<c>:209</c>), which is the case where the broker was never asked or did not
/// answer — <em>not</em> the case where it answered "none are authorized". The C ABI
/// carries the discriminant explicitly (a <c>has_authorized_operations</c> flag beside the
/// count) because a count of 0 covers both, so collapsing <see langword="null"/> into an
/// empty collection would discard information the ABI deliberately preserved. The same
/// reading <see cref="TopicDescription.AuthorizedOperations"/> already makes.
/// </para>
/// <para>
/// <b>Java's <c>Optional&lt;Integer&gt;</c> becomes a C# nullable.</b>
/// <see cref="GroupEpoch"/> and <see cref="TargetAssignmentEpoch"/> are set for a
/// <see cref="GroupType.Consumer"/> group and empty for a <see cref="GroupType.Classic"/>
/// one (<c>:217-218</c>), and the C ABI reports each as a presence flag beside the value —
/// so they are nullable rather than carrying a sentinel: no epoch value could stand in for
/// "absent".
/// </para>
/// <para>
/// ⚠ <b>Null arguments are accepted and coalesced, not rejected — but only three of
/// them.</b> <see cref="GroupId"/> and <see cref="PartitionAssignor"/> become the empty
/// string and <see cref="Members"/> becomes an empty collection, mirroring Java
/// <c>:113-116</c>. The remaining six are stored exactly as given, which is what makes the
/// absent-versus-empty and absent-versus-zero distinctions above survive. As on
/// <see cref="MemberDescription"/>, this differs from the
/// <see cref="ArgumentNullException"/> guard <see cref="ConsumerGroupListing"/> applies
/// because Java itself differs — coalescing here, rejecting there.
/// </para>
/// </remarks>
public sealed class ConsumerGroupDescription
{
    // Java stores List.copyOf(members) (:115), so this is ordered and its equality is
    // positional — see the note on Members.
    private readonly List<MemberDescription> _members;

    // Null means "the broker reported nothing", which is not the same as an empty set —
    // see the absent-versus-empty note in the type remarks.
    private readonly List<AclOperation>? _authorizedOperations;

    /// <summary>
    /// Creates a description — Java's current constructor
    /// (<c>ConsumerGroupDescription.java:103</c>).
    /// </summary>
    /// <param name="groupId">The group id. Null becomes the empty string.</param>
    /// <param name="isSimpleConsumerGroup">Whether the group is a simple consumer group.</param>
    /// <param name="members">
    /// The members of the group. Null becomes an empty collection; the collection is
    /// copied.
    /// </param>
    /// <param name="partitionAssignor">
    /// The group's partition assignor. Null becomes the empty string.
    /// </param>
    /// <param name="type">The group type (or protocol) of the group.</param>
    /// <param name="groupState">The group state.</param>
    /// <param name="coordinator">
    /// The group coordinator, or <see langword="null"/> if it is not known.
    /// </param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> if the broker reported none at
    /// all. Duplicates are collapsed and the collection is copied; see the
    /// absent-versus-empty note in the type remarks.
    /// </param>
    /// <param name="groupEpoch">
    /// The group epoch, or <see langword="null"/> for a classic group.
    /// </param>
    /// <param name="targetAssignmentEpoch">
    /// The target-assignment epoch, or <see langword="null"/> for a classic group.
    /// </param>
    public ConsumerGroupDescription(
        string? groupId,
        bool isSimpleConsumerGroup,
        IEnumerable<MemberDescription>? members,
        string? partitionAssignor,
        GroupType type,
        GroupState groupState,
        Node? coordinator,
        IEnumerable<AclOperation>? authorizedOperations,
        int? groupEpoch,
        int? targetAssignmentEpoch)
    {
        // Java :113-122 — the id, the members and the assignor coalesce; the other six are
        // stored as given.
        GroupId = groupId ?? string.Empty;
        IsSimpleConsumerGroup = isSimpleConsumerGroup;
        _members = members is null
            ? new List<MemberDescription>()
            : new List<MemberDescription>(members);
        PartitionAssignor = partitionAssignor ?? string.Empty;
        Type = type;
        GroupState = groupState;
        Coordinator = coordinator;
        _authorizedOperations = CopyAsSet(authorizedOperations);
        GroupEpoch = groupEpoch;
        TargetAssignmentEpoch = targetAssignmentEpoch;
    }

    /// <summary>
    /// The id of the consumer group — Java's <c>groupId()</c> (<c>:151</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// Whether the consumer group is simple — Java's <c>isSimpleConsumerGroup()</c>
    /// (<c>:158</c>).
    /// </summary>
    public bool IsSimpleConsumerGroup { get; }

    /// <summary>
    /// The members of the consumer group — Java's <c>members()</c> (<c>:165</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// Java's accessor declares a <c>Collection</c>, so this is an
    /// <c>IReadOnlyCollection&lt;T&gt;</c> rather than a list. The value Java stores is a
    /// <c>List.copyOf(...)</c> (<c>:115</c>) all the same, which is why
    /// <see cref="Equals(object)"/> compares the members <b>positionally</b> — Java's
    /// <c>Objects.equals</c> reaches <c>List.equals</c> there, and a list is order-sensitive
    /// where a set would not be.
    /// </remarks>
    public IReadOnlyCollection<MemberDescription> Members => _members;

    /// <summary>
    /// The consumer group partition assignor — Java's <c>partitionAssignor()</c>
    /// (<c>:172</c>). Never <see langword="null"/>.
    /// </summary>
    public string PartitionAssignor { get; }

    /// <summary>
    /// The group type (or the protocol) of this consumer group — Java's <c>type()</c>
    /// (<c>:180</c>). It defaults to <see cref="GroupType.Classic"/> if not provided by the
    /// server. Not optional; see the asymmetry note in the type remarks.
    /// </summary>
    public GroupType Type { get; }

    /// <summary>
    /// The group state, or <see cref="Confluent.Kafka.GroupState.Unknown"/> if the state is
    /// too new for this client to parse — Java's <c>groupState()</c> (<c>:197</c>). Not
    /// optional; see the asymmetry note in the type remarks.
    /// </summary>
    public GroupState GroupState { get; }

    /// <summary>
    /// The consumer group coordinator, or <see langword="null"/> if the coordinator is not
    /// known — Java's <c>coordinator()</c> (<c>:204</c>).
    /// </summary>
    public Node? Coordinator { get; }

    /// <summary>
    /// The operations the caller is authorized to perform on this group, or
    /// <see langword="null"/> if the broker reported none at all — Java's
    /// <c>authorizedOperations()</c> (<c>:211</c>). An <b>empty</b> collection means
    /// "reported, and none are authorized"; see the absent-versus-empty note in the type
    /// remarks.
    /// </summary>
    /// <remarks>
    /// Java returns a <c>Set</c>; <c>IReadOnlySet&lt;T&gt;</c> post-dates the netstandard2.0
    /// floor, so this is an <c>IReadOnlyCollection&lt;T&gt;</c> — the same substitution
    /// <see cref="TopicDescription.AuthorizedOperations"/> and
    /// <see cref="MemberAssignment.TopicPartitions"/> already make. The <em>set</em>
    /// semantics are not lost: the constructor collapses duplicates, and
    /// <see cref="Equals(object)"/> / <see cref="GetHashCode"/> compare and hash this member
    /// order-insensitively, exactly as a Java <c>Set</c> does.
    /// </remarks>
    public IReadOnlyCollection<AclOperation>? AuthorizedOperations => _authorizedOperations;

    /// <summary>
    /// The epoch of the consumer group, or <see langword="null"/> for a classic group —
    /// Java's <c>groupEpoch()</c> (<c>:220</c>).
    /// </summary>
    public int? GroupEpoch { get; }

    /// <summary>
    /// The epoch of the target assignment, or <see langword="null"/> for a classic group —
    /// Java's <c>targetAssignmentEpoch()</c> (<c>:229</c>).
    /// </summary>
    public int? TargetAssignmentEpoch { get; }

    /// <summary>
    /// The group state on the superseded <see cref="Confluent.Kafka.ConsumerGroupState"/>
    /// axis — Java's deprecated <c>state()</c> (<c>:190</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This is a lossy, one-way <em>projection</em> of <see cref="GroupState"/>, not a
    /// second stored field</b>, exactly as Java computes it at <c>:191</c>:
    /// <c>ConsumerGroupState.parse(groupState.toString())</c>. It takes no constructor
    /// argument for the same reason Java's takes none, and it is absent from
    /// <see cref="Equals(object)"/> because Java's <c>equals</c> omits it too
    /// (<c>:130-139</c>) — including it would state the same fact twice. The C ABI exports
    /// it as a second entry point beside the group state, but two exported accessors are
    /// not two independent fields: C cannot express Java's <c>parse</c>-of-<c>toString</c>
    /// composition, so the flattened boundary spells the projection as its own function.
    /// </para>
    /// <para>
    /// <b>The two agree everywhere except one member, where they must not.</b>
    /// <see cref="Confluent.Kafka.GroupState.NotReady"/> has no counterpart on
    /// <see cref="Confluent.Kafka.ConsumerGroupState"/> (Java never added one), so it
    /// projects to <see cref="Confluent.Kafka.ConsumerGroupState.Unknown"/> — Java's
    /// <c>parse</c> answering <c>UNKNOWN</c> for a name it does not recognise
    /// (<c>ConsumerGroupState.java:53-56</c>), which is what the accessor's own javadoc
    /// means by "or UNKNOWN if the state is too new for us to parse". That is why this
    /// member is kept rather than collapsed into <see cref="GroupState"/>: the projection
    /// loses information and cannot be run backwards.
    /// </para>
    /// <para>
    /// ⚠ <b>Unlike <see cref="ConsumerGroupListing.State"/> this one is not nullable</b>,
    /// because the member it projects is not: Java maps an <c>Optional</c> there and parses
    /// a bare value here. Routing through <c>GroupMarshal</c> — Java's <c>toString()</c>
    /// then its <c>parse</c>, composed — keeps the name table in one place rather than
    /// adding a second mapping that could drift from the one the ABI path uses.
    /// </para>
    /// </remarks>
    [Obsolete("Deprecated in Kafka since 4.0. Use GroupState instead.")]
    public ConsumerGroupState State =>
        GroupMarshal.ParseConsumerState(GroupMarshal.NameFromState(GroupState) ?? "Unknown");

    /// <summary>
    /// Compares all ten stored members — Java's <c>equals</c> (<c>:126</c>).
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when every member matches.</returns>
    /// <remarks>
    /// <para>
    /// Java's <c>getClass() != o.getClass()</c> check (<c>:128</c>) is met by the type being
    /// <see langword="sealed"/>. Strings compare ordinally, as Java's <c>String.equals</c>
    /// does. <see cref="State"/> is deliberately absent — it is derived; see its own
    /// remarks.
    /// </para>
    /// <para>
    /// ⚠ <b><see cref="Coordinator"/> is compared field-by-field here rather than through
    /// <see cref="object.Equals(object)"/>.</b> Java's <c>Objects.equals(coordinator, ...)</c>
    /// reaches <c>Node.equals</c>, which compares by value (<c>Node.java:143-154</c>), but
    /// this binding's <see cref="Node"/> deliberately carries no value equality — a recorded
    /// choice on that type, which this slice does not re-author. Performing the comparison
    /// here restores Java's answer for this class without changing a shipped type; it covers
    /// the four members <see cref="Node"/> models, Java's fifth (<c>isFenced</c>) having no
    /// counterpart on it.
    /// </para>
    /// </remarks>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is ConsumerGroupDescription other
            && IsSimpleConsumerGroup == other.IsSimpleConsumerGroup
            && string.Equals(GroupId, other.GroupId, StringComparison.Ordinal)
            && _members.SequenceEqual(other._members)
            && string.Equals(PartitionAssignor, other.PartitionAssignor, StringComparison.Ordinal)
            && Type == other.Type
            && GroupState == other.GroupState
            && CoordinatorsMatch(Coordinator, other.Coordinator)
            && OperationsMatch(_authorizedOperations, other._authorizedOperations)
            && GroupEpoch == other.GroupEpoch
            && TargetAssignmentEpoch == other.TargetAssignmentEpoch;
    }

    /// <summary>
    /// The hash of the same ten members — Java's <c>Objects.hash(...)</c> (<c>:144</c>),
    /// whose 31-fold this reproduces in Java's argument order.
    /// </summary>
    /// <returns>The hash code.</returns>
    /// <remarks>
    /// The two collections fold the way Java's own collections hash: the members
    /// positionally (Java holds a <c>List</c>) and the authorized operations as a sum (Java
    /// holds a <c>Set</c>), so each agrees with the corresponding half of
    /// <see cref="Equals(object)"/>. An absent nullable hashes 0, which is what
    /// <c>Objects.hash</c> gives an empty <c>Optional</c> and a null reference alike.
    /// </remarks>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(GroupId);
            hash = (hash * 31) + IsSimpleConsumerGroup.GetHashCode();
            hash = (hash * 31) + MembersHash(_members);
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(PartitionAssignor);
            hash = (hash * 31) + ((int)Type).GetHashCode();
            hash = (hash * 31) + ((int)GroupState).GetHashCode();
            hash = (hash * 31) + CoordinatorHash(Coordinator);
            hash = (hash * 31) + OperationsHash(_authorizedOperations);
            hash = (hash * 31) + GroupEpoch.GetHashCode();
            hash = (hash * 31) + TargetAssignmentEpoch.GetHashCode();
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:234</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Java renders absence three different ways in this one method, and each is
    /// mirrored.</b> The members join each element's own <c>toString()</c> with <c>","</c>
    /// and no space (<c>:237</c>), so an empty group renders as <c>members=</c>. An absent
    /// coordinator or authorized-operations set concatenates as the bare text <c>null</c>
    /// (<c>:241-242</c>), because Java is concatenating a null <em>reference</em>. The two
    /// epochs go through <c>orElse(null)</c> (<c>:243-244</c>), whose null result
    /// concatenates as that same text — the same spelling reached by a different route, and
    /// <b>not</b> the <c>Optional.empty</c> that <see cref="ConsumerGroupListing"/> and
    /// <see cref="MemberDescription"/> emit for members Java concatenates raw. Normalizing
    /// any of them would produce a diagnostic string that no longer matches the Java
    /// client's.
    /// </para>
    /// <para>
    /// <see cref="Type"/> and <see cref="GroupState"/> render through the shared name tables,
    /// which reproduce Java's <c>toString()</c> for those enums (<c>Classic</c>,
    /// <c>Stable</c>, …) rather than the C# member spelling; a value no member defines takes
    /// <c>Unknown</c>, the answer those tables give it everywhere else.
    /// <see cref="IsSimpleConsumerGroup"/> renders lowercase, as Java's
    /// <c>Boolean.toString()</c> does.
    /// </para>
    /// <para>
    /// ⚠ <b>The authorized operations render with their C# member names</b> —
    /// <c>[Read, Write]</c> where Java prints <c>[READ, WRITE]</c>, its <c>AclOperation</c>
    /// having no <c>toString()</c> override and so falling back to the constant name. This
    /// is the one place the rendering is <em>not</em> Java's spelling, and it is deliberate:
    /// <see cref="TopicDescription.ToString"/> already renders this same Java field this way,
    /// and a second spelling for one field across two sibling description types would be
    /// worse than one uniform approximation. There is no ACL name table to share, and adding
    /// one belongs with the ACL RPCs that bring the rest of that family.
    /// </para>
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(groupId={0}, isSimpleConsumerGroup={1}, members={2}, partitionAssignor={3}, " +
            "type={4}, groupState={5}, coordinator={6}, authorizedOperations={7}, " +
            "groupEpoch={8}, targetAssignmentEpoch={9})",
            GroupId,
            IsSimpleConsumerGroup ? "true" : "false",
            string.Join(",", _members.Select(static member => member.ToString())),
            PartitionAssignor,
            GroupMarshal.NameFromType(Type) ?? "Unknown",
            GroupMarshal.NameFromState(GroupState) ?? "Unknown",
            Coordinator is null ? "null" : Coordinator.ToString(),
            _authorizedOperations is null ? "null" : "[" + string.Join(", ", _authorizedOperations) + "]",
            GroupEpoch is null ? "null" : GroupEpoch.Value.ToString(CultureInfo.InvariantCulture),
            TargetAssignmentEpoch is null
                ? "null"
                : TargetAssignmentEpoch.Value.ToString(CultureInfo.InvariantCulture));

    /// <summary>
    /// Copies an authorized-operations argument the way Java's <c>Set</c> parameter behaves —
    /// absent stays absent, and duplicates collapse while first-seen order is kept for
    /// rendering.
    /// </summary>
    /// <param name="operations">The operations, or null for absent.</param>
    /// <returns>The copy, or null when the argument was absent.</returns>
    private static List<AclOperation>? CopyAsSet(IEnumerable<AclOperation>? operations)
    {
        if (operations is null)
        {
            return null;
        }

        var seen = new HashSet<AclOperation>();
        var copy = new List<AclOperation>();
        foreach (var operation in operations)
        {
            if (seen.Add(operation))
            {
                copy.Add(operation);
            }
        }

        return copy;
    }

    /// <summary>
    /// Compares two possibly-absent coordinators by value, mirroring Java's
    /// <c>Node.equals</c> over the four members this binding's <see cref="Node"/> models.
    /// </summary>
    /// <param name="left">The first coordinator, or null for absent.</param>
    /// <param name="right">The second coordinator, or null for absent.</param>
    /// <returns>True when both are absent or both are present and equal.</returns>
    private static bool CoordinatorsMatch(Node? left, Node? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        return left is not null
            && right is not null
            && left.Id == right.Id
            && left.Port == right.Port
            && string.Equals(left.Host, right.Host, StringComparison.Ordinal)
            && string.Equals(left.Rack, right.Rack, StringComparison.Ordinal);
    }

    /// <summary>
    /// The hash of a possibly-absent coordinator, over the same members
    /// <see cref="CoordinatorsMatch(Node, Node)"/> compares.
    /// </summary>
    /// <param name="coordinator">The coordinator, or null for absent.</param>
    /// <returns>The hash code, or 0 when absent.</returns>
    private static int CoordinatorHash(Node? coordinator)
    {
        if (coordinator is null)
        {
            return 0;
        }

        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + coordinator.Id;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(coordinator.Host);
            hash = (hash * 31) + coordinator.Port;
            hash = (hash * 31)
                + (coordinator.Rack is null ? 0 : StringComparer.Ordinal.GetHashCode(coordinator.Rack));
            return hash;
        }
    }

    /// <summary>
    /// Compares two possibly-absent authorized-operations sets the way Java's
    /// <c>Objects.equals</c> over a <c>Set</c> does — absent equals absent, and two present
    /// ones compare order-insensitively.
    /// </summary>
    /// <param name="left">The first set, or null for absent.</param>
    /// <param name="right">The second set, or null for absent.</param>
    /// <returns>True when both are absent or both are present and hold the same elements.</returns>
    private static bool OperationsMatch(List<AclOperation>? left, List<AclOperation>? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        return left.Count == right.Count && new HashSet<AclOperation>(left).SetEquals(right);
    }

    /// <summary>
    /// The positional 31-fold Java's <c>List.hashCode()</c> specifies, so that it agrees
    /// with the order-sensitive member comparison in <see cref="Equals(object)"/>.
    /// </summary>
    /// <param name="members">The members to hash.</param>
    /// <returns>The hash code.</returns>
    private static int MembersHash(List<MemberDescription> members)
    {
        unchecked
        {
            int hash = 1;
            foreach (var member in members)
            {
                hash = (hash * 31) + (member is null ? 0 : member.GetHashCode());
            }

            return hash;
        }
    }

    /// <summary>
    /// The element-hash sum Java's <c>Set.hashCode()</c> specifies — order-insensitive, so
    /// that it agrees with <see cref="OperationsMatch(List{AclOperation}, List{AclOperation})"/>.
    /// </summary>
    /// <param name="operations">The operations to hash, or null for absent.</param>
    /// <returns>The hash code, or 0 when absent.</returns>
    private static int OperationsHash(List<AclOperation>? operations)
    {
        if (operations is null)
        {
            return 0;
        }

        unchecked
        {
            int hash = 0;
            foreach (var operation in operations)
            {
                hash += ((int)operation).GetHashCode();
            }

            return hash;
        }
    }
}
