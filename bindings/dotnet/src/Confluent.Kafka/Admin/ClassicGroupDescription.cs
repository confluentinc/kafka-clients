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
/// A detailed description of a single classic group in the cluster — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.ClassicGroupDescription</c>
/// (<c>ClassicGroupDescription.java:33, :42, :51, :68, :83, :89, :96, :105, :112, :119,
/// :126, :133, :140, :145</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Both of Java's constructors ship, unlike on the sibling description types.</b>
/// Neither carries <c>@Deprecated</c> — this file has no deprecation anywhere — so the
/// standing ruling that omitted <see cref="ConsumerGroupDescription"/>'s three,
/// <see cref="MemberDescription"/>'s four and <see cref="ConsumerGroupListing"/>'s two does
/// not reach here: that ruling turns on the omitted constructors being <em>deprecated</em>
/// forwarders, not on their being forwarders. The six-argument form (<c>:42</c>) is current
/// Java API, so it is translated as a current C# overload.
/// </para>
/// <para>
/// ⚠ <b>The six-argument constructor yields <em>empty</em> authorized operations, not
/// absent ones.</b> Java forwards <c>Set.of()</c> (<c>:48</c>) — an empty set, not
/// <see langword="null"/> — so an object built through it answers
/// <see cref="AuthorizedOperations"/> with an empty collection and never with
/// <see langword="null"/>. The distinction is invisible in Java's own signature and is the
/// one detail of that forwarder worth stating: passing <see langword="null"/> through
/// instead would silently convert "the broker reported none are authorized" into "the
/// broker was never asked".
/// </para>
/// <para>
/// ⚠ <b><see cref="AuthorizedOperations"/> distinguishes <em>absent</em> from
/// <em>empty</em>.</b> Java's accessor documents "or <b>null</b> if that information is not
/// known" (<c>:138</c>), which is the case where the broker was never asked or did not
/// answer — <em>not</em> the case where it answered "none are authorized". The C ABI
/// carries the discriminant explicitly (a <c>has_authorized_operations</c> flag beside the
/// count) because a count of 0 covers both, so collapsing <see langword="null"/> into an
/// empty collection would discard information the ABI deliberately preserved. The same
/// reading <see cref="TopicDescription.AuthorizedOperations"/> and
/// <see cref="ConsumerGroupDescription.AuthorizedOperations"/> already make.
/// </para>
/// <para>
/// ⚠ <b>Null arguments are accepted and coalesced, not rejected — but only two of
/// them.</b> <see cref="GroupId"/> and <see cref="ProtocolData"/> become the empty string
/// (Java <c>:58</c>, <c>:60</c>) and <see cref="Members"/> becomes an empty collection and
/// is copied (<c>:61</c>). The remaining four are stored exactly as given, which is what
/// makes the absent-versus-empty distinction above survive. As on
/// <see cref="MemberDescription"/> and <see cref="ConsumerGroupDescription"/>, this differs
/// from the <see cref="ArgumentNullException"/> guard <see cref="ConsumerGroupListing"/>
/// applies because Java itself differs — coalescing here, rejecting there. No guards Java
/// does not have.
/// </para>
/// <para>
/// ⚠ <b><see cref="Protocol"/> is the one string Java does <em>not</em> coalesce
/// (<c>:59</c>), and also the one it later dereferences</b> — see
/// <see cref="IsSimpleConsumerGroup"/>. It is therefore nullable here where its two
/// neighbours are not; normalizing it would change what <c>protocol()</c>,
/// <see cref="ToString"/> and <see cref="IsSimpleConsumerGroup"/> all report.
/// </para>
/// <para>
/// ⚠ <b><see cref="State"/> is not optional, and there is no <c>GroupState</c> member
/// beside it.</b> Java exposes exactly one state accessor here (<c>:126</c>) and does not
/// deprecate it, where <see cref="ConsumerGroupDescription"/> exposes a current
/// <c>groupState()</c> plus a deprecated <c>state()</c>; the asymmetry is Java's and is
/// mirrored rather than normalized. Java's field could still hold <see langword="null"/> —
/// the constructor stores it raw — but a C# enum is a value type with no null to store, so
/// the difference is unreachable from this surface.
/// </para>
/// </remarks>
public sealed class ClassicGroupDescription
{
    // Java stores List.copyOf(members) (:61), so this is ordered and its equality is
    // positional — see the note on Members.
    private readonly List<MemberDescription> _members;

    // Null means "the broker reported nothing", which is not the same as an empty set —
    // see the absent-versus-empty note in the type remarks.
    private readonly List<AclOperation>? _authorizedOperations;

    /// <summary>
    /// Creates a description whose authorized operations are reported-but-empty — Java's
    /// six-argument constructor (<c>ClassicGroupDescription.java:42</c>).
    /// </summary>
    /// <param name="groupId">The group id. Null becomes the empty string.</param>
    /// <param name="protocol">The group protocol type. Null is stored as given.</param>
    /// <param name="protocolData">The group protocol data. Null becomes the empty string.</param>
    /// <param name="members">
    /// The members of the group. Null becomes an empty collection; the collection is
    /// copied.
    /// </param>
    /// <param name="state">The classic group state.</param>
    /// <param name="coordinator">
    /// The group coordinator, or <see langword="null"/> if it is not known.
    /// </param>
    /// <remarks>
    /// Java forwards <c>Set.of()</c> (<c>:48</c>), so <see cref="AuthorizedOperations"/> is
    /// an <b>empty</b> collection on the resulting object rather than
    /// <see langword="null"/>; see the type remarks.
    /// </remarks>
    public ClassicGroupDescription(
        string? groupId,
        string? protocol,
        string? protocolData,
        IEnumerable<MemberDescription>? members,
        ClassicGroupState state,
        Node? coordinator)
        : this(
            groupId,
            protocol,
            protocolData,
            members,
            state,
            coordinator,
            Array.Empty<AclOperation>())
    {
    }

    /// <summary>
    /// Creates a description — Java's seven-argument constructor
    /// (<c>ClassicGroupDescription.java:51</c>).
    /// </summary>
    /// <param name="groupId">The group id. Null becomes the empty string.</param>
    /// <param name="protocol">The group protocol type. Null is stored as given.</param>
    /// <param name="protocolData">The group protocol data. Null becomes the empty string.</param>
    /// <param name="members">
    /// The members of the group. Null becomes an empty collection; the collection is
    /// copied.
    /// </param>
    /// <param name="state">The classic group state.</param>
    /// <param name="coordinator">
    /// The group coordinator, or <see langword="null"/> if it is not known.
    /// </param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> if the broker reported none at
    /// all. Duplicates are collapsed and the collection is copied; see the
    /// absent-versus-empty note in the type remarks.
    /// </param>
    public ClassicGroupDescription(
        string? groupId,
        string? protocol,
        string? protocolData,
        IEnumerable<MemberDescription>? members,
        ClassicGroupState state,
        Node? coordinator,
        IEnumerable<AclOperation>? authorizedOperations)
    {
        // Java :58-64 — the id, the protocol data and the members coalesce; the other four
        // are stored as given.
        GroupId = groupId ?? string.Empty;
        Protocol = protocol;
        ProtocolData = protocolData ?? string.Empty;
        _members = members is null
            ? new List<MemberDescription>()
            : new List<MemberDescription>(members);
        State = state;
        Coordinator = coordinator;
        _authorizedOperations = CopyAsSet(authorizedOperations);
    }

    /// <summary>
    /// The id of the classic group — Java's <c>groupId()</c> (<c>:89</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// The group protocol type — Java's <c>protocol()</c> (<c>:96</c>). May be
    /// <see langword="null"/>; see the note in the type remarks.
    /// </summary>
    public string? Protocol { get; }

    /// <summary>
    /// The group protocol data — Java's <c>protocolData()</c> (<c>:105</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// The meaning depends on the group protocol type. For a classic consumer group this is
    /// the partition assignor name; for a classic connect group it indicates which Connect
    /// protocols are enabled.
    /// </remarks>
    public string ProtocolData { get; }

    /// <summary>
    /// Whether the group is a simple consumer group — Java's
    /// <c>isSimpleConsumerGroup()</c> (<c>:112</c>).
    /// </summary>
    /// <exception cref="NullReferenceException">
    /// <see cref="Protocol"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// ⚠ <b>This is a <em>projection</em> over <see cref="Protocol"/>, not a stored
    /// field</b>, exactly as Java computes it at <c>:113</c>: <c>protocol.isEmpty()</c>. It
    /// takes no constructor argument for the same reason Java's takes none. Contrast
    /// <see cref="ConsumerGroupDescription.IsSimpleConsumerGroup"/>, which Java <em>does</em>
    /// store. The C ABI exports this as its own accessor beside the protocol, but two
    /// exported entry points are not two independent fields — the flattened boundary has no
    /// way to spell a derived member other than as a function. Java also has no null guard
    /// here, so a null protocol faults rather than answering; adding a guard would invent an
    /// answer Java does not give.
    /// </remarks>
    public bool IsSimpleConsumerGroup => Protocol!.Length == 0;

    /// <summary>
    /// The members of the classic group — Java's <c>members()</c> (<c>:119</c>). Never
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// Java's accessor declares a <c>Collection</c>, so this is an
    /// <c>IReadOnlyCollection&lt;T&gt;</c> rather than a list. The value Java stores is a
    /// <c>List.copyOf(...)</c> (<c>:61</c>) all the same, which is why
    /// <see cref="Equals(object)"/> compares the members <b>positionally</b> — Java's
    /// <c>Objects.equals</c> reaches <c>List.equals</c> there, and a list is order-sensitive
    /// where a set would not be.
    /// </remarks>
    public IReadOnlyCollection<MemberDescription> Members => _members;

    /// <summary>
    /// The classic group state, or <see cref="ClassicGroupState.Unknown"/> if the state is
    /// too new for this client to parse — Java's <c>state()</c> (<c>:126</c>). Not
    /// optional; see the asymmetry note in the type remarks.
    /// </summary>
    public ClassicGroupState State { get; }

    /// <summary>
    /// The classic group coordinator, or <see langword="null"/> if the coordinator is not
    /// known — Java's <c>coordinator()</c> (<c>:133</c>).
    /// </summary>
    public Node? Coordinator { get; }

    /// <summary>
    /// The operations the caller is authorized to perform on this group, or
    /// <see langword="null"/> if the broker reported none at all — Java's
    /// <c>authorizedOperations()</c> (<c>:140</c>). An <b>empty</b> collection means
    /// "reported, and none are authorized"; see the absent-versus-empty note in the type
    /// remarks.
    /// </summary>
    /// <remarks>
    /// Java returns a <c>Set</c>; <c>IReadOnlySet&lt;T&gt;</c> post-dates the netstandard2.0
    /// floor, so this is an <c>IReadOnlyCollection&lt;T&gt;</c> — the same substitution
    /// <see cref="ConsumerGroupDescription.AuthorizedOperations"/> and
    /// <see cref="TopicDescription.AuthorizedOperations"/> already make. The <em>set</em>
    /// semantics are not lost: the constructor collapses duplicates, and
    /// <see cref="Equals(object)"/> / <see cref="GetHashCode"/> compare and hash this member
    /// order-insensitively, exactly as a Java <c>Set</c> does.
    /// </remarks>
    public IReadOnlyCollection<AclOperation>? AuthorizedOperations => _authorizedOperations;

    /// <summary>
    /// Compares all seven stored members — Java's <c>equals</c> (<c>:68</c>).
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when every member matches.</returns>
    /// <remarks>
    /// <para>
    /// Java's <c>getClass() != o.getClass()</c> check (<c>:70</c>) is met by the type being
    /// <see langword="sealed"/>. Strings compare ordinally, as Java's <c>String.equals</c>
    /// does. <see cref="IsSimpleConsumerGroup"/> is deliberately absent — it is derived from
    /// <see cref="Protocol"/>, which is compared, so including it would state the same fact
    /// twice; Java's <c>equals</c> omits it for that reason too.
    /// </para>
    /// <para>
    /// ⚠ <b><see cref="Coordinator"/> is compared field-by-field here rather than through
    /// <see cref="object.Equals(object)"/>.</b> Java's <c>Objects.equals(coordinator, ...)</c>
    /// (<c>:77</c>) reaches <c>Node.equals</c>, which compares by value
    /// (<c>Node.java:143-154</c>), but this binding's <see cref="Node"/> deliberately carries
    /// no value equality — a recorded choice on that type, which this slice does not
    /// re-author. Performing the comparison here restores Java's answer for this class
    /// without changing a shipped type; it covers the four members <see cref="Node"/>
    /// models, Java's fifth (<c>isFenced</c>) having no counterpart on it. The same
    /// reasoning, and the same helpers, as on
    /// <see cref="ConsumerGroupDescription.Equals(object)"/>; they stay private per type
    /// rather than being hoisted, because hoisting them would re-author that shipped file.
    /// </para>
    /// </remarks>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is ClassicGroupDescription other
            && string.Equals(GroupId, other.GroupId, StringComparison.Ordinal)
            && string.Equals(Protocol, other.Protocol, StringComparison.Ordinal)
            && string.Equals(ProtocolData, other.ProtocolData, StringComparison.Ordinal)
            && _members.SequenceEqual(other._members)
            && State == other.State
            && CoordinatorsMatch(Coordinator, other.Coordinator)
            && OperationsMatch(_authorizedOperations, other._authorizedOperations);
    }

    /// <summary>
    /// The hash of the same seven members — Java's <c>Objects.hash(...)</c> (<c>:83</c>),
    /// whose 31-fold this reproduces in Java's argument order.
    /// </summary>
    /// <returns>The hash code.</returns>
    /// <remarks>
    /// The two collections fold the way Java's own collections hash: the members
    /// positionally (Java holds a <c>List</c>) and the authorized operations as a sum (Java
    /// holds a <c>Set</c>), so each agrees with the corresponding half of
    /// <see cref="Equals(object)"/>. An absent member hashes 0, which is what
    /// <c>Objects.hash</c> gives a null reference.
    /// </remarks>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(GroupId);
            hash = (hash * 31) + (Protocol is null ? 0 : StringComparer.Ordinal.GetHashCode(Protocol));
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(ProtocolData);
            hash = (hash * 31) + MembersHash(_members);
            hash = (hash * 31) + ((int)State).GetHashCode();
            hash = (hash * 31) + CoordinatorHash(Coordinator);
            hash = (hash * 31) + OperationsHash(_authorizedOperations);
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:145</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Only <c>protocol</c> is quoted, and only <c>members</c> is joined.</b> Java
    /// wraps the protocol in single quotes (<c>:147</c>) but leaves <c>protocolData</c> bare
    /// one line below, and joins each member's own <c>toString()</c> with <c>","</c> and no
    /// space (<c>:149</c>), so an empty group renders as <c>members=</c>. An absent protocol,
    /// coordinator or authorized-operations set concatenates as the bare text <c>null</c>,
    /// because Java is concatenating a null <em>reference</em>. Normalizing any of these
    /// would produce a diagnostic string that no longer matches the Java client's.
    /// </para>
    /// <para>
    /// <see cref="State"/> renders through the shared name table, which reproduces Java's
    /// <c>ClassicGroupState.toString()</c> (<c>Stable</c>, <c>PreparingRebalance</c>, …)
    /// rather than the C# member spelling; a value no member defines takes <c>Unknown</c>,
    /// the answer that table gives it everywhere else.
    /// </para>
    /// <para>
    /// ⚠ <b>The authorized operations render with their C# member names</b> —
    /// <c>[Read, Write]</c> where Java prints <c>[READ, WRITE]</c>, its <c>AclOperation</c>
    /// having no <c>toString()</c> override and so falling back to the constant name. This
    /// is the one place the rendering is <em>not</em> Java's spelling, and it is deliberate:
    /// <see cref="TopicDescription.ToString"/> and
    /// <see cref="ConsumerGroupDescription.ToString"/> already render this same Java field
    /// this way, and a third spelling for one field across three sibling description types
    /// would be worse than one uniform approximation.
    /// </para>
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(groupId={0}, protocol='{1}', protocolData={2}, members={3}, state={4}, " +
            "coordinator={5}, authorizedOperations={6})",
            GroupId,
            Protocol ?? "null",
            ProtocolData,
            string.Join(",", _members.Select(static member => member.ToString())),
            GroupMarshal.NameFromClassicState(State) ?? "Unknown",
            Coordinator is null ? "null" : Coordinator.ToString(),
            _authorizedOperations is null ? "null" : "[" + string.Join(", ", _authorizedOperations) + "]");

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
