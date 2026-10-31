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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A detailed description of a single classic group in the cluster — Java's
/// <c>org.apache.kafka.clients.admin.ClassicGroupDescription</c>.
/// </summary>
/// <remarks>
/// <para>
/// Both of Java's constructors are translated, as neither is deprecated.
/// </para>
/// <para>
/// <see cref="AuthorizedOperations"/> is <see langword="null"/> when the broker reported none at
/// all, and empty when it reported that none are authorized, as in Java.
/// </para>
/// <para>
/// Null arguments are coalesced as in Java: <see cref="GroupId"/> and <see cref="ProtocolData"/>
/// become the empty string and <see cref="Members"/> an empty collection; the other four are
/// stored as given, so <see cref="Protocol"/> may be <see langword="null"/>.
/// </para>
/// </remarks>
public sealed class ClassicGroupDescription
{
    private readonly List<MemberDescription> _members;

    private readonly List<AclOperation>? _authorizedOperations;

    /// <summary>
    /// Creates a description whose authorized operations are reported-but-empty — Java's
    /// six-argument constructor.
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
    /// Java forwards <c>Set.of()</c>, so <see cref="AuthorizedOperations"/> is an empty collection on
    /// the resulting object rather than <see langword="null"/>.
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
    /// Creates a description — Java's seven-argument constructor.
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
    /// all. Duplicates are collapsed and the collection is copied.
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
    /// The id of the classic group — Java's <c>groupId()</c>. Never <see langword="null"/>.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// The group protocol type — Java's <c>protocol()</c>. May be <see langword="null"/>.
    /// </summary>
    public string? Protocol { get; }

    /// <summary>
    /// The group protocol data — Java's <c>protocolData()</c>. Never <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// The meaning depends on the group protocol type. For a classic consumer group this is
    /// the partition assignor name; for a classic connect group it indicates which Connect
    /// protocols are enabled.
    /// </remarks>
    public string ProtocolData { get; }

    /// <summary>
    /// Whether the group is a simple consumer group — Java's <c>isSimpleConsumerGroup()</c>.
    /// </summary>
    /// <exception cref="NullReferenceException">
    /// <see cref="Protocol"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// Computed from <see cref="Protocol"/> as Java's <c>protocol.isEmpty()</c>, with no null guard,
    /// as in Java.
    /// </remarks>
    public bool IsSimpleConsumerGroup => Protocol!.Length == 0;

    /// <summary>
    /// The members of the classic group — Java's <c>members()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// Java stores a <c>List.copyOf(...)</c>, so <see cref="Equals(object)"/> compares the members
    /// positionally.
    /// </remarks>
    public IReadOnlyCollection<MemberDescription> Members => _members;

    /// <summary>
    /// The classic group state, or <see cref="ClassicGroupState.Unknown"/> if the state is
    /// too new for this client to parse — Java's <c>state()</c>.
    /// </summary>
    public ClassicGroupState State { get; }

    /// <summary>
    /// The classic group coordinator, or <see langword="null"/> if the coordinator is not
    /// known — Java's <c>coordinator()</c>.
    /// </summary>
    public Node? Coordinator { get; }

    /// <summary>
    /// The operations the caller is authorized to perform on this group, or
    /// <see langword="null"/> if the broker reported none at all — Java's
    /// <c>authorizedOperations()</c>. An empty collection means "reported, and none are authorized".
    /// </summary>
    /// <remarks>
    /// Duplicates are collapsed, and <see cref="Equals(object)"/> and <see cref="GetHashCode"/> treat
    /// this member as a set, as Java's <c>Set</c> is.
    /// </remarks>
    public IReadOnlyCollection<AclOperation>? AuthorizedOperations => _authorizedOperations;

    /// <summary>
    /// Compares all seven stored members — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when every member matches.</returns>
    /// <remarks>
    /// <see cref="Coordinator"/> is compared by value over all of Java's <c>Node</c> fields, as Java's
    /// <c>Node.equals</c> does, although <see cref="Node"/> itself has no value equality.
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
    /// The hash of the same seven members — Java's <c>Objects.hash(...)</c>, whose 31-fold this
    /// reproduces in Java's argument order.
    /// </summary>
    /// <returns>The hash code.</returns>
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

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// The authorized operations render with their C# member names (<c>[Read, Write]</c> where Java
    /// prints <c>[READ, WRITE]</c>), as <see cref="TopicDescription.ToString"/> and
    /// <see cref="ConsumerGroupDescription.ToString"/> render them: a recorded deviation.
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
            && string.Equals(left.Rack, right.Rack, StringComparison.Ordinal)
            && left.IsFenced == right.IsFenced;
    }

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
            hash = (hash * 31) + (coordinator.IsFenced ? 1 : 0);
            return hash;
        }
    }

    private static bool OperationsMatch(List<AclOperation>? left, List<AclOperation>? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        return left.Count == right.Count && new HashSet<AclOperation>(left).SetEquals(right);
    }

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
