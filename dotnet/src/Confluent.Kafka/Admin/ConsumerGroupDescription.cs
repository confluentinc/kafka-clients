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
/// A detailed description of a single consumer group in the cluster — Java's
/// <c>org.apache.kafka.clients.admin.ConsumerGroupDescription</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's deprecated <c>state()</c> accessor and its three deprecated constructors are not
/// translated; read <see cref="GroupState"/>, as Java's deprecation note says.
/// </para>
/// <para>
/// <see cref="AuthorizedOperations"/> is <see langword="null"/> when the broker reported none at
/// all, and empty when it reported that none are authorized, as in Java.
/// </para>
/// <para>
/// <see cref="GroupEpoch"/> and <see cref="TargetAssignmentEpoch"/> are set for a
/// <see cref="GroupType.Consumer"/> group and <see langword="null"/>, Java's empty
/// <c>Optional</c>, for a <see cref="GroupType.Classic"/> one.
/// </para>
/// <para>
/// Null arguments are coalesced as in Java: <see cref="GroupId"/> and
/// <see cref="PartitionAssignor"/> become the empty string and <see cref="Members"/> an empty
/// collection.
/// </para>
/// </remarks>
public sealed class ConsumerGroupDescription
{
    private readonly List<MemberDescription> _members;

    private readonly List<AclOperation>? _authorizedOperations;

    /// <summary>
    /// Creates a description — Java's current constructor.
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
    /// all. Duplicates are collapsed and the collection is copied.
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
    /// The id of the consumer group — Java's <c>groupId()</c>. Never <see langword="null"/>.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// Whether the consumer group is simple — Java's <c>isSimpleConsumerGroup()</c>.
    /// </summary>
    public bool IsSimpleConsumerGroup { get; }

    /// <summary>
    /// The members of the consumer group — Java's <c>members()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// Java stores a <c>List.copyOf(...)</c>, so <see cref="Equals(object)"/> compares the members
    /// positionally.
    /// </remarks>
    public IReadOnlyCollection<MemberDescription> Members => _members;

    /// <summary>
    /// The consumer group partition assignor — Java's <c>partitionAssignor()</c>. Never
    /// <see langword="null"/>.
    /// </summary>
    public string PartitionAssignor { get; }

    /// <summary>
    /// The group type (or the protocol) of this consumer group — Java's <c>type()</c>. It
    /// defaults to <see cref="GroupType.Classic"/> if not provided by the server.
    /// </summary>
    public GroupType Type { get; }

    /// <summary>
    /// The group state, or <see cref="Confluent.Kafka.GroupState.Unknown"/> if the state is
    /// too new for this client to parse — Java's <c>groupState()</c>.
    /// </summary>
    public GroupState GroupState { get; }

    /// <summary>
    /// The consumer group coordinator, or <see langword="null"/> if the coordinator is not
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
    /// The epoch of the consumer group, or <see langword="null"/> for a classic group —
    /// Java's <c>groupEpoch()</c>.
    /// </summary>
    public int? GroupEpoch { get; }

    /// <summary>
    /// The epoch of the target assignment, or <see langword="null"/> for a classic group —
    /// Java's <c>targetAssignmentEpoch()</c>.
    /// </summary>
    public int? TargetAssignmentEpoch { get; }

    /// <summary>
    /// Compares all ten stored members — Java's <c>equals</c>.
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
    /// The hash of the same ten members — Java's <c>Objects.hash(...)</c>, whose 31-fold this
    /// reproduces in Java's argument order.
    /// </summary>
    /// <returns>The hash code.</returns>
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

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// The authorized operations render with their C# member names (<c>[Read, Write]</c> where Java
    /// prints <c>[READ, WRITE]</c>), as <see cref="TopicDescription.ToString"/> renders them: a
    /// recorded deviation.
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
