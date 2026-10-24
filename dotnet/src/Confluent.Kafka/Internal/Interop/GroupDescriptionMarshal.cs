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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the values the group RPCs deliver: the listings and errors of <c>listGroups</c>, and the
/// borrowed consumer and classic group descriptions, through their owned member and assignment
/// copies, which it frees.
/// </summary>
internal static class GroupDescriptionMarshal
{
    internal static IReadOnlyCollection<GroupListing> CopyOutListings(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<GroupListing> listings = new List<GroupListing>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr listing = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            listings.Add(CopyOutListing(listing));
        }

        return listings;
    }

    internal static IReadOnlyCollection<KafkaException> CopyOutErrors(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<KafkaException> errors = new List<KafkaException>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr error = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            KafkaException? exception = KafkaException.FromBorrowedHandle(error);
            if (exception is not null)
            {
                errors.Add(exception);
            }
        }

        return errors;
    }

    internal static ConsumerGroupDescription CopyOutConsumerGroupDescription(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr groupId = NativeMethods.ConsumerGroupDescriptionGroupId(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte isSimple = NativeMethods.ConsumerGroupDescriptionIsSimpleConsumerGroup(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr members = NativeMethods.ConsumerGroupDescriptionMembers(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<MemberDescription> memberDescriptions = CopyOutMembersAndDestroy(members);
        IntPtr assignor = NativeMethods.ConsumerGroupDescriptionPartitionAssignor(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr type = NativeMethods.ConsumerGroupDescriptionType(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr state = NativeMethods.ConsumerGroupDescriptionGroupState(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr coordinator = NativeMethods.ConsumerGroupDescriptionCoordinator(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr operations = NativeMethods.ConsumerGroupDescriptionAuthorizedOperations(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IReadOnlyCollection<AclOperation>? authorizedOperations =
            AuthorizedOperationsMarshal.CopyOutAndDestroy(operations);
        int groupEpoch = NativeMethods.ConsumerGroupDescriptionGroupEpoch(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int targetAssignmentEpoch =
            NativeMethods.ConsumerGroupDescriptionTargetAssignmentEpoch(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ConsumerGroupDescription(
            Utf8Marshal.PtrToString(groupId) ?? string.Empty,
            isSimple != 0,
            memberDescriptions,
            Utf8Marshal.PtrToString(assignor),
            GroupMarshal.TypeFrom(type) ?? GroupType.Unknown,
            GroupMarshal.StateFrom(state) ?? GroupState.Unknown,
            NodeMarshal.CopyOut(coordinator),
            authorizedOperations,
            OptionalEpoch(groupEpoch),
            OptionalEpoch(targetAssignmentEpoch));
    }

    internal static ClassicGroupDescription CopyOutClassicGroupDescription(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr groupId = NativeMethods.ClassicGroupDescriptionGroupId(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr protocol = NativeMethods.ClassicGroupDescriptionProtocol(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr protocolData = NativeMethods.ClassicGroupDescriptionProtocolData(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr members = NativeMethods.ClassicGroupDescriptionMembers(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<MemberDescription> memberDescriptions = CopyOutMembersAndDestroy(members);
        IntPtr state = NativeMethods.ClassicGroupDescriptionState(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr coordinator = NativeMethods.ClassicGroupDescriptionCoordinator(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr operations = NativeMethods.ClassicGroupDescriptionAuthorizedOperations(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ClassicGroupDescription(
            Utf8Marshal.PtrToString(groupId) ?? string.Empty,
            Utf8Marshal.PtrToString(protocol) ?? string.Empty,
            Utf8Marshal.PtrToString(protocolData) ?? string.Empty,
            memberDescriptions,
            GroupMarshal.ClassicStateFrom(state) ?? ClassicGroupState.Unknown,
            NodeMarshal.CopyOut(coordinator),
            AuthorizedOperationsMarshal.CopyOutAndDestroy(operations));
    }

    private static GroupListing CopyOutListing(IntPtr listing)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr groupId = NativeMethods.GroupListingGroupId(listing, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr type = NativeMethods.GroupListingType(listing, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr protocol = NativeMethods.GroupListingProtocol(listing, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr state = NativeMethods.GroupListingGroupState(listing, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new GroupListing(
            Utf8Marshal.PtrToString(groupId) ?? string.Empty,
            GroupMarshal.TypeFrom(type),
            Utf8Marshal.PtrToString(protocol) ?? string.Empty,
            GroupMarshal.StateFrom(state));
    }

    private static List<MemberDescription> CopyOutMembersAndDestroy(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return new List<MemberDescription>();
        }

        try
        {
            IntPtr outErr = IntPtr.Zero;
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<MemberDescription> members = new List<MemberDescription>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr member = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                members.Add(CopyOutMember(member));
            }

            return members;
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref outErr);
            if (outErr != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(outErr);
            }
        }
    }

    private static MemberDescription CopyOutMember(IntPtr member)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr consumerId = NativeMethods.MemberDescriptionConsumerId(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr groupInstanceId = NativeMethods.MemberDescriptionGroupInstanceId(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr rackId = NativeMethods.MemberDescriptionRackId(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr clientId = NativeMethods.MemberDescriptionClientId(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr host = NativeMethods.MemberDescriptionHost(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr assignment = NativeMethods.MemberDescriptionAssignment(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        MemberAssignment? memberAssignment = CopyOutAssignment(assignment);
        IntPtr targetAssignment = NativeMethods.MemberDescriptionTargetAssignment(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        MemberAssignment? memberTargetAssignment = CopyOutAssignment(targetAssignment);
        int memberEpoch = NativeMethods.MemberDescriptionMemberEpoch(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte upgraded = NativeMethods.MemberDescriptionUpgraded(member, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new MemberDescription(
            Utf8Marshal.PtrToString(consumerId),
            Utf8Marshal.PtrToString(groupInstanceId),
            Utf8Marshal.PtrToString(rackId),
            Utf8Marshal.PtrToString(clientId),
            Utf8Marshal.PtrToString(host),
            memberAssignment,
            memberTargetAssignment,
            OptionalEpoch(memberEpoch),
            upgraded < 0 ? null : upgraded != 0);
    }

    private static MemberAssignment? CopyOutAssignment(IntPtr assignment)
    {
        if (assignment == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr partitions = NativeMethods.MemberAssignmentTopicPartitions(assignment, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new MemberAssignment(TopicPartitionListMarshal.CopyOutAndDestroy(partitions));
    }

    // The C accessors answer -1 for Java's Optional.empty().
    private static int? OptionalEpoch(int epoch) => epoch == -1 ? null : epoch;
}
