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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ConsumerGroupDescription"/> against Java's <c>org.apache.kafka.clients.admin.ConsumerGroupDescription</c>:
/// the one constructor Java does not deprecate, the coalesced arguments, absent versus empty members, equality and
/// <c>ToString</c>.
/// </summary>
/// <remarks>
/// The shape half is reflection assertions: a behavioural test passes equally against <c>GroupType</c> and
/// <c>GroupType?</c>, the distinction drawn against <see cref="GroupListing"/>.
/// </remarks>
public sealed class PublicAdminConsumerGroupDescriptionTests
{
    private static readonly MemberDescription s_memberOne =
        new MemberDescription("m-1", null, null, "c-1", "h-1", null, null, null, null);

    private static readonly MemberDescription s_memberTwo =
        new MemberDescription("m-2", null, null, "c-2", "h-2", null, null, null, null);

    private static readonly Node s_coordinatorNode = new Node(7, "broker-1", 9092, "r-1");

    [Fact]
    public void Constructor_IsJavasCurrentOne_TheThreeDeprecatedOnesAreNotTranslated()
    {
        Type[][] actual = typeof(ConsumerGroupDescription)
            .GetConstructors()
            .Select(static constructor =>
                constructor.GetParameters().Select(static p => p.ParameterType).ToArray())
            .ToArray();

        Type[] only = Assert.Single(actual);

        Assert.Equal(
            new[]
            {
                typeof(string),
                typeof(bool),
                typeof(IEnumerable<MemberDescription>),
                typeof(string),
                typeof(GroupType),
                typeof(GroupState),
                typeof(Node),
                typeof(IEnumerable<AclOperation>),
                typeof(int?),
                typeof(int?),
            },
            only);
    }

    [Fact]
    public void TypeAndGroupState_AreNonOptionalHere_UnlikeOnTheListing()
    {
        Assert.Equal(typeof(GroupType), PropertyType("Type"));
        Assert.Equal(typeof(GroupState), PropertyType("GroupState"));

        Assert.Equal(typeof(GroupType?), typeof(GroupListing).GetProperty("Type")!.PropertyType);
        Assert.Equal(typeof(GroupState?), typeof(GroupListing).GetProperty("GroupState")!.PropertyType);
    }

    [Fact]
    public void Properties_AreJavasShapes()
    {
        Assert.Equal(typeof(string), PropertyType("GroupId"));
        Assert.Equal(typeof(bool), PropertyType("IsSimpleConsumerGroup"));
        Assert.Equal(typeof(IReadOnlyCollection<MemberDescription>), PropertyType("Members"));
        Assert.Equal(typeof(string), PropertyType("PartitionAssignor"));
        Assert.Equal(typeof(Node), PropertyType("Coordinator"));
        Assert.Equal(typeof(IReadOnlyCollection<AclOperation>), PropertyType("AuthorizedOperations"));
        Assert.Equal(typeof(int?), PropertyType("GroupEpoch"));
        Assert.Equal(typeof(int?), PropertyType("TargetAssignmentEpoch"));
    }

    [Fact]
    public void Constructor_RoundTripsEveryAccessor()
    {
        var description = Canonical();

        Assert.Equal("g-1", description.GroupId);
        Assert.False(description.IsSimpleConsumerGroup);
        Assert.Equal(new[] { s_memberOne, s_memberTwo }, description.Members);
        Assert.Equal("range", description.PartitionAssignor);
        Assert.Equal(GroupType.Consumer, description.Type);
        Assert.Equal(GroupState.Stable, description.GroupState);
        Assert.Same(s_coordinatorNode, description.Coordinator);
        Assert.NotNull(description.AuthorizedOperations);
        Assert.Equal(new[] { AclOperation.Read, AclOperation.Write }, description.AuthorizedOperations!);
        Assert.Equal(7, description.GroupEpoch);
        Assert.Equal(9, description.TargetAssignmentEpoch);
    }

    [Fact]
    public void Constructor_CoalescesTheThreeJavaCoalescesAndNoOthers()
    {
        var description = new ConsumerGroupDescription(
            null, false, null, null, GroupType.Unknown, GroupState.Unknown, null, null, null, null);

        Assert.Equal(string.Empty, description.GroupId);
        Assert.Empty(description.Members);
        Assert.Equal(string.Empty, description.PartitionAssignor);

        Assert.Null(description.Coordinator);
        Assert.Null(description.AuthorizedOperations);
        Assert.Null(description.GroupEpoch);
        Assert.Null(description.TargetAssignmentEpoch);
    }

    [Fact]
    public void Constructor_CopiesTheMembers()
    {
        var source = new List<MemberDescription> { s_memberOne };
        var description = new ConsumerGroupDescription(
            "g-1", false, source, "range", GroupType.Consumer, GroupState.Stable, null, null, null, null);

        source.Add(s_memberTwo);

        Assert.Equal(new[] { s_memberOne }, description.Members);
    }

    [Fact]
    public void State_IsNotTranslated_AndGroupStateAndTheClassAreNotObsolete()
    {
        Assert.Null(typeof(ConsumerGroupDescription).GetProperty("State"));

        Assert.Null(typeof(ConsumerGroupDescription)
            .GetProperty("GroupState")!
            .GetCustomAttribute<ObsoleteAttribute>());
        Assert.Null(typeof(ConsumerGroupDescription).GetCustomAttribute<ObsoleteAttribute>());
    }

    [Fact]
    public void AuthorizedOperations_DistinguishesAbsentFromEmpty()
    {
        var absent = WithOperations(null);
        var empty = WithOperations(Array.Empty<AclOperation>());

        Assert.Null(absent.AuthorizedOperations);

        Assert.NotNull(empty.AuthorizedOperations);
        Assert.Empty(empty.AuthorizedOperations!);

        Assert.False(absent.Equals(empty));
        Assert.False(empty.Equals(absent));

        Assert.Contains("authorizedOperations=null,", absent.ToString(), StringComparison.Ordinal);
        Assert.Contains("authorizedOperations=[],", empty.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AuthorizedOperations_CollapseDuplicatesAndKeepFirstSeenOrder()
    {
        var description = WithOperations(
            new[] { AclOperation.Read, AclOperation.Write, AclOperation.Read });

        Assert.NotNull(description.AuthorizedOperations);
        Assert.Equal(new[] { AclOperation.Read, AclOperation.Write }, description.AuthorizedOperations!);
    }

    [Fact]
    public void AnAbsentEpochIsNotASentinel()
    {
        var absent = WithEpochs(null, null);

        Assert.Null(absent.GroupEpoch);
        Assert.Null(absent.TargetAssignmentEpoch);

        Assert.False(absent.Equals(WithEpochs(0, 0)));
        Assert.False(absent.Equals(WithEpochs(-1, -1)));
        Assert.False(WithEpochs(0, 0).Equals(WithEpochs(-1, -1)));

        Assert.Contains("groupEpoch=null, targetAssignmentEpoch=null)", absent.ToString(), StringComparison.Ordinal);
        Assert.Contains("groupEpoch=0, targetAssignmentEpoch=0)", WithEpochs(0, 0).ToString(), StringComparison.Ordinal);

        var present = WithEpochs(7, 9);
        Assert.Equal(7, present.GroupEpoch);
        Assert.Equal(9, present.TargetAssignmentEpoch);
    }

    [Fact]
    public void Coordinator_CoversAbsentAndPresent()
    {
        var absent = WithCoordinator(null);

        Assert.Null(absent.Coordinator);
        Assert.Contains("coordinator=null,", absent.ToString(), StringComparison.Ordinal);
        Assert.False(absent.Equals(Canonical()));

        var equalByValue = WithCoordinator(new Node(7, "broker-1", 9092, "r-1"));
        Assert.True(Canonical().Equals(equalByValue));
        Assert.Equal(Canonical().GetHashCode(), equalByValue.GetHashCode());

        Assert.False(Canonical().Equals(WithCoordinator(new Node(8, "broker-1", 9092, "r-1"))));
        Assert.False(Canonical().Equals(WithCoordinator(new Node(7, "broker-2", 9092, "r-1"))));
        Assert.False(Canonical().Equals(WithCoordinator(new Node(7, "broker-1", 9093, "r-1"))));
        Assert.False(Canonical().Equals(WithCoordinator(new Node(7, "broker-1", 9092, null))));
        Assert.False(Canonical().Equals(WithCoordinator(new Node(7, "broker-1", 9092, "r-1", isFenced: true))));
    }

    [Fact]
    public void Equality_CoversAllTenMembers()
    {
        var description = Canonical();

        Assert.True(description.Equals(Canonical()));
        Assert.True(description.Equals(description));
        Assert.Equal(Canonical().GetHashCode(), description.GetHashCode());

        Assert.False(description.Equals(Describe(
            "other", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", true, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "uniform", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Classic,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Empty, s_coordinatorNode, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, null, Operations(), 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, new[] { AclOperation.Read }, 7, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 8, 9)));
        Assert.False(description.Equals(Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 10)));

        Assert.False(description.Equals(null));
        Assert.False(description.Equals("g-1"));
    }

    [Fact]
    public void Equality_ComparesMembersPositionallyAndOperationsAsASet()
    {
        var description = Canonical();

        var membersReversed = Describe(
            "g-1", false, new[] { s_memberTwo, s_memberOne }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9);

        var operationsReversed = Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, new[] { AclOperation.Write, AclOperation.Read }, 7, 9);

        Assert.False(description.Equals(membersReversed));

        Assert.True(description.Equals(operationsReversed));
        Assert.Equal(description.GetHashCode(), operationsReversed.GetHashCode());
    }

    [Fact]
    public void ToString_MirrorsJavasRendering()
    {
        var description = Describe(
            "g-1", false, new[] { s_memberOne }, "range", GroupType.Consumer, GroupState.Stable,
            s_coordinatorNode, Operations(), 7, 9);

        Assert.Equal(
            "(groupId=g-1, isSimpleConsumerGroup=false, members="
            + "(memberId=m-1, groupInstanceId=null, rackId=null, clientId=c-1, host=h-1, "
            + "assignment=(topicPartitions=), targetAssignment=Optional.empty, "
            + "memberEpoch=null, upgraded=null)"
            + ", partitionAssignor=range, type=Consumer, groupState=Stable, "
            + "coordinator=broker-1:9092 (id: 7 rack: r-1 isFenced: false), authorizedOperations=[Read, Write], "
            + "groupEpoch=7, targetAssignmentEpoch=9)",
            description.ToString());

        Assert.Equal(
            "(groupId=, isSimpleConsumerGroup=false, members=, partitionAssignor=, "
            + "type=Unknown, groupState=Unknown, coordinator=null, authorizedOperations=null, "
            + "groupEpoch=null, targetAssignmentEpoch=null)",
            new ConsumerGroupDescription(
                null, false, null, null, GroupType.Unknown, GroupState.Unknown, null, null, null, null)
                .ToString());
    }

    [Fact]
    public void ToString_SpellsASimpleGroupTheJavaWay()
    {
        var description = Describe(
            "g-1", true, null, "range", GroupType.Classic, GroupState.Empty, null, null, null, null);

        Assert.Contains("isSimpleConsumerGroup=true,", description.ToString(), StringComparison.Ordinal);
        Assert.Contains("type=Classic, groupState=Empty,", description.ToString(), StringComparison.Ordinal);
    }

    private static AclOperation[] Operations() =>
        new[] { AclOperation.Read, AclOperation.Write };

    private static ConsumerGroupDescription Canonical() =>
        Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, s_coordinatorNode, Operations(), 7, 9);

    private static ConsumerGroupDescription WithOperations(IEnumerable<AclOperation>? operations) =>
        Describe(
            "g-1", false, new[] { s_memberOne }, "range", GroupType.Consumer, GroupState.Stable,
            s_coordinatorNode, operations, 7, 9);

    private static ConsumerGroupDescription WithEpochs(int? groupEpoch, int? targetAssignmentEpoch) =>
        Describe(
            "g-1", false, new[] { s_memberOne }, "range", GroupType.Consumer, GroupState.Stable,
            s_coordinatorNode, Operations(), groupEpoch, targetAssignmentEpoch);

    private static ConsumerGroupDescription WithCoordinator(Node? coordinator) =>
        Describe(
            "g-1", false, new[] { s_memberOne, s_memberTwo }, "range", GroupType.Consumer,
            GroupState.Stable, coordinator, Operations(), 7, 9);

    private static ConsumerGroupDescription Describe(
        string? groupId,
        bool isSimpleConsumerGroup,
        IEnumerable<MemberDescription>? members,
        string? partitionAssignor,
        GroupType type,
        GroupState groupState,
        Node? coordinator,
        IEnumerable<AclOperation>? authorizedOperations,
        int? groupEpoch,
        int? targetAssignmentEpoch) =>
        new ConsumerGroupDescription(
            groupId,
            isSimpleConsumerGroup,
            members,
            partitionAssignor,
            type,
            groupState,
            coordinator,
            authorizedOperations,
            groupEpoch,
            targetAssignmentEpoch);

    private static Type PropertyType(string name) =>
        typeof(ConsumerGroupDescription).GetProperty(name)!.PropertyType;
}
