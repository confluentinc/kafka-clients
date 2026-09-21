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

using Xunit;

namespace Confluent.Kafka.UnitTests;

#pragma warning disable CS0618 // Java deprecates the State projection and its enum; exercising them is the point.

/// <summary>
/// Pins the shape and the behaviour of <see cref="ConsumerGroupDescription"/> against Java's
/// <c>org.apache.kafka.clients.admin.ConsumerGroupDescription</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a pure value type</b> — everything here is managed and broker-free, as it is
/// for its two siblings <see cref="MemberAssignment"/> and <see cref="MemberDescription"/>.
/// </para>
/// <para>
/// ⚠ <b>C# upcasts and widens silently, so the shape half has to be reflection
/// assertions.</b> A behavioural test passes equally against <c>GroupType</c> and
/// <c>GroupType?</c>, which is precisely the distinction this class draws against
/// <see cref="ConsumerGroupListing"/>.
/// </para>
/// </remarks>
public sealed class PublicAdminConsumerGroupDescriptionTests
{
    private static readonly MemberDescription s_memberOne =
        new MemberDescription("m-1", null, null, "c-1", "h-1", null, null, null, null);

    private static readonly MemberDescription s_memberTwo =
        new MemberDescription("m-2", null, null, "c-2", "h-2", null, null, null, null);

    private static readonly Node s_coordinatorNode = new Node(7, "broker-1", 9092, "r-1");

    /// <summary>
    /// Java's one <b>current</b> constructor ships, with Java's parameter shape. The three it
    /// deprecates deliberately do not — a ruling for this phase, and the reason the count is
    /// exactly one rather than four.
    /// </summary>
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

    /// <summary>
    /// ⚠ <b><see cref="ConsumerGroupDescription.Type"/> is non-optional here, unlike on
    /// <see cref="ConsumerGroupListing"/>.</b> Java's description carries a plain
    /// <c>GroupType</c> while its listing carries an <c>Optional&lt;GroupType&gt;</c>, and the
    /// contrast is asserted against the sibling rather than stated, because a behavioural test
    /// cannot see it — a <c>GroupType</c> widens into a <c>GroupType?</c> silently.
    /// </summary>
    [Fact]
    public void TypeAndGroupState_AreNonOptionalHere_UnlikeOnTheListing()
    {
        Assert.Equal(typeof(GroupType), PropertyType("Type"));
        Assert.Equal(typeof(GroupState), PropertyType("GroupState"));

        Assert.Equal(typeof(GroupType?), typeof(ConsumerGroupListing).GetProperty("Type")!.PropertyType);
        Assert.Equal(typeof(GroupState?), typeof(ConsumerGroupListing).GetProperty("GroupState")!.PropertyType);
    }

    /// <summary>
    /// The three <c>Optional</c>-shaped members are nullable and the two collections keep
    /// Java's element types — the absent-versus-present distinction the behaviour tests below
    /// rely on has to exist in the metadata first.
    /// </summary>
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

    /// <summary>
    /// Java coalesces exactly three arguments (the id, the members and the assignor); the
    /// other six are stored as given, which is what keeps absence observable.
    /// </summary>
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

    /// <summary>The members are copied, so a later mutation of the argument is not observed.</summary>
    [Fact]
    public void Constructor_CopiesTheMembers()
    {
        var source = new List<MemberDescription> { s_memberOne };
        var description = new ConsumerGroupDescription(
            "g-1", false, source, "range", GroupType.Consumer, GroupState.Stable, null, null, null, null);

        source.Add(s_memberTwo);

        Assert.Equal(new[] { s_memberOne }, description.Members);
    }

    /// <summary>
    /// ⚠ <b><see cref="ConsumerGroupDescription.State"/> is a projection of
    /// <see cref="ConsumerGroupDescription.GroupState"/>, not a stored member.</b> Setting the
    /// group state is the only thing that makes it read back, so the two cannot disagree —
    /// there is no constructor parameter and no setter that could desynchronize them.
    /// </summary>
    [Fact]
    public void State_IsAProjectionOfGroupState_NotAnIndependentMember()
    {
        Assert.Null(typeof(ConsumerGroupDescription).GetProperty("State")!.SetMethod);

        var pairs = new (GroupState Group, ConsumerGroupState Projected)[]
        {
            (GroupState.Unknown, ConsumerGroupState.Unknown),
            (GroupState.PreparingRebalance, ConsumerGroupState.PreparingRebalance),
            (GroupState.CompletingRebalance, ConsumerGroupState.CompletingRebalance),
            (GroupState.Stable, ConsumerGroupState.Stable),
            (GroupState.Dead, ConsumerGroupState.Dead),
            (GroupState.Empty, ConsumerGroupState.Empty),
            (GroupState.Assigning, ConsumerGroupState.Assigning),
            (GroupState.Reconciling, ConsumerGroupState.Reconciling),
        };

        foreach ((GroupState group, ConsumerGroupState projected) in pairs)
        {
            var description = WithGroupState(group);

            Assert.Equal(group, description.GroupState);
            Assert.Equal(projected, description.State);
        }
    }

    /// <summary>
    /// The projection is lossy and one-way: <see cref="GroupState.NotReady"/> has no
    /// counterpart on the deprecated enum and lands on <c>Unknown</c>, so two descriptions
    /// that report the same <c>State</c> can still differ in <c>GroupState</c>. Running the
    /// projection backwards is therefore not possible, which is why <c>GroupState</c> — and
    /// not <c>State</c> — is the member equality compares.
    /// </summary>
    [Fact]
    public void State_LosesNotReady_SoItCannotBeRunBackwards()
    {
        var notReady = WithGroupState(GroupState.NotReady);
        var unknown = WithGroupState(GroupState.Unknown);

        Assert.Equal(ConsumerGroupState.Unknown, notReady.State);
        Assert.Equal(ConsumerGroupState.Unknown, unknown.State);

        Assert.NotEqual(notReady.GroupState, unknown.GroupState);
        Assert.False(notReady.Equals(unknown));
    }

    /// <summary>
    /// Java deprecates <c>state()</c> in favour of <c>groupState()</c> but not the class
    /// itself — unlike <see cref="ConsumerGroupListing"/>, where the whole type carries the
    /// attribute.
    /// </summary>
    [Fact]
    public void State_IsObsolete_AndGroupStateAndTheClassAreNot()
    {
        ObsoleteAttribute onState = typeof(ConsumerGroupDescription)
            .GetProperty("State")!
            .GetCustomAttribute<ObsoleteAttribute>()!;

        Assert.NotNull(onState);
        Assert.Equal("Deprecated in Kafka since 4.0. Use GroupState instead.", onState.Message);

        Assert.Null(typeof(ConsumerGroupDescription)
            .GetProperty("GroupState")!
            .GetCustomAttribute<ObsoleteAttribute>());
        Assert.Null(typeof(ConsumerGroupDescription).GetCustomAttribute<ObsoleteAttribute>());
    }

    /// <summary>
    /// ⚠ <b>Absent is not empty.</b> The ABI reports whether the broker sent an
    /// authorized-operations set at all, so Java's <c>null</c> (not reported) and its empty
    /// set (reported, nothing authorized) are two distinct values. A test that only checked
    /// emptiness could not tell them apart — <c>Assert.Empty</c> passes on neither, and
    /// counting passes on both.
    /// </summary>
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

    /// <summary>
    /// Java's parameter is a <c>Set</c>: duplicates collapse. First-seen order is kept so the
    /// rendering is stable.
    /// </summary>
    [Fact]
    public void AuthorizedOperations_CollapseDuplicatesAndKeepFirstSeenOrder()
    {
        var description = WithOperations(
            new[] { AclOperation.Read, AclOperation.Write, AclOperation.Read });

        Assert.NotNull(description.AuthorizedOperations);
        Assert.Equal(new[] { AclOperation.Read, AclOperation.Write }, description.AuthorizedOperations!);
    }

    /// <summary>
    /// The two epochs are <c>Optional&lt;Integer&gt;</c> in Java — absent for a classic group.
    /// Absence must not collapse into a sentinel: neither <c>0</c> nor <c>-1</c> is it.
    /// </summary>
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

    /// <summary>
    /// The coordinator is nullable, and a present one is compared field-by-field — this
    /// binding's <see cref="Node"/> carries no value equality of its own, so the four members
    /// it models each have to discriminate here.
    /// </summary>
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
    }

    /// <summary>
    /// Equality covers the ten constructor members. The <c>State</c> projection is absent from
    /// it, as it is from Java's <c>equals</c> — it carries no information <c>GroupState</c>
    /// does not.
    /// </summary>
    [Fact]
    public void Equality_CoversAllTenMembers()
    {
        var description = Canonical();

        Assert.True(description.Equals(Canonical()));
        Assert.True(description.Equals(description));
        Assert.Equal(Canonical().GetHashCode(), description.GetHashCode());

        // Each member discriminates on its own.
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

    /// <summary>
    /// The two collections hash and compare the way Java's own do: the members are a
    /// <c>List</c> (positional), the authorized operations a <c>Set</c> (order-insensitive).
    /// Treating either like the other would be wrong in one direction or the other.
    /// </summary>
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

    /// <summary>
    /// ⚠ <b>Java renders absence three ways in this one method.</b> An empty members list
    /// renders as a bare <c>members=</c> (the elements join with <c>","</c> and no space); an
    /// absent coordinator or operations set concatenates the bare text <c>null</c>; and the
    /// two epochs reach that same text through <c>orElse(null)</c> — <b>not</b> the
    /// <c>Optional.empty</c> that <see cref="MemberDescription"/> emits.
    /// </summary>
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
            + "coordinator=broker-1:9092 (id: 7 rack: r-1), authorizedOperations=[Read, Write], "
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

    /// <summary>Java's <c>Boolean.toString()</c> is lowercase; .NET's would render "True".</summary>
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

    private static ConsumerGroupDescription WithGroupState(GroupState groupState) =>
        Describe(
            "g-1", false, new[] { s_memberOne }, "range", GroupType.Consumer, groupState,
            s_coordinatorNode, Operations(), 7, 9);

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

#pragma warning restore CS0618
