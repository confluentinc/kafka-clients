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

/// <summary>
/// <see cref="ClassicGroupDescription"/> against Java's <c>org.apache.kafka.clients.admin.ClassicGroupDescription</c>:
/// its two constructors, the coalesced arguments, the <c>IsSimpleConsumerGroup</c> projection, equality and
/// <c>ToString</c>; where it differs from <see cref="ConsumerGroupDescription"/>, the tests reflect over both types.
/// </summary>
public sealed class PublicAdminClassicGroupDescriptionTests
{
    private static readonly MemberDescription s_memberOne =
        new MemberDescription("m-1", null, null, "c-1", "h-1", null, null, null, null);

    private static readonly MemberDescription s_memberTwo =
        new MemberDescription("m-2", null, null, "c-2", "h-2", null, null, null, null);

    private static readonly Node s_coordinatorNode = new Node(7, "broker-1", 9092, "r-1");

    [Fact]
    public void Constructors_AreJavasTwo_BecauseNeitherIsDeprecated()
    {
        Type[][] actual = typeof(ClassicGroupDescription)
            .GetConstructors()
            .OrderBy(static constructor => constructor.GetParameters().Length)
            .Select(static constructor =>
                constructor.GetParameters().Select(static p => p.ParameterType).ToArray())
            .ToArray();

        Assert.Equal(2, actual.Length);

        Assert.Equal(
            new[]
            {
                typeof(string),
                typeof(string),
                typeof(string),
                typeof(IEnumerable<MemberDescription>),
                typeof(ClassicGroupState),
                typeof(Node),
            },
            actual[0]);

        Assert.Equal(
            new[]
            {
                typeof(string),
                typeof(string),
                typeof(string),
                typeof(IEnumerable<MemberDescription>),
                typeof(ClassicGroupState),
                typeof(Node),
                typeof(IEnumerable<AclOperation>),
            },
            actual[1]);

        Assert.All(
            typeof(ClassicGroupDescription).GetConstructors(),
            static constructor => Assert.Null(constructor.GetCustomAttribute<ObsoleteAttribute>()));
        Assert.Null(typeof(ClassicGroupDescription).GetCustomAttribute<ObsoleteAttribute>());

        Assert.Single(typeof(ConsumerGroupDescription).GetConstructors());
    }

    [Fact]
    public void SixArgumentConstructor_ForwardsAnEmptySet_NotAbsence()
    {
        var forwarded = new ClassicGroupDescription(
            "g-1", "consumer", "range", new[] { s_memberOne }, ClassicGroupState.Stable, s_coordinatorNode);

        Assert.NotNull(forwarded.AuthorizedOperations);
        Assert.Empty(forwarded.AuthorizedOperations!);

        Assert.True(forwarded.Equals(WithOperations(Array.Empty<AclOperation>())));
        Assert.Equal(WithOperations(Array.Empty<AclOperation>()).GetHashCode(), forwarded.GetHashCode());

        Assert.False(forwarded.Equals(WithOperations(null)));

        Assert.Contains("authorizedOperations=[])", forwarded.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void SixArgumentConstructor_PassesItsOwnSixThrough()
    {
        var forwarded = new ClassicGroupDescription(
            null, null, null, null, ClassicGroupState.Dead, null);

        Assert.Equal(string.Empty, forwarded.GroupId);
        Assert.Null(forwarded.Protocol);
        Assert.Equal(string.Empty, forwarded.ProtocolData);
        Assert.Empty(forwarded.Members);
        Assert.Equal(ClassicGroupState.Dead, forwarded.State);
        Assert.Null(forwarded.Coordinator);
    }

    [Fact]
    public void Properties_AreJavasShapes()
    {
        Assert.Equal(typeof(string), PropertyType("GroupId"));
        Assert.Equal(typeof(string), PropertyType("Protocol"));
        Assert.Equal(typeof(string), PropertyType("ProtocolData"));
        Assert.Equal(typeof(bool), PropertyType("IsSimpleConsumerGroup"));
        Assert.Equal(typeof(IReadOnlyCollection<MemberDescription>), PropertyType("Members"));
        Assert.Equal(typeof(ClassicGroupState), PropertyType("State"));
        Assert.Equal(typeof(Node), PropertyType("Coordinator"));
        Assert.Equal(typeof(IReadOnlyCollection<AclOperation>), PropertyType("AuthorizedOperations"));
    }

    [Fact]
    public void Constructor_RoundTripsEveryAccessor()
    {
        var description = Canonical();

        Assert.Equal("g-1", description.GroupId);
        Assert.Equal("consumer", description.Protocol);
        Assert.Equal("range", description.ProtocolData);
        Assert.False(description.IsSimpleConsumerGroup);
        Assert.Equal(new[] { s_memberOne, s_memberTwo }, description.Members);
        Assert.Equal(ClassicGroupState.Stable, description.State);
        Assert.Same(s_coordinatorNode, description.Coordinator);
        Assert.NotNull(description.AuthorizedOperations);
        Assert.Equal(new[] { AclOperation.Read, AclOperation.Write }, description.AuthorizedOperations!);
    }

    [Fact]
    public void Constructor_CoalescesTheTwoJavaCoalescesAndNoOthers()
    {
        var description = new ClassicGroupDescription(
            null, null, null, null, ClassicGroupState.Unknown, null, null);

        Assert.Equal(string.Empty, description.GroupId);
        Assert.Equal(string.Empty, description.ProtocolData);
        Assert.Empty(description.Members);

        Assert.Null(description.Protocol);
        Assert.Null(description.Coordinator);
        Assert.Null(description.AuthorizedOperations);
    }

    [Fact]
    public void Protocol_IsNotCoalesced_UnlikeItsTwoStringNeighbours()
    {
        var absent = WithProtocol(null);
        var empty = WithProtocol(string.Empty);

        Assert.Null(absent.Protocol);
        Assert.Equal(string.Empty, empty.Protocol);

        Assert.False(absent.Equals(empty));
        Assert.False(empty.Equals(absent));

        Assert.Contains("protocol='null',", absent.ToString(), StringComparison.Ordinal);
        Assert.Contains("protocol='',", empty.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Constructor_CopiesTheMembers()
    {
        var source = new List<MemberDescription> { s_memberOne };
        var description = new ClassicGroupDescription(
            "g-1", "consumer", "range", source, ClassicGroupState.Stable, null, null);

        source.Add(s_memberTwo);

        Assert.Equal(new[] { s_memberOne }, description.Members);
    }

    [Fact]
    public void IsSimpleConsumerGroup_IsAProjectionOfProtocol_NotAnIndependentMember()
    {
        Assert.Null(typeof(ClassicGroupDescription).GetProperty("IsSimpleConsumerGroup")!.SetMethod);

        Assert.True(WithProtocol(string.Empty).IsSimpleConsumerGroup);
        Assert.False(WithProtocol("consumer").IsSimpleConsumerGroup);
        Assert.False(WithProtocol("connect").IsSimpleConsumerGroup);

        Assert.True(new ConsumerGroupDescription(
            "g-1", true, null, "range", GroupType.Classic, GroupState.Empty, null, null, null, null)
            .IsSimpleConsumerGroup);
    }

    [Fact]
    public void IsSimpleConsumerGroup_FaultsOnAnAbsentProtocol_AsJavaDoes()
    {
        var description = WithProtocol(null);

        Assert.Throws<NullReferenceException>(() => _ = description.IsSimpleConsumerGroup);
    }

    [Fact]
    public void State_IsStoredAndCurrent_NotAProjection()
    {
        Assert.Null(typeof(ClassicGroupDescription).GetProperty("GroupState"));
        Assert.NotNull(typeof(ConsumerGroupDescription).GetProperty("GroupState"));

        Assert.Null(typeof(ClassicGroupDescription)
            .GetProperty("State")!
            .GetCustomAttribute<ObsoleteAttribute>());
        Assert.Null(typeof(ConsumerGroupDescription).GetProperty("State"));

        ClassicGroupState[] states =
        {
            ClassicGroupState.Unknown,
            ClassicGroupState.PreparingRebalance,
            ClassicGroupState.CompletingRebalance,
            ClassicGroupState.Stable,
            ClassicGroupState.Dead,
            ClassicGroupState.Empty,
        };

        foreach (ClassicGroupState state in states)
        {
            Assert.Equal(state, WithState(state).State);
        }

        var undefined = WithState((ClassicGroupState)int.MaxValue);

        Assert.Equal((ClassicGroupState)int.MaxValue, undefined.State);
        Assert.False(undefined.Equals(WithState(ClassicGroupState.Unknown)));
        Assert.Contains("state=Unknown,", undefined.ToString(), StringComparison.Ordinal);
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

        Assert.Contains("authorizedOperations=null)", absent.ToString(), StringComparison.Ordinal);
        Assert.Contains("authorizedOperations=[])", empty.ToString(), StringComparison.Ordinal);
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
    public void Equality_CoversAllSevenMembers()
    {
        var description = Canonical();

        Assert.True(description.Equals(Canonical()));
        Assert.True(description.Equals(description));
        Assert.Equal(Canonical().GetHashCode(), description.GetHashCode());

        Assert.False(description.Equals(Describe(
            "other", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, s_coordinatorNode, Operations())));
        Assert.False(description.Equals(Describe(
            "g-1", "connect", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, s_coordinatorNode, Operations())));
        Assert.False(description.Equals(Describe(
            "g-1", "consumer", "uniform", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, s_coordinatorNode, Operations())));
        Assert.False(description.Equals(Describe(
            "g-1", "consumer", "range", new[] { s_memberOne },
            ClassicGroupState.Stable, s_coordinatorNode, Operations())));
        Assert.False(description.Equals(Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Empty, s_coordinatorNode, Operations())));
        Assert.False(description.Equals(Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, null, Operations())));
        Assert.False(description.Equals(Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, s_coordinatorNode, new[] { AclOperation.Read })));

        Assert.False(description.Equals(null));
        Assert.False(description.Equals("g-1"));
    }

    [Fact]
    public void Equality_ComparesMembersPositionallyAndOperationsAsASet()
    {
        var description = Canonical();

        var membersReversed = Describe(
            "g-1", "consumer", "range", new[] { s_memberTwo, s_memberOne },
            ClassicGroupState.Stable, s_coordinatorNode, Operations());

        var operationsReversed = Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, s_coordinatorNode,
            new[] { AclOperation.Write, AclOperation.Read });

        Assert.False(description.Equals(membersReversed));

        Assert.True(description.Equals(operationsReversed));
        Assert.Equal(description.GetHashCode(), operationsReversed.GetHashCode());
    }

    [Fact]
    public void ToString_MirrorsJavasRendering()
    {
        var description = Describe(
            "g-1", "consumer", "range", new[] { s_memberOne },
            ClassicGroupState.Stable, s_coordinatorNode, Operations());

        Assert.Equal(
            "(groupId=g-1, protocol='consumer', protocolData=range, members="
            + "(memberId=m-1, groupInstanceId=null, rackId=null, clientId=c-1, host=h-1, "
            + "assignment=(topicPartitions=), targetAssignment=Optional.empty, "
            + "memberEpoch=null, upgraded=null)"
            + ", state=Stable, coordinator=broker-1:9092 (id: 7 rack: r-1 isFenced: false), "
            + "authorizedOperations=[Read, Write])",
            description.ToString());

        Assert.Equal(
            "(groupId=, protocol='null', protocolData=, members=, state=Unknown, "
            + "coordinator=null, authorizedOperations=null)",
            new ClassicGroupDescription(
                null, null, null, null, ClassicGroupState.Unknown, null, null).ToString());
    }

    [Fact]
    public void ToString_QuotesTheProtocolAndNotTheProtocolData()
    {
        var description = Describe(
            "g-1", "same", "same", null, ClassicGroupState.PreparingRebalance, null, null);

        Assert.Equal(
            "(groupId=g-1, protocol='same', protocolData=same, members=, "
            + "state=PreparingRebalance, coordinator=null, authorizedOperations=null)",
            description.ToString());
    }

    [Fact]
    public void ToString_JoinsTheMembersWithoutASpace()
    {
        var description = Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, null, null);

        Assert.Contains("upgraded=null),(memberId=m-2,", description.ToString(), StringComparison.Ordinal);
    }

    private static AclOperation[] Operations() =>
        new[] { AclOperation.Read, AclOperation.Write };

    private static ClassicGroupDescription Canonical() =>
        Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, s_coordinatorNode, Operations());

    private static ClassicGroupDescription WithProtocol(string? protocol) =>
        Describe(
            "g-1", protocol, "range", new[] { s_memberOne },
            ClassicGroupState.Stable, s_coordinatorNode, Operations());

    private static ClassicGroupDescription WithState(ClassicGroupState state) =>
        Describe(
            "g-1", "consumer", "range", new[] { s_memberOne },
            state, s_coordinatorNode, Operations());

    private static ClassicGroupDescription WithOperations(IEnumerable<AclOperation>? operations) =>
        Describe(
            "g-1", "consumer", "range", new[] { s_memberOne },
            ClassicGroupState.Stable, s_coordinatorNode, operations);

    private static ClassicGroupDescription WithCoordinator(Node? coordinator) =>
        Describe(
            "g-1", "consumer", "range", new[] { s_memberOne, s_memberTwo },
            ClassicGroupState.Stable, coordinator, Operations());

    private static ClassicGroupDescription Describe(
        string? groupId,
        string? protocol,
        string? protocolData,
        IEnumerable<MemberDescription>? members,
        ClassicGroupState state,
        Node? coordinator,
        IEnumerable<AclOperation>? authorizedOperations) =>
        new ClassicGroupDescription(
            groupId,
            protocol,
            protocolData,
            members,
            state,
            coordinator,
            authorizedOperations);

    private static Type PropertyType(string name) =>
        typeof(ClassicGroupDescription).GetProperty(name)!.PropertyType;
}
