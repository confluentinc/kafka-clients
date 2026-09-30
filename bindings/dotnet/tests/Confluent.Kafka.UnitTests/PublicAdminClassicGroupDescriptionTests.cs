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
/// Pins the shape and the behaviour of <see cref="ClassicGroupDescription"/> against Java's
/// <c>org.apache.kafka.clients.admin.ClassicGroupDescription</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a pure value type</b> — everything here is managed and broker-free, as it is
/// for its siblings <see cref="ConsumerGroupDescription"/> and
/// <see cref="MemberDescription"/>.
/// </para>
/// <para>
/// ⚠ <b>Two of the three things this class draws against
/// <see cref="ConsumerGroupDescription"/> are invisible to a behavioural test</b> — that
/// both constructors ship here where only one does there, and that the state member is
/// stored and current here where it is a deprecated projection there. Those halves are
/// therefore reflection assertions against the sibling, not prose.
/// </para>
/// </remarks>
public sealed class PublicAdminClassicGroupDescriptionTests
{
    private static readonly MemberDescription s_memberOne =
        new MemberDescription("m-1", null, null, "c-1", "h-1", null, null, null, null);

    private static readonly MemberDescription s_memberTwo =
        new MemberDescription("m-2", null, null, "c-2", "h-2", null, null, null, null);

    private static readonly Node s_coordinatorNode = new Node(7, "broker-1", 9092, "r-1");

    /// <summary>
    /// ⚠ <b>Both of Java's constructors ship, where only one of
    /// <see cref="ConsumerGroupDescription"/>'s four does.</b> The standing ruling omits
    /// <em>deprecated</em> forwarders, and this Java file deprecates nothing — neither the
    /// class nor either constructor — so the six-argument form is current API and is
    /// translated. Asserted against the sibling, because the contrast is a fact about
    /// metadata that no behavioural test can reach.
    /// </summary>
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

        // The sibling ships one of Java's four, precisely because the other three are deprecated.
        Assert.Single(typeof(ConsumerGroupDescription).GetConstructors());
    }

    /// <summary>
    /// ⚠ <b>The six-argument constructor yields <em>empty</em> authorized operations, not
    /// absent ones.</b> Java forwards <c>Set.of()</c>
    /// (<c>ClassicGroupDescription.java:48</c>), so the object it builds equals the
    /// seven-argument one given an empty collection and differs from the one given
    /// <see langword="null"/>. Forwarding null instead would be invisible in the signature
    /// and would silently convert "reported, none authorized" into "never asked".
    /// </summary>
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

    /// <summary>
    /// Every other member of the six-argument form is passed straight through, so the only
    /// difference between the two constructors is the one above.
    /// </summary>
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

    /// <summary>
    /// The collections keep Java's element types and the two nullable members are reference
    /// types — the absent-versus-present distinctions the behaviour tests below rely on have
    /// to exist in the metadata first.
    /// </summary>
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

    /// <summary>
    /// Java coalesces exactly two arguments (the id and the protocol data) and defaults the
    /// members to an empty list; the other four are stored as given, which is what keeps
    /// absence observable.
    /// </summary>
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

    /// <summary>
    /// ⚠ <b><see cref="ClassicGroupDescription.Protocol"/> is the one string Java does not
    /// coalesce</b> (<c>:59</c>, between two lines that do). An absent protocol is therefore
    /// a value distinct from the empty one — they are unequal, they hash apart, and they
    /// render apart.
    /// </summary>
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

    /// <summary>The members are copied, so a later mutation of the argument is not observed.</summary>
    [Fact]
    public void Constructor_CopiesTheMembers()
    {
        var source = new List<MemberDescription> { s_memberOne };
        var description = new ClassicGroupDescription(
            "g-1", "consumer", "range", source, ClassicGroupState.Stable, null, null);

        source.Add(s_memberTwo);

        Assert.Equal(new[] { s_memberOne }, description.Members);
    }

    /// <summary>
    /// ⚠ <b><see cref="ClassicGroupDescription.IsSimpleConsumerGroup"/> is a projection of
    /// <see cref="ClassicGroupDescription.Protocol"/>, not a stored member</b> — the opposite
    /// of <see cref="ConsumerGroupDescription.IsSimpleConsumerGroup"/>, which Java stores and
    /// takes as a constructor argument. Setting the protocol is the only thing that makes it
    /// read back, so the two cannot disagree.
    /// </summary>
    [Fact]
    public void IsSimpleConsumerGroup_IsAProjectionOfProtocol_NotAnIndependentMember()
    {
        Assert.Null(typeof(ClassicGroupDescription).GetProperty("IsSimpleConsumerGroup")!.SetMethod);

        Assert.True(WithProtocol(string.Empty).IsSimpleConsumerGroup);
        Assert.False(WithProtocol("consumer").IsSimpleConsumerGroup);
        Assert.False(WithProtocol("connect").IsSimpleConsumerGroup);

        // The sibling stores it, so there it can be set independently of every other member.
        Assert.True(new ConsumerGroupDescription(
            "g-1", true, null, "range", GroupType.Classic, GroupState.Empty, null, null, null, null)
            .IsSimpleConsumerGroup);
    }

    /// <summary>
    /// ⚠ <b>Java has no null guard on the protocol it dereferences</b>
    /// (<c>:113</c> <c>protocol.isEmpty()</c>), so an absent protocol faults rather than
    /// answering. Adding a guard would invent an answer Java does not give, and the answer it
    /// would have to invent — <c>true</c> — is the wrong one for a group whose protocol type
    /// was never reported.
    /// </summary>
    [Fact]
    public void IsSimpleConsumerGroup_FaultsOnAnAbsentProtocol_AsJavaDoes()
    {
        var description = WithProtocol(null);

        Assert.Throws<NullReferenceException>(() => _ = description.IsSimpleConsumerGroup);
    }

    /// <summary>
    /// ⚠ <b><see cref="ClassicGroupDescription.State"/> is a stored member and there is no
    /// <c>GroupState</c> beside it</b>, where <see cref="ConsumerGroupDescription"/> carries
    /// a current <c>GroupState</c> plus a deprecated <c>State</c> projection. Being stored, it
    /// round-trips every value — including one no member defines, which a parse-based
    /// projection would have normalized to <c>Unknown</c>.
    /// </summary>
    [Fact]
    public void State_IsStoredAndCurrent_UnlikeTheSiblingsProjection()
    {
        Assert.Null(typeof(ClassicGroupDescription).GetProperty("GroupState"));
        Assert.NotNull(typeof(ConsumerGroupDescription).GetProperty("GroupState"));

        Assert.Null(typeof(ClassicGroupDescription)
            .GetProperty("State")!
            .GetCustomAttribute<ObsoleteAttribute>());
        Assert.NotNull(typeof(ConsumerGroupDescription)
            .GetProperty("State")!
            .GetCustomAttribute<ObsoleteAttribute>());

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

        Assert.Contains("authorizedOperations=null)", absent.ToString(), StringComparison.Ordinal);
        Assert.Contains("authorizedOperations=[])", empty.ToString(), StringComparison.Ordinal);
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
    /// Equality covers the seven constructor members. <c>IsSimpleConsumerGroup</c> is absent
    /// from it, as it is from Java's <c>equals</c> — it carries no information
    /// <c>Protocol</c> does not.
    /// </summary>
    [Fact]
    public void Equality_CoversAllSevenMembers()
    {
        var description = Canonical();

        Assert.True(description.Equals(Canonical()));
        Assert.True(description.Equals(description));
        Assert.Equal(Canonical().GetHashCode(), description.GetHashCode());

        // Each member discriminates on its own.
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

    /// <summary>
    /// ⚠ <b>Java quotes the protocol and nothing else, and joins only the members.</b> The
    /// protocol is wrapped in single quotes (<c>:147</c>) while <c>protocolData</c> one line
    /// below is bare; the members join each element's own <c>toString()</c> with <c>","</c>
    /// and no space (<c>:149</c>), so an empty group renders as a bare <c>members=</c>; and an
    /// absent protocol, coordinator or operations set concatenates the bare text <c>null</c>.
    /// </summary>
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
            + ", state=Stable, coordinator=broker-1:9092 (id: 7 rack: r-1), "
            + "authorizedOperations=[Read, Write])",
            description.ToString());

        Assert.Equal(
            "(groupId=, protocol='null', protocolData=, members=, state=Unknown, "
            + "coordinator=null, authorizedOperations=null)",
            new ClassicGroupDescription(
                null, null, null, null, ClassicGroupState.Unknown, null, null).ToString());
    }

    /// <summary>
    /// The quoting is positional, not a property of the value: the same text renders quoted
    /// as the protocol and bare as the protocol data.
    /// </summary>
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

    /// <summary>
    /// Several members join with <c>","</c> and no space, unlike the comma-space the
    /// surrounding fields use.
    /// </summary>
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
