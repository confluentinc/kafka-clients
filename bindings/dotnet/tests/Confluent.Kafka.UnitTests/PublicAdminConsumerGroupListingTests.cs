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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

#pragma warning disable CS0618 // Java deprecates this listing type and its state enum; exercising them is the point.

/// <summary>
/// Pins the shape and the behaviour of <see cref="ConsumerGroupListing"/> against Java's
/// <c>org.apache.kafka.clients.admin.ConsumerGroupListing</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a pure value type — there is no client method, no options and no result
/// carrying it yet</b>, so everything here is managed and broker-free. The ABI wiring that
/// will populate one arrives in a later slice; when it does, the <see cref="State"/>
/// projection asserted below is the contract it must satisfy, because
/// <c>kafka_admin_ConsumerGroupListing_state</c> derives its answer the same way.
/// </para>
/// <para>
/// ⚠ <b>C# upcasts and widens silently, so the shape half has to be reflection
/// assertions.</b> A behavioural test passes equally against a property and a method, and
/// against <c>GroupState</c> and <c>GroupState?</c> — the reading
/// <c>PublicAdminP3ShapeParityTests</c> already makes for this surface.
/// </para>
/// </remarks>
public sealed class PublicAdminConsumerGroupListingTests
{
    /// <summary>
    /// Java's three <b>current</b> constructors ship, with Java's parameter shapes. The two
    /// it deprecates (<c>:58</c>, <c>:72</c>) deliberately do not — a ruling for this
    /// phase, and the reason the count is exactly three rather than five.
    /// </summary>
    [Fact]
    public void Constructors_AreJavasThreeCurrentOnes()
    {
        Type[][] actual = typeof(ConsumerGroupListing)
            .GetConstructors()
            .Select(constructor => constructor.GetParameters().Select(p => p.ParameterType).ToArray())
            .OrderBy(parameters => parameters.Length)
            .ToArray();

        Assert.Equal(3, actual.Length);

        // :45 — (String groupId, boolean isSimpleConsumerGroup)
        Assert.Equal(new[] { typeof(string), typeof(bool) }, actual[0]);

        // :88 — (String, Optional<GroupState>, boolean)
        Assert.Equal(new[] { typeof(string), typeof(GroupState?), typeof(bool) }, actual[1]);

        // :104 — (String, Optional<GroupState>, Optional<GroupType>, boolean)
        Assert.Equal(
            new[] { typeof(string), typeof(GroupState?), typeof(GroupType?), typeof(bool) }, actual[2]);
    }

    /// <summary>
    /// Every Java accessor is present, as a property, with the type Java's return maps to.
    /// </summary>
    [Fact]
    public void Accessors_AreJavasFive_AsProperties()
    {
        Assert.Equal(typeof(string), PropertyType(nameof(ConsumerGroupListing.GroupId)));
        Assert.Equal(typeof(bool), PropertyType(nameof(ConsumerGroupListing.IsSimpleConsumerGroup)));
        Assert.Equal(typeof(GroupState?), PropertyType(nameof(ConsumerGroupListing.GroupState)));
        Assert.Equal(typeof(GroupType?), PropertyType(nameof(ConsumerGroupListing.Type)));
        Assert.Equal(typeof(ConsumerGroupState?), PropertyType(nameof(ConsumerGroupListing.State)));

        // All five are read-only, as Java's accessors are: the four fields are final (:34-37)
        // and state() is computed (:143).
        foreach (string name in new[]
                 {
                     nameof(ConsumerGroupListing.GroupId),
                     nameof(ConsumerGroupListing.IsSimpleConsumerGroup),
                     nameof(ConsumerGroupListing.GroupState),
                     nameof(ConsumerGroupListing.Type),
                     nameof(ConsumerGroupListing.State),
                 })
        {
            Assert.Null(typeof(ConsumerGroupListing).GetProperty(name)!.SetMethod);
        }
    }

    /// <summary>
    /// Java's <c>@Deprecated</c> markers cross over: the type itself (<c>:32</c>) and
    /// <c>state()</c> (<c>:141</c>) carry <see cref="ObsoleteAttribute"/>, and both are
    /// <b>warnings</b> — <c>forRemoval = true</c> raises javac's removal-warning category,
    /// it does not reject the call, so an error here would be stricter than Java.
    /// </summary>
    [Fact]
    public void Deprecation_IsCarriedAcross_AtWarningSeverity()
    {
        ObsoleteAttribute onType =
            typeof(ConsumerGroupListing).GetCustomAttribute<ObsoleteAttribute>()!;
        Assert.False(onType.IsError);
        Assert.Contains("GroupListing", onType.Message, StringComparison.Ordinal);

        ObsoleteAttribute onState = typeof(ConsumerGroupListing)
            .GetProperty(nameof(ConsumerGroupListing.State))!
            .GetCustomAttribute<ObsoleteAttribute>()!;
        Assert.False(onState.IsError);
        Assert.Contains("GroupState", onState.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The canonical constructor (<c>:104</c>) stores every member, and each reads back.
    /// </summary>
    [Fact]
    public void CanonicalConstructor_ReadsBackEveryAccessor()
    {
        ConsumerGroupListing listing =
            new ConsumerGroupListing("cgl-a", GroupState.Stable, GroupType.Consumer, true);

        Assert.Equal("cgl-a", listing.GroupId);
        Assert.True(listing.IsSimpleConsumerGroup);
        Assert.Equal(GroupState.Stable, listing.GroupState);
        Assert.Equal(GroupType.Consumer, listing.Type);
        Assert.Equal(ConsumerGroupState.Stable, listing.State);
    }

    /// <summary>
    /// The two shorter constructors forward <c>Optional.empty()</c> exactly where Java does
    /// — <c>null</c> here — and an absent <see cref="ConsumerGroupListing.GroupState"/>
    /// makes <see cref="ConsumerGroupListing.State"/> absent too, because Java's
    /// <c>Optional.map</c> on an empty yields an empty (<c>:143</c>).
    /// </summary>
    [Fact]
    public void ShorterConstructors_LeaveTheOptionalsAbsent()
    {
        // :45 — both Optionals empty.
        ConsumerGroupListing neither = new ConsumerGroupListing("cgl-b", isSimpleConsumerGroup: false);
        Assert.Equal("cgl-b", neither.GroupId);
        Assert.False(neither.IsSimpleConsumerGroup);
        Assert.Null(neither.GroupState);
        Assert.Null(neither.Type);
        Assert.Null(neither.State);

        // :88 — type empty, state present.
        ConsumerGroupListing stateOnly =
            new ConsumerGroupListing("cgl-c", GroupState.Empty, isSimpleConsumerGroup: true);
        Assert.Equal(GroupState.Empty, stateOnly.GroupState);
        Assert.Null(stateOnly.Type);
        Assert.Equal(ConsumerGroupState.Empty, stateOnly.State);

        // An explicitly-null state through the canonical constructor is the same empty.
        ConsumerGroupListing explicitlyEmpty =
            new ConsumerGroupListing("cgl-d", null, GroupType.Classic, false);
        Assert.Null(explicitlyEmpty.GroupState);
        Assert.Null(explicitlyEmpty.State);
        Assert.Equal(GroupType.Classic, explicitlyEmpty.Type);
    }

    /// <summary>
    /// The two state axes are <b>independently readable</b> — different enums, and for one
    /// member deliberately different answers — while remaining one axis underneath, as
    /// Java's <c>state()</c> is a <c>map</c> of <c>groupState()</c> (<c>:143</c>) and as the
    /// C ABI documents its two accessors to be.
    /// </summary>
    /// <remarks>
    /// ⚠ <b><c>NotReady</c> is the whole reason the deprecated axis is kept.</b> It exists
    /// on <see cref="GroupState"/> and not on <see cref="ConsumerGroupState"/>, so the
    /// projection answers <c>Unknown</c> and cannot be run backwards — the one member where
    /// reading the two axes gives genuinely different information.
    /// </remarks>
    [Fact]
    public void TheTwoStateAxes_AgreeByName_ExceptWhereConsumerGroupStateCannotFollow()
    {
        (GroupState Group, ConsumerGroupState Consumer)[] sameName =
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

        foreach ((GroupState group, ConsumerGroupState consumer) in sameName)
        {
            ConsumerGroupListing listing = new ConsumerGroupListing("cgl-e", group, null, false);
            Assert.Equal(group, listing.GroupState);
            Assert.Equal(consumer, listing.State);
        }

        // The one divergence: GroupState.NotReady has no ConsumerGroupState counterpart, so
        // Java's parse answers UNKNOWN for its name (ConsumerGroupState.java:53-56).
        ConsumerGroupListing notReady =
            new ConsumerGroupListing("cgl-f", GroupState.NotReady, null, false);
        Assert.Equal(GroupState.NotReady, notReady.GroupState);
        Assert.Equal(ConsumerGroupState.Unknown, notReady.State);

        // ...and reading State alone cannot tell that apart from a genuine Unknown, which is
        // exactly the information loss that makes the current axis the one to prefer.
        ConsumerGroupListing unknown =
            new ConsumerGroupListing("cgl-f", GroupState.Unknown, null, false);
        Assert.Equal(notReady.State, unknown.State);
        Assert.NotEqual(notReady.GroupState, unknown.GroupState);
    }

    /// <summary>
    /// Value equality and hashing cover Java's four stored members (<c>:167</c>,
    /// <c>:175-178</c>) — and only those: <see cref="ConsumerGroupListing.State"/> is
    /// derived, so it is absent from both, as it is in Java.
    /// </summary>
    [Fact]
    public void Equality_CoversTheFourStoredMembers()
    {
        ConsumerGroupListing listing =
            new ConsumerGroupListing("cgl-g", GroupState.Stable, GroupType.Consumer, true);

        ConsumerGroupListing same =
            new ConsumerGroupListing("cgl-g", GroupState.Stable, GroupType.Consumer, true);
        Assert.Equal(same, listing);
        Assert.Equal(same.GetHashCode(), listing.GetHashCode());

        // Each of the four discriminates on its own.
        Assert.NotEqual(
            new ConsumerGroupListing("cgl-h", GroupState.Stable, GroupType.Consumer, true), listing);
        Assert.NotEqual(
            new ConsumerGroupListing("cgl-g", GroupState.Empty, GroupType.Consumer, true), listing);
        Assert.NotEqual(
            new ConsumerGroupListing("cgl-g", GroupState.Stable, GroupType.Classic, true), listing);
        Assert.NotEqual(
            new ConsumerGroupListing("cgl-g", GroupState.Stable, GroupType.Consumer, false), listing);

        // An absent Optional is not the Unknown member (GroupListing draws the same line).
        Assert.NotEqual(
            new ConsumerGroupListing("cgl-g", GroupState.Stable, null, true), listing);
        Assert.NotEqual(
            new ConsumerGroupListing("cgl-g", null, GroupType.Consumer, true), listing);

        Assert.False(listing.Equals(null));
        Assert.False(listing.Equals("cgl-g"));
    }

    /// <summary>
    /// <c>toString()</c> reproduces Java's rendering verbatim (<c>:156-163</c>): the
    /// <c>Optional</c>s go in raw, so they read <c>Optional[X]</c> / <c>Optional.empty</c>
    /// rather than the <c>none</c> that <see cref="GroupListing"/>'s Java chooses via
    /// <c>orElse</c> — and the id's quote is closed here, which that sibling's is not.
    /// </summary>
    [Fact]
    public void ToString_MirrorsJavasRendering()
    {
        Assert.Equal(
            "(groupId='cgl-i', isSimpleConsumerGroup=false, groupState=Optional[Stable], type=Optional[Consumer])",
            new ConsumerGroupListing("cgl-i", GroupState.Stable, GroupType.Consumer, false).ToString());

        Assert.Equal(
            "(groupId='cgl-j', isSimpleConsumerGroup=true, groupState=Optional.empty, type=Optional.empty)",
            new ConsumerGroupListing("cgl-j", isSimpleConsumerGroup: true).ToString());

        // The boolean is Java's lowercase spelling, not .NET's "True"/"False".
        Assert.Contains(
            "isSimpleConsumerGroup=true",
            new ConsumerGroupListing("cgl-k", GroupState.NotReady, null, true).ToString(),
            StringComparison.Ordinal);

        // NotReady renders on its own axis, unprojected.
        Assert.Contains(
            "groupState=Optional[NotReady]",
            new ConsumerGroupListing("cgl-k", GroupState.NotReady, null, true).ToString(),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// All three constructors reject a null id — deliberately stricter than Java, which
    /// assigns it raw (<c>:110</c>), because <see cref="ConsumerGroupListing.GroupId"/> is
    /// non-nullable under <c>#nullable enable</c>.
    /// </summary>
    [Fact]
    public void NullGroupId_IsRejectedByEveryConstructor()
    {
        Assert.Equal(
            "groupId",
            Assert.Throws<ArgumentNullException>(() => new ConsumerGroupListing(null!, false)).ParamName);
        Assert.Equal(
            "groupId",
            Assert.Throws<ArgumentNullException>(
                () => new ConsumerGroupListing(null!, GroupState.Stable, false)).ParamName);
        Assert.Equal(
            "groupId",
            Assert.Throws<ArgumentNullException>(
                () => new ConsumerGroupListing(null!, GroupState.Stable, GroupType.Consumer, false)).ParamName);
    }

    /// <summary>The declared type of one public property on the listing.</summary>
    /// <param name="name">The property name.</param>
    private static Type PropertyType(string name) =>
        typeof(ConsumerGroupListing).GetProperty(name)!.PropertyType;
}

#pragma warning restore CS0618
