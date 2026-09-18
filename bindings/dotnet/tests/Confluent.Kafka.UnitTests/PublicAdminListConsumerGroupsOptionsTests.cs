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

#pragma warning disable CS0618 // Java deprecates this options type and its state enum; exercising them is the point.

/// <summary>
/// Pins <see cref="ListConsumerGroupsOptions"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupsOptions</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The load-bearing fact here is that the two state axes are <em>one</em> stored
/// filter.</b> Java keeps a single <c>Set&lt;GroupState&gt; groupStates</c> (<c>:37</c>) and
/// has no <c>states</c> field at all: the deprecated setter maps into it (<c>:58-60</c>) and
/// the deprecated getter maps out of it (<c>:86</c>). A pair of independent fields would
/// pass every single-axis test below and differ only when both are assigned — so the
/// cross-assignment and the lossy round trip are asserted explicitly rather than inferred.
/// </para>
/// <para>
/// ⚠ <b>This class has no static factories to cover</b>, unlike
/// <see cref="ListGroupsOptions"/>: Java declares none on this generation (nor any
/// constructor). The shape test asserts that absence rather than skipping it, so a later
/// slice cannot quietly invent one.
/// </para>
/// <para>
/// <b>This is a pure value type</b> — the ABI wiring, the result type and the client method
/// arrive in later slices — so everything here is managed and broker-free.
/// </para>
/// </remarks>
public sealed class PublicAdminListConsumerGroupsOptionsTests
{
    /// <summary>
    /// A freshly constructed instance matches Java's field initializers — two empty sets
    /// (<c>:37-38</c>), a deprecated view of the empty one, and an unset timeout — so
    /// <c>options: null</c> at a call site will behave like one.
    /// </summary>
    [Fact]
    public void Defaults_MatchJavasFieldInitializers()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.Empty(options.GroupStates);
        Assert.Empty(options.States);
        Assert.Empty(options.Types);
    }

    /// <summary>
    /// The public property set is exactly the timeout plus Java's three accessors — no
    /// member dropped, none invented, and no factory Java does not have.
    /// </summary>
    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasThreeAccessors()
    {
        Assert.Equal(
            new[] { "GroupStates", "States", "TimeoutMs", "Types" },
            typeof(ListConsumerGroupsOptions)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java declares no static factories on this generation — ListGroupsOptions's three
        // arrived later — so nothing static may appear here.
        Assert.Empty(
            typeof(ListConsumerGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        // Java declares no constructor either, so the only one is the implicit default.
        ConstructorInfo constructor = Assert.Single(typeof(ListConsumerGroupsOptions).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        PropertyInfo timeout = typeof(ListConsumerGroupsOptions).GetProperty(
            nameof(ListConsumerGroupsOptions.TimeoutMs))!;
        Assert.Equal(typeof(int?), timeout.PropertyType);
        Assert.NotNull(timeout.SetMethod);

        // Java's Set<T> becomes IReadOnlyCollection<T>, settable so the object initializer
        // reads the way Java's chained setters do. The deprecated axis is settable too:
        // Java's inStates(Set) is a setter, unlike ConsumerGroupListing's read-only state().
        foreach ((string name, Type element) in new[]
                 {
                     (nameof(ListConsumerGroupsOptions.GroupStates), typeof(GroupState)),
                     (nameof(ListConsumerGroupsOptions.States), typeof(ConsumerGroupState)),
                     (nameof(ListConsumerGroupsOptions.Types), typeof(GroupType)),
                 })
        {
            PropertyInfo filter = typeof(ListConsumerGroupsOptions).GetProperty(name)!;
            Assert.Equal(typeof(IReadOnlyCollection<>).MakeGenericType(element), filter.PropertyType);
            Assert.NotNull(filter.SetMethod);
        }
    }

    /// <summary>
    /// Java's <c>@Deprecated(since = "4.1")</c> on the class (<c>:33</c>) and its
    /// <c>@Deprecated</c> on the state axis (<c>:56</c>, <c>:84</c>) are mirrored as
    /// <see cref="ObsoleteAttribute"/> — at <b>warning</b> severity, so the surface stays
    /// callable as it is in Java.
    /// </summary>
    [Fact]
    public void TheDeprecations_AreMirroredAsWarnings()
    {
        ObsoleteAttribute onClass = typeof(ListConsumerGroupsOptions)
            .GetCustomAttribute<ObsoleteAttribute>()!;
        Assert.NotNull(onClass);
        Assert.False(onClass.IsError);

        ObsoleteAttribute onStates = typeof(ListConsumerGroupsOptions)
            .GetProperty(nameof(ListConsumerGroupsOptions.States))!
            .GetCustomAttribute<ObsoleteAttribute>()!;
        Assert.NotNull(onStates);
        Assert.False(onStates.IsError);

        // The two current axes are not deprecated in Java, so they carry nothing here.
        Assert.Null(
            typeof(ListConsumerGroupsOptions)
                .GetProperty(nameof(ListConsumerGroupsOptions.GroupStates))!
                .GetCustomAttribute<ObsoleteAttribute>());
        Assert.Null(
            typeof(ListConsumerGroupsOptions)
                .GetProperty(nameof(ListConsumerGroupsOptions.Types))!
                .GetCustomAttribute<ObsoleteAttribute>());
    }

    /// <summary>
    /// ⚠ The two state axes are <b>one</b> filter: assigning either replaces what the other
    /// reports. Java stores only <c>groupStates</c> (<c>:37</c>); <c>inStates</c> writes it
    /// (<c>:58-60</c>) and <c>states()</c> reads it (<c>:86</c>).
    /// </summary>
    [Fact]
    public void TheTwoStateAxes_AreOneFilterViewedThroughTwoEnums()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable, GroupState.Empty },
        };

        // Reading the deprecated axis projects the stored one.
        Assert.Equal(
            new[] { ConsumerGroupState.Stable, ConsumerGroupState.Empty }.OrderBy(state => state),
            options.States.OrderBy(state => state));

        // Writing the deprecated axis REPLACES the stored one — it does not union with it.
        options.States = new[] { ConsumerGroupState.Dead };

        Assert.Equal(new[] { GroupState.Dead }, options.GroupStates);
        Assert.Equal(new[] { ConsumerGroupState.Dead }, options.States);

        // And back the other way.
        options.GroupStates = new[] { GroupState.Assigning };

        Assert.Equal(new[] { GroupState.Assigning }, options.GroupStates);
        Assert.Equal(new[] { ConsumerGroupState.Assigning }, options.States);

        // The group-type filter is a genuinely separate field (:38) and survives both.
        options.Types = new[] { GroupType.Consumer };
        options.States = new[] { ConsumerGroupState.Empty };
        Assert.Equal(new[] { GroupType.Consumer }, options.Types);
    }

    /// <summary>
    /// The write direction is total — every <see cref="ConsumerGroupState"/> member names a
    /// <see cref="GroupState"/> member — which is Java's <c>GroupState.parse(state
    /// .toString())</c> (<c>:60</c>) never falling back.
    /// </summary>
    [Fact]
    public void WritingTheDeprecatedAxis_MapsEveryMemberOntoItsNamesake()
    {
        ConsumerGroupState[] all = (ConsumerGroupState[])Enum.GetValues(typeof(ConsumerGroupState));

        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions { States = all };

        Assert.Equal(
            new[]
            {
                GroupState.Unknown,
                GroupState.PreparingRebalance,
                GroupState.CompletingRebalance,
                GroupState.Stable,
                GroupState.Dead,
                GroupState.Empty,
                GroupState.Assigning,
                GroupState.Reconciling,
            }.OrderBy(state => state),
            options.GroupStates.OrderBy(state => state));

        // Round-tripping is the identity in this direction, since nothing was lost.
        Assert.Equal(all.OrderBy(state => state), options.States.OrderBy(state => state));
    }

    /// <summary>
    /// ⚠ The read direction is <b>lossy</b>: <see cref="GroupState.NotReady"/> has no
    /// counterpart on the deprecated enum, so it projects to
    /// <see cref="ConsumerGroupState.Unknown"/> — Java's <c>parse</c> answering
    /// <c>UNKNOWN</c> for a name it does not recognise — and two members can collapse into
    /// one, which <c>Collectors.toSet()</c> (<c>:86</c>) de-duplicates.
    /// </summary>
    [Fact]
    public void ReadingTheDeprecatedAxis_ProjectsNotReadyToUnknownAndDeduplicates()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable, GroupState.NotReady },
        };

        Assert.Equal(
            new[] { ConsumerGroupState.Stable, ConsumerGroupState.Unknown }.OrderBy(state => state),
            options.States.OrderBy(state => state));

        // Two stored members, one projected member — the collapse is real, not theoretical.
        options.GroupStates = new[] { GroupState.NotReady, GroupState.Unknown };
        Assert.Equal(new[] { ConsumerGroupState.Unknown }, options.States);

        // So the round trip through the deprecated axis is not the identity: writing back
        // what was read stores Unknown, not NotReady.
        options.GroupStates = new[] { GroupState.NotReady };
        IReadOnlyCollection<ConsumerGroupState> readBack = options.States;
        options.States = readBack;
        Assert.Equal(new[] { GroupState.Unknown }, options.GroupStates);
    }

    /// <summary>
    /// Writing an undefined <see cref="ConsumerGroupState"/> through <see cref="ListConsumerGroupsOptions.States"/>
    /// throws — the same reject-an-undefined-cast contract <see cref="ListConsumerGroupsOptions.GroupStates"/> and
    /// <see cref="ListConsumerGroupsOptions.Types"/> get for free from <c>NativeAdminClient.FilterNames</c> at
    /// submit time, restated here since this setter has no submit-time check to fall back on.
    /// </summary>
    [Fact]
    public void AnUndefinedConsumerGroupStateValue_ThrowsWhenWrittenThroughStates()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();

        ArgumentOutOfRangeException thrown = Assert.Throws<ArgumentOutOfRangeException>(
            () => options.States = new[] { (ConsumerGroupState)99 });
        Assert.Equal("value", thrown.ParamName);
        Assert.Equal((ConsumerGroupState)99, thrown.ActualValue);
        Assert.StartsWith(
            "ListConsumerGroupsOptions.States must contain only defined ConsumerGroupState members.",
            thrown.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A <see cref="GroupState"/> value no member defines names nothing this client knows, so
    /// reading it back through the deprecated <see cref="ListConsumerGroupsOptions.States"/> axis
    /// takes the same <c>Unknown</c> answer the name tables give it everywhere else. Unlike the
    /// write direction above, this is a pure in-memory projection with no submit-time check behind
    /// it to make throwing the natural choice.
    /// </summary>
    [Fact]
    public void AnUndefinedGroupStateValue_ProjectsToUnknown_WhenReadThroughStates()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions
        {
            GroupStates = new[] { (GroupState)99 },
        };
        Assert.Equal(new[] { ConsumerGroupState.Unknown }, options.States);
    }

    /// <summary>
    /// Null and empty both normalize to the empty set on all three axes — Java's
    /// <c>(x == null || x.isEmpty()) ? Collections.emptySet()</c> (<c>:46</c>, <c>:58</c>,
    /// <c>:69</c>).
    /// </summary>
    [Fact]
    public void Filters_NormalizeNullAndEmptyToTheEmptySet()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable },
            Types = new[] { GroupType.Consumer },
        };

        options.GroupStates = Array.Empty<GroupState>();
        options.Types = Array.Empty<GroupType>();

        Assert.Empty(options.GroupStates);
        Assert.Empty(options.States);
        Assert.Empty(options.Types);

        // The deprecated setter clears the one stored filter the same way (:58).
        options.GroupStates = new[] { GroupState.Stable };
        options.States = Array.Empty<ConsumerGroupState>();
        Assert.Empty(options.GroupStates);

        // A caller that defeats the non-nullable annotation still gets Java's behavior
        // rather than a stored null, so the getters keep their never-null contract.
        options.GroupStates = new[] { GroupState.Stable };
        options.Types = new[] { GroupType.Consumer };

        options.GroupStates = null!;
        options.Types = null!;

        Assert.Empty(options.GroupStates);
        Assert.Empty(options.Types);

        options.GroupStates = new[] { GroupState.Stable };
        options.States = null!;
        Assert.Empty(options.GroupStates);
        Assert.Empty(options.States);
    }

    /// <summary>
    /// Each filter stores a de-duplicated copy, so a later mutation of the caller's
    /// collection is not observed — Java's <c>Set.copyOf</c> (<c>:46</c>) and its two
    /// <c>HashSet</c>-shaped siblings (<c>:60</c>, <c>:69</c>).
    /// </summary>
    [Fact]
    public void Filters_StoreADeduplicatedDefensiveCopy()
    {
        List<GroupState> groupStates = new List<GroupState> { GroupState.Stable, GroupState.Stable, GroupState.Empty };
        List<GroupType> types = new List<GroupType> { GroupType.Consumer, GroupType.Consumer };
        List<ConsumerGroupState> states = new List<ConsumerGroupState>
        {
            ConsumerGroupState.Dead,
            ConsumerGroupState.Dead,
        };

        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions
        {
            GroupStates = groupStates,
            Types = types,
        };

        Assert.Equal(
            new[] { GroupState.Stable, GroupState.Empty }.OrderBy(state => state),
            options.GroupStates.OrderBy(state => state));
        Assert.Equal(new[] { GroupType.Consumer }, options.Types);

        groupStates.Add(GroupState.Dead);
        types.Add(GroupType.Share);

        Assert.Equal(
            new[] { GroupState.Stable, GroupState.Empty }.OrderBy(state => state),
            options.GroupStates.OrderBy(state => state));
        Assert.Equal(new[] { GroupType.Consumer }, options.Types);

        options.States = states;
        Assert.Equal(new[] { GroupState.Dead }, options.GroupStates);

        states.Add(ConsumerGroupState.Empty);
        Assert.Equal(new[] { GroupState.Dead }, options.GroupStates);
    }

    /// <summary>
    /// The stored copy is immutable, so a caller who downcasts it cannot rewrite this
    /// instance's filter behind its back — Java's <c>Set.copyOf</c> result throws too.
    /// </summary>
    /// <remarks>
    /// The deprecated getter's collection is minted per call, as Java's
    /// <c>stream().collect(…)</c> (<c>:86</c>) is, so mutating it could never reach the
    /// stored filter — it is frozen anyway, for one reading of the surface rather than two.
    /// </remarks>
    [Fact]
    public void Filters_StoreAnImmutableCopy()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable },
            Types = new[] { GroupType.Consumer },
        };

        Assert.Throws<NotSupportedException>(
            () => ((IList<GroupState>)options.GroupStates).Add(GroupState.Dead));
        Assert.Throws<NotSupportedException>(
            () => ((IList<GroupType>)options.Types).Clear());
        Assert.Throws<NotSupportedException>(
            () => ((IList<ConsumerGroupState>)options.States).Add(ConsumerGroupState.Dead));

        // Assigning through the deprecated axis stores an immutable copy too.
        options.States = new[] { ConsumerGroupState.Empty };
        Assert.Throws<NotSupportedException>(
            () => ((IList<GroupState>)options.GroupStates).Add(GroupState.Dead));

        // The deprecated getter mints per call rather than handing back a stored object.
        Assert.NotSame(options.States, options.States);
    }

    /// <summary>
    /// The timeout is the nullable per-request timeout every options type in this binding
    /// carries — Java's inherited <c>AbstractOptions.timeoutMs()</c> (<c>:35</c>'s
    /// <c>extends</c>).
    /// </summary>
    [Fact]
    public void TimeoutMs_IsNullableAndRoundTrips()
    {
        ListConsumerGroupsOptions options = new ListConsumerGroupsOptions { TimeoutMs = 30_000 };
        Assert.Equal(30_000, options.TimeoutMs);

        options.TimeoutMs = null;
        Assert.Null(options.TimeoutMs);
    }
}
