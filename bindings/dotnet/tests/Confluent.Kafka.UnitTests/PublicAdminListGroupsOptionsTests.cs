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
/// Pins <see cref="ListGroupsOptions"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListGroupsOptions</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The normalization is the whole behavior of this class, so it is tested per filter
/// rather than sampled.</b> Java puts the same
/// <c>(x == null || x.isEmpty()) ? Set.of() : Set.copyOf(x)</c> ternary in all three setters
/// (<c>:68</c>, <c>:77</c>, <c>:86</c>), and the .NET realization routes two of them through
/// one helper and the third through another — so a single-filter test would leave the
/// string path, the one that differs, unproven.
/// </para>
/// <para>
/// ⚠ <b><see cref="ListGroupsOptions.ForConsumerGroups"/> is the one factory that sets two
/// filters</b> (<c>:39-41</c>), and the empty protocol type it asks for is easy to mistake
/// for a placeholder. Both members are asserted by name.
/// </para>
/// </remarks>
public sealed class PublicAdminListGroupsOptionsTests
{
    /// <summary>
    /// A freshly constructed instance matches Java's field initializers — three empty sets
    /// (<c>:30-32</c>) and an unset timeout — so <c>options: null</c> at a call site will
    /// behave like one.
    /// </summary>
    [Fact]
    public void Defaults_MatchJavasFieldInitializers()
    {
        ListGroupsOptions options = new ListGroupsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.Empty(options.GroupStates);
        Assert.Empty(options.ProtocolTypes);
        Assert.Empty(options.Types);
    }

    /// <summary>
    /// The public property set is exactly the timeout plus Java's three filters — no member
    /// dropped, none invented.
    /// </summary>
    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasThreeFilters()
    {
        Assert.Equal(
            new[] { "GroupStates", "ProtocolTypes", "TimeoutMs", "Types" },
            typeof(ListGroupsOptions)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java's three static factories, and nothing else static.
        Assert.Equal(
            new[] { "ForConsumerGroups", "ForShareGroups", "ForStreamsGroups" },
            typeof(ListGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        PropertyInfo timeout = typeof(ListGroupsOptions).GetProperty(nameof(ListGroupsOptions.TimeoutMs))!;
        Assert.Equal(typeof(int?), timeout.PropertyType);
        Assert.NotNull(timeout.SetMethod);

        // Java's Set<T> becomes IReadOnlyCollection<T>, settable so the object initializer
        // reads the way Java's chained setters do.
        foreach ((string name, Type element) in new[]
                 {
                     (nameof(ListGroupsOptions.GroupStates), typeof(GroupState)),
                     (nameof(ListGroupsOptions.ProtocolTypes), typeof(string)),
                     (nameof(ListGroupsOptions.Types), typeof(GroupType)),
                 })
        {
            PropertyInfo filter = typeof(ListGroupsOptions).GetProperty(name)!;
            Assert.Equal(typeof(IReadOnlyCollection<>).MakeGenericType(element), filter.PropertyType);
            Assert.NotNull(filter.SetMethod);
        }
    }

    /// <summary>
    /// <c>forConsumerGroups()</c> (<c>:38</c>) sets <b>both</b> the group-type and the
    /// protocol-type filter, and leaves the state filter alone.
    /// </summary>
    [Fact]
    public void ForConsumerGroups_SetsBothTheTypeAndProtocolTypeFilters()
    {
        ListGroupsOptions options = ListGroupsOptions.ForConsumerGroups();

        Assert.Equal(
            new[] { GroupType.Consumer, GroupType.Classic }.OrderBy(type => type),
            options.Types.OrderBy(type => type));

        // ⚠ The empty string is a member of the set, not a placeholder: it is the protocol
        // type of a classic group that is not using one. "consumer" is
        // ConsumerProtocol.PROTOCOL_TYPE (ConsumerProtocol.java:45).
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes.OrderBy(protocolType => protocolType, StringComparer.Ordinal));

        Assert.Empty(options.GroupStates);
        Assert.Null(options.TimeoutMs);
    }

    /// <summary>
    /// <c>forShareGroups()</c> (<c>:48</c>) and <c>forStreamsGroups()</c> (<c>:57</c>) set
    /// the group-type filter only — no protocol-type filter, unlike
    /// <see cref="ListGroupsOptions.ForConsumerGroups"/>.
    /// </summary>
    [Fact]
    public void ForShareAndStreamsGroups_SetTheTypeFilterOnly()
    {
        ListGroupsOptions share = ListGroupsOptions.ForShareGroups();
        Assert.Equal(new[] { GroupType.Share }, share.Types);
        Assert.Empty(share.ProtocolTypes);
        Assert.Empty(share.GroupStates);
        Assert.Null(share.TimeoutMs);

        ListGroupsOptions streams = ListGroupsOptions.ForStreamsGroups();
        Assert.Equal(new[] { GroupType.Streams }, streams.Types);
        Assert.Empty(streams.ProtocolTypes);
        Assert.Empty(streams.GroupStates);
        Assert.Null(streams.TimeoutMs);
    }

    /// <summary>
    /// Every factory mints a fresh instance, as Java's <c>new ListGroupsOptions()</c> does —
    /// so configuring one caller's options cannot reach another's.
    /// </summary>
    [Fact]
    public void Factories_MintAFreshInstancePerCall()
    {
        Assert.NotSame(ListGroupsOptions.ForConsumerGroups(), ListGroupsOptions.ForConsumerGroups());
        Assert.NotSame(ListGroupsOptions.ForShareGroups(), ListGroupsOptions.ForShareGroups());
        Assert.NotSame(ListGroupsOptions.ForStreamsGroups(), ListGroupsOptions.ForStreamsGroups());

        ListGroupsOptions first = ListGroupsOptions.ForShareGroups();
        first.TimeoutMs = 5_000;
        Assert.Equal(5_000, first.TimeoutMs);
        Assert.Null(ListGroupsOptions.ForShareGroups().TimeoutMs);
    }

    /// <summary>
    /// Null and empty both normalize to the empty set on all three filters — Java's
    /// <c>(x == null || x.isEmpty()) ? Set.of()</c> (<c>:68</c>, <c>:77</c>, <c>:86</c>).
    /// </summary>
    [Fact]
    public void Filters_NormalizeNullAndEmptyToTheEmptySet()
    {
        ListGroupsOptions options = ListGroupsOptions.ForConsumerGroups();
        options.GroupStates = new[] { GroupState.Stable };

        options.GroupStates = Array.Empty<GroupState>();
        options.Types = Array.Empty<GroupType>();
        options.ProtocolTypes = Array.Empty<string>();

        Assert.Empty(options.GroupStates);
        Assert.Empty(options.Types);
        Assert.Empty(options.ProtocolTypes);

        // A caller that defeats the non-nullable annotation still gets Java's behavior
        // rather than a stored null, so the getters keep their never-null contract.
        options.GroupStates = new[] { GroupState.Stable };
        options.Types = new[] { GroupType.Consumer };
        options.ProtocolTypes = new[] { "consumer" };

        options.GroupStates = null!;
        options.Types = null!;
        options.ProtocolTypes = null!;

        Assert.Empty(options.GroupStates);
        Assert.Empty(options.Types);
        Assert.Empty(options.ProtocolTypes);
    }

    /// <summary>
    /// Each filter stores a de-duplicated copy, so a later mutation of the caller's
    /// collection is not observed — Java's <c>Set.copyOf</c>.
    /// </summary>
    [Fact]
    public void Filters_StoreADeduplicatedDefensiveCopy()
    {
        List<GroupState> states = new List<GroupState> { GroupState.Stable, GroupState.Stable, GroupState.Empty };
        List<GroupType> types = new List<GroupType> { GroupType.Consumer, GroupType.Consumer };
        List<string> protocolTypes = new List<string> { "consumer", "consumer", "" };

        ListGroupsOptions options = new ListGroupsOptions
        {
            GroupStates = states,
            Types = types,
            ProtocolTypes = protocolTypes,
        };

        Assert.Equal(
            new[] { GroupState.Stable, GroupState.Empty }.OrderBy(state => state),
            options.GroupStates.OrderBy(state => state));
        Assert.Equal(new[] { GroupType.Consumer }, options.Types);
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes.OrderBy(protocolType => protocolType, StringComparer.Ordinal));

        states.Add(GroupState.Dead);
        types.Add(GroupType.Share);
        protocolTypes.Add("share");

        Assert.Equal(
            new[] { GroupState.Stable, GroupState.Empty }.OrderBy(state => state),
            options.GroupStates.OrderBy(state => state));
        Assert.Equal(new[] { GroupType.Consumer }, options.Types);
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes.OrderBy(protocolType => protocolType, StringComparer.Ordinal));
    }

    /// <summary>
    /// The stored copy is immutable, so a caller who downcasts it cannot rewrite this
    /// instance's filter behind its back — Java's <c>Set.copyOf</c> result throws too.
    /// </summary>
    [Fact]
    public void Filters_StoreAnImmutableCopy()
    {
        ListGroupsOptions options = new ListGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable },
            Types = new[] { GroupType.Consumer },
            ProtocolTypes = new[] { "consumer" },
        };

        Assert.Throws<NotSupportedException>(() => ((IList<GroupState>)options.GroupStates).Add(GroupState.Dead));
        Assert.Throws<NotSupportedException>(() => ((IList<GroupType>)options.Types).Clear());
        Assert.Throws<NotSupportedException>(() => ((IList<string>)options.ProtocolTypes).Add("share"));
    }

    /// <summary>
    /// A null protocol type is rejected — Java's <c>Set.copyOf</c> throws
    /// <c>NullPointerException</c> on a null element, and the element type here is
    /// non-nullable.
    /// </summary>
    [Fact]
    public void ProtocolTypes_RejectANullElement()
    {
        ListGroupsOptions options = new ListGroupsOptions();

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => options.ProtocolTypes = new[] { "consumer", null! });

        Assert.Contains("must not be null", error.Message, StringComparison.Ordinal);

        // The rejected set never lands: the filter is still Java's Set.of().
        Assert.Empty(options.ProtocolTypes);
    }

    /// <summary>
    /// The timeout is the nullable per-request timeout every options type in this binding
    /// carries — Java's inherited <c>AbstractOptions.timeoutMs()</c>.
    /// </summary>
    [Fact]
    public void TimeoutMs_IsNullableAndRoundTrips()
    {
        ListGroupsOptions options = new ListGroupsOptions { TimeoutMs = 30_000 };
        Assert.Equal(30_000, options.TimeoutMs);

        options.TimeoutMs = null;
        Assert.Null(options.TimeoutMs);
    }
}
