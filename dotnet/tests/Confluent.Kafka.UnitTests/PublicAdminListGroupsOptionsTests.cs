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
/// <see cref="ListGroupsOptions"/> against Java's <c>org.apache.kafka.clients.admin.ListGroupsOptions</c>: its three
/// factories (<see cref="ListGroupsOptions.ForConsumerGroups"/> sets two filters), and each filter's normalization of
/// null and empty to the empty set, tested per filter.
/// </summary>
public sealed class PublicAdminListGroupsOptionsTests
{
    [Fact]
    public void Defaults_MatchJavasFieldInitializers()
    {
        ListGroupsOptions options = new ListGroupsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.Empty(options.GroupStates);
        Assert.Empty(options.ProtocolTypes);
        Assert.Empty(options.Types);
    }

    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasThreeFilters()
    {
        Assert.Equal(
            new[] { "GroupStates", "ProtocolTypes", "TimeoutMs", "Types" },
            typeof(ListGroupsOptions)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Equal(
            new[] { "ForConsumerGroups", "ForShareGroups", "ForStreamsGroups" },
            typeof(ListGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        PropertyInfo timeout = typeof(ListGroupsOptions).GetProperty(nameof(ListGroupsOptions.TimeoutMs))!;
        Assert.Equal(typeof(int?), timeout.PropertyType);
        Assert.NotNull(timeout.SetMethod);

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

    [Fact]
    public void ForConsumerGroups_SetsBothTheTypeAndProtocolTypeFilters()
    {
        ListGroupsOptions options = ListGroupsOptions.ForConsumerGroups();

        Assert.Equal(
            new[] { GroupType.Consumer, GroupType.Classic }.OrderBy(type => type),
            options.Types.OrderBy(type => type));

        // The empty string is a member, not a placeholder: the protocol type of a classic group that uses none.
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes.OrderBy(protocolType => protocolType, StringComparer.Ordinal));

        Assert.Empty(options.GroupStates);
        Assert.Null(options.TimeoutMs);
    }

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

    [Fact]
    public void ProtocolTypes_RejectANullElement()
    {
        ListGroupsOptions options = new ListGroupsOptions();

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => options.ProtocolTypes = new[] { "consumer", null! });

        Assert.Contains("must not be null", error.Message, StringComparison.Ordinal);

        Assert.Empty(options.ProtocolTypes);
    }

    [Fact]
    public void TimeoutMs_IsNullableAndRoundTrips()
    {
        ListGroupsOptions options = new ListGroupsOptions { TimeoutMs = 30_000 };
        Assert.Equal(30_000, options.TimeoutMs);

        options.TimeoutMs = null;
        Assert.Null(options.TimeoutMs);
    }
}
