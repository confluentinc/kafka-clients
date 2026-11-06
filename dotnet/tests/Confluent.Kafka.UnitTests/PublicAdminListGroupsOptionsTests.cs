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

        Assert.Null(options.TimeoutMs());
        Assert.Empty(options.GroupStates());
        Assert.Empty(options.ProtocolTypes());
        Assert.Empty(options.Types());
    }

    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasThreeFilters()
    {
        Type type = typeof(ListGroupsOptions);
        Assert.Empty(type.GetProperties());
        Assert.Equal(new[] { "GroupStates", "ProtocolTypes", "TimeoutMs", "Types" }, Getters.NamesOf(type));
        Assert.Equal(
            new[] { "InGroupStates", "TimeoutMs", "WithProtocolTypes", "WithTypes" },
            SetterMethods.NamesOf(type));

        Assert.Equal(
            new[] { "ForConsumerGroups", "ForShareGroups", "ForStreamsGroups" },
            typeof(ListGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Equal(typeof(int?), Getters.Get(type, nameof(ListGroupsOptions.TimeoutMs)).ReturnType);
        Assert.Equal(typeof(int?), SetterMethods.ParameterTypeOf(type, nameof(ListGroupsOptions.TimeoutMs)));

        foreach ((string getter, string setter, Type element) in new[]
                 {
                     (nameof(ListGroupsOptions.GroupStates), nameof(ListGroupsOptions.InGroupStates), typeof(GroupState)),
                     (nameof(ListGroupsOptions.ProtocolTypes), nameof(ListGroupsOptions.WithProtocolTypes), typeof(string)),
                     (nameof(ListGroupsOptions.Types), nameof(ListGroupsOptions.WithTypes), typeof(GroupType)),
                 })
        {
            Type collection = typeof(IReadOnlyCollection<>).MakeGenericType(element);
            Assert.Equal(collection, Getters.Get(type, getter).ReturnType);
            Assert.Equal(collection, SetterMethods.ParameterTypeOf(type, setter));
        }
    }

    [Fact]
    public void ForConsumerGroups_SetsBothTheTypeAndProtocolTypeFilters()
    {
        ListGroupsOptions options = ListGroupsOptions.ForConsumerGroups();

        Assert.Equal(
            new[] { GroupType.CONSUMER, GroupType.CLASSIC }.OrderBy(type => type),
            options.Types().OrderBy(type => type));

        // The empty string is a member, not a placeholder: the protocol type of a classic group that uses none.
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes().OrderBy(protocolType => protocolType, StringComparer.Ordinal));

        Assert.Empty(options.GroupStates());
        Assert.Null(options.TimeoutMs());
    }

    [Fact]
    public void ForShareAndStreamsGroups_SetTheTypeFilterOnly()
    {
        ListGroupsOptions share = ListGroupsOptions.ForShareGroups();
        Assert.Equal(new[] { GroupType.SHARE }, share.Types());
        Assert.Empty(share.ProtocolTypes());
        Assert.Empty(share.GroupStates());
        Assert.Null(share.TimeoutMs());

        ListGroupsOptions streams = ListGroupsOptions.ForStreamsGroups();
        Assert.Equal(new[] { GroupType.STREAMS }, streams.Types());
        Assert.Empty(streams.ProtocolTypes());
        Assert.Empty(streams.GroupStates());
        Assert.Null(streams.TimeoutMs());
    }

    [Fact]
    public void Factories_MintAFreshInstancePerCall()
    {
        Assert.NotSame(ListGroupsOptions.ForConsumerGroups(), ListGroupsOptions.ForConsumerGroups());
        Assert.NotSame(ListGroupsOptions.ForShareGroups(), ListGroupsOptions.ForShareGroups());
        Assert.NotSame(ListGroupsOptions.ForStreamsGroups(), ListGroupsOptions.ForStreamsGroups());

        ListGroupsOptions first = ListGroupsOptions.ForShareGroups();
        first.TimeoutMs(5_000);
        Assert.Equal(5_000, first.TimeoutMs());
        Assert.Null(ListGroupsOptions.ForShareGroups().TimeoutMs());
    }

    [Fact]
    public void Filters_NormalizeNullAndEmptyToTheEmptySet()
    {
        ListGroupsOptions options = ListGroupsOptions.ForConsumerGroups();
        options.InGroupStates(new[] { GroupState.STABLE });

        options.InGroupStates(Array.Empty<GroupState>());
        options.WithTypes(Array.Empty<GroupType>());
        options.WithProtocolTypes(Array.Empty<string>());

        Assert.Empty(options.GroupStates());
        Assert.Empty(options.Types());
        Assert.Empty(options.ProtocolTypes());

        options.InGroupStates(new[] { GroupState.STABLE });
        options.WithTypes(new[] { GroupType.CONSUMER });
        options.WithProtocolTypes(new[] { "consumer" });

        options.InGroupStates(null!);
        options.WithTypes(null!);
        options.WithProtocolTypes(null!);

        Assert.Empty(options.GroupStates());
        Assert.Empty(options.Types());
        Assert.Empty(options.ProtocolTypes());
    }

    [Fact]
    public void Filters_StoreADeduplicatedDefensiveCopy()
    {
        List<GroupState> states = new List<GroupState> { GroupState.STABLE, GroupState.STABLE, GroupState.EMPTY };
        List<GroupType> types = new List<GroupType> { GroupType.CONSUMER, GroupType.CONSUMER };
        List<string> protocolTypes = new List<string> { "consumer", "consumer", "" };

        ListGroupsOptions options = new ListGroupsOptions()
            .InGroupStates(states)
            .WithTypes(types)
            .WithProtocolTypes(protocolTypes);

        Assert.Equal(
            new[] { GroupState.STABLE, GroupState.EMPTY }.OrderBy(state => state),
            options.GroupStates().OrderBy(state => state));
        Assert.Equal(new[] { GroupType.CONSUMER }, options.Types());
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes().OrderBy(protocolType => protocolType, StringComparer.Ordinal));

        states.Add(GroupState.DEAD);
        types.Add(GroupType.SHARE);
        protocolTypes.Add("share");

        Assert.Equal(
            new[] { GroupState.STABLE, GroupState.EMPTY }.OrderBy(state => state),
            options.GroupStates().OrderBy(state => state));
        Assert.Equal(new[] { GroupType.CONSUMER }, options.Types());
        Assert.Equal(
            new[] { "", "consumer" }.OrderBy(protocolType => protocolType, StringComparer.Ordinal),
            options.ProtocolTypes().OrderBy(protocolType => protocolType, StringComparer.Ordinal));
    }

    [Fact]
    public void Filters_StoreAnImmutableCopy()
    {
        ListGroupsOptions options = new ListGroupsOptions()
            .InGroupStates(new[] { GroupState.STABLE })
            .WithTypes(new[] { GroupType.CONSUMER })
            .WithProtocolTypes(new[] { "consumer" });

        Assert.Throws<NotSupportedException>(() => ((IList<GroupState>)options.GroupStates()).Add(GroupState.DEAD));
        Assert.Throws<NotSupportedException>(() => ((IList<GroupType>)options.Types()).Clear());
        Assert.Throws<NotSupportedException>(() => ((IList<string>)options.ProtocolTypes()).Add("share"));
    }

    [Fact]
    public void ProtocolTypes_RejectANullElement()
    {
        ListGroupsOptions options = new ListGroupsOptions();

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => options.WithProtocolTypes(new[] { "consumer", null! }));

        Assert.Contains("must not be null", error.Message, StringComparison.Ordinal);

        Assert.Empty(options.ProtocolTypes());
    }

    [Fact]
    public void TimeoutMs_IsNullableAndRoundTrips()
    {
        ListGroupsOptions options = new ListGroupsOptions().TimeoutMs(30_000);
        Assert.Equal(30_000, options.TimeoutMs());

        options.TimeoutMs(null);
        Assert.Null(options.TimeoutMs());
    }
}
