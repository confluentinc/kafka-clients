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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>ListGroups</c> end to end against the mocks. Groups are seeded through <c>IncrementalAlterConfigs</c> on group
/// resources, as Java's mock lists one listing per group config; the mock ignores the filters, so these tests check
/// that every filter spelling completes.
/// </summary>
public sealed class PublicAdminListGroupsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string ConsumerProtocol = "consumer";

    [Fact]
    public async Task ListGroups_ReportsEverySeededGroup()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await SeedGroups(admin, "public-lg-a", "public-lg-b", "public-lg-c");

        AsyncListGroupsResult result = admin.ListGroups();

        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => result.Valid().Get(), s_deadline);
        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors().Get(), s_deadline);

        Assert.Equal(
            new[] { "public-lg-a", "public-lg-b", "public-lg-c" },
            valid.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal));
        Assert.Empty(errors);

        foreach (GroupListing listing in valid)
        {
            Assert.Equal(GroupType.Consumer, listing.Type);
            Assert.Equal(GroupState.Stable, listing.GroupState);
            Assert.Equal(ConsumerProtocol, listing.Protocol);
            Assert.False(listing.IsSimpleConsumerGroup);
        }
    }

    [Fact]
    public async Task ListGroups_AllAgreesWithValid_WhenNothingFailed()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await SeedGroups(admin, "public-lg-all-1", "public-lg-all-2");

        AsyncListGroupsResult result = admin.ListGroups();

        IReadOnlyCollection<GroupListing> all = await TestTimeout.Run(() => result.All().Get(), s_deadline);
        IReadOnlyCollection<GroupListing> valid = await TestTimeout.Run(() => result.Valid().Get(), s_deadline);

        Assert.Equal(
            valid.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal),
            all.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal));
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task ListGroups_WithNoGroups_YieldsTwoEmptyCollections()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncListGroupsResult result = admin.ListGroups();

        Assert.Empty(await TestTimeout.Run(() => result.Valid().Get(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => result.Errors().Get(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task ListGroups_IsReachableThroughTheInterface()
    {
        AsyncMockAdminClient mock = AsyncMockAdminClient.Create().Build();
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IAsyncAdmin admin = mock;

        await SeedGroups(mock, "public-lg-iface");

        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => admin.ListGroups().Valid().Get(), s_deadline);

        Assert.Equal("public-lg-iface", Assert.Single(valid).GroupId);
    }

    [Fact]
    public async Task ListGroups_NoFilter_IsTheSameCallHoweverItIsSpelled()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await SeedGroups(admin, "public-lg-nofilter");

        foreach (AsyncListGroupsResult result in new[]
        {
            admin.ListGroups(),
            admin.ListGroups(null),
            admin.ListGroups(new ListGroupsOptions()),
        })
        {
            Assert.Equal("public-lg-nofilter", Assert.Single(await TestTimeout.Run(() => result.Valid().Get(), s_deadline)).GroupId);
        }
    }

    [Fact]
    public async Task ListGroups_AFullyFilteredCall_CompletesNormally()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await SeedGroups(admin, "public-lg-filtered");

        AsyncListGroupsResult result = admin.ListGroups(new ListGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable, GroupState.Empty },
            ProtocolTypes = new[] { ConsumerProtocol },
            Types = new[] { GroupType.Consumer },
            TimeoutMs = 30_000,
        });

        Assert.Equal("public-lg-filtered", Assert.Single(await TestTimeout.Run(() => result.Valid().Get(), s_deadline)).GroupId);
    }

    [Fact]
    public async Task ListGroups_JavasFilterFactories_AreCallableEndToEnd()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await SeedGroups(admin, "public-lg-factory");

        foreach (ListGroupsOptions options in new[]
        {
            ListGroupsOptions.ForConsumerGroups(),
            ListGroupsOptions.ForShareGroups(),
            ListGroupsOptions.ForStreamsGroups(),
        })
        {
            Assert.Empty(await TestTimeout.Run(() => admin.ListGroups(options).Errors().Get(), s_deadline));
        }
    }

    [Fact]
    public void ListGroups_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(() => admin.ListGroups(new ListGroupsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void ListGroups_AnUndefinedEnumValue_ThrowsRightAway()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentOutOfRangeException state = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(new ListGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }));
        Assert.Equal("options", state.ParamName);
        Assert.Contains("ListGroupsOptions.GroupStates", state.Message, StringComparison.Ordinal);

        ArgumentOutOfRangeException type = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(new ListGroupsOptions { Types = new[] { (GroupType)99 } }));
        Assert.Equal("options", type.ParamName);
        Assert.Contains("ListGroupsOptions.Types", type.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ListGroups_AfterDispose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.ListGroups());
    }

    private static Task SeedGroups(AsyncMockAdminClient admin, params string[] groupIds) =>
        TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                groupIds.ToDictionary(
                    groupId => new ConfigResource(ConfigResourceType.Group, groupId),
                    _ => (IReadOnlyCollection<AlterConfigOp>)new[]
                    {
                        new AlterConfigOp(
                            new ConfigEntry("consumer.session.timeout.ms", "45000"),
                            AlterConfigOpType.Set),
                    })).All().Get(),
            s_deadline);
}
