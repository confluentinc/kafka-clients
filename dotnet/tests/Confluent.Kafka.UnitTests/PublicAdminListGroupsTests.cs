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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public behaviour of M15/P5's <c>listGroups</c>, driven end to end through
/// <see cref="MockAdminClient"/> — the real submit, the real marshalling, the real bridge
/// and teardown, with no broker.
/// </summary>
/// <remarks>
/// <para>
/// <b>Groups are seeded through <c>incrementalAlterConfigs</c>.</b> The mock has no
/// "create group" call; it lists one <c>CONSUMER</c>/<c>STABLE</c> listing per group
/// <em>config</em> it holds, exactly as Java's <c>MockAdminClient.listGroups</c> does, and
/// a config is put there by altering a <see cref="ConfigResourceType.Group"/> resource. So
/// the alter below is the fixture, not the subject.
/// </para>
/// <para>
/// ⚠ <b>The filters are not exercised here, and cannot be.</b> Filtering is broker-side
/// and the mock ignores the options entirely, so a filtered call and an unfiltered one
/// return the same thing — which makes this file structurally unable to tell a forwarded
/// filter from a dropped one. That claim is owned by
/// <c>Interop/AdminP5SubmitArgumentTests</c>, at the P/Invoke seam where the three axes
/// are observable; and the two-different-lengths result walk by
/// <c>Interop/AdminP5ResultMarshalTests</c>, since the mock never emits an error. What
/// this file owns is everything those two cannot reach: that the whole path fits together.
/// </para>
/// </remarks>
public sealed class PublicAdminListGroupsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The protocol type the mock reports, Java's <c>ConsumerProtocol.PROTOCOL_TYPE</c>.</summary>
    private const string ConsumerProtocol = "consumer";

    /// <summary>
    /// The happy path: every seeded group comes back, fully populated, with no error
    /// alongside it.
    /// </summary>
    [Fact]
    public async Task ListGroups_ReportsEverySeededGroup()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lg-a", "public-lg-b", "public-lg-c");

        ListGroupsResult result = admin.ListGroups();

        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);
        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);

        Assert.Equal(
            new[] { "public-lg-a", "public-lg-b", "public-lg-c" },
            valid.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal));
        Assert.Empty(errors);

        // Every field crossed the ABI, not just the id: an unset enum would surface as null
        // and an unread string as empty, so asserting all four pins the whole reader.
        foreach (GroupListing listing in valid)
        {
            Assert.Equal(GroupType.Consumer, listing.Type);
            Assert.Equal(GroupState.Stable, listing.GroupState);
            Assert.Equal(ConsumerProtocol, listing.Protocol);
            Assert.False(listing.IsSimpleConsumerGroup);
        }
    }

    /// <summary>
    /// With no error reported, <c>All()</c> yields exactly what <c>Valid()</c> does rather
    /// than throwing — the two agree only because the error list came back empty.
    /// </summary>
    [Fact]
    public async Task ListGroups_AllAgreesWithValid_WhenNothingFailed()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lg-all-1", "public-lg-all-2");

        ListGroupsResult result = admin.ListGroups();

        IReadOnlyCollection<GroupListing> all = await TestTimeout.Run(() => result.All(), s_deadline);
        IReadOnlyCollection<GroupListing> valid = await TestTimeout.Run(() => result.Valid(), s_deadline);

        Assert.Equal(
            valid.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal),
            all.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal));
        Assert.Equal(2, all.Count);
    }

    /// <summary>
    /// A client with nothing to list yields two <b>empty</b> collections and no failure —
    /// "no groups" is a successful answer, not an error.
    /// </summary>
    [Fact]
    public async Task ListGroups_WithNoGroups_YieldsTwoEmptyCollections()
    {
        using MockAdminClient admin = new MockAdminClient();

        ListGroupsResult result = admin.ListGroups();

        Assert.Empty(await TestTimeout.Run(() => result.Valid(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => result.Errors(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => result.All(), s_deadline));
    }

    /// <summary>
    /// The call is reachable through the <see cref="IAdmin"/> surface, which is what a user
    /// holding the interface — and every test double written against it — actually calls.
    /// </summary>
    [Fact]
    public async Task ListGroups_IsReachableThroughTheInterface()
    {
        using MockAdminClient mock = new MockAdminClient();
        IAdmin admin = mock;

        await SeedGroups(mock, "public-lg-iface");

        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => admin.ListGroups().Valid(), s_deadline);

        Assert.Equal("public-lg-iface", Assert.Single(valid).GroupId);
    }

    /// <summary>
    /// Every way of asking for "no filter" — omitting the options, passing
    /// <see langword="null"/>, and passing a fresh <see cref="ListGroupsOptions"/> — is the
    /// same call, and none of them means "match nothing".
    /// </summary>
    [Fact]
    public async Task ListGroups_NoFilter_IsTheSameCallHoweverItIsSpelled()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lg-nofilter");

        foreach (ListGroupsResult result in new[]
        {
            admin.ListGroups(),
            admin.ListGroups(null),
            admin.ListGroups(new ListGroupsOptions()),
        })
        {
            Assert.Equal("public-lg-nofilter", Assert.Single(await TestTimeout.Run(() => result.Valid(), s_deadline)).GroupId);
        }
    }

    /// <summary>
    /// A filtered call completes normally and yields the mock's answer unchanged — the
    /// filters are evaluated by the broker, so the binding's job here is only to carry them
    /// without disturbing the call.
    /// </summary>
    /// <remarks>
    /// ⚠ This does <b>not</b> assert the filter was applied: the mock ignores the options,
    /// so an implementation that dropped the filters outright would pass this too. See the
    /// remarks on the class for where that claim is actually tested.
    /// </remarks>
    [Fact]
    public async Task ListGroups_AFullyFilteredCall_CompletesNormally()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lg-filtered");

        ListGroupsResult result = admin.ListGroups(new ListGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable, GroupState.Empty },
            ProtocolTypes = new[] { ConsumerProtocol },
            Types = new[] { GroupType.Consumer },
            TimeoutMs = 30_000,
        });

        Assert.Equal("public-lg-filtered", Assert.Single(await TestTimeout.Run(() => result.Valid(), s_deadline)).GroupId);
    }

    /// <summary>
    /// Java's three convenience filters are callable end to end and each yields a result
    /// rather than tripping the enum validation on its own factory output.
    /// </summary>
    [Fact]
    public async Task ListGroups_JavasFilterFactories_AreCallableEndToEnd()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lg-factory");

        foreach (ListGroupsOptions options in new[]
        {
            ListGroupsOptions.ForConsumerGroups(),
            ListGroupsOptions.ForShareGroups(),
            ListGroupsOptions.ForStreamsGroups(),
        })
        {
            Assert.Empty(await TestTimeout.Run(() => admin.ListGroups(options).Errors(), s_deadline));
        }
    }

    /// <summary>
    /// A negative timeout is a caller mistake, so it is an
    /// <see cref="ArgumentOutOfRangeException"/> from the call itself — not a faulted task,
    /// which a caller could ignore.
    /// </summary>
    [Fact]
    public void ListGroups_ANegativeTimeout_ThrowsRightAway()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(new ListGroupsOptions { TimeoutMs = -1 }));

        Assert.Equal("options", failure.ParamName);
        Assert.Contains("ListGroupsOptions.TimeoutMs", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// An enum value no member defines cannot be named for the wire — the filters cross as
    /// Java's <c>toString()</c> names — so it is rejected before the call is submitted,
    /// naming the property that carried it.
    /// </summary>
    [Fact]
    public void ListGroups_AnUndefinedEnumValue_ThrowsRightAway()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentOutOfRangeException state = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(new ListGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }));
        Assert.Equal("options", state.ParamName);
        Assert.Contains("ListGroupsOptions.GroupStates", state.Message, StringComparison.Ordinal);

        ArgumentOutOfRangeException type = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(new ListGroupsOptions { Types = new[] { (GroupType)99 } }));
        Assert.Equal("options", type.ParamName);
        Assert.Contains("ListGroupsOptions.Types", type.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// After disposal the call is refused with an <see cref="ObjectDisposedException"/>
    /// rather than reaching a freed handle.
    /// </summary>
    [Fact]
    public void ListGroups_AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.ListGroups());
    }

    /// <summary>
    /// Puts one group config per id into the mock, which is what makes the mock list them.
    /// </summary>
    private static Task SeedGroups(MockAdminClient admin, params string[] groupIds) =>
        TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                groupIds.ToDictionary(
                    groupId => new ConfigResource(ConfigResourceType.Group, groupId),
                    _ => (IReadOnlyCollection<AlterConfigOp>)new[]
                    {
                        new AlterConfigOp(
                            new ConfigEntry("consumer.session.timeout.ms", "45000"),
                            AlterConfigOpType.Set),
                    })).All(),
            s_deadline);
}
