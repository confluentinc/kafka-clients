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

#pragma warning disable CS0618 // Java deprecates this RPC and its three types; exercising them is the point.

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public behaviour of M15/P5's <c>listConsumerGroups</c>, driven end to end through
/// <see cref="MockAdminClient"/> — the real submit, the real marshalling, the real bridge
/// and teardown, with no broker. The deprecated predecessor of <c>listGroups</c>, and the
/// twin of <see cref="PublicAdminListGroupsTests"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Groups are seeded through <c>incrementalAlterConfigs</c>.</b> The mock has no "create
/// group" call; it lists one listing per group <em>config</em> it holds, exactly as Java's
/// <c>MockAdminClient.listConsumerGroups</c> (<c>MockAdminClient.java:743</c>) does, and a
/// config is put there by altering a <see cref="ConfigResourceType.Group"/> resource. So
/// the alter below is the fixture, not the subject — the same fixture the <c>listGroups</c>
/// twin uses, because both RPCs read the same seeded map.
/// </para>
/// <para>
/// ⚠ <b>This RPC's listings come back with no state and no type, and that is Java's
/// mock, not a gap here.</b> Java builds them with the two-argument
/// <c>new ConsumerGroupListing(g, false)</c> (<c>:743</c>), which forwards
/// <c>Optional.empty()</c> for both — unlike <c>MockAdminClient.listGroups</c>, which fills
/// in <c>CONSUMER</c>/<c>STABLE</c>/<c>"consumer"</c>. So the happy path below asserts
/// <b>absence</b> on those two fields, which is its own claim worth pinning: a null name
/// pointer must decode to <see langword="null"/> — Java's <c>Optional.empty()</c> — and not
/// to <see cref="GroupState.Unknown"/> / <see cref="GroupType.Unknown"/>, which is what an
/// unrecognised <em>name</em> decodes to. The two are deliberately not collapsed.
/// </para>
/// <para>
/// ⚠ <b>The filters are not exercised here, and cannot be.</b> Filtering is broker-side and
/// the mock ignores the options entirely —
/// <c>fn list_consumer_groups(&amp;self, _options: ListConsumerGroupsOptions)</c> — so a
/// filtered call and an unfiltered one return the same thing, which makes this file
/// structurally unable to tell a forwarded filter from a dropped one. That claim is owned
/// by <c>Interop/AdminP5SubmitArgumentTests</c>, at the P/Invoke seam where the <b>two</b>
/// axes are observable; and the two-different-lengths result walk by
/// <c>Interop/AdminP5ResultMarshalTests</c>, since the mock never emits an error. What this
/// file owns is everything those two cannot reach: that the whole path fits together.
/// </para>
/// </remarks>
public sealed class PublicAdminListConsumerGroupsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The happy path: every seeded group comes back, with no error alongside it, and each
    /// field carries what Java's mock put there.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroups_ReportsEverySeededGroup()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lcg-a", "public-lcg-b", "public-lcg-c");

        ListConsumerGroupsResult result = admin.ListConsumerGroups();

        IReadOnlyCollection<ConsumerGroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);
        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);

        Assert.Equal(
            new[] { "public-lcg-a", "public-lcg-b", "public-lcg-c" },
            valid.Select(listing => listing.GroupId).OrderBy(id => id, StringComparer.Ordinal));
        Assert.Empty(errors);

        foreach (ConsumerGroupListing listing in valid)
        {
            // The one boolean the ABI carries, and it is not simply the default of an
            // unread field: false is what Java's mock passes (:743).
            Assert.False(listing.IsSimpleConsumerGroup);

            // ⚠ Absent, NOT Unknown — see the class remarks. A reader that treated a null
            // name pointer as an unrecognised name would surface Unknown here and pass a
            // test that only checked "not Stable".
            Assert.Null(listing.GroupState);
            Assert.Null(listing.Type);

            // The deprecated projection follows the field it projects, rather than
            // inventing a value for an absent state.
            Assert.Null(listing.State);
        }
    }

    /// <summary>
    /// With no error reported, <c>All()</c> yields exactly what <c>Valid()</c> does rather
    /// than throwing — the two agree only because the error list came back empty.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroups_AllAgreesWithValid_WhenNothingFailed()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lcg-all-1", "public-lcg-all-2");

        ListConsumerGroupsResult result = admin.ListConsumerGroups();

        IReadOnlyCollection<ConsumerGroupListing> all =
            await TestTimeout.Run(() => result.All(), s_deadline);
        IReadOnlyCollection<ConsumerGroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);

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
    public async Task ListConsumerGroups_WithNoGroups_YieldsTwoEmptyCollections()
    {
        using MockAdminClient admin = new MockAdminClient();

        ListConsumerGroupsResult result = admin.ListConsumerGroups();

        Assert.Empty(await TestTimeout.Run(() => result.Valid(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => result.Errors(), s_deadline));
        Assert.Empty(await TestTimeout.Run(() => result.All(), s_deadline));
    }

    /// <summary>
    /// The call is reachable through the <see cref="IAdmin"/> surface, which is what a user
    /// holding the interface — and every test double written against it — actually calls.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroups_IsReachableThroughTheInterface()
    {
        using MockAdminClient mock = new MockAdminClient();
        IAdmin admin = mock;

        await SeedGroups(mock, "public-lcg-iface");

        IReadOnlyCollection<ConsumerGroupListing> valid =
            await TestTimeout.Run(() => admin.ListConsumerGroups().Valid(), s_deadline);

        Assert.Equal("public-lcg-iface", Assert.Single(valid).GroupId);
    }

    /// <summary>
    /// Every way of asking for "no filter" — omitting the options, passing
    /// <see langword="null"/>, and passing a fresh <see cref="ListConsumerGroupsOptions"/> —
    /// is the same call, and none of them means "match nothing".
    /// </summary>
    /// <remarks>
    /// The unfiltered call returning the seeded group is the end-to-end half of the claim;
    /// that each empty axis actually crosses as a count of <c>0</c> rather than as some
    /// filter is the seam's half (<c>Interop/AdminP5SubmitArgumentTests</c>).
    /// </remarks>
    [Fact]
    public async Task ListConsumerGroups_NoFilter_IsTheSameCallHoweverItIsSpelled()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lcg-nofilter");

        foreach (ListConsumerGroupsResult result in new[]
        {
            admin.ListConsumerGroups(),
            admin.ListConsumerGroups(null),
            admin.ListConsumerGroups(new ListConsumerGroupsOptions()),
        })
        {
            Assert.Equal(
                "public-lcg-nofilter",
                Assert.Single(await TestTimeout.Run(() => result.Valid(), s_deadline)).GroupId);
        }
    }

    /// <summary>
    /// A filtered call — <b>both</b> axes at once — completes normally and yields the mock's
    /// answer unchanged: the filters are evaluated by the broker, so the binding's job here
    /// is only to carry them without disturbing the call.
    /// </summary>
    /// <remarks>
    /// ⚠ This does <b>not</b> assert the filters were applied, or even forwarded: the mock
    /// ignores the options, so an implementation that dropped them outright would pass this
    /// too. See the remarks on the class for where that claim is actually tested.
    /// </remarks>
    [Fact]
    public async Task ListConsumerGroups_AFullyFilteredCall_CompletesNormally()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lcg-filtered");

        ListConsumerGroupsResult result = admin.ListConsumerGroups(new ListConsumerGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable, GroupState.Empty },
            Types = new[] { GroupType.Consumer },
            TimeoutMs = 30_000,
        });

        Assert.Equal(
            "public-lcg-filtered",
            Assert.Single(await TestTimeout.Run(() => result.Valid(), s_deadline)).GroupId);
    }

    /// <summary>
    /// The deprecated <see cref="ListConsumerGroupsOptions.States"/> spelling of the state
    /// filter is callable end to end, and reaches the same place
    /// <see cref="ListConsumerGroupsOptions.GroupStates"/> does rather than tripping the
    /// enum validation on its own projection.
    /// </summary>
    /// <remarks>
    /// ⚠ That the two are <em>one</em> filter, and that exactly one of them reaches native,
    /// is pinned at the seam — this only establishes that the deprecated spelling survives
    /// the whole path.
    /// </remarks>
    [Fact]
    public async Task ListConsumerGroups_TheDeprecatedStatesFilter_IsCallableEndToEnd()
    {
        using MockAdminClient admin = new MockAdminClient();

        await SeedGroups(admin, "public-lcg-deprecated-states");

        ListConsumerGroupsResult result = admin.ListConsumerGroups(new ListConsumerGroupsOptions
        {
            States = new[] { ConsumerGroupState.Stable, ConsumerGroupState.Empty },
        });

        Assert.Empty(await TestTimeout.Run(() => result.Errors(), s_deadline));
        Assert.Equal(
            "public-lcg-deprecated-states",
            Assert.Single(await TestTimeout.Run(() => result.Valid(), s_deadline)).GroupId);
    }

    /// <summary>
    /// A negative timeout is a caller mistake, so it is an
    /// <see cref="ArgumentOutOfRangeException"/> from the call itself — not a faulted task,
    /// which a caller could ignore.
    /// </summary>
    /// <remarks>
    /// The blame names <c>ListConsumerGroupsOptions.TimeoutMs</c>, not its successor's
    /// property: two generations of the same RPC share the validator, and naming the wrong
    /// one would send the caller to a property the type they passed does not have.
    /// </remarks>
    [Fact]
    public void ListConsumerGroups_ANegativeTimeout_ThrowsRightAway()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListConsumerGroups(new ListConsumerGroupsOptions { TimeoutMs = -1 }));

        Assert.Equal("options", failure.ParamName);
        Assert.Contains("ListConsumerGroupsOptions.TimeoutMs", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// An enum value no member defines cannot be named for the wire — the filters cross as
    /// Java's <c>toString()</c> names — so it is rejected before the call is submitted,
    /// naming the property that carried it.
    /// </summary>
    /// <remarks>
    /// ⚠ Only <b>two</b> axes exist on this options type, so only two properties can be
    /// named. There is deliberately no protocol-type case here, unlike the
    /// <c>listGroups</c> twin.
    /// </remarks>
    [Fact]
    public void ListConsumerGroups_AnUndefinedEnumValue_ThrowsRightAway()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentOutOfRangeException state = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListConsumerGroups(
                new ListConsumerGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }));
        Assert.Equal("options", state.ParamName);
        Assert.Contains("ListConsumerGroupsOptions.GroupStates", state.Message, StringComparison.Ordinal);

        ArgumentOutOfRangeException type = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListConsumerGroups(
                new ListConsumerGroupsOptions { Types = new[] { (GroupType)99 } }));
        Assert.Equal("options", type.ParamName);
        Assert.Contains("ListConsumerGroupsOptions.Types", type.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// After disposal the call is refused with an <see cref="ObjectDisposedException"/>
    /// rather than reaching a freed handle.
    /// </summary>
    [Fact]
    public void ListConsumerGroups_AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.ListConsumerGroups());
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

#pragma warning restore CS0618
