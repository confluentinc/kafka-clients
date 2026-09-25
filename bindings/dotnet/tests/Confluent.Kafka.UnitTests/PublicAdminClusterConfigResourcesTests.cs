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
/// The public behaviour of M15/P3 Stage 1's three RPCs, driven end to end through
/// <see cref="MockAdminClient"/> — the real marshalling, the real bridge, the real
/// teardown, with no broker.
/// </summary>
public sealed class PublicAdminClusterConfigResourcesTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The mock's <c>DEFAULT_CLUSTER_ID</c> (Java's <c>MockAdminClient</c> value).</summary>
    private const string MockClusterId = "4A5xz_QZTB2CtL4wc0X0Jw";

    /// <summary>
    /// <c>describeCluster</c> reports the mock's nodes, its controller and its cluster id,
    /// and returns <b>immediately</b> — the result is handed back before the broker (here,
    /// the mock) has answered.
    /// </summary>
    [Fact]
    public async Task DescribeCluster_ReportsTheMocksNodesControllerAndClusterId()
    {
        using MockAdminClient admin = new MockAdminClient(3);

        DescribeClusterResult result = admin.DescribeCluster();

        IReadOnlyCollection<Node> nodes = await TestTimeout.Run(result.Nodes, s_deadline);
        Assert.Equal(new[] { 0, 1, 2 }, nodes.Select(node => node.Id).OrderBy(id => id));
        Assert.Equal(new[] { 1000, 1001, 1002 }, nodes.Select(node => node.Port).OrderBy(port => port));

        Node? controller = await TestTimeout.Run(result.Controller, s_deadline);
        Assert.NotNull(controller);
        Assert.Equal(0, controller!.Id);

        Assert.Equal(MockClusterId, await TestTimeout.Run(result.ClusterId, s_deadline));
    }

    /// <summary>
    /// The mock reports an <b>empty but present</b> authorized-operation set, mirroring
    /// Java's own mock, so the public accessor yields an empty collection rather than
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// The <see langword="null"/> half — the broker reporting nothing — is not reachable
    /// through the mock and is proven at the marshaller in
    /// <c>AdminP3ResultMarshalTests.AuthorizedOperations_TheGateDecidesNullVersusEmpty_NotTheCount</c>.
    /// </remarks>
    [Fact]
    public async Task DescribeCluster_AuthorizedOperations_AreEmptyNotNull_AgainstTheMock()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeClusterResult result = admin.DescribeCluster(
            new DescribeClusterOptions { IncludeAuthorizedOperations = true });

        IReadOnlyCollection<AclOperation>? operations =
            await TestTimeout.Run(result.AuthorizedOperations, s_deadline);

        Assert.NotNull(operations);
        Assert.Empty(operations!);
    }

    /// <summary>
    /// ⚠ <b>An empty or absent type filter means "every supported type", and must not be
    /// rejected</b> — that is Java's own default (<c>Admin.java:1812</c> delegates with
    /// <c>Set.of()</c>). All three spellings agree.
    /// </summary>
    [Fact]
    public async Task ListConfigResources_NullOrEmptyFilter_MeansEverySupportedType()
    {
        using MockAdminClient admin = new MockAdminClient();

        IReadOnlyCollection<ConfigResource> byDefault =
            await TestTimeout.Run(() => admin.ListConfigResources().All(), s_deadline);
        IReadOnlyCollection<ConfigResource> explicitNull =
            await TestTimeout.Run(() => admin.ListConfigResources(null).All(), s_deadline);
        IReadOnlyCollection<ConfigResource> empty = await TestTimeout.Run(
            () => admin.ListConfigResources(Array.Empty<ConfigResourceType>()).All(), s_deadline);

        Assert.NotEmpty(byDefault);
        Assert.Equal(byDefault, explicitNull);
        Assert.Equal(byDefault, empty);

        // A one-broker mock reports its broker's config and logger resources even with no
        // topics, so "every supported type" really did return more than one type.
        Assert.True(byDefault.Select(resource => resource.Type).Distinct().Count() > 1);
    }

    /// <summary>
    /// The filter is honoured: a created topic appears under
    /// <see cref="ConfigResourceType.Topic"/> and is excluded when the filter names only
    /// <see cref="ConfigResourceType.Broker"/>.
    /// </summary>
    [Fact]
    public async Task ListConfigResources_HonoursTheTypeFilter()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p3-topic", 1, 1) }).All(), s_deadline);

        IReadOnlyCollection<ConfigResource> topics = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.Topic }).All(), s_deadline);
        Assert.Equal(
            new ConfigResource(ConfigResourceType.Topic, "p3-topic"),
            Assert.Single(topics));

        IReadOnlyCollection<ConfigResource> brokers = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.Broker }).All(), s_deadline);
        Assert.DoesNotContain(brokers, resource => resource.Type == ConfigResourceType.Topic);
        Assert.Contains(brokers, resource => resource.Type == ConfigResourceType.Broker);
    }

    /// <summary>
    /// A repeated type is one entry, because Java's parameter is a <c>Set</c>.
    /// </summary>
    [Fact]
    public async Task ListConfigResources_DeduplicatesTheRequestedTypes()
    {
        using MockAdminClient admin = new MockAdminClient();

        IReadOnlyCollection<ConfigResource> once = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.Broker }).All(), s_deadline);
        IReadOnlyCollection<ConfigResource> repeated = await TestTimeout.Run(
            () => admin.ListConfigResources(
                new[] { ConfigResourceType.Broker, ConfigResourceType.Broker, ConfigResourceType.Broker }).All(),
            s_deadline);

        Assert.Equal(once, repeated);
    }

    /// <summary>
    /// <c>All()</c> returns the <b>same</b> task instance on every call, so a projection
    /// cannot observe a different cluster state than an earlier one did.
    /// </summary>
    [Fact]
    public async Task ListConfigResources_AllReturnsTheSameTaskInstance()
    {
        using MockAdminClient admin = new MockAdminClient();

        ListConfigResourcesResult result = admin.ListConfigResources();
        Assert.Same(result.All(), result.All());

        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// <c>listClientMetricsResources</c> is bound and returns an empty listing against a
    /// fresh mock.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The seeded case is Stage 2's debt.</b> The Rust mock creates a client-metrics
    /// resource only as a side effect of an <c>incrementalAlterConfigs</c> against a
    /// <c>CLIENT_METRICS</c> resource, which is not bound until Stage 2 — so Stage 2 owes
    /// the seeded-listing test that also cross-checks
    /// <see cref="IAdmin.ListConfigResources"/> filtered to
    /// <see cref="ConfigResourceType.ClientMetrics"/>.
    /// </remarks>
    [Fact]
    public async Task ListClientMetricsResources_IsEmptyOnAFreshMock()
    {
        using MockAdminClient admin = new MockAdminClient();

#pragma warning disable CS0618 // Java deprecates this RPC; exercising it is the point.
        ListClientMetricsResourcesResult result = admin.ListClientMetricsResources();
        Assert.Same(result.All(), result.All());

        IReadOnlyCollection<ClientMetricsResourceListing> listings =
            await TestTimeout.Run(result.All, s_deadline);
#pragma warning restore CS0618

        Assert.Empty(listings);

        // …and the non-deprecated replacement agrees, which is what the deprecation asks
        // callers to switch to.
        IReadOnlyCollection<ConfigResource> clientMetrics = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.ClientMetrics }).All(), s_deadline);
        Assert.Empty(clientMetrics);
    }

    /// <summary>
    /// A negative <c>TimeoutMs</c> is rejected <b>before</b> any native call, on all three
    /// RPCs — the ABI reads a negative timeout as "unset", so forwarding one would silently
    /// substitute the client default for the value asked for (ffi §B5).
    /// </summary>
    [Fact]
    public void NegativeTimeout_IsRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentOutOfRangeException cluster = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeCluster(new DescribeClusterOptions { TimeoutMs = -1 }));
        Assert.Equal("options", cluster.ParamName);
        Assert.Contains("DescribeClusterOptions.TimeoutMs must not be negative", cluster.Message, StringComparison.Ordinal);

        ArgumentOutOfRangeException configResources = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListConfigResources(null, new ListConfigResourcesOptions { TimeoutMs = -5 }));
        Assert.Equal("options", configResources.ParamName);
        Assert.Contains(
            "ListConfigResourcesOptions.TimeoutMs must not be negative",
            configResources.Message,
            StringComparison.Ordinal);

#pragma warning disable CS0618 // Java deprecates this RPC; mirrored, not avoided.
        ArgumentOutOfRangeException clientMetrics = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListClientMetricsResources(new ListClientMetricsResourcesOptions { TimeoutMs = -2 }));
#pragma warning restore CS0618
        Assert.Equal("options", clientMetrics.ParamName);
        Assert.Contains(
            "ListClientMetricsResourcesOptions.TimeoutMs must not be negative",
            clientMetrics.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Every new RPC rejects use after close with <see cref="ObjectDisposedException"/>,
    /// like the shipped ones.
    /// </summary>
    [Fact]
    public void AfterDispose_EveryNewRpcThrowsObjectDisposed()
    {
        MockAdminClient admin = new MockAdminClient();
        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => admin.DescribeCluster());
        Assert.Throws<ObjectDisposedException>(() => admin.ListConfigResources());
#pragma warning disable CS0618 // Java deprecates this RPC; mirrored, not avoided.
        Assert.Throws<ObjectDisposedException>(() => admin.ListClientMetricsResources());
#pragma warning restore CS0618
    }

    /// <summary>
    /// The TFM-matrix smoke leg for M15/P3 Stage 1: the three new RPCs load and round-trip
    /// on whichever framework is executing (net462 via netstandard2.0, net8.0, net10.0).
    /// </summary>
    /// <remarks>
    /// Uses only APIs available on the netstandard2.0 floor, so the net462 leg <b>builds</b>
    /// locally even though its <em>run</em> is Windows/CI-only.
    /// </remarks>
    [Fact]
    public async Task TfmSmoke_TheThreeNewRpcsWorkOnThisFramework()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        Assert.Equal(MockClusterId, await TestTimeout.Run(() => admin.DescribeCluster().ClusterId(), s_deadline));
        Assert.NotEmpty(await TestTimeout.Run(() => admin.ListConfigResources().All(), s_deadline));
#pragma warning disable CS0618 // Java deprecates this RPC; exercising it on every TFM is the point.
        Assert.Empty(await TestTimeout.Run(() => admin.ListClientMetricsResources().All(), s_deadline));
#pragma warning restore CS0618
    }

    /// <summary>
    /// The three RPCs work through the <see cref="IAdmin"/> interface, not only the concrete
    /// mock — the surface a caller actually programs against.
    /// </summary>
    [Fact]
    public async Task TheThreeRpcs_AreReachableThroughIAdmin()
    {
        using IAdmin admin = new MockAdminClient();

        Assert.Equal(MockClusterId, await TestTimeout.Run(() => admin.DescribeCluster().ClusterId(), s_deadline));
        Assert.NotEmpty(await TestTimeout.Run(() => admin.ListConfigResources().All(), s_deadline));
#pragma warning disable CS0618 // Java deprecates this RPC; mirrored, not avoided.
        Assert.Empty(await TestTimeout.Run(() => admin.ListClientMetricsResources().All(), s_deadline));
#pragma warning restore CS0618
    }
}
