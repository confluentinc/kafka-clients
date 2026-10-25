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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DescribeCluster</c> and <c>ListConfigResources</c> end to end against the mocks, with no broker.
/// </summary>
public sealed class PublicAdminClusterConfigResourcesTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string ListConfigResourcesThreadExited = "The AdminClient thread has exited. Call: listConfigResources";

    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    private const string MockClusterId = "I4ZmrWqfT2e-upky_4fdPA";

    [Fact]
    public async Task DescribeCluster_ReportsTheMocksNodesControllerAndClusterId()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClusterResult result = admin.DescribeCluster();

        IReadOnlyCollection<Node> nodes = await TestTimeout.Run(() => result.Nodes().Get(), s_deadline);
        Assert.Equal(new[] { 0, 1, 2 }, nodes.Select(node => node.Id).OrderBy(id => id));
        Assert.Equal(new[] { 1000, 1001, 1002 }, nodes.Select(node => node.Port).OrderBy(port => port));

        Node? controller = await TestTimeout.Run(() => result.Controller().Get(), s_deadline);
        Assert.NotNull(controller);
        Assert.Equal(0, controller!.Id);

        Assert.Equal(MockClusterId, await TestTimeout.Run(() => result.ClusterId().Get(), s_deadline));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DescribeCluster_TheMocksNodesAreUnfenced_AndRenderIt(bool includeFencedBrokers)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClusterResult result = admin.DescribeCluster(
            new DescribeClusterOptions { IncludeFencedBrokers = includeFencedBrokers });

        IReadOnlyCollection<Node> nodes = await TestTimeout.Run(() => result.Nodes().Get(), s_deadline);
        Assert.Equal(3, nodes.Count);
        Assert.All(nodes, node => Assert.False(node.IsFenced));
        Assert.All(nodes, node => Assert.EndsWith(" isFenced: false)", node.ToString(), StringComparison.Ordinal));

        Node? controller = await TestTimeout.Run(() => result.Controller().Get(), s_deadline);
        Assert.NotNull(controller);
        Assert.False(controller!.IsFenced);
    }

    [Fact]
    public async Task DescribeCluster_AuthorizedOperations_AreEmptyNotNull_AgainstTheMock()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClusterResult result = admin.DescribeCluster(
            new DescribeClusterOptions { IncludeAuthorizedOperations = true });

        IReadOnlyCollection<AclOperation>? operations =
            await TestTimeout.Run(() => result.AuthorizedOperations().Get(), s_deadline);

        Assert.NotNull(operations);
        Assert.Empty(operations!);
    }

    [Fact]
    public async Task ListConfigResources_NullOrEmptyFilter_MeansEverySupportedType()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        IReadOnlyCollection<ConfigResource> byDefault =
            await TestTimeout.Run(() => admin.ListConfigResources().All().Get(), s_deadline);
        IReadOnlyCollection<ConfigResource> explicitNull =
            await TestTimeout.Run(() => admin.ListConfigResources(null).All().Get(), s_deadline);
        IReadOnlyCollection<ConfigResource> empty = await TestTimeout.Run(
            () => admin.ListConfigResources(Array.Empty<ConfigResourceType>()).All().Get(), s_deadline);

        Assert.NotEmpty(byDefault);
        Assert.Equal(Sorted(byDefault), Sorted(explicitNull));
        Assert.Equal(Sorted(byDefault), Sorted(empty));

        // A one-broker mock reports its broker's config and logger resources even with no topics.
        Assert.True(byDefault.Select(resource => resource.Type).Distinct().Count() > 1);
    }

    [Fact]
    public async Task ListConfigResources_HonoursTheTypeFilter()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p3-topic", 1, 1) }).All().Get(), s_deadline);

        IReadOnlyCollection<ConfigResource> topics = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.Topic }).All().Get(), s_deadline);
        Assert.Equal(
            new ConfigResource(ConfigResourceType.Topic, "p3-topic"),
            Assert.Single(topics));

        IReadOnlyCollection<ConfigResource> brokers = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.Broker }).All().Get(), s_deadline);
        Assert.DoesNotContain(brokers, resource => resource.Type == ConfigResourceType.Topic);
        Assert.Contains(brokers, resource => resource.Type == ConfigResourceType.Broker);
    }

    [Fact]
    public async Task ListConfigResources_DeduplicatesTheRequestedTypes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        IReadOnlyCollection<ConfigResource> once = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.Broker }).All().Get(), s_deadline);
        IReadOnlyCollection<ConfigResource> repeated = await TestTimeout.Run(
            () => admin.ListConfigResources(
                new[] { ConfigResourceType.Broker, ConfigResourceType.Broker, ConfigResourceType.Broker }).All().Get(),
            s_deadline);

        Assert.Equal(once, repeated);
    }

    [Fact]
    public void ListConfigResources_CancellingOneAll_LeavesTheStoredFutureAndTheOtherAllsPending()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
        try
        {
            ListConfigResourcesResult result = admin.ListConfigResources();
            KafkaFuture<IReadOnlyCollection<ConfigResource>> before = result.All();
            KafkaFuture<IReadOnlyCollection<ConfigResource>> cancelled = result.All();

            Assert.True(cancelled.Cancel(true));

            Assert.True(cancelled.IsCancelled);
            KafkaFuture<IReadOnlyCollection<ConfigResource>> stored =
                StoredFuture<KafkaFuture<IReadOnlyCollection<ConfigResource>>>(result);
            KafkaFuture<IReadOnlyCollection<ConfigResource>> after = result.All();
            Assert.NotEqual(cancelled, after);
            Assert.False(stored.IsCancelled);
            Assert.False(stored.IsDone);
            Assert.False(before.IsDone);
            Assert.False(after.IsDone);

            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

            AssertListConfigResourcesThreadExited(BlockingCall.RunGet(() => stored.Get()));
            AssertListConfigResourcesThreadExited(BlockingCall.RunGet(() => before.Get()));
            AssertListConfigResourcesThreadExited(BlockingCall.RunGet(() => after.Get()));
            Assert.IsType<OperationCanceledException>(BlockingCall.RunGet(() => cancelled.Get()));
        }
        finally
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public async Task ListConfigResources_CancellingOneAsyncAll_LeavesTheStoredFutureAndTheOtherAllsPending()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);
        try
        {
            AsyncListConfigResourcesResult result = admin.ListConfigResources();
            AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> before = result.All();
            AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> cancelled = result.All();

            Assert.True(cancelled.Cancel(true));

            Assert.True(cancelled.IsCancelled);
            AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> stored =
                StoredFuture<AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>>>(result);
            AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> after = result.All();
            Assert.NotEqual(cancelled, after);
            Assert.False(stored.IsCancelled);
            Assert.False(stored.IsDone);
            Assert.False(before.IsDone);
            Assert.False(after.IsDone);

            await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), BlockingCall.Deadline);

            AssertListConfigResourcesThreadExited(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => stored.Get(), BlockingCall.Deadline)));
            AssertListConfigResourcesThreadExited(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => before.Get(), BlockingCall.Deadline)));
            AssertListConfigResourcesThreadExited(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => after.Get(), BlockingCall.Deadline)));
            Assert.IsAssignableFrom<OperationCanceledException>(
                await Record.ExceptionAsync(() => TestTimeout.Run(() => cancelled.Get(), BlockingCall.Deadline)));
        }
        finally
        {
            await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), BlockingCall.Deadline);
            await TestTimeout.DisposeAsync(admin);
        }
    }

    [Fact]
    public void NegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.DescribeCluster(new DescribeClusterOptions { TimeoutMs = -1 })));
        Assert.Null(Record.Exception(
            () => admin.ListConfigResources(null, new ListConfigResourcesOptions { TimeoutMs = -5 })));
    }

    [Fact]
    public void AfterDispose_EveryNewRpcThrowsObjectDisposed()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => admin.DescribeCluster());
        Assert.Throws<ObjectDisposedException>(() => admin.ListConfigResources());
    }

    [Fact]
    public async Task TfmSmoke_TheNewRpcsWorkOnThisFramework()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Assert.Equal(MockClusterId, await TestTimeout.Run(() => admin.DescribeCluster().ClusterId().Get(), s_deadline));
        Assert.NotEmpty(await TestTimeout.Run(() => admin.ListConfigResources().All().Get(), s_deadline));
    }

    [Fact]
    public async Task TheRpcs_AreReachableThroughIAdmin()
    {
        IAsyncAdmin admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(MockClusterId, await TestTimeout.Run(() => admin.DescribeCluster().ClusterId().Get(), s_deadline));
        Assert.NotEmpty(await TestTimeout.Run(() => admin.ListConfigResources().All().Get(), s_deadline));
    }

    private static IEnumerable<ConfigResource> Sorted(IEnumerable<ConfigResource> resources) =>
        resources.OrderBy(resource => (int)resource.Type).ThenBy(resource => resource.Name, StringComparer.Ordinal);

    private static void AssertListConfigResourcesThreadExited(object? thrown) =>
        BlockingCall.AssertThrew(thrown, 7, ListConfigResourcesThreadExited);

    private static T StoredFuture<T>(object result) =>
        (T)result.GetType().GetField("_future", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(result)!;
}
