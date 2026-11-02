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
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The <c>describeCluster</c> and <c>listConfigResources</c> futures over the core mock, and the
/// copy-out of a native <c>ConfigResource_t</c>.
/// </summary>
public sealed class AdminP3ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string MockClusterId = "I4ZmrWqfT2e-upky_4fdPA";

    [Fact]
    public async Task DescribeCluster_EachFutureDeliversItsValue_AfterTheResultIsDestroyed()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(2);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        (KafkaFutureImpl<IReadOnlyCollection<Node>> nodesFuture,
            KafkaFutureImpl<Node?> controllerFuture,
            KafkaFutureImpl<string> clusterIdFuture,
            KafkaFutureImpl<IReadOnlyCollection<AclOperation>?> operationsFuture,
            SafeAdminResultHandle result) = admin.DescribeCluster(options: null);
        result.Dispose();

        Assert.Equal(MockClusterId, await TestTimeout.Run(() => clusterIdFuture.GetAsync(), s_deadline));

        List<Node> nodes = (await TestTimeout.Run(() => nodesFuture.GetAsync(), s_deadline))
            .OrderBy(node => node.Id)
            .ToList();
        Assert.Equal(new[] { 0, 1 }, nodes.Select(node => node.Id));
        Assert.Equal(new[] { "localhost", "localhost" }, nodes.Select(node => node.Host));
        Assert.Equal(new[] { 1000, 1001 }, nodes.Select(node => node.Port));

        Node? controller = await TestTimeout.Run(() => controllerFuture.GetAsync(), s_deadline);
        Assert.NotNull(controller);
        Assert.Equal(0, controller!.Id);
        Assert.Equal("localhost", controller.Host);

        // The core mock reports an empty set whatever the options say, as Java's mock does.
        IReadOnlyCollection<AclOperation>? operations =
            await TestTimeout.Run(() => operationsFuture.GetAsync(), s_deadline);
        Assert.NotNull(operations);
        Assert.Empty(operations!);
    }

    [Fact]
    public void DescribeCluster_TheTwoNullableValues_CopyOutANullAsNull()
    {
        Assert.Null(DescribeClusterMarshal.CopyOutController(IntPtr.Zero));
        Assert.Null(DescribeClusterMarshal.CopyOutAuthorizedOperations(IntPtr.Zero));
    }

    [Fact]
    public async Task DescribeCluster_EachAccessorReturnsItsOwnFuture_AndANullIsASuccess()
    {
        KafkaFutureImpl<IReadOnlyCollection<Node>> nodes =
            KafkaFutureImpl.CompletedFuture<IReadOnlyCollection<Node>>(new[] { new Node(7, "h", 9, null) });
        KafkaFutureImpl<Node?> controller = KafkaFutureImpl.CompletedFuture<Node?>(null);
        KafkaFutureImpl<string> clusterId = KafkaFutureImpl.CompletedFuture("cid");
        KafkaFutureImpl<IReadOnlyCollection<AclOperation>?> operations =
            KafkaFutureImpl.CompletedFuture<IReadOnlyCollection<AclOperation>?>(null);

        AsyncDescribeClusterResult asyncResult = new AsyncDescribeClusterResult(
            new AsyncKafkaFuture<IReadOnlyCollection<Node>>(nodes),
            new AsyncKafkaFuture<Node?>(controller),
            new AsyncKafkaFuture<string>(clusterId),
            new AsyncKafkaFuture<IReadOnlyCollection<AclOperation>?>(operations));
        DescribeClusterResult syncResult = new DescribeClusterResult(
            new KafkaFuture<IReadOnlyCollection<Node>>(nodes),
            new KafkaFuture<Node?>(controller),
            new KafkaFuture<string>(clusterId),
            new KafkaFuture<IReadOnlyCollection<AclOperation>?>(operations));

        Assert.Equal(asyncResult.Nodes(), asyncResult.Nodes());
        Assert.Equal(asyncResult.Controller(), asyncResult.Controller());
        Assert.Equal(asyncResult.ClusterId(), asyncResult.ClusterId());
        Assert.Equal(asyncResult.AuthorizedOperations(), asyncResult.AuthorizedOperations());

        Assert.Equal(7, Assert.Single(await TestTimeout.Run(() => asyncResult.Nodes().Get(), s_deadline)).Id);
        Assert.Null(await TestTimeout.Run(() => asyncResult.Controller().Get(), s_deadline));
        Assert.Equal("cid", await TestTimeout.Run(() => asyncResult.ClusterId().Get(), s_deadline));
        Assert.Null(await TestTimeout.Run(() => asyncResult.AuthorizedOperations().Get(), s_deadline));

        Assert.Equal(7, Assert.Single(syncResult.Nodes().Get()).Id);
        Assert.Null(syncResult.Controller().Get());
        Assert.Equal("cid", syncResult.ClusterId().Get());
        Assert.Null(syncResult.AuthorizedOperations().Get());
    }

    [Fact]
    public async Task DescribeCluster_AFailedCall_FailsEachFuture()
    {
        IAsyncAdmin real = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });
        try
        {
            AsyncDescribeClusterResult result = real.DescribeCluster(new DescribeClusterOptions { TimeoutMs = 0 });

            Assert.Equal(7, (await Failure(() => result.Nodes().Get())).Code);
            Assert.Equal(7, (await Failure(() => result.Controller().Get())).Code);
            Assert.Equal(7, (await Failure(() => result.ClusterId().Get())).Code);
            Assert.Equal(7, (await Failure(() => result.AuthorizedOperations().Get())).Code);
        }
        finally
        {
            TestTimeout.Dispose(real);
        }
    }

    [Fact]
    public async Task ListConfigResources_IsCopiedOut_AndOutlivesTheResult()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        (KafkaFutureImpl<IReadOnlyCollection<ConfigResource>> All, SafeAdminResultHandle Result) native =
            admin.ListConfigResources(null, null);
        AsyncListConfigResourcesResult result = AsyncListConfigResourcesResult.OfClient(native);

        IReadOnlyCollection<ConfigResource> resources = await TestTimeout.Run(() => result.All().Get(), s_deadline);
        native.Result.Dispose();
        GC.Collect();
        GC.WaitForPendingFinalizers();

        Assert.Contains(resources, resource => resource.Type == ConfigResourceType.Broker && resource.Name == "0");
        Assert.Contains(
            resources, resource => resource.Type == ConfigResourceType.BrokerLogger && resource.Name == "0");
        Assert.Equal(2, resources.Count(resource => resource.Name == "0"));
        Assert.Equal(resources, await TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public void ListConfigResources_KeepsTheCoresOrder()
    {
        ConfigResource[] unsorted =
        {
            new ConfigResource(ConfigResourceType.Group, "g"),
            new ConfigResource(ConfigResourceType.Topic, "b"),
            new ConfigResource(ConfigResourceType.Topic, "a"),
            new ConfigResource(ConfigResourceType.Unknown, "u"),
        };
        Assert.Equal(unsorted, CopyOutThroughANativeList(unsorted));
    }

    [Fact]
    public async Task ListConfigResources_TheTypeFilterReachesTheCore()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        IReadOnlyCollection<ConfigResource> unfiltered = await List(admin, null);
        IReadOnlyCollection<ConfigResource> brokersOnly = await List(admin, new[] { ConfigResourceType.Broker });
        IReadOnlyCollection<ConfigResource> groups = await List(admin, new[] { ConfigResourceType.Group });

        Assert.Contains(unfiltered, resource => resource.Type == ConfigResourceType.BrokerLogger);
        Assert.DoesNotContain(brokersOnly, resource => resource.Type == ConfigResourceType.BrokerLogger);
        Assert.Contains(brokersOnly, resource => resource.Type == ConfigResourceType.Broker);
        Assert.Empty(groups);
    }

    [Fact]
    public void ConfigResourceTypeFromId_MapsJavasIds_AndAnyOtherIdToUnknown()
    {
        Assert.Equal(ConfigResourceType.Unknown, ConfigResourceMarshal.TypeFromId(0));
        Assert.Equal(ConfigResourceType.Topic, ConfigResourceMarshal.TypeFromId(2));
        Assert.Equal(ConfigResourceType.Broker, ConfigResourceMarshal.TypeFromId(4));
        Assert.Equal(ConfigResourceType.BrokerLogger, ConfigResourceMarshal.TypeFromId(8));
        Assert.Equal(ConfigResourceType.ClientMetrics, ConfigResourceMarshal.TypeFromId(16));
        Assert.Equal(ConfigResourceType.Group, ConfigResourceMarshal.TypeFromId(32));

        foreach (int other in new[] { 64, 1, int.MaxValue, -1, int.MinValue })
        {
            Assert.Equal(ConfigResourceType.Unknown, ConfigResourceMarshal.TypeFromId(other));
        }
    }

    [Theory]
    [InlineData(ConfigResourceType.Unknown)]
    [InlineData(ConfigResourceType.Topic)]
    [InlineData(ConfigResourceType.Broker)]
    [InlineData(ConfigResourceType.BrokerLogger)]
    [InlineData(ConfigResourceType.ClientMetrics)]
    [InlineData(ConfigResourceType.Group)]
    public void ConfigResource_RoundTripsThroughTheCore(ConfigResourceType type)
    {
        ConfigResource resource = new ConfigResource(type, "résumé-中");

        Assert.Equal(new[] { resource }, CopyOutThroughANativeList(new[] { resource }));
    }

    private static async Task<IReadOnlyCollection<ConfigResource>> List(
        NativeAdminClient admin, IReadOnlyCollection<ConfigResourceType>? types) =>
        await TestTimeout.Run(
            () => AsyncListConfigResourcesResult.OfClient(admin.ListConfigResources(types, null)).All().Get(),
            s_deadline);

    private static IReadOnlyCollection<ConfigResource> CopyOutThroughANativeList(IReadOnlyList<ConfigResource> resources)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ListNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr[] natives = new IntPtr[resources.Count];
        try
        {
            for (int i = 0; i < natives.Length; i++)
            {
                natives[i] = ConfigResourceMarshal.Build(resources[i]);
                NativeMethods.ListAdd(list, natives[i], ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return NativeAdminClient.CopyOutConfigResources(list);
        }
        finally
        {
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr native in natives)
            {
                ConfigResourceMarshal.Destroy(native);
            }
        }
    }

    private static Task<KafkaException> Failure(Func<Task> call) =>
        TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(call), s_deadline);
}
