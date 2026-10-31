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
/// <c>DescribeConfigs</c> and <c>IncrementalAlterConfigs</c> end to end against the mocks: an alter is visible to a
/// later describe, a resource with no operations completes, and each resource fails on its own.
/// </summary>
public sealed class PublicAdminConfigsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "public-cfg-topic";

    [Fact]
    public async Task DescribeConfigs_ReportsATopicsConfiguration()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[]
            {
                new NewTopic(Topic, 1, 1)
                {
                    Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
                },
            }).All().Get(),
            s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        AsyncDescribeConfigsResult result = admin.DescribeConfigs(new[] { resource });

        Config config = await TestTimeout.Run(() => result.Values[resource].Get(), s_deadline);
        ConfigEntry entry = Assert.IsType<ConfigEntry>(config.Get("cleanup.policy"));
        Assert.Equal("compact", entry.Value);

        IReadOnlyDictionary<ConfigResource, Config> all = await TestTimeout.Run(() => result.All().Get(), s_deadline);
        Assert.Equal("compact", all[resource].Get("cleanup.policy")!.Value);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_SetIsVisibleToASubsequentDescribe()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[]
                    {
                        new AlterConfigOp(new ConfigEntry("retention.ms", "1000"), AlterConfigOpType.Set),
                    },
                }).All().Get(),
            s_deadline);

        AsyncDescribeConfigsResult described = admin.DescribeConfigs(new[] { resource });
        Config config = await TestTimeout.Run(() => described.Values[resource].Get(), s_deadline);
        Assert.Equal("1000", config.Get("retention.ms")!.Value);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_DeleteWithANullValue_RemovesTheEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[]
            {
                new NewTopic(Topic, 1, 1)
                {
                    Configs = new Dictionary<string, string> { ["retention.ms"] = "1000" },
                },
            }).All().Get(),
            s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[]
                    {
                        new AlterConfigOp(new ConfigEntry("retention.ms", null), AlterConfigOpType.Delete),
                    },
                }).All().Get(),
            s_deadline);

        AsyncDescribeConfigsResult described = admin.DescribeConfigs(new[] { resource });
        Config config = await TestTimeout.Run(() => described.Values[resource].Get(), s_deadline);
        Assert.Null(config.Get("retention.ms"));
    }

    [Fact]
    public async Task IncrementalAlterConfigs_AnUnknownTopic_FaultsOnlyItsOwnAwaitable()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "public-cfg-absent");

        AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [present] = new[] { new AlterConfigOp(new ConfigEntry("a", "1"), AlterConfigOpType.Set) },
                [missing] = new[] { new AlterConfigOp(new ConfigEntry("a", "1"), AlterConfigOpType.Set) },
            });

        await TestTimeout.Run(() => result.Values[present].Get(), s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[missing].Get()), s_deadline);
        Assert.Equal("No such topic as public-cfg-absent", failure.Message);
    }

    [Fact]
    public async Task ListConfigResources_ReportsASeededClientMetricsResource()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ConfigResource subscription = new ConfigResource(ConfigResourceType.ClientMetrics, "sub-1");

        Assert.Empty(await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.ClientMetrics }).All().Get(), s_deadline));

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [subscription] = new[]
                    {
                        new AlterConfigOp(
                            new ConfigEntry("interval.ms", "60000"), AlterConfigOpType.Set),
                    },
                }).All().Get(),
            s_deadline);

        IReadOnlyCollection<ConfigResource> filtered = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.ClientMetrics }).All().Get(), s_deadline);
        Assert.Equal(subscription, Assert.Single(filtered));

        Config config = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { subscription }).Values[subscription].Get(), s_deadline);
        Assert.Equal("60000", config.Get("interval.ms")!.Value);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_AResourceWithNoOps_CompletesSuccessfully()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(
                new[] { new NewTopic(Topic, 1, 1), new NewTopic("public-cfg-sibling", 1, 1) }).All().Get(),
            s_deadline);

        ConfigResource noOps = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource sibling = new ConfigResource(ConfigResourceType.Topic, "public-cfg-sibling");

        AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [noOps] = Array.Empty<AlterConfigOp>(),
                [sibling] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            });

        await TestTimeout.Run(() => result.Values[noOps].Get(), s_deadline);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        // The sibling's operation was applied, so the success above is not a blanket one.
        Config config = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { sibling }).Values[sibling].Get(), s_deadline);
        Assert.Equal("v", config.Get("k")!.Value);

        Config untouched = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { noOps }).Values[noOps].Get(), s_deadline);
        Assert.Empty(untouched.Entries);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_OnlyZeroOpResources_CompletesSuccessfully()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [resource] = Array.Empty<AlterConfigOp>(),
            });

        await TestTimeout.Run(() => result.Values[resource].Get(), s_deadline);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_ZeroOpsAgainstAnAbsentResource_FaultsExactlyLikeANonEmptyOne()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ConfigResource absent = new ConfigResource(ConfigResourceType.Topic, "public-cfg-never-created");

        // Zero operations are sent as an empty operation list, so the core answers and rejects them.
        AsyncAlterConfigsResult zeroOps = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [absent] = Array.Empty<AlterConfigOp>(),
            });
        KafkaException zeroOpsFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => zeroOps.Values[absent].Get()), s_deadline);
        Assert.Equal("No such topic as public-cfg-never-created", zeroOpsFailure.Message);

        // The same absent resource with one operation is sent, so the core rejects it.
        AsyncAlterConfigsResult withOps = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [absent] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            });
        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => withOps.Values[absent].Get()), s_deadline);
        Assert.Equal("No such topic as public-cfg-never-created", failure.Message);
    }

    [Fact]
    public async Task DescribeConfigs_OfAnUndefinedType_IsAnsweredAsUnknown()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ConfigResource undefined = new ConfigResource((ConfigResourceType)64, "x");
        AsyncDescribeConfigsResult result = admin.DescribeConfigs(new[] { undefined });

        ConfigResource key = Assert.Single(result.Values.Keys);
        Assert.Equal(ConfigResourceType.Unknown, key.Type);
        Assert.True(result.Values.ContainsKey(new ConfigResource(ConfigResourceType.Unknown, "x")));

        NotSupportedException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.Values[undefined].Get()), s_deadline);
        Assert.Equal("Not implemented yet", failure.Message);
    }

    [Fact]
    public void NegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        Assert.Null(Record.Exception(
            () => admin.DescribeConfigs(new[] { resource }, new DescribeConfigsOptions { TimeoutMs = -1 })));
        Assert.Null(Record.Exception(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(),
                new AlterConfigsOptions { TimeoutMs = -2 })));
    }

    [Fact]
    public void NullInputs_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.DescribeConfigs(null!));
        Assert.Throws<ArgumentNullException>(() => admin.IncrementalAlterConfigs(null!));

        ArgumentException nullResource = Assert.Throws<ArgumentException>(
            () => admin.DescribeConfigs(new ConfigResource[] { null! }));
        Assert.Equal("resources", nullResource.ParamName);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        ArgumentException nullOps = Assert.Throws<ArgumentException>(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> { [resource] = null! }));
        Assert.Equal("configs", nullOps.ParamName);

        ArgumentException nullOp = Assert.Throws<ArgumentException>(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new AlterConfigOp[] { null! },
                }));
        Assert.Equal("configs", nullOp.ParamName);

        // AlterConfigOp itself rejects a null entry, so the submit path never sees one.
        Assert.Throws<ArgumentNullException>(() => new AlterConfigOp(null!, AlterConfigOpType.Set));
    }

    [Fact]
    public void AfterDispose_BothRpcsThrowObjectDisposed()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Run(admin.Dispose, s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        Assert.Throws<ObjectDisposedException>(() => admin.DescribeConfigs(new[] { resource }));
        Assert.Throws<ObjectDisposedException>(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>()));
    }

    [Fact]
    public async Task TfmSmoke_TheTwoNewRpcsWorkOnThisFramework()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cfg-smoke", 1, 1) }).All().Get(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, "cfg-smoke");

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
                }).All().Get(),
            s_deadline);

        Config config = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { resource }).Values[resource].Get(), s_deadline);
        Assert.Equal("v", config.Get("k")!.Value);
    }

    [Fact]
    public async Task TheTwoRpcs_AreReachableThroughIAdmin()
    {
        IAsyncAdmin admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[] { new AlterConfigOp(new ConfigEntry("x", "y"), AlterConfigOpType.Set) },
                }).All().Get(),
            s_deadline);

        IReadOnlyDictionary<ConfigResource, Config> all =
            await TestTimeout.Run(() => admin.DescribeConfigs(new[] { resource }).All().Get(), s_deadline);
        Assert.Equal("y", all[resource].Get("x")!.Value);
    }
}
