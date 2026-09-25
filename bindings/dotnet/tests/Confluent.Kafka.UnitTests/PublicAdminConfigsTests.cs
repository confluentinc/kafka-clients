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
/// The public behaviour of M15/P3 Stage 2's two RPCs, driven end to end through
/// <see cref="MockAdminClient"/> — the real marshalling, the real bridge, the real
/// teardown, with no broker.
/// </summary>
public sealed class PublicAdminConfigsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "public-cfg-topic";

    /// <summary>
    /// <c>describeConfigs</c> reports the configuration a topic was created with, keyed by
    /// the resource.
    /// </summary>
    [Fact]
    public async Task DescribeConfigs_ReportsATopicsConfiguration()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(
            () => admin.CreateTopics(new[]
            {
                new NewTopic(Topic, 1, 1)
                {
                    Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
                },
            }).All(),
            s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        DescribeConfigsResult result = admin.DescribeConfigs(new[] { resource });

        Config config = await TestTimeout.Run(() => result.Values[resource], s_deadline);
        ConfigEntry entry = Assert.IsType<ConfigEntry>(config.Get("cleanup.policy"));
        Assert.Equal("compact", entry.Value);

        // …and All() gathers the same value under the same key, which only holds because the
        // aggregate is built with the bridge's own comparer.
        IReadOnlyDictionary<ConfigResource, Config> all = await TestTimeout.Run(result.All, s_deadline);
        Assert.Equal("compact", all[resource].Get("cleanup.policy")!.Value);
    }

    /// <summary>
    /// <c>incrementalAlterConfigs</c> applies a <see cref="AlterConfigOpType.Set"/>, and the
    /// change is visible to a subsequent describe — the round trip through the mock's own
    /// state.
    /// </summary>
    [Fact]
    public async Task IncrementalAlterConfigs_SetIsVisibleToASubsequentDescribe()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[]
                    {
                        new AlterConfigOp(new ConfigEntry("retention.ms", "1000"), AlterConfigOpType.Set),
                    },
                }).All(),
            s_deadline);

        DescribeConfigsResult described = admin.DescribeConfigs(new[] { resource });
        Config config = await TestTimeout.Run(() => described.Values[resource], s_deadline);
        Assert.Equal("1000", config.Get("retention.ms")!.Value);
    }

    /// <summary>
    /// A <see cref="AlterConfigOpType.Delete"/> — whose entry value is
    /// <see langword="null"/> — removes the entry.
    /// </summary>
    /// <remarks>
    /// ⚠ The null value is the request, not an omission; the submit path passes it to the
    /// ABI as a NULL pointer. That the mock then deletes the key is the end-to-end
    /// confirmation that no <c>?? string.Empty</c> intervened.
    /// </remarks>
    [Fact]
    public async Task IncrementalAlterConfigs_DeleteWithANullValue_RemovesTheEntry()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(
            () => admin.CreateTopics(new[]
            {
                new NewTopic(Topic, 1, 1)
                {
                    Configs = new Dictionary<string, string> { ["retention.ms"] = "1000" },
                },
            }).All(),
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
                }).All(),
            s_deadline);

        DescribeConfigsResult described = admin.DescribeConfigs(new[] { resource });
        Config config = await TestTimeout.Run(() => described.Values[resource], s_deadline);
        Assert.Null(config.Get("retention.ms"));
    }

    /// <summary>
    /// A failing resource faults only its own awaitable, with the mock's exact message.
    /// </summary>
    [Fact]
    public async Task IncrementalAlterConfigs_AnUnknownTopic_FaultsOnlyItsOwnAwaitable()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "public-cfg-absent");

        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [present] = new[] { new AlterConfigOp(new ConfigEntry("a", "1"), AlterConfigOpType.Set) },
                [missing] = new[] { new AlterConfigOp(new ConfigEntry("a", "1"), AlterConfigOpType.Set) },
            });

        await TestTimeout.Run(() => result.Values[present], s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[missing]), s_deadline);
        Assert.Equal("No such topic as public-cfg-absent", failure.Message);
    }

    /// <summary>
    /// ⚠ <b>Stage 1's test debt, paid.</b> The Rust mock creates a client-metrics resource
    /// only as a <em>side effect</em> of an <c>incrementalAlterConfigs</c> against a
    /// <see cref="ConfigResourceType.ClientMetrics"/> resource, so until this stage
    /// <c>ListClientMetricsResources</c> could only ever be exercised <b>empty</b>. Now it
    /// can be seeded, and both the deprecated listing and its non-deprecated replacement
    /// must agree.
    /// </summary>
    [Fact]
    public async Task ListClientMetricsResources_ReturnsASeededResource_AndListConfigResourcesAgrees()
    {
        using MockAdminClient admin = new MockAdminClient();

        ConfigResource subscription = new ConfigResource(ConfigResourceType.ClientMetrics, "sub-1");

        // Empty before — the state Stage 1 was limited to asserting.
#pragma warning disable CS0618 // Java deprecates this RPC; exercising it is the point.
        Assert.Empty(await TestTimeout.Run(() => admin.ListClientMetricsResources().All(), s_deadline));
#pragma warning restore CS0618

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [subscription] = new[]
                    {
                        new AlterConfigOp(
                            new ConfigEntry("interval.ms", "60000"), AlterConfigOpType.Set),
                    },
                }).All(),
            s_deadline);

        // The deprecated listing now reports it…
#pragma warning disable CS0618 // Java deprecates this RPC; exercising it is the point.
        IReadOnlyCollection<ClientMetricsResourceListing> listings =
            await TestTimeout.Run(() => admin.ListClientMetricsResources().All(), s_deadline);
        Assert.Equal("sub-1", Assert.Single(listings).Name);
#pragma warning restore CS0618

        // …and so does the replacement Java's deprecation points callers at.
        IReadOnlyCollection<ConfigResource> filtered = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.ClientMetrics }).All(), s_deadline);
        Assert.Equal(subscription, Assert.Single(filtered));

        // …and describing it returns the config the alter set.
        Config config = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { subscription }).Values[subscription], s_deadline);
        Assert.Equal("60000", config.Get("interval.ms")!.Value);
    }

    /// <summary>
    /// ⚠ <b>A resource mapped to an EMPTY operation collection completes SUCCESSFULLY</b> —
    /// Java's outcome, and the fix for M15/P3 round 3's finding 69.6.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This drives the SUCCESS path end to end through the mock, which is the whole
    /// point.</b> The test this replaces completed the operation through the
    /// <em>submit-failure</em> path, under which every awaitable faults regardless — so it
    /// passed while the behaviour was wrong. Here nothing is injected: the real ABI runs,
    /// the real callback fires, and the assertion is that the zero-op resource's awaitable
    /// <b>completes</b>.
    /// </para>
    /// <para>
    /// The mixed case is asserted alongside it so the zero-op key cannot be riding on a
    /// blanket success: the sibling resource's operation is checked for its actual effect.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task IncrementalAlterConfigs_AResourceWithNoOps_CompletesSuccessfully()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(
            () => admin.CreateTopics(
                new[] { new NewTopic(Topic, 1, 1), new NewTopic("public-cfg-sibling", 1, 1) }).All(),
            s_deadline);

        ConfigResource noOps = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource sibling = new ConfigResource(ConfigResourceType.Topic, "public-cfg-sibling");

        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [noOps] = Array.Empty<AlterConfigOp>(),
                [sibling] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            });

        // The zero-op resource completes — it does not fault.
        await TestTimeout.Run(() => result.Values[noOps], s_deadline);
        await TestTimeout.Run(result.All, s_deadline);

        // …and the sibling's operation really was applied, so the success above is not a
        // blanket one that would also cover a broken row-flattening.
        Config config = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { sibling }).Values[sibling], s_deadline);
        Assert.Equal("v", config.Get("k")!.Value);

        // The zero-op resource was genuinely left untouched.
        Config untouched = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { noOps }).Values[noOps], s_deadline);
        Assert.Empty(untouched.Entries);
    }

    /// <summary>
    /// A map consisting only of zero-op resources completes successfully — the degenerate
    /// case, where the ABI request carries <b>no rows at all</b>.
    /// </summary>
    [Fact]
    public async Task IncrementalAlterConfigs_OnlyZeroOpResources_CompletesSuccessfully()
    {
        using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [resource] = Array.Empty<AlterConfigOp>(),
            });

        await TestTimeout.Run(() => result.Values[resource], s_deadline);
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// ⚠⚠ <b>PINS A KNOWN DIVERGENCE, not desired behaviour.</b> A zero-op resource that
    /// <b>does not exist</b> completes successfully here, where Java's future would fail —
    /// because the ABI request is row-flattened, so the resource is never sent and the
    /// broker is never asked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The A/B is the evidence: the <em>same absent resource</em> with a <b>non-empty</b>
    /// operation list <b>does</b> fault, with the core's own message — so the difference is
    /// caused purely by the resource dropping out of the request, not by anything about the
    /// resource itself. The Rust core checks the resource before applying operations
    /// (<c>mock_admin_client.rs:630-636</c>), so it would fail this too if the FFI could
    /// carry a zero-operation resource.
    /// </para>
    /// <para>
    /// ⚠ <b>This test is expected to go RED when that Mode-B gap is closed</b>, and that is
    /// the point of pinning it: the divergence is recorded where a reader meets it, and
    /// closing the gap cannot pass unnoticed. See the divergence note on
    /// <c>NativeAdminClient.IncrementalAlterConfigs</c> for the three cases and their
    /// evidence.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task IncrementalAlterConfigs_ZeroOpsAgainstAnAbsentResource_SucceedsLocally_AKnownDivergence()
    {
        using MockAdminClient admin = new MockAdminClient();

        ConfigResource absent = new ConfigResource(ConfigResourceType.Topic, "public-cfg-never-created");

        // (A) zero ops — never sent, so it succeeds locally.
        AlterConfigsResult zeroOps = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [absent] = Array.Empty<AlterConfigOp>(),
            });
        await TestTimeout.Run(() => zeroOps.Values[absent], s_deadline);

        // (B) the SAME absent resource with one op — sent, so the core rejects it.
        AlterConfigsResult withOps = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [absent] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
            });
        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => withOps.Values[absent]), s_deadline);
        Assert.Equal("No such topic as public-cfg-never-created", failure.Message);
    }

    /// <summary>
    /// A negative <c>TimeoutMs</c> is rejected <b>before</b> any native call, on both RPCs.
    /// </summary>
    [Fact]
    public void NegativeTimeout_IsRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient();
        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        ArgumentOutOfRangeException describe = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeConfigs(new[] { resource }, new DescribeConfigsOptions { TimeoutMs = -1 }));
        Assert.Equal("options", describe.ParamName);
        Assert.Contains(
            "DescribeConfigsOptions.TimeoutMs must not be negative", describe.Message, StringComparison.Ordinal);

        ArgumentOutOfRangeException alter = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(),
                new AlterConfigsOptions { TimeoutMs = -2 }));
        Assert.Equal("options", alter.ParamName);
        Assert.Contains(
            "AlterConfigsOptions.TimeoutMs must not be negative", alter.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Null and null-element inputs are rejected before any pin or native call (ffi §B5).
    /// </summary>
    [Fact]
    public void NullInputs_AreRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient();

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

    /// <summary>Both RPCs reject use after close.</summary>
    [Fact]
    public void AfterDispose_BothRpcsThrowObjectDisposed()
    {
        MockAdminClient admin = new MockAdminClient();
        TestTimeout.Run(admin.Dispose, s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        Assert.Throws<ObjectDisposedException>(() => admin.DescribeConfigs(new[] { resource }));
        Assert.Throws<ObjectDisposedException>(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>()));
    }

    /// <summary>
    /// The TFM-matrix smoke leg for Stage 2: both RPCs load and round-trip on whichever
    /// framework is executing (net462 via netstandard2.0, net8.0, net10.0).
    /// </summary>
    [Fact]
    public async Task TfmSmoke_TheTwoNewRpcsWorkOnThisFramework()
    {
        await using MockAdminClient admin = new MockAdminClient();

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cfg-smoke", 1, 1) }).All(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, "cfg-smoke");

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) },
                }).All(),
            s_deadline);

        Config config = await TestTimeout.Run(
            () => admin.DescribeConfigs(new[] { resource }).Values[resource], s_deadline);
        Assert.Equal("v", config.Get("k")!.Value);
    }

    /// <summary>
    /// Both RPCs work through the <see cref="IAdmin"/> interface, not only the concrete
    /// mock.
    /// </summary>
    [Fact]
    public async Task TheTwoRpcs_AreReachableThroughIAdmin()
    {
        using IAdmin admin = new MockAdminClient();

        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);

        await TestTimeout.Run(
            () => admin.IncrementalAlterConfigs(
                new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
                {
                    [resource] = new[] { new AlterConfigOp(new ConfigEntry("x", "y"), AlterConfigOpType.Set) },
                }).All(),
            s_deadline);

        IReadOnlyDictionary<ConfigResource, Config> all =
            await TestTimeout.Run(() => admin.DescribeConfigs(new[] { resource }).All(), s_deadline);
        Assert.Equal("y", all[resource].Get("x")!.Value);
    }
}
