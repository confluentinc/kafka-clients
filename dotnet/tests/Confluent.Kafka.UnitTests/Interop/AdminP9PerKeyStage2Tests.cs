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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P9 CP3 — the nine value-carrying RPCs (shape <b>4a</b>) driven end to end over the
/// <em>real</em> per-key ABI, where each key resolves from its own callback, fired as that
/// key's own future resolves, with no result root anywhere.
/// </summary>
/// <remarks>
/// <para>
/// Three key shapes cross here, and only the first is a plain <c>const char*</c>: the
/// composite <c>(type id, name)</c> scalar pair of <c>describeConfigs</c> and the bare
/// <c>int32_t</c> broker id of <c>describeLogDirs</c> are not pointers at all, so each
/// needs its own proof that the key survives the trip.
/// </para>
/// <para>
/// ⚠ <b>Residual coverage gap.</b> <c>describeTransactions</c> and <c>fenceProducers</c>
/// read their per-key value out of an index-addressed <c>*Result_t</c> at index 0, but the
/// Rust mock faults every transactional id with <c>"Not implemented yet"</c>, so no value
/// is reachable without a broker. Their per-key <em>failure</em> path is covered below;
/// their value readers are not, and cannot be from a managed test.
/// </para>
/// </remarks>
public sealed class AdminP9PerKeyStage2Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code the mock reports for every unimplemented RPC.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// One key succeeding and another failing resolve <b>independently</b>, and the
    /// copied-out value outlives the owned <c>TopicDescription_t</c> the callback destroyed.
    /// </summary>
    [Fact]
    public async Task DescribeTopicsByName_PerKeyOutcomes_AreIndependent()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp3-present", 3, 1) }, options: null).All(),
            s_deadline);

        DescribeTopicsResult result = admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { "cp3-present", "cp3-absent" }), options: null);

        TopicDescription present = await TestTimeout.Run(
            () => result.TopicNameValues!["cp3-present"], s_deadline);
        Assert.Equal("cp3-present", present.Name);
        Assert.Equal(3, present.Partitions.Count);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.TopicNameValues!["cp3-absent"], s_deadline));
        Assert.Equal("Topic cp3-absent not found.", failure.Message);
    }

    /// <summary>
    /// The by-id form's key crosses as Java's <c>Uuid.toString()</c> base64 text and comes
    /// back as the very <see cref="Uuid"/> that was requested — the only thing that proves
    /// the key survived both the encode and the parse.
    /// </summary>
    [Fact]
    public async Task DescribeTopicsById_ResolvesTheRequestedUuid()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CreateTopicsResult created =
            admin.CreateTopics(new[] { new NewTopic("cp3-by-id", 1, 1) }, options: null);
        Uuid id = await TestTimeout.Run(() => created.TopicId("cp3-by-id"), s_deadline);
        Assert.NotEqual(Uuid.Zero, id);

        DescribeTopicsResult result =
            admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { id }), options: null);

        TopicDescription description = await TestTimeout.Run(() => result.TopicIdValues![id], s_deadline);
        Assert.Equal(id, description.TopicId);
        Assert.Equal("cp3-by-id", description.Name);
    }

    /// <summary>
    /// ⚠ <b>The composite key.</b> <c>describeConfigs</c>' key is two scalars — a
    /// <c>ConfigResource.Type.id()</c> and a name — so a callback that dropped either half
    /// would resolve the wrong entry, or none. Two resources of <em>different types</em>
    /// plus one absent topic make both halves load-bearing.
    /// </summary>
    [Fact]
    public async Task DescribeConfigs_ResolvesTheCompositeResourceKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp3-cfg", 1, 1) }, options: null).All(),
            s_deadline);

        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, "cp3-cfg");
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "0");
        ConfigResource absent = new ConfigResource(ConfigResourceType.Topic, "cp3-cfg-absent");

        DescribeConfigsResult result =
            admin.DescribeConfigs(new[] { topic, broker, absent }, options: null);

        Assert.NotNull(await TestTimeout.Run(() => result.Values[topic], s_deadline));
        Assert.NotNull(await TestTimeout.Run(() => result.Values[broker], s_deadline));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[absent], s_deadline));
        Assert.Equal(
            "Resource ConfigResource(type=Topic, name='cp3-cfg-absent') not found.", failure.Message);
    }

    /// <summary>
    /// ⚠ <b>The scalar key.</b> <c>describeLogDirs</c>' key is a bare <c>int32_t</c>, not a
    /// pointer, so the two brokers below discriminate a callback that read the wrong
    /// parameter slot — and the copied-out map outlives the owned
    /// <c>LogDirDescriptionMap_t</c> the callback destroyed.
    /// </summary>
    [Fact]
    public async Task DescribeLogDirs_ResolvesTheInt32BrokerKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(2);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp3-logdirs", 2, 1) }, options: null).All(),
            s_deadline);

        // Requested in DESCENDING order, so a callback that mistook the key for a position
        // would resolve them swapped.
        DescribeLogDirsResult result = admin.DescribeLogDirs(new[] { 1, 0 }, options: null);

        Assert.Equal(2, result.Descriptions.Count);
        Assert.Contains(0, result.Descriptions.Keys);
        Assert.Contains(1, result.Descriptions.Keys);

        // Both keys resolve, and between them they account for every replica of the topic.
        // Which broker hosts which replica is the mock's choice, so the union is what is
        // asserted; a key that never arrived would time out instead.
        int replicas = 0;
        foreach (int broker in new[] { 1, 0 })
        {
            IReadOnlyDictionary<string, LogDirDescription> directories =
                await TestTimeout.Run(() => result.Descriptions[broker], s_deadline);
            foreach (KeyValuePair<string, LogDirDescription> entry in directories)
            {
                Assert.Equal("/tmp/kafka-logs", entry.Key);
                replicas += entry.Value.ReplicaInfos.Count;
            }
        }

        Assert.Equal(2, replicas);
    }

    /// <summary>
    /// The five RPCs the mock cannot serve still fault <b>every</b> key independently, with
    /// Java's own message — which is what proves the key marshalling and the per-key error
    /// ownership on each of them (the value readers are the residual noted in the class
    /// remarks).
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeConsumerGroups)]
    [InlineData(Rpc.DescribeClassicGroups)]
    [InlineData(Rpc.ListConsumerGroupOffsets)]
    [InlineData(Rpc.DescribeTransactions)]
    [InlineData(Rpc.FenceProducers)]
    public async Task EveryKeyFaultsIndependently_WithJavasOwnMessage(Rpc rpc)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        string[] keys = new[] { "cp3-key-a", "cp3-key-b", "cp3-key-c" };
        IReadOnlyList<Func<Task>> awaitables = Submit(admin, rpc, keys);

        Assert.Equal(keys.Length, awaitables.Count);
        foreach (Func<Task> awaitable in awaitables)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(awaitable, s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// The <c>n == 0</c> rule at the RPC level, for all nine: an empty collection fires
    /// <b>no</b> callback at all, so only the submit's own countdown slot can release the
    /// operation. Without it the <c>GCHandle</c> and the span-the-op reference are held
    /// forever and <c>AdminClient_destroy</c> never runs — a leak the countdown introduces
    /// and this asserts away one RPC at a time.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeTopicsByName)]
    [InlineData(Rpc.DescribeTopicsById)]
    [InlineData(Rpc.DescribeConfigs)]
    [InlineData(Rpc.DescribeLogDirs)]
    [InlineData(Rpc.DescribeConsumerGroups)]
    [InlineData(Rpc.DescribeClassicGroups)]
    [InlineData(Rpc.ListConsumerGroupOffsets)]
    [InlineData(Rpc.DescribeTransactions)]
    [InlineData(Rpc.FenceProducers)]
    public void EmptyCollection_ResolvesAtTheSubmitBoundary_AndReleasesTheClient(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Empty(Submit(admin, rpc, Array.Empty<string>()));

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(
            handle.IsClosed,
            "zero callbacks must still release: the submit token is the only thing that can");
    }

    /// <summary>
    /// Many keys per operation, over many operations and seven of the nine RPCs, leave the
    /// reference count <b>balanced</b> — the countdown's core claim once arrivals are
    /// concurrent. An over-release would have thrown out of the
    /// <see cref="System.Runtime.InteropServices.SafeHandle"/>; an under-release leaves
    /// <c>IsClosed</c> false forever.
    /// </summary>
    /// <remarks>
    /// The two by-index RPCs are excluded only because their key axes are not strings:
    /// <c>describeTopics</c>-by-id needs real topic ids and <c>describeLogDirs</c> real
    /// broker ids, both of which the per-key tests above cover directly.
    /// </remarks>
    [Fact]
    public async Task ManyKeysAcrossEveryRpc_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        string[] keys = new string[10];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = $"cp3-balance-{i}";
        }

        foreach (Rpc rpc in new[]
        {
            Rpc.DescribeTopicsByName,
            Rpc.DescribeConfigs,
            Rpc.DescribeConsumerGroups,
            Rpc.DescribeClassicGroups,
            Rpc.ListConsumerGroupOffsets,
            Rpc.DescribeTransactions,
            Rpc.FenceProducers,
        })
        {
            for (int round = 0; round < 3; round++)
            {
                IReadOnlyList<Func<Task>> awaitables = Submit(admin, rpc, keys);
                Assert.Equal(keys.Length, awaitables.Count);

                foreach (Func<Task> awaitable in awaitables)
                {
                    // Every key resolves one way or the other; only that it SETTLES matters.
                    await TestTimeout.Run(() => Settle(awaitable), s_deadline);
                }

                Assert.False(handle.IsClosed, "the client is still alive between operations");
            }
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "210 per-key callbacks must leave the count balanced");
    }

    /// <summary>The nine RPCs CP3 migrated, as a test axis.</summary>
    public enum Rpc
    {
        /// <summary>Keyed by topic name.</summary>
        DescribeTopicsByName,

        /// <summary>Keyed by a base64 topic id.</summary>
        DescribeTopicsById,

        /// <summary>Keyed by the composite <c>(type id, name)</c> pair.</summary>
        DescribeConfigs,

        /// <summary>Keyed by a bare <c>int32_t</c> broker id.</summary>
        DescribeLogDirs,

        /// <summary>Keyed by group id.</summary>
        DescribeConsumerGroups,

        /// <summary>Keyed by group id.</summary>
        DescribeClassicGroups,

        /// <summary>Keyed by group id.</summary>
        ListConsumerGroupOffsets,

        /// <summary>Keyed by transactional id, value read at index 0.</summary>
        DescribeTransactions,

        /// <summary>Keyed by transactional id, value read at index 0.</summary>
        FenceProducers,
    }

    /// <summary>Awaits one key's outcome, treating a fault as a settlement.</summary>
    private static async Task Settle(Func<Task> awaitable)
    {
        try
        {
            await awaitable().ConfigureAwait(false);
        }
        catch (KafkaException)
        {
        }
    }

    /// <summary>
    /// Submits <paramref name="rpc"/> for <paramref name="keys"/> and hands back one
    /// awaitable factory per key the result exposed — so a key the bridge dropped shows up
    /// as a count mismatch rather than as a passing loop over nothing.
    /// </summary>
    private static IReadOnlyList<Func<Task>> Submit(NativeAdminClient admin, Rpc rpc, string[] keys)
    {
        List<Func<Task>> awaitables = new List<Func<Task>>(keys.Length);
        switch (rpc)
        {
            case Rpc.DescribeTopicsByName:
                {
                    DescribeTopicsResult result =
                        admin.DescribeTopics(TopicCollection.OfTopicNames(keys), options: null);
                    foreach (KeyValuePair<string, Task<TopicDescription>> entry in result.TopicNameValues!)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.DescribeTopicsById:
                {
                    List<Uuid> ids = new List<Uuid>(keys.Length);
                    for (int i = 0; i < keys.Length; i++)
                    {
                        ids.Add(new Uuid(i + 1, i + 1));
                    }

                    DescribeTopicsResult result =
                        admin.DescribeTopics(TopicCollection.OfTopicIds(ids), options: null);
                    foreach (KeyValuePair<Uuid, Task<TopicDescription>> entry in result.TopicIdValues!)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.DescribeConfigs:
                {
                    List<ConfigResource> resources = new List<ConfigResource>(keys.Length);
                    foreach (string key in keys)
                    {
                        resources.Add(new ConfigResource(ConfigResourceType.Topic, key));
                    }

                    DescribeConfigsResult result = admin.DescribeConfigs(resources, options: null);
                    foreach (KeyValuePair<ConfigResource, Task<Config>> entry in result.Values)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.DescribeLogDirs:
                {
                    List<int> brokers = new List<int>(keys.Length);
                    for (int i = 0; i < keys.Length; i++)
                    {
                        brokers.Add(i);
                    }

                    DescribeLogDirsResult result = admin.DescribeLogDirs(brokers, options: null);
                    foreach (KeyValuePair<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> entry
                        in result.Descriptions)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.DescribeConsumerGroups:
                {
                    DescribeConsumerGroupsResult result =
                        admin.DescribeConsumerGroups(keys, options: null);
                    foreach (KeyValuePair<string, Task<ConsumerGroupDescription>> entry
                        in result.DescribedGroups)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.DescribeClassicGroups:
                {
                    DescribeClassicGroupsResult result =
                        admin.DescribeClassicGroups(keys, options: null);
                    foreach (KeyValuePair<string, Task<ClassicGroupDescription>> entry
                        in result.DescribedGroups)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.ListConsumerGroupOffsets:
                {
                    Dictionary<string, ListConsumerGroupOffsetsSpec> specs =
                        new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal);
                    foreach (string key in keys)
                    {
                        specs[key] = new ListConsumerGroupOffsetsSpec();
                    }

                    ListConsumerGroupOffsetsResult result =
                        admin.ListConsumerGroupOffsets(specs, options: null);
                    foreach (string key in specs.Keys)
                    {
                        string captured = key;
                        awaitables.Add(() => result.PartitionsToOffsetAndMetadata(captured));
                    }

                    break;
                }

            case Rpc.DescribeTransactions:
                {
                    DescribeTransactionsResult result = admin.DescribeTransactions(keys, options: null);
                    foreach (string key in keys)
                    {
                        string captured = key;
                        awaitables.Add(() => result.Description(captured));
                    }

                    break;
                }

            default:
                {
                    FenceProducersResult result = admin.FenceProducers(keys, options: null);
                    foreach (KeyValuePair<string, Task> entry in result.FencedProducers)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }
        }

        return awaitables;
    }
}
