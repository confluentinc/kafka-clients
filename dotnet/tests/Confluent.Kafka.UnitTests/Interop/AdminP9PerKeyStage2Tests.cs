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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The nine value-carrying keyed RPCs over the core mock: each key's future resolves on its own, and each key read
/// back from the Result is the key that was requested, for a topic name, a topic id, a config resource and a broker id.
/// </summary>
/// <remarks>
/// The core mock fails every transactional id of <c>describeTransactions</c> and <c>fenceProducers</c> with
/// <c>"Not implemented yet"</c>, so only their per-key failures are covered here, not their value readers.
/// </remarks>
public sealed class AdminP9PerKeyStage2Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task DescribeTopicsByName_PerKeyOutcomes_AreIndependent()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("cp3-present", 3, 1) }, null)).All().Get(),
            s_deadline);

        AsyncDescribeTopicsResult result = AsyncDescribeTopicsResult.OfClient(admin.DescribeTopics(
            TopicCollection.OfTopicNames(new[] { "cp3-present", "cp3-absent" }), options: null));

        TopicDescription present = await TestTimeout.Run(
            () => result.TopicNameValues!["cp3-present"].Get(), s_deadline);
        Assert.Equal("cp3-present", present.Name);
        Assert.Equal(3, present.Partitions.Count);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.TopicNameValues!["cp3-absent"].Get(), s_deadline));
        Assert.Equal("Topic cp3-absent not found.", failure.Message);
    }

    [Fact]
    public async Task DescribeTopicsById_ResolvesTheRequestedUuid()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult created =
            AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("cp3-by-id", 1, 1) }, null));
        Uuid id = await TestTimeout.Run(() => created.TopicId("cp3-by-id").Get(), s_deadline);
        Assert.NotEqual(Uuid.Zero, id);

        AsyncDescribeTopicsResult result =
            AsyncDescribeTopicsResult.OfClient(admin.DescribeTopics(TopicCollection.OfTopicIds(new[] { id }), options: null));

        TopicDescription description = await TestTimeout.Run(() => result.TopicIdValues![id].Get(), s_deadline);
        Assert.Equal(id, description.TopicId);
        Assert.Equal("cp3-by-id", description.Name);
    }

    [Fact]
    public async Task DescribeConfigs_ResolvesTheCompositeResourceKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("cp3-cfg", 1, 1) }, null)).All().Get(),
            s_deadline);

        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, "cp3-cfg");
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "0");
        ConfigResource absent = new ConfigResource(ConfigResourceType.Topic, "cp3-cfg-absent");

        AsyncDescribeConfigsResult result =
            AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(new[] { topic, broker, absent }, options: null));

        Assert.NotNull(await TestTimeout.Run(() => result.Values[topic].Get(), s_deadline));
        Assert.NotNull(await TestTimeout.Run(() => result.Values[broker].Get(), s_deadline));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[absent].Get(), s_deadline));
        Assert.Equal(
            "Resource ConfigResource(type=TOPIC, name='cp3-cfg-absent') not found.", failure.Message);
    }

    [Fact]
    public async Task DescribeLogDirs_ResolvesTheInt32BrokerKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(2);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("cp3-logdirs", 2, 1) }, null)).All().Get(),
            s_deadline);

        // Requested in descending order, so a reader that took a key for a position would swap them.
        AsyncDescribeLogDirsResult result = AsyncDescribeLogDirsResult.OfClient(admin.DescribeLogDirs(new[] { 1, 0 }, options: null));

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
                await TestTimeout.Run(() => result.Descriptions[broker].Get(), s_deadline);
            foreach (KeyValuePair<string, LogDirDescription> entry in directories)
            {
                Assert.Equal("/tmp/kafka-logs", entry.Key);
                replicas += entry.Value.ReplicaInfos.Count;
            }
        }

        Assert.Equal(2, replicas);
    }

    [Theory]
    [InlineData(Rpc.DescribeConsumerGroups)]
    [InlineData(Rpc.DescribeClassicGroups)]
    [InlineData(Rpc.ListConsumerGroupOffsets)]
    [InlineData(Rpc.DescribeTransactions)]
    [InlineData(Rpc.FenceProducers)]
    public async Task EveryKeyFaultsIndependently_WithJavasOwnMessage(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        string[] keys = new[] { "cp3-key-a", "cp3-key-b", "cp3-key-c" };
        IReadOnlyList<Func<Task>> awaitables = Submit(admin, rpc, keys);

        Assert.Equal(keys.Length, awaitables.Count);
        foreach (Func<Task> awaitable in awaitables)
        {
            NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
                () => TestTimeout.Run(awaitable, s_deadline));
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>The nine RPCs, as a test axis.</summary>
    public enum Rpc
    {
        DescribeTopicsByName,

        DescribeTopicsById,

        DescribeConfigs,

        DescribeLogDirs,

        DescribeConsumerGroups,

        DescribeClassicGroups,

        ListConsumerGroupOffsets,

        DescribeTransactions,

        FenceProducers,
    }

    private static IReadOnlyList<Func<Task>> Submit(NativeAdminClient admin, Rpc rpc, string[] keys)
    {
        List<Func<Task>> awaitables = new List<Func<Task>>(keys.Length);
        switch (rpc)
        {
            case Rpc.DescribeTopicsByName:
                {
                    AsyncDescribeTopicsResult result =
                        AsyncDescribeTopicsResult.OfClient(admin.DescribeTopics(TopicCollection.OfTopicNames(keys), options: null));
                    foreach (KeyValuePair<string, AsyncKafkaFuture<TopicDescription>> entry in result.TopicNameValues!)
                    {
                        awaitables.Add(() => entry.Value.Get());
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

                    AsyncDescribeTopicsResult result =
                        AsyncDescribeTopicsResult.OfClient(admin.DescribeTopics(TopicCollection.OfTopicIds(ids), options: null));
                    foreach (KeyValuePair<Uuid, AsyncKafkaFuture<TopicDescription>> entry in result.TopicIdValues!)
                    {
                        awaitables.Add(() => entry.Value.Get());
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

                    AsyncDescribeConfigsResult result = AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(resources, options: null));
                    foreach (KeyValuePair<ConfigResource, AsyncKafkaFuture<Config>> entry in result.Values)
                    {
                        awaitables.Add(() => entry.Value.Get());
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

                    AsyncDescribeLogDirsResult result = AsyncDescribeLogDirsResult.OfClient(admin.DescribeLogDirs(brokers, options: null));
                    foreach (KeyValuePair<int, AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> entry
                        in result.Descriptions)
                    {
                        awaitables.Add(() => entry.Value.Get());
                    }

                    break;
                }

            case Rpc.DescribeConsumerGroups:
                {
                    AsyncDescribeConsumerGroupsResult result =
                        AsyncDescribeConsumerGroupsResult.OfClient(admin.DescribeConsumerGroups(keys, options: null));
                    foreach (KeyValuePair<string, AsyncKafkaFuture<ConsumerGroupDescription>> entry
                        in result.DescribedGroups)
                    {
                        awaitables.Add(() => entry.Value.Get());
                    }

                    break;
                }

            case Rpc.DescribeClassicGroups:
                {
                    AsyncDescribeClassicGroupsResult result =
                        AsyncDescribeClassicGroupsResult.OfClient(admin.DescribeClassicGroups(keys, options: null));
                    foreach (KeyValuePair<string, AsyncKafkaFuture<ClassicGroupDescription>> entry
                        in result.DescribedGroups)
                    {
                        awaitables.Add(() => entry.Value.Get());
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

                    AsyncListConsumerGroupOffsetsResult result =
                        AsyncListConsumerGroupOffsetsResult.OfClient(admin.ListConsumerGroupOffsets(specs, options: null));
                    foreach (string key in specs.Keys)
                    {
                        string captured = key;
                        awaitables.Add(() => result.PartitionsToOffsetAndMetadata(captured).Get());
                    }

                    break;
                }

            case Rpc.DescribeTransactions:
                {
                    AsyncDescribeTransactionsResult result = AsyncDescribeTransactionsResult.OfClient(admin.DescribeTransactions(keys, options: null));
                    foreach (string key in keys)
                    {
                        string captured = key;
                        awaitables.Add(() => result.Description(captured).Get());
                    }

                    break;
                }

            default:
                {
                    AsyncFenceProducersResult result = AsyncFenceProducersResult.OfClient(admin.FenceProducers(keys, options: null));
                    foreach (KeyValuePair<string, AsyncKafkaFuture> entry in result.FencedProducers)
                    {
                        awaitables.Add(() => entry.Value.Get());
                    }

                    break;
                }
        }

        return awaitables;
    }
}
