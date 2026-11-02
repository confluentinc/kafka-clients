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
/// The mock admin clients' own surface on both mocks: Java's <c>Builder</c>, the two constructors, <c>Close</c>, the
/// seeders and the log dirs a <c>Builder</c> seeds, with the core's messages. Python's keyword-constructor and
/// setter-order cases have no .NET counterpart and are not translated.
/// </summary>
public sealed class PublicAdminMockAdminClientTests
{
    private const string DefaultClusterId = "I4ZmrWqfT2e-upky_4fdPA";
    private const string DefaultLogDir = "/tmp/kafka-logs";
    private const string ControllerNotABroker = "The controller node must be in the list of brokers";
    private const int UnknownTopicOrPartitionCode = 3;
    private const int KafkaStorageErrorCode = 56;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Builder_Defaults_MatchJava_OnBothMocks()
    {
        foreach (MockAdminClient admin in new[] { MockAdminClient.Create().Build(), new MockAdminClient.Builder().Build() })
        {
            using IDisposable disposal = TestTimeout.Disposal(admin);
            DescribeClusterResult cluster = admin.DescribeCluster();
            AssertSeeded(new[] { 0 }, cluster.Nodes().Get(s_deadline));
            AssertNode(Seeded(0), cluster.Controller().Get(s_deadline));
            Assert.Equal(DefaultClusterId, cluster.ClusterId().Get(s_deadline));
            CreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic("t", (int?)null, (short?)null) });
            Assert.Equal(1, created.NumPartitions("t").Get(s_deadline));
            Assert.Equal(1, created.ReplicationFactor("t").Get(s_deadline));
        }

        foreach (AsyncMockAdminClient admin in new[] { AsyncMockAdminClient.Create().Build(), new AsyncMockAdminClient.Builder().Build() })
        {
            using IDisposable disposal = TestTimeout.Disposal(admin);
            AsyncDescribeClusterResult cluster = admin.DescribeCluster();
            AssertSeeded(new[] { 0 }, await TestTimeout.Run(() => cluster.Nodes().Get(), s_deadline));
            AssertNode(Seeded(0), await TestTimeout.Run(() => cluster.Controller().Get(), s_deadline));
            Assert.Equal(DefaultClusterId, await TestTimeout.Run(() => cluster.ClusterId().Get(), s_deadline));
            AsyncCreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic("t", (int?)null, (short?)null) });
            Assert.Equal(1, await TestTimeout.Run(() => created.NumPartitions("t").Get(), s_deadline));
            Assert.Equal(1, await TestTimeout.Run(() => created.ReplicationFactor("t").Get(), s_deadline));
        }
    }

    [Fact]
    public async Task DefaultClusterIdAndDefaultLogDirs_AreJavasValues_AndTheBuildersDefaults_OnBothMocks()
    {
        Assert.Equal(DefaultClusterId, MockAdminClient.DefaultClusterId);
        Assert.Equal(DefaultClusterId, AsyncMockAdminClient.DefaultClusterId);
        Assert.Equal(new[] { DefaultLogDir }, MockAdminClient.DefaultLogDirs);
        Assert.Equal(new[] { DefaultLogDir }, AsyncMockAdminClient.DefaultLogDirs);
        Assert.Throws<NotSupportedException>(() => ((IList<string>)MockAdminClient.DefaultLogDirs)[0] = "/elsewhere");
        Assert.Throws<NotSupportedException>(() => ((IList<string>)AsyncMockAdminClient.DefaultLogDirs)[0] = "/elsewhere");

        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        Assert.Equal(MockAdminClient.DefaultClusterId, sync.DescribeCluster().ClusterId().Get(s_deadline));
        sync.CreateTopics(new[] { new NewTopic("t", 1, 1) }).All().Get(s_deadline);
        Assert.Equal(
            MockAdminClient.DefaultLogDirs, sync.DescribeLogDirs(new[] { 0 }).Descriptions[0].Get(s_deadline).Keys);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Assert.Equal(
            AsyncMockAdminClient.DefaultClusterId,
            await TestTimeout.Run(() => async.DescribeCluster().ClusterId().Get(), s_deadline));
        await TestTimeout.Run(() => async.CreateTopics(new[] { new NewTopic("t", 1, 1) }).All().Get(), s_deadline);
        Assert.Equal(
            AsyncMockAdminClient.DefaultLogDirs,
            (await TestTimeout.Run(() => async.DescribeLogDirs(new[] { 0 }).Descriptions[0].Get(), s_deadline)).Keys);
    }

    [Fact]
    public void Seeders_TakeJavasParameterNames_OnBothMocks()
    {
        Dictionary<string, string> expected = new Dictionary<string, string>
        {
            [nameof(MockAdminClient.TimeoutNextRequest)] = "numberOfRequest",
            [nameof(MockAdminClient.UpdateBeginningOffsets)] = "newOffsets",
            [nameof(MockAdminClient.UpdateEndOffsets)] = "newOffsets",
            [nameof(MockAdminClient.UpdateConsumerGroupOffsets)] = "newOffsets",
        };

        foreach (Type mock in new[] { typeof(MockAdminClient), typeof(AsyncMockAdminClient) })
        {
            foreach (KeyValuePair<string, string> seeder in expected)
            {
                Assert.Equal(
                    new[] { seeder.Value },
                    Assert.Single(mock.GetMethods(), method => method.Name == seeder.Key)
                        .GetParameters().Select(static parameter => parameter.Name ?? string.Empty));
            }
        }
    }

    [Fact]
    public async Task Builder_Setters_ReachTheMock_OnBothMocks()
    {
        Node[] brokers = { Seeded(0), Seeded(1), Seeded(2) };

        MockAdminClient sync = MockAdminClient.Create().ClusterId("c").Brokers(brokers).Controller(2)
            .DefaultPartitions(4).DefaultReplicationFactor(2).Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        DescribeClusterResult syncCluster = sync.DescribeCluster();
        Assert.Equal("c", syncCluster.ClusterId().Get(s_deadline));
        AssertNode(Seeded(2), syncCluster.Controller().Get(s_deadline));
        AssertSeeded(new[] { 0, 1, 2 }, syncCluster.Nodes().Get(s_deadline));
        CreateTopicsResult syncCreated = sync.CreateTopics(new[] { new NewTopic("t", (int?)null, (short?)null) });
        Assert.Equal(4, syncCreated.NumPartitions("t").Get(s_deadline));
        Assert.Equal(2, syncCreated.ReplicationFactor("t").Get(s_deadline));

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().ClusterId("c").Brokers(brokers).Controller(2)
            .DefaultPartitions(4).DefaultReplicationFactor(2).Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AsyncDescribeClusterResult asyncCluster = async.DescribeCluster();
        Assert.Equal("c", await TestTimeout.Run(() => asyncCluster.ClusterId().Get(), s_deadline));
        AssertNode(Seeded(2), await TestTimeout.Run(() => asyncCluster.Controller().Get(), s_deadline));
        AssertSeeded(new[] { 0, 1, 2 }, await TestTimeout.Run(() => asyncCluster.Nodes().Get(), s_deadline));
        AsyncCreateTopicsResult asyncCreated = async.CreateTopics(new[] { new NewTopic("t", (int?)null, (short?)null) });
        Assert.Equal(4, await TestTimeout.Run(() => asyncCreated.NumPartitions("t").Get(), s_deadline));
        Assert.Equal(2, await TestTimeout.Run(() => asyncCreated.ReplicationFactor("t").Get(), s_deadline));
    }

    [Fact]
    public async Task Builder_NumBrokers_ShrinksAndGrows_AndTheBuilderBuildsAgain_OnBothMocks()
    {
        MockAdminClient.Builder syncBuilder = MockAdminClient.Create().NumBrokers(3).NumBrokers(2);
        MockAdminClient syncTwo = syncBuilder.Build();
        using IDisposable syncTwoDisposal = TestTimeout.Disposal(syncTwo);
        MockAdminClient syncFour = syncBuilder.NumBrokers(4).Build();
        using IDisposable syncFourDisposal = TestTimeout.Disposal(syncFour);
        AssertSeeded(new[] { 0, 1 }, syncTwo.DescribeCluster().Nodes().Get(s_deadline));
        AssertSeeded(new[] { 0, 1, 2, 3 }, syncFour.DescribeCluster().Nodes().Get(s_deadline));

        AsyncMockAdminClient.Builder asyncBuilder = AsyncMockAdminClient.Create().NumBrokers(3).NumBrokers(2);
        AsyncMockAdminClient asyncTwo = asyncBuilder.Build();
        using IDisposable asyncTwoDisposal = TestTimeout.Disposal(asyncTwo);
        AsyncMockAdminClient asyncFour = asyncBuilder.NumBrokers(4).Build();
        using IDisposable asyncFourDisposal = TestTimeout.Disposal(asyncFour);
        AssertSeeded(new[] { 0, 1 }, await TestTimeout.Run(() => asyncTwo.DescribeCluster().Nodes().Get(), s_deadline));
        AssertSeeded(new[] { 0, 1, 2, 3 }, await TestTimeout.Run(() => asyncFour.DescribeCluster().Nodes().Get(), s_deadline));
    }

    [Fact]
    public void Builder_ControllerOutOfRange_ThrowsAtTheSetter_OnBothMocks()
    {
        ArgumentException sync = Assert.Throws<ArgumentException>(() => MockAdminClient.Create().Controller(5));
        ArgumentException async = Assert.Throws<ArgumentException>(() => AsyncMockAdminClient.Create().Controller(5));

        Assert.Equal("Index 5 out of bounds for length 1", sync.Message);
        Assert.Equal("Index 5 out of bounds for length 1", async.Message);
    }

    [Fact]
    public async Task Builder_DefaultReplicationFactor_IsItsShortValue_OnBothMocks()
    {
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "0");

        MockAdminClient sync = MockAdminClient.Create().DefaultReplicationFactor(65538).Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        Config syncConfig = sync.DescribeConfigs(new[] { broker }).Values[broker].Get(s_deadline);
        Assert.Equal("2", syncConfig.Get("default.replication.factor")?.Value);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().DefaultReplicationFactor(65538).Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Config asyncConfig = await TestTimeout.Run(() => async.DescribeConfigs(new[] { broker }).Values[broker].Get(), s_deadline);
        Assert.Equal("2", asyncConfig.Get("default.replication.factor")?.Value);
    }

    [Fact]
    public void Create_ReturnsEachMocksOwnBuilder_WhichBuildsThatMock()
    {
        AsyncMockAdminClient.Builder asyncBuilder = Assert.IsType<AsyncMockAdminClient.Builder>(AsyncMockAdminClient.Create());
        Assert.False(typeof(MockAdminClient.Builder).IsAssignableFrom(typeof(AsyncMockAdminClient.Builder)));

        AsyncMockAdminClient async = Assert.IsType<AsyncMockAdminClient>(asyncBuilder.NumBrokers(2).Build());
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Assert.IsAssignableFrom<IAsyncAdmin>(async);
        AsyncMockAdminClient withClusterId =
            Assert.IsType<AsyncMockAdminClient>(new AsyncMockAdminClient.Builder().ClusterId("c").Build());
        using IDisposable withClusterIdDisposal = TestTimeout.Disposal(withClusterId);

        MockAdminClient.Builder syncBuilder = Assert.IsType<MockAdminClient.Builder>(MockAdminClient.Create());
        MockAdminClient sync = Assert.IsType<MockAdminClient>(syncBuilder.Build());
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        Assert.IsAssignableFrom<IAdmin>(sync);
    }

    [Fact]
    public void AsyncMock_Builder_RejectsNoBrokerAtBuild_AndANegativeCountAtTheSetter()
    {
        AsyncMockAdminClient.Builder builder = AsyncMockAdminClient.Create().NumBrokers(0);
        ArgumentException noBroker = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Equal("Index 0 out of bounds for length 0", noBroker.Message);

        ArgumentException negative = Assert.Throws<ArgumentException>(() => AsyncMockAdminClient.Create().NumBrokers(-1));
        Assert.Equal("fromIndex(0) > toIndex(-1)", negative.Message);
    }

    [Fact]
    public async Task Controller_IsAlwaysOneOfTheSeededNodes_OnBothMocks()
    {
        for (int numBrokers = 1; numBrokers <= 3; numBrokers++)
        {
            int[] ids = Enumerable.Range(0, numBrokers).ToArray();

            MockAdminClient sync = MockAdminClient.Create().NumBrokers(numBrokers).Build();
            using (TestTimeout.Disposal(sync))
            {
                DescribeClusterResult cluster = sync.DescribeCluster();
                AssertSeeded(ids, cluster.Nodes().Get(s_deadline));
                AssertNode(Seeded(0), cluster.Controller().Get(s_deadline));
            }

            AsyncMockAdminClient async = AsyncMockAdminClient.Create().NumBrokers(numBrokers).Build();
            using (TestTimeout.Disposal(async))
            {
                AsyncDescribeClusterResult cluster = async.DescribeCluster();
                AssertSeeded(ids, await TestTimeout.Run(() => cluster.Nodes().Get(), s_deadline));
                AssertNode(Seeded(0), await TestTimeout.Run(() => cluster.Controller().Get(), s_deadline));
            }
        }
    }

    [Fact]
    public async Task NoArgumentConstructor_HasOneNoNodeBrokerAsItsController_OnBothMocks()
    {
        Node noNode = new Node(-1, string.Empty, -1);

        MockAdminClient sync = new MockAdminClient();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        DescribeClusterResult syncCluster = sync.DescribeCluster();
        AssertNode(noNode, Assert.Single(syncCluster.Nodes().Get(s_deadline)));
        AssertNode(noNode, syncCluster.Controller().Get(s_deadline));

        AsyncMockAdminClient async = new AsyncMockAdminClient();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AsyncDescribeClusterResult asyncCluster = async.DescribeCluster();
        AssertNode(noNode, Assert.Single(await TestTimeout.Run(() => asyncCluster.Nodes().Get(), s_deadline)));
        AssertNode(noNode, await TestTimeout.Run(() => asyncCluster.Controller().Get(), s_deadline));
    }

    [Fact]
    public async Task BrokersControllerConstructor_UsesTheBrokerCountAsTheReplicationFactor_OnBothMocks()
    {
        Node[] brokers = { Seeded(0), Seeded(1) };

        MockAdminClient sync = new MockAdminClient(brokers, Seeded(1));
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AssertSeeded(new[] { 0, 1 }, sync.DescribeCluster().Nodes().Get(s_deadline));
        AssertNode(Seeded(1), sync.DescribeCluster().Controller().Get(s_deadline));
        CreateTopicsResult syncCreated = sync.CreateTopics(new[] { new NewTopic("t", (int?)null, (short?)null) });
        Assert.Equal(2, syncCreated.ReplicationFactor("t").Get(s_deadline));

        AsyncMockAdminClient async = new AsyncMockAdminClient(brokers, Seeded(1));
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AssertSeeded(new[] { 0, 1 }, await TestTimeout.Run(() => async.DescribeCluster().Nodes().Get(), s_deadline));
        AssertNode(Seeded(1), await TestTimeout.Run(() => async.DescribeCluster().Controller().Get(), s_deadline));
        AsyncCreateTopicsResult asyncCreated = async.CreateTopics(new[] { new NewTopic("t", (int?)null, (short?)null) });
        Assert.Equal(2, await TestTimeout.Run(() => asyncCreated.ReplicationFactor("t").Get(), s_deadline));
    }

    [Fact]
    public void BrokersControllerConstructor_RejectsAControllerNotInTheBrokers_OnBothMocks()
    {
        ArgumentException sync = Assert.Throws<ArgumentException>(() => new MockAdminClient(new[] { Seeded(0) }, Seeded(1)));
        ArgumentException async = Assert.Throws<ArgumentException>(() => new AsyncMockAdminClient(new[] { Seeded(0) }, Seeded(1)));

        Assert.Equal(ControllerNotABroker, sync.Message);
        Assert.Equal(ControllerNotABroker, async.Message);
    }

    [Fact]
    public void BrokersControllerConstructor_RejectsANullController_AsANodeNotInTheBrokers_OnBothMocks()
    {
        ArgumentException sync = Assert.Throws<ArgumentException>(() => new MockAdminClient(new[] { Seeded(0) }, null!));
        ArgumentException async = Assert.Throws<ArgumentException>(() => new AsyncMockAdminClient(new[] { Seeded(0) }, null!));

        Assert.StartsWith(ControllerNotABroker, sync.Message);
        Assert.Equal("controller", sync.ParamName);
        Assert.StartsWith(ControllerNotABroker, async.Message);
        Assert.Equal("controller", async.ParamName);
    }

    [Fact]
    public async Task Close_DoesNothing_AndTheMockKeepsAnswering_OnBothMocks()
    {
        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.Close();
        sync.Close(TimeSpan.FromSeconds(-1));
        Assert.Equal(3, sync.DescribeCluster().Nodes().Get(s_deadline).Count);
        Assert.Empty(sync.ListTopics().Names().Get(s_deadline));

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await TestTimeout.Run(() => async.Close(), s_deadline);
        await TestTimeout.Run(() => async.Close(TimeSpan.FromSeconds(-1)), s_deadline);
        Assert.Equal(3, (await TestTimeout.Run(() => async.DescribeCluster().Nodes().Get(), s_deadline)).Count);
        Assert.Empty(await TestTimeout.Run(() => async.ListTopics().Names().Get(), s_deadline));
    }

    [Fact]
    public async Task AddTopic_AcceptsPartitionsWhoseBrokersAreAllKnown_OnBothMocks()
    {
        Node[] replicas = { Seeded(0), Seeded(1) };
        TopicPartitionInfo[] partitions = { new TopicPartitionInfo(0, Seeded(0), replicas, replicas) };

        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.AddTopic(false, "topic", partitions, new Dictionary<string, string>());
        Assert.Equal(new[] { "topic" }, sync.ListTopics().Names().Get(s_deadline));

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        async.AddTopic(false, "topic", partitions, new Dictionary<string, string>());
        Assert.Equal(new[] { "topic" }, await TestTimeout.Run(() => async.ListTopics().Names().Get(), s_deadline));
    }

    [Fact]
    public async Task AddTopic_APartitionWithNoElrSets_ReadsBackAbsent_AndOneWithThemReadsThemBack_OnBothMocks()
    {
        Node[] replicas = { Seeded(0), Seeded(1) };
        TopicPartitionInfo[] partitions =
        {
            new TopicPartitionInfo(0, Seeded(0), replicas, replicas),
            new TopicPartitionInfo(1, Seeded(1), replicas, replicas, new[] { Seeded(1) }, Array.Empty<Node>()),
        };

        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.AddTopic(false, "topic", partitions, new Dictionary<string, string>());
        AssertElrRead(sync.DescribeTopics(TopicCollection.OfTopicNames(new[] { "topic" })).TopicNameValues!["topic"].Get(s_deadline));

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        async.AddTopic(false, "topic", partitions, new Dictionary<string, string>());
        AsyncKafkaFuture<TopicDescription> described =
            async.DescribeTopics(TopicCollection.OfTopicNames(new[] { "topic" })).TopicNameValues!["topic"];
        AssertElrRead(await TestTimeout.Run(() => described.Get(), s_deadline));

        static void AssertElrRead(TopicDescription description)
        {
            TopicPartitionInfo none = description.Partitions.Single(p => p.Partition == 0);
            Assert.Null(none.Elr);
            Assert.Null(none.LastKnownElr);
            TopicPartitionInfo reported = description.Partitions.Single(p => p.Partition == 1);
            AssertNode(Seeded(1), Assert.Single(reported.Elr!));
            Assert.NotNull(reported.LastKnownElr);
            Assert.Empty(reported.LastKnownElr!);
        }
    }

    [Fact]
    public void AddTopic_RejectsADuplicateTopic_OnBothMocks()
    {
        TopicPartitionInfo[] partitions = { new TopicPartitionInfo(0, Seeded(0), new[] { Seeded(0) }, new[] { Seeded(0) }) };

        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.AddTopic(false, "topic", partitions, new Dictionary<string, string>());
        ArgumentException syncFailure = Assert.Throws<ArgumentException>(
            () => sync.AddTopic(false, "topic", partitions, new Dictionary<string, string>()));
        Assert.Equal("Topic topic was already added.", syncFailure.Message);

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        async.AddTopic(false, "topic", partitions, new Dictionary<string, string>());
        ArgumentException asyncFailure = Assert.Throws<ArgumentException>(
            () => async.AddTopic(false, "topic", partitions, new Dictionary<string, string>()));
        Assert.Equal("Topic topic was already added.", asyncFailure.Message);
    }

    [Fact]
    public async Task AddTopic_RejectsAnUnknownOrNoLeader_AndRecordsNothing_OnBothMocks()
    {
        TopicPartitionInfo[] unknownLeader = { new TopicPartitionInfo(0, Unknown(), new[] { Seeded(0) }, Array.Empty<Node>()) };
        TopicPartitionInfo[] noLeader = { new TopicPartitionInfo(0, null, new[] { Seeded(0) }, Array.Empty<Node>()) };

        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        Assert.Equal("Leader broker unknown", Assert.Throws<ArgumentException>(
            () => sync.AddTopic(false, "topic", unknownLeader, new Dictionary<string, string>())).Message);
        Assert.Equal("Leader broker unknown", Assert.Throws<ArgumentException>(
            () => sync.AddTopic(false, "other", noLeader, new Dictionary<string, string>())).Message);
        Assert.Empty(sync.ListTopics().Names().Get(s_deadline));

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Assert.Equal("Leader broker unknown", Assert.Throws<ArgumentException>(
            () => async.AddTopic(false, "topic", unknownLeader, new Dictionary<string, string>())).Message);
        Assert.Equal("Leader broker unknown", Assert.Throws<ArgumentException>(
            () => async.AddTopic(false, "other", noLeader, new Dictionary<string, string>())).Message);
        Assert.Empty(await TestTimeout.Run(() => async.ListTopics().Names().Get(), s_deadline));
    }

    [Theory]
    [InlineData(true, "Unknown brokers in replica list")]
    [InlineData(false, "Unknown brokers in isr list")]
    public void AddTopic_RejectsUnknownBrokersInTheReplicaOrIsrList_OnBothMocks(bool inReplicas, string message)
    {
        TopicPartitionInfo[] partitions = inReplicas
            ? new[] { new TopicPartitionInfo(0, Seeded(0), new[] { Seeded(0), Unknown() }, Array.Empty<Node>()) }
            : new[] { new TopicPartitionInfo(0, Seeded(0), new[] { Seeded(0) }, new[] { Unknown() }) };

        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        Assert.Equal(message, Assert.Throws<ArgumentException>(
            () => sync.AddTopic(false, "topic", partitions, new Dictionary<string, string>())).Message);

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Assert.Equal(message, Assert.Throws<ArgumentException>(
            () => async.AddTopic(false, "topic", partitions, new Dictionary<string, string>())).Message);
    }

    [Fact]
    public void AddTopic_RejectsALeaderWithNoLogDirectories_OnBothMocks()
    {
        IReadOnlyList<string>[] noLogDirs = { Array.Empty<string>() };
        TopicPartitionInfo[] partitions = { new TopicPartitionInfo(0, Seeded(0), new[] { Seeded(0) }, Array.Empty<Node>()) };

        MockAdminClient sync = MockAdminClient.Create().BrokerLogDirs(noLogDirs).Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        Assert.Equal("Broker 0 has no log directories.", Assert.Throws<ArgumentException>(
            () => sync.AddTopic(false, "topic", partitions, new Dictionary<string, string>())).Message);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().BrokerLogDirs(noLogDirs).Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        Assert.Equal("Broker 0 has no log directories.", Assert.Throws<ArgumentException>(
            () => async.AddTopic(false, "topic", partitions, new Dictionary<string, string>())).Message);
    }

    [Fact]
    public void MarkTopicForDeletion_RejectsAnUnknownTopic_OnBothMocks()
    {
        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        Assert.Equal("Topic nope did not exist.", Assert.Throws<ArgumentException>(() => sync.MarkTopicForDeletion("nope")).Message);
        Assert.Equal("Topic nope did not exist.", Assert.Throws<ArgumentException>(() => async.MarkTopicForDeletion("nope")).Message);
    }

    [Fact]
    public async Task MarkedForDeletion_AndFetchesRemainingUntilVisible_OnBothMocks()
    {
        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.CreateTopics(new[] { new NewTopic("t", 1, 1) }).All().Get(s_deadline);
        sync.SetFetchesRemainingUntilVisible("t", 1);
        Assert.Empty(sync.ListTopics().Names().Get(s_deadline));
        Assert.Equal(new[] { "t" }, sync.ListTopics().Names().Get(s_deadline));
        sync.MarkTopicForDeletion("t");
        KafkaFuture<TopicDescription> syncDescribed =
            sync.DescribeTopics(TopicCollection.OfTopicNames(new[] { "t" })).TopicNameValues!["t"];
        Assert.Equal(UnknownTopicOrPartitionCode, Assert.Throws<KafkaException>(() => syncDescribed.Get(s_deadline)).Code);
        Assert.Equal("No such topic as x", Assert.Throws<ArgumentException>(() => sync.SetFetchesRemainingUntilVisible("x", 1)).Message);

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await TestTimeout.Run(() => async.CreateTopics(new[] { new NewTopic("t", 1, 1) }).All().Get(), s_deadline);
        async.SetFetchesRemainingUntilVisible("t", 1);
        Assert.Empty(await TestTimeout.Run(() => async.ListTopics().Names().Get(), s_deadline));
        Assert.Equal(new[] { "t" }, await TestTimeout.Run(() => async.ListTopics().Names().Get(), s_deadline));
        async.MarkTopicForDeletion("t");
        AsyncKafkaFuture<TopicDescription> asyncDescribed =
            async.DescribeTopics(TopicCollection.OfTopicNames(new[] { "t" })).TopicNameValues!["t"];
        KafkaException asyncFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncDescribed.Get()), s_deadline);
        Assert.Equal(UnknownTopicOrPartitionCode, asyncFailure.Code);
        Assert.Equal("No such topic as x", Assert.Throws<ArgumentException>(() => async.SetFetchesRemainingUntilVisible("x", 1)).Message);
    }

    [Fact]
    public async Task Controller_MovesToAKnownBroker_AndRejectsAnyOtherNode_OnBothMocks()
    {
        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.Controller(Seeded(2));
        AssertNode(Seeded(2), sync.DescribeCluster().Controller().Get(s_deadline));
        Assert.Equal(ControllerNotABroker, Assert.Throws<ArgumentException>(() => sync.Controller(Unknown())).Message);
        ArgumentException syncNoNode = Assert.Throws<ArgumentException>(() => sync.Controller(null!));
        Assert.StartsWith(ControllerNotABroker, syncNoNode.Message);
        Assert.Equal("controller", syncNoNode.ParamName);
        AssertNode(Seeded(2), sync.DescribeCluster().Controller().Get(s_deadline));

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        async.Controller(Seeded(2));
        AssertNode(Seeded(2), await TestTimeout.Run(() => async.DescribeCluster().Controller().Get(), s_deadline));
        Assert.Equal(ControllerNotABroker, Assert.Throws<ArgumentException>(() => async.Controller(Unknown())).Message);
        ArgumentException asyncNoNode = Assert.Throws<ArgumentException>(() => async.Controller(null!));
        Assert.StartsWith(ControllerNotABroker, asyncNoNode.Message);
        Assert.Equal("controller", asyncNoNode.ParamName);
        AssertNode(Seeded(2), await TestTimeout.Run(() => async.DescribeCluster().Controller().Get(), s_deadline));
    }

    [Fact]
    public async Task CreateTopics_FailsALeaderWithNoLogDirectories_OnBothMocks()
    {
        IReadOnlyList<string>[] logDirs = { Array.Empty<string>(), new[] { "/a" }, new[] { "/b" } };
        Node[] brokers = { Seeded(0), Seeded(1), Seeded(2) };

        MockAdminClient sync = MockAdminClient.Create().Brokers(brokers).BrokerLogDirs(logDirs).Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        KafkaFuture syncCreated = sync.CreateTopics(new[] { new NewTopic("t", 1, 1) }).Values["t"];
        Assert.Equal("Broker 0 has no log directories.", Assert.Throws<ArgumentException>(() => syncCreated.Get(s_deadline)).Message);

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Brokers(brokers).BrokerLogDirs(logDirs).Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        AsyncKafkaFuture asyncCreated = async.CreateTopics(new[] { new NewTopic("t", 1, 1) }).Values["t"];
        ArgumentException asyncFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<ArgumentException>(() => asyncCreated.Get()), s_deadline);
        Assert.Equal("Broker 0 has no log directories.", asyncFailure.Message);
    }

    [Fact]
    public async Task SeededLogDirs_ReachAlterAndDescribeReplicaLogDirs_OnBothMocks()
    {
        IReadOnlyList<string>[] logDirs = { new[] { "/a", "/b" } };
        TopicPartitionReplica replica = new TopicPartitionReplica("t", 0, 0);

        MockAdminClient sync = MockAdminClient.Create().BrokerLogDirs(logDirs).Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.CreateTopics(new[] { new NewTopic("t", 1, 1) }).All().Get(s_deadline);
        sync.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string> { [replica] = "/b" }).Values[replica].Get(s_deadline);
        KafkaFuture syncOffline = sync.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string> { [replica] = "/c" }).Values[replica];
        KafkaException syncFailure = Assert.Throws<KafkaException>(() => syncOffline.Get(s_deadline));
        Assert.Equal(KafkaStorageErrorCode, syncFailure.Code);
        Assert.Equal("Log directory /c is offline", syncFailure.Message);
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo syncInfo =
            sync.DescribeReplicaLogDirs(new[] { replica }).Values[replica].Get(s_deadline);
        Assert.Equal("/a", syncInfo.GetCurrentReplicaLogDir());
        Assert.Equal("/b", syncInfo.GetFutureReplicaLogDir());

        AsyncMockAdminClient async = AsyncMockAdminClient.Create().BrokerLogDirs(logDirs).Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await TestTimeout.Run(() => async.CreateTopics(new[] { new NewTopic("t", 1, 1) }).All().Get(), s_deadline);
        await TestTimeout.Run(
            () => async.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string> { [replica] = "/b" }).Values[replica].Get(),
            s_deadline);
        AsyncKafkaFuture asyncOffline =
            async.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string> { [replica] = "/c" }).Values[replica];
        KafkaException asyncFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncOffline.Get()), s_deadline);
        Assert.Equal(KafkaStorageErrorCode, asyncFailure.Code);
        Assert.Equal("Log directory /c is offline", asyncFailure.Message);
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo asyncInfo = await TestTimeout.Run(
            () => async.DescribeReplicaLogDirs(new[] { replica }).Values[replica].Get(), s_deadline);
        Assert.Equal("/a", asyncInfo.GetCurrentReplicaLogDir());
        Assert.Equal("/b", asyncInfo.GetFutureReplicaLogDir());
    }

    [Fact]
    public async Task DescribeLogDirs_ReportsTheDefaultLogDirOfEachReplica_OnBothMocks()
    {
        int[] brokers = { 0, 1, 2 };
        TopicPartition partition = new TopicPartition("t", 0);

        MockAdminClient sync = Admin();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        sync.CreateTopics(new[] { new NewTopic("t", 1, 2) }).All().Get(s_deadline);
        DescribeLogDirsResult syncResult = sync.DescribeLogDirs(brokers);
        LogDirDescription syncDescription = syncResult.Descriptions[0].Get(s_deadline)[DefaultLogDir];
        Assert.Equal(new[] { partition }, syncDescription.ReplicaInfos.Keys);
        Assert.Null(syncDescription.TotalBytes);
        Assert.Empty(syncResult.Descriptions[2].Get(s_deadline));

        AsyncMockAdminClient async = AsyncAdmin();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);
        await TestTimeout.Run(() => async.CreateTopics(new[] { new NewTopic("t", 1, 2) }).All().Get(), s_deadline);
        AsyncDescribeLogDirsResult asyncResult = async.DescribeLogDirs(brokers);
        LogDirDescription asyncDescription = (await TestTimeout.Run(() => asyncResult.Descriptions[0].Get(), s_deadline))[DefaultLogDir];
        Assert.Equal(new[] { partition }, asyncDescription.ReplicaInfos.Keys);
        Assert.Null(asyncDescription.TotalBytes);
        Assert.Empty(await TestTimeout.Run(() => asyncResult.Descriptions[2].Get(), s_deadline));
    }

    private static MockAdminClient Admin() => MockAdminClient.Create().NumBrokers(3).Build();

    private static AsyncMockAdminClient AsyncAdmin() => AsyncMockAdminClient.Create().NumBrokers(3).Build();

    private static Node Seeded(int id) => new Node(id, "localhost", 1000 + id);

    private static Node Unknown() => new Node(99, "elsewhere", 9999);

    private static void AssertSeeded(IReadOnlyList<int> ids, IReadOnlyCollection<Node> nodes)
    {
        Assert.Equal(ids.Count, nodes.Count);
        int i = 0;
        foreach (Node node in nodes)
        {
            AssertNode(Seeded(ids[i++]), node);
        }
    }

    private static void AssertNode(Node expected, Node? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal((expected.Id, expected.Host, expected.Port), (actual!.Id, actual.Host, actual.Port));
    }
}
