// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The mock consumers' updates in Java's form: <c>UpdateBeginningOffsets</c> and <c>UpdateEndOffsets</c> set every
/// partition of one call, as Java's <c>putAll</c>, and <c>UpdatePartitions</c> registers the given
/// <see cref="PartitionInfo"/> list, which <c>PartitionsFor</c> and <c>ListTopics</c> answer as given. A null the C ABI
/// cannot hold, which Java stores, is refused, and so is a NUL.
/// </summary>
public sealed class PublicMockConsumerUpdateTests
{
    private const string Topic = "topic";

    private const string NulTopicMessage =
        "A topic must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    private const string NulArgumentMessage =
        "A string argument must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);
    private static readonly TopicPartition s_tp1 = new TopicPartition(Topic, 1);
    private static readonly Node s_node1 = new Node(1, "broker-1", 9092, "rack-a");
    private static readonly Node s_node2 = new Node(2, "broker-2", 9093);
    private static readonly Node s_node3 = new Node(3, "broker-3", 9094, "rack-c");

    public static TheoryData<string> Mocks => new TheoryData<string> { "MockConsumer", "AsyncMockConsumer" };

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdateBeginningOffsets_SetsEveryPartitionOfOneCall(string kind)
    {
        using Mock mock = Mock.Create(kind);

        mock.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [s_tp0] = 5, [s_tp1] = 7 });

        Assert.Equal(
            new Dictionary<TopicPartition, long> { [s_tp0] = 5, [s_tp1] = 7 },
            await mock.BeginningOffsets(s_tp0, s_tp1));
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdateEndOffsets_SetsEveryPartitionOfOneCall(string kind)
    {
        using Mock mock = Mock.Create(kind);

        mock.UpdateEndOffsets(new Dictionary<TopicPartition, long> { [s_tp0] = 100, [s_tp1] = 250 });

        Assert.Equal(
            new Dictionary<TopicPartition, long> { [s_tp0] = 100, [s_tp1] = 250 },
            await mock.EndOffsets(s_tp0, s_tp1));
    }

    // Java's putAll replaces the partitions it is given and keeps the others.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdateOffsets_ALaterCall_ReplacesOnlyThePartitionsItNames(string kind)
    {
        using Mock mock = Mock.Create(kind);

        mock.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [s_tp0] = 5, [s_tp1] = 7 });
        mock.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [s_tp1] = 9 });
        mock.UpdateEndOffsets(new Dictionary<TopicPartition, long> { [s_tp0] = 100, [s_tp1] = 250 });
        mock.UpdateEndOffsets(new Dictionary<TopicPartition, long> { [s_tp0] = 120 });

        Assert.Equal(
            new Dictionary<TopicPartition, long> { [s_tp0] = 5, [s_tp1] = 9 },
            await mock.BeginningOffsets(s_tp0, s_tp1));
        Assert.Equal(
            new Dictionary<TopicPartition, long> { [s_tp0] = 120, [s_tp1] = 250 },
            await mock.EndOffsets(s_tp0, s_tp1));
    }

    // Java's TopicPartition and MockConsumer take any partition number.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdateOffsets_ANegativePartition_IsStoredAsJavaStoresIt(string kind)
    {
        using Mock mock = Mock.Create(kind);
        TopicPartition negative = new TopicPartition(Topic, -1);

        mock.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [negative] = 3 });
        mock.UpdateEndOffsets(new Dictionary<TopicPartition, long> { [negative] = 8 });

        Assert.Equal(new Dictionary<TopicPartition, long> { [negative] = 3 }, await mock.BeginningOffsets(negative));
        Assert.Equal(new Dictionary<TopicPartition, long> { [negative] = 8 }, await mock.EndOffsets(negative));
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdatePartitions_PartitionsForAndListTopicsAnswerTheGivenList(string kind)
    {
        using Mock mock = Mock.Create(kind);
        PartitionInfo[] partitions =
        {
            new PartitionInfo(Topic, 0, s_node1, new[] { s_node1, s_node2, s_node3 }, new[] { s_node1, s_node2 }, new[] { s_node3 }),
            new PartitionInfo(Topic, 1, null, new[] { s_node2 }, Array.Empty<Node>(), new[] { s_node2 }),
        };
        PartitionInfo[] other = { new PartitionInfo("other", 0, s_node3, new[] { s_node3 }, new[] { s_node3 }) };

        mock.UpdatePartitions(Topic, partitions);
        mock.UpdatePartitions("other", other);

        Assert.Equal(partitions, await mock.PartitionsFor(Topic));
        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> topics = await mock.ListTopics();
        Assert.Equal(partitions, topics[Topic]);
        Assert.Equal(other, topics["other"]);
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdatePartitions_ALaterCall_ReplacesTheTopicsList(string kind)
    {
        using Mock mock = Mock.Create(kind);
        PartitionInfo[] replacement = { new PartitionInfo(Topic, 0, s_node2, new[] { s_node2 }, new[] { s_node2 }) };

        mock.UpdatePartitions(Topic, TestPartitions.Led(Topic, 3, 1, "broker-1", 9092));
        mock.UpdatePartitions(Topic, replacement);

        Assert.Equal(replacement, await mock.PartitionsFor(Topic));
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdatePartitions_AnEmptyList_IsAnsweredEmpty(string kind)
    {
        using Mock mock = Mock.Create(kind);

        mock.UpdatePartitions(Topic, Array.Empty<PartitionInfo>());

        Assert.Empty(await mock.PartitionsFor(Topic));
    }

    // Java stores the list under the topic it is given, each PartitionInfo keeping its own topic.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task UpdatePartitions_EachPartitionInfoKeepsItsOwnTopic(string kind)
    {
        using Mock mock = Mock.Create(kind);
        PartitionInfo[] partitions = { new PartitionInfo("elsewhere", 4, s_node1, new[] { s_node1 }, new[] { s_node1 }) };

        mock.UpdatePartitions(Topic, partitions);

        Assert.Equal(partitions, await mock.PartitionsFor(Topic));
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public void UpdatePartitions_RefusesANullTheNativeClientCannotHold(string kind)
    {
        using Mock mock = Mock.Create(kind);
        Node[] nodes = { s_node1 };

        Assert.Equal("topic", Assert.Throws<ArgumentNullException>(() => mock.UpdatePartitions(null!, Array.Empty<PartitionInfo>())).ParamName);
        Assert.Equal("partitions", Assert.Throws<ArgumentNullException>(() => mock.UpdatePartitions(Topic, null!)).ParamName);
        AssertNullIn("partitions must not contain null", () => mock.UpdatePartitions(Topic, new PartitionInfo[] { null! }));
        AssertNullIn(
            "partitions must not contain a PartitionInfo with a null topic",
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(null!, 0, s_node1, nodes, nodes) }));
        AssertNullIn(
            "partitions must not contain a PartitionInfo with null replicas",
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(Topic, 0, s_node1, null!, nodes) }));
        AssertNullIn(
            "partitions must not contain a PartitionInfo with null inSyncReplicas",
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(Topic, 0, s_node1, nodes, null!) }));
        AssertNullIn(
            "partitions must not contain a PartitionInfo with null offlineReplicas",
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(Topic, 0, s_node1, nodes, nodes, null!) }));
        AssertNullIn(
            "partitions must not contain a PartitionInfo with a null node",
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(Topic, 0, s_node1, new Node[] { null! }, nodes) }));
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public void UpdatePartitions_RefusesANulInAPartition(string kind)
    {
        using Mock mock = Mock.Create(kind);
        Node[] nodes = { s_node1 };

        AssertNul(
            NulTopicMessage,
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo("to\0pic", 0, s_node1, nodes, nodes) }));
        AssertNul(
            NulArgumentMessage,
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(Topic, 0, new Node(1, "h\0st", 9092), nodes, nodes) }));
        AssertNul(
            NulArgumentMessage,
            () => mock.UpdatePartitions(Topic, new[] { new PartitionInfo(Topic, 0, null, new[] { new Node(2, "host", 9093, "ra\0ck") }, nodes) }));
    }

    private static void AssertNullIn(string message, Action action)
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(action);
        Assert.Equal("partitions", e.ParamName);
        Assert.Equal(new ArgumentNullException("partitions", message).Message, e.Message);
    }

    private static void AssertNul(string message, Action action)
    {
        ArgumentException e = Assert.Throws<ArgumentException>(action);
        Assert.Equal("partitions", e.ParamName);
        Assert.Equal(new ArgumentException(message, "partitions").Message, e.Message);
    }

    /// <summary>One of the two mock consumers, behind the calls these tests make.</summary>
    private sealed class Mock : IDisposable
    {
        private readonly MockConsumer<byte[], byte[]>? _sync;
        private readonly AsyncMockConsumer<byte[], byte[]>? _async;

        private Mock(MockConsumer<byte[], byte[]>? sync, AsyncMockConsumer<byte[], byte[]>? async)
        {
            _sync = sync;
            _async = async;
        }

        internal static Mock Create(string kind) =>
            kind == "MockConsumer"
                ? new Mock(new MockConsumer<byte[], byte[]>("earliest"), null)
                : new Mock(null, new AsyncMockConsumer<byte[], byte[]>("earliest"));

        internal void UpdateBeginningOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets)
        {
            if (_sync is not null)
            {
                _sync.UpdateBeginningOffsets(newOffsets);
            }
            else
            {
                _async!.UpdateBeginningOffsets(newOffsets);
            }
        }

        internal void UpdateEndOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets)
        {
            if (_sync is not null)
            {
                _sync.UpdateEndOffsets(newOffsets);
            }
            else
            {
                _async!.UpdateEndOffsets(newOffsets);
            }
        }

        internal void UpdatePartitions(string topic, IReadOnlyList<PartitionInfo> partitions)
        {
            if (_sync is not null)
            {
                _sync.UpdatePartitions(topic, partitions);
            }
            else
            {
                _async!.UpdatePartitions(topic, partitions);
            }
        }

        internal async Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(params TopicPartition[] partitions) =>
            _sync is not null ? _sync.BeginningOffsets(partitions) : await _async!.BeginningOffsets(partitions);

        internal async Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(params TopicPartition[] partitions) =>
            _sync is not null ? _sync.EndOffsets(partitions) : await _async!.EndOffsets(partitions);

        internal async Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic) =>
            _sync is not null ? _sync.PartitionsFor(topic) : await _async!.PartitionsFor(topic);

        internal async Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics() =>
            _sync is not null ? _sync.ListTopics() : await _async!.ListTopics();

        public void Dispose() => TestTimeout.Dispose(_sync is not null ? _sync : _async);
    }
}
