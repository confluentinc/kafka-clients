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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The preconditions of the sync <see cref="IConsumer{TKey, TValue}"/> and their messages, each checked before the
/// native call and before the closed check, and the errors of Position, Seek and Pause on an unassigned partition.
/// </summary>
public sealed class PublicSyncConsumerPreconditionTests
{
    private const string Topic = "sync-precondition-topic";
    private const int Partition = 0;

    [Theory]
    [InlineData(nameof(KafkaConsumer<byte[], byte[]>))]
    [InlineData(nameof(AsyncKafkaConsumer<byte[], byte[]>))]
    [InlineData(nameof(MockConsumer<byte[], byte[]>))]
    [InlineData(nameof(AsyncMockConsumer<byte[], byte[]>))]
    public void Constructor_NullDeserializer_ThrowsJavasConfigException(string consumer)
    {
        Dictionary<string, object?> config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
        };
        Func<IDeserializer<byte[]>?, IDeserializer<byte[]>?, object> create = consumer switch
        {
            nameof(KafkaConsumer<byte[], byte[]>) => (k, v) => new KafkaConsumer<byte[], byte[]>(config, k!, v!),
            nameof(AsyncKafkaConsumer<byte[], byte[]>) => (k, v) => new AsyncKafkaConsumer<byte[], byte[]>(config, k!, v!),
            nameof(MockConsumer<byte[], byte[]>) => (k, v) => new MockConsumer<byte[], byte[]>(k!, v!),
            _ => (k, v) => new AsyncMockConsumer<byte[], byte[]>(k!, v!),
        };

        ConfigException key = Assert.Throws<ConfigException>(() => create(null, null));
        Assert.Equal("Invalid value null for configuration key.deserializer: must be non-null.", key.Message);
        ConfigException value = Assert.Throws<ConfigException>(() => create(Serdes.ByteArray().Deserializer(), null));
        Assert.Equal("Invalid value null for configuration value.deserializer: must be non-null.", value.Message);
    }

    [Fact]
    public void Subscribe_NullTopics_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Subscribe((IReadOnlyCollection<string>)null!));
        Assert.Equal("topics", ex.ParamName);
    }

    [Fact]
    public void Subscribe_NullElementTopic_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentException ex = Assert.Throws<ArgumentException>(() => consumer.Subscribe(new string[] { null! }));
        Assert.Equal("topics", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("assign")]
    [InlineData("pause")]
    [InlineData("resume")]
    [InlineData("seekToBeginning")]
    [InlineData("seekToEnd")]
    public void PartitionOps_NullCollection_ThrowArgumentNull(string op)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => InvokePartitionOp(consumer, op, null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Theory]
    [InlineData("assign")]
    [InlineData("seekToBeginning")]
    [InlineData("seekToEnd")]
    public void PartitionOps_NullElementTopic_ThrowArgumentException(string op)
    {
        // default(TopicPartition) has a null Topic, as a TopicPartition built with a null topic does.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => InvokePartitionOp(consumer, op, new[] { default(TopicPartition) }));
        Assert.Equal("partitions", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Commit_NullOffsets_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Commit(null!));
        Assert.Equal("offsets", ex.ParamName);
    }

    [Fact]
    public void Commit_NullOffsetValue_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, Partition)] = null!,
        };

        ArgumentException ex = Assert.Throws<ArgumentException>(() => consumer.Commit(offsets));
        Assert.Equal("offsets", ex.ParamName);
        Assert.Contains("Offset value must not be null.", ex.Message, StringComparison.Ordinal);
    }

    // Java's SubscriptionState.assignedState throws for a partition that is not assigned, which a null topic never is.
    [Theory]
    [InlineData("pause")]
    [InlineData("resume")]
    public void PauseAndResume_NullElementTopic_ThrowIllegalState(string op)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IllegalStateException ex = Assert.Throws<IllegalStateException>(
            () => InvokePartitionOp(consumer, op, new[] { new TopicPartition(null!, 3) }));
        Assert.Equal("No current assignment for partition null-3", ex.Message);
    }

    // Java's MockConsumer.position throws IllegalArgumentException for a partition that is not assigned.
    [Fact]
    public void Position_NullTopic_ThrowsIllegalArgument()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(() => consumer.Position(default));
        Assert.Equal("You can only check the position for partitions assigned to this consumer.", ex.Message);
    }

    [Fact]
    public void Poll_NegativeTimeout_ThrowsIllegalArgument()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => consumer.Poll(TimeSpan.FromMilliseconds(-1)));
        Assert.Equal("Invalid negative timeout -1", ex.Message);
    }

    [Fact]
    public void Poll_NegativeTimeout_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        TestTimeout.Dispose(consumer);

        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => consumer.Poll(TimeSpan.FromSeconds(-2)));
        Assert.Equal("Invalid negative timeout -2000", ex.Message);
    }

    [Fact]
    public void Subscribe_NullTopics_WhenClosed_ReportsTheClosedConsumerFirst()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        TestTimeout.Dispose(consumer);

        IllegalStateException ex = Assert.Throws<IllegalStateException>(() => consumer.Subscribe((IReadOnlyCollection<string>)null!));
        Assert.Equal("This consumer has already been closed.", ex.Message);
    }

    [Fact]
    public void Position_UnassignedPartition_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(() => consumer.Position(new TopicPartition("unassigned", 0)));
        Assert.Equal("You can only check the position for partitions assigned to this consumer.", ex.Message);
    }

    [Fact]
    public void Seek_UnassignedPartition_ThrowsIllegalState()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IllegalStateException ex = Assert.Throws<IllegalStateException>(
            () => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
        Assert.Equal("No current assignment for partition unassigned-0", ex.Message);
    }

    [Fact]
    public void Pause_UnassignedPartition_ThrowsIllegalState()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IllegalStateException ex = Assert.Throws<IllegalStateException>(
            () => consumer.Pause(new[] { new TopicPartition("unassigned", 0) }));
        Assert.Equal("No current assignment for partition unassigned-0", ex.Message);
    }

    private static void InvokePartitionOp(IConsumer<byte[], byte[]> consumer, string op, IReadOnlyCollection<TopicPartition> partitions)
    {
        switch (op)
        {
            case "assign":
                consumer.Assign(partitions);
                break;
            case "pause":
                consumer.Pause(partitions);
                break;
            case "resume":
                consumer.Resume(partitions);
                break;
            case "seekToBeginning":
                consumer.SeekToBeginning(partitions);
                break;
            case "seekToEnd":
                consumer.SeekToEnd(partitions);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(op), op, "Unknown partition op.");
        }
    }
}
