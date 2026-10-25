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

    [Fact]
    public void Subscribe_NullTopics_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Subscribe(null!));
        Assert.Equal("topics", ex.ParamName);
    }

    [Fact]
    public void Subscribe_NullElementTopic_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => InvokePartitionOp(consumer, op, null!));
        Assert.Equal("partitions", ex.ParamName);
    }

    [Theory]
    [InlineData("assign")]
    [InlineData("pause")]
    [InlineData("resume")]
    [InlineData("seekToBeginning")]
    [InlineData("seekToEnd")]
    public void PartitionOps_NullElementTopic_ThrowArgumentException(string op)
    {
        // default(TopicPartition) has a null Topic, as a TopicPartition built with a null topic does.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => InvokePartitionOp(consumer, op, new[] { default(TopicPartition) }));
        Assert.Equal("partitions", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Commit_NullOffsets_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Commit(null!));
        Assert.Equal("offsets", ex.ParamName);
    }

    [Fact]
    public void Commit_NullOffsetValue_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, Partition)] = null!,
        };

        ArgumentException ex = Assert.Throws<ArgumentException>(() => consumer.Commit(offsets));
        Assert.Equal("offsets", ex.ParamName);
        Assert.Contains("Offset value must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Position_NullTopic_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Position(default));
        Assert.Equal("partition", ex.ParamName);
    }

    [Fact]
    public void Poll_NegativeTimeout_ThrowsArgumentOutOfRange()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Poll(TimeSpan.FromMilliseconds(-1)));
        Assert.Equal("timeout", ex.ParamName);
        Assert.StartsWith("Invalid negative timeout -1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Poll_NegativeTimeout_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Poll(TimeSpan.FromSeconds(-2)));
        Assert.Equal("timeout", ex.ParamName);
    }

    [Fact]
    public void Subscribe_NullTopics_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ArgumentNullException>(() => consumer.Subscribe(null!));
    }

    [Fact]
    public void Position_UnassignedPartition_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ArgumentException ex = Assert.Throws<ArgumentException>(() => consumer.Position(new TopicPartition("unassigned", 0)));
        Assert.Equal("You can only check the position for partitions assigned to this consumer.", ex.Message);
    }

    [Fact]
    public void Seek_UnassignedPartition_ThrowsInvalidOperationException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
        Assert.Equal("No current assignment for partition unassigned-0", ex.Message);
    }

    [Fact]
    public void Pause_UnassignedPartition_ThrowsInvalidOperationException()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
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
