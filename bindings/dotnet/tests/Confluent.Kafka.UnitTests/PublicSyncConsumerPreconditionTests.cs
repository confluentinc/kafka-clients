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
/// M5/P8a — preconditions (validated <b>before any native call</b>, ffi §B5) and their exact
/// message content (DoD §3) for the <b>synchronous</b> <see cref="IConsumer"/> surface, plus
/// the unassigned-partition synchronous <see cref="KafkaException"/>. Each argument check
/// fires before the P/Invoke — asserted even when the consumer is closed (the argument check
/// precedes the disposed check).
/// </summary>
/// <remarks>
/// <b>Concurrent-use is a documented mock limit (not a flaky test).</b> A concurrent op from
/// another thread while one is in flight is rejected by the core's access guard (a
/// ConcurrentModification <see cref="KafkaException"/>, thrown synchronously — ffi §B5). But
/// the mock poll runs to completion synchronously (source-verified,
/// <c>src/consumer/mock_consumer.rs</c>) and holds the guard only for that instant, so a second
/// op cannot deterministically observe the guard held — the same ceiling the async M5 phases
/// recorded. The mapping (concurrent sync op → synchronous ConcurrentModification
/// <see cref="KafkaException"/>; concurrent sync state read → <see cref="InvalidOperationException"/>)
/// is verified by inspection of the core's <c>acquire</c> path; no flaky overlap test is shipped.
/// </remarks>
public sealed class PublicSyncConsumerPreconditionTests
{
    private const string Topic = "sync-precondition-topic";
    private const int Partition = 0;

    // ---- Null collections / maps → ArgumentNullException ----

    [Fact]
    public void Subscribe_NullTopics_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Subscribe(null!));
        Assert.Equal("topics", ex.ParamName);
    }

    [Fact]
    public void Subscribe_NullElementTopic_ThrowsArgumentException()
    {
        using MockConsumer consumer = new MockConsumer();
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
        using MockConsumer consumer = new MockConsumer();
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
        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way to
        // present a null element topic without the TopicPartition ctor validation firing.
        using MockConsumer consumer = new MockConsumer();
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => InvokePartitionOp(consumer, op, new[] { default(TopicPartition) }));
        Assert.Equal("partitions", ex.ParamName);
        Assert.Contains("Topic names must not be null.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Commit_NullOffsets_ThrowsArgumentNull()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Commit(null!));
        Assert.Equal("offsets", ex.ParamName);
    }

    [Fact]
    public void Commit_NullOffsetValue_ThrowsArgumentException()
    {
        using MockConsumer consumer = new MockConsumer();
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
        using MockConsumer consumer = new MockConsumer();
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => consumer.Position(default));
        Assert.Equal("partition", ex.ParamName);
    }

    // ---- Negative partition → ArgumentOutOfRangeException (the TopicPartition ctor guard) ----

    [Fact]
    public void TopicPartition_NegativePartition_ThrowsArgumentOutOfRange()
    {
        // A negative partition cannot reach the sync ops through a constructed TopicPartition —
        // the ctor rejects it first (the shipped precondition, exact message).
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new TopicPartition("t", -1));
        Assert.Equal("partition", ex.ParamName);
        Assert.Contains("Partition must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    // ---- Negative timeout → ArgumentOutOfRangeException, before any native call (even closed) ----

    [Fact]
    public void Poll_NegativeTimeout_ThrowsArgumentOutOfRange()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Poll(TimeSpan.FromMilliseconds(-1)));
        Assert.Equal("timeout", ex.ParamName);
        Assert.Contains("Timeout must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Poll_NegativeTimeout_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // The timeout precondition precedes the disposed check — a closed consumer still throws
        // ArgumentOutOfRangeException, not ObjectDisposedException.
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Poll(TimeSpan.FromSeconds(-2)));
        Assert.Equal("timeout", ex.ParamName);
    }

    [Fact]
    public void Close_NegativeTimeout_ThrowsArgumentOutOfRange()
    {
        using MockConsumer consumer = new MockConsumer();
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Close(TimeSpan.FromMilliseconds(-1)));
        Assert.Equal("timeout", ex.ParamName);
        Assert.Contains("Timeout must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Close_NegativeTimeout_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Close(TimeSpan.FromSeconds(-3)));
        Assert.Equal("timeout", ex.ParamName);
    }

    [Fact]
    public void Close_ZeroTimeout_IsValid()
    {
        // TimeSpan.Zero is a valid close timeout (an immediate best-effort close), not rejected.
        MockConsumer consumer = new MockConsumer();
        consumer.Close(TimeSpan.Zero);
    }

    [Fact]
    public void Subscribe_NullTopics_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // The null-argument check precedes the disposed check.
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();

        Assert.Throws<ArgumentNullException>(() => consumer.Subscribe(null!));
    }

    // ---- Unassigned partition → synchronous KafkaException ----

    [Fact]
    public void Position_UnassignedPartition_ThrowsKafkaException()
    {
        using MockConsumer consumer = new MockConsumer();
        Assert.Throws<KafkaException>(() => consumer.Position(new TopicPartition("unassigned", 0)));
    }

    [Fact]
    public void Seek_UnassignedPartition_ThrowsKafkaException()
    {
        // Seek is sync (M5/P7): an unassigned partition surfaces as a SYNCHRONOUS KafkaException.
        using MockConsumer consumer = new MockConsumer();
        Assert.Throws<KafkaException>(() => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
    }

    [Fact]
    public void Pause_UnassignedPartition_ThrowsKafkaException()
    {
        // Pause of an unassigned partition is the one partition op with a deterministic
        // broker-free failure ("No current assignment for partition …").
        using MockConsumer consumer = new MockConsumer();
        KafkaException ex = Assert.Throws<KafkaException>(
            () => consumer.Pause(new[] { new TopicPartition("unassigned", 0) }));
        Assert.Contains("No current assignment for partition", ex.Message, StringComparison.Ordinal);
    }

    private static void InvokePartitionOp(IConsumer consumer, string op, IReadOnlyCollection<TopicPartition> partitions)
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
