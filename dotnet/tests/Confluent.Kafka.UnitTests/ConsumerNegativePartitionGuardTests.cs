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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The consumer's "Partition must not be negative." preconditions, one test per guard family, each asserting the
/// type, <see cref="ArgumentException.ParamName"/>, <see cref="ArgumentOutOfRangeException.ActualValue"/> and the
/// message.
/// </summary>
/// <remarks>
/// The guards are stricter than Java, whose assign accepts a negative partition. Assign checks its partitions before
/// the closed check, so a disposed consumer proves its guard runs before the native call; every other call makes the
/// closed check first, as Python's consumer does, so for it the witness is the live outcome the guard pre-empts.
/// </remarks>
public sealed class ConsumerNegativePartitionGuardTests
{
    private const string Topic = "neg-guard";

    private const string NegativePartition = "Partition must not be negative.";

    private static readonly TopicPartition s_negative = new TopicPartition(Topic, -1);

    [Fact]
    public void SnapshotPartitions_Assign_RejectsANegativePartition_AndAssignsNothing()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(
                () => consumer.Assign(new[] { new TopicPartition(Topic, 0), s_negative })),
            "partitions");

        // Without the guard the core would have assigned both partitions.
        Assert.Empty(consumer.Assignment());

        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Committed(new[] { s_negative })),
            "partitions");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => disposed.Assign(new[] { s_negative })),
            "partitions");
    }

    [Fact]
    public void SnapshotPartitions_AsyncAssign_RejectsANegativePartition_BeforeAnySubmit()
    {
        AsyncMockConsumer<byte[], byte[]> disposed = new AsyncMockConsumer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(disposed);

        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = disposed.Assign(new[] { s_negative }); }),
            "partitions");
    }

    [Fact]
    public void SnapshotCommitOffsets_Commit_RejectsANegativePartition_BeforeAnyNativeCall()
    {
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_negative] = new OffsetAndMetadata(1) };

        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Commit(offsets)), "offsets");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        Assert.Throws<ObjectDisposedException>(() => disposed.Commit(offsets));
    }

    [Fact]
    public void SnapshotTimestamps_OffsetsForTimes_RejectsANegativePartition_BeforeAnyNativeCall()
    {
        Dictionary<TopicPartition, long> timestamps = new Dictionary<TopicPartition, long> { [s_negative] = 0 };

        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => consumer.OffsetsForTimes(timestamps)),
            "timestampsToSearch");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        Assert.Throws<ObjectDisposedException>(() => disposed.OffsetsForTimes(timestamps));
    }

    [Fact]
    public void Seek_SeekWithMetadata_CurrentLag_RejectANegativePartition_BeforeAnyNativeCall()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Seek(s_negative, 0)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Seek(s_negative, new OffsetAndMetadata(0))),
            "partition");
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.CurrentLag(s_negative)), "partition");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        Assert.Throws<ObjectDisposedException>(() => disposed.Seek(s_negative, 0));
        Assert.Throws<ObjectDisposedException>(() => disposed.Seek(s_negative, new OffsetAndMetadata(0)));
        Assert.Throws<ObjectDisposedException>(() => disposed.CurrentLag(s_negative));
    }

    [Fact]
    public void Position_SyncAndAsync_RejectANegativePartition_BeforeAnyNativeCall()
    {
        // Without the guard the mock would answer that the partition is not assigned.
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Position(s_negative)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Position(s_negative, TimeSpan.FromSeconds(1))),
            "partition");

        AsyncMockConsumer<byte[], byte[]> asyncConsumer = new AsyncMockConsumer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable asyncDisposal = TestTimeout.Disposal(asyncConsumer);
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = asyncConsumer.Position(s_negative); }),
            "partition");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        Assert.Throws<ObjectDisposedException>(() => disposed.Position(s_negative));
    }

    [Fact]
    public void ConsumerHandle_SeekPositionAndCurrentLag_RejectANegativePartition_BeforeAnyNativeCall()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        // Without the guard the core would answer each of these on a mock handle, whatever the partition.
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Seek(s_negative, 0)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => handle.Seek(s_negative, new OffsetAndMetadata(0))),
            "partition");
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_negative)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_negative, TimeSpan.FromSeconds(1))),
            "partition");
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => handle.CurrentLag(s_negative)), "partition");
    }

    private static void AssertGuard(ArgumentOutOfRangeException error, string paramName)
    {
        Assert.Equal(paramName, error.ParamName);
        Assert.Equal(-1, error.ActualValue);
        Assert.StartsWith(NegativePartition, error.Message, StringComparison.Ordinal);
    }

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static MockConsumer<byte[], byte[]> NewDisposedMock()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        TestTimeout.Dispose(consumer);
        return consumer;
    }
}
