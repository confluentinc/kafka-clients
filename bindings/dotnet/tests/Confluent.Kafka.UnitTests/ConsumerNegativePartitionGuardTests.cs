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
/// The consumer's own "Partition must not be negative." preconditions, one test per guard
/// family — reachable through the public API only since M15/P13.2 G3-4 made
/// <see cref="TopicPartition"/>'s constructor store a negative partition as Java's does
/// (<c>TopicPartition.java:32-35</c>). Until then the constructor threw first, so these guards
/// had no public-surface test at all.
/// </summary>
/// <remarks>
/// <para>
/// The guards are kept deliberately, stricter than Java (M15/P13.2 D11: Java's <c>assign</c>
/// accepts a negative partition, and the consumer ABI passes one straight to the core). Each
/// test asserts the exception type, <see cref="ArgumentException.ParamName"/>,
/// <see cref="ArgumentOutOfRangeException.ActualValue"/> and the exact message (DoD §3).
/// </para>
/// <para>
/// <b>"No native call" is witnessed, not assumed.</b> Every guard on a consumer runs before
/// <c>ThrowIfClosed</c>, the gate in front of every native call, so on a <em>disposed</em>
/// consumer a negative partition still surfaces as <see cref="ArgumentOutOfRangeException"/>:
/// had the check run later — or not at all — the result would be
/// <see cref="ObjectDisposedException"/>. On a live consumer the witnesses are the native
/// outcomes the guard pre-empts: <c>Assign</c> would assign the partition (the core accepts a
/// negative one, as Java does), and a <see cref="ConsumerHandle"/> op on a partition this
/// consumer does not own would report a <see cref="KafkaException"/> from the core.
/// </para>
/// </remarks>
public sealed class ConsumerNegativePartitionGuardTests
{
    private const string Topic = "neg-guard";

    private const string NegativePartition = "Partition must not be negative.";

    private static readonly TopicPartition s_negative = new TopicPartition(Topic, -1);

    [Fact]
    public void SnapshotPartitions_Assign_RejectsANegativePartition_AndAssignsNothing()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(
                () => consumer.Assign(new[] { new TopicPartition(Topic, 0), s_negative })),
            "partitions");

        // Without the guard the core would have assigned both partitions.
        Assert.Empty(consumer.Assignment());

        // The same shared guard backs the collection-input queries.
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
        disposed.Dispose();

        // Thrown synchronously from the call, before any operation is submitted.
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = disposed.Assign(new[] { s_negative }); }),
            "partitions");
    }

    [Fact]
    public void SnapshotCommitOffsets_Commit_RejectsANegativePartition_BeforeAnyNativeCall()
    {
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_negative] = new OffsetAndMetadata(1) };

        using MockConsumer<byte[], byte[]> consumer = NewMock();
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Commit(offsets)), "offsets");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => disposed.Commit(offsets)), "offsets");
    }

    [Fact]
    public void SnapshotTimestamps_OffsetsForTimes_RejectsANegativePartition_BeforeAnyNativeCall()
    {
        Dictionary<TopicPartition, long> timestamps = new Dictionary<TopicPartition, long> { [s_negative] = 0 };

        using MockConsumer<byte[], byte[]> consumer = NewMock();
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => consumer.OffsetsForTimes(timestamps)),
            "timestampsToSearch");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => disposed.OffsetsForTimes(timestamps)),
            "timestampsToSearch");
    }

    [Fact]
    public void Seek_SeekWithMetadata_CurrentLag_RejectANegativePartition_BeforeAnyNativeCall()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Seek(s_negative, 0)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Seek(s_negative, new OffsetAndMetadata(0))),
            "partition");
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.CurrentLag(s_negative)), "partition");

        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => disposed.Seek(s_negative, 0)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => disposed.Seek(s_negative, new OffsetAndMetadata(0))),
            "partition");
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => disposed.CurrentLag(s_negative)), "partition");
    }

    [Fact]
    public void Position_SyncAndAsync_RejectANegativePartition_BeforeAnyNativeCall()
    {
        MockConsumer<byte[], byte[]> disposed = NewDisposedMock();
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => disposed.Position(s_negative)), "partition");

        AsyncMockConsumer<byte[], byte[]> disposedAsync = new AsyncMockConsumer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray);
        disposedAsync.Dispose();

        // Thrown synchronously from the call, before any operation is submitted.
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = disposedAsync.Position(s_negative); }),
            "partition");
    }

    [Fact]
    public void ConsumerHandle_SeekAndPosition_RejectANegativePartition_BeforeAnyNativeCall()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        // Each of these would reach the core (and fail there with a KafkaException for a
        // partition this consumer does not own) if the guard did not run first.
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Seek(s_negative, 0)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => handle.Seek(s_negative, new OffsetAndMetadata(0))),
            "partition");
        AssertGuard(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_negative)), "partition");
        AssertGuard(
            Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_negative, TimeSpan.FromSeconds(1))),
            "partition");
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
        consumer.Dispose();
        return consumer;
    }
}
