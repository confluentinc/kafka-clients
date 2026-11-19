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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Seek</c>, both forms, and <c>CurrentLag</c> on the mocks and the real consumers: the sought offset seen through
/// <c>Position</c>, the lag, the unassigned-partition faults, the preconditions and the lifecycle.
/// </summary>
/// <remarks>
/// The mock's seek keeps only the offset, so the metadata and leader epoch of <c>Seek(tp, OffsetAndMetadata)</c> are
/// passed to it but not read back.
/// </remarks>
public sealed class PublicConsumerSeekLagTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "seeklag-topic";
    private const int Partition = 0;

    private const string PositionNotAssigned = "You can only check the position for partitions assigned to this consumer.";

    private static async Task<AsyncMockConsumer<byte[], byte[]>> ReadyAssigned(string topic = Topic, int partition = Partition)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        await TestTimeout.Run(
            () => consumer.Assign(new[] { new TopicPartition(topic, partition) }), s_deadline);
        return consumer;
    }

    [Fact]
    public async Task Seek_ToOffset_ObservableViaPosition()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);

        await TestTimeout.Run(() => consumer.Seek(tp, 42L), s_deadline);

        Assert.Equal(42L, await PositionOf(consumer, tp));
    }

    [Theory]
    [InlineData(42, "meta", 7)]       // leader epoch present
    [InlineData(42, "meta", null)]    // leader epoch null → the ABI's -1 "no epoch" sentinel
    [InlineData(7, "café-Ω-🎉", 3)]   // non-ASCII metadata (call-scoped UTF-8 pin)
    [InlineData(11, "", null)]         // empty metadata ("")
    public async Task Seek_WithMetadata_OffsetRoundTrips_MarshallingSucceeds(
        long offset, string metadata, int? leaderEpoch)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);

        await TestTimeout.Run(() => consumer.Seek(tp, new OffsetAndMetadata(offset, leaderEpoch, metadata)), s_deadline);

        Assert.Equal(offset, await PositionOf(consumer, tp));
    }

    [Fact]
    public async Task Seek_WithMetadata_DefaultMetadata_RoundTripsOffset()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);

        await TestTimeout.Run(() => consumer.Seek(tp, new OffsetAndMetadata(99)), s_deadline);

        Assert.Equal(99L, await PositionOf(consumer, tp));
    }

    [Fact]
    public async Task CurrentLag_AssignedWithEndOffset_ReturnsEndMinusPosition()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.UpdateEndOffset(Topic, Partition, 100);
        await TestTimeout.Run(() => consumer.Seek(tp, 10L), s_deadline);

        Assert.Equal(90L, await TestTimeout.Run(() => consumer.CurrentLag(tp), s_deadline));
    }

    [Fact]
    public async Task CurrentLag_AssignedNoEndOffset_ReturnsZero()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);

        Assert.Equal(0L, await TestTimeout.Run(() => consumer.CurrentLag(tp), s_deadline));
    }

    [Fact]
    public async Task CurrentLag_UnassignedPartition_ReturnsZeroOnTheMock()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal(0L, await TestTimeout.Run(() => consumer.CurrentLag(new TopicPartition("unassigned-topic", 0)), s_deadline));
    }

    [Fact]
    public async Task CurrentLag_WaitsForTheBackgroundTask_OnTheAsyncKafkaConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        try
        {
            TopicPartition tp = new TopicPartition(Topic, Partition);
            TopicPartition unassigned = new TopicPartition(Topic, 7);
            await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

            Assert.Null(await TestTimeout.Run(() => consumer.CurrentLag(tp), s_deadline));
            IllegalStateException ex = await Assert.ThrowsAsync<IllegalStateException>(
                () => TestTimeout.Run(() => consumer.CurrentLag(unassigned), s_deadline));
            Assert.Equal($"No current assignment for partition {unassigned}", ex.Message);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public void CurrentLag_WaitsForTheBackgroundTask_OnTheKafkaConsumer()
    {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        try
        {
            TopicPartition tp = new TopicPartition(Topic, Partition);
            TopicPartition unassigned = new TopicPartition(Topic, 7);
            consumer.Assign(new[] { tp });

            Assert.Null(consumer.CurrentLag(tp));
            IllegalStateException ex = Assert.Throws<IllegalStateException>(() => consumer.CurrentLag(unassigned));
            Assert.Equal($"No current assignment for partition {unassigned}", ex.Message);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void CurrentLag_AssignedWithEndOffset_ReturnsEndMinusPosition_OnTheSyncMock()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });
        consumer.UpdateEndOffset(Topic, Partition, 100);
        consumer.Seek(tp, 10L);

        Assert.Equal(90L, consumer.CurrentLag(tp));
    }

    [Fact]
    public void CurrentLag_APositionPastTheEnd_IsANegativeValue_OnTheSyncMock()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });
        consumer.UpdateEndOffset(Topic, Partition, 2);
        consumer.Seek(tp, 5L);

        Assert.Equal(-3L, consumer.CurrentLag(tp));
    }

    [Fact]
    public async Task CurrentLag_APositionPastTheEnd_IsANegativeValue()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.UpdateEndOffset(Topic, Partition, 2);
        await TestTimeout.Run(() => consumer.Seek(tp, 5L), s_deadline);

        Assert.Equal(-3L, await TestTimeout.Run(() => consumer.CurrentLag(tp), s_deadline));
    }

    [Fact]
    public void CurrentLag_AFailingPosition_ThrowsTheCoreError_OnTheSyncMock()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset("unassigned-topic", 0, 2);

        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => consumer.CurrentLag(new TopicPartition("unassigned-topic", 0)));

        Assert.Equal(PositionNotAssigned, ex.Message);
    }

    [Fact]
    public async Task CurrentLag_AFailingPosition_FaultsWithTheCoreError()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset("unassigned-topic", 0, 2);

        IllegalArgumentException ex = await Assert.ThrowsAsync<IllegalArgumentException>(
            () => TestTimeout.Run(() => consumer.CurrentLag(new TopicPartition("unassigned-topic", 0)), s_deadline));

        Assert.Equal(PositionNotAssigned, ex.Message);
    }

    [Fact]
    public void Seek_WithMetadata_ObservableViaPosition_OnTheSyncMock()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.Assign(new[] { tp });

        consumer.Seek(tp, new OffsetAndMetadata(42, 7, "meta"));

        Assert.Equal(42L, consumer.Position(tp));
    }

    [Fact]
    public async Task Seek_UnassignedPartition_ThrowsIllegalState()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IllegalStateException ex = await Assert.ThrowsAsync<IllegalStateException>(
            () => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
        Assert.Equal("No current assignment for partition unassigned-0", ex.Message);
    }

    [Fact]
    public async Task SeekWithMetadata_UnassignedPartition_ThrowsIllegalState()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IllegalStateException ex = await Assert.ThrowsAsync<IllegalStateException>(
            () => consumer.Seek(new TopicPartition("unassigned", 0), new OffsetAndMetadata(0)));
        Assert.Equal("No current assignment for partition unassigned-0", ex.Message);
    }

    // Java's MockConsumer.seek checks no offset: an unassigned partition is refused as unassigned.
    [Fact]
    public async Task Seek_NegativeOffset_OnTheMock_UnassignedPartition_ThrowsIllegalState()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IllegalStateException ex = await Assert.ThrowsAsync<IllegalStateException>(
            () => consumer.Seek(new TopicPartition("t", 0), offset: -1));

        Assert.Equal("No current assignment for partition t-0", ex.Message);
    }

    // Java's MockConsumer.seek checks no offset: an assigned partition accepts a negative one.
    [Fact]
    public async Task Seek_NegativeOffset_OnTheMock_AssignedPartition_IsAccepted()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition("t", 0);
        await consumer.Assign(new[] { tp });

        await consumer.Seek(tp, offset: -1);
    }

    [Fact]
    public async Task Seek_NegativeOffset_OnAClosedConsumer_ReportsTheClosedConsumerFirst()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<IllegalStateException>(() => { _ = consumer.Seek(new TopicPartition("t", 0), offset: -2); });
    }

    [Fact]
    public void Seek_NullTopic_ThrowsIllegalStateAtTheCall()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IllegalStateException ex = Assert.Throws<IllegalStateException>(() => { _ = consumer.Seek(default, 0L); });
        Assert.Equal("No current assignment for partition null-0", ex.Message);
    }

    [Fact]
    public void SeekWithMetadata_NullOffsetAndMetadata_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => { _ = consumer.Seek(new TopicPartition("t", 0), (OffsetAndMetadata)null!); });
        Assert.Equal("offsetAndMetadata", ex.ParamName);
    }

    [Fact]
    public void OffsetAndMetadata_NegativeOffset_ThrowsAtConstruction()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new OffsetAndMetadata(-1));
        Assert.Equal("Invalid negative offset", ex.Message);
    }

    [Fact]
    public async Task Seek_AfterDispose_ThrowsIllegalState()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<IllegalStateException>(() => { _ = consumer.Seek(new TopicPartition("t", 0), 0L); });
    }

    [Fact]
    public async Task SeekWithMetadata_AfterDispose_ThrowsIllegalState()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<IllegalStateException>(
            () => { _ = consumer.Seek(new TopicPartition("t", 0), new OffsetAndMetadata(0)); });
    }

    [Fact]
    public async Task CurrentLag_AfterDispose_ThrowsIllegalState()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<IllegalStateException>(() => { _ = consumer.CurrentLag(new TopicPartition("t", 0)); });
    }

    [Fact]
    public async Task SeekAndCurrentLag_ViaIAsyncConsumer_Work()
    {
        AsyncMockConsumer<byte[], byte[]> mock = await ReadyAssigned();
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IAsyncConsumer<byte[], byte[]> consumer = mock;
        TopicPartition tp = new TopicPartition(Topic, Partition);

        await TestTimeout.Run(() => consumer.Seek(tp, 5L), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(tp, new OffsetAndMetadata(6, 2, "m")), s_deadline);
        long? lag = await TestTimeout.Run(() => consumer.CurrentLag(tp), s_deadline);

        Assert.Equal(6L, await PositionOf(consumer, tp));
        Assert.NotNull(lag);
    }

    private static Dictionary<string, object?> RealConfig() => new Dictionary<string, object?>
    {
        ["bootstrap.servers"] = "127.0.0.1:1",
        ["group.protocol"] = "consumer",
    };

    private static async Task<long> PositionOf(IAsyncConsumer<byte[], byte[]> consumer, TopicPartition partition)
    {
        long result = 0;
        await TestTimeout.Run(async () => result = await consumer.Position(partition), s_deadline);
        return result;
    }
}
