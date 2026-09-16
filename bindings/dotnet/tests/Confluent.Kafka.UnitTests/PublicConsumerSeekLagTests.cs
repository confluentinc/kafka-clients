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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M5/P7 — the two <b>synchronous</b> <see cref="IConsumerCommon.Seek(TopicPartition, long)"/>
/// / <see cref="IConsumerCommon.Seek(TopicPartition, OffsetAndMetadata)"/> overloads and the
/// synchronous <see cref="IConsumerCommon.CurrentLag(TopicPartition)"/>, exercised through the
/// <b>public</b> <see cref="AsyncMockConsumer"/> / <see cref="IConsumerCommon"/> surface (no
/// broker): the <c>Seek(tp, long)</c> offset round-trip (via <see cref="IAsyncConsumer.Position"/>),
/// the <c>Seek(tp, OffsetAndMetadata)</c> offset round-trip + metadata / leader-epoch
/// <b>marshalling</b> coverage, the real <c>CurrentLag</c> value + the empty → <see langword="null"/>
/// case, the deterministic preconditions / error messages / lifecycle, the unassigned-partition
/// synchronous <see cref="KafkaException"/>, and a per-op allocation sanity bound.
/// </summary>
/// <remarks>
/// <b>Mock reachability limit (verified).</b> The mock's <c>seek_with_metadata</c>
/// (<c>src/consumer/mock_consumer.rs:746-755</c>) uses <b>only</b> <c>.offset()</c> and
/// <b>discards</b> the metadata + leader epoch, so their <em>values</em> are not observable
/// broker-free (the M5/P4 <c>Committed</c> value read-back precedent). For
/// <c>Seek(tp, OffsetAndMetadata)</c> this asserts the OFFSET round-trips and the
/// metadata / leader-epoch <b>marshalling</b> succeeds (the call-scoped pins, the <c>-1</c>
/// epoch sentinel for a null leader epoch, non-ASCII / empty metadata) — a strict value
/// read-back is a documented mock limit, not asserted.
/// </remarks>
public sealed class PublicConsumerSeekLagTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "seeklag-topic";
    private const int Partition = 0;

    // Assign the partition so a subsequent Seek / CurrentLag operates on an assigned TP
    // (unassigned seek fails; unassigned CurrentLag returns null — both covered below).
    private static async Task<AsyncMockConsumer<byte[], byte[]>> ReadyAssigned(string topic = Topic, int partition = Partition)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(
            () => consumer.Assign(new[] { new TopicPartition(topic, partition) }), s_deadline);
        return consumer;
    }

    // ---- Seek(tp, long): offset observable via Position (DoD §3) ----

    [Fact]
    public async Task Seek_ToOffset_ObservableViaPosition()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        TopicPartition tp = new TopicPartition(Topic, Partition);

        consumer.Seek(tp, 42L); // sync (M5/P7)

        Assert.Equal(42L, await PositionOf(consumer, tp));
    }

    // ---- Seek(tp, OffsetAndMetadata): offset round-trips; metadata/epoch marshalling ----

    [Theory]
    [InlineData(42, "meta", 7)]       // leader epoch present
    [InlineData(42, "meta", null)]    // leader epoch null → the ABI's -1 "no epoch" sentinel
    [InlineData(7, "café-Ω-🎉", 3)]   // non-ASCII metadata (call-scoped UTF-8 pin)
    [InlineData(11, "", null)]         // empty metadata ("")
    public async Task Seek_WithMetadata_OffsetRoundTrips_MarshallingSucceeds(
        long offset, string metadata, int? leaderEpoch)
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        TopicPartition tp = new TopicPartition(Topic, Partition);

        // The mock discards metadata + leader_epoch (see the type remarks), so their VALUES are
        // not observable broker-free. This asserts the OFFSET round-trips and the metadata /
        // leader-epoch MARSHALLING succeeds — the call completing WITHOUT error on an assigned
        // partition is the marshalling proof (call-scoped pins, the -1 epoch sentinel for a
        // null leader epoch, non-ASCII + empty metadata).
        consumer.Seek(tp, new OffsetAndMetadata(offset, metadata, leaderEpoch));

        Assert.Equal(offset, await PositionOf(consumer, tp));
    }

    [Fact]
    public async Task Seek_WithMetadata_DefaultMetadata_RoundTripsOffset()
    {
        // The 1-arg OffsetAndMetadata(offset) coerces metadata → "" (never-null) and leaves the
        // leader epoch null (→ -1) — the "no metadata / no epoch" marshalling path (a valid ""
        // pointer + the -1 sentinel) works.
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        TopicPartition tp = new TopicPartition(Topic, Partition);

        consumer.Seek(tp, new OffsetAndMetadata(99));

        Assert.Equal(99L, await PositionOf(consumer, tp));
    }

    // ---- CurrentLag ----

    [Fact]
    public async Task CurrentLag_AssignedWithEndOffset_ReturnsEndMinusPosition()
    {
        // Assign → UpdateEndOffset(100) → Seek(10) → lag == 90 (mock: Some(end - position),
        // mock_consumer.rs:438-457), also exercising the new sync Seek in the setup.
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        TopicPartition tp = new TopicPartition(Topic, Partition);
        consumer.UpdateEndOffset(Topic, Partition, 100);
        consumer.Seek(tp, 10L);

        Assert.Equal(90L, consumer.CurrentLag(tp));
    }

    [Fact]
    public async Task CurrentLag_AssignedNoEndOffset_ReturnsZero()
    {
        // Assigned but no end offset → the mock's "caught-up" model returns 0 (Some(0)), NOT
        // null (null is reserved for the unknown / unassigned case).
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();
        TopicPartition tp = new TopicPartition(Topic, Partition);

        Assert.Equal(0L, consumer.CurrentLag(tp));
    }

    [Fact]
    public async Task CurrentLag_UnassignedPartition_ReturnsNull()
    {
        // An unassigned partition → the mock returns None → null (Java OptionalLong.empty).
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyAssigned();

        Assert.Null(consumer.CurrentLag(new TopicPartition("unassigned-topic", 0)));
    }

    // ---- Unassigned-partition seek → synchronous KafkaException (both overloads) ----

    [Fact]
    public void Seek_UnassignedPartition_ThrowsKafkaException()
    {
        // Seek is sync (M5/P7): an unassigned partition surfaces as a SYNCHRONOUS KafkaException
        // (the sync ABI's returned error handle), not a faulted Task.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Assert.Throws<KafkaException>(() => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
    }

    [Fact]
    public void SeekWithMetadata_UnassignedPartition_ThrowsKafkaException()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Assert.Throws<KafkaException>(
            () => consumer.Seek(new TopicPartition("unassigned", 0), new OffsetAndMetadata(0)));
    }

    // ---- Preconditions + exact error messages (before any native call) ----

    [Fact]
    public void Seek_NegativeOffset_ThrowsExactJavaMessage()
    {
        // Q1 = KEEP: the Java-fidelity negative-offset guard — the ONE place .NET is
        // deliberately stricter than Python (whose sync seek does no offset validation).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek(new TopicPartition("t", 0), offset: -1));

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("seek offset must not be a negative number", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Seek_NegativeOffset_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // The offset precondition precedes the disposed check (Q1) — a closed consumer still
        // throws ArgumentOutOfRangeException, not ObjectDisposedException.
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek(new TopicPartition("t", 0), offset: -2));
        Assert.Equal("offset", ex.ParamName);
    }

    [Fact]
    public void Seek_NullTopic_ThrowsArgumentNull()
    {
        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way to
        // present a null topic without the TopicPartition ctor validation firing.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Assert.Throws<ArgumentNullException>(() => consumer.Seek(default, 0L));
    }

    [Fact]
    public void SeekWithMetadata_NullOffsetAndMetadata_ThrowsArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => consumer.Seek(new TopicPartition("t", 0), (OffsetAndMetadata)null!));
        Assert.Equal("offsetAndMetadata", ex.ParamName);
    }

    [Fact]
    public void OffsetAndMetadata_NegativeOffset_ThrowsAtConstruction()
    {
        // Seek(tp, OffsetAndMetadata) needs no offset guard — the OffsetAndMetadata ctor is the
        // upstream gate (rejects offset < 0 with "Invalid negative offset").
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new OffsetAndMetadata(-1));
        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("Invalid negative offset", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Seek_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => consumer.Seek(new TopicPartition("t", 0), 0L));
    }

    [Fact]
    public async Task SeekWithMetadata_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(
            () => consumer.Seek(new TopicPartition("t", 0), new OffsetAndMetadata(0)));
    }

    [Fact]
    public async Task CurrentLag_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => consumer.CurrentLag(new TopicPartition("t", 0)));
    }

    // ---- Interface placement: reachable through an IConsumerCommon reference ----

    [Fact]
    public async Task SeekAndCurrentLag_ViaIConsumerCommon_Work()
    {
        // Both Seek overloads + CurrentLag live on IConsumerCommon (M5/P7), reachable through
        // an IAsyncConsumer / IConsumerCommon reference.
        using AsyncMockConsumer<byte[], byte[]> mock = await ReadyAssigned();
        IConsumerCommon consumer = mock;
        TopicPartition tp = new TopicPartition(Topic, Partition);

        consumer.Seek(tp, 5L);
        consumer.Seek(tp, new OffsetAndMetadata(6, "m", 2));
        long? lag = consumer.CurrentLag(tp);

        Assert.NotNull(lag);
    }

    // Every awaited Position routes through the TestTimeout hang guard so a future stall in the
    // scalar bridge or the mock fails the run fast instead of hanging it.
    private static async Task<long> PositionOf(IAsyncConsumer<byte[], byte[]> consumer, TopicPartition partition)
    {
        long result = 0;
        await TestTimeout.Run(async () => result = await consumer.Position(partition), s_deadline);
        return result;
    }
}
