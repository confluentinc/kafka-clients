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
using System.Linq;

using Confluent.Kafka.GrpcServer.Chaos;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The record encoding, the record check and the rebalance event (PLAN §7.1 T1–T3): the parts of
/// <see cref="ChaosEvents"/> the verifier reads byte for byte and text for text, so they must
/// match the Rust harness (<c>rust/tests/chaos/workload.rs</c> <c>build_value</c> /
/// <c>check_record</c>) and the Python server exactly. Flavour-independent: both servicers share
/// this code.
/// </summary>
public sealed class ChaosEventsTests
{
    private const ulong TwoToThe40 = 1UL << 40;

    /// <summary>The key of each T1 index, written out by hand (8 bytes, big-endian).</summary>
    public static TheoryData<ulong, byte[]> Keys => new TheoryData<ulong, byte[]>
    {
        { 0UL, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 } },
        { 1UL, new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 } },
        { TwoToThe40, new byte[] { 0, 0, 1, 0, 0, 0, 0, 0 } },
        { (ulong)long.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF } },
    };

    /// <summary>Every T1 (index, msg_size) pair.</summary>
    public static TheoryData<ulong, uint> IndexAndSize
    {
        get
        {
            TheoryData<ulong, uint> data = new TheoryData<ulong, uint>();
            foreach (ulong index in new[] { 0UL, 1UL, TwoToThe40, (ulong)long.MaxValue })
            {
                foreach (uint size in new uint[] { 0, 3, 8, 9, 100 })
                {
                    data.Add(index, size);
                }
            }

            return data;
        }
    }

    // ---- T1: Key / Value ----

    [Theory]
    [MemberData(nameof(Keys))]
    public void Key_IsTheEightByteBigEndianIndex(ulong index, byte[] expected)
    {
        Assert.Equal(expected, ChaosEvents.Key(index));
    }

    [Theory]
    [MemberData(nameof(IndexAndSize))]
    public void Value_MatchesRustBuildValue_ByteForByte(ulong index, uint msgSize)
    {
        // build_value: the index's 8 bytes truncated to msg_size, or zero-padded up to it.
        byte[] indexBytes = BigEndian(index);
        byte[] expected = msgSize <= 8
            ? indexBytes.Take((int)msgSize).ToArray()
            : indexBytes.Concat(new byte[msgSize - 8]).ToArray();

        byte[] value = ChaosEvents.Value(index, msgSize);

        Assert.Equal(expected, value);
        Assert.Equal((int)msgSize, value.Length);
    }

    [Fact]
    public void Value_LiteralVectors()
    {
        Assert.Empty(ChaosEvents.Value(TwoToThe40, 0));
        Assert.Equal(new byte[] { 0, 0, 1 }, ChaosEvents.Value(TwoToThe40, 3));
        Assert.Equal(new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0 }, ChaosEvents.Value(TwoToThe40, 9));
        Assert.Equal(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, ChaosEvents.Value((ulong)long.MaxValue, 8));
    }

    [Fact]
    public void Key_AndValue_AreFreshArraysPerCall()
    {
        // The producer hands each array to an async send, so no two records may share one.
        Assert.NotSame(ChaosEvents.Key(7), ChaosEvents.Key(7));
        Assert.NotSame(ChaosEvents.Value(7, 16), ChaosEvents.Value(7, 16));
    }

    // ---- T2: CheckRecord, the four §3.6 texts verbatim ----

    [Fact]
    public void CheckRecord_MissingKey()
    {
        AssertCorrupted(null, ChaosEvents.Value(5, 16), 16, "key is missing (expected the 8-byte index)");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(9)]
    public void CheckRecord_KeyOfTheWrongLength(int length)
    {
        AssertCorrupted(new byte[length], ChaosEvents.Value(5, 16), 16, $"key is {length} byte(s), expected the 8-byte index");
    }

    [Fact]
    public void CheckRecord_MissingValue()
    {
        AssertCorrupted(ChaosEvents.Key(5), null, 16, "value is missing (expected 16 byte(s) encoding index 5)");
    }

    [Fact]
    public void CheckRecord_ValueOfTheWrongLength()
    {
        AssertCorrupted(
            ChaosEvents.Key(5),
            ChaosEvents.Value(5, 15),
            16,
            "value of 15 byte(s) does not match the producer's encoding of index 5 (16 byte(s))");
    }

    [Fact]
    public void CheckRecord_EmptyValueWhereBytesWereExpected_IsAMismatch_NotMissing()
    {
        // Absent and empty are different records (T14c): an empty value is "0 byte(s)".
        AssertCorrupted(
            ChaosEvents.Key(5),
            Array.Empty<byte>(),
            16,
            "value of 0 byte(s) does not match the producer's encoding of index 5 (16 byte(s))");
    }

    [Fact]
    public void CheckRecord_ValueOfTheRightLengthButWrongBytes()
    {
        byte[] value = ChaosEvents.Value(5, 16);
        value[15] = 1;
        AssertCorrupted(
            ChaosEvents.Key(5),
            value,
            16,
            "value of 16 byte(s) does not match the producer's encoding of index 5 (16 byte(s))");
    }

    [Fact]
    public void CheckRecord_ValueOfAnotherIndex_IsAMismatch()
    {
        AssertCorrupted(
            ChaosEvents.Key(5),
            ChaosEvents.Value(6, 16),
            16,
            "value of 16 byte(s) does not match the producer's encoding of index 5 (16 byte(s))");
    }

    [Theory]
    [InlineData(0UL, 16U)]
    [InlineData(5UL, 3U)]
    [InlineData(5UL, 8U)]
    [InlineData(ulong.MaxValue, 100U)]
    public void CheckRecord_TheProducersOwnRecord_IsOk(ulong index, uint msgSize)
    {
        AssertOk(ChaosEvents.Key(index), ChaosEvents.Value(index, msgSize), msgSize, index);
    }

    [Fact]
    public void CheckRecord_MissingValueWithMsgSizeZero_IsOk()
    {
        AssertOk(ChaosEvents.Key(5), null, 0, 5);
    }

    [Fact]
    public void CheckRecord_EmptyValueWithMsgSizeZero_IsOk()
    {
        AssertOk(ChaosEvents.Key(5), Array.Empty<byte>(), 0, 5);
    }

    // ---- T3: the rebalance event ----

    [Fact]
    public void Rebalance_SortsByTopicOrdinalThenPartition_AndCarriesItsKind()
    {
        TopicPartition[] partitions =
        {
            new TopicPartition("b", 1),
            new TopicPartition("a", 2),
            new TopicPartition("a", 0),
            new TopicPartition("B", 0),
            new TopicPartition("a", 10),
        };

        Proto.WorkloadEvent rebalance = ChaosEvents.Rebalance(Proto.RebalanceKind.Revoked, partitions, 42);

        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Rebalance, rebalance.EventCase);
        Assert.Equal(Proto.RebalanceKind.Revoked, rebalance.Rebalance.Kind);
        Assert.Equal(42, rebalance.Rebalance.ObservedAtUnixNanos);
        Assert.Equal(
            new[] { ("B", 0), ("a", 0), ("a", 2), ("a", 10), ("b", 1) },
            rebalance.Rebalance.Partitions.Select(p => (p.Topic, p.Partition)).ToArray());
    }

    [Fact]
    public void Rebalance_FromTheListener_IsObservedBetweenBeforeAndAfter()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using ConsumerHandle handle = consumer.Handle();
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRebalanceListener listener = new ChaosRebalanceListener(events.Add, handle, checkCommits: false);

        long before = UnixNanos();
        listener.OnPartitionsAssigned(new[] { new TopicPartition("t", 1), new TopicPartition("t", 0) });
        long after = UnixNanos();

        Proto.WorkloadEvent rebalance = Assert.Single(events);
        Assert.Equal(Proto.RebalanceKind.Assigned, rebalance.Rebalance.Kind);
        Assert.InRange(rebalance.Rebalance.ObservedAtUnixNanos, before, after);
        Assert.Equal(new[] { 0, 1 }, rebalance.Rebalance.Partitions.Select(p => p.Partition).ToArray());
    }

    // ---- T15 (the lost half, which MockConsumer.Rebalance never fires) ----

    [Fact]
    public void Listener_Lost_ReportsLost_AndNeverCommits()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using ConsumerHandle handle = consumer.Handle();
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRebalanceListener listener = new ChaosRebalanceListener(events.Add, handle, checkCommits: true);

        listener.OnPartitionsLost(new[] { new TopicPartition("t", 3) });

        // A commit through a mock-derived handle always fails (UnsupportedVersion), so a lost
        // callback that committed would leave a ConsumerError behind: its absence proves none ran.
        Proto.WorkloadEvent lost = Assert.Single(events);
        Assert.Equal(Proto.RebalanceKind.Lost, lost.Rebalance.Kind);
        Assert.Equal(("t", 3), (lost.Rebalance.Partitions[0].Topic, lost.Rebalance.Partitions[0].Partition));
    }

    // ---- terminal detection ----

    [Fact]
    public void IsTerminal_OnlyFinishedAndFailed()
    {
        Assert.True(ChaosEvents.IsTerminal(ChaosEvents.Finished()));
        Assert.True(ChaosEvents.IsTerminal(ChaosEvents.Failed(new InvalidOperationException("x"))));
        Assert.False(ChaosEvents.IsTerminal(ChaosEvents.Closed()));
        Assert.False(ChaosEvents.IsTerminal(ChaosEvents.Closing()));
        Assert.False(ChaosEvents.IsTerminal(ChaosEvents.Sent(1)));
        Assert.False(ChaosEvents.IsTerminal(ChaosEvents.Marker(1)));
    }

    [Fact]
    public void Failed_OfANonKafkaException_IsTheTranslatedLocalIllegalState()
    {
        Proto.WorkloadEvent failed = ChaosEvents.Failed(new InvalidOperationException("boom"));

        Assert.Equal(-4, failed.Failed.Error.Code);
        Assert.Equal("dotnet server: InvalidOperationException: boom", failed.Failed.Error.Message);
    }

    internal static long UnixNanos() => (DateTime.UtcNow - DateTime.UnixEpoch).Ticks * 100;

    private static byte[] BigEndian(ulong index)
    {
        byte[] bytes = BitConverter.GetBytes(index);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }

        return bytes;
    }

    private static void AssertCorrupted(byte[]? key, byte[]? value, uint msgSize, string expected)
    {
        Assert.False(ChaosEvents.TryCheckRecord(key, value, msgSize, out _, out string? detail));
        Assert.Equal(expected, detail);
    }

    private static void AssertOk(byte[]? key, byte[]? value, uint msgSize, ulong expectedIndex)
    {
        Assert.True(ChaosEvents.TryCheckRecord(key, value, msgSize, out ulong index, out string? detail));
        Assert.Null(detail);
        Assert.Equal(expectedIndex, index);
    }
}
