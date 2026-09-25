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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P8's value readers, driven over injected accessors.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Every shape here is unreachable end to end.</b> All six P8 RPCs mirror Java's own
/// <c>UnsupportedOperationException("Not implemented yet")</c> in both mocks, so no populated
/// result root exists without a broker — this is the phase's concentrated risk and the reason
/// every reader is exercised here rather than sampled.
/// </para>
/// <para>
/// ⚠ The <b>wiring</b> — that each reader is built on its own ABI symbols — is
/// <see cref="AdminP4ReaderWiringTests"/>'s job. Injected accessors prove the walk, not the
/// binding.
/// </para>
/// </remarks>
public sealed class AdminP8ResultMarshalTests : IDisposable
{
    // ---- fenceProducers: the two inline scalars ------------------------------------------

    /// <summary>
    /// Each scalar reaches <b>its own</b> field, read at the requested index. The two
    /// accessors return deliberately disjoint value ranges so a body that read the producer id
    /// into the epoch (or either at the wrong index) cannot produce these numbers.
    /// </summary>
    [Fact]
    public void FenceProducersValue_ReadsEachScalarIntoItsOwnFieldAtItsOwnIndex()
    {
        long[] producerIds = { 1_000L, 2_000L, 3_000L };
        short[] epochs = { 11, 22, 33 };

        Func<IntPtr, int, ProducerIdAndEpoch> reader = AdminCallbacks.FenceProducersValueReader(
            (result, index) => producerIds[index],
            (result, index) => epochs[index]);

        Assert.Equal(1_000L, reader(IntPtr.Zero, 0).ProducerId);
        Assert.Equal((short)11, reader(IntPtr.Zero, 0).Epoch);
        Assert.Equal(3_000L, reader(IntPtr.Zero, 2).ProducerId);
        Assert.Equal((short)33, reader(IntPtr.Zero, 2).Epoch);
    }

    /// <summary>
    /// ⚠ <b>Reachability control-positive.</b> The reader really is the code under test: an
    /// accessor that throws propagates out of it, so a green assertion above cannot have come
    /// from a harness double that never invoked the injected accessors.
    /// </summary>
    [Fact]
    public void FenceProducersValue_ControlPositive_DrivesTheInjectedAccessors()
    {
        Func<IntPtr, int, ProducerIdAndEpoch> throwsOnProducerId =
            AdminCallbacks.FenceProducersValueReader(
                (result, index) => throw new InvalidOperationException("producer id reader reached"),
                (result, index) => (short)0);

        Func<IntPtr, int, ProducerIdAndEpoch> throwsOnEpoch =
            AdminCallbacks.FenceProducersValueReader(
                (result, index) => 0L,
                (result, index) => throw new InvalidOperationException("epoch reader reached"));

        Assert.Equal(
            "producer id reader reached",
            Assert.Throws<InvalidOperationException>(() => throwsOnProducerId(IntPtr.Zero, 0)).Message);
        Assert.Equal(
            "epoch reader reached",
            Assert.Throws<InvalidOperationException>(() => throwsOnEpoch(IntPtr.Zero, 0)).Message);
    }

    /// <summary>
    /// ⚠ <c>-1</c> from either accessor is <c>ProducerIdAndEpoch.NONE</c> — a <b>value</b>,
    /// carried through unchanged. The per-id error, not this sentinel, is the walker's
    /// success/failure verdict.
    /// </summary>
    [Fact]
    public void FenceProducersValue_MinusOneIsCarriedThroughAsAValue()
    {
        Func<IntPtr, int, ProducerIdAndEpoch> reader = AdminCallbacks.FenceProducersValueReader(
            (result, index) => -1L,
            (result, index) => (short)-1);

        ProducerIdAndEpoch value = reader(IntPtr.Zero, 0);

        Assert.Equal(-1L, value.ProducerId);
        Assert.Equal((short)-1, value.Epoch);
    }

    // ---- describeTransactions: six scalars, one optional, one nested (i, j) walk ---------

    /// <summary>
    /// Every scalar reaches <b>its own</b> field, read at the requested row. The value ranges
    /// are disjoint per accessor, so a body that read one into another's field — the hazard
    /// the same-typed accessors create — cannot produce these numbers.
    /// </summary>
    [Fact]
    public void TransactionDescription_ReadsEachScalarIntoItsOwnField()
    {
        TransactionDescription description = ReadRow(0, Rows());

        Assert.Equal(11, description.CoordinatorId);
        Assert.Equal(TransactionState.PrepareAbort, description.State);
        Assert.Equal(2_100L, description.ProducerId);
        Assert.Equal(31, description.ProducerEpoch);
        Assert.Equal(41_000L, description.TransactionTimeoutMs);
        Assert.Equal(51_000L, description.TransactionStartTimeMs);
    }

    /// <summary>Every accessor is read at the <b>requested</b> row, not at a fixed one.</summary>
    [Fact]
    public void TransactionDescription_ReadsEveryAccessorAtTheRequestedRow()
    {
        TransactionDescription description = ReadRow(1, Rows());

        Assert.Equal(12, description.CoordinatorId);
        Assert.Equal(TransactionState.CompleteCommit, description.State);
        Assert.Equal(2_200L, description.ProducerId);
        Assert.Equal(32, description.ProducerEpoch);
        Assert.Equal(42_000L, description.TransactionTimeoutMs);
    }

    /// <summary>
    /// ⚠ The inner walk is bounded by <b>that row's own</b> partition count, and each
    /// <c>(i, j)</c> accessor is called with both indices — so row 1's two partitions are read
    /// from row 1, not from row 0.
    /// </summary>
    [Fact]
    public void TransactionDescription_WalksThatRowsOwnPartitions()
    {
        Assert.Equal(
            new[] { new TopicPartition("t-1-0", 70), new TopicPartition("t-1-1", 71) },
            ReadRow(1, Rows()).TopicPartitions);

        Assert.Equal(
            new[] { new TopicPartition("t-0-0", 60) },
            ReadRow(0, Rows()).TopicPartitions);
    }

    /// <summary>
    /// ⚠⚠ A row whose count is <c>0</c> yields an <b>empty</b> partition set — the failed-row
    /// shape (<c>test_mock_admin.c:5938-5940</c>) — rather than being driven by the outer count.
    /// </summary>
    [Fact]
    public void TransactionDescription_ZeroPartitionCount_YieldsNoPartitions() =>
        Assert.Empty(ReadRow(2, Rows()).TopicPartitions);

    /// <summary>
    /// ⚠⚠ The optional start time is <b>absent</b> when the accessor returns false, and the
    /// out-param it left behind is not read — the value below is deliberately a plausible
    /// timestamp, so a body that ignored the discriminant would surface it.
    /// </summary>
    [Fact]
    public void TransactionDescription_AbsentStartTime_IsNullAndIgnoresTheOutParam() =>
        Assert.Null(ReadRow(2, Rows()).TransactionStartTimeMs);

    /// <summary>
    /// ⚠ <c>-1</c> and <c>0</c> from the optional are <b>values</b>, not absences: only the
    /// discriminant decides.
    /// </summary>
    [Theory]
    [InlineData(-1L)]
    [InlineData(0L)]
    public void TransactionDescription_PresentSentinelStartTime_IsAValue(long startTime)
    {
        Row[] rows = Rows();
        rows[0].HasStartTime = true;
        rows[0].StartTimeMs = startTime;

        Assert.Equal(startTime, ReadRow(0, rows).TransactionStartTimeMs);
    }

    /// <summary>
    /// ⚠⚠ <b>Reachability control-positive.</b> Each of the nine accessors really is driven by
    /// the production reader: one at a time, an accessor that throws propagates out, so no
    /// green assertion above can have come from a harness double.
    /// </summary>
    [Theory]
    [InlineData("coordinator id")]
    [InlineData("state")]
    [InlineData("producer id")]
    [InlineData("producer epoch")]
    [InlineData("transaction timeout")]
    [InlineData("start time")]
    [InlineData("partition count")]
    [InlineData("partition topic")]
    [InlineData("partition id")]
    public void TransactionDescription_ControlPositive_DrivesEveryInjectedAccessor(string poisoned)
    {
        Row[] rows = Rows();
        TransactionDescriptionMarshal.Accessors accessors = new TransactionDescriptionMarshal.Accessors(
            Poison(poisoned, "coordinator id", (result, index) => rows[index].CoordinatorId),
            PoisonState(poisoned, rows),
            Poison(poisoned, "producer id", (result, index) => rows[index].ProducerId),
            Poison(poisoned, "producer epoch", (result, index) => rows[index].ProducerEpoch),
            Poison(poisoned, "transaction timeout", (result, index) => rows[index].TimeoutMs),
            PoisonOptional(poisoned, rows),
            Poison(poisoned, "partition count", (result, index) => rows[index].Partitions.Count),
            PoisonNestedString(poisoned, rows),
            PoisonNestedInt32(poisoned, rows));

        Assert.Equal(
            poisoned + " reader reached",
            Assert.Throws<InvalidOperationException>(
                () => TransactionDescriptionMarshal.ReadDescription(IntPtr.Zero, 0, accessors)).Message);
    }

    /// <summary>
    /// A partition with no topic name inside the row's own count is a malformed result and is
    /// rejected, rather than silently dropped or turned into a null-topic partition.
    /// </summary>
    [Fact]
    public void TransactionDescription_PartitionWithNoTopic_IsRejected()
    {
        Row[] rows = Rows();
        TransactionDescriptionMarshal.Accessors accessors = Accessors(
            rows, getTopic: (result, index, partition) => IntPtr.Zero);

        Assert.Contains(
            "no topic name",
            Assert.Throws<KafkaException>(
                    () => TransactionDescriptionMarshal.ReadDescription(IntPtr.Zero, 0, accessors))
                .Message,
            StringComparison.Ordinal);
    }

    // ---- describeProducers: the nested (i, j) producer walk ------------------------------

    /// <summary>
    /// Every scalar reaches <b>its own</b> field of <b>its own</b> producer. The four values
    /// are disjoint per accessor and per producer, so neither a field swap — the hazard the
    /// same-typed pairs create — nor a fixed producer index can produce these numbers.
    /// </summary>
    [Fact]
    public void PartitionProducerState_ReadsEachScalarIntoItsOwnFieldPerProducer()
    {
        IReadOnlyList<ProducerState> producers =
            ReadPartition(0, ProducerRows()).ActiveProducers;

        Assert.Equal(2, producers.Count);

        Assert.Equal(1_000L, producers[0].ProducerId);
        Assert.Equal(11, producers[0].ProducerEpoch);
        Assert.Equal(21, producers[0].LastSequence);
        Assert.Equal(31_000L, producers[0].LastTimestamp);

        Assert.Equal(2_000L, producers[1].ProducerId);
        Assert.Equal(12, producers[1].ProducerEpoch);
        Assert.Equal(22, producers[1].LastSequence);
        Assert.Equal(32_000L, producers[1].LastTimestamp);
    }

    /// <summary>
    /// ⚠ The inner walk is bounded by <b>that row's own</b> producer count, and every
    /// <c>(i, j)</c> accessor is called with both indices — so row 1's single producer is read
    /// from row 1.
    /// </summary>
    [Fact]
    public void PartitionProducerState_WalksThatRowsOwnProducers()
    {
        IReadOnlyList<ProducerState> producers =
            ReadPartition(1, ProducerRows()).ActiveProducers;

        Assert.Single(producers);
        Assert.Equal(3_000L, producers[0].ProducerId);
        Assert.Equal(13, producers[0].ProducerEpoch);
    }

    /// <summary>
    /// ⚠⚠ A row whose count is <c>0</c> yields no producers — the failed-partition shape
    /// (<c>test_mock_admin.c:5881</c>) — rather than being driven by the outer count.
    /// </summary>
    [Fact]
    public void PartitionProducerState_ZeroProducerCount_YieldsNoProducers() =>
        Assert.Empty(ReadPartition(2, ProducerRows()).ActiveProducers);

    /// <summary>
    /// ⚠⚠ The two optionals are independent: each is absent only when <b>its own</b>
    /// discriminant is false, and the out-param it left behind is not read. The stand-in
    /// leaves plausible values behind, so a body that ignored a discriminant would surface one.
    /// </summary>
    [Fact]
    public void PartitionProducerState_EachOptionalIsIndependentlyAbsent()
    {
        IReadOnlyList<ProducerState> producers =
            ReadPartition(0, ProducerRows()).ActiveProducers;

        // Producer 0: both present, and each in its own field.
        Assert.Equal(41, producers[0].CoordinatorEpoch);
        Assert.Equal(51_000L, producers[0].CurrentTransactionStartOffset);

        // Producer 1: the coordinator epoch is present, the start offset is not.
        Assert.Equal(42, producers[1].CoordinatorEpoch);
        Assert.Null(producers[1].CurrentTransactionStartOffset);

        // Row 1's only producer: the mirror image — the start offset is present, the
        // coordinator epoch is not.
        IReadOnlyList<ProducerState> second = ReadPartition(1, ProducerRows()).ActiveProducers;
        Assert.Null(second[0].CoordinatorEpoch);
        Assert.Equal(53_000L, second[0].CurrentTransactionStartOffset);
    }

    /// <summary>
    /// ⚠ <c>-1</c> and <c>0</c> from either optional are <b>values</b>, not absences: only the
    /// discriminant decides. The four plain scalars beside them do use <c>-1</c> as a sentinel.
    /// </summary>
    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public void PartitionProducerState_PresentSentinelOptionals_AreValues(int sentinel)
    {
        ProducerRow[] rows = ProducerRows();
        Producer first = rows[0].Producers[0];
        first.HasCoordinatorEpoch = true;
        first.CoordinatorEpoch = sentinel;
        first.HasStartOffset = true;
        first.StartOffset = sentinel;

        ProducerState producer = ReadPartition(0, rows).ActiveProducers[0];

        Assert.Equal(sentinel, producer.CoordinatorEpoch);
        Assert.Equal((long)sentinel, producer.CurrentTransactionStartOffset);
    }

    /// <summary>
    /// ⚠⚠ <b>Reachability control-positive.</b> Each of the seven accessors really is driven by
    /// the production reader: one at a time, an accessor that throws propagates out.
    /// </summary>
    [Theory]
    [InlineData("producer count")]
    [InlineData("producer id")]
    [InlineData("producer epoch")]
    [InlineData("last sequence")]
    [InlineData("last timestamp")]
    [InlineData("start offset")]
    [InlineData("coordinator epoch")]
    public void PartitionProducerState_ControlPositive_DrivesEveryInjectedAccessor(string poisoned)
    {
        ProducerRow[] rows = ProducerRows();
        PartitionProducerStateMarshal.Accessors accessors = new PartitionProducerStateMarshal.Accessors(
            Poison(poisoned, "producer count", (result, index) => rows[index].Producers.Count),
            PoisonNestedInt64(poisoned, "producer id", rows, producer => producer.ProducerId),
            PoisonNestedInt32(poisoned, "producer epoch", rows, producer => producer.Epoch),
            PoisonNestedInt32(poisoned, "last sequence", rows, producer => producer.LastSequence),
            PoisonNestedInt64(poisoned, "last timestamp", rows, producer => producer.LastTimestamp),
            PoisonStartOffset(poisoned, rows),
            PoisonCoordinatorEpoch(poisoned, rows));

        Assert.Equal(
            poisoned + " reader reached",
            Assert.Throws<InvalidOperationException>(
                    () => PartitionProducerStateMarshal.ReadPartitionProducerState(
                        IntPtr.Zero, 0, accessors))
                .Message);
    }

    // ---- listTransactions: the nested (i, j) listing walk --------------------------------

    /// <summary>
    /// Every field reaches <b>its own</b> slot of <b>its own</b> listing. The transactional id
    /// and the state are read from two same-typed accessors, so a transposition is the hazard:
    /// the values below make one unmistakable for the other.
    /// </summary>
    [Fact]
    public void TransactionListings_ReadEachFieldIntoItsOwnSlot()
    {
        List<TransactionListing> listings =
            ReadListings(0, ListingRows()).ToList();

        Assert.Equal(2, listings.Count);

        Assert.Equal("txn-a", listings[0].TransactionalId);
        Assert.Equal(1_000L, listings[0].ProducerId);
        Assert.Equal(TransactionState.Ongoing, listings[0].State);

        Assert.Equal("txn-b", listings[1].TransactionalId);
        Assert.Equal(2_000L, listings[1].ProducerId);
        Assert.Equal(TransactionState.PrepareAbort, listings[1].State);
    }

    /// <summary>
    /// ⚠ The inner walk is bounded by <b>that broker's own</b> listing count — the outer count
    /// counts brokers — and every <c>(i, j)</c> accessor is called with both indices.
    /// </summary>
    [Fact]
    public void TransactionListings_WalkThatBrokersOwnListings()
    {
        List<TransactionListing> listings = ReadListings(1, ListingRows()).ToList();

        Assert.Single(listings);
        Assert.Equal("txn-c", listings[0].TransactionalId);
        Assert.Equal(TransactionState.CompleteCommit, listings[0].State);
    }

    /// <summary>
    /// ⚠⚠ A broker whose count is <c>0</c> yields no listings — the failed-broker shape
    /// (<c>h:10225-10228</c>) — rather than being driven by the outer count.
    /// </summary>
    [Fact]
    public void TransactionListings_ZeroListingCount_YieldsNoListings() =>
        Assert.Empty(ReadListings(2, ListingRows()));

    /// <summary>
    /// ⚠ An unrecognised state name decodes to <see cref="TransactionState.Unknown"/> rather
    /// than throwing, as Java's <c>parse</c> does — so the walk survives a broker reporting a
    /// state this client does not know.
    /// </summary>
    [Fact]
    public void TransactionListings_UnrecognisedState_DecodesToUnknown()
    {
        ListingRow[] rows = ListingRows();
        rows[0].Listings[0].State = "Dead";

        Assert.Equal(TransactionState.Unknown, ReadListings(0, rows).First().State);
    }

    /// <summary>
    /// ⚠⚠ <b>Reachability control-positive.</b> Each of the four accessors really is driven by
    /// the production reader: one at a time, an accessor that throws propagates out.
    /// </summary>
    [Theory]
    [InlineData("listing count")]
    [InlineData("transactional id")]
    [InlineData("listing producer id")]
    [InlineData("listing state")]
    public void TransactionListings_ControlPositive_DrivesEveryInjectedAccessor(string poisoned)
    {
        ListingRow[] rows = ListingRows();
        TransactionListingMarshal.Accessors accessors = new TransactionListingMarshal.Accessors(
            Poison(poisoned, "listing count", (result, index) => rows[index].Listings.Count),
            PoisonListingString(poisoned, "transactional id", rows, listing => listing.TransactionalId),
            poisoned == "listing producer id"
                ? (result, index, listing) =>
                    throw new InvalidOperationException("listing producer id reader reached")
                : (TransactionListingMarshal.NestedInt64Accessor)(
                    (result, index, listing) => rows[index].Listings[listing].ProducerId),
            PoisonListingString(poisoned, "listing state", rows, listing => listing.State));

        Assert.Equal(
            poisoned + " reader reached",
            Assert.Throws<InvalidOperationException>(
                    () => TransactionListingMarshal.ReadListings(IntPtr.Zero, 0, accessors))
                .Message);
    }

    /// <summary>
    /// A listing with no transactional id inside the broker's own count is a malformed result
    /// and is rejected, rather than silently dropped.
    /// </summary>
    [Fact]
    public void TransactionListings_ListingWithNoId_IsRejected()
    {
        ListingRow[] rows = ListingRows();
        TransactionListingMarshal.Accessors accessors = ListingAccessors(
            rows, getTransactionalId: (result, index, listing) => IntPtr.Zero);

        Assert.Contains(
            "no transactional id",
            Assert.Throws<KafkaException>(
                    () => TransactionListingMarshal.ReadListings(IntPtr.Zero, 0, accessors))
                .Message,
            StringComparison.Ordinal);
    }

    // ---- the TransactionState wire table -------------------------------------------------

    /// <summary>
    /// ⚠⚠ Every state round-trips through <b>Java's <c>toString()</c> spelling</b>
    /// (<c>TransactionState.java:25-32</c>), asserted against the literal rather than against
    /// the C# identifier — the two coincide today, so a rename would change the wire format
    /// without failing to compile, and this is what turns red instead.
    /// </summary>
    [Theory]
    [InlineData(TransactionState.Ongoing, "Ongoing")]
    [InlineData(TransactionState.PrepareAbort, "PrepareAbort")]
    [InlineData(TransactionState.PrepareCommit, "PrepareCommit")]
    [InlineData(TransactionState.CompleteAbort, "CompleteAbort")]
    [InlineData(TransactionState.CompleteCommit, "CompleteCommit")]
    [InlineData(TransactionState.Empty, "Empty")]
    [InlineData(TransactionState.PrepareEpochFence, "PrepareEpochFence")]
    [InlineData(TransactionState.Unknown, "Unknown")]
    public void TransactionState_RoundTripsThroughItsWireName(TransactionState state, string wireName)
    {
        Assert.Equal(wireName, TransactionMarshal.WireName(state));
        Assert.Equal(state, TransactionMarshal.Parse(wireName));
    }

    /// <summary>The eight wire names are distinct, so no two states collide in the parse table.</summary>
    [Fact]
    public void TransactionState_WireNamesAreDistinct()
    {
        string[] wireNames = Enum.GetValues(typeof(TransactionState))
            .Cast<TransactionState>()
            .Select(TransactionMarshal.WireName)
            .ToArray();

        Assert.Equal(8, wireNames.Length);
        Assert.Equal(wireNames.Length, wireNames.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// ⚠ Java's <c>parse</c> never throws: an unrecognised name — including the broker-side
    /// <c>DEAD</c>, the <c>name()</c> spelling, and a case variant — is
    /// <see cref="TransactionState.Unknown"/>.
    /// </summary>
    [Theory]
    [InlineData("Dead")]
    [InlineData("PREPARE_ABORT")]
    [InlineData("ongoing")]
    [InlineData("")]
    [InlineData(null)]
    public void TransactionState_UnrecognisedName_ParsesToUnknown(string? name) =>
        Assert.Equal(TransactionState.Unknown, TransactionMarshal.Parse(name));

    /// <summary>
    /// ⚠⚠ A <b>numeric</b> name is not a state. <see cref="Enum.Parse(Type, string)"/> would
    /// decode <c>"3"</c> to <see cref="TransactionState.CompleteAbort"/>, inventing an id
    /// Java's constants do not have; the table decodes it to
    /// <see cref="TransactionState.Unknown"/>. The control below shows the two really do
    /// disagree, so this is a property of the table and not of the input.
    /// </summary>
    [Fact]
    public void TransactionState_NumericName_ParsesToUnknownRatherThanByOrdinal()
    {
        Assert.Equal(TransactionState.Unknown, TransactionMarshal.Parse("3"));

        Assert.Equal(
            TransactionState.CompleteAbort,
            (TransactionState)Enum.Parse(typeof(TransactionState), "3"));
    }

    /// <summary>A null state pointer decodes to Unknown rather than throwing.</summary>
    [Fact]
    public void TransactionState_NullPointer_ReadsAsUnknown() =>
        Assert.Equal(TransactionState.Unknown, TransactionMarshal.ReadState(IntPtr.Zero));

    // ---- helpers -------------------------------------------------------------------------

    /// <summary>
    /// One row of a stand-in <c>describeTransactions</c> table. Every field carries a value
    /// disjoint from its same-typed siblings, so a mis-wired read is visible.
    /// </summary>
    private sealed class Row
    {
        internal int CoordinatorId { get; set; }

        internal string State { get; set; } = "Unknown";

        internal long ProducerId { get; set; }

        internal int ProducerEpoch { get; set; }

        internal long TimeoutMs { get; set; }

        internal bool HasStartTime { get; set; }

        internal long StartTimeMs { get; set; }

        internal List<TopicPartition> Partitions { get; } = new List<TopicPartition>();
    }

    private static Row[] Rows()
    {
        Row first = new Row
        {
            CoordinatorId = 11,
            State = "PrepareAbort",
            ProducerId = 2_100L,
            ProducerEpoch = 31,
            TimeoutMs = 41_000L,
            HasStartTime = true,
            StartTimeMs = 51_000L,
        };
        first.Partitions.Add(new TopicPartition("t-0-0", 60));

        Row second = new Row
        {
            CoordinatorId = 12,
            State = "CompleteCommit",
            ProducerId = 2_200L,
            ProducerEpoch = 32,
            TimeoutMs = 42_000L,
            HasStartTime = true,
            StartTimeMs = 52_000L,
        };
        second.Partitions.Add(new TopicPartition("t-1-0", 70));
        second.Partitions.Add(new TopicPartition("t-1-1", 71));

        // The failed-row shape: sentinels everywhere, no partitions, no start time — and an
        // out-param value the reader must not surface.
        Row failed = new Row
        {
            CoordinatorId = -1,
            State = "Unknown",
            ProducerId = -1L,
            ProducerEpoch = -1,
            TimeoutMs = -1L,
            HasStartTime = false,
            StartTimeMs = 53_000L,
        };

        return new[] { first, second, failed };
    }

    /// <summary>One active producer of a stand-in <c>describeProducers</c> row.</summary>
    private sealed class Producer
    {
        internal long ProducerId { get; set; }

        internal int Epoch { get; set; }

        internal int LastSequence { get; set; }

        internal long LastTimestamp { get; set; }

        internal bool HasCoordinatorEpoch { get; set; }

        internal int CoordinatorEpoch { get; set; }

        internal bool HasStartOffset { get; set; }

        internal long StartOffset { get; set; }
    }

    /// <summary>One partition row of a stand-in <c>describeProducers</c> table.</summary>
    private sealed class ProducerRow
    {
        internal List<Producer> Producers { get; } = new List<Producer>();
    }

    private static ProducerRow[] ProducerRows()
    {
        ProducerRow first = new ProducerRow();

        // Both optionals present.
        first.Producers.Add(new Producer
        {
            ProducerId = 1_000L,
            Epoch = 11,
            LastSequence = 21,
            LastTimestamp = 31_000L,
            HasCoordinatorEpoch = true,
            CoordinatorEpoch = 41,
            HasStartOffset = true,
            StartOffset = 51_000L,
        });

        // Coordinator epoch present, start offset absent — with a plausible value left in
        // the out-param, so ignoring the discriminant would surface it.
        first.Producers.Add(new Producer
        {
            ProducerId = 2_000L,
            Epoch = 12,
            LastSequence = 22,
            LastTimestamp = 32_000L,
            HasCoordinatorEpoch = true,
            CoordinatorEpoch = 42,
            HasStartOffset = false,
            StartOffset = 52_000L,
        });

        // The mirror image, so neither discriminant can stand in for the other.
        ProducerRow second = new ProducerRow();
        second.Producers.Add(new Producer
        {
            ProducerId = 3_000L,
            Epoch = 13,
            LastSequence = 23,
            LastTimestamp = 33_000L,
            HasCoordinatorEpoch = false,
            CoordinatorEpoch = 43,
            HasStartOffset = true,
            StartOffset = 53_000L,
        });

        // The failed-partition shape: no producers at all.
        return new[] { first, second, new ProducerRow() };
    }

    private static DescribeProducersResult.PartitionProducerState ReadPartition(
        int index, ProducerRow[] rows) =>
        PartitionProducerStateMarshal.ReadPartitionProducerState(
            IntPtr.Zero, index, ProducerAccessors(rows));

    private static PartitionProducerStateMarshal.Accessors ProducerAccessors(ProducerRow[] rows) =>
        new PartitionProducerStateMarshal.Accessors(
            (result, index) => rows[index].Producers.Count,
            (result, index, producer) => rows[index].Producers[producer].ProducerId,
            (result, index, producer) => rows[index].Producers[producer].Epoch,
            (result, index, producer) => rows[index].Producers[producer].LastSequence,
            (result, index, producer) => rows[index].Producers[producer].LastTimestamp,
            (IntPtr result, int index, int producer, out long value) =>
            {
                value = rows[index].Producers[producer].StartOffset;
                return rows[index].Producers[producer].HasStartOffset;
            },
            (IntPtr result, int index, int producer, out int value) =>
            {
                value = rows[index].Producers[producer].CoordinatorEpoch;
                return rows[index].Producers[producer].HasCoordinatorEpoch;
            });

    private static PartitionProducerStateMarshal.NestedInt64Accessor PoisonNestedInt64(
        string poisoned, string name, ProducerRow[] rows, Func<Producer, long> read) =>
        poisoned == name
            ? (result, index, producer) =>
                throw new InvalidOperationException(name + " reader reached")
            : (result, index, producer) => read(rows[index].Producers[producer]);

    private static PartitionProducerStateMarshal.NestedInt32Accessor PoisonNestedInt32(
        string poisoned, string name, ProducerRow[] rows, Func<Producer, int> read) =>
        poisoned == name
            ? (result, index, producer) =>
                throw new InvalidOperationException(name + " reader reached")
            : (result, index, producer) => read(rows[index].Producers[producer]);

    private static PartitionProducerStateMarshal.NestedOptionalInt64Accessor PoisonStartOffset(
        string poisoned, ProducerRow[] rows)
    {
        if (poisoned == "start offset")
        {
            return (IntPtr result, int index, int producer, out long value) =>
                throw new InvalidOperationException("start offset reader reached");
        }

        return (IntPtr result, int index, int producer, out long value) =>
        {
            value = rows[index].Producers[producer].StartOffset;
            return rows[index].Producers[producer].HasStartOffset;
        };
    }

    private static PartitionProducerStateMarshal.NestedOptionalInt32Accessor PoisonCoordinatorEpoch(
        string poisoned, ProducerRow[] rows)
    {
        if (poisoned == "coordinator epoch")
        {
            return (IntPtr result, int index, int producer, out int value) =>
                throw new InvalidOperationException("coordinator epoch reader reached");
        }

        return (IntPtr result, int index, int producer, out int value) =>
        {
            value = rows[index].Producers[producer].CoordinatorEpoch;
            return rows[index].Producers[producer].HasCoordinatorEpoch;
        };
    }

    /// <summary>One transaction listing of a stand-in <c>listTransactions</c> broker row.</summary>
    private sealed class Listing
    {
        internal string TransactionalId { get; set; } = string.Empty;

        internal long ProducerId { get; set; }

        internal string State { get; set; } = "Unknown";
    }

    /// <summary>One broker row of a stand-in <c>listTransactions</c> table.</summary>
    private sealed class ListingRow
    {
        internal List<Listing> Listings { get; } = new List<Listing>();
    }

    private static ListingRow[] ListingRows()
    {
        ListingRow first = new ListingRow();
        first.Listings.Add(new Listing
        {
            TransactionalId = "txn-a",
            ProducerId = 1_000L,
            State = "Ongoing",
        });
        first.Listings.Add(new Listing
        {
            TransactionalId = "txn-b",
            ProducerId = 2_000L,
            State = "PrepareAbort",
        });

        ListingRow second = new ListingRow();
        second.Listings.Add(new Listing
        {
            TransactionalId = "txn-c",
            ProducerId = 3_000L,
            State = "CompleteCommit",
        });

        // The failed-broker shape: no listings at all.
        return new[] { first, second, new ListingRow() };
    }

    private IReadOnlyCollection<TransactionListing> ReadListings(int index, ListingRow[] rows) =>
        TransactionListingMarshal.ReadListings(IntPtr.Zero, index, ListingAccessors(rows));

    private TransactionListingMarshal.Accessors ListingAccessors(
        ListingRow[] rows,
        TransactionListingMarshal.NestedStringAccessor? getTransactionalId = null) =>
        new TransactionListingMarshal.Accessors(
            (result, index) => rows[index].Listings.Count,
            getTransactionalId
                ?? ((result, index, listing) => Pin(rows[index].Listings[listing].TransactionalId)),
            (result, index, listing) => rows[index].Listings[listing].ProducerId,
            (result, index, listing) => Pin(rows[index].Listings[listing].State));

    private TransactionListingMarshal.NestedStringAccessor PoisonListingString(
        string poisoned, string name, ListingRow[] rows, Func<Listing, string> read) =>
        poisoned == name
            ? (result, index, listing) =>
                throw new InvalidOperationException(name + " reader reached")
            : (result, index, listing) => Pin(read(rows[index].Listings[listing]));

    private TransactionDescription ReadRow(int index, Row[] rows) =>
        TransactionDescriptionMarshal.ReadDescription(IntPtr.Zero, index, Accessors(rows));

    private TransactionDescriptionMarshal.Accessors Accessors(
        Row[] rows, TransactionDescriptionMarshal.NestedStringAccessor? getTopic = null) =>
        new TransactionDescriptionMarshal.Accessors(
            (result, index) => rows[index].CoordinatorId,
            (result, index) => Pin(rows[index].State),
            (result, index) => rows[index].ProducerId,
            (result, index) => rows[index].ProducerEpoch,
            (result, index) => rows[index].TimeoutMs,
            (IntPtr result, int index, out long value) =>
            {
                value = rows[index].StartTimeMs;
                return rows[index].HasStartTime;
            },
            (result, index) => rows[index].Partitions.Count,
            getTopic ?? ((result, index, partition) => Pin(rows[index].Partitions[partition].Topic)),
            (result, index, partition) => rows[index].Partitions[partition].Partition);

    private static Func<IntPtr, int, T> Poison<T>(
        string poisoned, string name, Func<IntPtr, int, T> accessor) =>
        poisoned == name
            ? (result, index) => throw new InvalidOperationException(name + " reader reached")
            : accessor;

    private KeyedResultMarshal.IndexedAccessor PoisonState(string poisoned, Row[] rows) =>
        poisoned == "state"
            ? (result, index) => throw new InvalidOperationException("state reader reached")
            : (result, index) => Pin(rows[index].State);

    private static TransactionDescriptionMarshal.OptionalInt64Accessor PoisonOptional(
        string poisoned, Row[] rows)
    {
        if (poisoned == "start time")
        {
            return (IntPtr result, int index, out long value) =>
                throw new InvalidOperationException("start time reader reached");
        }

        return (IntPtr result, int index, out long value) =>
        {
            value = rows[index].StartTimeMs;
            return rows[index].HasStartTime;
        };
    }

    private TransactionDescriptionMarshal.NestedStringAccessor PoisonNestedString(
        string poisoned, Row[] rows) =>
        poisoned == "partition topic"
            ? (result, index, partition) =>
                throw new InvalidOperationException("partition topic reader reached")
            : (result, index, partition) => Pin(rows[index].Partitions[partition].Topic);

    private static TransactionDescriptionMarshal.NestedInt32Accessor PoisonNestedInt32(
        string poisoned, Row[] rows) =>
        poisoned == "partition id"
            ? (result, index, partition) =>
                throw new InvalidOperationException("partition id reader reached")
            : (result, index, partition) => rows[index].Partitions[partition].Partition;

    /// <summary>
    /// A borrowed NUL-terminated UTF-8 pointer for a stand-in accessor, interned for the
    /// lifetime of this test instance — a stand-in row is read many times and the reader
    /// copies out, so releasing per call would dangle rather than save.
    /// </summary>
    private IntPtr Pin(string value)
    {
        if (!_pins.TryGetValue(value, out Utf8Marshal.PinnedUtf8String? pin))
        {
            pin = Utf8Marshal.Pin(value);
            _pins.Add(value, pin);
        }

        return pin.Pointer;
    }

    /// <summary>Releases the stand-in pins — xUnit disposes one instance per test.</summary>
    public void Dispose()
    {
        foreach (Utf8Marshal.PinnedUtf8String pin in _pins.Values)
        {
            pin.Dispose();
        }

        _pins.Clear();
    }

    private readonly Dictionary<string, Utf8Marshal.PinnedUtf8String> _pins =
        new Dictionary<string, Utf8Marshal.PinnedUtf8String>(StringComparer.Ordinal);
}
