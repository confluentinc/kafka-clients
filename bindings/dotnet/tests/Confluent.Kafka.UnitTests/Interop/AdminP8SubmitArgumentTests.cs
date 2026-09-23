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
/// What M15/P8's inputs become on the wire. Both mocks throw Java's own "Not implemented yet"
/// for all six RPCs, so every option and scalar is asserted at the submit seam rather than
/// end to end.
/// </summary>
/// <remarks>
/// ⚠ Every pointer is decoded <b>inside</b> the stand-in: production unpins in its
/// <c>finally</c> (ffi §A4), so a read after the submit returns is a use-after-unpin.
/// </remarks>
public sealed class AdminP8SubmitArgumentTests
{
    // ---- abortTransaction: six scalars, and none of them interchangeable -----------------

    /// <summary>
    /// Every scalar reaches the submit in its own slot. The four numbers are deliberately
    /// distinct so a transposition between <c>producerId</c>, <c>producerEpoch</c>,
    /// <c>coordinatorEpoch</c> and <c>partition</c> fails rather than cancelling out.
    /// </summary>
    [Fact]
    public void AbortTransaction_ForwardsEveryScalarInItsOwnSlot()
    {
        CapturedAbort captured = CaptureAbort(
            new AbortTransactionSpec(new TopicPartition("txn-topic", 3), 91_234L, 7, 42),
            new AbortTransactionOptions { TimeoutMs = 500 });

        Assert.Equal("txn-topic", captured.Topic);
        Assert.Equal(3, captured.Partition);
        Assert.Equal(91_234L, captured.ProducerId);
        Assert.Equal(7, captured.ProducerEpoch);
        Assert.Equal(42, captured.CoordinatorEpoch);
        Assert.Equal(500, captured.TimeoutMs);
    }

    /// <summary>
    /// A <see langword="null"/> options means "the client default", which the ABI spells as a
    /// <b>negative</b> timeout — a C# <c>int</c> defaulting to <c>0</c> would mean "0 ms".
    /// </summary>
    [Fact]
    public void AbortTransaction_NoOptions_SendsTheDefaultTimeoutSentinel() =>
        Assert.Equal(
            -1,
            CaptureAbort(new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1), null)
                .TimeoutMs);

    /// <summary>A zero timeout is a real request, not the unset sentinel.</summary>
    [Fact]
    public void AbortTransaction_ZeroTimeout_IsNotTheDefaultSentinel() =>
        Assert.Equal(
            0,
            CaptureAbort(
                new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1),
                new AbortTransactionOptions { TimeoutMs = 0 })
                .TimeoutMs);

    /// <summary>A non-ASCII topic survives the UTF-8 round trip (ffi §A3).</summary>
    [Fact]
    public void AbortTransaction_NonAsciiTopic_RoundTrips() =>
        Assert.Equal(
            "témas-ünïcode-🎉",
            CaptureAbort(
                new AbortTransactionSpec(new TopicPartition("témas-ünïcode-🎉", 0), 1L, 1, 1), null)
                .Topic);

    /// <summary>
    /// The epoch travels as an <c>int32_t</c> but is a <see langword="short"/> managed-side, so
    /// a negative epoch is sign-extended rather than reinterpreted as a large positive value —
    /// which is what <c>ProducerIdAndEpoch.NONE</c>'s <c>-1</c> relies on.
    /// </summary>
    [Fact]
    public void AbortTransaction_NegativeEpoch_IsSignExtendedNotWidened() =>
        Assert.Equal(
            -1,
            CaptureAbort(new AbortTransactionSpec(new TopicPartition("t", 0), 1L, -1, 1), null)
                .ProducerEpoch);

    // ---- forceTerminateTransaction: one string and a timeout -----------------------------

    [Fact]
    public void ForceTerminateTransaction_ForwardsTheIdAndTimeout()
    {
        (string? Id, int TimeoutMs) captured = CaptureForceTerminate(
            "txn-a", new TerminateTransactionOptions { TimeoutMs = 250 });

        Assert.Equal("txn-a", captured.Id);
        Assert.Equal(250, captured.TimeoutMs);
    }

    /// <inheritdoc cref="AbortTransaction_NoOptions_SendsTheDefaultTimeoutSentinel"/>
    [Fact]
    public void ForceTerminateTransaction_NoOptions_SendsTheDefaultTimeoutSentinel() =>
        Assert.Equal(-1, CaptureForceTerminate("txn-a", null).TimeoutMs);

    /// <summary>
    /// The empty string is a legal id at the boundary and is <b>not</b> normalized to the
    /// NULL the ABI rejects.
    /// </summary>
    [Fact]
    public void ForceTerminateTransaction_EmptyId_IsSentAsAnEmptyStringNotNull() =>
        Assert.Equal(string.Empty, CaptureForceTerminate(string.Empty, null).Id);

    [Fact]
    public void ForceTerminateTransaction_NonAsciiId_RoundTrips() =>
        Assert.Equal("txn-ünïcode-🎉", CaptureForceTerminate("txn-ünïcode-🎉", null).Id);

    // ---- fenceProducers: one string array -------------------------------------------------

    [Fact]
    public void FenceProducers_ForwardsEveryIdAndTheCount()
    {
        (IReadOnlyList<string?> Ids, int Count, int TimeoutMs) captured = CaptureFence(
            new[] { "txn-y", "txn-x" }, new FenceProducersOptions { TimeoutMs = 750 });

        Assert.Equal(new[] { "txn-y", "txn-x" }, captured.Ids);
        Assert.Equal(2, captured.Count);
        Assert.Equal(750, captured.TimeoutMs);
    }

    /// <summary>A repeated id is collapsed before the submit, as Java's map-keyed result is.</summary>
    [Fact]
    public void FenceProducers_DeduplicatesBeforeTheSubmit()
    {
        (IReadOnlyList<string?> Ids, int Count, int TimeoutMs) captured =
            CaptureFence(new[] { "txn-a", "txn-a", "txn-b" }, null);

        Assert.Equal(new[] { "txn-a", "txn-b" }, captured.Ids);
        Assert.Equal(2, captured.Count);
    }

    /// <inheritdoc cref="AbortTransaction_NoOptions_SendsTheDefaultTimeoutSentinel"/>
    [Fact]
    public void FenceProducers_NoOptions_SendsTheDefaultTimeoutSentinel() =>
        Assert.Equal(-1, CaptureFence(new[] { "txn-a" }, null).TimeoutMs);

    /// <summary>An empty request sends a zero count, not a stale one.</summary>
    [Fact]
    public void FenceProducers_EmptyRequest_SendsAZeroCount() =>
        Assert.Equal(0, CaptureFence(Array.Empty<string>(), null).Count);

    // ---- describeTransactions: the same string array, its own entry point -----------------

    [Fact]
    public void DescribeTransactions_ForwardsEveryIdAndTheCount()
    {
        (IReadOnlyList<string?> Ids, int Count, int TimeoutMs) captured = CaptureDescribeTransactions(
            new[] { "txn-b", "txn-a" }, new DescribeTransactionsOptions { TimeoutMs = 900 });

        Assert.Equal(new[] { "txn-b", "txn-a" }, captured.Ids);
        Assert.Equal(2, captured.Count);
        Assert.Equal(900, captured.TimeoutMs);
    }

    /// <inheritdoc cref="AbortTransaction_NoOptions_SendsTheDefaultTimeoutSentinel"/>
    [Fact]
    public void DescribeTransactions_NoOptions_SendsTheDefaultTimeoutSentinel() =>
        Assert.Equal(-1, CaptureDescribeTransactions(new[] { "txn-a" }, null).TimeoutMs);

    [Fact]
    public void DescribeTransactions_DeduplicatesBeforeTheSubmit()
    {
        (IReadOnlyList<string?> Ids, int Count, int TimeoutMs) captured =
            CaptureDescribeTransactions(new[] { "txn-a", "txn-a", "txn-b" }, null);

        Assert.Equal(new[] { "txn-a", "txn-b" }, captured.Ids);
        Assert.Equal(2, captured.Count);
    }

    [Fact]
    public void DescribeTransactions_EmptyRequest_SendsAZeroCount() =>
        Assert.Equal(0, CaptureDescribeTransactions(Array.Empty<string>(), null).Count);

    [Fact]
    public void DescribeTransactions_NonAsciiId_RoundTrips() =>
        Assert.Equal(
            new[] { "txn-ünïcode-🎉" },
            CaptureDescribeTransactions(new[] { "txn-ünïcode-🎉" }, null).Ids);

    // ---- describeProducers: parallel arrays and an OptionalInt discriminant ---------------

    /// <summary>
    /// ⚠ The partitions cross as <b>parallel arrays</b>, so a ragged request — two partitions
    /// of one topic and one of another — is what shows the columns stay in step
    /// (<c>test_mock_admin.c:5862-5864</c>).
    /// </summary>
    [Fact]
    public void DescribeProducers_ForwardsTheColumnsInStep()
    {
        CapturedDescribeProducers captured = CaptureDescribeProducers(
            new[]
            {
                new TopicPartition("alpha", 0),
                new TopicPartition("alpha", 4),
                new TopicPartition("beta", 2),
            },
            new DescribeProducersOptions { TimeoutMs = 600, BrokerId = 7 });

        Assert.Equal(new[] { "alpha", "alpha", "beta" }, captured.Topics);
        Assert.Equal(new[] { 0, 4, 2 }, captured.Partitions);
        Assert.Equal(3, captured.Count);
        Assert.Equal(600, captured.TimeoutMs);
    }

    /// <summary>
    /// ⚠⚠ <c>BrokerId</c> is Java's <c>OptionalInt</c>: the discriminant, not the value,
    /// decides. An unset option sends <c>false</c>, and <c>0</c> — a legal broker id — sends
    /// <c>true</c>.
    /// </summary>
    [Theory]
    [InlineData(null, false, 0)]
    [InlineData(0, true, 0)]
    [InlineData(7, true, 7)]
    [InlineData(-1, true, -1)]
    public void DescribeProducers_BrokerIdCrossesAsADiscriminantPlusAValue(
        int? brokerId, bool expectedHasBrokerId, int expectedBrokerId)
    {
        CapturedDescribeProducers captured = CaptureDescribeProducers(
            new[] { new TopicPartition("t", 0) },
            new DescribeProducersOptions { BrokerId = brokerId });

        Assert.Equal(expectedHasBrokerId, captured.HasBrokerId);
        Assert.Equal(expectedBrokerId, captured.BrokerId);
    }

    /// <summary>A null options sends no broker id at all, and the default timeout sentinel.</summary>
    [Fact]
    public void DescribeProducers_NoOptions_SendsNoBrokerIdAndTheDefaultTimeout()
    {
        CapturedDescribeProducers captured =
            CaptureDescribeProducers(new[] { new TopicPartition("t", 0) }, null);

        Assert.False(captured.HasBrokerId);
        Assert.Equal(-1, captured.TimeoutMs);
    }

    /// <summary>A repeated partition collapses before the submit, as Java's map-keyed result does.</summary>
    [Fact]
    public void DescribeProducers_DeduplicatesBeforeTheSubmit()
    {
        CapturedDescribeProducers captured = CaptureDescribeProducers(
            new[]
            {
                new TopicPartition("alpha", 0),
                new TopicPartition("alpha", 0),
                new TopicPartition("alpha", 1),
            },
            null);

        Assert.Equal(new[] { "alpha", "alpha" }, captured.Topics);
        Assert.Equal(new[] { 0, 1 }, captured.Partitions);
        Assert.Equal(2, captured.Count);
    }

    [Fact]
    public void DescribeProducers_EmptyRequest_SendsAZeroCount() =>
        Assert.Equal(0, CaptureDescribeProducers(Array.Empty<TopicPartition>(), null).Count);

    [Fact]
    public void DescribeProducers_NonAsciiTopic_RoundTrips() =>
        Assert.Equal(
            new[] { "témas-ünïcode-🎉" },
            CaptureDescribeProducers(new[] { new TopicPartition("témas-ünïcode-🎉", 0) }, null)
                .Topics);

    // ---- listTransactions: two filters, each with its own count ---------------------------

    /// <summary>
    /// ⚠⚠ <b>The two counts belong to different filters.</b> The request deliberately carries
    /// <b>2</b> states and <b>3</b> producer ids — the C suite's own shape
    /// (<c>test_mock_admin.c:5988-5989</c>) — so a transposed count is a length mismatch
    /// rather than a coincidence, and the states cross as Java's <c>toString()</c> spellings.
    /// </summary>
    [Fact]
    public void ListTransactions_ForwardsEachFilterWithItsOwnCount()
    {
        CapturedListTransactions captured = CaptureListTransactions(
            new ListTransactionsOptions
            {
                FilteredStates = new[] { TransactionState.Ongoing, TransactionState.PrepareAbort },
                FilteredProducerIds = new[] { 11L, 22L, 33L },
                FilteredDuration = 60_000L,
                FilteredTransactionalIdPattern = "txn-.*",
                TimeoutMs = 400,
            });

        Assert.Equal(new[] { "Ongoing", "PrepareAbort" }, captured.States);
        Assert.Equal(2, captured.StateCount);
        Assert.Equal(new[] { 11L, 22L, 33L }, captured.ProducerIds);
        Assert.Equal(3, captured.ProducerIdCount);
        Assert.Equal(60_000L, captured.DurationMs);
        Assert.Equal("txn-.*", captured.Pattern);
        Assert.Equal(400, captured.TimeoutMs);
    }

    /// <summary>
    /// ⚠ Every state crosses as Java's <c>toString()</c> spelling, not <c>name()</c> — the one
    /// place the binding <em>writes</em> the table <c>describeTransactions</c> reads.
    /// </summary>
    [Fact]
    public void ListTransactions_StatesCrossAsJavasToStringSpellings() =>
        Assert.Equal(
            new[]
            {
                "Ongoing", "PrepareAbort", "PrepareCommit", "CompleteAbort",
                "CompleteCommit", "Empty", "PrepareEpochFence", "Unknown",
            },
            CaptureListTransactions(
                new ListTransactionsOptions
                {
                    FilteredStates = new[]
                    {
                        TransactionState.Ongoing,
                        TransactionState.PrepareAbort,
                        TransactionState.PrepareCommit,
                        TransactionState.CompleteAbort,
                        TransactionState.CompleteCommit,
                        TransactionState.Empty,
                        TransactionState.PrepareEpochFence,
                        TransactionState.Unknown,
                    },
                })
                .States);

    /// <summary>
    /// ⚠⚠ A null options sends <b>every</b> neutral encoding: two empty filters, Java's own
    /// <c>-1</c> duration, a NULL pattern and the default timeout sentinel.
    /// </summary>
    [Fact]
    public void ListTransactions_NoOptions_SendsEveryNeutralEncoding()
    {
        CapturedListTransactions captured = CaptureListTransactions(null);

        Assert.Equal(0, captured.StateCount);
        Assert.Equal(0, captured.ProducerIdCount);
        Assert.Equal(-1L, captured.DurationMs);
        Assert.True(captured.PatternWasNull);
        Assert.Equal(-1, captured.TimeoutMs);
    }

    /// <summary>
    /// ⚠ <c>0</c> is a <b>real</b> duration filter, and only a negative value is neutral — so
    /// the default cannot be spelled as <c>default(long)</c>.
    /// </summary>
    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(-5L)]
    public void ListTransactions_DurationCrossesUnchanged(long duration) =>
        Assert.Equal(
            duration,
            CaptureListTransactions(new ListTransactionsOptions { FilteredDuration = duration })
                .DurationMs);

    /// <summary>
    /// ⚠⚠ A <b>null</b> pattern and an <b>empty</b> pattern are distinct at the boundary: the
    /// first is NULL (no filter), the second a string the broker evaluates.
    /// </summary>
    [Fact]
    public void ListTransactions_NullAndEmptyPatternAreDistinct()
    {
        Assert.True(
            CaptureListTransactions(
                new ListTransactionsOptions { FilteredTransactionalIdPattern = null })
                .PatternWasNull);

        CapturedListTransactions empty = CaptureListTransactions(
            new ListTransactionsOptions { FilteredTransactionalIdPattern = string.Empty });

        Assert.False(empty.PatternWasNull);
        Assert.Equal(string.Empty, empty.Pattern);
    }

    [Fact]
    public void ListTransactions_NonAsciiPattern_RoundTrips() =>
        Assert.Equal(
            "txn-ünïcode-🎉.*",
            CaptureListTransactions(
                new ListTransactionsOptions { FilteredTransactionalIdPattern = "txn-ünïcode-🎉.*" })
                .Pattern);

    // ---- helpers -------------------------------------------------------------------------

    private static CapturedListTransactions CaptureListTransactions(ListTransactionsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CapturedListTransactions captured = new CapturedListTransactions();
        admin.ListTransactions(
            options,
            (handle, states, stateCount, producerIds, producerIdCount, durationMs, pattern,
             timeoutMs, callback, data) =>
            {
                captured.States = states.Select(value => Utf8Marshal.PtrToString(value)).ToArray();
                captured.StateCount = stateCount;
                captured.ProducerIds = producerIds.ToArray();
                captured.ProducerIdCount = producerIdCount;
                captured.DurationMs = durationMs;
                captured.PatternWasNull = pattern == IntPtr.Zero;
                captured.Pattern = Utf8Marshal.PtrToString(pattern);
                captured.TimeoutMs = timeoutMs;
                captured.UserData = data;
            });

        AdminCallbacks.ListTransactions(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    private sealed class CapturedListTransactions
    {
        internal IReadOnlyList<string?> States { get; set; } = Array.Empty<string?>();

        internal int StateCount { get; set; }

        internal IReadOnlyList<long> ProducerIds { get; set; } = Array.Empty<long>();

        internal int ProducerIdCount { get; set; }

        internal long DurationMs { get; set; }

        internal bool PatternWasNull { get; set; }

        internal string? Pattern { get; set; }

        internal int TimeoutMs { get; set; }

        internal IntPtr UserData { get; set; }
    }

    private static CapturedDescribeProducers CaptureDescribeProducers(
        IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CapturedDescribeProducers captured = new CapturedDescribeProducers();
        admin.DescribeProducers(
            partitions,
            options,
            (handle, topics, partitionIds, count, hasBrokerId, brokerId, timeoutMs, callback, data) =>
            {
                captured.Topics = topics.Select(value => Utf8Marshal.PtrToString(value)).ToArray();
                captured.Partitions = partitionIds.ToArray();
                captured.Count = count;
                captured.HasBrokerId = hasBrokerId;
                captured.BrokerId = brokerId;
                captured.TimeoutMs = timeoutMs;
                captured.UserData = data;
            });

        AdminCallbacks.DescribeProducers(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    private static (IReadOnlyList<string?> Ids, int Count, int TimeoutMs) CaptureDescribeTransactions(
        IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IReadOnlyList<string?> ids = Array.Empty<string?>();
        int count = 0;
        int timeout = 0;
        IntPtr userData = IntPtr.Zero;

        admin.DescribeTransactions(
            transactionalIds,
            options,
            (handle, pinned, pinnedCount, timeoutMs, callback, data) =>
            {
                ids = pinned.Select(value => Utf8Marshal.PtrToString(value)).ToArray();
                count = pinnedCount;
                timeout = timeoutMs;
                userData = data;
            });

        AdminCallbacks.DescribeTransactions(IntPtr.Zero, CapturedError(), userData);
        return (ids, count, timeout);
    }

    private static (IReadOnlyList<string?> Ids, int Count, int TimeoutMs) CaptureFence(
        IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IReadOnlyList<string?> ids = Array.Empty<string?>();
        int count = 0;
        int timeout = 0;
        IntPtr userData = IntPtr.Zero;

        admin.FenceProducers(
            transactionalIds,
            options,
            (handle, pinned, pinnedCount, timeoutMs, callback, data) =>
            {
                ids = pinned.Select(value => Utf8Marshal.PtrToString(value)).ToArray();
                count = pinnedCount;
                timeout = timeoutMs;
                userData = data;
            });

        AdminCallbacks.FenceProducers(IntPtr.Zero, CapturedError(), userData);
        return (ids, count, timeout);
    }

    private static CapturedAbort CaptureAbort(
        AbortTransactionSpec spec, AbortTransactionOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CapturedAbort captured = new CapturedAbort();
        admin.AbortTransaction(
            spec,
            options,
            (handle, topic, partition, producerId, producerEpoch, coordinatorEpoch, timeoutMs,
             callback, userData) =>
            {
                captured.Topic = Utf8Marshal.PtrToString(topic);
                captured.Partition = partition;
                captured.ProducerId = producerId;
                captured.ProducerEpoch = producerEpoch;
                captured.CoordinatorEpoch = coordinatorEpoch;
                captured.TimeoutMs = timeoutMs;
                captured.UserData = userData;
            });

        // Settle the operation so its GCHandle and span-the-op reference are released.
        AdminCallbacks.AbortTransaction(CapturedError(), captured.UserData);
        return captured;
    }

    private static (string? Id, int TimeoutMs) CaptureForceTerminate(
        string transactionalId, TerminateTransactionOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        string? id = null;
        int timeout = 0;
        IntPtr userData = IntPtr.Zero;

        admin.ForceTerminateTransaction(
            transactionalId,
            options,
            (handle, pinnedId, timeoutMs, callback, data) =>
            {
                id = Utf8Marshal.PtrToString(pinnedId);
                timeout = timeoutMs;
                userData = data;
            });

        AdminCallbacks.ForceTerminateTransaction(CapturedError(), userData);
        return (id, timeout);
    }

    private static IntPtr CapturedError()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("captured");
        IntPtr error = NativeMethods.KafkaErrorNew(1, message.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private sealed class CapturedDescribeProducers
    {
        internal IReadOnlyList<string?> Topics { get; set; } = Array.Empty<string?>();

        internal IReadOnlyList<int> Partitions { get; set; } = Array.Empty<int>();

        internal int Count { get; set; }

        internal bool HasBrokerId { get; set; }

        internal int BrokerId { get; set; }

        internal int TimeoutMs { get; set; }

        internal IntPtr UserData { get; set; }
    }

    private sealed class CapturedAbort
    {
        internal string? Topic { get; set; }

        internal int Partition { get; set; }

        internal long ProducerId { get; set; }

        internal int ProducerEpoch { get; set; }

        internal int CoordinatorEpoch { get; set; }

        internal int TimeoutMs { get; set; }

        internal IntPtr UserData { get; set; }
    }
}
