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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What M15/P4 Stage 2's inputs become at the P/Invoke — above all, how each of the
/// <b>seven</b> <see cref="OffsetSpec"/> kinds is encoded.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The seven kinds are walked exhaustively here, as a (flag, value) PAIR.</b> Six of
/// them collapse onto overlapping numbers with the seventh, so asserting the value alone —
/// or sampling a few kinds — would pass against a binding that dropped
/// <c>is_timestamp</c> entirely. The behavioural half of that claim, through the real ABI,
/// is <c>PublicAdminReassignmentsOffsetsTests.ListOffsets_ForTimestampAtTheEarliestSentinel_DiffersFromEarliest</c>.
/// </para>
/// <para>
/// ⚠ The Rust <c>MockAdminClient</c> ignores the <em>options</em> it is handed, so the
/// timeout and the isolation level are read here, at the seam, where they are facts rather
/// than inferences.
/// </para>
/// </remarks>
public sealed class AdminP4Stage2SubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// ⚠⚠ <b>Every one of the seven kinds, as the PAIR the ABI reads.</b> The six
    /// no-argument kinds clear the flag and carry their <c>ListOffsets</c> wire sentinel;
    /// <see cref="OffsetSpec.ForTimestamp"/> sets it and carries its own number.
    /// </summary>
    /// <remarks>
    /// The sentinels are Java's, from <c>KafkaAdminClient.getOffsetFromSpec</c>
    /// (<c>KafkaAdminClient.java:5176-5191</c>). ⚠ Java has no <c>LatestSpec</c> branch
    /// there — it falls out of the chain to <c>LATEST_TIMESTAMP</c> at <c>:5190</c> — so
    /// <c>-1</c> is asserted here as the value that fall-through produces, reached by an
    /// explicit match rather than by a default arm.
    /// </remarks>
    [Theory]
    [InlineData("latest", false, -1L)]
    [InlineData("earliest", false, -2L)]
    [InlineData("maxTimestamp", false, -3L)]
    [InlineData("earliestLocal", false, -4L)]
    [InlineData("latestTiered", false, -5L)]
    [InlineData("earliestPendingUpload", false, -6L)]
    [InlineData("timestamp:1700000000000", true, 1700000000000L)]
    public void EveryOffsetSpecKind_ReachesTheSubmitAsAFlagAndValuePair(
        string kind, bool expectedFlag, long expectedValue)
    {
        Captured captured = CaptureOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [new TopicPartition("p4s2-spec", 0)] = Spec(kind),
            },
            options: null);

        Assert.Equal(1, captured.Count);
        Assert.Equal(expectedFlag, captured.IsTimestamp![0]);
        Assert.Equal(expectedValue, captured.SpecTimestamps![0]);
    }

    /// <summary>
    /// ⚠⚠ <b>The collision, at the seam: <c>ForTimestamp(-2)</c> and
    /// <see cref="OffsetSpec.Earliest"/> carry the SAME number and DIFFERENT flags.</b>
    /// </summary>
    /// <remarks>
    /// Asserted in one request so the two rows are directly comparable. This is the case
    /// the header calls out — "both yield <c>-2</c>, yet Java treats them differently up to
    /// that point" — and it is why the value alone can never be the encoding.
    /// </remarks>
    [Fact]
    public void ForTimestampAtASentinelValue_DiffersFromTheSentinelKind_OnlyByTheFlag()
    {
        TopicPartition viaTimestamp = new TopicPartition("p4s2-collide", 0);
        TopicPartition viaEarliest = new TopicPartition("p4s2-collide", 1);

        Captured captured = CaptureOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [viaTimestamp] = OffsetSpec.ForTimestamp(-2),
                [viaEarliest] = OffsetSpec.Earliest(),
            },
            options: null);

        int timestampRow = Array.IndexOf(captured.Partitions!, 0);
        int earliestRow = Array.IndexOf(captured.Partitions!, 1);

        // Same number…
        Assert.Equal(-2L, captured.SpecTimestamps![timestampRow]);
        Assert.Equal(-2L, captured.SpecTimestamps[earliestRow]);

        // …different flag. That is the ENTIRE difference between the two requests.
        Assert.True(captured.IsTimestamp![timestampRow]);
        Assert.False(captured.IsTimestamp[earliestRow]);
    }

    /// <summary>
    /// Every sentinel value is reachable as a timestamp too, and each keeps the flag set —
    /// so the collision is not special to <c>-2</c>.
    /// </summary>
    [Theory]
    [InlineData(-1L)]
    [InlineData(-2L)]
    [InlineData(-3L)]
    [InlineData(-4L)]
    [InlineData(-5L)]
    [InlineData(-6L)]
    [InlineData(0L)]
    public void ForTimestamp_KeepsTheFlagSetForAnyValue(long timestamp)
    {
        Captured captured = CaptureOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [new TopicPartition("p4s2-any", 0)] = OffsetSpec.ForTimestamp(timestamp),
            },
            options: null);

        Assert.True(captured.IsTimestamp![0], "a timestamp query stays a timestamp query at any value");
        Assert.Equal(timestamp, captured.SpecTimestamps![0]);
    }

    /// <summary>
    /// The isolation level crosses as Java's <c>id()</c>, in its own argument slot, and a
    /// value outside the enum is rejected <b>before</b> the native call.
    /// </summary>
    [Fact]
    public void IsolationLevel_ReachesTheSubmit_AndAnUndefinedOneIsRejected()
    {
        Dictionary<TopicPartition, OffsetSpec> request = new Dictionary<TopicPartition, OffsetSpec>
        {
            [new TopicPartition("p4s2-iso", 0)] = OffsetSpec.Latest(),
        };

        Assert.Equal(0, CaptureOffsets(request, options: null).IsolationLevel);
        Assert.Equal(0, CaptureOffsets(request, new ListOffsetsOptions()).IsolationLevel);
        Assert.Equal(
            1,
            CaptureOffsets(
                request, new ListOffsetsOptions { IsolationLevel = IsolationLevel.ReadCommitted }).IsolationLevel);

        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        bool submitted = false;
        ArgumentOutOfRangeException rejected = Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.ListOffsets(
                request,
                new ListOffsetsOptions { IsolationLevel = (IsolationLevel)7 },
                (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs,
                    isolationLevel, callback, userData) => submitted = true));

        Assert.Equal("options", rejected.ParamName);
        Assert.False(submitted, "an undefined isolation level must be rejected before the native call");
    }

    /// <summary>
    /// ⚠ <c>listPartitionReassignments</c>' <see langword="null"/> selection is Java's
    /// <c>Optional.empty()</c> — "every ongoing reassignment" — and an <b>empty</b>
    /// collection is not.
    /// </summary>
    [Fact]
    public void ListPartitionReassignments_NullSelectionIsAllPartitions_AndAnEmptyOneIsNot()
    {
        Captured all = CaptureReassignments(partitions: null, options: null);
        Assert.True(all.AllPartitions, "a null selection is Java's Optional.empty()");
        Assert.Equal(0, all.Count);

        Captured none = CaptureReassignments(Array.Empty<TopicPartition>(), options: null);
        Assert.False(none.AllPartitions, "an empty selection asks about no partitions");
        Assert.Equal(0, none.Count);

        Captured some = CaptureReassignments(new[] { new TopicPartition("t", 3) }, options: null);
        Assert.False(some.AllPartitions);
        Assert.Equal(1, some.Count);
        Assert.Equal(new[] { "t" }, some.Topics);
        Assert.Equal(new[] { 3 }, some.Partitions);
    }

    /// <summary>The selection is de-duplicated, because Java's parameter is a <c>Set</c>.</summary>
    [Fact]
    public void ListPartitionReassignments_TheSelectionIsDeduplicated()
    {
        Captured captured = CaptureReassignments(
            new[]
            {
                new TopicPartition("p4s2-dedup", 1),
                new TopicPartition("p4s2-dedup", 0),
                new TopicPartition("p4s2-dedup", 1),
            },
            options: null);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { 1, 0 }, captured.Partitions);
    }

    /// <summary>
    /// A topic partition with a <see langword="null"/> topic is rejected before the native
    /// call, on both RPCs — the ABI would <em>silently skip</em> such an entry.
    /// </summary>
    [Fact]
    public void BothRpcs_RejectANullTopic_BeforeTheNativeCall()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ArgumentException listing = Assert.Throws<ArgumentException>(() =>
            admin.ListPartitionReassignments(
                new[] { default(TopicPartition) },
                options: null,
                (nativeHandle, allPartitions, topics, partitions, count, timeoutMs, callback, userData) =>
                    submitted = true));
        Assert.Equal("partitions", listing.ParamName);

        ArgumentException offsets = Assert.Throws<ArgumentException>(() =>
            admin.ListOffsets(
                new Dictionary<TopicPartition, OffsetSpec> { [default] = OffsetSpec.Latest() },
                options: null,
                (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs,
                    isolationLevel, callback, userData) => submitted = true));
        Assert.Equal("topicPartitionOffsets", offsets.ParamName);

        Assert.False(submitted, "both must be rejected before the native call");
    }

    /// <summary>A null spec, and a null map, are rejected.</summary>
    [Fact]
    public void ListOffsets_RejectsANullSpecAndANullMap()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        ArgumentException nullSpec = Assert.Throws<ArgumentException>(() =>
            admin.ListOffsets(
                new Dictionary<TopicPartition, OffsetSpec> { [new TopicPartition("t", 0)] = null! },
                options: null,
                (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs,
                    isolationLevel, callback, userData) =>
                { }));
        Assert.Equal("topicPartitionOffsets", nullSpec.ParamName);

        Assert.Equal(
            "topicPartitionOffsets",
            Assert.Throws<ArgumentNullException>(() => admin.ListOffsets(null!, options: null)).ParamName);
    }

    /// <summary>
    /// A <see langword="null"/> timeout maps to a <b>negative</b> <c>timeout_ms</c> — the
    /// ABI's "unset" — never to <c>0</c>, which means "do not wait"; an explicit one is
    /// forwarded verbatim; and a negative one is rejected.
    /// </summary>
    [Fact]
    public void Timeouts_MapNullToANegative_AndForwardTheRestVerbatim()
    {
        Dictionary<TopicPartition, OffsetSpec> request = new Dictionary<TopicPartition, OffsetSpec>
        {
            [new TopicPartition("p4s2-timeout", 0)] = OffsetSpec.Latest(),
        };

        Assert.True(CaptureOffsets(request, options: null).TimeoutMs < 0);
        Assert.Equal(0, CaptureOffsets(request, new ListOffsetsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(4_242, CaptureOffsets(request, new ListOffsetsOptions { TimeoutMs = 4_242 }).TimeoutMs);

        Assert.True(CaptureReassignments(null, options: null).TimeoutMs < 0);
        Assert.Equal(
            0, CaptureReassignments(null, new ListPartitionReassignmentsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            5_353,
            CaptureReassignments(null, new ListPartitionReassignmentsOptions { TimeoutMs = 5_353 }).TimeoutMs);

        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.ListOffsets(request, new ListOffsetsOptions { TimeoutMs = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.ListPartitionReassignments(null, new ListPartitionReassignmentsOptions { TimeoutMs = -1 }));
    }

    /// <summary>
    /// ⚠ <b>Each RPC publishes the completion bridge its Java shape requires</b> — the
    /// routing decision made visible, as Stage 1's twins are.
    /// </summary>
    /// <remarks>
    /// <c>listPartitionReassignments</c> holds one aggregate future
    /// (<c>ListPartitionReassignmentsResult.java:31</c>) so it registers a
    /// <see cref="SingleAdminOperation{TValue}"/>; <c>listOffsets</c> holds one future per
    /// partition (<c>ListOffsetsResult.java:32</c>) so it registers the per-key bridge —
    /// <b>with a value</b>, unlike Stage 1's void one. Swapping them compiles and would
    /// leave the awaiters hanging, so each is then driven through its production trampoline.
    /// </remarks>
    [Fact]
    public async Task EachRpc_RegistersTheBridgeItsJavaShapeRequires()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        object? listingContext = null;
        IntPtr listingUserData = IntPtr.Zero;
        ListPartitionReassignmentsResult listing = admin.ListPartitionReassignments(
            null,
            options: null,
            (nativeHandle, allPartitions, topics, partitions, count, timeoutMs, callback, userData) =>
            {
                listingUserData = userData;
                listingContext = GCHandle.FromIntPtr(userData).Target;
            });

        Assert.IsType<SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>>(
            listingContext);

        TopicPartition partition = new TopicPartition("p4s2-routing", 0);
        object? offsetsContext = null;
        IntPtr offsetsUserData = IntPtr.Zero;
        ListOffsetsResult offsets = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [partition] = OffsetSpec.Latest() },
            options: null,
            (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs, isolationLevel,
                callback, userData) =>
            {
                offsetsUserData = userData;
                offsetsContext = GCHandle.FromIntPtr(userData).Target;
            });

        Assert.IsType<KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>(
            offsetsContext);

        // Each production trampoline must be able to complete THAT context.
        AdminCallbacks.ListPartitionReassignments(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(3, "routing"), listingUserData);
        AdminCallbacks.ListOffsets(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(4, "routing"), offsetsUserData);

        Assert.Equal(
            3,
            (await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(listing.Reassignments), s_deadline)).Code);
        Assert.Equal(
            4,
            (await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => offsets.PartitionResult(partition)),
                s_deadline)).Code);
    }

    private static OffsetSpec Spec(string kind) => kind switch
    {
        "latest" => OffsetSpec.Latest(),
        "earliest" => OffsetSpec.Earliest(),
        "maxTimestamp" => OffsetSpec.MaxTimestamp(),
        "earliestLocal" => OffsetSpec.EarliestLocal(),
        "latestTiered" => OffsetSpec.LatestTiered(),
        "earliestPendingUpload" => OffsetSpec.EarliestPendingUpload(),
        _ => OffsetSpec.ForTimestamp(long.Parse(
            kind.Substring("timestamp:".Length), System.Globalization.CultureInfo.InvariantCulture)),
    };

    private static Captured CaptureOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> request, ListOffsetsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        IntPtr toRelease = IntPtr.Zero;
        admin.ListOffsets(
            request,
            options,
            (nativeHandle, topics, partitionIds, isTimestamp, specTimestamps, count, timeoutMs, isolationLevel,
                callback, userData) =>
            {
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;
                captured.IsolationLevel = isolationLevel;
                captured.IsTimestamp = (bool[])isTimestamp.Clone();
                captured.SpecTimestamps = (long[])specTimestamps.Clone();
                captured.Partitions = (int[])partitionIds.Clone();

                string[] names = new string[count];
                for (int i = 0; i < count; i++)
                {
                    names[i] = Utf8Marshal.PtrToString(topics[i])!;
                }

                captured.Topics = names;
                toRelease = userData;
            });

        AdminCallbacks.ListOffsets(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(1, "captured"), toRelease);
        return captured;
    }

    private static Captured CaptureReassignments(
        IReadOnlyCollection<TopicPartition>? partitions, ListPartitionReassignmentsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        IntPtr toRelease = IntPtr.Zero;
        admin.ListPartitionReassignments(
            partitions,
            options,
            (nativeHandle, allPartitions, topics, partitionIds, count, timeoutMs, callback, userData) =>
            {
                captured.AllPartitions = allPartitions;
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;
                captured.Partitions = (int[])partitionIds.Clone();

                string[] names = new string[count];
                for (int i = 0; i < count; i++)
                {
                    names[i] = Utf8Marshal.PtrToString(topics[i])!;
                }

                captured.Topics = names;
                toRelease = userData;
            });

        AdminCallbacks.ListPartitionReassignments(
            IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(1, "captured"), toRelease);
        return captured;
    }

    private sealed class Captured
    {
        internal bool AllPartitions { get; set; }

        internal int Count { get; set; }

        internal int TimeoutMs { get; set; }

        internal int IsolationLevel { get; set; }

        internal string[]? Topics { get; set; }

        internal int[]? Partitions { get; set; }

        internal bool[]? IsTimestamp { get; set; }

        internal long[]? SpecTimestamps { get; set; }
    }
}
