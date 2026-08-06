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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for the M5/P6 commit family — the two confirming
/// <see cref="IAsyncConsumer.Commit(CancellationToken)"/> /
/// <see cref="IAsyncConsumer.Commit(IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, CancellationToken)"/>
/// overloads (Java <c>commitSync</c> / <c>commitSync(Map)</c>), the fire-and-forget
/// <see cref="IConsumerCommon.CommitAsync"/> (Java <c>commitAsync</c>), and the new public
/// <see cref="OffsetAndMetadata"/> constructor — exercised end to end through the
/// <b>public</b> <see cref="AsyncMockConsumer"/> / <see cref="IAsyncConsumer"/> surface
/// (PLAN §8). The marquee test is the non-empty <see cref="IAsyncConsumer.Committed"/>
/// round-trip that E1 (M5/P4) could only defer: commit real offsets via the new 5-array
/// marshaller, read them back via E1's copy-out.
/// </summary>
/// <remarks>
/// <para>
/// <b>Reachability (PLAN §8).</b> On the mock, both confirming commits and
/// <see cref="IConsumerCommon.CommitAsync"/> resolve <b>broker-free</b> —
/// <c>commit_async_impl</c> only checks <c>ensure_not_closed()</c> and then stores into
/// the committed map, never faulting on a broker outcome. <see cref="IAsyncConsumer.Commit"/>
/// with offsets populates the mock's committed map, so a subsequent
/// <see cref="IAsyncConsumer.Committed"/> on an <b>assigned</b> partition reads the exact
/// stored value (offset, metadata, and leader epoch all round-trip faithfully — verified
/// against <c>mock_consumer.rs</c> + the FFI <c>read_offset_map</c>, which builds the
/// <c>OffsetAndMetadata</c> from the parallel arrays). An <b>operational</b>
/// <see cref="KafkaException"/> commit fault is therefore <b>not reachable broker-free</b>
/// on the mock (the only mock failure — a closed consumer — is intercepted as
/// <see cref="ObjectDisposedException"/> before the native call), so that assertion is a
/// documented D-Q4 reachability limit rather than a dropped requirement.
/// </para>
/// <para>
/// Every awaited op runs under a <see cref="TestTimeout"/> hang guard. Serial execution is
/// assembly-wide (<c>CollectionBehavior(DisableTestParallelization = true)</c> in
/// <c>AssemblyInfo.cs</c>, the D8.8 gate), so this class inherits it without a per-class
/// attribute.
/// </para>
/// </remarks>
public sealed class PublicConsumerCommitTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "commit-topic";

    // ---- Marquee: the non-empty Committed round-trip (unblocks E1's deferred test) ----

    [Fact]
    public async Task Commit_ThenCommitted_RoundTripsOffsetMetadataAndEpoch()
    {
        // The full round-trip E1 (M5/P4) could not reach: the 5-array WithPinnedCommitOffsets
        // marshaller on the way in (non-null metadata + a real leader epoch) AND E1's
        // OffsetMap copy-out on the way out, with REAL data. committed() returns the exact
        // stored value ONLY for an assigned TP — so Assign first (public async Assign,
        // broker-free), matching the mock's `subscriptions.is_assigned(tp)` gate.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        TopicPartition tp = new TopicPartition(Topic, 0);
        await AssignOf(consumer, new[] { tp });

        // Exercises the new public OffsetAndMetadata constructor (offset, metadata, epoch).
        OffsetAndMetadata oam = new OffsetAndMetadata(42, "meta-x", 7);
        await CommitOffsetsOf(
            consumer, new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = oam });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = await CommittedOf(consumer, new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(42, result[tp].Offset);
        Assert.Equal("meta-x", result[tp].Metadata);
        // Leader epoch round-trips faithfully (7 in, 7 out): the FFI read_offset_map builds
        // OffsetAndMetadata::with_leader_epoch(offset, Some(7), meta) from leader_epochs[i] >= 0,
        // the mock stores it, and committed() clones it back — verified against
        // src/ffi/consumer.rs + src/consumer/mock_consumer.rs.
        Assert.Equal(7, result[tp].LeaderEpoch);
    }

    [Fact]
    public async Task Commit_NullMetadataAndEpoch_RoundTripsAsEmptyMetadataAndNullEpoch()
    {
        // The null-metadata / null-epoch variant (PLAN §8.2): the ctor coerces null metadata
        // to "" and SnapshotCommitOffsets maps a null epoch to the -1 sentinel on the wire;
        // the copy-out maps the epoch -1 sentinel back to null (OffsetMapMarshal honors the
        // presence flag). Metadata reads back as "".
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        TopicPartition tp = new TopicPartition(Topic, 1);
        await AssignOf(consumer, new[] { tp });

        OffsetAndMetadata oam = new OffsetAndMetadata(100);
        await CommitOffsetsOf(
            consumer, new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = oam });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = await CommittedOf(consumer, new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(100, result[tp].Offset);
        Assert.Equal(string.Empty, result[tp].Metadata);
        Assert.Null(result[tp].LeaderEpoch);
    }

    // ---- Happy paths (broker-free, PLAN §8.3) ----

    [Fact]
    public async Task Commit_NoOffsets_ResolvesBrokerFree()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        // The confirming commit of the current positions — resolves on the mock (the awaited
        // Task completes under the hang guard), no assignment required.
        await CommitOf(consumer);
    }

    [Fact]
    public async Task Commit_EmptyMap_ResolvesBrokerFree()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        // count == 0 — a valid pass-through through WithPinnedCommitOffsets, never a throw.
        await CommitOffsetsOf(consumer, new Dictionary<TopicPartition, OffsetAndMetadata>());
    }

    [Fact]
    public void CommitAsync_ReturnsImmediatelyWithoutThrowing()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        // Fire-and-forget: sync, non-blocking, returns without throwing on the mock. No
        // CancellationToken (nothing to cancel). Guarded by the hang deadline in case the
        // sync FFI call were ever to block.
        TestTimeout.Run(() => consumer.CommitAsync(), s_deadline);
    }

    [Fact]
    public void CommitAsync_ViaIConsumerCommonInterface_ReturnsWithoutThrowing()
    {
        using AsyncMockConsumer mock = new AsyncMockConsumer();
        IConsumerCommon consumer = mock;

        // CommitAsync lives on IConsumerCommon (the shared non-blocking base) — reachable
        // through the base interface, confirming the user-resolved placement (PLAN §5).
        TestTimeout.Run(() => consumer.CommitAsync(), s_deadline);
    }

    [Fact]
    public async Task Commit_ViaIAsyncConsumerInterface_ResolvesBrokerFree()
    {
        using AsyncMockConsumer mock = new AsyncMockConsumer();
        IAsyncConsumer consumer = mock;

        // Both confirming overloads reachable through the interface.
        await CommitOf(consumer);
        await CommitOffsetsOf(consumer, new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(1, "m"),
        });
    }

    // ---- Preconditions (§B5), each BEFORE any native call (PLAN §8.3) ----

    [Fact]
    public async Task Commit_NullOffsets_ThrowsArgumentNull()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Commit(null!));
    }

    [Fact]
    public async Task Commit_NullElementTopic_ThrowsArgument()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way to
        // present a null element topic without TopicPartition's own ctor validation firing.
        // SnapshotCommitOffsets rejects it (the shipped offset-query precedent).
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [default] = new OffsetAndMetadata(1),
        };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Commit(offsets));
    }

    [Fact]
    public void Commit_NegativePartition_RejectedByTopicPartitionCtor()
    {
        // TopicPartition's ctor rejects a negative partition itself (the Position/Assign
        // precedent), so a negative value cannot reach SnapshotCommitOffsets through a
        // constructed TopicPartition — assert the ctor guard is that same exception type and
        // message, which is what the commit would throw were the value smuggled in.
        ArgumentOutOfRangeException ex =
            Assert.Throws<ArgumentOutOfRangeException>(() => new TopicPartition(Topic, -1));
        Assert.Contains("Partition must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Commit_NullOffsetValue_ThrowsArgument()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = null!,
        };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Commit(offsets));
    }

    // ---- OffsetAndMetadata public ctor (PLAN §8.3) ----

    [Fact]
    public void OffsetAndMetadata_NegativeOffset_ThrowsArgumentOutOfRangeWithJavaMessage()
    {
        // Java's exact message "Invalid negative offset" is asserted (DoD §3 — error message
        // content is part of the behavioral contract). ArgumentOutOfRangeException appends
        // "(Parameter 'offset')\nActual value was -1." so assert with Contains, not Equal.
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new OffsetAndMetadata(-1));

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("Invalid negative offset", ex.Message);
    }

    [Fact]
    public void OffsetAndMetadata_NullMetadata_CoercedToEmptyAndNullEpoch()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(5);

        Assert.Equal(5, value.Offset);
        Assert.Equal(string.Empty, value.Metadata);
        Assert.Null(value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndMetadata_AllThreeSet_AreStored()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(5, "m", 3);

        Assert.Equal(5, value.Offset);
        Assert.Equal("m", value.Metadata);
        Assert.Equal(3, value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndMetadata_ZeroOffset_IsAccepted()
    {
        // Boundary: offset 0 is non-negative (Java accepts it), so no throw.
        OffsetAndMetadata value = new OffsetAndMetadata(0, "boundary");

        Assert.Equal(0, value.Offset);
        Assert.Equal("boundary", value.Metadata);
    }

    // ---- Lifecycle: post-dispose (all three members, PLAN §8.3) ----

    [Fact]
    public async Task Commit_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Commit());
    }

    [Fact]
    public async Task CommitOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(1),
            }));
    }

    [Fact]
    public async Task CommitAsync_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => consumer.CommitAsync());
    }

    // ---- Cancellation: pre-canceled token on Commit → OperationCanceledException (§B5) ----

    [Fact]
    public async Task Commit_PreCanceledToken_ThrowsOperationCanceled()
    {
        // An already-canceled token is honored synchronously BEFORE any native call
        // (OperationCanceledException, distinct from a wakeup KafkaException), via
        // ThrowIfCancellationRequested in SubmitVoidOperation — user-initiated cancellation,
        // NOT a timeout. CommitAsync takes NO CancellationToken (fire-and-forget), so it has
        // no cancellation path.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Commit(cts.Token));
    }

    [Fact]
    public async Task CommitOffsets_PreCanceledToken_ThrowsOperationCanceled()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.Commit(
                new Dictionary<TopicPartition, OffsetAndMetadata>
                {
                    [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(1),
                },
                cts.Token));
    }

    // ---- Helpers (every awaited op under the TestTimeout hang guard) ----

    private static async Task AssignOf(IAsyncConsumer consumer, IReadOnlyCollection<TopicPartition> partitions) =>
        await TestTimeout.Run(() => consumer.Assign(partitions), s_deadline);

    private static async Task CommitOf(IAsyncConsumer consumer) =>
        await TestTimeout.Run(() => consumer.Commit(), s_deadline);

    private static async Task CommitOffsetsOf(
        IAsyncConsumer consumer, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        await TestTimeout.Run(() => consumer.Commit(offsets), s_deadline);

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> CommittedOf(
        IAsyncConsumer consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = null!;
        await TestTimeout.Run(async () => result = await consumer.Committed(partitions), s_deadline);
        return result;
    }
}
