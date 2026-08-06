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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M5/P1 consumer sync read surface — <see cref="IConsumerCommon.Assignment"/> /
/// <see cref="IConsumerCommon.Subscription"/> / <see cref="IConsumerCommon.Paused"/> /
/// <see cref="IConsumerCommon.EnforceRebalance"/> — through the <b>public</b>
/// <see cref="AsyncMockConsumer"/> surface, broker-free (PLAN §7). Every awaited op runs
/// under a <see cref="TestTimeout"/> hang guard; serial execution (D8.8) stays enabled at
/// the assembly level.
/// </summary>
/// <remarks>
/// <para>
/// The three getters return an <see cref="IReadOnlyCollection{T}"/> materialized from a
/// native <c>HashSet</c>, so element <b>order is not guaranteed</b> — every assertion
/// compares as a <b>set</b> (membership + count), never a sequence.
/// </para>
/// <para>
/// <b>Reachability limits (recorded, not silently skipped — PLAN §7).</b> Two Java
/// behaviors are not reachable broker-free with today's mock:
/// </para>
/// <list type="bullet">
/// <item>
/// A <b>non-empty <see cref="IConsumerCommon.Paused"/></b> needs a public <c>Pause</c>
/// (a later phase). The mock's <c>paused()</c> starts empty and nothing can add to it
/// yet, so the tested states are empty / assigned-but-not-paused.
/// </item>
/// <item>
/// A <b>deterministic forced-concurrency overlap</b> (the concurrent-null →
/// <see cref="InvalidOperationException"/> mapping) needs a controllable-duration
/// guard-holding op. Broker-free ops resolve instantly, so — exactly as the shipped
/// <see cref="IConsumerCommon.GroupMetadata"/> path (D-Q4) — the mapping is verified by
/// code inspection of the shared <c>ThrowIfConcurrentNull</c> helper (reused verbatim
/// from the group-metadata read); no flaky forced-overlap test is shipped. The reachable
/// seam (the read round-trips on a free guard) is asserted below.
/// </item>
/// </list>
/// </remarks>
public sealed class PublicConsumerSyncReadTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- Assignment() reflects Assign (§7.1) ----

    [Fact]
    public void Assignment_FreshConsumer_IsEmpty()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Assignment_ReflectsAssign_ExactlyTheAssignedPartitions()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        TopicPartition[] assigned =
        {
            new TopicPartition("t1", 0),
            new TopicPartition("t2", 3),
        };
        consumer.Assign(assigned);

        IReadOnlyCollection<TopicPartition> result = consumer.Assignment();

        // Set equality (order not guaranteed — the core returns a HashSet).
        Assert.Equal(2, result.Count);
        Assert.Equal(
            new HashSet<TopicPartition>(assigned),
            new HashSet<TopicPartition>(result));
    }

    [Fact]
    public void Assignment_ReturnsFreshSnapshotEachCall()
    {
        // Each call materializes a fresh owned snapshot (the FDG "fresh collection per
        // call → method" rationale). Two reads are equal by value but not the same
        // instance.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Assign(new[] { new TopicPartition("t", 0) });

        IReadOnlyCollection<TopicPartition> first = consumer.Assignment();
        IReadOnlyCollection<TopicPartition> second = consumer.Assignment();

        Assert.NotSame(first, second);
        Assert.Equal(
            new HashSet<TopicPartition>(first),
            new HashSet<TopicPartition>(second));
    }

    // ---- Subscription() reflects Subscribe (§7.2) ----

    [Fact]
    public void Subscription_FreshConsumer_IsEmpty()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        Assert.Empty(consumer.Subscription());
    }

    [Fact]
    public async Task Subscription_ReflectsSubscribe_ExactlyTheSubscribedTopics()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        string[] topics = { "t1", "t2" };
        await TestTimeout.Run(() => consumer.Subscribe(topics), s_deadline);

        IReadOnlyCollection<string> result = consumer.Subscription();

        Assert.Equal(2, result.Count);
        Assert.Equal(
            new HashSet<string>(topics, StringComparer.Ordinal),
            new HashSet<string>(result, StringComparer.Ordinal));
    }

    // ---- Paused() reachable states only (§7.3) ----

    [Fact]
    public void Paused_FreshConsumer_IsEmpty()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public void Paused_AfterAssign_IsStillEmpty()
    {
        // Assigning does not pause; a non-empty Paused() is unreachable until a public
        // Pause lands (later phase — recorded in the type remarks / COMMENTS.DONE.10).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Assign(new[] { new TopicPartition("t", 0) });

        Assert.Empty(consumer.Paused());
    }

    // ---- Non-ASCII round-trip through both list marshallers (§7.4, §B3 guard) ----

    [Fact]
    public async Task Subscription_NonAsciiTopic_RoundTripsByteForByte()
    {
        // Guards the NUL-terminated PtrToString path through StringListMarshal (catches
        // an LPStr regression) — the §0.1 non-ASCII requirement.
        const string nonAscii = "café-topic-Ω-日本語-😀";
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        await TestTimeout.Run(() => consumer.Subscribe(new[] { nonAscii }), s_deadline);

        IReadOnlyCollection<string> result = consumer.Subscription();

        string only = Assert.Single(result);
        Assert.Equal(nonAscii, only);
    }

    [Fact]
    public void Assignment_NonAsciiTopic_RoundTripsByteForByte()
    {
        // Guards TopicPartitionListMarshal → TopicPartition_topic (the NUL-terminated
        // getter form, §B3), byte-for-byte.
        const string nonAscii = "topic-grüße-Ω-🎉";
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        TopicPartition tp = new TopicPartition(nonAscii, 7);
        consumer.Assign(new[] { tp });

        TopicPartition only = Assert.Single(consumer.Assignment());
        Assert.Equal(nonAscii, only.Topic);
        Assert.Equal(7, only.Partition);
    }

    // ---- Concurrent access → InvalidOperationException: reachable seam + mapping (§7.5) ----

    // Note (§7.5 / D-Q4): the null-handle → InvalidOperationException mapping is verified
    // by code inspection of the shared ThrowIfConcurrentNull helper (reused verbatim from
    // the GroupMetadata read; the core returns a null list handle on its concurrent-access
    // rejection). A deterministic forced submit→read overlap is not reproducible broker-free
    // (mock ops resolve instantly; the one guard-holding op with a controllable duration is
    // poll, out of scope). Same non-deterministic ceiling as the shipped state reads —
    // documented in COMMENTS.DONE.10.md. Here we assert the reachable seam: on a free guard
    // each read round-trips.

    [Fact]
    public void SyncReads_OnFreeGuard_RoundTrip()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Assign(new[] { new TopicPartition("t", 0) });

        // No op in flight → the core guard is free → every read succeeds (no throw).
        Assert.Single(consumer.Assignment());
        Assert.Empty(consumer.Subscription());
        Assert.Empty(consumer.Paused());
    }

    // ---- Post-dispose → ObjectDisposedException (§7.6, deterministic) ----

    [Fact]
    public void Assignment_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public void Subscription_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
    }

    [Fact]
    public void Paused_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.Paused());
    }

    [Fact]
    public void EnforceRebalance_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.EnforceRebalance());
    }

    // ---- EnforceRebalance is a no-op that returns normally (§7.7, §1 resolution) ----

    [Fact]
    public void EnforceRebalance_NoArg_ReturnsWithoutThrowing()
    {
        // KIP-848 logged no-op → null error → no throw. Do NOT assert an
        // unsupported-version KafkaException (that would encode the stale ABI doc, not the
        // real behavior).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.EnforceRebalance();
    }

    [Fact]
    public void EnforceRebalance_WithReason_ReturnsWithoutThrowing()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.EnforceRebalance("some reason");
    }

    [Fact]
    public void EnforceRebalance_ReasonAndNoArg_BehaveIdentically()
    {
        // Java's two overloads have byte-identical bodies; the single string? reason = null
        // method collapses them. Both return normally, including a non-ASCII reason.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.EnforceRebalance();
        consumer.EnforceRebalance(null);
        consumer.EnforceRebalance("café-Ω-reason");
        consumer.EnforceRebalance(string.Empty);
    }

    // ---- The surface is on IConsumerCommon (via IAsyncConsumer) ----

    [Fact]
    public async Task SyncReads_ReachableViaIAsyncConsumerInterface()
    {
        // Held as the interface (the shared IConsumerCommon base), not the concrete type —
        // proves the members live on IConsumerCommon. Subscription and assignment are
        // mutually exclusive in the core (a subscribed consumer's assignment starts empty
        // until a rebalance assigns partitions), so this drives the subscription path;
        // Assignment() via the assign path is covered separately above.
        using AsyncMockConsumer mock = new AsyncMockConsumer();
        await TestTimeout.Run(() => mock.Subscribe(new[] { "sub" }), s_deadline);

        IAsyncConsumer consumer = mock;
        Assert.Single(consumer.Subscription());
        Assert.Empty(consumer.Assignment());
        Assert.Empty(consumer.Paused());
        consumer.EnforceRebalance();
    }
}

#if NET8_0_OR_GREATER

/// <summary>
/// Allocation sanity for a repeated sync state read (PLAN §7.8; DoD §10 spirit). Not a hot
/// path (low-frequency sync reads), so a sanity bound — the copy-out is <em>the</em>
/// allocation and is bounded by assignment size (Java's own behavior) — not a zero-alloc
/// assertion. The read runs entirely on the caller's thread (unlike the poll copy-out on
/// the foreign dispatcher thread), so the process-wide precise counter measures it
/// directly.
/// </summary>
public sealed class ConsumerSyncReadAllocationTests
{
    private const string Topic = "sync-read-alloc-topic";

    [Fact]
    public void Assignment_RepeatedRead_DoesNotAllocateUnboundedlyPerCall()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Assign(new[]
        {
            new TopicPartition(Topic, 0),
            new TopicPartition(Topic, 1),
            new TopicPartition(Topic, 2),
            new TopicPartition(Topic, 3),
        });

        // Warm up the JIT / any first-call caching so the measured window is steady-state.
        for (int i = 0; i < 100; i++)
        {
            _ = consumer.Assignment();
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        const int iterations = 1000;
        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < iterations; i++)
        {
            IReadOnlyCollection<TopicPartition> result = consumer.Assignment();
            // Touch the result so nothing is optimized away.
            Assert.Equal(4, result.Count);
        }

        long after = GC.GetTotalAllocatedBytes(precise: true);
        long perCall = (after - before) / iterations;

        // The owned snapshot (a TopicPartition[4]) + the four TopicPartition topic-string
        // copies is the whole per-call cost. A generous ceiling that still catches an
        // unbounded / per-element extra allocation regression on a 4-element assignment.
        const long PerCallBudgetBytes = 2048;
        Assert.True(
            perCall <= PerCallBudgetBytes,
            $"Per-call Assignment() allocation {perCall} B exceeded the copy-out sanity budget " +
            $"{PerCallBudgetBytes} B over {iterations} reads of a 4-partition assignment.");
    }
}

#endif
