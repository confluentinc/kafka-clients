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
/// Public-surface tests for the two M5/P5 partition-metadata queries (Category E2) —
/// <see cref="IAsyncConsumer.PartitionsFor"/> and <see cref="IAsyncConsumer.ListTopics"/> —
/// exercised end to end through the <b>public</b> <see cref="AsyncMockConsumer"/> /
/// <see cref="IAsyncConsumer"/> surface (PLAN §7). Covers the owned-handle
/// <c>PartitionInfoList_t</c> / <c>TopicPartitionInfoMap_t</c> completion bridges, the nested
/// copy-out marshallers (list/map → <see cref="PartitionInfo"/> → leader / replica
/// <see cref="Node"/> → node strings), the two string forms coexisting in one tree
/// (NUL-terminated topic, length-delimited <c>Node</c> host/rack), the faulted paths (message
/// asserted, DoD §3), preconditions, lifecycle, and cancellation.
/// </summary>
/// <remarks>
/// <b>Reachability (PLAN §6).</b> With <see cref="AsyncMockConsumer.UpdatePartitions"/> wired,
/// the mock builds each partition with a single leader <see cref="Node"/> that is also its
/// sole replica and in-sync replica (offline replicas empty, no rack). <b>Data-testable
/// broker-free:</b> <see cref="PartitionInfo.Topic"/> / <see cref="PartitionInfo.Partition"/>
/// / <see cref="PartitionInfo.Leader"/> (id / host / port) / <see cref="PartitionInfo.Replicas"/>[0]
/// / <see cref="PartitionInfo.InSyncReplicas"/>[0]. <b>Documented-empty (a reachable-slice limit
/// of the mock, not a silent gap):</b> <see cref="PartitionInfo.OfflineReplicas"/> (always
/// empty) and <see cref="Node.Rack"/> (always null) — their marshaller paths are still
/// exercised structurally (an empty list / a null). Every awaited op runs under a
/// <see cref="TestTimeout"/> hang guard.
/// </remarks>
public sealed class PublicConsumerPartitionMetadataTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "partition-metadata-topic";
    private const string LeaderHost = "broker-1.example.com";
    private const int LeaderId = 7;
    private const int LeaderPort = 9092;

    // ---- PartitionsFor — reachable data (PLAN §6/§7) ----

    [Fact]
    public async Task PartitionsFor_AfterUpdatePartitions_ReturnsReachableFields()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 3, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);

        Assert.Equal(3, partitions.Count);
        for (int p = 0; p < 3; p++)
        {
            PartitionInfo info = FindPartition(partitions, p);

            // Data-testable broker-free (PLAN §6).
            Assert.Equal(Topic, info.Topic);
            Assert.Equal(p, info.Partition);

            Assert.NotNull(info.Leader);
            Node leader = info.Leader!;
            Assert.Equal(LeaderId, leader.Id);
            Assert.Equal(LeaderHost, leader.Host);
            Assert.Equal(LeaderPort, leader.Port);

            // The leader is also the sole replica + in-sync replica (the mock's shape) —
            // exercises the replica-list + in-sync-list copy-out (each a length-delimited-host
            // Node), not just the leader Node.
            Node replica = Assert.Single(info.Replicas);
            Assert.Equal(LeaderId, replica.Id);
            Assert.Equal(LeaderHost, replica.Host);
            Assert.Equal(LeaderPort, replica.Port);

            Node isr = Assert.Single(info.InSyncReplicas);
            Assert.Equal(LeaderId, isr.Id);
            Assert.Equal(LeaderHost, isr.Host);

            // Documented-empty (mock limit, not a silent gap) — the marshaller's offline-replica
            // and rack paths ARE exercised, as an empty list / a null.
            Assert.Empty(info.OfflineReplicas);
            Assert.Null(leader.Rack);
        }
    }

    [Fact]
    public async Task PartitionsFor_ViaIAsyncConsumerInterface_ReturnsData()
    {
        using AsyncMockConsumer mock = new AsyncMockConsumer();
        mock.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IAsyncConsumer consumer = mock;
        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);

        PartitionInfo info = Assert.Single(partitions);
        Assert.Equal(Topic, info.Topic);
        Assert.Equal(LeaderId, info.Leader!.Id);
    }

    [Fact]
    public async Task PartitionsFor_UnregisteredTopic_ReturnsEmptyList()
    {
        // The mock returns an empty list broker-free for a topic with no registered partitions
        // (partitions.get(topic).unwrap_or_default()) — a valid, non-null empty result
        // (success, not a fault). Exercises the empty-container copy-out path.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, "no-such-topic");

        Assert.Empty(partitions);
    }

    [Fact]
    public async Task PartitionsFor_NonAsciiTopic_RoundTripsThroughNulScanTopicMarshalling()
    {
        // The registered topic is a NUL-terminated string in the result (PartitionInfo_topic,
        // §B3 NUL-scan form). A non-ASCII topic exercises both the input pin AND the
        // receive-path NUL-scan copy — distinct from the length-delimited Node host below.
        const string nonAscii = "topic-grüße-Ω-🎉";
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(nonAscii, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, nonAscii);

        PartitionInfo info = Assert.Single(partitions);
        Assert.Equal(nonAscii, info.Topic);
    }

    [Fact]
    public async Task PartitionsFor_NonAsciiLeaderHost_RoundTripsThroughLengthDelimitedMarshalling()
    {
        // Node_host is LENGTH-DELIMITED ((ptr, out_len), NOT NUL-terminated, §B3). A non-ASCII
        // host (multi-byte chars) guards against a NUL-scan over-read on the length-delimited
        // slice — the one shape difference from E1 (PLAN §3). Both the multi-byte width and the
        // exact round-trip are asserted.
        const string nonAsciiHost = "hôte-Ω-🎉.example.com";
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, nonAsciiHost, LeaderPort);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);

        PartitionInfo info = Assert.Single(partitions);
        Assert.Equal(nonAsciiHost, info.Leader!.Host);
        // The replica copy of the node marshals identically (same length-delimited host).
        Assert.Equal(nonAsciiHost, info.Replicas[0].Host);
    }

    [Fact]
    public async Task PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        // Java/Python-faithful (PLAN §8.2): an EMPTY topic is FORWARDED to the core, NOT
        // rejected client-side (the binding guards only null). The mock has no partitions
        // registered for "", so it returns an empty list — proving the empty topic reached the
        // core (a client-side rejection would have thrown ArgumentException instead).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, string.Empty);

        Assert.Empty(partitions);
    }

    // ---- ListTopics — reachable data (PLAN §6/§7) ----

    [Fact]
    public async Task ListTopics_AfterUpdatePartitions_ReturnsMapKeyedByTopic()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions("topic-a", partitionCount: 2, LeaderId, LeaderHost, LeaderPort);
        consumer.UpdatePartitions("topic-b", partitionCount: 1, leaderId: 9, "broker-2", leaderPort: 9093);

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.Equal(2, map.Count);

        IReadOnlyList<PartitionInfo> a = map["topic-a"];
        Assert.Equal(2, a.Count);
        Assert.Equal("topic-a", a[0].Topic);
        Node aLeader = a[0].Leader!;
        Assert.Equal(LeaderId, aLeader.Id);
        Assert.Equal(LeaderHost, aLeader.Host);

        IReadOnlyList<PartitionInfo> b = map["topic-b"];
        PartitionInfo bInfo = Assert.Single(b);
        Assert.Equal("topic-b", bInfo.Topic);
        Node bLeader = bInfo.Leader!;
        Assert.Equal(9, bLeader.Id);
        Assert.Equal("broker-2", bLeader.Host);
        Assert.Equal(9093, bLeader.Port);
    }

    [Fact]
    public async Task ListTopics_ViaIAsyncConsumerInterface_ReturnsData()
    {
        using AsyncMockConsumer mock = new AsyncMockConsumer();
        mock.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IAsyncConsumer consumer = mock;
        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.True(map.ContainsKey(Topic));
        Assert.Single(map[Topic]);
    }

    [Fact]
    public async Task ListTopics_NoTopics_ReturnsEmptyMap()
    {
        // No topics registered → an empty, non-null map (success, not a fault). Exercises the
        // empty-map copy-out path (the shared EmptyReadOnlyDictionary singleton).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.Empty(map);
    }

    [Fact]
    public async Task ListTopics_NonAsciiTopicKey_RoundTripsThroughNulScanTopicMarshalling()
    {
        // The map key topic is NUL-terminated (TopicPartitionInfoMap_get_topic, §B3 NUL-scan).
        const string nonAscii = "тема-Ω-🎉";
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(nonAscii, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);

        Assert.True(map.ContainsKey(nonAscii));
        Assert.Equal(nonAscii, map[nonAscii][0].Topic);
    }

    // ---- Faulted path — reachability limit (PLAN §6, documented not silently skipped) ----
    //
    // There is NO clean broker-free operational-failure path for these two queries: the mock's
    // partitions_for ALWAYS returns a valid list (empty for an unregistered topic, populated
    // after UpdatePartitions — src/consumer/mock_consumer.rs), and its list_topics always
    // returns a valid (possibly empty) map, so neither can fault broker-free. A real
    // AsyncKafkaConsumer against an unreachable broker does NOT fault quickly — a metadata fetch
    // retries until the request timeout (many seconds), so a "fault fast" assertion is not
    // deterministic and would either hang the suite or be flaky (confirmed: it exceeded the 30 s
    // TestTimeout guard). The faulted-Task MECHANISM (trampoline Complete(error) -> FromHandle,
    // error handle + GCHandle freed exactly once, faulted Task not a synchronous throw) is
    // identical to the E1 offset-map bridges and is already proven end-to-end there
    // (PublicConsumerOffsetQueryTests: BeginningOffsets/EndOffsets unset-partition faults,
    // OffsetsForTimes unsupported-version fault — same OnPoll-clone shape, same
    // OperationCompletionSource.Complete path). So the operational-failure path is a documented
    // reachability limit for E2, not a silent gap. The concurrent-op fault is likewise a D-Q4
    // non-blockable-mock ceiling (the mock resolves instantly; a submit->callback overlap is not
    // reproducible broker-free — the shipped Position / offset-query precedent).

    // ---- Preconditions (deterministic, before any native call; PLAN §7 / §B5) ----

    [Fact]
    public async Task PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        // The binding guards only null (FFI panic-safety); an empty topic is forwarded (above).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.PartitionsFor(null!));
    }

    [Fact]
    public async Task PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.PartitionsFor(Topic));
    }

    [Fact]
    public async Task ListTopics_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.ListTopics());
    }

    // ---- Cancellation / wakeup (PLAN §7 / §B5) ----

    [Fact]
    public async Task PartitionsFor_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored synchronously BEFORE any native
        // call (OperationCanceledException, distinct from a wakeup KafkaException), via
        // ThrowIfCancellationRequested in SubmitOwnedHandleOperation — user cancellation, NOT a
        // timeout.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.PartitionsFor(Topic, cts.Token));
    }

    [Fact]
    public async Task ListTopics_PreCanceledToken_ThrowsOperationCanceled()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.ListTopics(cts.Token));
    }

    [Fact]
    public async Task PartitionsFor_AfterWakeup_StillUsable()
    {
        // A wakeup() does not corrupt the consumer: the reachable seam (a subsequent
        // PartitionsFor on the free guard succeeds) holds. The mock's metadata query does not
        // check-and-clear the wakeup flag and resolves instantly, so an in-flight overlap is not
        // reproducible broker-free — the D-Q4 ceiling, mirroring the offset-query precedent.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        consumer.Wakeup();

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);
        Assert.Single(partitions);
    }

    // ---- Handle lifecycle: fault leaves the consumer reusable (free-exactly-once seam) ----

    [Fact]
    public async Task ListTopics_AfterFaultedPartitionsFor_StillReturnsData()
    {
        // A faulted PartitionsFor on a real consumer frees its error handle + GCHandle exactly
        // once; but that consumer is a real one (no broker). This reusable-after-op seam is
        // exercised on the mock: repeated queries do not leak / corrupt (the GCHandle + list root
        // freed once per op).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        for (int i = 0; i < 5; i++)
        {
            IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(consumer, Topic);
            Assert.Single(partitions);
        }

        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> map = await ListTopicsOf(consumer);
        Assert.True(map.ContainsKey(Topic));
    }

    // ---- Per-op allocation sanity (per-RPC, NOT zero-alloc; DoD §10 / PLAN §7) ----

#if NET8_0_OR_GREATER
    [Fact]
    public async Task PartitionsFor_PerOpAllocation_IsBounded()
    {
        // Per-RPC top-level surface, not a hot path (CLAUDE.md §11): a Task + GCHandle +
        // OperationCompletionSource + the owned result tree (list + PartitionInfo + Node + node
        // strings) per call is amortized and fine. A LIGHT sanity bound (not zero-alloc): the
        // marginal per-op cost does NOT scale beyond the fixed one-partition result. Uses the
        // PROCESS-WIDE counter (the copy-out runs on the foreign dispatcher thread; a per-thread
        // counter across the await hop mis-measures — the M3/P4 flake finding) and a marginal
        // (large - small) subtraction to cancel fixed/ambient allocation.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.UpdatePartitions(Topic, partitionCount: 1, LeaderId, LeaderHost, LeaderPort);

        for (int i = 0; i < 10; i++)
        {
            await PartitionsForOf(consumer, Topic);
        }

        long small = await MeasurePartitionsFor(consumer, count: 50);
        long large = await MeasurePartitionsFor(consumer, count: 500);

        long perOp = (large - small) / (500 - 50);

        const long PerOpBudgetBytes = 4096;
        Assert.True(
            perOp <= PerOpBudgetBytes,
            $"Per-op PartitionsFor allocation {perOp} B exceeded the per-RPC sanity budget " +
            $"{PerOpBudgetBytes} B (small={small} B/50 op, large={large} B/500 op) — " +
            "an unbounded / per-something allocation would show here.");
    }

    private async Task<long> MeasurePartitionsFor(AsyncMockConsumer consumer, int count)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < count; i++)
        {
            await consumer.PartitionsFor(Topic);
        }

        long after = GC.GetTotalAllocatedBytes(precise: true);
        return after - before;
    }
#endif

    // ---- Helpers (every awaited op under the TestTimeout hang guard) ----

    private static PartitionInfo FindPartition(IReadOnlyList<PartitionInfo> partitions, int partition)
    {
        foreach (PartitionInfo info in partitions)
        {
            if (info.Partition == partition)
            {
                return info;
            }
        }

        throw new Xunit.Sdk.XunitException($"No PartitionInfo for partition {partition}.");
    }

    private static async Task<IReadOnlyList<PartitionInfo>> PartitionsForOf(IAsyncConsumer consumer, string topic)
    {
        IReadOnlyList<PartitionInfo> result = null!;
        await TestTimeout.Run(async () => result = await consumer.PartitionsFor(topic), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopicsOf(
        IAsyncConsumer consumer)
    {
        IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> result = null!;
        await TestTimeout.Run(async () => result = await consumer.ListTopics(), s_deadline);
        return result;
    }
}
