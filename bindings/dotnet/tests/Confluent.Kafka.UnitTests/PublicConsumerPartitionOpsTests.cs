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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M5/P3 consumer partition-ops surface — the five void-async members on
/// <see cref="IAsyncConsumer"/> (<see cref="IAsyncConsumer.Assign"/> /
/// <see cref="IAsyncConsumer.Pause"/> / <see cref="IAsyncConsumer.Resume"/> /
/// <see cref="IAsyncConsumer.SeekToBeginning"/> / <see cref="IAsyncConsumer.SeekToEnd"/>)
/// — through the <b>public</b> <see cref="AsyncMockConsumer"/> surface, broker-free
/// (PLAN §6). All five reuse the proven void completion bridge; the only new managed
/// work is marshalling a <see cref="TopicPartition"/> collection into the ABI's parallel
/// arrays. Every awaited op runs under a <see cref="TestTimeout"/> hang guard; serial
/// execution (D8.8) stays enabled at the assembly level.
/// </summary>
/// <remarks>
/// <para>
/// This phase closes the <b>M5/P1 non-empty <c>Paused()</c> gap</b>: with a public
/// <see cref="IAsyncConsumer.Pause"/> now landed, a non-empty
/// <see cref="IConsumerCommon.Paused"/> is finally reachable broker-free — see
/// <see cref="Pause_ThenPaused_ReturnsThePausedSet"/>.
/// </para>
/// <para>
/// <b>Empty-collection semantics (§5, resolved against the Java/Rust source).</b>
/// <c>assign([])</c> <em>clears</em> the assignment; <c>pause/resume/seekTo*([])</c> are a
/// no-op — all resolve successfully. A <see langword="null"/> collection is rejected
/// (null ≠ empty). Both are asserted below.
/// </para>
/// </remarks>
public sealed class PublicConsumerPartitionOpsTests
{
    private const string Topic = "partition-ops-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(200);

    // ---- Assign / empty-collection semantics (§6.1) ----

    [Fact]
    public async Task Assign_EmptyCollection_ClearsTheAssignment()
    {
        // §5: assign([]) is Java's assign(emptyList) — a CLEAR of the assignment, NOT a
        // no-op error. Assign a set, then assign the empty set → Assignment() is empty.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, 0) }), s_deadline);
        Assert.Single(consumer.Assignment());

        await TestTimeout.Run(() => consumer.Assign(Array.Empty<TopicPartition>()), s_deadline);

        Assert.Empty(consumer.Assignment());
    }

    // ---- Pause then Paused() returns the paused set — closes the M5/P1 gap (§6.3) ----

    [Fact]
    public async Task Pause_ThenPaused_ReturnsThePausedSet()
    {
        // The M5/P1 Paused() rustdoc noted a non-empty Paused() is "not reachable broker-free
        // until a public Pause lands". It has landed — assert the non-empty case here.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        await TestTimeout.Run(() => consumer.Pause(new[] { tp }), s_deadline);

        TopicPartition only = Assert.Single(consumer.Paused());
        Assert.Equal(tp, only);
    }

    [Fact]
    public async Task Resume_AfterPause_ClearsThePausedSet()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);
        await TestTimeout.Run(() => consumer.Pause(new[] { tp }), s_deadline);
        Assert.Single(consumer.Paused());

        await TestTimeout.Run(() => consumer.Resume(new[] { tp }), s_deadline);

        Assert.Empty(consumer.Paused());
    }

    // ---- SeekToBeginning / SeekToEnd resolve broker-free (baseline, §6.5) ----

    [Fact]
    public async Task SeekToBeginning_ResolvesBrokerFree_NoOffsetSetup()
    {
        // The reset-strategy-only path (§0): the seek sets the strategy and resolves
        // broker-free with NO offset setup (the offsets are consulted lazily at poll time).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        await TestTimeout.Run(() => consumer.SeekToBeginning(new[] { tp }), s_deadline);
    }

    [Fact]
    public async Task SeekToEnd_ResolvesBrokerFree_NoOffsetSetup()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        await TestTimeout.Run(() => consumer.SeekToEnd(new[] { tp }), s_deadline);
    }

    // ---- SeekToBeginning / SeekToEnd observable via poll (§6.6, via the mock offset helpers) ----

    [Fact]
    public async Task SeekToBeginning_ObservableViaPoll_ResetsToBeginningOffset()
    {
        // End-to-end: set the beginning offset to 5, add records at offsets 0..9, seek to
        // beginning → the next poll resets the position to 5, so ONLY records at offset >= 5
        // are returned (the < 5 records are skipped by the reset). This exercises the two
        // new mock offset helpers + SeekToBeginning + poll together.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        consumer.UpdateBeginningOffset(Topic, 0, offset: 5);
        for (int i = 0; i < 10; i++)
        {
            consumer.AddRecord(Topic, 0, offset: i, key: null, value: new byte[] { (byte)i });
        }

        await TestTimeout.Run(() => consumer.SeekToBeginning(new[] { tp }), s_deadline);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        // The reset moves the fetch position to the beginning offset (5); records at
        // offsets 5..9 come back (5 records), offsets 0..4 are dropped.
        long[] offsets = records.Select(r => r.Offset).OrderBy(o => o).ToArray();
        Assert.Equal(new long[] { 5, 6, 7, 8, 9 }, offsets);
    }

    [Fact]
    public async Task SeekToEnd_ObservableViaPoll_ResetsToEndOffset()
    {
        // The LATEST analog: set the end offset to 8, add records at offsets 0..9, seek to
        // end → the next poll resets the position to 8, so ONLY records at offset >= 8 come
        // back (offsets 8, 9).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        consumer.UpdateEndOffset(Topic, 0, offset: 8);
        for (int i = 0; i < 10; i++)
        {
            consumer.AddRecord(Topic, 0, offset: i, key: null, value: new byte[] { (byte)i });
        }

        await TestTimeout.Run(() => consumer.SeekToEnd(new[] { tp }), s_deadline);

        ConsumerRecords<byte[], byte[]> records = await Poll(consumer);

        long[] offsets = records.Select(r => r.Offset).OrderBy(o => o).ToArray();
        Assert.Equal(new long[] { 8, 9 }, offsets);
    }

    // ---- Empty-collection no-op success for the four non-assign ops (§6.7 / §5) ----

    [Fact]
    public async Task Pause_EmptyCollection_IsNoOpSuccess()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Pause(Array.Empty<TopicPartition>()), s_deadline);
        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public async Task Resume_EmptyCollection_IsNoOpSuccess()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Resume(Array.Empty<TopicPartition>()), s_deadline);
    }

    [Fact]
    public async Task SeekToBeginning_EmptyCollection_IsNoOpSuccess()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.SeekToBeginning(Array.Empty<TopicPartition>()), s_deadline);
    }

    [Fact]
    public async Task SeekToEnd_EmptyCollection_IsNoOpSuccess()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.SeekToEnd(Array.Empty<TopicPartition>()), s_deadline);
    }

    // ---- Failure path with asserted KafkaException message (§6.8, DoD §3) ----

    [Fact]
    public async Task Pause_UnassignedPartition_FaultsWithKafkaExceptionMessage()
    {
        // A deterministic broker-free OPERATIONAL failure: the mock's pause() delegates to
        // SubscriptionState.pause(tp), which errors on a partition that is not in the
        // assignment ("No current assignment for partition <tp>"). This is cleaner than the
        // non-deterministic concurrent-op path and asserts the KafkaException MESSAGE
        // content (part of the behavioral contract, DoD §3).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        // Pause a DIFFERENT (unassigned) partition → the op faults.
        TopicPartition unassigned = new TopicPartition(Topic, 99);
        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.Pause(new[] { unassigned }), s_deadline));

        Assert.Contains("No current assignment for partition", ex.Message, StringComparison.Ordinal);
        Assert.Contains(unassigned.ToString(), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pause_UnassignedPartition_FaultsThenConsumerReusable()
    {
        // After the operational fault the consumer stays usable (single-owner: the awaiter
        // is done, the core guard is free) — a subsequent op on an assigned partition works.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition assigned = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { assigned }), s_deadline);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.Pause(new[] { new TopicPartition(Topic, 99) }), s_deadline));

        // Reusable: pausing the assigned partition now succeeds and is reflected.
        await TestTimeout.Run(() => consumer.Pause(new[] { assigned }), s_deadline);
        Assert.Single(consumer.Paused());
    }

    // ---- Preconditions (deterministic, before any native call, §6.9 / §5) ----

    [Fact]
    public async Task Ops_NullCollection_ThrowArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // One per op family: a null collection is rejected before any pin/P-Invoke (null ≠
        // empty — empty is a valid clear/no-op, §5).
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Assign(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Pause(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Resume(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.SeekToBeginning(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.SeekToEnd(null!));
    }

    [Fact]
    public async Task Ops_ElementWithNullTopic_ThrowArgumentException()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way to
        // present a per-element null topic without TopicPartition's own ctor validation
        // firing. Rejected as ArgumentException (the Subscribe per-element precedent).
        TopicPartition[] withNullTopic = { default };

        // ArgumentException is the base of ArgumentNullException, so ThrowsAsync<ArgumentException>
        // accepts either — the binding throws ArgumentException with the collection param name.
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Assign(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Pause(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Resume(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.SeekToBeginning(withNullTopic));
        await Assert.ThrowsAsync<ArgumentException>(() => consumer.SeekToEnd(withNullTopic));
    }

    // ---- Post-dispose → ObjectDisposedException (§6.10, deterministic) ----

    [Fact]
    public async Task Ops_AfterDispose_ThrowObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition[] tps = { new TopicPartition(Topic, 0) };
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Assign(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Pause(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Resume(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekToBeginning(tps));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekToEnd(tps));
    }

    // ---- Cancellation / wakeup (§6.11) ----

    [Fact]
    public async Task Ops_PreCanceledToken_ThrowOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored synchronously BEFORE any
        // native call (via ThrowIfCancellationRequested in SubmitVoidOperation) — the
        // .NET-native cancel (OperationCanceledException), distinct from a wakeup
        // KafkaException. Mirrors the SubscribeWithCallback / Position precedent.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition[] tps = { new TopicPartition(Topic, 0) };
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Assign(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Pause(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Resume(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.SeekToBeginning(tps, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.SeekToEnd(tps, cts.Token));
    }

    [Fact]
    public async Task Ops_AfterWakeup_ConsumerStillUsable()
    {
        // Reachability note (matches the shipped Position wakeup test): mock ops resolve
        // instantly, so a deterministic in-flight wakeup-vs-op overlap is not reproducible
        // broker-free. The reachable, deterministic property: a wakeup() does not corrupt
        // the consumer — a subsequent partition op on the free guard succeeds.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        consumer.Wakeup();

        await TestTimeout.Run(() => consumer.Pause(new[] { tp }), s_deadline);
        Assert.Single(consumer.Paused());
    }

    private static async Task<ConsumerRecords<byte[], byte[]>> Poll(AsyncMockConsumer<byte[], byte[]> consumer)
    {
        ConsumerRecords<byte[], byte[]> result = null!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
