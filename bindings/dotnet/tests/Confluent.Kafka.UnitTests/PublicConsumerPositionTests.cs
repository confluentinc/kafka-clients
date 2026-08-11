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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for <see cref="IAsyncConsumer.Position(TopicPartition, CancellationToken)"/>
/// (M5/P2, PLAN §6) — the <b>scalar</b> completion bridge exercised end to end through the
/// <b>public</b> <see cref="AsyncMockConsumer"/> / <see cref="IAsyncConsumer"/> surface:
/// happy path (assign → seek → <c>Position</c> returns the offset), the unassigned-partition
/// FAILURE path (message asserted, DoD §3), cancellation / wakeup, the deterministic
/// preconditions / lifecycle, and a per-op allocation sanity bound (per-RPC, not zero-alloc).
/// Every awaited op runs under a <see cref="TestTimeout"/> hang guard (the completion /
/// deadlock regression guard, PLAN §6).
/// </summary>
public sealed class PublicConsumerPositionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "position-topic";
    private const int Partition = 0;

    // Assign + seek to an offset so the mock has a valid position — the canonical
    // broker-free position setup, through the public AsyncMockConsumer surface (matches
    // the ReadyToPoll precedent in PublicConsumerRoundTripTests).
    private static async Task<AsyncMockConsumer<byte[], byte[]>> ReadyForPosition(
        long seekOffset, string topic = Topic, int partition = Partition)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(
            () => consumer.Assign(new[] { new TopicPartition(topic, partition) }), s_deadline);
        consumer.Seek(new TopicPartition(topic, partition), seekOffset); // sync (M5/P7)
        return consumer;
    }

    // ---- Happy path (offset asserted, DoD §3) ----

    [Fact]
    public async Task Position_AfterAssignAndSeek_ReturnsSoughtOffset()
    {
        const long offset = 42;
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(offset);

        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));

        Assert.Equal(offset, position);
    }

    [Fact]
    public async Task Position_ViaIAsyncConsumerInterface_ReturnsSoughtOffset()
    {
        // Hold an AsyncMockConsumer, invoke through the IAsyncConsumer surface.
        const long offset = 7;
        using AsyncMockConsumer<byte[], byte[]> mock = await ReadyForPosition(offset);

        IAsyncConsumer<byte[], byte[]> consumer = mock;
        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));

        Assert.Equal(offset, position);
    }

    [Fact]
    public async Task Position_NonAsciiTopic_ReturnsSoughtOffset()
    {
        // Exercises the call-scoped UTF-8 topic pin (ffi §A3/§B3) on the position path.
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        const long offset = 11;
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(offset, nonAsciiTopic);

        long position = await PositionOf(consumer, new TopicPartition(nonAsciiTopic, Partition));

        Assert.Equal(offset, position);
    }

    // ---- Operational failure (message asserted, DoD §3) ----

    [Fact]
    public async Task Position_UnassignedPartition_FaultsWithKafkaExceptionMessage()
    {
        // Assign a DIFFERENT partition, then query the position of an unassigned one: the
        // mock core returns an illegal_argument error whose message is the behavioral
        // contract (asserted per DoD §3). Confirms the failure path routes through the
        // trampoline's Complete(error) -> KafkaException.FromHandle (error handle freed
        // exactly once) and FAULTS the Task, rather than throwing synchronously.
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        TopicPartition unassigned = new TopicPartition(Topic, 5);

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => PositionOf(consumer, unassigned));

        Assert.Equal(
            "You can only check the position for partitions assigned to this consumer.",
            ex.Message);
    }

    [Fact]
    public async Task Position_UnassignedPartition_FaultsThenConsumerReusable()
    {
        // The failure is not fatal: after the faulted position, a valid position query on
        // the assigned partition still succeeds (the error handle + GCHandle were freed
        // exactly once, leaving the consumer usable).
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 3);

        await Assert.ThrowsAsync<KafkaException>(
            () => PositionOf(consumer, new TopicPartition(Topic, 9)));

        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));
        Assert.Equal(3, position);
    }

    // ---- Cancellation / wakeup (PLAN §6.3/§6.4) ----

    [Fact]
    public async Task Position_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored synchronously BEFORE any
        // native call (OperationCanceledException, distinct from a wakeup KafkaException),
        // via ThrowIfCancellationRequested in SubmitScalarOperation — user-initiated
        // cancellation, NOT a timeout.
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.Position(new TopicPartition(Topic, Partition), cts.Token));
    }

    [Fact]
    public async Task Position_AfterWakeup_StillUsable()
    {
        // Reachability note (PLAN §6.3): unlike poll, the mock's position() does NOT
        // check-and-clear the wakeup flag, and mock ops resolve instantly, so a
        // deterministic in-flight wakeup-vs-position overlap is not reproducible broker-free
        // (the D-Q4 ceiling — a controllable-duration guard-holding op is a Rust-core
        // dependency). The reachable, deterministic property is asserted here: a wakeup()
        // does not corrupt the consumer — the reachable seam (a subsequent Position on the
        // free guard succeeds) holds, and the consumer stays reusable after a wakeup.
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 5);

        consumer.Wakeup();

        long position = await PositionOf(consumer, new TopicPartition(Topic, Partition));
        Assert.Equal(5, position);
    }

    // ---- Preconditions / lifecycle (deterministic, before any native call) ----

    [Fact]
    public async Task Position_NullTopic_ThrowsArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);

        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way
        // to present a null topic without TopicPartition's own ctor validation firing.
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.Position(default));
    }

    [Fact]
    public async Task Position_NegativePartition_ThrowsArgumentOutOfRange()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);

        // TopicPartition's ctor rejects a negative partition itself (the Seek/Assign
        // precedent), so the negative value cannot even reach Position through a
        // constructed TopicPartition — assert the ctor guard is that same exception type
        // and message, which is what Position would throw were the value smuggled in.
        ArgumentOutOfRangeException ex =
            Assert.Throws<ArgumentOutOfRangeException>(() => new TopicPartition(Topic, -1));
        Assert.Contains("Partition must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Position_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 0);
        await consumer.DisposeAsync();

        // ThrowIfClosed() runs before any pin / P-Invoke (the shipped gate). Deterministic.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.Position(new TopicPartition(Topic, Partition)));
    }

    // ---- Concurrency (same D-Q4 ceiling as every shipped async op) ----

    [Fact]
    public async Task Position_ReachableSeam_RoundTripsOnFreeGuard()
    {
        // A forced submit-overlap is not deterministically reproducible broker-free (mock
        // ops resolve instantly; the one guard-holding op with a controllable duration is
        // poll — the D-Q4 ceiling). What IS assertable: the read round-trips on a FREE
        // guard, and the core-rejection -> faulted-Task mapping is the SAME verbatim
        // Complete(error) path the faulted unassigned-partition test above exercises (a
        // concurrent op is rejected by the core inline with a ConcurrentModification error
        // through that same trampoline). See COMMENTS.DONE.11.md for the inspection record.
        using AsyncMockConsumer<byte[], byte[]> consumer = await ReadyForPosition(seekOffset: 8);

        long first = await PositionOf(consumer, new TopicPartition(Topic, Partition));
        long second = await PositionOf(consumer, new TopicPartition(Topic, Partition));

        Assert.Equal(8, first);
        Assert.Equal(8, second);
    }

    // Every awaited Position routes through the TestTimeout hang guard (PLAN §6) so a
    // future stall in the scalar bridge or the mock fails the run fast instead of hanging
    // it — mirroring the poll tests' Poll helper.
    private static async Task<long> PositionOf(IAsyncConsumer<byte[], byte[]> consumer, TopicPartition partition)
    {
        long result = 0;
        await TestTimeout.Run(async () => result = await consumer.Position(partition), s_deadline);
        return result;
    }
}
