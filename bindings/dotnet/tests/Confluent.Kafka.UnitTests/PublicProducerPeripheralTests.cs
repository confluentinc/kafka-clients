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
/// Public-surface tests for the M11/P2 producer async peripherals —
/// <see cref="IAsyncProducer.Flush"/>, <see cref="IAsyncProducer.Close(CancellationToken)"/>,
/// <see cref="IAsyncProducer.Close(TimeSpan, CancellationToken)"/>, and
/// <see cref="IAsyncProducer.PartitionsFor"/> — exercised end to end through the <b>public</b>
/// <see cref="AsyncMockProducer"/> / <see cref="IAsyncProducer"/> surface (PLAN §3). Covers the
/// void completion bridge (flush / close), the owned-handle <c>PartitionInfoList_t</c> completion
/// (partitions_for) with its copy-out, preconditions (before any native call), and best-effort
/// cancellation (no native abort — the producer has no <c>wakeup()</c>).
/// </summary>
/// <remarks>
/// <b>Honest reachability (PLAN §2).</b> On a <see cref="AsyncMockProducer"/> flush / close resolve
/// broker-free (no pending sends → immediate success; close is idempotent), and
/// <see cref="IAsyncProducer.PartitionsFor"/> succeeds but returns an <b>empty</b> list for every
/// topic (the mock ctor builds an empty cluster) — a populated list is integration-only. There is
/// no clean broker-free operational-failure path for these peripherals (the mock never faults
/// them), so the faulted-Task mechanism is proven by the identical consumer bridges
/// (PublicConsumerPartitionMetadataTests / offset-query faults), not re-exercised here. Every
/// awaited op runs under a <see cref="TestTimeout"/> hang guard (the completion-bridge / teardown
/// regression guard, ffi §A7).
/// </remarks>
public sealed class PublicProducerPeripheralTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "peripheral-topic";

    // ---- Flush (void completion bridge) ----

    [Fact]
    public async Task Flush_OnMock_Succeeds()
    {
        using AsyncMockProducer producer = new AsyncMockProducer();

        // No pending sends → the mock resolves the flush immediately (a completed Task, not a
        // fault). Proves the void completion bridge over Producer_flush_async round-trips.
        await TestTimeout.Run(() => producer.Flush(), s_deadline);
    }

    [Fact]
    public async Task Flush_CalledRepeatedly_Succeeds()
    {
        // Repeated flushes do not leak / corrupt (per-op GCHandle + span-the-op ref freed once
        // per op) — the free-exactly-once seam on the reusable producer.
        using AsyncMockProducer producer = new AsyncMockProducer();

        for (int i = 0; i < 5; i++)
        {
            await TestTimeout.Run(() => producer.Flush(), s_deadline);
        }
    }

    // ---- Close (void completion bridge) ----

    [Fact]
    public async Task Close_OnMock_Succeeds()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Graceful close (Producer_close_async) resolves broker-free, then destroys. Surfaces
        // any close error (there is none on the mock).
        await TestTimeout.Run(() => producer.Close(), s_deadline);
    }

    [Fact]
    public async Task Close_CalledTwice_IsIdempotent()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // The one-shot latch: the first Close closes+destroys; the second is a no-op (no double
        // close / double destroy / throw).
        await TestTimeout.Run(() => producer.Close(), s_deadline);
        await TestTimeout.Run(() => producer.Close(), s_deadline);
    }

    [Fact]
    public async Task Close_ThenFlush_ThrowsObjectDisposed()
    {
        AsyncMockProducer producer = new AsyncMockProducer();
        await TestTimeout.Run(() => producer.Close(), s_deadline);

        // After Close the producer is torn down (destroyed) — a subsequent op throws the
        // use-after-dispose guard, before any P/Invoke.
        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    [Fact]
    public async Task Close_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored synchronously BEFORE the close is
        // submitted / the latch is taken (OperationCanceledException) — user cancellation.
        AsyncMockProducer producer = new AsyncMockProducer();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => producer.Close(cts.Token));

        // The latch was not taken → a real Close still works (the pre-cancel did not consume the
        // one-shot teardown).
        await TestTimeout.Run(() => producer.Close(), s_deadline);
    }

    // ---- Close(TimeSpan) — .NET-side deadline over Producer_close_async (no timed ABI) ----

    [Fact]
    public async Task CloseTimeout_OnMock_Succeeds()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // A generous deadline: the broker-free close resolves well within it (closeTask wins the
        // race), so this surfaces the successful close then destroys.
        await TestTimeout.Run(() => producer.Close(TimeSpan.FromSeconds(10)), s_deadline);
    }

    [Fact]
    public async Task CloseTimeout_ZeroDeadline_ReturnsAndDestroys()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Zero is a valid "don't wait" deadline: the awaiter completes best-effort (the native
        // close continues + frees its own rooting), then destroys. Regression: it RETURNS (no
        // hang) and the producer is torn down afterward.
        await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    [Fact]
    public async Task CloseTimeout_NegativeTimeout_ThrowsArgumentOutOfRange()
    {
        using AsyncMockProducer producer = new AsyncMockProducer();

        // A negative deadline is a programmer error (Java close(Duration) rejects it) — thrown
        // synchronously before any native call, before the latch is taken. Pin the
        // contract-bearing paramName + message (DoD §3 / ffi §A5), mirroring the consumer's
        // directly-analogous Close_NegativeTimeout_ThrowsArgumentOutOfRange.
        ArgumentOutOfRangeException ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => producer.Close(TimeSpan.FromMilliseconds(-1)));
        Assert.Equal("timeout", ex.ParamName);
        Assert.Contains("Timeout must not be negative.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CloseTimeout_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockProducer producer = new AsyncMockProducer();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => producer.Close(TimeSpan.FromSeconds(10), cts.Token));
    }

    // ---- PartitionsFor (owned-handle completion bridge; empty on the mock) ----

    [Fact]
    public async Task PartitionsFor_OnMock_ReturnsEmptyList()
    {
        // Honest caveat (PLAN §2): the mock ctor builds an EMPTY cluster, so partitions_for
        // succeeds broker-free but returns an empty list for every topic. This is a success with
        // an empty result (not a fault) — a populated list is integration-only. Exercises the
        // owned-handle completion + the empty-container copy-out path.
        using AsyncMockProducer producer = new AsyncMockProducer();

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(producer, Topic);

        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public async Task PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        // Java/Python-faithful: an EMPTY topic is FORWARDED to the core, NOT rejected client-side
        // (the binding guards only null). The mock returns an empty list — proving the empty
        // topic reached the core (a client-side rejection would have thrown ArgumentException).
        using AsyncMockProducer producer = new AsyncMockProducer();

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(producer, string.Empty);

        Assert.Empty(partitions);
    }

    [Fact]
    public async Task PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        // The binding guards only null (FFI panic-safety, §A5) — thrown before any native call.
        // Pin the contract-bearing paramName (DoD §3 / ffi §A5), mirroring the consumer's
        // PartitionsFor_NullTopic ParamName assertion. ArgumentNullException(nameof(topic)) sets no
        // custom message, so only ParamName is a contract to pin.
        using AsyncMockProducer producer = new AsyncMockProducer();

        ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public async Task PartitionsFor_NullTopic_ThrownBeforeDisposedCheck_EvenWhenDisposed()
    {
        // The null guard in PartitionsForWithCallback precedes the disposed check (the submit
        // helper's ThrowIfDisposed), so a disposed producer + null topic surfaces
        // ArgumentNullException, NOT ObjectDisposedException — the consumer's
        // *_ThrownBeforeNativeCall_EvenWhenClosed precedent.
        AsyncMockProducer producer = new AsyncMockProducer();
        await producer.DisposeAsync();

        ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public async Task PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer producer = new AsyncMockProducer();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.PartitionsFor(Topic));
    }

    [Fact]
    public async Task PartitionsFor_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored synchronously BEFORE any native
        // call (OperationCanceledException, via ThrowIfCancellationRequested in the submit
        // helper) — user cancellation, distinct from a wakeup (the producer has none).
        using AsyncMockProducer producer = new AsyncMockProducer();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => producer.PartitionsFor(Topic, cts.Token));
    }

    [Fact]
    public async Task PartitionsFor_CalledRepeatedly_Succeeds()
    {
        // The reusable-after-op seam: repeated queries free the GCHandle + list root once per op
        // (no leak / corruption).
        using AsyncMockProducer producer = new AsyncMockProducer();

        for (int i = 0; i < 5; i++)
        {
            IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(producer, Topic);
            Assert.Empty(partitions);
        }
    }

    // ---- Preconditions on Flush after dispose (before any native call) ----

    [Fact]
    public async Task Flush_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer producer = new AsyncMockProducer();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    // ---- Helpers (every awaited op under the TestTimeout hang guard) ----

    private static async Task<IReadOnlyList<PartitionInfo>> PartitionsForOf(IAsyncProducer producer, string topic)
    {
        IReadOnlyList<PartitionInfo> result = null!;
        await TestTimeout.Run(async () => result = await producer.PartitionsFor(topic), s_deadline);
        return result;
    }
}
