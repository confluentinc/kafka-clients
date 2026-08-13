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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M11/P2 producer <b>Dispose upgrade</b> teardown (ffi-marshalling.md §A7, PLAN §4
/// Decision 1): <see cref="AsyncKafkaProducer.DisposeAsync"/> closes gracefully
/// (<c>Producer_close_async</c>) before destroy; <see cref="AsyncKafkaProducer.Dispose"/> is the
/// blocking fallback (sync <c>Producer_close</c> → destroy). Both are idempotent (one-shot latch)
/// and mutually exclusive with <see cref="AsyncKafkaProducer.Close(System.Threading.CancellationToken)"/>,
/// and a close error never prevents the destroy. The regression these tests guard is that teardown
/// <b>returns without hanging</b> (the completion-bridge / pump-join guard) and does not
/// double-free / use-after-free (surfaced as a crash under GC churn). The graceful upgrade is
/// layered above <c>NativeProducer</c>, whose teardown stays the pinned <c>Producer_destroy</c>-only
/// sequence.
/// </summary>
/// <remarks>
/// <b>Close-error-still-destroys is a documented reachability limit.</b> The "a close error still
/// proceeds to destroy" path (the <c>finally</c> in <c>NativeProducer</c>'s teardown) cannot be
/// triggered broker-free: the mock's <c>close_async</c> / <c>close</c> always succeed, and a real
/// producer against no broker does not fault a close quickly. The mechanism (destroy in a
/// <c>finally</c>, independent of the close outcome) is verified by inspection and by the identical
/// consumer teardown (which swallows the close error then destroys). So it is a documented limit,
/// not a silent gap.
/// </remarks>
public sealed class PublicProducerTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- DisposeAsync (primary) / Dispose (fallback) round-trip ----

    [Fact]
    public async Task DisposeAsync_WhenIdle_Returns()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Graceful close_async → destroy, returns without hanging (the pump-join / bridge guard).
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WhenIdle_Returns()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Graceful sync close → destroy, returns without hanging.
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WithFlushInFlight_Returns()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Submit and do NOT await: the span-the-op ref keeps the handle alive until the flush
        // callback fires, so teardown is use-after-free-safe. The regression is that DisposeAsync
        // RETURNS without hanging even with an unawaited op in flight.
        _ = producer.Flush();

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WithFlushInFlight_Returns()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        _ = producer.Flush();

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    // ---- Idempotence / mixed teardown (one-shot latch) ----

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsSafe()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
        // The second call is a no-op (one-shot latch) — no double close / double destroy / throw.
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_ThenDisposeAsync_IsSafe()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        TestTimeout.Run(producer.Dispose, s_deadline);
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Close_ThenDispose_IsSafe()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Close wins the latch (close+destroy); the following Dispose loses it and no-ops — no
        // double destroy.
        await TestTimeout.Run(async () => await producer.Close(), s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task ConcurrentDispose_IsSafe()
    {
        AsyncMockProducer producer = new AsyncMockProducer();

        // Many threads race Dispose: exactly one wins the latch; the rest no-op. No double
        // destroy, no crash, all return.
        Task[] disposers = Enumerable
            .Range(0, 8)
            .Select(_ => Task.Run(() => producer.Dispose()))
            .ToArray();

        await TestTimeout.Run(() => Task.WhenAll(disposers), s_deadline);
    }

    // ---- Use-after-dispose guard ----

    [Fact]
    public async Task Flush_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockProducer producer = new AsyncMockProducer();
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    // ---- Churn under GC pressure (surfaces double-free / use-after-free as a crash) ----

    [Fact]
    public void Dispose_WithFlushInFlight_Churned_ReturnsUnderChurn()
    {
        // Churn create → submit → Dispose under GC pressure. The regression is that teardown
        // RETURNS without hanging every time (the TestTimeout guard fails fast on any hang), and
        // aggressive GC + WaitForPendingFinalizers surfaces a double-free / use-after-free on the
        // straggler-callback path as a crash instead of hiding it. The unawaited op's Task is not
        // observed.
        for (int i = 0; i < 100; i++)
        {
            AsyncMockProducer producer = new AsyncMockProducer();
            _ = producer.Flush();

            TestTimeout.Run(producer.Dispose, s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    // ---- Real producer (config path) teardown round-trips broker-free ----

    [Fact]
    public async Task RealProducer_DisposeAsync_Returns()
    {
        // The real AsyncKafkaProducer (bootstrap.servers only, no broker) tears down without a
        // broker: graceful close_async (no pending sends → resolves) → destroy, returns without
        // hanging. Proves the Dispose upgrade on the real client, not just the mock.
        AsyncKafkaProducer producer = new AsyncKafkaProducer(
            new System.Collections.Generic.Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:9092",
            });

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }
}
