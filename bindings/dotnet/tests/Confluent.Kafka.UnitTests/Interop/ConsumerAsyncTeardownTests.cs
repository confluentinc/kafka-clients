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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Single-owner teardown (ffi-marshalling.md §B7): <c>DisposeAsync</c> closes
/// asynchronously (<c>close_async</c> joins the bg task) before destroy;
/// <c>Dispose</c> is the blocking fallback (<c>close_with_timeout → destroy</c>).
/// Under the not-thread-safe contract the awaiter of an op is its disposer, so
/// neither path drains a <em>separately-submitted</em> op; the regression these
/// tests guard is that teardown <b>returns without hanging</b> even with an
/// (unawaited) op still in flight — an accepted misuse case that may strand the op's
/// <c>Task</c> + leak its <c>GCHandle</c> once, so the tests must NOT assert the op
/// <c>Task</c> reaches a terminal state. The atomic closed flag makes double /
/// concurrent / mixed teardown safe, and use-after-dispose throws.
/// </summary>
public sealed class ConsumerAsyncTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task DisposeAsync_WithOpInFlight_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        // Submit and do NOT await: under single-owner, DisposeAsync does NOT drain a
        // separately-submitted op — it closes gracefully (close_async joins the bg
        // task) then destroys. The regression is that teardown RETURNS without hanging
        // even with an unawaited op in flight. The op's own Task is an accepted
        // strand+leak residual (misuse case), so it is deliberately NOT observed here.
        _ = consumer.SubscribeAsync(ProofTopic());

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WhenIdle_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WithOpInFlight_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        // Submit and do NOT await. Under single-owner the sync Dispose does not drain
        // a separately-submitted op — it closes gracefully (close_with_timeout) then
        // destroys. The regression is that Dispose RETURNS without hanging even with
        // an unawaited op in flight. The op's Task is an accepted strand+leak residual
        // (misuse case), so it is deliberately NOT observed to a terminal state.
        _ = consumer.SubscribeAsync(ProofTopic());

        TestTimeout.Run(consumer.Dispose, s_deadline);
    }

    [Fact]
    public void Dispose_WithOpInFlight_Churned_ReturnsUnderChurn()
    {
        // Churn create → submit → Dispose under GC pressure. The regression is that
        // teardown RETURNS without hanging every time (the TestTimeout guard fails
        // fast on any hang). Aggressive GC + WaitForPendingFinalizers surface a
        // double-free / use-after-free on the straggler-callback path as a crash
        // instead of hiding. The unawaited op's Task is the accepted strand+leak
        // residual (DisposeAsync on the awaiting task avoids it), so it is not
        // observed here.
        for (int i = 0; i < 100; i++)
        {
            NativeConsumer consumer = NativeConsumer.CreateMock();
            _ = consumer.SubscribeAsync(ProofTopic());

            TestTimeout.Run(consumer.Dispose, s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
        // The second call is a no-op (thread-safe closed flag) — no double close /
        // double destroy / throw.
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_ThenDisposeAsync_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        TestTimeout.Run(consumer.Dispose, s_deadline);
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task ConcurrentDispose_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        // Many threads race Dispose: exactly one wins TryBeginClose; the rest no-op.
        // No double destroy, no crash, all return.
        Task[] disposers = Enumerable
            .Range(0, 8)
            .Select(_ => Task.Run(() => consumer.Dispose()))
            .ToArray();

        await TestTimeout.Run(() => Task.WhenAll(disposers), s_deadline);
    }

    [Fact]
    public async Task Operation_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.SubscribeAsync(ProofTopic()));
    }

    [Fact]
    public async Task Handle_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Handle);
    }
}
