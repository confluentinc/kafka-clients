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

using Confluent.Kafka.ShareConsumer.Internal;

using Xunit;

namespace Confluent.Kafka.ShareConsumer.UnitTests.Interop;

/// <summary>
/// Async-aware teardown (ffi-marshalling.md §B7): <c>DisposeAsync</c> drains the
/// in-flight op (wakeup + await) then closes asynchronously before destroy;
/// <c>Dispose</c> is the blocking fallback. A bare destroy before the in-flight
/// callback fires would hang the <c>Task</c> and leak the <c>GCHandle</c>, so the
/// key regression is that teardown with an op in flight <b>returns</b> (under
/// <see cref="TestTimeout"/>). The thread-safe closed flag makes double / concurrent
/// / mixed teardown safe, and use-after-dispose throws.
/// </summary>
public sealed class ConsumerAsyncTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task DisposeAsync_WithOpInFlight_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        // Submit and do NOT await: DisposeAsync must drain it (wakeup + await) and
        // return, not hang on the never-fired-callback path.
        Task op = consumer.SubscribeAsync(ProofTopic());

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);

        // The op resolved (or was drained); observing it must not hang either.
        await TestTimeout.Run(
            async () =>
            {
                try
                {
                    await op;
                }
                catch (KafkaException)
                {
                    // A drained op may fault; the point is that it completes.
                }
            },
            s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WhenIdle_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_WithOpInFlight_ReturnsAndCompletesTheOpTask()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        // OBSERVE the op Task — do NOT discard it. The sync Dispose's guarded
        // close_with_timeout is rejected while the op holds the core guard, so it
        // does not drain; the following destroy cancels the op's callback, which
        // (before the strand fix) STRANDED this Task (never terminal). Dispose must
        // now fault it deterministically (FaultTaskOnly) — without freeing the
        // GCHandle (the callback is the sole owner of that free).
        Task op = consumer.SubscribeAsync(ProofTopic());

        // The sync fallback returns without hanging (close_with_timeout → destroy).
        TestTimeout.Run(consumer.Dispose, s_deadline);

        // The op Task must reach a TERMINAL state — RanToCompletion if it resolved
        // before teardown, or Faulted (ObjectDisposedException) if Dispose faulted an
        // op whose callback destroy cancelled. A stranded Task would hang here and
        // the TestTimeout guard would fail the run fast (the strand can't hide).
        await TestTimeout.Run(
            async () =>
            {
                try
                {
                    await op;
                }
                catch (ObjectDisposedException)
                {
                    // Dispose faulted an op whose callback was cancelled by destroy.
                }
                catch (KafkaException)
                {
                    // A drained/reclaimed op may fault with a KafkaException.
                }
            },
            s_deadline);

        Assert.True(op.IsCompleted);
    }

    [Fact]
    public async Task Dispose_WithOpInFlight_Churned_EveryOpTaskCompletes()
    {
        // Churn create → submit → Dispose → observe under GC pressure. If FaultTaskOnly
        // failed to fault the Task it would strand (the await below hangs → the guard
        // fails fast). In the common Mock path the callback fires and frees the
        // GCHandle itself (sole owner), so there is no leak; only the rare case where
        // destroy cancels the op before its callback is queued leaks one handle (the
        // accepted teardown-only residual — DisposeAsync avoids it). Aggressive GC +
        // WaitForPendingFinalizers make a double-free / use-after-free on the
        // straggler-callback path surface as a crash instead of hiding. Every op Task
        // must reach a terminal state.
        for (int i = 0; i < 100; i++)
        {
            NativeConsumer consumer = NativeConsumer.CreateMock();
            Task op = consumer.SubscribeAsync(ProofTopic());

            TestTimeout.Run(consumer.Dispose, s_deadline);

            await TestTimeout.Run(
                async () =>
                {
                    try
                    {
                        await op;
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    catch (KafkaException)
                    {
                    }
                },
                s_deadline);

            Assert.True(op.IsCompleted);

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
