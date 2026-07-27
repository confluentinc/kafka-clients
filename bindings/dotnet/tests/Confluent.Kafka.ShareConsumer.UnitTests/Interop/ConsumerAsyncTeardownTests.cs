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
    public void Dispose_WithOpInFlight_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        _ = consumer.SubscribeAsync(ProofTopic());

        // The sync fallback returns without hanging (close_with_timeout → destroy).
        TestTimeout.Run(consumer.Dispose, s_deadline);
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
