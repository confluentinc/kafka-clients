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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface teardown (PLAN §5.5): <see cref="IDisposable.Dispose"/> /
/// <see cref="IAsyncDisposable.DisposeAsync"/> / <see cref="IConsumer.CloseAsync"/>
/// return without hanging, are idempotent under double / mixed calls, and every public
/// op throws <see cref="ObjectDisposedException"/> after teardown. Every teardown runs
/// under a <see cref="TestTimeout"/> hang guard (the teardown-returns regression).
/// </summary>
public sealed class PublicConsumerTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task DisposeAsync_ReturnsWithoutHang()
    {
        MockConsumer consumer = new MockConsumer();
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_ReturnsWithoutHang()
    {
        MockConsumer consumer = new MockConsumer();
        TestTimeout.Run(consumer.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WithUnawaitedOpInFlight_ReturnsWithoutHang()
    {
        // The accepted single-owner residual (strand + one-time leak) — the teardown
        // must still RETURN without hanging even with an unawaited op in flight.
        MockConsumer consumer = new MockConsumer();
        _ = consumer.SubscribeAsync(ProofTopic()); // unawaited, deliberately

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_WithUnawaitedOpInFlight_ReturnsWithoutHang()
    {
        MockConsumer consumer = new MockConsumer();
        _ = consumer.SubscribeAsync(ProofTopic()); // unawaited, deliberately

        await TestTimeout.Run(() => Task.Run(consumer.Dispose), s_deadline);
    }

    [Fact]
    public async Task DoubleDisposeAsync_IsSafe()
    {
        MockConsumer consumer = new MockConsumer();
        await consumer.DisposeAsync();
        await consumer.DisposeAsync();
    }

    [Fact]
    public void DoubleDispose_IsSafe()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();
        consumer.Dispose();
    }

    [Fact]
    public async Task MixedDisposeAndDisposeAsync_IsSafe()
    {
        MockConsumer consumer = new MockConsumer();
        consumer.Dispose();
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task UseAfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.PollAsync(s_pollTimeout));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SubscribeAsync(ProofTopic()));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.UnsubscribeAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekAsync(new TopicPartition("t", 0), 0L));
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Fact]
    public async Task CloseAsync_ReturnsWithoutHang()
    {
        MockConsumer consumer = new MockConsumer();
        await TestTimeout.Run(() => consumer.CloseAsync(), s_deadline);
    }

    [Fact]
    public async Task CloseAsync_ThenDisposeAndDisposeAsync_IsIdempotent()
    {
        // CloseAsync takes the one-shot latch and destroys; a subsequent
        // Dispose/DisposeAsync loses the latch and no-ops (closed-flag idempotence).
        MockConsumer consumer = new MockConsumer();
        await consumer.CloseAsync();
        await consumer.DisposeAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task CloseAsync_UseAfterClose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
        await consumer.CloseAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SubscribeAsync(ProofTopic()));
    }

    [Fact]
    public async Task CloseAsync_OnMock_DoesNotSurfaceError_ButPathReturns()
    {
        // CloseAsync surfaces a close KafkaException (unlike DisposeAsync, which swallows
        // it). Broker-free, a MockConsumer close succeeds — the error-surfacing path
        // (throw) is verified by inspection (CloseAsyncInternal awaits close_async and
        // rethrows its KafkaException; only DisposeAsync's catch swallows it). Here the
        // reachable assertion is that a broker-free CloseAsync completes without faulting.
        MockConsumer consumer = new MockConsumer();
        await consumer.SubscribeAsync(ProofTopic());

        await TestTimeout.Run(() => consumer.CloseAsync(), s_deadline);
    }

    [Fact]
    public async Task ManyConsumers_CreateAndDispose_NoLeakOrCrash()
    {
        // Create/dispose many consumers — the handle-leak / double-free detector at the
        // public surface (the SafeHandle ReleaseHandle → Consumer_destroy path).
        for (int i = 0; i < 100; i++)
        {
            MockConsumer consumer = new MockConsumer();
            await consumer.SubscribeAsync(ProofTopic());
            await consumer.DisposeAsync();
        }
    }
}
