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
/// <see cref="IAsyncDisposable.DisposeAsync"/> / <see cref="IAsyncConsumer.Close"/>
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
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_ReturnsWithoutHang()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Run(consumer.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WithUnawaitedOpInFlight_ReturnsWithoutHang()
    {
        // The accepted single-owner residual (strand + one-time leak) — the teardown
        // must still RETURN without hanging even with an unawaited op in flight.
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        _ = consumer.Subscribe(ProofTopic()); // unawaited, deliberately

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_WithUnawaitedOpInFlight_ReturnsWithoutHang()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        _ = consumer.Subscribe(ProofTopic()); // unawaited, deliberately

        await TestTimeout.Run(() => Task.Run(consumer.Dispose), s_deadline);
    }

    [Fact]
    public async Task DoubleDisposeAsync_IsSafe()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();
        await consumer.DisposeAsync();
    }

    [Fact]
    public void DoubleDispose_IsSafe()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();
        consumer.Dispose();
    }

    [Fact]
    public async Task MixedDisposeAndDisposeAsync_IsSafe()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task UseAfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Poll(s_pollTimeout));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Unsubscribe());
        Assert.Throws<ObjectDisposedException>(() => consumer.Seek(new TopicPartition("t", 0), 0L)); // sync (M5/P7)
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Fact]
    public async Task Close_ReturnsWithoutHang()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Close(), s_deadline);
    }

    [Fact]
    public async Task Close_ThenDisposeAndDisposeAsync_IsIdempotent()
    {
        // Close takes the one-shot latch and destroys; a subsequent
        // Dispose/DisposeAsync loses the latch and no-ops (closed-flag idempotence).
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.Close();
        await consumer.DisposeAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task Close_UseAfterClose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.Close();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
    }

    [Fact]
    public async Task Close_OnMock_DoesNotSurfaceError_ButPathReturns()
    {
        // Close surfaces a close KafkaException (unlike DisposeAsync, which swallows
        // it). Broker-free, an AsyncMockConsumer close succeeds — the error-surfacing path
        // (throw) is verified by inspection (CloseWithCallbackInternal awaits close_async
        // and rethrows its KafkaException; only DisposeAsync's catch swallows it). Here
        // the reachable assertion is that a broker-free Close completes without faulting.
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.Subscribe(ProofTopic());

        await TestTimeout.Run(() => consumer.Close(), s_deadline);
    }

    [Fact]
    public async Task ManyConsumers_CreateAndDispose_NoLeakOrCrash()
    {
        // Create/dispose many consumers — the handle-leak / double-free detector at the
        // public surface (the SafeHandle ReleaseHandle → Consumer_destroy path).
        for (int i = 0; i < 100; i++)
        {
            AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            await consumer.Subscribe(ProofTopic());
            await consumer.DisposeAsync();
        }
    }
}
