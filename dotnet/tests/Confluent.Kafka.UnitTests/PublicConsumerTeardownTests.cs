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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Teardown through the public surface: Dispose, DisposeAsync and Close return, an unawaited operation in flight
/// included, are idempotent under double and mixed calls, and every operation throws
/// <see cref="ObjectDisposedException"/> after them.
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
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        _ = consumer.Subscribe(ProofTopic());

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_WithUnawaitedOpInFlight_ReturnsWithoutHang()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        _ = consumer.Subscribe(ProofTopic());

        await TestTimeout.Run(() => Task.Run(consumer.Dispose), s_deadline);
    }

    [Fact]
    public async Task DoubleDisposeAsync_IsSafe()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);
        await TestTimeout.DisposeAsync(consumer);
    }

    [Fact]
    public void DoubleDispose_IsSafe()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        TestTimeout.Dispose(consumer);
    }

    [Fact]
    public async Task MixedDisposeAndDisposeAsync_IsSafe()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        await TestTimeout.DisposeAsync(consumer);
    }

    [Fact]
    public async Task UseAfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Poll(s_pollTimeout));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Unsubscribe());
        Assert.Throws<ObjectDisposedException>(() => { _ = consumer.Seek(new TopicPartition("t", 0), 0L); });
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
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.TeardownAsync(consumer, () => consumer.Close());
        await TestTimeout.DisposeAsync(consumer);
        TestTimeout.Dispose(consumer);
    }

    [Fact]
    public async Task Close_UseAfterClose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.TeardownAsync(consumer, () => consumer.Close());

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
    }

    [Fact]
    public async Task Close_OnMock_DoesNotSurfaceError_ButPathReturns()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.Subscribe(ProofTopic());

        await TestTimeout.Run(() => consumer.Close(), s_deadline);
    }

    [Fact]
    public async Task ManyConsumers_CreateAndDispose_NoLeakOrCrash()
    {
        await TestTimeout.Run(
            async () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    await consumer.Subscribe(ProofTopic());
                    await consumer.DisposeAsync();
                }
            },
            s_deadline);
    }
}
