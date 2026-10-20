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
/// The asynchronous producer's Flush, Close and PartitionsFor on <see cref="AsyncMockProducer{TKey, TValue}"/>: their
/// completions, preconditions and pre-cancelled tokens. The mock's cluster is empty, so PartitionsFor returns an
/// empty list. Every awaited operation runs under a <see cref="TestTimeout"/> hang guard.
/// </summary>
public sealed class PublicProducerPeripheralTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "peripheral-topic";

    [Fact]
    public async Task Flush_OnMock_Succeeds()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(() => producer.Flush(), s_deadline);
    }

    [Fact]
    public async Task Flush_CalledRepeatedly_Succeeds()
    {
        // Repeated flushes free their per-call state once each.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 5; i++)
        {
            await TestTimeout.Run(() => producer.Flush(), s_deadline);
        }
    }

    [Fact]
    public async Task Close_OnMock_Succeeds()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(() => producer.Close(), s_deadline);
    }

    [Fact]
    public async Task Close_CalledTwice_IsIdempotent()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // The first Close closes and destroys; the second is a no-op.
        await TestTimeout.Run(() => producer.Close(), s_deadline);
        await TestTimeout.Run(() => producer.Close(), s_deadline);

        AsyncMockProducer<byte[], byte[]> timed = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => timed.Close(TimeSpan.Zero), s_deadline);
        await TestTimeout.Run(() => timed.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public async Task Close_ThenFlush_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => producer.Close(), s_deadline);

        // After Close a call throws ObjectDisposedException, before any native call.
        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    [Fact]
    public async Task Close_PreCanceledToken_ThrowsOperationCanceled()
    {
        // An already-cancelled token throws before the close begins.
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => producer.Close(cts.Token));

        // The pre-cancelled call did not begin the close, so a real Close still works.
        await TestTimeout.Run(() => producer.Close(), s_deadline);
    }

    [Fact]
    public async Task PartitionsFor_OnMock_ReturnsEmptyList()
    {
        // The mock's cluster is empty, so PartitionsFor succeeds with an empty list; a populated list needs a broker.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(producer, Topic);

        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public async Task PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        // An empty topic reaches the core, as in Java and Python; only null is rejected.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(producer, string.Empty);

        Assert.Empty(partitions);
    }

    [Fact]
    public async Task PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        // Only null is rejected, before any native call; ArgumentNullException sets no custom message, so ParamName
        // is the asserted contract.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public async Task PartitionsFor_NullTopic_ThrownBeforeDisposedCheck_EvenWhenDisposed()
    {
        // The null check precedes the closed check, so a closed producer with a null topic throws
        // ArgumentNullException.
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public async Task PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.PartitionsFor(Topic));
    }

    [Fact]
    public async Task PartitionsFor_PreCanceledToken_ThrowsOperationCanceled()
    {
        // An already-cancelled token throws before any native call.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => producer.PartitionsFor(Topic, cts.Token));
    }

    [Fact]
    public async Task PartitionsFor_CalledRepeatedly_Succeeds()
    {
        // Repeated queries free their per-call state and list root once each.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 5; i++)
        {
            IReadOnlyList<PartitionInfo> partitions = await PartitionsForOf(producer, Topic);
            Assert.Empty(partitions);
        }
    }

    [Fact]
    public async Task Flush_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    private static async Task<IReadOnlyList<PartitionInfo>> PartitionsForOf(IAsyncProducer<byte[], byte[]> producer, string topic)
    {
        IReadOnlyList<PartitionInfo> result = null!;
        await TestTimeout.Run(async () => result = await producer.PartitionsFor(topic), s_deadline);
        return result;
    }
}
