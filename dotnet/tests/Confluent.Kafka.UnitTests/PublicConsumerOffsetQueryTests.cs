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
/// <see cref="IAsyncConsumer.Committed"/>, <see cref="IAsyncConsumer.OffsetsForTimes"/>,
/// <see cref="IAsyncConsumer.BeginningOffsets"/> and <see cref="IAsyncConsumer.EndOffsets"/> on the
/// <see cref="AsyncMockConsumer"/>: the copied-out maps, the faults, the preconditions, cancellation and reuse.
/// </summary>
/// <remarks>
/// The mock's <c>OffsetsForTimes</c> always fails with Java's "Not implemented yet.", so only its fault is asserted.
/// </remarks>
public sealed class PublicConsumerOffsetQueryTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "offset-query-topic";

    [Fact]
    public async Task BeginningOffsets_ReturnsSetOffsets()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset(Topic, 0, 5);
        consumer.UpdateBeginningOffset(Topic, 1, 7);

        IReadOnlyDictionary<TopicPartition, long> result = await BeginningOffsetsOf(
            consumer,
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });

        Assert.Equal(2, result.Count);
        Assert.Equal(5, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(7, result[new TopicPartition(Topic, 1)]);
    }

    [Fact]
    public async Task BeginningOffsets_NonAsciiTopic_RoundTripsThroughKeyMarshalling()
    {
        const string nonAscii = "topic-grüße-Ω-🎉";
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset(nonAscii, 3, 99);

        IReadOnlyDictionary<TopicPartition, long> result = await BeginningOffsetsOf(
            consumer, new[] { new TopicPartition(nonAscii, 3) });

        Assert.Equal(99, result[new TopicPartition(nonAscii, 3)]);
    }

    [Fact]
    public async Task BeginningOffsets_UnsetPartition_FaultsWithInvalidOperationExceptionMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => BeginningOffsetsOf(consumer, new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal(
            $"The partition {Topic}-0 does not have a beginning offset.",
            ex.Message);
    }

    [Fact]
    public async Task EndOffsets_ReturnsSetOffsets()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset(Topic, 0, 100);
        consumer.UpdateEndOffset(Topic, 2, 250);

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer,
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 2) });

        Assert.Equal(2, result.Count);
        Assert.Equal(100, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(250, result[new TopicPartition(Topic, 2)]);
    }

    [Fact]
    public async Task EndOffsets_UnsetPartition_FaultsWithInvalidOperationExceptionMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => EndOffsetsOf(consumer, new[] { new TopicPartition(Topic, 4) }));

        Assert.Equal(
            $"The partition {Topic}-4 does not have an end offset.",
            ex.Message);
    }

    [Fact]
    public async Task EndOffsets_AfterFault_ConsumerReusable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset(Topic, 0, 11);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => EndOffsetsOf(consumer, new[] { new TopicPartition(Topic, 9) }));

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer, new[] { new TopicPartition(Topic, 0) });
        Assert.Equal(11, result[new TopicPartition(Topic, 0)]);
    }

    [Fact]
    public async Task Committed_EmptyCollection_ReturnsEmptyMap()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = await CommittedOf(
            consumer, Array.Empty<TopicPartition>());

        Assert.Empty(result);
    }

    [Fact]
    public async Task Committed_UncommittedPartition_ReturnsEmptyMap()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = await CommittedOf(
            consumer, new[] { new TopicPartition(Topic, 0) });

        Assert.Empty(result);
    }

    [Fact]
    public async Task OffsetsForTimes_OnMock_FaultsWithNotSupportedException()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = 1_000L,
        };

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => OffsetsForTimesOf(consumer, search));

        Assert.Equal("Not implemented yet.", ex.Message);
    }

    [Fact]
    public async Task OffsetsForTimes_NegativeTimestamp_Accepted_ThenFaultsOnMock()
    {
        // A negative timestamp is a valid sentinel (earliest or latest), so it reaches the mock.
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = -2L, // LATEST sentinel
        };

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => OffsetsForTimesOf(consumer, search));

        Assert.Equal("Not implemented yet.", ex.Message);
    }

    [Fact]
    public async Task OffsetsForTimes_EmptyMap_StillFaultsOnMock()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => OffsetsForTimesOf(consumer, new Dictionary<TopicPartition, long>()));
    }

    [Fact]
    public async Task BeginningOffsets_EmptyCollection_ReturnsEmptyMap()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<TopicPartition, long> result = await BeginningOffsetsOf(
            consumer, Array.Empty<TopicPartition>());

        Assert.Empty(result);
    }

    [Fact]
    public async Task EndOffsets_EmptyCollection_ReturnsEmptyMap()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer, Array.Empty<TopicPartition>());

        Assert.Empty(result);
    }

    [Fact]
    public async Task Committed_NullCollection_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Committed(null!));
    }

    [Fact]
    public async Task BeginningOffsets_NullCollection_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.BeginningOffsets(null!));
    }

    [Fact]
    public async Task EndOffsets_NullCollection_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.EndOffsets(null!));
    }

    [Fact]
    public async Task OffsetsForTimes_NullMap_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.OffsetsForTimes(null!));
    }

    [Fact]
    public async Task Committed_NullElementTopic_ThrowsArgument()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentException>(
            () => consumer.Committed(new[] { default(TopicPartition) }));
    }

    [Fact]
    public async Task OffsetsForTimes_NullKeyTopic_ThrowsArgument()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [default] = 1L,
        };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.OffsetsForTimes(search));
    }

    [Fact]
    public async Task Committed_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.Committed(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public async Task BeginningOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public async Task EndOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.EndOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public async Task OffsetsForTimes_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.OffsetsForTimes(new Dictionary<TopicPartition, long>
            {
                [new TopicPartition(Topic, 0)] = 1L,
            }));
    }

    [Fact]
    public async Task BeginningOffsets_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateBeginningOffset(Topic, 0, 1);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }, cts.Token));
    }

    [Fact]
    public async Task Committed_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.Committed(new[] { new TopicPartition(Topic, 0) }, cts.Token));
    }

    [Fact]
    public async Task EndOffsets_AfterWakeup_StillUsable()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.UpdateEndOffset(Topic, 0, 5);

        consumer.Wakeup();

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer, new[] { new TopicPartition(Topic, 0) });
        Assert.Equal(5, result[new TopicPartition(Topic, 0)]);
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsetsOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, long> result = null!;
        await TestTimeout.Run(async () => result = await consumer.BeginningOffsets(partitions), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsetsOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, long> result = null!;
        await TestTimeout.Run(async () => result = await consumer.EndOffsets(partitions), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> CommittedOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = null!;
        await TestTimeout.Run(async () => result = await consumer.Committed(partitions), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimesOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyDictionary<TopicPartition, long> search)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> result = null!;
        await TestTimeout.Run(async () => result = await consumer.OffsetsForTimes(search), s_deadline);
        return result;
    }
}
