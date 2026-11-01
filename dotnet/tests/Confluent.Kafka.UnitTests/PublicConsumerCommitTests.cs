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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The commit family on the <see cref="AsyncMockConsumer"/>: both <c>Commit</c> forms, <c>CommitAsync</c>, the
/// public <see cref="OffsetAndMetadata"/> constructor, and a commit read back through
/// <see cref="IAsyncConsumer.Committed"/>.
/// </summary>
public sealed class PublicConsumerCommitTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "commit-topic";

    [Fact]
    public async Task Commit_ThenCommitted_RoundTripsOffsetMetadataAndEpoch()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 0);
        await AssignOf(consumer, new[] { tp });

        OffsetAndMetadata oam = new OffsetAndMetadata(42, "meta-x", 7);
        await CommitOffsetsOf(
            consumer, new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = oam });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = await CommittedOf(consumer, new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(42, result[tp]!.Offset);
        Assert.Equal("meta-x", result[tp]!.Metadata);
        Assert.Equal(7, result[tp]!.LeaderEpoch);
    }

    [Fact]
    public async Task Commit_NullMetadataAndEpoch_RoundTripsAsEmptyMetadataAndNullEpoch()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(Topic, 1);
        await AssignOf(consumer, new[] { tp });

        OffsetAndMetadata oam = new OffsetAndMetadata(100);
        await CommitOffsetsOf(
            consumer, new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = oam });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = await CommittedOf(consumer, new[] { tp });

        Assert.True(result.ContainsKey(tp));
        Assert.Equal(100, result[tp]!.Offset);
        Assert.Equal(string.Empty, result[tp]!.Metadata);
        Assert.Null(result[tp]!.LeaderEpoch);
    }

    [Fact]
    public async Task Commit_NoOffsets_ResolvesBrokerFree()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await CommitOf(consumer);
    }

    [Fact]
    public async Task Commit_EmptyMap_ResolvesBrokerFree()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await CommitOffsetsOf(consumer, new Dictionary<TopicPartition, OffsetAndMetadata>());
    }

    [Fact]
    public async Task CommitAsync_CompletesOnceAccepted()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await TestTimeout.Run(() => consumer.CommitAsync(), s_deadline);
    }

    [Fact]
    public async Task CommitAsync_ViaIAsyncConsumerInterface_Completes()
    {
        AsyncMockConsumer<byte[], byte[]> mock = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IAsyncConsumer<byte[], byte[]> consumer = mock;

        await TestTimeout.Run(() => consumer.CommitAsync(), s_deadline);
    }

    [Fact]
    public async Task Commit_NullOffsets_ThrowsArgumentNull()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Commit(null!));
    }

    [Fact]
    public async Task Commit_NullElementTopic_ThrowsArgument()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [default] = new OffsetAndMetadata(1),
        };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Commit(offsets));
    }

    [Fact]
    public async Task Commit_NullOffsetValue_ThrowsArgument()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = null!,
        };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.Commit(offsets));
    }

    [Fact]
    public void OffsetAndMetadata_NegativeOffset_ThrowsArgumentOutOfRangeWithJavaMessage()
    {
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new OffsetAndMetadata(-1));

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("Invalid negative offset", ex.Message);
    }

    [Fact]
    public void OffsetAndMetadata_NullMetadata_CoercedToEmptyAndNullEpoch()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(5);

        Assert.Equal(5, value.Offset);
        Assert.Equal(string.Empty, value.Metadata);
        Assert.Null(value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndMetadata_AllThreeSet_AreStored()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(5, "m", 3);

        Assert.Equal(5, value.Offset);
        Assert.Equal("m", value.Metadata);
        Assert.Equal(3, value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndMetadata_ZeroOffset_IsAccepted()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(0, "boundary");

        Assert.Equal(0, value.Offset);
        Assert.Equal("boundary", value.Metadata);
    }

    [Fact]
    public async Task Commit_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Commit());
    }

    [Fact]
    public async Task CommitOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(1),
            }));
    }

    [Fact]
    public async Task CommitAsync_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<ObjectDisposedException>(() => { _ = consumer.CommitAsync(); });
    }

    [Fact]
    public async Task Commit_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.Commit(cts.Token));
    }

    [Fact]
    public async Task CommitOffsets_PreCanceledToken_ThrowsOperationCanceled()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.Commit(
                new Dictionary<TopicPartition, OffsetAndMetadata>
                {
                    [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(1),
                },
                cts.Token));
    }

    private static async Task AssignOf(IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions) =>
        await TestTimeout.Run(() => consumer.Assign(partitions), s_deadline);

    private static async Task CommitOf(IAsyncConsumer<byte[], byte[]> consumer) =>
        await TestTimeout.Run(() => consumer.Commit(), s_deadline);

    private static async Task CommitOffsetsOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        await TestTimeout.Run(() => consumer.Commit(offsets), s_deadline);

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> CommittedOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = null!;
        await TestTimeout.Run(async () => result = await consumer.Committed(partitions), s_deadline);
        return result;
    }
}
