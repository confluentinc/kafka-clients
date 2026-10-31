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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IConsumerCommon.Assignment"/>, <see cref="IConsumerCommon.Subscription"/>,
/// <see cref="IConsumerCommon.Paused"/> and <see cref="IConsumerCommon.EnforceRebalance"/> on the
/// <see cref="AsyncMockConsumer"/>.
/// </summary>
/// <remarks>
/// The core returns each set as a hash set, so the tests compare sets, never sequences.
/// </remarks>
public sealed class PublicConsumerSyncReadTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Assignment_FreshConsumer_IsEmpty()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public async Task Assignment_ReflectsAssign_ExactlyTheAssignedPartitions()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition[] assigned =
        {
            new TopicPartition("t1", 0),
            new TopicPartition("t2", 3),
        };
        await TestTimeout.Run(() => consumer.Assign(assigned), s_deadline);

        IReadOnlyCollection<TopicPartition> result = consumer.Assignment();

        Assert.Equal(2, result.Count);
        Assert.Equal(
            new HashSet<TopicPartition>(assigned),
            new HashSet<TopicPartition>(result));
    }

    [Fact]
    public async Task Assignment_ReturnsFreshSnapshotEachCall()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition("t", 0) }), s_deadline);

        IReadOnlyCollection<TopicPartition> first = consumer.Assignment();
        IReadOnlyCollection<TopicPartition> second = consumer.Assignment();

        Assert.NotSame(first, second);
        Assert.Equal(
            new HashSet<TopicPartition>(first),
            new HashSet<TopicPartition>(second));
    }

    [Fact]
    public void Subscription_FreshConsumer_IsEmpty()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Assert.Empty(consumer.Subscription());
    }

    [Fact]
    public async Task Subscription_ReflectsSubscribe_ExactlyTheSubscribedTopics()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        string[] topics = { "t1", "t2" };
        await TestTimeout.Run(() => consumer.Subscribe(topics), s_deadline);

        IReadOnlyCollection<string> result = consumer.Subscription();

        Assert.Equal(2, result.Count);
        Assert.Equal(
            new HashSet<string>(topics, StringComparer.Ordinal),
            new HashSet<string>(result, StringComparer.Ordinal));
    }

    [Fact]
    public void Paused_FreshConsumer_IsEmpty()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public async Task Paused_AfterAssign_IsStillEmpty()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition("t", 0) }), s_deadline);

        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public async Task Subscription_NonAsciiTopic_RoundTripsByteForByte()
    {
        const string nonAscii = "café-topic-Ω-日本語-😀";
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { nonAscii }), s_deadline);

        IReadOnlyCollection<string> result = consumer.Subscription();

        string only = Assert.Single(result);
        Assert.Equal(nonAscii, only);
    }

    [Fact]
    public async Task Assignment_NonAsciiTopic_RoundTripsByteForByte()
    {
        const string nonAscii = "topic-grüße-Ω-🎉";
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition tp = new TopicPartition(nonAscii, 7);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);

        TopicPartition only = Assert.Single(consumer.Assignment());
        Assert.Equal(nonAscii, only.Topic);
        Assert.Equal(7, only.Partition);
    }

    [Fact]
    public async Task SyncReads_OnFreeGuard_RoundTrip()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition("t", 0) }), s_deadline);

        Assert.Single(consumer.Assignment());
        Assert.Empty(consumer.Subscription());
        Assert.Empty(consumer.Paused());
    }

    [Fact]
    public void Assignment_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public void Subscription_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
    }

    [Fact]
    public void Paused_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        Assert.Throws<ObjectDisposedException>(() => consumer.Paused());
    }

    [Fact]
    public void EnforceRebalance_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        Assert.Throws<ObjectDisposedException>(() => consumer.EnforceRebalance());
    }

    [Fact]
    public void EnforceRebalance_NoArg_ReturnsWithoutThrowing()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.EnforceRebalance();
    }

    [Fact]
    public void EnforceRebalance_WithReason_ReturnsWithoutThrowing()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.EnforceRebalance("some reason");
    }

    [Fact]
    public void EnforceRebalance_ReasonAndNoArg_BehaveIdentically()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.EnforceRebalance();
        consumer.EnforceRebalance(null);
        consumer.EnforceRebalance("café-Ω-reason");
        consumer.EnforceRebalance(string.Empty);
    }

    [Fact]
    public async Task SyncReads_ReachableViaIAsyncConsumerInterface()
    {
        AsyncMockConsumer<byte[], byte[]> mock = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        await TestTimeout.Run(() => mock.Subscribe(new[] { "sub" }), s_deadline);

        IAsyncConsumer<byte[], byte[]> consumer = mock;
        Assert.Single(consumer.Subscription());
        Assert.Empty(consumer.Assignment());
        Assert.Empty(consumer.Paused());
        consumer.EnforceRebalance();
    }
}
