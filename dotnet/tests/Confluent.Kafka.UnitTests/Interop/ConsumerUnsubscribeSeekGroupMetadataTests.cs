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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The internal <c>NativeConsumer</c>'s <c>UnsubscribeWithCallback</c>, its <c>Seek</c> preconditions,
/// <c>GroupMetadata()</c> on the real and mock consumers, and <c>CloseWithCallback</c>.
/// </summary>
public sealed class ConsumerUnsubscribeSeekGroupMetadataTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    private static Dictionary<string, string> RealConsumerConfig(
        string groupId, string? groupInstanceId = null)
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
            ["group.id"] = groupId,
        };
        if (groupInstanceId is not null)
        {
            config["group.instance.id"] = groupInstanceId;
        }

        return config;
    }

    [Fact]
    public async Task UnsubscribeWithCallback_OnMock_Completes()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await consumer.SubscribeWithCallback(ProofTopic());
        await TestTimeout.Run(() => consumer.UnsubscribeWithCallback(), s_deadline);
    }

    [Fact]
    public async Task UnsubscribeWithCallback_WithoutSubscribe_Completes()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await TestTimeout.Run(() => consumer.UnsubscribeWithCallback(), s_deadline);
    }

    [Fact]
    public async Task UnsubscribeWithCallback_Churned_NoLeakOrHang()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 100; i++)
        {
            await consumer.SubscribeWithCallback(ProofTopic());
            await TestTimeout.Run(() => consumer.UnsubscribeWithCallback(), s_deadline);
        }
    }

    [Fact]
    public async Task UnsubscribeWithCallback_AfterDispose_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.UnsubscribeWithCallback());
    }

    [Fact]
    public void Seek_NegativeOffset_ThrowsWithExactJavaMessage()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek("proof-topic", 0, offset: -1));

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("seek offset must not be a negative number", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Seek_NegativeOffset_OnAClosedConsumer_ReportsTheClosedConsumerFirst()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.Seek("proof-topic", 0, offset: -5));
    }

    [Fact]
    public void Seek_NegativePartition_ThrowsArgumentOutOfRange()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek("proof-topic", partition: -1, offset: 0));
        Assert.Equal("partition", ex.ParamName);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_PreJoin_ReturnsConfiguredGroupIdAndDefaults()
    {
        NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("proof-group"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("proof-group", meta.GroupId);
        Assert.Equal(-1, meta.GenerationId);
        Assert.Equal(string.Empty, meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_NonAsciiGroupId_RoundTrips()
    {
        NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("grüße-café-Ω-日本語-😀"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("grüße-café-Ω-日本語-😀", meta.GroupId);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_WithGroupInstanceId_IsTheConfiguredValuePreJoin()
    {
        NativeConsumer consumer = NativeConsumer.Create(
            RealConsumerConfig("static-group", groupInstanceId: "instance-7"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("static-group", meta.GroupId);
        Assert.Equal("instance-7", meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_MockConsumer_ReturnsMockSentinels()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("dummy.group.id", meta.GroupId);
        Assert.Equal(1, meta.GenerationId);
        Assert.Equal("1", meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_AfterDispose_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Fact]
    public async Task CloseWithCallback_OnMock_ReturnsWithoutHang()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.CloseWithCallback(), s_deadline);
    }

    [Fact]
    public async Task CloseWithCallback_ThenDisposeAsync_IsIdempotent()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await consumer.CloseWithCallback();
        await TestTimeout.DisposeAsync(consumer);
        TestTimeout.Dispose(consumer);
    }

    [Fact]
    public async Task CloseWithCallback_UseAfterClose_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await consumer.CloseWithCallback();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SubscribeWithCallback(ProofTopic()));
    }
}
