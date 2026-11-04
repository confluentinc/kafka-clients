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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Which consumer members are sync and which async, the <c>Seek</c> negative-offset precondition,
/// <see cref="ConsumerGroupMetadata"/> on the real and mock consumers, and the group-id checks.
/// </summary>
public sealed class PublicConsumerApiTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string UnreachableBootstrap = "127.0.0.1:1";

    private const int InvalidGroupIdCode = 24;

    private const string GroupIdNotDefinedMessage =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private const string InvalidGroupInstanceIdMessage =
        "Group instance id is invalid: 'bad/id' contains one or more characters other than ASCII "
        + "alphanumerics, '.', '_' and '-'";

    private const int InvalidConfigCode = 40;

    private static string[] ProofTopic() => new[] { "proof-topic" };

    private static Dictionary<string, string> RealConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    [Fact]
    public async Task Subscribe_ReturnsTask_Completes()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task Unsubscribe_ReturnsTask_Completes()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await consumer.Subscribe(ProofTopic());
        await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);
    }

    [Fact]
    public async Task SubscribeUnsubscribe_Churned_NoLeakOrHang()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        for (int i = 0; i < 50; i++)
        {
            await consumer.Subscribe(ProofTopic());
            await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);
        }
    }

    [Fact]
    public async Task Seek_UnassignedPartition_ThrowsInvalidOperationException()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
        Assert.Equal("No current assignment for partition unassigned-0", ex.Message);
    }

    [Fact]
    public void Wakeup_IsSync_AndSafeWhenIdle()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Wakeup();
    }

    [Fact]
    public void GroupMetadata_IsSync_ReturnsSynchronously()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(RealConfig("sync-group"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerGroupMetadata meta = consumer.GroupMetadata();
        Assert.Equal("sync-group", meta.GroupId);
    }

    [Fact]
    public void Seek_NegativeOffset_ThrowsExactJavaMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => { _ = consumer.Seek(new TopicPartition("t", 0), offset: -1); });

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("seek offset must not be a negative number", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Seek_NegativeOffset_OnAClosedConsumer_ReportsTheClosedConsumerFirst()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        Assert.Throws<ObjectDisposedException>(() => { _ = consumer.Seek(new TopicPartition("t", 0), offset: -3); });
    }

    [Fact]
    public void GroupMetadata_RealConsumer_PreJoinDefaults()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(RealConfig("public-group"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("public-group", meta.GroupId);
        Assert.Equal(-1, meta.GenerationId);
        Assert.Equal(string.Empty, meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_NonAsciiGroupId_RoundTrips()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(RealConfig("café-Ω-日本語-😀"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal("café-Ω-日本語-😀", consumer.GroupMetadata().GroupId);
    }

    [Fact]
    public void GroupMetadata_MockConsumer_ReturnsMockSentinels()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
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
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GroupMetadata_RealConsumer_WithoutGroupId_ThrowsInvalidGroupId(bool sync)
    {
        IConsumerCommon consumer = CreateRealConsumer(sync, ConfigWithoutGroupId());
        try
        {
            KafkaException failure = Assert.Throws<KafkaException>(() => consumer.GroupMetadata());

            Assert.Equal(InvalidGroupIdCode, failure.Code);
            Assert.False(failure.IsRetriable);
            Assert.Equal(GroupIdNotDefinedMessage, failure.Message);
            Assert.Null(failure.InnerException);
        }
        finally
        {
            DisposeConsumer(consumer);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GroupMetadata_RealConsumer_WithoutGroupId_AfterDispose_ThrowsObjectDisposed(bool sync)
    {
        IConsumerCommon consumer = CreateRealConsumer(sync, ConfigWithoutGroupId());
        DisposeConsumer(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Fact]
    public void GroupMetadata_SyncRealConsumer_PreJoinDefaults()
    {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            RealConfig("sync-public-group"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("sync-public-group", meta.GroupId);
        Assert.Equal(-1, meta.GenerationId);
        Assert.Equal(string.Empty, meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GroupMetadata_MockConsumer_WithoutGroupId_ReturnsMockSentinels(bool sync)
    {
        IConsumerCommon consumer = sync
            ? new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray)
            : new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            ConsumerGroupMetadata meta = consumer.GroupMetadata();

            Assert.Equal("dummy.group.id", meta.GroupId);
            Assert.Equal(1, meta.GenerationId);
            Assert.Equal("1", meta.MemberId);
            Assert.Null(meta.GroupInstanceId);
        }
        finally
        {
            DisposeConsumer(consumer);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Ctor_InvalidGroupInstanceId_WithoutClientId_ThrowsInvalidConfig(bool sync)
    {
        Dictionary<string, string> config = ConfigWithoutGroupId();
        config["group.id"] = "clientid-invalid";
        config["group.instance.id"] = "bad/id";

        KafkaException ex = Assert.Throws<KafkaException>(() => DisposeConsumer(CreateRealConsumer(sync, config)));

        Assert.Equal(InvalidGroupInstanceIdMessage, ex.Message);
        Assert.Equal(InvalidConfigCode, ex.Code);
        Assert.Null(ex.InnerException);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Ctor_InvalidGroupInstanceId_WithExplicitClientId_ThrowsFailedToConstructWithTheCause(bool sync)
    {
        Dictionary<string, string> config = ConfigWithoutGroupId();
        config["group.id"] = "clientid-invalid";
        config["group.instance.id"] = "bad/id";
        config["client.id"] = "explicit-client";

        KafkaException ex = Assert.Throws<KafkaException>(() => DisposeConsumer(CreateRealConsumer(sync, config)));

        Assert.Equal("Failed to construct kafka consumer", ex.Message);
        KafkaException cause = Assert.IsType<KafkaException>(ex.InnerException);
        Assert.Equal(InvalidGroupInstanceIdMessage, cause.Message);
        Assert.Equal(InvalidConfigCode, cause.Code);
    }

    private static Dictionary<string, string> ConfigWithoutGroupId() => new()
    {
        ["bootstrap.servers"] = UnreachableBootstrap,
        ["group.protocol"] = "consumer",
    };

    private static IConsumerCommon CreateRealConsumer(bool sync, IReadOnlyDictionary<string, string> config) => sync
        ? new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)
        : new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);

    private static void DisposeConsumer(IConsumerCommon consumer) => ((IDisposable)consumer).Dispose();
}
