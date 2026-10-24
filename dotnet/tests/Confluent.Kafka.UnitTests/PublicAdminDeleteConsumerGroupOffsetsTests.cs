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

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DeleteConsumerGroupOffsets</c> against the mocks, which fail the call with the mock's own refusal, as Java's mock
/// does: every accessor of the sync and the async result carries it, and an unrequested partition throws Java's
/// message.
/// </summary>
public sealed class PublicAdminDeleteConsumerGroupOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task DeleteConsumerGroupOffsets_SurfacesTheMocksDocumentedRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        TopicPartition tp = new TopicPartition("p5-dcgo", 0);
        AsyncDeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "p5-group",
            new[] { tp });

        KafkaException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(tp).Get()), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromPartitionResult.Code);
        Assert.Equal(NotImplemented, fromPartitionResult.Message);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    [Fact]
    public async Task DeleteConsumerGroupOffsets_ThrowsAfterDispose()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await TestTimeout.DisposeAsync(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.DeleteConsumerGroupOffsets(
            "p5-group",
            new[] { new TopicPartition("p5-disposed", 0) }));
    }

    [Fact]
    public async Task PartitionResult_UnknownPartition_ThrowsSynchronouslyWithJavasExactMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        AsyncDeleteConsumerGroupOffsetsResult result =
            admin.DeleteConsumerGroupOffsets("p5-group", new[] { new TopicPartition("p5-known", 0) });

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => result.PartitionResult(new TopicPartition("p5-unknown", 7)));
        Assert.StartsWith("Partition p5-unknown-7 was not included in the original request", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARepeatedPartition_IsOnePartition_AndEveryAccessorCarriesTheMocksRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        TopicPartition first = new TopicPartition("p5-dcgo", 0);
        TopicPartition second = new TopicPartition("p5-dcgo", 1);
        AsyncDeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "p5-group", new[] { first, second, new TopicPartition("p5-dcgo", 0) });

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);

        foreach (TopicPartition partition in new[] { first, second })
        {
            KafkaException thrown = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(partition).Get()), s_deadline);
            Assert.Equal(UnsupportedVersionCode, thrown.Code);
            Assert.Equal(NotImplemented, thrown.Message);
        }
    }

    [Fact]
    public async Task All_AnEmptyRequest_WaitsForTheCall_AndCarriesTheCoresOutcome()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Task all = admin.DeleteConsumerGroupOffsets("p5-group", Array.Empty<TopicPartition>()).All().Get();

        Assert.NotEqual(TaskStatus.RanToCompletion, all.Status);
        KafkaException thrown = await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => all), s_deadline);
        Assert.Equal(UnsupportedVersionCode, thrown.Code);
        Assert.Equal(NotImplemented, thrown.Message);
    }

    [Fact]
    public void TheSyncResult_ChecksAndCarriesTheMocksRefusal()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition tp = new TopicPartition("p5-dcgo", 0);
        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets("p5-group", new[] { tp });
        DeleteConsumerGroupOffsetsResult empty = admin.DeleteConsumerGroupOffsets("p5-group", Array.Empty<TopicPartition>());

        Assert.StartsWith(
            "Partition p5-unknown-7 was not included in the original request",
            Assert.Throws<ArgumentException>(() => result.PartitionResult(new TopicPartition("p5-unknown", 7))).Message,
            StringComparison.Ordinal);
        foreach (Action get in new Action[] { () => result.All().Get(), () => result.PartitionResult(tp).Get(), () => empty.All().Get() })
        {
            KafkaException thrown = Assert.Throws<KafkaException>(get);
            Assert.Equal(UnsupportedVersionCode, thrown.Code);
            Assert.Equal(NotImplemented, thrown.Message);
        }
    }
}
