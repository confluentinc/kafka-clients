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

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>AlterConsumerGroupOffsets</c> against the mocks, which fail the call with the mock's own refusal, as Java's mock
/// does: every accessor of the sync and the async result carries that error, for a requested partition or not.
/// </summary>
public sealed class PublicAdminAlterConsumerGroupOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implement yet";

    [Fact]
    public async Task AlterConsumerGroupOffsets_SurfacesTheMocksDocumentedRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        TopicPartition tp = new TopicPartition("p5-acgo", 0);
        AsyncAlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(10) });

        NotSupportedException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.PartitionResult(tp).Get()), s_deadline);
        Assert.Equal(NotImplemented, fromPartitionResult.Message);

        NotSupportedException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    [Fact]
    public async Task AlterConsumerGroupOffsets_ThrowsAfterDispose()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await TestTimeout.DisposeAsync(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition("p5-disposed", 0)] = new OffsetAndMetadata(0),
            }));
    }

    [Fact]
    public async Task PartitionResult_OnAWholeCallFailure_EveryPartitionCarriesTheCallsError_RequestedOrNot()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        TopicPartition requested = new TopicPartition("p5-acgo", 0);
        AsyncAlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [requested] = new OffsetAndMetadata(10),
                [new TopicPartition("p5-acgo", 1)] = new OffsetAndMetadata(11, "m", 4),
            });

        foreach (TopicPartition partition in new[] { requested, new TopicPartition("p5-unknown", 7) })
        {
            NotSupportedException thrown = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => result.PartitionResult(partition).Get()), s_deadline);
            Assert.Equal(NotImplemented, thrown.Message);
        }
    }

    [Fact]
    public async Task All_AnEmptyRequest_WaitsForTheCall_AndCarriesTheCoresOutcome()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Task all = admin.AlterConsumerGroupOffsets("p5-group", new Dictionary<TopicPartition, OffsetAndMetadata>()).All().Get();

        Assert.NotEqual(TaskStatus.RanToCompletion, all.Status);
        NotSupportedException thrown = await TestTimeout.Run(() => Assert.ThrowsAsync<NotSupportedException>(() => all), s_deadline);
        Assert.Equal(NotImplemented, thrown.Message);
    }

    [Fact]
    public void TheSyncResult_CarriesTheMocksRefusal_FromEveryAccessor()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition tp = new TopicPartition("p5-acgo", 0);
        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "p5-group", new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(10) });
        AlterConsumerGroupOffsetsResult empty = admin.AlterConsumerGroupOffsets(
            "p5-group", new Dictionary<TopicPartition, OffsetAndMetadata>());

        foreach (Action get in new Action[] { () => result.All().Get(), () => result.PartitionResult(tp).Get(), () => empty.All().Get() })
        {
            NotSupportedException thrown = Assert.Throws<NotSupportedException>(get);
            Assert.Equal(NotImplemented, thrown.Message);
        }
    }
}
