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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The <see cref="MockAdminClient"/> seeding members and its <see cref="MockAdminClient.Builder"/>'s feature levels:
/// each test asserts that the seed changes what a later RPC returns.
/// </summary>
public class PublicAdminMockSeedingTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const long Unseeded = -1;

    [Fact]
    public async Task TimeoutNextRequest_TimesOutExactlyThatManyOperations()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("seed-timeout-a", 1, 1) }).All().Get(), s_deadline);

        admin.TimeoutNextRequest(1);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                () => admin.CreateTopics(new[] { new NewTopic("seed-timeout-b", 1, 1) }).All().Get()),
            s_deadline);
        Assert.Equal("The mock timed out the request.", failure.Message);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("seed-timeout-c", 1, 1) }).All().Get(), s_deadline);
    }

    [Fact]
    public async Task UpdateBeginningOffsets_ChangesWhatListOffsetsEarliestReports()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition seeded = new TopicPartition("seed-begin", 0);
        TopicPartition unseeded = new TopicPartition("seed-begin", 1);

        Assert.Equal(Unseeded, await EarliestOffset(admin, seeded));

        admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [seeded] = 42 });

        Assert.Equal(42, await EarliestOffset(admin, seeded));
        Assert.Equal(Unseeded, await EarliestOffset(admin, unseeded));

        // The seeder merges rather than replaces.
        admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [unseeded] = 7 });
        Assert.Equal(42, await EarliestOffset(admin, seeded));
        Assert.Equal(7, await EarliestOffset(admin, unseeded));
    }

    [Fact]
    public async Task UpdateEndOffsets_ChangesWhatListOffsetsLatestReports()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition partition = new TopicPartition("seed-end", 0);

        Assert.Equal(Unseeded, await LatestOffset(admin, partition));

        admin.UpdateEndOffsets(new Dictionary<TopicPartition, long> { [partition] = 99 });

        Assert.Equal(99, await LatestOffset(admin, partition));

        Assert.Equal(Unseeded, await EarliestOffset(admin, partition));
    }

    [Fact]
    public async Task UpdateConsumerGroupOffsets_ChangesWhatListConsumerGroupOffsetsReports()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition partition = new TopicPartition("seed-committed", 3);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> before = await TestTimeout.Run(
            () => admin.ListConsumerGroupOffsets("g1").PartitionsToOffsetAndMetadata().Get(), s_deadline);
        Assert.Empty(before);

        admin.UpdateConsumerGroupOffsets(new Dictionary<TopicPartition, long> { [partition] = 17 });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> after = await TestTimeout.Run(
            () => admin.ListConsumerGroupOffsets("g1").PartitionsToOffsetAndMetadata().Get(), s_deadline);
        OffsetAndMetadata? committed = Assert.Contains(partition, after);
        Assert.NotNull(committed);
        Assert.Equal(17, committed!.Offset);
    }

    [Fact]
    public async Task BuilderFeatureLevels_ChangeWhatDescribeFeaturesReports()
    {
        AsyncMockAdminClient unseeded = AsyncMockAdminClient.Create().Build();
        using IDisposable unseededDisposal = TestTimeout.Disposal(unseeded);
        FeatureMetadata before = await TestTimeout.Run(
            () => unseeded.DescribeFeatures().FeatureMetadata().Get(), s_deadline);
        Assert.Empty(before.FinalizedFeatures);
        Assert.Empty(before.SupportedFeatures);

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create()
            .FeatureLevels(new Dictionary<string, short>(StringComparer.Ordinal) { ["metadata.version"] = 5 })
            .MinSupportedFeatureLevels(new Dictionary<string, short>(StringComparer.Ordinal) { ["metadata.version"] = 1 })
            .MaxSupportedFeatureLevels(new Dictionary<string, short>(StringComparer.Ordinal) { ["metadata.version"] = 9 })
            .Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        FeatureMetadata after = await TestTimeout.Run(
            () => admin.DescribeFeatures().FeatureMetadata().Get(), s_deadline);

        FinalizedVersionRange finalized = Assert.Contains("metadata.version", after.FinalizedFeatures);
        Assert.Equal(5, finalized.MinVersionLevel);
        Assert.Equal(5, finalized.MaxVersionLevel);

        SupportedVersionRange supported = Assert.Contains("metadata.version", after.SupportedFeatures);
        Assert.Equal(1, supported.MinVersion);
        Assert.Equal(9, supported.MaxVersion);
    }

    [Fact]
    public void SeedingRejectsANullMapBeforeAnyNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal("newOffsets", Assert.Throws<ArgumentNullException>(() => admin.UpdateBeginningOffsets(null!)).ParamName);
        Assert.Equal("newOffsets", Assert.Throws<ArgumentNullException>(() => admin.UpdateEndOffsets(null!)).ParamName);
        Assert.Equal("newOffsets", Assert.Throws<ArgumentNullException>(() => admin.UpdateConsumerGroupOffsets(null!)).ParamName);
        Assert.Throws<ArgumentNullException>(() => MockAdminClient.Create().FeatureLevels(null!));
    }

    [Fact]
    public void SeedingRejectsANullTopicBeforeAnyNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        Dictionary<TopicPartition, long> withNullTopic =
            new Dictionary<TopicPartition, long> { [default] = 1 };

        Assert.Throws<ArgumentException>(() => admin.UpdateBeginningOffsets(withNullTopic));
        Assert.Throws<ArgumentException>(() => admin.UpdateEndOffsets(withNullTopic));
        Assert.Throws<ArgumentException>(() => admin.UpdateConsumerGroupOffsets(withNullTopic));
    }

    [Fact]
    public async Task SeedingRejectsAConstructedNullTopic_AndSeedsNothing()
    {
        const string Message = "The offsets map must not contain a topic partition with a null topic.";
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition valid = new TopicPartition("seed-null-topic", 0);
        Dictionary<TopicPartition, long> withNullTopic = new Dictionary<TopicPartition, long>
        {
            [valid] = 42,
            [new TopicPartition(null!, 0)] = 1,
        };

        foreach (Action seed in new Action[]
        {
            () => admin.UpdateBeginningOffsets(withNullTopic),
            () => admin.UpdateEndOffsets(withNullTopic),
            () => admin.UpdateConsumerGroupOffsets(withNullTopic),
        })
        {
            ArgumentException rejected = Assert.Throws<ArgumentException>(seed);
            Assert.Equal("newOffsets", rejected.ParamName);
            Assert.StartsWith(Message, rejected.Message, StringComparison.Ordinal);
        }

        Assert.Equal(Unseeded, await EarliestOffset(admin, valid));
        Assert.Equal(Unseeded, await LatestOffset(admin, valid));
    }

    [Fact]
    public void SeedingAfterDisposeThrowsObjectDisposed()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.TimeoutNextRequest(1));
        Assert.Throws<ObjectDisposedException>(
            () => admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.UpdateEndOffsets(new Dictionary<TopicPartition, long>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.UpdateConsumerGroupOffsets(new Dictionary<TopicPartition, long>()));
    }

    private static async Task<long> EarliestOffset(IAsyncAdmin admin, TopicPartition partition) =>
        await Offset(admin, partition, OffsetSpec.Earliest());

    private static async Task<long> LatestOffset(IAsyncAdmin admin, TopicPartition partition) =>
        await Offset(admin, partition, OffsetSpec.Latest());

    private static async Task<long> Offset(IAsyncAdmin admin, TopicPartition partition, OffsetSpec spec)
    {
        ListOffsetsResult.ListOffsetsResultInfo info = await TestTimeout.Run(
            () => admin
                .ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [partition] = spec })
                .PartitionResult(partition).Get(),
            s_deadline);
        return info.Offset;
    }
}
