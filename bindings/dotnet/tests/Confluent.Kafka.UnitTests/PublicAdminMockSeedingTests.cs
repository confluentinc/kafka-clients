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
/// M15/P12's five <see cref="MockAdminClient"/> seeding members. Each test asserts the
/// seed <b>changes what a subsequent <see cref="IAdmin"/> RPC returns</b> — a binding that
/// P/Invokes but whose effect is unobserved is not tested.
/// </summary>
public class PublicAdminMockSeedingTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The offset the mock reports for a partition it has no seed for.</summary>
    private const long Unseeded = -1;

    [Fact]
    public async Task TimeoutNextRequest_TimesOutExactlyThatManyOperations()
    {
        using MockAdminClient admin = new MockAdminClient();

        // Baseline: the same call succeeds when nothing is seeded.
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("seed-timeout-a", 1, 1) }).All(), s_deadline);

        admin.TimeoutNextRequest(1);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                () => admin.CreateTopics(new[] { new NewTopic("seed-timeout-b", 1, 1) }).All()),
            s_deadline);
        Assert.Equal("The mock timed out the request.", failure.Message);

        // The counter is consumed, so the next call succeeds again.
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("seed-timeout-c", 1, 1) }).All(), s_deadline);
    }

    [Fact]
    public async Task UpdateBeginningOffsets_ChangesWhatListOffsetsEarliestReports()
    {
        using MockAdminClient admin = new MockAdminClient();
        TopicPartition seeded = new TopicPartition("seed-begin", 0);
        TopicPartition unseeded = new TopicPartition("seed-begin", 1);

        Assert.Equal(Unseeded, await EarliestOffset(admin, seeded));

        admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [seeded] = 42 });

        Assert.Equal(42, await EarliestOffset(admin, seeded));
        Assert.Equal(Unseeded, await EarliestOffset(admin, unseeded));

        // Merges rather than replaces (the header's contract for this setter).
        admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [unseeded] = 7 });
        Assert.Equal(42, await EarliestOffset(admin, seeded));
        Assert.Equal(7, await EarliestOffset(admin, unseeded));
    }

    [Fact]
    public async Task UpdateEndOffsets_ChangesWhatListOffsetsLatestReports()
    {
        using MockAdminClient admin = new MockAdminClient();
        TopicPartition partition = new TopicPartition("seed-end", 0);

        Assert.Equal(Unseeded, await LatestOffset(admin, partition));

        admin.UpdateEndOffsets(new Dictionary<TopicPartition, long> { [partition] = 99 });

        Assert.Equal(99, await LatestOffset(admin, partition));

        // End offsets are a separate map from the beginning offsets.
        Assert.Equal(Unseeded, await EarliestOffset(admin, partition));
    }

    [Fact]
    public async Task UpdateConsumerGroupOffsets_ChangesWhatListConsumerGroupOffsetsReports()
    {
        using MockAdminClient admin = new MockAdminClient();
        TopicPartition partition = new TopicPartition("seed-committed", 3);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> before = await TestTimeout.Run(
            () => admin.ListConsumerGroupOffsets("g1").PartitionsToOffsetAndMetadata(), s_deadline);
        Assert.Empty(before);

        admin.UpdateConsumerGroupOffsets(new Dictionary<TopicPartition, long> { [partition] = 17 });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> after = await TestTimeout.Run(
            () => admin.ListConsumerGroupOffsets("g1").PartitionsToOffsetAndMetadata(), s_deadline);
        OffsetAndMetadata? committed = Assert.Contains(partition, after);
        Assert.NotNull(committed);
        Assert.Equal(17, committed!.Offset);
    }

    [Fact]
    public async Task SetFeatureLevels_ChangesWhatDescribeFeaturesReports()
    {
        using MockAdminClient admin = new MockAdminClient();

        FeatureMetadata before = await TestTimeout.Run(
            () => admin.DescribeFeatures().FeatureMetadata(), s_deadline);
        Assert.Empty(before.FinalizedFeatures);
        Assert.Empty(before.SupportedFeatures);

        admin.SetFeatureLevels(
            new Dictionary<string, (short Level, short MinSupported, short MaxSupported)>(StringComparer.Ordinal)
            {
                ["metadata.version"] = (5, 1, 9),
            });

        FeatureMetadata after = await TestTimeout.Run(
            () => admin.DescribeFeatures().FeatureMetadata(), s_deadline);

        // All three levels reach the core — a single-level binding would lose min/max.
        FinalizedVersionRange finalized = Assert.Contains("metadata.version", after.FinalizedFeatures);
        Assert.Equal(5, finalized.MinVersionLevel);
        Assert.Equal(5, finalized.MaxVersionLevel);

        SupportedVersionRange supported = Assert.Contains("metadata.version", after.SupportedFeatures);
        Assert.Equal(1, supported.MinVersion);
        Assert.Equal(9, supported.MaxVersion);

        // Replaces rather than merges (the header's contract for this setter).
        admin.SetFeatureLevels(
            new Dictionary<string, (short Level, short MinSupported, short MaxSupported)>(StringComparer.Ordinal)
            {
                ["group.version"] = (2, 0, 3),
            });

        FeatureMetadata replaced = await TestTimeout.Run(
            () => admin.DescribeFeatures().FeatureMetadata(), s_deadline);
        Assert.DoesNotContain("metadata.version", replaced.FinalizedFeatures);
        Assert.Contains("group.version", replaced.FinalizedFeatures);
    }

    [Fact]
    public void SeedingRejectsANullMapBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentNullException>(() => admin.UpdateBeginningOffsets(null!));
        Assert.Throws<ArgumentNullException>(() => admin.UpdateEndOffsets(null!));
        Assert.Throws<ArgumentNullException>(() => admin.UpdateConsumerGroupOffsets(null!));
        Assert.Throws<ArgumentNullException>(() => admin.SetFeatureLevels(null!));
    }

    /// <summary>
    /// A null topic is SKIPPED by the core, which would silently drop the entry, so the
    /// binding rejects it first (ffi §B5).
    /// </summary>
    [Fact]
    public void SeedingRejectsANullTopicBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient();
        Dictionary<TopicPartition, long> withNullTopic =
            new Dictionary<TopicPartition, long> { [default] = 1 };

        Assert.Throws<ArgumentException>(() => admin.UpdateBeginningOffsets(withNullTopic));
        Assert.Throws<ArgumentException>(() => admin.UpdateEndOffsets(withNullTopic));
        Assert.Throws<ArgumentException>(() => admin.UpdateConsumerGroupOffsets(withNullTopic));
    }

    /// <summary>
    /// The same guard, reached through a <c>new TopicPartition(null!, 0)</c> — constructible
    /// since M15/P13.2 G3-4, as in Java — with the exact message and parameter name. The seed
    /// is rejected as a whole: a valid entry enumerated before the null one is not applied
    /// either, which is the witness that nothing reached the core.
    /// </summary>
    [Fact]
    public async Task SeedingRejectsAConstructedNullTopic_AndSeedsNothing()
    {
        const string Message = "The offsets map must not contain a topic partition with a null topic.";
        using MockAdminClient admin = new MockAdminClient();
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
            Assert.Equal("offsets", rejected.ParamName);
            Assert.StartsWith(Message, rejected.Message, StringComparison.Ordinal);
        }

        Assert.Equal(Unseeded, await EarliestOffset(admin, valid));
        Assert.Equal(Unseeded, await LatestOffset(admin, valid));
    }

    [Fact]
    public void SeedingAfterDisposeThrowsObjectDisposed()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.TimeoutNextRequest(1));
        Assert.Throws<ObjectDisposedException>(
            () => admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.UpdateEndOffsets(new Dictionary<TopicPartition, long>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.UpdateConsumerGroupOffsets(new Dictionary<TopicPartition, long>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.SetFeatureLevels(
                new Dictionary<string, (short Level, short MinSupported, short MaxSupported)>(StringComparer.Ordinal)));
    }

    private static async Task<long> EarliestOffset(IAdmin admin, TopicPartition partition) =>
        await Offset(admin, partition, OffsetSpec.Earliest());

    private static async Task<long> LatestOffset(IAdmin admin, TopicPartition partition) =>
        await Offset(admin, partition, OffsetSpec.Latest());

    private static async Task<long> Offset(IAdmin admin, TopicPartition partition, OffsetSpec spec)
    {
        ListOffsetsResult.ListOffsetsResultInfo info = await TestTimeout.Run(
            () => admin
                .ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [partition] = spec })
                .PartitionResult(partition),
            s_deadline);
        return info.Offset;
    }
}
