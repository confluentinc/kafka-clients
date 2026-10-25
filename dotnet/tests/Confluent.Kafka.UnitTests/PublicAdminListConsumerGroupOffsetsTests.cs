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

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>ListConsumerGroupOffsets</c> end to end against the mocks: one group completes with its offsets (none for an
/// unseeded mock), and more than one is refused with the mock's own answer, as Java's mock does; with the partition
/// selections, the options and the preconditions.
/// </summary>
public sealed class PublicAdminListConsumerGroupOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task ListConsumerGroupOffsets_SingleGroupCompletesWithItsOffsets()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets("p5-lcgo-one");

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            await TestTimeout.Run(() => result.PartitionsToOffsetAndMetadata().Get(), s_deadline);

        Assert.Empty(offsets);

        Assert.Same(
            result.PartitionsToOffsetAndMetadata().Get(),
            result.PartitionsToOffsetAndMetadata("p5-lcgo-one").Get());

        ArgumentException unknown = Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionsToOffsetAndMetadata("p5-lcgo-absent"); });
        Assert.Equal("groupId", unknown.ParamName);
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_FansOutOneFuturePerGroupId()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-a"] = new ListConsumerGroupOffsetsSpec(),
                ["p5-lcgo-b"] = new ListConsumerGroupOffsetsSpec(),
                ["p5-lcgo-c"] = new ListConsumerGroupOffsetsSpec(),
            });

        Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>[] futures =
            new[] { "p5-lcgo-a", "p5-lcgo-b", "p5-lcgo-c" }
                .Select(groupId => result.PartitionsToOffsetAndMetadata(groupId).Get())
                .ToArray();

        Assert.Equal(3, futures.Distinct().Count());

        foreach (Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future in futures)
        {
            NotSupportedException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => future), s_deadline);

            Assert.Equal(NotImplemented, failure.Message);
        }

        // The no-argument accessor needs exactly one group, as Java's guard does.
        Assert.Throws<InvalidOperationException>(
            () => { _ = result.PartitionsToOffsetAndMetadata(); });
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_AllFaultsWhenAKeyFails()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-all-a"] = new ListConsumerGroupOffsetsSpec(),
                ["p5-lcgo-all-b"] = new ListConsumerGroupOffsetsSpec(),
            });

        NotSupportedException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);

        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_AnEmptyRequestCompletesEmpty()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal));

        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> all =
            await TestTimeout.Run(() => result.All().Get(), s_deadline);

        Assert.Empty(all);

        Assert.Throws<InvalidOperationException>(
            () => { _ = result.PartitionsToOffsetAndMetadata(); });
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_AcceptsNullAndEmptyPartitionSelections()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Assert.Null(new ListConsumerGroupOffsetsSpec().TopicPartitions);

        foreach (IReadOnlyCollection<TopicPartition>? selection in
            new IReadOnlyCollection<TopicPartition>?[] { null, Array.Empty<TopicPartition>() })
        {
            AsyncListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                {
                    ["p5-lcgo-sel"] = new ListConsumerGroupOffsetsSpec
                    {
                        TopicPartitions = selection,
                    },
                });

            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
                await TestTimeout.Run(() => result.PartitionsToOffsetAndMetadata().Get(), s_deadline);

            Assert.Empty(offsets);
        }

        AsyncListConsumerGroupOffsetsResult selected = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-sel-2"] = new ListConsumerGroupOffsetsSpec
                {
                    TopicPartitions = new[]
                    {
                        new TopicPartition("p5-lcgo-topic", 0),
                        new TopicPartition("p5-lcgo-topic", 1),
                    },
                },
            });

        Assert.Empty(
            await TestTimeout.Run(() => selected.PartitionsToOffsetAndMetadata().Get(), s_deadline));
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_IsReachableThroughTheInterface()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        IAsyncAdmin api = admin;

        AsyncListConsumerGroupOffsetsResult single = api.ListConsumerGroupOffsets("p5-lcgo-iface");
        Assert.Empty(
            await TestTimeout.Run(() => single.PartitionsToOffsetAndMetadata().Get(), s_deadline));

        AsyncListConsumerGroupOffsetsResult batched = api.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-iface-2"] = new ListConsumerGroupOffsetsSpec(),
            });
        Assert.Empty(
            await TestTimeout.Run(() => batched.PartitionsToOffsetAndMetadata().Get(), s_deadline));
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_RejectsAMalformedRequest()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        ArgumentNullException missingId = Assert.Throws<ArgumentNullException>(
            () => { admin.ListConsumerGroupOffsets((string)null!); });
        Assert.Equal("groupId", missingId.ParamName);

        ArgumentNullException missingSpecs = Assert.Throws<ArgumentNullException>(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    (IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec>)null!);
            });
        Assert.Equal("groupSpecs", missingSpecs.ParamName);

        ArgumentException nullSpec = Assert.Throws<ArgumentException>(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                    {
                        ["p5-lcgo-bad"] = null!,
                    });
            });
        Assert.Equal("groupSpecs", nullSpec.ParamName);
        Assert.Contains(
            "The spec for group id 'p5-lcgo-bad' must not be null.",
            nullSpec.Message,
            StringComparison.Ordinal);

        // A default(TopicPartition) has a null Topic, which the ABI would read as an absent name.
        ArgumentException nullTopic = Assert.Throws<ArgumentException>(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                    {
                        ["p5-lcgo-bad-tp"] = new ListConsumerGroupOffsetsSpec
                        {
                            TopicPartitions = new[] { default(TopicPartition) },
                        },
                    });
            });
        Assert.Equal("groupSpecs", nullTopic.ParamName);
        Assert.Contains(
            "must not select a topic partition with a null topic",
            nullTopic.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Assert.Null(Record.Exception(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    "p5-lcgo-timeout",
                    new ListConsumerGroupOffsetsOptions { TimeoutMs = -1 });
            }));
        Assert.Null(Record.Exception(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                    {
                        ["p5-lcgo-timeout-2"] = new ListConsumerGroupOffsetsSpec(),
                    },
                    new ListConsumerGroupOffsetsOptions { TimeoutMs = -1 });
            }));
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_TheSingleGroupForm_ForwardsItsOptions()
    {
        AsyncKafkaAdminClient real = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions { TimeoutMs = -1 };

        KafkaException batched = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                () => real.ListConsumerGroupOffsets(
                    new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                    {
                        ["p5-lcgo-forward"] = new ListConsumerGroupOffsetsSpec(),
                    },
                    options).All().Get()),
            s_deadline);
        KafkaException single = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                () => real.ListConsumerGroupOffsets("p5-lcgo-forward", options).All().Get()),
            s_deadline);

        Assert.Equal(7, batched.Code);
        Assert.Equal(batched.Code, single.Code);
        Assert.Equal(batched.Message, single.Message);
    }

    [Fact]
    public async Task ListConsumerGroupOffsets_AfterDispose_Throws()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await TestTimeout.DisposeAsync(admin);

        Assert.Throws<ObjectDisposedException>(
            () => { admin.ListConsumerGroupOffsets("p5-lcgo-disposed"); });

        Assert.Throws<ObjectDisposedException>(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                    {
                        ["p5-lcgo-disposed-2"] = new ListConsumerGroupOffsetsSpec(),
                    });
            });
    }
}
