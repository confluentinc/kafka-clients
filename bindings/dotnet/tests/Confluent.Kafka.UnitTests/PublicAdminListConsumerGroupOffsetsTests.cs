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
/// <c>listConsumerGroupOffsets</c> end-to-end through <see cref="MockAdminClient"/> — the
/// public half of M15/P5's fifth RPC.
/// </summary>
/// <remarks>
/// <para>
/// Unlike the four RPCs before it, the <b>single-group</b> path here really does succeed:
/// the Rust <c>MockAdminClient::list_consumer_group_offsets</c> filters its in-memory
/// committed offsets, so an unseeded mock answers with an <b>empty</b> map rather than a
/// refusal. Only <c>group_specs.len() != 1</c> is refused with
/// <c>unsupported_version("Not implemented yet")</c>, faithfully translating Java's
/// <c>MockAdminClient.java:750</c> (<c>admin-client.md</c> §9). Both halves are exercised
/// below.
/// </para>
/// <para>
/// <b>What this file therefore does NOT cover, and who does.</b> A non-empty offset map,
/// the committed-vs-uncommitted <see langword="null"/> value split, and — crucially — the
/// observable difference between a <see langword="null"/> and an <b>empty</b>
/// <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions"/> need seeded offsets the mock
/// does not expose. Those are driven over production's own walker and read at the P/Invoke
/// seam in the <c>Interop/AdminP5*</c> files, where the <c>allPartitions</c> flag and the
/// per-group partition counts are visible. This file proves both spec shapes are accepted
/// and reach the core, not what the core then does with them.
/// </para>
/// <para>
/// ⚠ The result type's own fan-out, its accessors' argument validation at rest and the
/// value types' behaviour belong to <c>PublicAdminListConsumerGroupOffsetsResultTests</c>
/// and the <see cref="OffsetAndMetadata"/> tests; nothing here re-asserts those beyond the
/// reachability each accessor needs.
/// </para>
/// </remarks>
public sealed class PublicAdminListConsumerGroupOffsetsTests
{
    /// <summary>The per-await ceiling; a hang must fail the test, not the run.</summary>
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The Rust mock's refusal code — <c>UNSUPPORTED_VERSION</c>, what
    /// <c>Error::unsupported_version</c> carries.
    /// </summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws and the Rust mock translates.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// The single-group convenience form reaches the mock's implemented path: it keys the
    /// result by the requested id and completes with that group's committed offsets — an
    /// empty map on an unseeded mock, not a refusal.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_SingleGroupCompletesWithItsOffsets()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets("p5-lcgo-one");

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            await TestTimeout.Run(result.PartitionsToOffsetAndMetadata, s_deadline);

        Assert.Empty(offsets);

        // The per-key accessor names the same future; the no-arg accessor above is only
        // legal because the request carried exactly one group.
        Assert.Same(
            result.PartitionsToOffsetAndMetadata(),
            result.PartitionsToOffsetAndMetadata("p5-lcgo-one"));

        ArgumentException unknown = Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionsToOffsetAndMetadata("p5-lcgo-absent"); });
        Assert.Equal("groupId", unknown.ParamName);
    }

    /// <summary>
    /// The batched form fans out one future per group id, keyed by that id, each carrying
    /// its own outcome — and with more than one group the mock refuses every key, which is
    /// its contract rather than a defect.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_FansOutOneFuturePerGroupId()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-a"] = new ListConsumerGroupOffsetsSpec(),
                ["p5-lcgo-b"] = new ListConsumerGroupOffsetsSpec(),
                ["p5-lcgo-c"] = new ListConsumerGroupOffsetsSpec(),
            });

        Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>[] futures =
            new[] { "p5-lcgo-a", "p5-lcgo-b", "p5-lcgo-c" }
                .Select(result.PartitionsToOffsetAndMetadata)
                .ToArray();

        // Distinct Task instances: a single shared future would satisfy a key-set
        // assertion while losing the per-group granularity the Java shape promises.
        Assert.Equal(3, futures.Distinct().Count());

        foreach (Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future in futures)
        {
            KafkaException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => future), s_deadline);

            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }

        // Java's guard is `futures.size() != 1`, so the no-arg accessor is illegal here.
        Assert.Throws<InvalidOperationException>(
            () => { _ = result.PartitionsToOffsetAndMetadata(); });
    }

    /// <summary>
    /// <c>all()</c> faults when any key does — Java's
    /// <c>ListConsumerGroupOffsetsResult.all()</c> is <c>KafkaFuture.allOf</c> over the
    /// per-group futures, not an independent request.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_AllFaultsWhenAKeyFails()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-all-a"] = new ListConsumerGroupOffsetsSpec(),
                ["p5-lcgo-all-b"] = new ListConsumerGroupOffsetsSpec(),
            });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// An empty spec map is a well-formed request for nothing: <c>all()</c> completes with
    /// an empty map rather than faulting or hanging, and the no-arg accessor is illegal
    /// because Java's guard rejects a size of zero as well as of many.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_AnEmptyRequestCompletesEmpty()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal));

        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> all =
            await TestTimeout.Run(result.All, s_deadline);

        Assert.Empty(all);

        Assert.Throws<InvalidOperationException>(
            () => { _ = result.PartitionsToOffsetAndMetadata(); });
    }

    /// <summary>
    /// A <see langword="null"/> selection ("every committed partition") and an <b>empty</b>
    /// one ("nothing") are both accepted and both reach the core. They are distinct
    /// requests — the bridge maps null to <c>allPartitions = true</c> and empty to
    /// <c>false</c> with a count of zero — but an unseeded mock answers both with an empty
    /// map, so the difference itself is asserted at the seam, not here.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_AcceptsNullAndEmptyPartitionSelections()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        // The convenience form's own spec: TopicPartitions left null.
        Assert.Null(new ListConsumerGroupOffsetsSpec().TopicPartitions);

        foreach (IReadOnlyCollection<TopicPartition>? selection in
            new IReadOnlyCollection<TopicPartition>?[] { null, Array.Empty<TopicPartition>() })
        {
            ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                {
                    ["p5-lcgo-sel"] = new ListConsumerGroupOffsetsSpec
                    {
                        TopicPartitions = selection,
                    },
                });

            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
                await TestTimeout.Run(result.PartitionsToOffsetAndMetadata, s_deadline);

            Assert.Empty(offsets);
        }

        // A selection naming real partitions is accepted too, and is not refused as a
        // malformed argument on the way down.
        ListConsumerGroupOffsetsResult selected = admin.ListConsumerGroupOffsets(
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
            await TestTimeout.Run(selected.PartitionsToOffsetAndMetadata, s_deadline));
    }

    /// <summary>
    /// Both forms are reachable through <see cref="IAdmin"/>, not only through the concrete
    /// mock — the interface is the shape Java's <c>Admin</c> declares.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_IsReachableThroughTheInterface()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        IAdmin api = admin;

        ListConsumerGroupOffsetsResult single = api.ListConsumerGroupOffsets("p5-lcgo-iface");
        Assert.Empty(
            await TestTimeout.Run(single.PartitionsToOffsetAndMetadata, s_deadline));

        ListConsumerGroupOffsetsResult batched = api.ListConsumerGroupOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["p5-lcgo-iface-2"] = new ListConsumerGroupOffsetsSpec(),
            });
        Assert.Empty(
            await TestTimeout.Run(batched.PartitionsToOffsetAndMetadata, s_deadline));
    }

    /// <summary>
    /// A malformed request is refused synchronously, as an argument fault — not deferred
    /// into a faulted future a caller reading the per-key accessors would never see.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_RejectsAMalformedRequest()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

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

        // A `default(TopicPartition)` has a null Topic, which the ABI would read as an
        // absent name rather than a request.
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

    /// <summary>
    /// A negative timeout is refused rather than silently substituting the client default,
    /// and the message names the property so the caller can find it — through the
    /// single-group convenience form as well as the batched one, since the convenience form
    /// must not swallow the options on its way through.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_RejectsANegativeTimeout()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ArgumentOutOfRangeException single = Assert.Throws<ArgumentOutOfRangeException>(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    "p5-lcgo-timeout",
                    new ListConsumerGroupOffsetsOptions { TimeoutMs = -1 });
            });

        Assert.Equal("options", single.ParamName);
        Assert.Contains(
            "ListConsumerGroupOffsetsOptions.TimeoutMs",
            single.Message,
            StringComparison.Ordinal);

        ArgumentOutOfRangeException batched = Assert.Throws<ArgumentOutOfRangeException>(
            () =>
            {
                admin.ListConsumerGroupOffsets(
                    new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                    {
                        ["p5-lcgo-timeout-2"] = new ListConsumerGroupOffsetsSpec(),
                    },
                    new ListConsumerGroupOffsetsOptions { TimeoutMs = -1 });
            });

        Assert.Equal("options", batched.ParamName);
    }

    /// <summary>
    /// After the client is closed both forms throw <see cref="ObjectDisposedException"/>
    /// rather than reaching a destroyed native handle.
    /// </summary>
    [Fact]
    public async Task ListConsumerGroupOffsets_AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

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
