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
/// The end-to-end behaviour of M15/P5's RPC 4.6 (<c>alterConsumerGroupOffsets</c>) against
/// <see cref="MockAdminClient"/> — no broker.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The mock has no success path, and that is FAITHFUL, not a gap.</b> Java's
/// <c>MockAdminClient.alterConsumerGroupOffsets</c> throws
/// <c>UnsupportedOperationException("Not implement yet")</c> (Java's own typo —
/// <c>MockAdminClient.java:1213</c>), and the core fails the RPC's one future with it. The
/// ABI delivers that as the callback's whole-request error, so the single awaitable
/// <b>faults</b>, and both accessors rethrow the mock's error unchanged — Java's
/// <c>partitionResult</c> tests the <c>throwable</c> first
/// (<c>AlterConsumerGroupOffsetsResult.java:46-47</c>) and its <c>all()</c> is a
/// <c>thenApply</c> that the failure bypasses (<c>:68</c>).
/// </para>
/// <para>
/// The map-value semantics are additionally tested against the public type directly, with an
/// outcome the test supplies — the same accommodation
/// <c>PublicAdminElectionsReassignmentsTests.ElectLeadersResult_APerPartitionFailureIsAValue_AndAllReportsTheFirst</c>
/// makes for <see cref="ElectLeadersResult"/>, which shares this result's shape. The supplied
/// <c>All</c> stands for the core's <c>kafka_admin_AlterConsumerGroupOffsetsResult_all</c>,
/// which the binding reads rather than derives.
/// </para>
/// </remarks>
public sealed class PublicAdminAlterConsumerGroupOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws (with its own typo) and the
    /// Rust mock translates verbatim.
    /// </summary>
    private const string NotImplemented = "Not implement yet";

    /// <summary>
    /// <c>alterConsumerGroupOffsets</c> reaches the core, whose mock fails the whole request,
    /// and both accessors rethrow the mock's documented refusal <b>verbatim</b> — no
    /// "Failed altering group offsets for the following partitions" wording, which Java
    /// attaches only to a per-partition failure on a resolved map.
    /// </summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_SurfacesTheMocksDocumentedRefusal()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        TopicPartition tp = new TopicPartition("p5-acgo", 0);
        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(10) });

        KafkaException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(tp)), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromPartitionResult.Code);
        Assert.Equal(NotImplemented, fromPartitionResult.Message);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    /// <summary>A closed client rejects the call before reaching the core.</summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_ThrowsAfterDispose()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition("p5-disposed", 0)] = new OffsetAndMetadata(0),
            }));
    }

    /// <summary>
    /// ⚠⚠ <b>A per-partition failure is a map VALUE on a SUCCESSFUL task</b> — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c> (<c>AlterConsumerGroupOffsetsResult.java:33</c>)
    /// — and <see cref="AlterConsumerGroupOffsetsResult.PartitionResult"/> is what turns a
    /// non-null entry into a fault (<c>:53-56</c>).
    /// </summary>
    [Fact]
    public async Task PartitionResult_SucceedsOnNull_AndThrowsThePartitionsOwnError()
    {
        TopicPartition good = new TopicPartition("p5-partition", 0);
        TopicPartition bad = new TopicPartition("p5-partition", 1);

        KafkaException error = new KafkaException(11, "partition failure", isRetriable: false);
        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = error,
        };

        AlterConsumerGroupOffsetsResult result = Resolved(outcomes, all: null);

        await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// A partition absent from a <b>resolved</b> map — including one never named in the
    /// request — faults on the returned task with Java's exact
    /// <c>IllegalArgumentException</c> message, translated to <see cref="ArgumentException"/>
    /// (<c>AlterConsumerGroupOffsetsResult.java:48-50</c>). The success half of G5-4; the
    /// failure half is <see cref="AFaultedFuture_PropagatesFromBothAccessors"/>.
    /// </summary>
    [Fact]
    public async Task PartitionResult_UnknownPartition_ThrowsWithJavasExactMessage()
    {
        TopicPartition known = new TopicPartition("p5-known", 0);
        TopicPartition unknown = new TopicPartition("p5-unknown", 7);

        AlterConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?> { [known] = null }, all: null);

        ArgumentException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<ArgumentException>(() => result.PartitionResult(unknown)), s_deadline);
        Assert.Equal(
            "Alter offset for partition \"" + unknown + "\" was not attempted",
            thrown.Message);
    }

    /// <summary>
    /// ⚠ <see cref="AlterConsumerGroupOffsetsResult.All"/> rethrows the <b>stored</b>
    /// outcome — the same instance, unchanged — rather than building its own aggregate from
    /// the map. Java's aggregate message (<c>AlterConsumerGroupOffsetsResult.java:68-81</c>)
    /// is the core's to compose now; the binding only carries it.
    /// </summary>
    [Fact]
    public async Task All_RethrowsTheStoredOutcomeUnchanged()
    {
        TopicPartition bad = new TopicPartition("p5-all", 1);
        TopicPartition worse = new TopicPartition("p5-all", 2);

        KafkaException stored = new KafkaException(
            11,
            "Failed altering group offsets for the following partitions: [p5-all-1, p5-all-2]",
            isRetriable: true);

        AlterConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?>
            {
                [bad] = new KafkaException(11, "first failure", isRetriable: true),
                [worse] = new KafkaException(12, "second failure", isRetriable: false),
            },
            stored);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(stored, thrown);
    }

    /// <summary>
    /// ⚠ The control for <see cref="All_RethrowsTheStoredOutcomeUnchanged"/>: a <b>null</b>
    /// stored outcome completes <see cref="AlterConsumerGroupOffsetsResult.All"/> even though
    /// the map carries a failure. Only an implementation that reads the stored outcome — and
    /// derives nothing from the map — passes both; a derivation over the map faults here.
    /// </summary>
    [Fact]
    public async Task All_CompletesOnANullStoredOutcome_WhateverTheMapHolds()
    {
        TopicPartition bad = new TopicPartition("p5-all-null", 0);
        KafkaException perPartition = new KafkaException(11, "partition failure", isRetriable: false);

        AlterConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?> { [bad] = perPartition }, all: null);

        await TestTimeout.Run(result.All, s_deadline);

        // The map value is still what PartitionResult reports — the two accessors read
        // different halves of one outcome.
        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        Assert.Same(perPartition, thrown);
    }

    /// <summary>
    /// A whole-request failure faults the single awaitable, and both accessors rethrow it
    /// unchanged — <see cref="AlterConsumerGroupOffsetsResult.PartitionResult"/> even for a
    /// partition that was never requested, because Java tests the <c>throwable</c> before it
    /// looks at the map (<c>AlterConsumerGroupOffsetsResult.java:46-47</c>). The failure
    /// half of G5-4.
    /// </summary>
    [Fact]
    public async Task AFaultedFuture_PropagatesFromBothAccessors()
    {
        KafkaException callLevel = new KafkaException(35, "call failed", isRetriable: false);
        TaskCompletionSource<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> source =
            new TaskCompletionSource<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>();
        source.SetException(callLevel);

        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(source.Task);

        KafkaException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                () => result.PartitionResult(new TopicPartition("p5-fault", 0))),
            s_deadline);
        Assert.Same(callLevel, fromPartitionResult);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(callLevel, fromAll);
    }

    /// <summary>A result over an already-resolved outcome, the shape the trampoline builds.</summary>
    private static AlterConsumerGroupOffsetsResult Resolved(
        IReadOnlyDictionary<TopicPartition, KafkaException?> perKey,
        KafkaException? all) =>
        new AlterConsumerGroupOffsetsResult(
            Task.FromResult<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
                (perKey, all)));
}
