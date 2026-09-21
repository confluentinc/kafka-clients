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
/// The end-to-end behaviour of M15/P5's RPC 4.7 (<c>deleteConsumerGroupOffsets</c>) against
/// <see cref="MockAdminClient"/> — no broker.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The mock has no success path, and that is FAITHFUL, not a gap.</b> Java's
/// <c>MockAdminClient.deleteConsumerGroupOffsets</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c> (correctly spelled — unlike
/// <c>alterConsumerGroupOffsets</c>'s "Not implement yet" typo,
/// <c>MockAdminClient.java:783-785</c>), and the core surfaces that as a resolved map whose
/// <b>every requested partition</b> carries the identical "unsupported" error, the same
/// shape as <see cref="AlterConsumerGroupOffsetsResult"/>.
/// </para>
/// <para>
/// ⚠ Unlike <see cref="AlterConsumerGroupOffsetsResult.All"/>, which aggregates every
/// failure into one message, <see cref="DeleteConsumerGroupOffsetsResult.All"/> reports only
/// the <b>first</b> failing partition (by topic-then-partition order) and throws that
/// partition's own error verbatim — no synthesized aggregate message.
/// </para>
/// </remarks>
public sealed class PublicAdminDeleteConsumerGroupOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws (correctly spelled here) and the
    /// Rust mock translates verbatim.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// <c>deleteConsumerGroupOffsets</c> reaches the core and its single awaitable resolves
    /// to a map where the requested partition carries the mock's documented refusal —
    /// verbatim from both
    /// <see cref="DeleteConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/> and
    /// <see cref="DeleteConsumerGroupOffsetsResult.All"/> (a single requested partition, so
    /// "first failure" and "the failure" coincide here).
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroupOffsets_SurfacesTheMocksDocumentedRefusal()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        TopicPartition tp = new TopicPartition("p5-dcgo", 0);
        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
            "p5-group",
            new[] { tp });

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
    public async Task DeleteConsumerGroupOffsets_ThrowsAfterDispose()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => admin.DeleteConsumerGroupOffsets(
            "p5-group",
            new[] { new TopicPartition("p5-disposed", 0) }));
    }

    /// <summary>
    /// ⚠⚠ <b>A per-partition failure is a map VALUE on a SUCCESSFUL task</b> — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c>
    /// (<c>DeleteConsumerGroupOffsetsResult.java:33</c>) — and
    /// <see cref="DeleteConsumerGroupOffsetsResult.PartitionResult"/> /
    /// <see cref="DeleteConsumerGroupOffsetsResult.All"/> are what turn a non-null entry into
    /// a fault.
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

        DeleteConsumerGroupOffsetsResult result = new DeleteConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(outcomes),
            new[] { good, bad });

        await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// A partition never named in the request faults <b>synchronously</b> with Java's exact
    /// <c>IllegalArgumentException</c> message (no quotes around the partition) — the
    /// precondition check that runs before the future is even consulted
    /// (<c>DeleteConsumerGroupOffsetsResult.java:47-48</c>).
    /// </summary>
    [Fact]
    public void PartitionResult_UnknownPartition_ThrowsSynchronouslyWithJavasExactMessage()
    {
        TopicPartition known = new TopicPartition("p5-known", 0);
        TopicPartition unknown = new TopicPartition("p5-unknown", 7);

        DeleteConsumerGroupOffsetsResult result = new DeleteConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?> { [known] = null }),
            new[] { known });

        ArgumentException? thrown = null;
        try
        {
            result.PartitionResult(unknown);
        }
        catch (ArgumentException ex)
        {
            thrown = ex;
        }

        Assert.NotNull(thrown);
        Assert.Equal(
            "Partition " + unknown + " was not included in the original request",
            thrown!.Message);
    }

    /// <summary>
    /// A partition that WAS in the original request but is missing from the resolved map
    /// faults <b>asynchronously</b> (via the returned <see cref="Task"/>) with a distinct
    /// message — Java's <c>KafkaAdminClient.getSubLevelError</c>
    /// (<c>KafkaAdminClient.java:5197-5203</c>).
    /// </summary>
    [Fact]
    public async Task PartitionResult_PartitionNotInResponse_ThrowsAsynchronouslyWithDistinctMessage()
    {
        TopicPartition requested = new TopicPartition("p5-missing", 0);

        DeleteConsumerGroupOffsetsResult result = new DeleteConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?>()),
            new[] { requested });

        // Captured BEFORE the assertion: a synchronous throw here would surface at this line,
        // outside Assert.ThrowsAsync's catch — which is what proves the fault is genuinely
        // asynchronous rather than merely tolerated by an assertion helper that accepts either.
        Task task = result.PartitionResult(requested);

        ArgumentException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<ArgumentException>(() => task), s_deadline);
        Assert.Equal(
            "Offset deletion result for partition \"" + requested + "\" was not included in the response",
            thrown.Message);
    }

    /// <summary>
    /// <see cref="DeleteConsumerGroupOffsetsResult.All"/> reports only the <b>first</b>
    /// failing partition (by topic-then-partition order), throwing that partition's own error
    /// verbatim — unlike <see cref="AlterConsumerGroupOffsetsResult.All"/>, which aggregates
    /// every failure into one message.
    /// </summary>
    [Fact]
    public async Task All_ReportsOnlyTheFirstFailingPartition_SortedByTopicThenPartition()
    {
        TopicPartition good = new TopicPartition("p5-all", 0);
        TopicPartition bad = new TopicPartition("p5-all", 1);
        TopicPartition worse = new TopicPartition("p5-all", 2);

        KafkaException first = new KafkaException(11, "first failure", isRetriable: true);
        KafkaException second = new KafkaException(12, "second failure", isRetriable: false);

        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = first,
            [worse] = second,
        };

        DeleteConsumerGroupOffsetsResult mixed = new DeleteConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(outcomes),
            new[] { worse, bad, good });

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(mixed.All), s_deadline);

        Assert.Same(first, thrown);

        DeleteConsumerGroupOffsetsResult clean = new DeleteConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?> { [good] = null }),
            new[] { good });
        await TestTimeout.Run(clean.All, s_deadline);
    }

    /// <summary>A call-level failure of the aggregate future propagates from both accessors.</summary>
    [Fact]
    public async Task AFaultedFuture_PropagatesFromBothAccessors()
    {
        TopicPartition tp = new TopicPartition("p5-fault", 0);
        KafkaException callLevel = new KafkaException(35, "call failed", isRetriable: false);
        TaskCompletionSource<IReadOnlyDictionary<TopicPartition, KafkaException?>> source =
            new TaskCompletionSource<IReadOnlyDictionary<TopicPartition, KafkaException?>>();
        source.SetException(callLevel);

        DeleteConsumerGroupOffsetsResult result = new DeleteConsumerGroupOffsetsResult(source.Task, new[] { tp });

        KafkaException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(tp)),
            s_deadline);
        Assert.Same(callLevel, fromPartitionResult);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(callLevel, fromAll);
    }
}
