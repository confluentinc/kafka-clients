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
/// <c>MockAdminClient.java:783-785</c>), and the core fails the RPC's one future with it, so
/// the single awaitable faults and both accessors rethrow the refusal verbatim — the same
/// shape as <see cref="AlterConsumerGroupOffsetsResult"/>.
/// </para>
/// <para>
/// ⚠ Unlike <see cref="AlterConsumerGroupOffsetsResult.All"/>, whose failure message names
/// every failed partition, <see cref="DeleteConsumerGroupOffsetsResult.All"/> reports the
/// <b>first</b> failing partition's own outcome. Both choices are the core's — the binding
/// rethrows the stored <c>all()</c> outcome — so the result-level tests below supply that
/// outcome and assert it is rethrown, not re-derived.
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
    /// <c>deleteConsumerGroupOffsets</c> reaches the core, whose mock fails the whole request,
    /// and both accessors rethrow the mock's documented refusal verbatim — Java's
    /// <c>throwable != null</c> branches (<c>DeleteConsumerGroupOffsetsResult.java:50-51</c>,
    /// <c>:67-68</c>).
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
    /// (<c>DeleteConsumerGroupOffsetsResult.java:31</c>) — and
    /// <see cref="DeleteConsumerGroupOffsetsResult.PartitionResult"/> is what turns a
    /// non-null entry into a fault (<c>:52</c>).
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

        DeleteConsumerGroupOffsetsResult result = Resolved(outcomes, all: null, good, bad);

        await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// A partition never named in the request faults <b>synchronously</b> with Java's exact
    /// <c>IllegalArgumentException</c> message (no quotes around the partition) — the
    /// precondition check that runs before the future is even consulted
    /// (<c>DeleteConsumerGroupOffsetsResult.java:44-46</c>).
    /// </summary>
    [Fact]
    public void PartitionResult_UnknownPartition_ThrowsSynchronouslyWithJavasExactMessage()
    {
        TopicPartition known = new TopicPartition("p5-known", 0);
        TopicPartition unknown = new TopicPartition("p5-unknown", 7);

        DeleteConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?> { [known] = null }, all: null, known);

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
    /// A requested partition's map value is thrown <b>asynchronously</b>, via the returned
    /// <see cref="Task"/> — including the core's own "not included in the response" error
    /// for a requested partition the broker did not answer (Java's
    /// <c>KafkaAdminClient.getSubLevelError</c>, reached from
    /// <c>DeleteConsumerGroupOffsetsResult.java:84-85</c>). The binding no longer composes
    /// that message; it rethrows the stored value.
    /// </summary>
    [Fact]
    public async Task PartitionResult_ARequestedPartitionsStoredError_IsThrownAsynchronously()
    {
        TopicPartition requested = new TopicPartition("p5-missing", 0);
        KafkaException stored = new KafkaException(
            -1,
            "Offset deletion result for partition \"" + requested + "\" was not included in the response",
            isRetriable: false);

        DeleteConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?> { [requested] = stored }, all: null, requested);

        // Captured BEFORE the assertion: a synchronous throw here would surface at this line,
        // outside Assert.ThrowsAsync's catch — which is what proves the fault is genuinely
        // asynchronous rather than merely tolerated by an assertion helper that accepts either.
        Task task = result.PartitionResult(requested);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => task), s_deadline);
        Assert.Same(stored, thrown);
    }

    /// <summary>
    /// ⚠ A requested partition missing from the resolved map is a <b>core contract
    /// violation</b> — the core reports one row per requested partition — and it faults the
    /// task rather than reporting a success nobody observed.
    /// </summary>
    [Fact]
    public async Task PartitionResult_ARequestedPartitionMissingFromTheMap_FaultsInsteadOfSucceeding()
    {
        TopicPartition requested = new TopicPartition("p5-violation", 0);

        DeleteConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?>(), all: null, requested);

        Task task = result.PartitionResult(requested);

        await TestTimeout.Run(
            () => Assert.ThrowsAsync<KeyNotFoundException>(() => task), s_deadline);
    }

    /// <summary>
    /// ⚠ <see cref="DeleteConsumerGroupOffsetsResult.All"/> rethrows the <b>stored</b>
    /// outcome — the same instance, unchanged. Which failing partition is "first"
    /// (<c>DeleteConsumerGroupOffsetsResult.java:61</c>, <c>:70-74</c>) is the core's choice
    /// now; the binding only carries it.
    /// </summary>
    [Fact]
    public async Task All_RethrowsTheStoredOutcomeUnchanged()
    {
        TopicPartition good = new TopicPartition("p5-all", 0);
        TopicPartition bad = new TopicPartition("p5-all", 1);
        TopicPartition worse = new TopicPartition("p5-all", 2);

        KafkaException first = new KafkaException(11, "first failure", isRetriable: true);

        DeleteConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?>
            {
                [good] = null,
                [bad] = first,
                [worse] = new KafkaException(12, "second failure", isRetriable: false),
            },
            first,
            worse,
            bad,
            good);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(first, thrown);
    }

    /// <summary>
    /// ⚠ The control for <see cref="All_RethrowsTheStoredOutcomeUnchanged"/>: a <b>null</b>
    /// stored outcome completes <see cref="DeleteConsumerGroupOffsetsResult.All"/> even though
    /// the map carries a failure — so nothing is derived from the map.
    /// </summary>
    [Fact]
    public async Task All_CompletesOnANullStoredOutcome_WhateverTheMapHolds()
    {
        TopicPartition bad = new TopicPartition("p5-all-null", 0);
        KafkaException perPartition = new KafkaException(11, "partition failure", isRetriable: false);

        DeleteConsumerGroupOffsetsResult result = Resolved(
            new Dictionary<TopicPartition, KafkaException?> { [bad] = perPartition }, all: null, bad);

        await TestTimeout.Run(result.All, s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        Assert.Same(perPartition, thrown);
    }

    /// <summary>A call-level failure of the single awaitable propagates from both accessors.</summary>
    [Fact]
    public async Task AFaultedFuture_PropagatesFromBothAccessors()
    {
        TopicPartition tp = new TopicPartition("p5-fault", 0);
        KafkaException callLevel = new KafkaException(35, "call failed", isRetriable: false);
        TaskCompletionSource<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> source =
            new TaskCompletionSource<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>();
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

    /// <summary>A result over an already-resolved outcome, the shape the trampoline builds.</summary>
    private static DeleteConsumerGroupOffsetsResult Resolved(
        IReadOnlyDictionary<TopicPartition, KafkaException?> perKey,
        KafkaException? all,
        params TopicPartition[] requested) =>
        new DeleteConsumerGroupOffsetsResult(
            Task.FromResult<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
                (perKey, all)),
            requested);
}
