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
/// The end-to-end behaviour of M15/P4 Stage 2's two RPCs against
/// <see cref="MockAdminClient"/> — no broker.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The mock gives this stage a real, end-to-end discriminator for the
/// non-injective <see cref="OffsetSpec"/> encoding, and it is the strongest test in the
/// phase.</b> The Rust <c>MockAdminClient.list_offsets</c> fails a <c>TimestampSpec</c>
/// partition with <c>unsupported_version("Not implemented yet")</c> — mirroring Java's
/// mock, which throws <c>UnsupportedOperationException</c> there
/// (<c>MockAdminClient.java:1230</c>) — while <c>Earliest</c> succeeds. So
/// <c>ForTimestamp(-2)</c> and <c>Earliest()</c> have <b>observably different
/// outcomes through the real ABI</b>: if the <c>is_timestamp</c> flag were dropped, the
/// core would decode <c>ForTimestamp(-2)</c> as <c>earliest()</c> and it would
/// <em>succeed</em>. That is a behavioural test of the collision, not a seam inspection.
/// </para>
/// <para>
/// Both RPCs are implemented by the Rust mock, so unlike Stage 1's <c>electLeaders</c>
/// there is a success path for each.
/// </para>
/// </remarks>
public sealed class PublicAdminReassignmentsOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws for a timestamp spec and the
    /// Rust mock translates.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// ⚠⚠ <b><c>ForTimestamp(-2)</c> and <c>Earliest()</c> produce DIFFERENT calls</b> —
    /// measured through the real ABI, in one request, where the two would be
    /// indistinguishable if the <c>is_timestamp</c> flag were dropped.
    /// </summary>
    /// <remarks>
    /// <c>-2</c> is deliberately the <em>earliest</em> sentinel, so the projection collides
    /// exactly. The timestamp partition faults and the earliest partition succeeds; a
    /// binding that sent only the number would see both succeed.
    /// </remarks>
    [Fact]
    public async Task ListOffsets_ForTimestampAtTheEarliestSentinel_DiffersFromEarliest()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-collide", 2, 1) }).All(), s_deadline);

        TopicPartition viaTimestamp = new TopicPartition("p4s2-collide", 0);
        TopicPartition viaEarliest = new TopicPartition("p4s2-collide", 1);

        ListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                // ⚠ -2 IS the earliest sentinel. This is still a timestamp query.
                [viaTimestamp] = OffsetSpec.ForTimestamp(-2),
                [viaEarliest] = OffsetSpec.Earliest(),
            });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(viaTimestamp)), s_deadline);
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        ListOffsetsResult.ListOffsetsResultInfo earliest =
            await TestTimeout.Run(() => result.PartitionResult(viaEarliest), s_deadline);
        Assert.NotNull(earliest);
    }

    /// <summary>
    /// Each of the six no-argument kinds reaches the core as a <b>recognised</b> sentinel —
    /// none is rejected — and each yields a value rather than a fault.
    /// </summary>
    /// <remarks>
    /// ⚠ This is the round-trip half of the seven-kind walk: a wrong sentinel would be
    /// rejected by the ABI (the whole call fails) or silently decode as a different kind.
    /// The <c>TimestampSpec</c> kind is covered by
    /// <see cref="ListOffsets_ForTimestampAtTheEarliestSentinel_DiffersFromEarliest"/>,
    /// which is where its distinctness actually shows.
    /// </remarks>
    [Theory]
    [InlineData("latest")]
    [InlineData("earliest")]
    [InlineData("maxTimestamp")]
    [InlineData("earliestLocal")]
    [InlineData("latestTiered")]
    [InlineData("earliestPendingUpload")]
    public async Task ListOffsets_EverySentinelKindIsAccepted(string kind)
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-kinds", 1, 1) }).All(), s_deadline);

        TopicPartition partition = new TopicPartition("p4s2-kinds", 0);
        OffsetSpec spec = kind switch
        {
            "latest" => OffsetSpec.Latest(),
            "earliest" => OffsetSpec.Earliest(),
            "maxTimestamp" => OffsetSpec.MaxTimestamp(),
            "earliestLocal" => OffsetSpec.EarliestLocal(),
            "latestTiered" => OffsetSpec.LatestTiered(),
            _ => OffsetSpec.EarliestPendingUpload(),
        };

        ListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [partition] = spec });

        // The mock seeds no offsets through the ABI, so -1 is the expected value; what is
        // under test is that the call was ACCEPTED and the partition resolved.
        ListOffsetsResult.ListOffsetsResultInfo info =
            await TestTimeout.Run(() => result.PartitionResult(partition), s_deadline);
        Assert.Equal(-1, info.Offset);
        Assert.Equal(-1, info.Timestamp);

        // The mock builds every info with Optional.empty(), so absence round-trips as null.
        Assert.Null(info.LeaderEpoch);
    }

    /// <summary>
    /// <c>listOffsets</c> keeps <b>per-partition granularity</b>: a failing partition faults
    /// only its own awaitable, and <c>all()</c> faults while the healthy partition still
    /// resolves.
    /// </summary>
    [Fact]
    public async Task ListOffsets_FaultsOnlyTheFailingPartition()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-granular", 2, 1) }).All(), s_deadline);

        TopicPartition good = new TopicPartition("p4s2-granular", 0);
        TopicPartition bad = new TopicPartition("p4s2-granular", 1);

        ListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [good] = OffsetSpec.Latest(),
                [bad] = OffsetSpec.ForTimestamp(123456789L),
            });

        Assert.NotNull(await TestTimeout.Run(() => result.PartitionResult(good), s_deadline));
        await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// <c>partitionResult</c> <b>throws</b> for a partition that was not requested — Java's
    /// <c>IllegalArgumentException</c>, message carried across verbatim
    /// (<c>ListOffsetsResult.java:43-46</c>).
    /// </summary>
    [Fact]
    public async Task ListOffsets_PartitionResultThrowsForAnUnrequestedPartition()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-unrequested", 1, 1) }).All(), s_deadline);

        TopicPartition requested = new TopicPartition("p4s2-unrequested", 0);
        ListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [requested] = OffsetSpec.Latest() });

        // ⚠ A STATEMENT lambda, so this binds to the Action overload. `PartitionResult`
        // returns a Task, but it throws SYNCHRONOUSLY rather than handing back a faulted
        // one — which is Java's behaviour (`ListOffsetsResult.java:43-46` throws from the
        // method) and is the distinction the expression form would have hidden behind
        // xunit's Func<Task> overload.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionResult(new TopicPartition("p4s2-unrequested", 7)); });
        Assert.Equal("partition", rejected.ParamName);
        Assert.Contains(
            "List Offsets for partition \"p4s2-unrequested-7\" was not attempted",
            rejected.Message,
            StringComparison.Ordinal);

        await TestTimeout.Run(() => result.PartitionResult(requested), s_deadline);
    }

    /// <summary>
    /// <c>listPartitionReassignments</c> round-trips a reassignment created through
    /// <c>alterPartitionReassignments</c>, with all three broker lists surviving the result
    /// root's destruction.
    /// </summary>
    /// <remarks>
    /// The mock derives <c>addingReplicas</c> / <c>removingReplicas</c> by comparing the
    /// target replicas against the topic's current ones, so a non-trivial three-list value
    /// comes back rather than three empty lists.
    /// </remarks>
    [Fact]
    public async Task ListPartitionReassignments_ReturnsTheReassignmentJustCreated()
    {
        await using MockAdminClient admin = new MockAdminClient(3);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-lpr", 1, 2) }).All(), s_deadline);

        TopicPartition partition = new TopicPartition("p4s2-lpr", 0);
        await TestTimeout.Run(
            () => admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>
                {
                    [partition] = new NewPartitionReassignment(new[] { 0, 2 }),
                }).All(),
            s_deadline);

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
            await TestTimeout.Run(
                () => admin.ListPartitionReassignments(new[] { partition }).Reassignments(), s_deadline);

        PartitionReassignment reassignment = Assert.Contains(partition, reassignments);
        Assert.NotEmpty(reassignment.Replicas);

        // Everything here is owned managed state copied out during the walk (ffi §B4) — the
        // native root was destroyed by the trampoline before this line ran.
        Assert.All(reassignment.AddingReplicas, broker => Assert.DoesNotContain(broker, reassignment.Replicas));
        Assert.All(reassignment.RemovingReplicas, broker => Assert.Contains(broker, reassignment.Replicas));
    }

    /// <summary>
    /// ⚠ A partition with <b>no</b> ongoing reassignment is <b>absent</b> from the map, not
    /// present-and-empty and not an error — the header's "the result can be shorter than
    /// the request".
    /// </summary>
    /// <remarks>
    /// This is why the RPC uses the aggregate bridge: a per-key bridge would pre-register
    /// the caller's partitions and fault every quiet one.
    /// </remarks>
    [Fact]
    public async Task ListPartitionReassignments_AQuietPartitionIsAbsentNotFaulted()
    {
        await using MockAdminClient admin = new MockAdminClient(2);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-quiet", 2, 1) }).All(), s_deadline);

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
            await TestTimeout.Run(
                () => admin.ListPartitionReassignments(
                    new[]
                    {
                        new TopicPartition("p4s2-quiet", 0),
                        new TopicPartition("p4s2-quiet", 1),
                    }).Reassignments(),
                s_deadline);

        Assert.Empty(reassignments);
    }

    /// <summary>
    /// A <see langword="null"/> selection — Java's <c>Optional.empty()</c> — lists the whole
    /// cluster and is accepted, as is an empty one. Both are valid; they are different
    /// requests, which is pinned at the submit seam.
    /// </summary>
    [Fact]
    public async Task ListPartitionReassignments_AcceptsNullAndEmptySelections()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        Assert.Empty(await TestTimeout.Run(
            () => admin.ListPartitionReassignments(null).Reassignments(), s_deadline));
        Assert.Empty(await TestTimeout.Run(
            () => admin.ListPartitionReassignments(Array.Empty<TopicPartition>()).Reassignments(), s_deadline));
    }

    /// <summary>
    /// <see cref="ListPartitionReassignmentsResult.Reassignments"/> returns the <b>same</b>
    /// <see cref="Task"/> instance every call, as Java's accessor returns its stored future.
    /// </summary>
    [Fact]
    public async Task ListPartitionReassignmentsResult_ReturnsTheSameTaskInstance()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        ListPartitionReassignmentsResult result = admin.ListPartitionReassignments(null);

        Assert.Same(result.Reassignments(), result.Reassignments());
        await TestTimeout.Run(result.Reassignments, s_deadline);
    }

    /// <summary>
    /// Both RPCs reject use after close, before any native call, and are reachable through
    /// <see cref="IAdmin"/>.
    /// </summary>
    [Fact]
    public async Task BothRpcs_ThrowObjectDisposedAfterClose_AndAreReachableThroughTheInterface()
    {
        await using (MockAdminClient live = new MockAdminClient(1))
        {
            IAdmin admin = live;
            await TestTimeout.Run(() => admin.ListPartitionReassignments(null).Reassignments(), s_deadline);
            await TestTimeout.Run(
                () => admin.ListOffsets(new Dictionary<TopicPartition, OffsetSpec>()).All(), s_deadline);
        }

        MockAdminClient closed = new MockAdminClient(1);
        closed.Dispose();

        Assert.Throws<ObjectDisposedException>(() => closed.ListPartitionReassignments(null));
        Assert.Throws<ObjectDisposedException>(
            () => closed.ListOffsets(new Dictionary<TopicPartition, OffsetSpec>()));

        Assert.NotNull(typeof(KafkaAdminClient).GetMethod(nameof(IAdmin.ListPartitionReassignments)));
        Assert.NotNull(typeof(KafkaAdminClient).GetMethod(nameof(IAdmin.ListOffsets)));
    }
}
