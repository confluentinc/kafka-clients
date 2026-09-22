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
/// The end-to-end behaviour of M15/P4 Stage 1's two RPCs against
/// <see cref="MockAdminClient"/> — no broker.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><c>electLeaders</c> has no success path in the mock, and that is FAITHFUL, not a
/// gap.</b> Java's <c>MockAdminClient.electLeaders</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:797</c>), and the Rust mock translates that into a future
/// failed with an "unsupported" <c>KafkaError</c> (<c>admin-client.md</c> §9 —
/// CLAUDE.md §10.1 forbids a panic in a public API). So the tests below assert that
/// <b>exact message</b> (<c>definition-of-done.md</c> §3) rather than routing around it or
/// inventing mock behaviour the core does not have.
/// </para>
/// <para>
/// <c>alterPartitionReassignments</c> <em>is</em> implemented by Java's mock and by the
/// core's, so its per-partition behaviour is exercised for real here — including the
/// cancel path, which is the phase's null-versus-empty hazard.
/// </para>
/// <para>
/// ⚠ <b>The map-value semantics of <see cref="ElectLeadersResult"/> are therefore tested
/// against the public type directly</b>, with a map the test supplies — the same
/// accommodation <c>AdminP3ResultMarshalTests.NullController_ProjectsAsASuccessfulNull</c>
/// makes for a branch the mock cannot produce. The walk that builds such a map from a real
/// native root is measured in <c>Interop.AdminP4ResultMarshalTests</c>.
/// </para>
/// </remarks>
public sealed class PublicAdminElectionsReassignmentsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>The code Kafka assigns to <c>UNKNOWN_TOPIC_OR_PARTITION</c>.</summary>
    private const int UnknownTopicOrPartitionCode = 3;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws and the Rust mock
    /// translates.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// <c>electLeaders</c> reaches the core and its single awaitable carries the mock's
    /// documented refusal — for an explicit selection, an empty one, and the
    /// <see langword="null"/> "every partition" one alike.
    /// </summary>
    /// <remarks>
    /// ⚠ The three inputs are different <em>requests</em> (see
    /// <c>Interop.AdminP4SubmitArgumentTests</c>, which reads the flag they set); against
    /// this mock they share an outcome, which is why the distinction is pinned at the seam
    /// rather than here.
    /// </remarks>
    [Fact]
    public async Task ElectLeaders_SurfacesTheMocksDocumentedRefusal_ForEverySelectionShape()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        IReadOnlyCollection<TopicPartition>?[] selections =
        {
            new[] { new TopicPartition("p4-elect", 0) },
            Array.Empty<TopicPartition>(),
            null,
        };

        foreach (IReadOnlyCollection<TopicPartition>? selection in selections)
        {
            ElectLeadersResult result = admin.ElectLeaders(ElectionType.Preferred, selection);

            KafkaException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(result.Partitions), s_deadline);
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);

            // all() forwards a call-level failure rather than swallowing it —
            // ElectLeadersResult.java:59-60.
            KafkaException fromAll = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
            Assert.Equal(UnsupportedVersionCode, fromAll.Code);
            Assert.Equal(NotImplemented, fromAll.Message);
        }
    }

    /// <summary>
    /// Both election types are accepted and reach the core — the enum's values are the ABI
    /// codes, so a wrong one would be rejected rather than refused by the mock.
    /// </summary>
    [Theory]
    [InlineData(ElectionType.Preferred)]
    [InlineData(ElectionType.Unclean)]
    public async Task ElectLeaders_AcceptsBothElectionTypes(ElectionType type)
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                admin.ElectLeaders(type, new[] { new TopicPartition("p4-type", 0) }).Partitions),
            s_deadline);

        // The mock's refusal, not an IllegalArgument from the ABI's election-type check —
        // which is what a wrongly-encoded enum value would have produced.
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// <see cref="ElectLeadersResult.Partitions"/> returns the <b>same</b>
    /// <see cref="Task"/> instance on every call, as Java's accessor returns the stored
    /// future field.
    /// </summary>
    [Fact]
    public async Task ElectLeadersResult_PartitionsReturnsTheSameTaskInstance()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        ElectLeadersResult result = admin.ElectLeaders(ElectionType.Preferred, null);

        Assert.Same(result.Partitions(), result.Partitions());

        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.Partitions), s_deadline);
    }

    /// <summary>
    /// ⚠⚠ <b>A per-partition failure is a map VALUE on a SUCCESSFUL task</b> — Java's
    /// <c>Optional&lt;Throwable&gt;</c> (<c>ElectLeadersResult.java:43-46</c>) — and
    /// <c>all()</c> is what turns the first present one into a failure.
    /// </summary>
    /// <remarks>
    /// The mock cannot produce this input (see the type remarks), so the public type is
    /// driven directly. A binding that routed <c>electLeaders</c> through the per-key
    /// bridge would have no map to assert against at all — the structural half of that
    /// claim is in <c>PublicAdminP4ShapeParityTests</c>.
    /// </remarks>
    [Fact]
    public async Task ElectLeadersResult_APerPartitionFailureIsAValue_AndAllReportsTheFirst()
    {
        TopicPartition good = new TopicPartition("p4-all", 0);
        TopicPartition bad = new TopicPartition("p4-all", 1);
        TopicPartition worse = new TopicPartition("p4-all", 2);

        KafkaException first = new KafkaException(11, "first failure", isRetriable: false);
        KafkaException second = new KafkaException(12, "second failure", isRetriable: false);

        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = first,
            [worse] = second,
        };

        ElectLeadersResult mixed = new ElectLeadersResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(outcomes));

        // partitions() SUCCEEDS: the failures are values, not faults.
        IReadOnlyDictionary<TopicPartition, KafkaException?> map =
            await TestTimeout.Run(mixed.Partitions, s_deadline);
        Assert.Null(map[good]);
        Assert.Same(first, map[bad]);
        Assert.Same(second, map[worse]);

        // all() completes exceptionally with the FIRST present Optional and does not
        // inspect the rest (ElectLeadersResult.java:62-67).
        KafkaException thrown =
            await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(mixed.All), s_deadline);
        Assert.Same(map.Values.First(value => value is not null), thrown);

        // …and with every election succeeded, all() completes.
        ElectLeadersResult clean = new ElectLeadersResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?> { [good] = null }));
        await TestTimeout.Run(clean.All, s_deadline);
    }

    /// <summary>
    /// <c>alterPartitionReassignments</c> round-trips through the mock with <b>per-partition
    /// granularity</b>: a partition the mock does not know faults only its own awaitable,
    /// while the others succeed.
    /// </summary>
    [Fact]
    public async Task AlterPartitionReassignments_FaultsOnlyTheFailingPartition()
    {
        await using MockAdminClient admin = new MockAdminClient(3);
        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic("p4-apr", 2, 1) }).All(), s_deadline);

        TopicPartition zero = new TopicPartition("p4-apr", 0);
        TopicPartition one = new TopicPartition("p4-apr", 1);
        TopicPartition missing = new TopicPartition("p4-apr-missing", 0);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [zero] = new NewPartitionReassignment(new[] { 0, 1 }),
                [one] = new NewPartitionReassignment(new[] { 1, 2 }),
                [missing] = new NewPartitionReassignment(new[] { 0 }),
            });

        Assert.Equal(3, result.Values.Count);

        await TestTimeout.Run(() => result.Values[zero], s_deadline);
        await TestTimeout.Run(() => result.Values[one], s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[missing]), s_deadline);
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);

        // all() is KafkaFuture.allOf, so one failure fails the batch — but only after the
        // successful partitions have completed on their own.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// ⚠ Cancelling a reassignment — a <see langword="null"/> entry, Java's
    /// <c>Optional.empty()</c> — succeeds end to end, and is <b>not</b> the same request as
    /// a reassignment.
    /// </summary>
    /// <remarks>
    /// The mock implements both halves (its <c>alter_partition_reassignments</c> inserts on
    /// a present reassignment and removes on a cancel), so this is a real round trip rather
    /// than an inspection. A binding that turned a cancellation into a present-but-empty
    /// replica list would fail the whole call here, because the ABI rejects that outright —
    /// which is what <c>AdminP4ResultMarshalTests</c> measures at the ABI itself.
    /// </remarks>
    [Fact]
    public async Task AlterPartitionReassignments_CancelSucceeds_AlongsideAReassignment()
    {
        await using MockAdminClient admin = new MockAdminClient(3);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4-apr-cancel", 2, 1) }).All(), s_deadline);

        TopicPartition reassigned = new TopicPartition("p4-apr-cancel", 0);
        TopicPartition cancelled = new TopicPartition("p4-apr-cancel", 1);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [reassigned] = new NewPartitionReassignment(new[] { 0, 2 }),
                [cancelled] = null,
            });

        await TestTimeout.Run(result.All, s_deadline);
        Assert.Equal(2, result.Values.Count);
    }

    /// <summary>
    /// <see cref="AlterPartitionReassignmentsOptions.AllowReplicationFactorChange"/> is
    /// accepted both ways end to end — the flag reaches the core rather than being dropped.
    /// </summary>
    /// <remarks>
    /// The mock ignores its options, so this asserts only that neither value breaks the
    /// call; that the flag reaches the submit in its own slot is read at the seam by
    /// <c>Interop.AdminP4SubmitArgumentTests</c>.
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AlterPartitionReassignments_AcceptsBothReplicationFactorSettings(bool allow)
    {
        await using MockAdminClient admin = new MockAdminClient(2);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4-apr-rf", 1, 1) }).All(), s_deadline);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-apr-rf", 0)] = new NewPartitionReassignment(new[] { 0, 1 }),
            },
            new AlterPartitionReassignmentsOptions { AllowReplicationFactorChange = allow });

        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// An empty request map is a legitimate call — it produces no awaitables, and
    /// <c>all()</c> completes.
    /// </summary>
    [Fact]
    public async Task AlterPartitionReassignments_AnEmptyMapCompletes()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// Both RPCs reject use after close, before any native call — the shared
    /// <c>ThrowIfClosed</c> guard, re-asserted for each new entry point.
    /// </summary>
    [Fact]
    public void BothRpcs_ThrowObjectDisposedAfterClose()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.ElectLeaders(ElectionType.Preferred, null));
        Assert.Throws<ObjectDisposedException>(() => admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>()));
    }

    /// <summary>
    /// Both RPCs are reachable through <see cref="IAdmin"/> itself, on the real client type
    /// as well as the mock — the interface is what a caller programs against.
    /// </summary>
    /// <remarks>
    /// Driven through the mock instance, because constructing a
    /// <see cref="KafkaAdminClient"/> would need a broker; what is asserted is that the
    /// interface dispatch reaches the same implementation, and that
    /// <see cref="KafkaAdminClient"/> implements both members at all.
    /// </remarks>
    [Fact]
    public async Task BothRpcs_AreReachableThroughTheInterface()
    {
        await using MockAdminClient mock = new MockAdminClient(1);
        IAdmin admin = mock;

        await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                admin.ElectLeaders(ElectionType.Preferred, null).Partitions),
            s_deadline);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>());
        await TestTimeout.Run(result.All, s_deadline);

        Assert.NotNull(typeof(KafkaAdminClient).GetMethod(nameof(IAdmin.ElectLeaders)));
        Assert.NotNull(typeof(KafkaAdminClient).GetMethod(nameof(IAdmin.AlterPartitionReassignments)));
    }
}
