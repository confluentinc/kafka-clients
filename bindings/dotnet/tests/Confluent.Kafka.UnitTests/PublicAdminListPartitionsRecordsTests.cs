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
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The end-to-end behaviour of M15/P2b's three RPCs against <see cref="MockAdminClient"/>
/// — no broker.
/// </summary>
/// <remarks>
/// ⚠ <b>Two of the three have no success path in the mock, and that is FAITHFUL, not a
/// gap.</b> The Rust <c>MockAdminClient</c> completes every key of
/// <c>createPartitions</c> — and of a <em>non-empty</em> <c>deleteRecords</c> — with
/// <c>unsupported_version("Not implemented yet")</c>, translating Java's own
/// <c>MockAdminClient.java:626-628</c> / <c>:631-638</c>, which throw
/// <c>UnsupportedOperationException("Not implemented yet")</c> there
/// (<c>admin-client.md</c> §9). So the tests below assert that <b>exact message</b>
/// (<c>definition-of-done.md</c> §3) rather than routing around it or inventing mock
/// behaviour the core does not have. An <b>empty</b> <c>deleteRecords</c> does succeed,
/// mirroring Java, and is tested as such.
/// </remarks>
public sealed class PublicAdminListPartitionsRecordsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The C code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws and the Rust mock
    /// translates.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// <c>listTopics</c> round-trips through the mock, and the three projections are views
    /// of the <b>same</b> completed map — asserted together, on one result, because that is
    /// the property that would be lost if they were three independent requests.
    /// </summary>
    [Fact]
    public async Task ListTopics_ReturnsTheCreatedTopics_AndTheThreeProjectionsAgree()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(
                new[] { new NewTopic("p2b-list-a", 1, 1), new NewTopic("p2b-list-b", 2, 1) }).All(),
            s_deadline);

        ListTopicsResult result = admin.ListTopics();

        IReadOnlyDictionary<string, TopicListing> map =
            await TestTimeout.Run(() => result.NamesToListings(), s_deadline);
        IReadOnlyCollection<TopicListing> listings = await TestTimeout.Run(() => result.Listings(), s_deadline);
        IReadOnlyCollection<string> names = await TestTimeout.Run(() => result.Names(), s_deadline);

        Assert.Contains("p2b-list-a", map.Keys);
        Assert.Contains("p2b-list-b", map.Keys);

        // The projections cannot disagree with the map they are derived from.
        Assert.Equal(map.Count, listings.Count);
        Assert.Equal(map.Count, names.Count);
        Assert.Equal(
            map.Keys.OrderBy(name => name, StringComparer.Ordinal),
            names.OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal(
            map.Values.Select(listing => listing.Name).OrderBy(name => name, StringComparer.Ordinal),
            listings.Select(listing => listing.Name).OrderBy(name => name, StringComparer.Ordinal));

        // The listing's fields survive the result root being destroyed: everything here is
        // owned managed state copied out during the walk (ffi §B4).
        TopicListing a = map["p2b-list-a"];
        Assert.Equal("p2b-list-a", a.Name);
        Assert.False(a.IsInternal);
        Assert.NotEqual(Uuid.Zero, a.TopicId);
    }

    /// <summary>
    /// <see cref="ListTopicsOptions.ListInternal"/> reaches the ABI: the flag crosses as
    /// the <c>list_internal</c> parameter rather than being dropped.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Asserted at the submit boundary rather than through the mock's topic set, because
    /// the mock has no internal topics to filter — so a behavioural assertion would pass
    /// whichever value crossed. What this pins is the managed half: the option reaches the
    /// submit as its own argument, in the right position, rather than being dropped or
    /// swapped with the timeout beside it.
    /// </para>
    /// <para>
    /// ⚠ <b>It does NOT test <c>MarshalAs(I1)</c>, and an earlier version of this comment
    /// claimed it did.</b> The injected submit never crosses the P/Invoke, so no
    /// marshalling happens here at all — measured by deleting the attribute, which left
    /// this test green. The attribute is pinned structurally instead, in
    /// <c>AdminNativeMethodsMarshallingTests</c>.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ListTopics_ListInternalCrossesTheBoundary(bool listInternal)
    {
        using Confluent.Kafka.Internal.NativeAdminClient admin =
            Confluent.Kafka.Internal.NativeAdminClient.CreateMock(1);

        bool? captured = null;
        int capturedTimeout = int.MinValue;
        ListTopicsResult result = admin.ListTopics(
            new ListTopicsOptions { ListInternal = listInternal, TimeoutMs = 4321 },
            (nativeHandle, timeoutMs, flag, callback, userData) =>
            {
                captured = flag;
                capturedTimeout = timeoutMs;

                // The operation must still be completed, or its GCHandle and span-the-op
                // reference would leak. ⚠ It is completed with an ERROR, not with
                // `(IntPtr.Zero, IntPtr.Zero)`: the ABI states "exactly one of result /
                // error is non-null", so a both-null callback is outside the contract and
                // feeds `ListTopicsResult_count` a NULL — which ABORTS the test host
                // rather than failing an assertion.
                using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("probe");
                AdminCallbacks.ListTopics(
                    IntPtr.Zero, NativeMethods.KafkaErrorNew(1, message.Pointer), userData);
            });

        Assert.Equal(listInternal, captured);

        // The argument after the bool is intact — the I1 marshalling guard.
        Assert.Equal(4321, capturedTimeout);

        // Observe the fault so the awaiter is not left unobserved.
        Assert.True(result.NamesToListings().IsFaulted);
    }

    /// <summary>
    /// ⚠ <b><c>createPartitions</c> against the mock fails per key with Java's exact
    /// message.</b> This is the faithful translation of Java's own mock, not a hole to
    /// route around — see the type remarks.
    /// </summary>
    [Fact]
    public async Task CreatePartitions_AgainstTheMock_FailsPerTopicWithJavasExactMessage()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p2b-grow", 1, 1) }).All(), s_deadline);

        CreatePartitionsResult result = admin.CreatePartitions(
            new Dictionary<string, NewPartitions> { ["p2b-grow"] = NewPartitions.IncreaseTo(3) });

        // Per-KEY, not a call failure: the awaitable for that topic is what carries it.
        Task perTopic = Assert.Single(result.Values).Value;
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => perTopic, s_deadline));

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        // all() aggregates the same failure.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All(), s_deadline));
    }

    /// <summary>
    /// An <b>empty</b> <c>deleteRecords</c> succeeds with an empty result — Java's mock
    /// returns an empty result for an empty request, and only the non-empty path is
    /// unsupported.
    /// </summary>
    [Fact]
    public async Task DeleteRecords_WithAnEmptyRequest_SucceedsWithAnEmptyResult()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DeleteRecordsResult result = admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete>());

        Assert.Empty(result.LowWatermarks);

        // Task.WhenAll over nothing completes, so the empty batch really does succeed.
        await TestTimeout.Run(() => result.All(), s_deadline);
        Assert.Equal(TaskStatus.RanToCompletion, result.All().Status);
    }

    /// <summary>
    /// ⚠ A <b>non-empty</b> <c>deleteRecords</c> against the mock fails per partition with
    /// Java's exact message — the other half of Java's mock behaviour.
    /// </summary>
    [Fact]
    public async Task DeleteRecords_AgainstTheMock_FailsPerPartitionWithJavasExactMessage()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        TopicPartition partition = new TopicPartition("p2b-delete", 0);
        DeleteRecordsResult result = admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete>
            {
                [partition] = RecordsToDelete.BeforeOffset(10),
            });

        Task<DeletedRecords> perPartition = Assert.Single(result.LowWatermarks).Value;
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => perPartition, s_deadline));

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        // The composite key survived the round trip into the result's dictionary.
        Assert.Equal(partition, Assert.Single(result.LowWatermarks).Key);
    }

    /// <summary>
    /// ⚠ <b><c>increaseTo(n)</c> and <c>increaseTo(n, emptyList())</c> are DIFFERENT
    /// requests, and the difference must reach the ABI.</b>
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java's <c>increaseTo(n)</c> leaves <c>newAssignments</c> <b>null</b>, which the
    /// broker reads as "you choose"; <c>increaseTo(n, emptyList())</c> sends a
    /// <em>present but empty</em> list, which the broker rejects with
    /// <c>INVALID_REPLICA_ASSIGNMENT</c> (<c>CreatePartitionsRequest.json:36</c> marks
    /// <c>Assignments</c> <c>"nullableVersions": "0+"</c>). A
    /// <c>?? Array.Empty&lt;…&gt;()</c> anywhere on this path silently rewrites one request
    /// into the other.
    /// </para>
    /// <para>
    /// The ABI carries the distinction as the <c>has_assignments</c> discriminant and
    /// exposes <b>no getter</b> for it, so the flag cannot be read back once it has
    /// crossed. The assertion is therefore made on production's own decision function,
    /// <see cref="NewPartitionsMarshal.HasAssignments"/> — the value <c>Build</c> is about
    /// to pass — together with the public property it is derived from
    /// (<c>definition-of-done.md</c> §12).
    /// </para>
    /// </remarks>
    [Fact]
    public void NewPartitions_NullAndEmptyAssignments_ProduceDifferentRequests()
    {
        NewPartitions broker = NewPartitions.IncreaseTo(3);
        NewPartitions empty = NewPartitions.IncreaseTo(3, Array.Empty<IReadOnlyList<int>>());
        NewPartitions explicitAssignment = NewPartitions.IncreaseTo(3, new[] { new[] { 0 } });

        // The public shape keeps them apart…
        Assert.Null(broker.Assignments);
        Assert.NotNull(empty.Assignments);
        Assert.Empty(empty.Assignments!);
        Assert.Single(explicitAssignment.Assignments!);

        // …and so does the value that crosses the boundary. THIS is the assertion that
        // fails if the two are ever coalesced.
        Assert.False(NewPartitionsMarshal.HasAssignments(broker));
        Assert.True(NewPartitionsMarshal.HasAssignments(empty));
        Assert.True(NewPartitionsMarshal.HasAssignments(explicitAssignment));

        // A count-based discriminant would agree with the flag for the third case and
        // disagree for the second — which is exactly what makes the second the probe.
        Assert.Empty(empty.Assignments!);

        // The assignment list is defensively copied, so a caller mutating theirs afterwards
        // cannot change what was requested.
        List<IReadOnlyList<int>> mutable = new List<IReadOnlyList<int>> { new[] { 1, 2 } };
        NewPartitions copied = NewPartitions.IncreaseTo(4, mutable);
        mutable.Clear();
        Assert.Equal(new[] { 1, 2 }, Assert.Single(copied.Assignments!));
    }

    /// <summary>
    /// Preconditions are validated <b>before</b> any pin / marshal / P-Invoke (ffi §B5):
    /// the ABI does not validate them, and it silently <em>skips</em> a pair with a null
    /// side rather than reporting it — so a dropped topic would surface only as a
    /// "result contained no entry" fault much later.
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Throws<ArgumentNullException>(() => admin.CreatePartitions(null!));
        Assert.Throws<ArgumentNullException>(() => admin.DeleteRecords(null!));

        ArgumentException nullEntry = Assert.Throws<ArgumentException>(
            () => admin.CreatePartitions(new Dictionary<string, NewPartitions> { ["t"] = null! }));
        Assert.Equal("newPartitions", nullEntry.ParamName);
        Assert.Contains("must not be null", nullEntry.Message, StringComparison.Ordinal);

        ArgumentException nullRecords = Assert.Throws<ArgumentException>(
            () => admin.DeleteRecords(
                new Dictionary<TopicPartition, RecordsToDelete> { [new TopicPartition("t", 0)] = null! }));
        Assert.Equal("recordsToDelete", nullRecords.ParamName);

        // A `default(TopicPartition)` has a null Topic, which the ABI would skip.
        ArgumentException nullTopic = Assert.Throws<ArgumentException>(
            () => admin.DeleteRecords(
                new Dictionary<TopicPartition, RecordsToDelete>
                {
                    [default] = RecordsToDelete.BeforeOffset(1),
                }));
        Assert.Equal("recordsToDelete", nullTopic.ParamName);

        // A negative timeout is read by the ABI as "unset", so it must be rejected rather
        // than silently reinterpreted.
        foreach (Action negative in new Action[]
                 {
                     () => admin.ListTopics(new ListTopicsOptions { TimeoutMs = -1 }),
                     () => admin.CreatePartitions(
                         new Dictionary<string, NewPartitions>(),
                         new CreatePartitionsOptions { TimeoutMs = -1 }),
                     () => admin.DeleteRecords(
                         new Dictionary<TopicPartition, RecordsToDelete>(),
                         new DeleteRecordsOptions { TimeoutMs = -1 }),
                 })
        {
            ArgumentOutOfRangeException range = Assert.Throws<ArgumentOutOfRangeException>(negative);
            Assert.Equal("options", range.ParamName);
            Assert.Contains("must not be negative", range.Message, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Every new RPC is guarded against use after close, like the P1/P2a ones.
    /// </summary>
    [Fact]
    public void AfterClose_EveryNewRpcThrowsObjectDisposed()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
        Assert.Throws<ObjectDisposedException>(
            () => admin.CreatePartitions(new Dictionary<string, NewPartitions>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.DeleteRecords(new Dictionary<TopicPartition, RecordsToDelete>()));
    }

    /// <summary>
    /// A non-ASCII topic survives the round trip through <c>listTopics</c>' key and its
    /// listing — the manual UTF-8 marshalling guard (ffi §B3; an <c>LPStr</c> mistake
    /// corrupts non-ASCII silently and hides in ASCII-only tests).
    /// </summary>
    [Fact]
    public async Task NonAsciiTopic_RoundTripsThroughListTopics()
    {
        const string Topic = "témas-日本語-🎉";

        await using MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All(), s_deadline);

        IReadOnlyDictionary<string, TopicListing> map =
            await TestTimeout.Run(() => admin.ListTopics().NamesToListings(), s_deadline);

        Assert.True(map.ContainsKey(Topic));
        Assert.Equal(Topic, map[Topic].Name);
    }

    /// <summary>
    /// The TFM-matrix smoke leg for P2b: the three new RPCs load and round-trip on
    /// whichever framework is executing (net462 via netstandard2.0, net8.0, net10.0).
    /// </summary>
    [Fact]
    public async Task TfmSmoke_TheThreeNewRpcsWorkOnThisFramework()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p2b-smoke", 1, 1) }).All(), s_deadline);

        Assert.NotEmpty(await TestTimeout.Run(() => admin.ListTopics().Names(), s_deadline));

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(
                () => admin.CreatePartitions(
                    new Dictionary<string, NewPartitions> { ["p2b-smoke"] = NewPartitions.IncreaseTo(2) }).All(),
                s_deadline));

        await TestTimeout.Run(
            () => admin.DeleteRecords(new Dictionary<TopicPartition, RecordsToDelete>()).All(), s_deadline);
    }
}
