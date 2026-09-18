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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives M15/P5's result walker — <see cref="KeyedResultMarshal.CompleteTwoLists"/>, the
/// sub-shape-3c marshaller behind <c>listGroups</c> <b>and</b> its deprecated predecessor
/// <c>listConsumerGroups</c> — over the one input shape the Rust mock cannot produce: a
/// result whose <b>two lists have different lengths</b>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>This is the defect the whole RPC is shaped to prevent.</b> A <c>listGroups</c>
/// result carries listings and per-broker errors as two <em>independent</em> lists, each
/// with its own ABI count accessor and no positional correspondence whatsoever. A single
/// loop bounded by the valid count would silently truncate the errors — and would still
/// pass every round-trip a no-failure fixture can produce, because with zero errors the
/// truncation is invisible. Only a case where the counts <b>disagree</b> distinguishes
/// "walked its own count" from "walked the other one", so the two headline cases here are
/// 1 listing + 3 errors and 3 listings + 1 error: one where the error list is longer and
/// one where it is shorter, so neither a min() nor a max() nor a first-count bound
/// survives both.
/// </para>
/// <para>
/// <b>Why the walker is driven directly rather than through the client.</b> The Rust
/// <c>MockAdminClient::list_groups</c> emits one listing per seeded group and <b>never any
/// error</b>, so the mixed outcome is unreachable end-to-end; and the native count
/// accessors dereference their root, so a synthetic root cannot be pushed through the real
/// trampoline either. Injecting the two <see cref="KeyedResultMarshal.CountAccessor"/>s
/// and the two readers exercises production's own walk (<c>definition-of-done.md</c> §12 —
/// the code under test is the shipped walker, not a re-implementation) with the only
/// inputs the mock withholds.
/// </para>
/// <para>
/// <b>The root is a sentinel, and nothing dereferences it.</b> The walker never touches
/// <c>result</c> itself — it only hands it to the accessors and readers — so a non-zero
/// sentinel is both safe and strictly more informative than <see cref="IntPtr.Zero"/>:
/// every injected callback asserts it received that exact pointer, which pins the
/// pass-through the real readers depend on. No native memory is allocated, borrowed, or
/// freed for the two <c>list*Groups</c> sections. The <c>describeConsumerGroups</c> section
/// added later is the one exception, and says so at its own banner: a per-key error must be
/// a real <c>kafka_common_KafkaError_t</c> for the walk to decode, so those cases allocate
/// one and destroy it themselves.
/// </para>
/// <para>
/// <b>The assertions go through the public <see cref="ListGroupsResult"/>.</b> The walker
/// completes a <see cref="SingleAdminOperation{TValue}"/> whose task is exactly what the
/// public result wraps, so asserting on <c>Valid()</c> / <c>Errors()</c> / <c>All()</c>
/// proves the two lengths survive all the way to the user-visible surface rather than
/// merely reaching the tuple.
/// </para>
/// <para>
/// ⚠ <b>The file covers two walkers, not one.</b> The <c>describeConsumerGroups</c> section
/// at the end drives <see cref="KeyedResultMarshal.Complete{TKey, TValue}"/> — shape 1, one
/// count, per-key value OR error — for the same reason: the Rust mock fails every key, so a
/// described value is unreachable end-to-end. Its banner states what differs.
/// </para>
/// </remarks>
public sealed class AdminP5ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A stand-in for the <c>ListGroupsResult_t *</c>. Never dereferenced — see the remarks
    /// on the class.
    /// </summary>
    private static readonly IntPtr s_root = new IntPtr(0x5150);

    /// <summary>
    /// ⚠ <b>THE test for this stage.</b> Each list must come back at <em>its own</em>
    /// length, whichever way the two counts differ.
    /// </summary>
    [Theory]
    [InlineData(1, 3)]   // errors outnumber listings — a valid-count bound truncates them
    [InlineData(3, 1)]   // listings outnumber errors — a valid-count bound over-reads them
    [InlineData(0, 2)]   // every broker failed: no listing at all, but the errors survive
    [InlineData(2, 0)]   // the ordinary success: a valid-count bound is indistinguishable here
    [InlineData(0, 0)]   // nothing at all, and neither walk runs
    [InlineData(2, 2)]   // equal lengths: also indistinguishable, and included to say so
    public async Task TwoLists_EachWalkItsOwnCount(int validCount, int errorCount)
    {
        List<int> listingIndices = new List<int>();
        List<int> errorIndices = new List<int>();

        ListGroupsResult result = Walk(
            validCount,
            errorCount,
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                listingIndices.Add(index);
                return Listing(index);
            },
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                errorIndices.Add(index);
                return Error(index);
            });

        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);
        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);

        // The lengths, independently.
        Assert.Equal(validCount, valid.Count);
        Assert.Equal(errorCount, errors.Count);

        // Each walk visited 0..n-1 of ITS OWN axis, in order and exactly once.
        Assert.Equal(Enumerable.Range(0, validCount), listingIndices);
        Assert.Equal(Enumerable.Range(0, errorCount), errorIndices);

        // And the elements landed in ABI order, so nothing was reordered or shifted.
        Assert.Equal(
            Enumerable.Range(0, validCount).Select(index => "g" + index.ToString(CultureInfo.InvariantCulture)),
            valid.Select(listing => listing.GroupId));
        Assert.Equal(
            Enumerable.Range(0, errorCount).Select(index => "e" + index.ToString(CultureInfo.InvariantCulture)),
            errors.Select(error => error.Message));
    }

    /// <summary>
    /// The second count accessor is consulted <b>after</b> the first walk finishes, and its
    /// own value is what bounds the second walk — so a result that reports a shorter error
    /// list than its listing list cannot be over-read.
    /// </summary>
    [Fact]
    public async Task EachCountAccessor_IsAskedForItsOwnAxis()
    {
        int firstCalls = 0;
        int secondCalls = 0;

        SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation =
            new SingleAdminOperation<(IReadOnlyCollection<GroupListing>, IReadOnlyCollection<KafkaException>)>("listGroups");

        KeyedResultMarshal.CompleteTwoLists(
            s_root,
            root =>
            {
                Assert.Equal(s_root, root);
                firstCalls++;
                return 3;
            },
            root =>
            {
                Assert.Equal(s_root, root);
                secondCalls++;
                return 1;
            },
            operation,
            (_, index) => Listing(index),
            (_, index) => Error(index));

        (IReadOnlyCollection<GroupListing> valid, IReadOnlyCollection<KafkaException> errors) =
            await TestTimeout.Run(() => operation.Task, s_deadline);

        // One ask each — not one per element, and not one axis asked twice.
        Assert.Equal(1, firstCalls);
        Assert.Equal(1, secondCalls);
        Assert.Equal(3, valid.Count);
        Assert.Single(errors);
    }

    /// <summary>
    /// A negative count — which the ABI should never report, but which would otherwise size
    /// a <see cref="List{T}"/> into an <see cref="ArgumentOutOfRangeException"/> — yields an
    /// empty list on that axis and leaves the other one intact.
    /// </summary>
    [Theory]
    [InlineData(-1, 2)]
    [InlineData(2, -1)]
    public async Task ANegativeCount_YieldsAnEmptyListOnThatAxisAlone(int validCount, int errorCount)
    {
        ListGroupsResult result = Walk(validCount, errorCount, (_, index) => Listing(index), (_, index) => Error(index));

        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);
        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);

        Assert.Equal(Math.Max(validCount, 0), valid.Count);
        Assert.Equal(Math.Max(errorCount, 0), errors.Count);
    }

    /// <summary>
    /// <c>All()</c> throws the <b>first</b> error — the very object <c>Errors()</c> reports
    /// first — although later errors and every successful listing are also present
    /// (<c>ListGroupsResult.java:52-53</c>).
    /// </summary>
    [Fact]
    public async Task All_ThrowsTheFirstError_WhileValidStillYieldsThePartialResults()
    {
        ListGroupsResult result = Walk(2, 3, (_, index) => Listing(index), (_, index) => Error(index));

        KafkaException thrown = await TestTimeout
            .Run(() => Assert.ThrowsAsync<KafkaException>(() => result.All()), s_deadline)
            ;

        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);
        IReadOnlyCollection<GroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);

        // The first error object itself, not a copy and not the last one.
        Assert.Same(errors.First(), thrown);
        Assert.Equal("e0", thrown.Message);

        // The partial results are still reachable, and so are the errors All() dropped.
        Assert.Equal(2, valid.Count);
        Assert.Equal(3, errors.Count);
    }

    /// <summary>
    /// With no error on the wire, <c>All()</c> yields every listing — the case a
    /// valid-count-bounded walk would also pass, which is why it cannot be the only test.
    /// </summary>
    [Fact]
    public async Task All_YieldsEveryListing_WhenNoErrorOccurred()
    {
        ListGroupsResult result = Walk(3, 0, (_, index) => Listing(index), (_, index) => Error(index));

        IReadOnlyCollection<GroupListing> all =
            await TestTimeout.Run(() => result.All(), s_deadline);

        Assert.Equal(new[] { "g0", "g1", "g2" }, all.Select(listing => listing.GroupId));
    }

    /// <summary>
    /// A throw from either reader faults the one task rather than escaping the walk with
    /// the operation left uncompleted — shape 3c has no per-key channel, so every failure
    /// is a call failure.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AReaderThatThrows_PropagatesAndLeavesTheOperationUncompleted(bool fromFirstReader)
    {
        SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation =
            new SingleAdminOperation<(IReadOnlyCollection<GroupListing>, IReadOnlyCollection<KafkaException>)>("listGroups");

        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() =>
            KeyedResultMarshal.CompleteTwoLists(
                s_root,
                _ => 2,
                _ => 2,
                operation,
                (_, index) => fromFirstReader ? throw new InvalidOperationException("boom") : Listing(index),
                (_, index) => fromFirstReader ? Error(index) : throw new InvalidOperationException("boom")));

        Assert.Equal("boom", thrown.Message);

        // The walk completed nothing; the trampoline's FailUncompleted is what rescues the
        // awaiter from hanging, so prove it is still there to do it.
        Assert.False(operation.Task.IsCompleted);
        operation.FailUncompleted();
        Assert.True(operation.Task.IsFaulted);
    }

    /// <summary>Runs the walker and hands back the public result the user would hold.</summary>
    private static ListGroupsResult Walk(
        int validCount,
        int errorCount,
        Func<IntPtr, int, GroupListing> readListing,
        Func<IntPtr, int, KafkaException> readError)
    {
        SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation =
            new SingleAdminOperation<(IReadOnlyCollection<GroupListing>, IReadOnlyCollection<KafkaException>)>("listGroups");

        KeyedResultMarshal.CompleteTwoLists(
            s_root,
            root =>
            {
                Assert.Equal(s_root, root);
                return validCount;
            },
            root =>
            {
                Assert.Equal(s_root, root);
                return errorCount;
            },
            operation,
            readListing,
            readError);

        return new ListGroupsResult(operation.Task);
    }

    /// <summary>The listing an index stands for, so a shifted walk is visible by name.</summary>
    private static GroupListing Listing(int index) =>
        new GroupListing("g" + index.ToString(CultureInfo.InvariantCulture), GroupType.Consumer, "consumer", GroupState.Stable);

    /// <summary>The error an index stands for, likewise.</summary>
    private static KafkaException Error(int index) => new KafkaException("e" + index.ToString(CultureInfo.InvariantCulture));

#pragma warning disable CS0618 // Java deprecates this RPC's result and listing types; mirrored, not avoided.

    /// <summary>
    /// ⚠ <b>The same headline claim for <c>listConsumerGroups</c>.</b> Each list comes back
    /// at <em>its own</em> length, whichever way the two counts differ.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>What this adds over its <c>listGroups</c> twin above, given they share a walker.</b>
    /// The walk itself is the same shipped code, so this is not a second proof that
    /// <see cref="KeyedResultMarshal.CompleteTwoLists"/> loops twice. What is new is the
    /// <em>wiring</em>: that <c>listConsumerGroups</c>'s completion reaches that walker with
    /// its own element type and its own public result, so the two independent lengths
    /// survive into <see cref="ListConsumerGroupsResult.Valid"/> /
    /// <see cref="ListConsumerGroupsResult.Errors"/> rather than into the other RPC's. A
    /// completion that reused the <c>listGroups</c> element reader, or bounded the second
    /// walk by the first count, changes a value asserted here.
    /// </para>
    /// <para>
    /// ⚠ The mock is no help: <c>MockAdminClient::list_consumer_groups</c> emits one listing
    /// per seeded group config and <b>never any error</b>, so a disagreeing pair of counts
    /// is unreachable end to end — the same reason the twin gives.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData(1, 3)]   // errors outnumber listings — a valid-count bound truncates them
    [InlineData(3, 1)]   // listings outnumber errors — a valid-count bound over-reads them
    [InlineData(0, 2)]   // every broker failed: no listing at all, but the errors survive
    [InlineData(2, 0)]   // the ordinary success: a valid-count bound is indistinguishable here
    [InlineData(0, 0)]   // nothing at all, and neither walk runs
    [InlineData(2, 2)]   // equal lengths: also indistinguishable, and included to say so
    public async Task ConsumerTwoLists_EachWalkItsOwnCount(int validCount, int errorCount)
    {
        List<int> listingIndices = new List<int>();
        List<int> errorIndices = new List<int>();

        ListConsumerGroupsResult result = WalkConsumer(
            validCount,
            errorCount,
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                listingIndices.Add(index);
                return ConsumerListing(index);
            },
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                errorIndices.Add(index);
                return Error(index);
            });

        IReadOnlyCollection<ConsumerGroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);
        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);

        // The lengths, independently.
        Assert.Equal(validCount, valid.Count);
        Assert.Equal(errorCount, errors.Count);

        // Each walk visited 0..n-1 of ITS OWN axis, in order and exactly once.
        Assert.Equal(Enumerable.Range(0, validCount), listingIndices);
        Assert.Equal(Enumerable.Range(0, errorCount), errorIndices);

        // And the elements landed in ABI order, so nothing was reordered or shifted.
        Assert.Equal(
            Enumerable.Range(0, validCount).Select(index => "cg" + index.ToString(CultureInfo.InvariantCulture)),
            valid.Select(listing => listing.GroupId));
        Assert.Equal(
            Enumerable.Range(0, errorCount).Select(index => "e" + index.ToString(CultureInfo.InvariantCulture)),
            errors.Select(error => error.Message));
    }

    /// <summary>
    /// The second count accessor is consulted once, on its own axis, and its own value
    /// bounds the second walk — so a result reporting fewer errors than listings cannot be
    /// over-read by a count borrowed from the first axis.
    /// </summary>
    [Fact]
    public async Task ConsumerEachCountAccessor_IsAskedForItsOwnAxis()
    {
        int firstCalls = 0;
        int secondCalls = 0;

        SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation =
            new SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing>, IReadOnlyCollection<KafkaException>)>("listConsumerGroups");

        KeyedResultMarshal.CompleteTwoLists(
            s_root,
            root =>
            {
                Assert.Equal(s_root, root);
                firstCalls++;
                return 3;
            },
            root =>
            {
                Assert.Equal(s_root, root);
                secondCalls++;
                return 1;
            },
            operation,
            (_, index) => ConsumerListing(index),
            (_, index) => Error(index));

        (IReadOnlyCollection<ConsumerGroupListing> valid, IReadOnlyCollection<KafkaException> errors) =
            await TestTimeout.Run(() => operation.Task, s_deadline);

        // One ask each — not one per element, and not one axis asked twice.
        Assert.Equal(1, firstCalls);
        Assert.Equal(1, secondCalls);
        Assert.Equal(3, valid.Count);
        Assert.Single(errors);
    }

    /// <summary>
    /// <c>All()</c> throws the <b>first</b> error — the very object <c>Errors()</c> reports
    /// first — although later errors and every successful listing are also present, and
    /// remain reachable through the other two accessors.
    /// </summary>
    [Fact]
    public async Task ConsumerAll_ThrowsTheFirstError_WhileValidStillYieldsThePartialResults()
    {
        ListConsumerGroupsResult result =
            WalkConsumer(2, 3, (_, index) => ConsumerListing(index), (_, index) => Error(index));

        KafkaException thrown = await TestTimeout
            .Run(() => Assert.ThrowsAsync<KafkaException>(() => result.All()), s_deadline);

        IReadOnlyCollection<KafkaException> errors =
            await TestTimeout.Run(() => result.Errors(), s_deadline);
        IReadOnlyCollection<ConsumerGroupListing> valid =
            await TestTimeout.Run(() => result.Valid(), s_deadline);

        // The first error object itself, not a copy and not the last one.
        Assert.Same(errors.First(), thrown);
        Assert.Equal("e0", thrown.Message);

        // The partial results are still reachable, and so are the errors All() dropped.
        Assert.Equal(2, valid.Count);
        Assert.Equal(3, errors.Count);
    }

    /// <summary>
    /// The listings the walk produced keep every field the reader set, so a walk that
    /// carried the right <em>count</em> but the wrong elements is still visible.
    /// </summary>
    /// <remarks>
    /// The two absent fields are the interesting ones: <see langword="null"/> is Java's
    /// <c>Optional.empty()</c>, and it must survive the walk as absence rather than being
    /// filled in with <see cref="GroupState.Unknown"/> / <see cref="GroupType.Unknown"/>.
    /// </remarks>
    [Fact]
    public async Task ConsumerWalk_CarriesEveryListingField()
    {
        ListConsumerGroupsResult result = WalkConsumer(
            2,
            0,
            (_, index) => index == 0
                ? new ConsumerGroupListing("cg0", GroupState.Stable, GroupType.Consumer, true)
                : new ConsumerGroupListing("cg1", null, null, false),
            (_, index) => Error(index));

        List<ConsumerGroupListing> valid =
            (await TestTimeout.Run(() => result.Valid(), s_deadline)).ToList();

        Assert.Equal("cg0", valid[0].GroupId);
        Assert.Equal(GroupState.Stable, valid[0].GroupState);
        Assert.Equal(GroupType.Consumer, valid[0].Type);
        Assert.True(valid[0].IsSimpleConsumerGroup);

        Assert.Equal("cg1", valid[1].GroupId);
        Assert.Null(valid[1].GroupState);
        Assert.Null(valid[1].Type);
        Assert.False(valid[1].IsSimpleConsumerGroup);
    }

    /// <summary>
    /// Runs the walker for the consumer shape and hands back the public result the user
    /// would hold.
    /// </summary>
    private static ListConsumerGroupsResult WalkConsumer(
        int validCount,
        int errorCount,
        Func<IntPtr, int, ConsumerGroupListing> readListing,
        Func<IntPtr, int, KafkaException> readError)
    {
        SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation =
            new SingleAdminOperation<(IReadOnlyCollection<ConsumerGroupListing>, IReadOnlyCollection<KafkaException>)>("listConsumerGroups");

        KeyedResultMarshal.CompleteTwoLists(
            s_root,
            root =>
            {
                Assert.Equal(s_root, root);
                return validCount;
            },
            root =>
            {
                Assert.Equal(s_root, root);
                return errorCount;
            },
            operation,
            readListing,
            readError);

        return new ListConsumerGroupsResult(operation.Task);
    }

    /// <summary>
    /// The consumer listing an index stands for. The ids are deliberately <b>not</b> the
    /// <c>listGroups</c> twin's, so a completion wired to the wrong element reader is
    /// visible by name rather than only by type.
    /// </summary>
    private static ConsumerGroupListing ConsumerListing(int index) =>
        new ConsumerGroupListing(
            "cg" + index.ToString(CultureInfo.InvariantCulture),
            GroupState.Stable,
            GroupType.Consumer,
            false);

#pragma warning restore CS0618

    // ------------------------------------------------------------------------------------
    // describeConsumerGroups — a different shape at the same layer.
    //
    // ⚠ A different walker: KeyedResultMarshal.Complete (shape 1), not CompleteTwoLists.
    // There is ONE count here, and index i of the key, the value and the error all name
    // the same group — so the two-list truncation defect the rest of this file is built
    // around cannot occur. The defects that CAN occur are different ones, and they are
    // what the cases below drive.
    //
    // ⚠ Why here and not end-to-end: the Rust MockAdminClient fails EVERY key with
    // `unsupported_version("Not implemented yet")` (mirroring MockAdminClient.java:735),
    // so a key that SUCCEEDS — let alone a result mixing success and failure — is
    // unreachable through the client. The all-fail path is covered end-to-end in
    // PublicAdminDescribeConsumerGroupsTests; everything that needs a described value
    // lives here.
    //
    // ⚠⚠ This section DOES touch native memory, unlike the two above. A per-key error is
    // decoded by KafkaException.FromBorrowedHandle, which reads a real
    // kafka_common_KafkaError_t, so the error cases allocate one, hand it to the walk
    // BORROWED, and destroy it themselves in a finally. That is also a blunt detector: if
    // the walker ever freed a per-key error, the test's own destroy would be a double free.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The keyed shape's whole point: one result settles some keys with a value and others
    /// with their own error, each on its own future — and <c>All()</c> faults because one
    /// did, without disturbing the sibling that succeeded.
    /// </summary>
    /// <remarks>
    /// Also pins that the value reader is <b>not</b> consulted for a key whose error is
    /// non-zero. Reading a value beside a reported error is how a result root's
    /// "exactly one of value / error" contract gets violated from the managed side.
    /// </remarks>
    [Fact]
    public async Task Describe_AMixedResult_SettlesEachKeyOnItsOwnFuture()
    {
        IntPtr error = NewError(15, "the coordinator is not available");
        try
        {
            List<int> valueReads = new List<int>();
            DescribeConsumerGroupsResult result = WalkDescribe(
                new[] { "good", "bad" },
                count: 2,
                getError: index => index == 1 ? error : IntPtr.Zero,
                readKey: index => index == 0 ? "good" : "bad",
                readValue: index =>
                {
                    valueReads.Add(index);
                    return Described("good");
                });

            Assert.Equal(new[] { 0 }, valueReads);

            Task<ConsumerGroupDescription> good = result.DescribedGroups["good"];
            Task<ConsumerGroupDescription> bad = result.DescribedGroups["bad"];

            Assert.Equal(TaskStatus.RanToCompletion, good.Status);
            Assert.Equal(TaskStatus.Faulted, bad.Status);

            // The happy key's whole surface survives the walk as owned managed state.
            ConsumerGroupDescription description = await TestTimeout.Run(() => good, s_deadline);
            Assert.Equal("good", description.GroupId);
            Assert.False(description.IsSimpleConsumerGroup);
            Assert.Equal("range", description.PartitionAssignor);
            Assert.Equal(GroupType.Consumer, description.Type);
            Assert.Equal(GroupState.Stable, description.GroupState);
            Assert.NotNull(description.Coordinator);
            Assert.Equal(7, description.Coordinator!.Id);

            MemberDescription member = Assert.Single(description.Members);
            Assert.Equal("good-m0", member.ConsumerId);
            Assert.Equal("gi-0", member.GroupInstanceId);
            Assert.Equal("rack-0", member.RackId);
            Assert.Equal("client-0", member.ClientId);
            Assert.Equal("host-0", member.Host);
            Assert.Equal(
                new[] { new TopicPartition("t", 0), new TopicPartition("t", 1) },
                member.Assignment.TopicPartitions.OrderBy(partition => partition.Partition));
            Assert.NotNull(member.TargetAssignment);
            Assert.Equal(
                new[] { new TopicPartition("t", 2) },
                member.TargetAssignment!.TopicPartitions);

            // The failing key carries ITS OWN error, decoded from the borrowed handle.
            KafkaException perKey = Assert.IsType<KafkaException>(bad.Exception!.InnerException);
            Assert.Equal(15, perKey.Code);
            Assert.Equal("the coordinator is not available", perKey.Message);

            // The aggregate follows the one failure; the good future is untouched by it.
            KafkaException fromAll = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
            Assert.Equal(15, fromAll.Code);
            Assert.Equal(TaskStatus.RanToCompletion, good.Status);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    /// <summary>
    /// <c>authorizedOperations</c> is <b>absent</b> when the broker did not report it and
    /// <b>empty</b> when it reported none — two states the ABI's count alone cannot tell
    /// apart, because both are <c>0</c>.
    /// </summary>
    /// <remarks>
    /// The discriminant is the <c>has_authorized_operations</c> boolean, so the absent case
    /// must not even consult the count. That is asserted by counting calls: a
    /// implementation that reached for the count first would still return <c>null</c> here
    /// and pass a value-only assertion.
    /// </remarks>
    [Fact]
    public void Describe_AuthorizedOperations_AbsentIsNotEmpty()
    {
        int countCalls = 0;
        int readCalls = 0;

        IReadOnlyCollection<AclOperation>? absent = AuthorizedOperationsMarshal.CopyOut(
            s_root,
            owner =>
            {
                Assert.Equal(s_root, owner);
                return false;
            },
            owner =>
            {
                countCalls++;
                return 3;
            },
            (owner, index) =>
            {
                readCalls++;
                return (int)AclOperation.Read;
            });

        Assert.Null(absent);
        Assert.Equal(0, countCalls);
        Assert.Equal(0, readCalls);

        IReadOnlyCollection<AclOperation>? reportedNone = AuthorizedOperationsMarshal.CopyOut(
            s_root,
            owner => true,
            owner => 0,
            (owner, index) => throw new InvalidOperationException("there is no element to read"));

        Assert.NotNull(reportedNone);
        Assert.Empty(reportedNone!);
    }

    /// <summary>Every reported code is decoded, in order, once each.</summary>
    [Fact]
    public void Describe_AuthorizedOperations_DecodeEveryReportedCode()
    {
        int[] codes = { (int)AclOperation.Read, (int)AclOperation.Write, (int)AclOperation.Describe };

        IReadOnlyCollection<AclOperation>? operations = AuthorizedOperationsMarshal.CopyOut(
            s_root,
            owner => true,
            owner => codes.Length,
            (owner, index) =>
            {
                Assert.Equal(s_root, owner);
                return codes[index];
            });

        Assert.Equal(
            new[] { AclOperation.Read, AclOperation.Write, AclOperation.Describe },
            operations);
    }

    /// <summary>
    /// A code outside the managed enum's range reads as <see cref="AclOperation.Unknown"/>
    /// rather than becoming an undefined enum value the caller could never switch on.
    /// </summary>
    [Theory]
    [InlineData(-5)]
    [InlineData(16)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void Describe_AuthorizedOperations_AnUnrecognisedCodeReadsAsUnknown(int code)
    {
        IReadOnlyCollection<AclOperation>? operations = AuthorizedOperationsMarshal.CopyOut(
            s_root,
            owner => true,
            owner => 1,
            (owner, index) => code);

        Assert.Equal(new[] { AclOperation.Unknown }, operations);
    }

    /// <summary>
    /// A negative count is reported as an empty set, not as a negative-capacity throw —
    /// the walk must not turn a malformed count into an exception escaping into Rust.
    /// </summary>
    [Fact]
    public void Describe_AuthorizedOperations_ANegativeCountYieldsAnEmptySet()
    {
        IReadOnlyCollection<AclOperation>? operations = AuthorizedOperationsMarshal.CopyOut(
            s_root,
            owner => true,
            owner => -3,
            (owner, index) => throw new InvalidOperationException("there is no element to read"));

        Assert.NotNull(operations);
        Assert.Empty(operations!);
    }

    /// <summary>
    /// The four nullable presence pairs survive the walk as <see langword="null"/> when
    /// absent — and <b>not</b> as <c>0</c>, <c>-1</c> or <c>false</c>, the three sentinels
    /// a <c>bool</c>-plus-out-param ABI exists to avoid.
    /// </summary>
    [Fact]
    public void Describe_AnAbsentNullable_SurvivesTheWalkAsNull_NotASentinel()
    {
        ConsumerGroupDescription description = WalkDescribeOne(
            Described(
                "g",
                groupEpoch: null,
                targetAssignmentEpoch: null,
                memberEpoch: null,
                upgraded: null));

        Assert.Null(description.GroupEpoch);
        Assert.Null(description.TargetAssignmentEpoch);

        MemberDescription member = Assert.Single(description.Members);
        Assert.Null(member.MemberEpoch);
        Assert.Null(member.Upgraded);

        // Spelled out, because "absent" collapsing onto a falsy value is the failure mode:
        // each of these would be true of a sentinel-encoded absence.
        Assert.NotEqual((int?)0, description.GroupEpoch);
        Assert.NotEqual((int?)(-1), description.GroupEpoch);
        Assert.NotEqual((int?)0, description.TargetAssignmentEpoch);
        Assert.NotEqual((int?)(-1), description.TargetAssignmentEpoch);
        Assert.NotEqual((int?)0, member.MemberEpoch);
        Assert.NotEqual((int?)(-1), member.MemberEpoch);
        Assert.NotEqual((bool?)false, member.Upgraded);
    }

    /// <summary>
    /// The other half of the pair: a <b>present</b> value that happens to be falsy stays
    /// present across the walk, so absent and present-but-falsy remain distinguishable.
    /// </summary>
    [Fact]
    public void Describe_AFalsyPresentNullable_StaysPresentAcrossTheWalk()
    {
        ConsumerGroupDescription description = WalkDescribeOne(
            Described("g", groupEpoch: 0, targetAssignmentEpoch: 0, memberEpoch: 0, upgraded: false));

        Assert.Equal(0, description.GroupEpoch);
        Assert.Equal(0, description.TargetAssignmentEpoch);

        MemberDescription member = Assert.Single(description.Members);
        Assert.Equal(0, member.MemberEpoch);
        Assert.False(member.Upgraded);

        Assert.NotNull(description.GroupEpoch);
        Assert.NotNull(description.TargetAssignmentEpoch);
        Assert.NotNull(member.MemberEpoch);
        Assert.NotNull(member.Upgraded);
    }

    /// <summary>
    /// The ABI side of the same claim, asserted structurally: each of the four optionals is
    /// a <c>bool</c> return plus an <c>out</c> parameter, never a value-returning accessor
    /// that would have to encode absence as a sentinel.
    /// </summary>
    /// <remarks>
    /// Distinct from <c>AdminNativeMethodsMarshallingTests</c>, which sweeps every
    /// <c>kafka_admin_*</c> boolean for <c>MarshalAs(I1)</c>: that pins how the bool
    /// crosses, this pins that there is a bool to cross at all.
    /// </remarks>
    [Theory]
    [InlineData("ConsumerGroupDescriptionGroupEpoch", typeof(int))]
    [InlineData("ConsumerGroupDescriptionTargetAssignmentEpoch", typeof(int))]
    [InlineData("MemberDescriptionMemberEpoch", typeof(int))]
    [InlineData("MemberDescriptionUpgraded", typeof(bool))]
    public void Describe_APresenceAccessor_IsABoolPlusAnOutParam(string method, Type valueType)
    {
        MethodInfo? accessor = typeof(NativeMethods).GetMethod(
            method,
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);

        Assert.NotNull(accessor);
        Assert.Equal(typeof(bool), accessor!.ReturnType);

        ParameterInfo[] parameters = accessor.GetParameters();
        Assert.Equal(2, parameters.Length);
        Assert.Equal(typeof(IntPtr), parameters[0].ParameterType);
        Assert.True(parameters[1].IsOut, $"{method}'s value must be an out param, not a return");
        Assert.Equal(valueType.MakeByRefType(), parameters[1].ParameterType);
    }

    /// <summary>
    /// <c>State</c> and <c>GroupState</c> cannot disagree across the marshalling path: the
    /// ABI names exactly one state, and <c>State</c> is a computed projection of whatever
    /// that name decoded to — there is no second field for it to drift from.
    /// </summary>
    /// <remarks>
    /// The value-level projection is already pinned in
    /// <c>PublicAdminConsumerGroupDescriptionTests</c>. What is added here is the path:
    /// the name is encoded, pinned as UTF-8, decoded back through
    /// <see cref="GroupMarshal.StateFromName"/> exactly as the value reader does, carried
    /// through the walk, and only then compared — plus the structural reason the two can
    /// never diverge.
    /// </remarks>
    [Fact]
    public void Describe_State_IsAProjectionOfTheGroupStateTheAbiNamed()
    {
#pragma warning disable CS0618 // The projection is deprecated in Java; mirrored, not avoided.
        // There is no ConsumerGroupState parameter to set independently, and State is
        // get-only, so no caller and no walk can give the two different origins.
        Assert.DoesNotContain(
            typeof(ConsumerGroupDescription).GetConstructors().Single().GetParameters(),
            parameter => parameter.ParameterType == typeof(ConsumerGroupState));
#pragma warning restore CS0618
        Assert.False(typeof(ConsumerGroupDescription).GetProperty("State")!.CanWrite);

        foreach (GroupState state in Enum.GetValues(typeof(GroupState)).Cast<GroupState>())
        {
            string? name = GroupMarshal.NameFromState(state);
            Assert.NotNull(name);

            using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name!);
            GroupState? decoded = GroupMarshal.StateFromName(pinned.Pointer);
            Assert.Equal(state, decoded);

            ConsumerGroupDescription description =
                WalkDescribeOne(Described("g", state: decoded!.Value));

            Assert.Equal(state, description.GroupState);
#pragma warning disable CS0618 // The projection is deprecated in Java; mirrored, not avoided.
            Assert.Equal(GroupMarshal.ParseConsumerState(name!), description.State);
#pragma warning restore CS0618
        }
    }

    /// <summary>
    /// The bridge dictionary and <see cref="DescribeConsumerGroupsResult"/>'s aggregate are
    /// keyed the same way, so two ids differing only in case stay two distinct groups all
    /// the way out.
    /// </summary>
    /// <remarks>
    /// ⚠ <see cref="DescribeConsumerGroupsResult"/> hardcodes
    /// <see cref="StringComparer.Ordinal"/> and — unlike <c>DescribeTopicsResult</c> — has
    /// a public constructor with no internal factory to thread a different comparer
    /// through. So a bridge built with any other comparer would disagree with the result it
    /// feeds, and would do so silently: a case-insensitive side collapses the two keys
    /// rather than reporting anything.
    /// </remarks>
    [Fact]
    public async Task Describe_TheBridgeAndTheAggregate_KeepCaseVariantsApart()
    {
        DescribeConsumerGroupsResult result = WalkDescribe(
            new[] { "g", "G" },
            count: 2,
            getError: index => IntPtr.Zero,
            readKey: index => index == 0 ? "g" : "G",
            readValue: index => Described(index == 0 ? "lower" : "upper"));

        Assert.Equal(2, result.DescribedGroups.Count);
        Assert.Equal(
            "lower",
            (await TestTimeout.Run(() => result.DescribedGroups["g"], s_deadline)).GroupId);
        Assert.Equal(
            "upper",
            (await TestTimeout.Run(() => result.DescribedGroups["G"], s_deadline)).GroupId);

        IReadOnlyDictionary<string, ConsumerGroupDescription> all =
            await TestTimeout.Run(result.All, s_deadline);

        Assert.Equal(2, all.Count);
        Assert.Equal("lower", all["g"].GroupId);
        Assert.Equal("upper", all["G"].GroupId);
    }

    /// <summary>
    /// Drives production's shape-1 walk over the synthetic root with injected accessors and
    /// readers, then applies the trampoline's own rescue, and hands back the public result.
    /// </summary>
    /// <param name="keys">The requested keys, as the bridge was built with them.</param>
    /// <param name="count">What the ABI's single count accessor reports.</param>
    /// <param name="getError">The per-index error pointer — <b>borrowed</b>, never freed here.</param>
    /// <param name="readKey">The per-index key reader.</param>
    /// <param name="readValue">The per-index value reader.</param>
    private static DescribeConsumerGroupsResult WalkDescribe(
        IReadOnlyCollection<string> keys,
        int count,
        Func<int, IntPtr> getError,
        Func<int, string> readKey,
        Func<int, ConsumerGroupDescription> readValue)
    {
        // ⚠ StringComparer.Ordinal, exactly as NativeAdminClient builds it. See
        // Describe_TheBridgeAndTheAggregate_KeepCaseVariantsApart for why it matters.
        KeyedAdminOperation<string, ConsumerGroupDescription> operation =
            new KeyedAdminOperation<string, ConsumerGroupDescription>(
                "describeConsumerGroups",
                keys,
                StringComparer.Ordinal);

        KeyedResultMarshal.Complete(
            s_root,
            new KeyedResultMarshal.Accessors(
                root =>
                {
                    Assert.Equal(s_root, root);
                    return count;
                },
                (root, index) =>
                {
                    Assert.Equal(s_root, root);
                    return getError(index);
                }),
            operation,
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                return readKey(index);
            },
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                return readValue(index);
            });

        // The trampoline's rescue, mirrored: any key the walk did not name must still be
        // settled or its future would hang forever.
        operation.FailUncompleted();
        return new DescribeConsumerGroupsResult(operation.Tasks);
    }

    /// <summary>
    /// The single-key case of <see cref="WalkDescribe"/>, returning the one description the
    /// walk produced.
    /// </summary>
    /// <param name="value">The description the value reader yields at index 0.</param>
    private static ConsumerGroupDescription WalkDescribeOne(ConsumerGroupDescription value)
    {
        DescribeConsumerGroupsResult result = WalkDescribe(
            new[] { "g" },
            count: 1,
            getError: index => IntPtr.Zero,
            readKey: index => "g",
            readValue: index => value);

        Task<ConsumerGroupDescription> described = result.DescribedGroups["g"];
        Assert.Equal(TaskStatus.RanToCompletion, described.Status);
        return described.Result;
    }

    /// <summary>
    /// The description an index stands for: every field distinct and non-defaulted, so a
    /// reader wired to the wrong accessor is visible by value rather than only by type.
    /// </summary>
    /// <param name="groupId">The group id, echoed into the member's consumer id.</param>
    /// <param name="state">The group state under test.</param>
    /// <param name="groupEpoch">Java's <c>groupEpoch()</c> Optional.</param>
    /// <param name="targetAssignmentEpoch">Java's <c>targetAssignmentEpoch()</c> Optional.</param>
    /// <param name="memberEpoch">The member's <c>memberEpoch()</c> Optional.</param>
    /// <param name="upgraded">The member's <c>upgraded()</c> Optional.</param>
    private static ConsumerGroupDescription Described(
        string groupId,
        GroupState state = GroupState.Stable,
        int? groupEpoch = 11,
        int? targetAssignmentEpoch = 12,
        int? memberEpoch = 13,
        bool? upgraded = true) =>
        new ConsumerGroupDescription(
            groupId,
            isSimpleConsumerGroup: false,
            new[]
            {
                new MemberDescription(
                    groupId + "-m0",
                    "gi-0",
                    "rack-0",
                    "client-0",
                    "host-0",
                    new MemberAssignment(new[] { new TopicPartition("t", 0), new TopicPartition("t", 1) }),
                    new MemberAssignment(new[] { new TopicPartition("t", 2) }),
                    memberEpoch,
                    upgraded),
            },
            "range",
            GroupType.Consumer,
            state,
            new Node(7, "broker-1", 9092, "r-1"),
            authorizedOperations: null,
            groupEpoch,
            targetAssignmentEpoch);

    /// <summary>
    /// A real <c>kafka_common_KafkaError_t</c> for the walk to read as a <b>borrowed</b>
    /// per-key error. The caller owns it and must destroy it; the walk must not.
    /// </summary>
    /// <param name="code">The protocol error code to carry.</param>
    /// <param name="message">The message to carry.</param>
    private static IntPtr NewError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    // ------------------------------------------------------------------------------------
    // describeClassicGroups — the same walker again, over a different value type.
    //
    // ⚠ Shape 1, as describeConsumerGroups above: ONE count, index i of the key, the value
    // and the error all naming the same group, KeyedResultMarshal.Complete unchanged and no
    // new walker callable. What differs is the value, and the two axes where the classic
    // description is NOT its consumer sibling:
    //
    //   - isSimpleConsumerGroup is DERIVED from protocol, where the consumer class stores
    //     it. The ABI exports a flag for it and this RPC deliberately does not declare it.
    //   - there is ONE state accessor and it is a ClassicGroupState, where the consumer
    //     class carries a GroupState plus a deprecated projection.
    //
    // ⚠ Why here and not end-to-end: the same reason the section above gives — the Rust
    // MockAdminClient fails every key, so a described classic value is unreachable through
    // the client. The per-key error cases allocate a real kafka_common_KafkaError_t, hand it
    // to the walk BORROWED, and destroy it themselves in a finally.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The keyed shape's whole point, for this RPC's value type: one result settles some
    /// keys with a description and others with their own error, each on its own future.
    /// </summary>
    /// <remarks>
    /// Also pins that the value reader is <b>not</b> consulted for a key whose error is
    /// non-zero, and that the failing key decodes <em>its own</em> borrowed error rather
    /// than the submit-level one.
    /// </remarks>
    [Fact]
    public async Task DescribeClassic_AMixedResult_SettlesEachKeyOnItsOwnFuture()
    {
        IntPtr error = NewError(15, "the coordinator is not available");
        try
        {
            List<int> valueReads = new List<int>();
            DescribeClassicGroupsResult result = WalkDescribeClassic(
                new[] { "good", "bad" },
                count: 2,
                getError: index => index == 1 ? error : IntPtr.Zero,
                readKey: index => index == 0 ? "good" : "bad",
                readValue: index =>
                {
                    valueReads.Add(index);
                    return DescribedClassic("good");
                });

            Assert.Equal(new[] { 0 }, valueReads);

            Task<ClassicGroupDescription> good = result.DescribedGroups["good"];
            Task<ClassicGroupDescription> bad = result.DescribedGroups["bad"];

            Assert.Equal(TaskStatus.RanToCompletion, good.Status);
            Assert.Equal(TaskStatus.Faulted, bad.Status);

            // The happy key's whole surface survives the walk as owned managed state.
            ClassicGroupDescription description = await TestTimeout.Run(() => good, s_deadline);
            Assert.Equal("good", description.GroupId);
            Assert.Equal("consumer", description.Protocol);
            Assert.Equal("range", description.ProtocolData);
            Assert.Equal(ClassicGroupState.Stable, description.State);
            Assert.NotNull(description.Coordinator);
            Assert.Equal(7, description.Coordinator!.Id);

            // The member type is describeConsumerGroups' — one native type, one reader.
            MemberDescription member = Assert.Single(description.Members);
            Assert.Equal("good-m0", member.ConsumerId);
            Assert.Equal("gi-0", member.GroupInstanceId);
            Assert.Equal("rack-0", member.RackId);
            Assert.Equal("client-0", member.ClientId);
            Assert.Equal("host-0", member.Host);
            Assert.Equal(
                new[] { new TopicPartition("t", 0), new TopicPartition("t", 1) },
                member.Assignment.TopicPartitions.OrderBy(partition => partition.Partition));

            // The failing key carries ITS OWN error, decoded from the borrowed handle.
            KafkaException perKey = Assert.IsType<KafkaException>(bad.Exception!.InnerException);
            Assert.Equal(15, perKey.Code);
            Assert.Equal("the coordinator is not available", perKey.Message);

            KafkaException fromAll = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
            Assert.Equal(15, fromAll.Code);
            Assert.Equal(TaskStatus.RanToCompletion, good.Status);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    /// <summary>
    /// ⚠ <b>The axis where this RPC is not its template.</b>
    /// <c>isSimpleConsumerGroup</c> is a projection of <c>protocol</c>, so the ABI's
    /// <c>..._is_simple_consumer_group</c> export must stay <b>undeclared</b> — declaring it
    /// would be dead P/Invoke offering a second, independently-settable origin for one
    /// field.
    /// </summary>
    /// <remarks>
    /// Asserted from both ends, because either alone passes a wrong implementation: the
    /// structural half would still pass if the projection were computed from a stored flag,
    /// and the value half would still pass if the declaration existed but went unused. The
    /// contrast with <see cref="ConsumerGroupDescription"/>, where Java <em>does</em> store
    /// the flag and the declaration is therefore correct, is asserted too — otherwise a
    /// blanket "no such declaration anywhere" reading of this test would be the lesson
    /// taken from it.
    /// </remarks>
    [Fact]
    public void DescribeClassic_IsSimpleConsumerGroup_IsAProjectionOfProtocol_NeverAnAbiRead()
    {
        // The ABI export exists, and this binding deliberately does not declare it.
        Assert.Null(
            typeof(NativeMethods).GetMethod(
                "ClassicGroupDescriptionIsSimpleConsumerGroup",
                BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public));

        // ⚠ Not a blanket rule: the consumer twin stores the flag in Java, so ITS
        // declaration is read and must stay.
        Assert.NotNull(
            typeof(NativeMethods).GetMethod(
                "ConsumerGroupDescriptionIsSimpleConsumerGroup",
                BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public));

        // Nothing can set the projection independently: it is get-only and no constructor
        // takes it, so protocol is its only input.
        Assert.False(typeof(ClassicGroupDescription).GetProperty("IsSimpleConsumerGroup")!.CanWrite);
        Assert.All(
            typeof(ClassicGroupDescription).GetConstructors(),
            constructor => Assert.DoesNotContain(
                constructor.GetParameters(),
                parameter => parameter.Name == "isSimpleConsumerGroup"));

        // And the projection tracks protocol through the walk, in both directions.
        Assert.True(WalkDescribeClassicOne(DescribedClassic("g", protocol: "")).IsSimpleConsumerGroup);
        Assert.False(
            WalkDescribeClassicOne(DescribedClassic("g", protocol: "consumer")).IsSimpleConsumerGroup);
    }

    /// <summary>
    /// <c>authorizedOperations</c> is <b>absent</b> when the broker did not report it and
    /// <b>empty</b> when it reported none — and on this type the choice of constructor is
    /// what preserves the difference.
    /// </summary>
    /// <remarks>
    /// ⚠ Two halves, because the ABI and the managed type can each collapse the distinction
    /// on their own. The ABI half: <c>has_authorized_operations</c>, not the count, is the
    /// gate — asserted by counting calls, since an implementation reaching for the count
    /// first would still return <see langword="null"/> and pass a value-only check. The
    /// constructor half: Java's six-argument overload forwards <c>Set.of()</c>
    /// (<c>ClassicGroupDescription.java:48</c>), so only the seven-argument form can carry
    /// absence — which is why the copy-out uses it. The <c>describeConsumerGroups</c> twin
    /// needs no such half: it has one constructor.
    /// </remarks>
    [Fact]
    public void DescribeClassic_AuthorizedOperations_AbsentIsNotEmpty()
    {
        int countCalls = 0;
        int readCalls = 0;

        // The ABI half: the boolean gate alone decides, and absence never reads the count.
        IReadOnlyCollection<AclOperation>? absent = AuthorizedOperationsMarshal.CopyOut(
            s_root,
            owner =>
            {
                Assert.Equal(s_root, owner);
                return false;
            },
            owner =>
            {
                countCalls++;
                return 3;
            },
            (owner, index) =>
            {
                readCalls++;
                return (int)AclOperation.Read;
            });

        Assert.Null(absent);
        Assert.Equal(0, countCalls);
        Assert.Equal(0, readCalls);

        // The constructor half: the six-argument overload cannot express absence, so a
        // copy-out routed through it would report "asked, none authorized" for a group that
        // was never asked.
        Assert.Empty(
            new ClassicGroupDescription("g", "consumer", "range", null, ClassicGroupState.Stable, null)
                .AuthorizedOperations!);

        // And absence survives the walk end to end, as the seven-argument form carries it.
        Assert.Null(WalkDescribeClassicOne(DescribedClassic("g")).AuthorizedOperations);
        Assert.Equal(
            new[] { AclOperation.Read },
            WalkDescribeClassicOne(
                DescribedClassic("g", authorizedOperations: new[] { AclOperation.Read }))
                .AuthorizedOperations!);
    }

    /// <summary>
    /// The state the ABI names decodes as a <see cref="ClassicGroupState"/> and survives the
    /// walk — for every member of the enum, not a sampled one.
    /// </summary>
    /// <remarks>
    /// ⚠ The decoder is <see cref="GroupMarshal.ClassicStateFromName"/>, <b>not</b> the
    /// <c>StateFromName</c> / <c>ConsumerStateFromName</c> the consumer description uses:
    /// the three enums have overlapping but non-identical members, so a value reader wired
    /// to the wrong one would still round-trip the states they share and fail only on the
    /// ones they do not.
    /// </remarks>
    [Fact]
    public void DescribeClassic_State_IsTheClassicStateTheAbiNamed()
    {
        // One state accessor on this class, and it is get-only — there is no second field
        // for a projection to drift from, unlike ConsumerGroupDescription's pair.
        Assert.False(typeof(ClassicGroupDescription).GetProperty("State")!.CanWrite);
        Assert.Null(typeof(ClassicGroupDescription).GetProperty("GroupState"));

        foreach (ClassicGroupState state in
            Enum.GetValues(typeof(ClassicGroupState)).Cast<ClassicGroupState>())
        {
            string? name = GroupMarshal.NameFromClassicState(state);
            Assert.NotNull(name);

            using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name!);
            ClassicGroupState? decoded = GroupMarshal.ClassicStateFromName(pinned.Pointer);
            Assert.Equal(state, decoded);

            Assert.Equal(
                state,
                WalkDescribeClassicOne(DescribedClassic("g", state: decoded!.Value)).State);
        }
    }

    /// <summary>
    /// Drives production's shape-1 walk over the synthetic root with injected accessors and
    /// readers, then applies the trampoline's own rescue, and hands back the public result.
    /// </summary>
    /// <param name="keys">The requested keys, as the bridge was built with them.</param>
    /// <param name="count">What the ABI's single count accessor reports.</param>
    /// <param name="getError">The per-index error pointer — <b>borrowed</b>, never freed here.</param>
    /// <param name="readKey">The per-index key reader.</param>
    /// <param name="readValue">The per-index value reader.</param>
    private static DescribeClassicGroupsResult WalkDescribeClassic(
        IReadOnlyCollection<string> keys,
        int count,
        Func<int, IntPtr> getError,
        Func<int, string> readKey,
        Func<int, ClassicGroupDescription> readValue)
    {
        // ⚠ StringComparer.Ordinal, matching what DescribeClassicGroupsResult's aggregate
        // hardcodes — the same constraint DescribeConsumerGroupsKey documents.
        KeyedAdminOperation<string, ClassicGroupDescription> operation =
            new KeyedAdminOperation<string, ClassicGroupDescription>(
                "describeClassicGroups",
                keys,
                StringComparer.Ordinal);

        KeyedResultMarshal.Complete(
            s_root,
            new KeyedResultMarshal.Accessors(
                root =>
                {
                    Assert.Equal(s_root, root);
                    return count;
                },
                (root, index) =>
                {
                    Assert.Equal(s_root, root);
                    return getError(index);
                }),
            operation,
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                return readKey(index);
            },
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                return readValue(index);
            });

        // The trampoline's rescue, mirrored: any key the walk did not name must still be
        // settled or its future would hang forever.
        operation.FailUncompleted();
        return new DescribeClassicGroupsResult(operation.Tasks);
    }

    /// <summary>
    /// The single-key case of <see cref="WalkDescribeClassic"/>, returning the one
    /// description the walk produced.
    /// </summary>
    /// <param name="value">The description the value reader yields at index 0.</param>
    private static ClassicGroupDescription WalkDescribeClassicOne(ClassicGroupDescription value)
    {
        DescribeClassicGroupsResult result = WalkDescribeClassic(
            new[] { "g" },
            count: 1,
            getError: index => IntPtr.Zero,
            readKey: index => "g",
            readValue: index => value);

        Task<ClassicGroupDescription> described = result.DescribedGroups["g"];
        Assert.Equal(TaskStatus.RanToCompletion, described.Status);
        return described.Result;
    }

    /// <summary>
    /// The classic description an index stands for: every field distinct and non-defaulted,
    /// so a reader wired to the wrong accessor is visible by value rather than only by type.
    /// </summary>
    /// <param name="groupId">The group id, echoed into the member's consumer id.</param>
    /// <param name="protocol">The protocol — also the sole input to IsSimpleConsumerGroup.</param>
    /// <param name="state">The classic group state under test.</param>
    /// <param name="authorizedOperations">Java's <c>authorizedOperations()</c>, null for absent.</param>
    private static ClassicGroupDescription DescribedClassic(
        string groupId,
        string? protocol = "consumer",
        ClassicGroupState state = ClassicGroupState.Stable,
        IEnumerable<AclOperation>? authorizedOperations = null) =>
        new ClassicGroupDescription(
            groupId,
            protocol,
            "range",
            new[]
            {
                new MemberDescription(
                    groupId + "-m0",
                    "gi-0",
                    "rack-0",
                    "client-0",
                    "host-0",
                    new MemberAssignment(new[] { new TopicPartition("t", 0), new TopicPartition("t", 1) }),
                    null,
                    null,
                    null),
            },
            state,
            new Node(7, "broker-1", 9092, "r-1"),
            authorizedOperations);

    // ------------------------------------------------------------------------------------
    // listConsumerGroupOffsets — the same keyed walker, over a value that is itself a map.
    //
    // ⚠ Shape 1 again: ONE count, index i of the key, the value and the error all naming the
    // same group, KeyedResultMarshal.Complete unchanged and no new walker callable. What is
    // new is the VALUE, and it carries the one distinction this whole RPC turns on:
    //
    //   Java's map value is NULLABLE. A requested partition the group has never committed
    //   for comes back PRESENT WITH A NULL OffsetAndMetadata — a third state, distinct both
    //   from a committed offset of 0 and from the partition being absent from the map. The
    //   ABI preserves it with a has_offset gate beside accessors that return FILLERS
    //   (-1 / null / false) in that state, and -1 is not a legal offset: OffsetAndMetadata
    //   rejects a negative one, so an implementation that reads the filler as data does not
    //   return a wrong number, it faults the whole group's future.
    //
    // ⚠ Why the map walk is driven with injected accessors: a test cannot fabricate a native
    // kafka_admin_OffsetAndMetadataMap_t, and the rule under test is "which accessor is
    // consulted, and when" — so production's own CopyOutOffsetAndMetadataMap is driven over
    // a synthetic map pointer, exactly as AuthorizedOperationsMarshal.CopyOut is above.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The keyed shape's whole point, for this RPC's value type: one result settles some
    /// groups with their committed offsets and others with their own error, each on its own
    /// future.
    /// </summary>
    /// <remarks>
    /// Also pins that the value reader is <b>not</b> consulted for a key whose error is
    /// non-zero, and that the failing key decodes <em>its own</em> borrowed error rather
    /// than the submit-level one.
    /// </remarks>
    [Fact]
    public async Task ListOffsets_AMixedResult_SettlesEachKeyOnItsOwnFuture()
    {
        IntPtr error = NewError(15, "the coordinator is not available");
        try
        {
            List<int> valueReads = new List<int>();
            ListConsumerGroupOffsetsResult result = WalkListOffsets(
                new[] { "good", "bad" },
                count: 2,
                getError: index => index == 1 ? error : IntPtr.Zero,
                readKey: index => index == 0 ? "good" : "bad",
                readValue: index =>
                {
                    valueReads.Add(index);
                    return new Dictionary<TopicPartition, OffsetAndMetadata?>
                    {
                        [new TopicPartition("t", 0)] = new OffsetAndMetadata(42, "m", 9),
                    };
                });

            Assert.Equal(new[] { 0 }, valueReads);

            Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> good =
                result.PartitionsToOffsetAndMetadata("good");
            Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> bad =
                result.PartitionsToOffsetAndMetadata("bad");

            Assert.Equal(TaskStatus.RanToCompletion, good.Status);
            Assert.Equal(TaskStatus.Faulted, bad.Status);

            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
                await TestTimeout.Run(() => good, s_deadline);
            OffsetAndMetadata committed = Assert.Single(offsets).Value!;
            Assert.Equal(42, committed.Offset);
            Assert.Equal("m", committed.Metadata);
            Assert.Equal(9, committed.LeaderEpoch);

            // The failing key carries ITS OWN error, decoded from the borrowed handle.
            KafkaException perKey = Assert.IsType<KafkaException>(bad.Exception!.InnerException);
            Assert.Equal(15, perKey.Code);
            Assert.Equal("the coordinator is not available", perKey.Message);

            KafkaException fromAll = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
            Assert.Equal(15, fromAll.Code);
            Assert.Equal(TaskStatus.RanToCompletion, good.Status);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    /// <summary>
    /// ⚠ <b>THE test for this RPC.</b> A partition the group has never committed for is
    /// <b>listed with a null value</b> — not dropped, and not given the <c>-1</c> filler.
    /// </summary>
    /// <remarks>
    /// Three entries, because each failure mode needs a different one to show up: a normal
    /// committed offset, an uncommitted partition whose fillers are exactly what the ABI
    /// documents (<c>-1</c> / null / <see langword="false"/>), and a committed offset of
    /// <c>0</c> — which is what makes "absent" and "zero" distinguishable at all. An
    /// implementation that reads the offset without the gate does not merely report a wrong
    /// number: <see cref="OffsetAndMetadata"/> rejects a negative offset, so it throws out of
    /// the walk and faults the group's whole future. One that drops the uncommitted entry
    /// collapses it into "not in the map". The accessor call counts are asserted too, so the
    /// gate is shown to be read <em>first</em> rather than merely honoured afterwards.
    /// </remarks>
    [Fact]
    public void ListOffsets_AnUncommittedPartition_IsListedWithANullValue()
    {
        List<int> gatedReads = new List<int>();

        using Utf8Marshal.PinnedUtf8String metadata = Utf8Marshal.Pin("m");
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets = CopyOutOffsets(
            count: 3,
            hasOffset: index => index != 1,
            getOffset: index =>
            {
                gatedReads.Add(index);

                // ⚠ Exactly what the ABI returns at index 1 — so a gate-less implementation
                // really does hit OffsetAndMetadata's negative-offset rejection here.
                return index switch { 0 => 42L, 1 => -1L, _ => 0L };
            },
            getMetadata: index =>
            {
                gatedReads.Add(index);
                return index == 0 ? metadata.Pointer : IntPtr.Zero;
            },
            getLeaderEpoch: index =>
            {
                gatedReads.Add(index);
                return index == 0 ? 9 : (int?)null;
            });

        Assert.Equal(3, offsets.Count);

        // The committed entry, whole.
        OffsetAndMetadata committed = offsets[new TopicPartition("t", 0)]!;
        Assert.Equal(42, committed.Offset);
        Assert.Equal("m", committed.Metadata);
        Assert.Equal(9, committed.LeaderEpoch);

        // ⚠ Present, and null. Both halves matter: ContainsKey rules out "dropped", the null
        // rules out "-1 got through" and "coerced to 0".
        Assert.True(offsets.ContainsKey(new TopicPartition("t", 1)));
        Assert.Null(offsets[new TopicPartition("t", 1)]);

        // ⚠ And zero is NOT absence: a committed 0 with no metadata and no epoch is still a
        // real OffsetAndMetadata, whose metadata is the empty string Java normalises to.
        OffsetAndMetadata zero = offsets[new TopicPartition("t", 2)]!;
        Assert.Equal(0, zero.Offset);
        Assert.Equal(string.Empty, zero.Metadata);
        Assert.Null(zero.LeaderEpoch);

        // The gate was read first: the three filler accessors were never consulted at the
        // uncommitted index, on any of their three calls.
        Assert.DoesNotContain(1, gatedReads);
        Assert.Equal(new[] { 0, 0, 0, 2, 2, 2 }, gatedReads.OrderBy(index => index));
    }

    /// <summary>
    /// The leader epoch is a <b>presence pair</b>, so it becomes <see langword="null"/> when
    /// absent — never the value the ABI happened to leave in the <c>out</c> parameter.
    /// </summary>
    /// <remarks>
    /// The fake writes a non-zero epoch <em>and</em> returns <see langword="false"/>, which
    /// is the only input that separates "honoured the flag" from "read the out-value
    /// anyway": a zero-writing fake would let the second implementation pass by coincidence,
    /// since <c>0</c> is also a plausible epoch.
    /// </remarks>
    [Fact]
    public void ListOffsets_AnAbsentLeaderEpoch_IsNullNotTheOutValue()
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            AdminCallbacks.CopyOutOffsetAndMetadataMap(
                s_root,
                new AdminCallbacks.OffsetAndMetadataMapAccessors(
                    map => 1,
                    (map, index) => IntPtr.Zero,
                    (map, index) => 7,
                    (map, index) => true,
                    (map, index) => 5L,
                    (map, index) => IntPtr.Zero,
                    OutOfBandEpoch));

        OffsetAndMetadata committed = offsets[new TopicPartition(string.Empty, 7)]!;
        Assert.Equal(5, committed.Offset);
        Assert.Null(committed.LeaderEpoch);

        // ⚠ Writes an epoch and still says "absent" — the flag alone decides.
        static bool OutOfBandEpoch(IntPtr map, int index, out int epoch)
        {
            epoch = 99;
            return false;
        }
    }

    /// <summary>
    /// A group with no committed offsets at all, and a null map, both yield an empty
    /// dictionary rather than <see langword="null"/> — the caller indexes it either way.
    /// </summary>
    /// <remarks>
    /// The null-map case also asserts the count accessor is never called, since
    /// dereferencing a null map root is exactly what it would do.
    /// </remarks>
    [Fact]
    public void ListOffsets_AnEmptyOrNullMap_IsAnEmptyDictionary()
    {
        Assert.Empty(CopyOutOffsets(count: 0, hasOffset: index => true));

        int countCalls = 0;
        Assert.Empty(
            AdminCallbacks.CopyOutOffsetAndMetadataMap(
                IntPtr.Zero,
                new AdminCallbacks.OffsetAndMetadataMapAccessors(
                    map =>
                    {
                        countCalls++;
                        return 1;
                    },
                    (map, index) => IntPtr.Zero,
                    (map, index) => 0,
                    (map, index) => true,
                    (map, index) => 0L,
                    (map, index) => IntPtr.Zero,
                    NoEpoch)));

        Assert.Equal(0, countCalls);

        static bool NoEpoch(IntPtr map, int index, out int epoch)
        {
            epoch = 0;
            return false;
        }
    }

    /// <summary>
    /// Drives production's shape-1 walk for <c>listConsumerGroupOffsets</c> over the
    /// synthetic root with injected accessors and readers, then applies the trampoline's own
    /// rescue, and hands back the public result.
    /// </summary>
    /// <param name="keys">The requested group ids, as the bridge was built with them.</param>
    /// <param name="count">What the ABI's single count accessor reports.</param>
    /// <param name="getError">The per-index error pointer — <b>borrowed</b>, never freed here.</param>
    /// <param name="readKey">The per-index key reader.</param>
    /// <param name="readValue">The per-index value reader.</param>
    private static ListConsumerGroupOffsetsResult WalkListOffsets(
        IReadOnlyCollection<string> keys,
        int count,
        Func<int, IntPtr> getError,
        Func<int, string> readKey,
        Func<int, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> readValue)
    {
        // ⚠ StringComparer.Ordinal, matching what ListConsumerGroupOffsetsResult's aggregate
        // hardcodes — and what its PartitionsToOffsetAndMetadata(groupId) lookup rejects a
        // miss on, so a comparer mismatch would surface as an ArgumentException rather than
        // a wrong offset.
        KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> operation =
            new KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                "listConsumerGroupOffsets",
                keys,
                StringComparer.Ordinal);

        KeyedResultMarshal.Complete(
            s_root,
            new KeyedResultMarshal.Accessors(
                root =>
                {
                    Assert.Equal(s_root, root);
                    return count;
                },
                (root, index) =>
                {
                    Assert.Equal(s_root, root);
                    return getError(index);
                }),
            operation,
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                return readKey(index);
            },
            (root, index) =>
            {
                Assert.Equal(s_root, root);
                return readValue(index);
            });

        operation.FailUncompleted();
        return new ListConsumerGroupOffsetsResult(operation.Tasks);
    }

    /// <summary>
    /// Drives production's <see cref="AdminCallbacks.CopyOutOffsetAndMetadataMap(IntPtr,
    /// AdminCallbacks.OffsetAndMetadataMapAccessors)"/> over the synthetic map with fake
    /// accessors. Entry <c>i</c> is topic <c>"t"</c> partition <c>i</c>, so a reader wired to
    /// the wrong index is visible by key.
    /// </summary>
    /// <param name="count">What the map's count accessor reports.</param>
    /// <param name="hasOffset">The gate, per index.</param>
    /// <param name="getOffset">The offset, per index — must not be called when the gate is false.</param>
    /// <param name="getMetadata">The metadata pointer, per index.</param>
    /// <param name="getLeaderEpoch">The epoch, per index; <see langword="null"/> for absent.</param>
    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> CopyOutOffsets(
        int count,
        Func<int, bool> hasOffset,
        Func<int, long>? getOffset = null,
        Func<int, IntPtr>? getMetadata = null,
        Func<int, int?>? getLeaderEpoch = null)
    {
        // Call-scoped, as every pin on this surface is (ffi §A4): the walk reads the topic
        // pointer before returning, so the pin never outlives the call that hands it out.
        using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin("t");

        return AdminCallbacks.CopyOutOffsetAndMetadataMap(
            s_root,
            new AdminCallbacks.OffsetAndMetadataMapAccessors(
                map =>
                {
                    Assert.Equal(s_root, map);
                    return count;
                },
                (map, index) => topic.Pointer,
                (map, index) => index,
                (map, index) => hasOffset(index),
                (map, index) => getOffset is null ? 0L : getOffset(index),
                (map, index) => getMetadata is null ? IntPtr.Zero : getMetadata(index),
                (IntPtr map, int index, out int epoch) =>
                {
                    int? present = getLeaderEpoch is null ? null : getLeaderEpoch(index);
                    epoch = present ?? 0;
                    return present.HasValue;
                }));
    }
}
