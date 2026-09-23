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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives M15/P4 Stage 1's walk over a <b>real</b> native result root — a
/// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> carrying both a succeeded and a
/// failed partition.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests own the result handle instead of going through the client's success
/// path.</b> The production trampoline destroys the result root in its <c>finally</c> —
/// correctly — so it never lets a caller inspect the borrowed pointers afterwards, and
/// "the per-partition error was copied out before the root died" is exactly what has to be
/// proven. So the test submits the <c>_async</c> entry point directly with its own
/// capturing callback, keeps the root alive, walks it with <em>production's</em> walker
/// and readers (<c>definition-of-done.md</c> §12), and destroys it exactly once.
/// </para>
/// <para>
/// ⚠⚠ <b>The behavioural half of the phase's swap detection is
/// <see cref="ThePerPartitionError_IsAFaultOrAValue_DependingOnlyOnTheWalkerChosen"/>.</b>
/// One root, two walks, opposite outcomes: through <c>Complete</c> (shape 2) the error
/// faults that partition's own task; through <c>CompleteAggregate</c> with the error
/// supplied as the value reader it becomes a map <em>value</em> on a successful task. That
/// is the whole difference between <c>alterPartitionReassignments</c>' Java shape and
/// <c>electLeaders</c>', and the identical ABI accessor sets say nothing about it.
/// </para>
/// <para>
/// ⚠ <b>The root is the reassignments one even where the claim is about
/// <c>electLeaders</c>, and that is forced.</b> The Rust <c>MockAdminClient</c> fails
/// <c>electLeaders</c> outright — Java's mock throws
/// <c>UnsupportedOperationException("Not implemented yet")</c> at
/// <c>MockAdminClient.java:797</c> and the core translates that faithfully — so no
/// <c>kafka_admin_ElectLeadersResult_t</c> with a per-partition error can be obtained
/// without a broker. The twin root is the closest real input there is, and it is a good
/// one precisely because its accessor set is byte-identical.
/// </para>
/// <para>
/// ⚠ <b>Exactly what the twin covers, and what it does not.</b> The value reader below is
/// <em>production's own</em>, built from
/// <see cref="AdminCallbacks.BorrowedOptionalError"/> with the twin's accessor injected —
/// so a defect in the reader <b>body</b>, including the borrowed-versus-owned direction,
/// is caught here for <c>electLeaders</c> too. It does <b>not</b> cover which accessor
/// <c>electLeaders</c>' own instance is built on (structural, in
/// <see cref="AdminP4ReaderWiringTests"/>), nor that production routes that reader to the
/// aggregate walker (at the submit seam, in
/// <see cref="AdminP4SubmitArgumentTests.ElectLeaders_RegistersTheAggregateBridge_AndItsTrampolineAgrees"/>).
/// </para>
/// <para>
/// ⚠ <b>The per-partition error is BORROWED.</b> It is read with
/// <see cref="KafkaException.FromBorrowedHandle"/> and never destroyed — it dies with the
/// root. Reaching for <see cref="KafkaException.FromHandle"/> frees it a second time when
/// the root is destroyed. <b>Measured</b>, by swapping the two here: the run does not fail
/// an assertion, it ends with "Test host process crashed" and <c>Test Run Aborted</c> — a
/// process abort no managed assertion can observe, which is why the loop in
/// <see cref="TheBorrowedError_IsReadNeverDestroyed"/> is the detector rather than an
/// <c>Assert</c>.
/// </para>
/// </remarks>
public sealed class AdminP4ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNKNOWN_TOPIC_OR_PARTITION</c>.</summary>
    private const int UnknownTopicOrPartitionCode = 3;

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.AlterPartitionReassignmentsCallback s_capture = OnCapture;

    /// <summary>
    /// ⚠⚠ <b>THE discriminator for this phase.</b> The same borrowed per-partition error,
    /// read from the same root, is a <b>fault</b> under one walker and a <b>map value</b>
    /// under the other.
    /// </summary>
    /// <remarks>
    /// A maintainer reading only the ABI accessor set would conclude that
    /// <c>electLeaders</c> and <c>alterPartitionReassignments</c> must be walked the same
    /// way — the sets are byte-identical. This test is the measurement that says the choice
    /// is observable, so it cannot be made by inspection of the header alone.
    /// </remarks>
    [Fact]
    public async Task ThePerPartitionError_IsAFaultOrAValue_DependingOnlyOnTheWalkerChosen()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition present = SeedTopic(admin, "p4-marshal-present");
        TopicPartition missing = new TopicPartition("p4-marshal-missing", 0);

        IntPtr result = SubmitAndCapture(admin, present, missing);
        try
        {
            // The input really is the mixed one: one entry failed and one did not. Without
            // this the two walks below would prove nothing.
            Assert.Equal(2, NativeMethods.AlterPartitionReassignmentsResultCount(result));
            int missingIndex = IndexOf(result, missing);
            int presentIndex = IndexOf(result, present);
            Assert.NotEqual(IntPtr.Zero, NativeMethods.AlterPartitionReassignmentsResultGetError(result, missingIndex));
            Assert.Equal(IntPtr.Zero, NativeMethods.AlterPartitionReassignmentsResultGetError(result, presentIndex));

            // ---- (A) shape 2 — production's own routing for THIS RPC: the error FAULTS
            // that partition's task, and the other partition is untouched. ----
            VoidKeyedAdminOperation<TopicPartition> perKey = new VoidKeyedAdminOperation<TopicPartition>(
                "alterPartitionReassignments",
                new[] { present, missing },
                EqualityComparer<TopicPartition>.Default);
            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.AlterPartitionReassignmentsAccessors,
                perKey,
                AdminCallbacks.AlterPartitionReassignmentsKey);

            Assert.True(await TestTimeout.Run(() => perKey.Tasks[present], s_deadline));
            KafkaException faulted = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => perKey.Tasks[missing]), s_deadline);
            Assert.Equal(UnknownTopicOrPartitionCode, faulted.Code);

            // ---- (B) the SAME root, the SAME key reader, with `get_error(i)` supplied as
            // the VALUE reader — electLeaders' routing. The task SUCCEEDS and the error is
            // an entry in its map. ----
            SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>> aggregate =
                new SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>>("electLeaders");
            KeyedResultMarshal.CompleteAggregate(
                result,
                NativeMethods.AlterPartitionReassignmentsResultCount,
                aggregate,
                AdminCallbacks.AlterPartitionReassignmentsKey,
                s_optionalError,
                EqualityComparer<TopicPartition>.Default);

            IReadOnlyDictionary<TopicPartition, KafkaException?> map =
                await TestTimeout.Run(() => aggregate.Task, s_deadline);

            Assert.Equal(2, map.Count);
            Assert.Null(map[present]);
            Assert.NotNull(map[missing]);
            Assert.Equal(UnknownTopicOrPartitionCode, map[missing]!.Code);

            // The two outcomes really are different for the same input: one task, both
            // partitions accounted for as values — versus two tasks, one of them faulted.
            Assert.Equal(faulted.Code, map[missing]!.Code);
            Assert.Equal(faulted.Message, map[missing]!.Message);
        }
        finally
        {
            NativeMethods.AlterPartitionReassignmentsResultDestroy(result);
        }
    }

    /// <summary>
    /// The per-partition error is <b>copied out</b> before the root dies: reading it after
    /// <c>AlterPartitionReassignmentsResult_destroy</c> is safe and unchanged.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This is the test that catches a lazily-held borrowed pointer, and nothing else
    /// will.</b> Both the message and the key's topic name borrow into the result root; an
    /// implementation that stored an <see cref="IntPtr"/> and read it on demand would pass
    /// every assertion made <em>before</em> the destroy.
    /// </remarks>
    [Fact]
    public async Task TheKeyAndTheError_AreCopiedOut_AndSurviveTheRootsDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition present = SeedTopic(admin, "p4-copyout-present");
        TopicPartition missing = new TopicPartition("p4-copyout-missing", 0);

        SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>> aggregate =
            new SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>>("electLeaders");

        IntPtr result = SubmitAndCapture(admin, present, missing);
        try
        {
            KeyedResultMarshal.CompleteAggregate(
                result,
                NativeMethods.AlterPartitionReassignmentsResultCount,
                aggregate,
                AdminCallbacks.AlterPartitionReassignmentsKey,
                s_optionalError,
                EqualityComparer<TopicPartition>.Default);
        }
        finally
        {
            NativeMethods.AlterPartitionReassignmentsResultDestroy(result);
        }

        // The root is gone. Everything below reads only owned managed state.
        IReadOnlyDictionary<TopicPartition, KafkaException?> map =
            await TestTimeout.Run(() => aggregate.Task, s_deadline);

        Assert.Equal(2, map.Count);
        Assert.Equal("p4-copyout-missing", missing.Topic);
        Assert.NotNull(map[missing]);
        Assert.Equal(UnknownTopicOrPartitionCode, map[missing]!.Code);
        Assert.False(string.IsNullOrEmpty(map[missing]!.Message));
        Assert.Null(map[present]);
    }

    /// <summary>
    /// Reading the borrowed per-partition error many times over one root neither frees it
    /// nor corrupts it — the borrowed handle is read, never destroyed.
    /// </summary>
    /// <remarks>
    /// A double free aborts the run rather than failing an assertion, so the loop is the
    /// detector: the message is re-read from the same borrowed pointer often enough that a
    /// freed-then-reused allocation would show up as a changed one. Measured — see the type
    /// remarks for what the swapped reader actually produces.
    /// </remarks>
    [Fact]
    public void TheBorrowedError_IsReadNeverDestroyed()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition present = SeedTopic(admin, "p4-borrow-present");
        TopicPartition missing = new TopicPartition("p4-borrow-missing", 0);

        IntPtr result = SubmitAndCapture(admin, present, missing);
        try
        {
            int missingIndex = IndexOf(result, missing);
            KafkaException first = s_optionalError(result, missingIndex)!;

            for (int i = 0; i < 200; i++)
            {
                KafkaException again = s_optionalError(result, missingIndex)!;
                Assert.Equal(first.Code, again.Code);
                Assert.Equal(first.Message, again.Message);
            }
        }
        finally
        {
            // Exactly once, and only here: the errors read above died with it.
            NativeMethods.AlterPartitionReassignmentsResultDestroy(result);
        }
    }

    /// <summary>
    /// ⚠ The <c>cancel</c> flag really crosses the P/Invoke as a packed array of one-byte
    /// C <c>bool</c>s, and it is what the core reads: cancelling the <b>second</b> entry
    /// succeeds where the identical call with that one flag cleared is rejected outright.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>This drives the cancel channel through the real ABI</b>, which
    /// <see cref="AdminP4SubmitArgumentTests"/> cannot: an injected submit never crosses the
    /// P/Invoke, so it sees the arrays but not their encoding. Both calls below send two
    /// entries whose first carries replicas and whose second carries none; <c>cancel[1]</c>
    /// is the only difference between them.
    /// With it set the core reads Java's <c>Optional.empty()</c> and the call succeeds;
    /// with it clear the core reads a present-but-empty replica list, which Java rejects,
    /// and the whole call fails.
    /// </para>
    /// <para>
    /// ⚠ <b>The flag under test is deliberately the SECOND, which is what makes this a
    /// marshalling proof as well as a semantic one.</b> A <c>bool[]</c> written as four-byte
    /// Win32 <c>BOOL</c>s puts <c>true</c> at byte 4 of the buffer, so a core reading
    /// one-byte C <c>bool</c>s finds <c>0</c> at index 1 and takes the rejected path. A
    /// single-entry version of this test could not tell the two encodings apart at all —
    /// <c>true</c> occupies byte 0 either way — which is exactly the shape of a test that
    /// only ever passes. <b>Measured</b>, by widening the declaration's <c>ArraySubType</c>
    /// to <c>UnmanagedType.Bool</c>: three tests go red — this one, the public
    /// <c>AlterPartitionReassignments_CancelSucceeds_AlongsideAReassignment</c>, and the
    /// structural sweep that guards the attribute. So the widening is observable
    /// behaviourally and not only structurally, which is what distinguishes this array from
    /// the <em>scalar</em> <c>bool</c> case M15/P2b measured as behaviourally insensitive.
    /// </para>
    /// <para>
    /// It also drives the ABI's <b>inline</b> callback path with ordinary bad input: the
    /// header names "a non-cancelled entry with no target replicas" as a trigger that fires
    /// the callback synchronously, before the entry point returns.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheCancelFlag_ReachesTheCore_AndSeparatesCancelFromAnEmptyReplicaList()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(2);
        TopicPartition first = SeedTopic(admin, "p4-cancel-abi", partitions: 2);
        TopicPartition second = new TopicPartition(first.Topic, 1);

        // ---- cancel[1] = true: Java's Optional.empty(). Its replica arrays are not read. ----
        Capture cancelled = SubmitRaw(admin, first, second, cancelSecond: true);
        Assert.Equal(IntPtr.Zero, cancelled.Error);
        Assert.NotEqual(IntPtr.Zero, cancelled.Result);
        NativeMethods.AlterPartitionReassignmentsResultDestroy(cancelled.Result);

        // ---- cancel[1] = false, everything else identical: a present-but-empty replica
        // list, which Java rejects and the ABI refuses to submit at all. ----
        Capture rejected = SubmitRaw(admin, first, second, cancelSecond: false);
        Assert.Equal(IntPtr.Zero, rejected.Result);
        Assert.NotEqual(IntPtr.Zero, rejected.Error);

        KafkaException failure = KafkaException.FromHandle(rejected.Error)!;
        Assert.Contains(
            "Cannot create a new partition reassignment without any replicas",
            failure.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠ <b>PRODUCTION's own value-reader body</b>, built from the same factory
    /// <c>AdminCallbacks.ElectLeadersOptionalError</c> is built from, with the twin
    /// result's byte-identical <c>get_error</c> accessor injected.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>It is deliberately NOT a hand-written copy of that lambda, and the difference
    /// is the whole guard (M15/P4 round 1, finding 70.2).</b> While this file carried a
    /// mirror, a defect in production's reader was invisible to it by construction —
    /// measured: swapping <see cref="KafkaException.FromBorrowedHandle"/> for
    /// <see cref="KafkaException.FromHandle"/> inside
    /// <c>AdminCallbacks.ElectLeadersOptionalError</c> left the suite at <b>1231/1231
    /// green</b>. Routed through <see cref="AdminCallbacks.BorrowedOptionalError"/>, the
    /// same swap now runs on this borrowed twin error and aborts the host.
    /// <para>
    /// ⚠ <b>What this does NOT pin, stated plainly:</b> it exercises the reader's
    /// <em>body</em>, not <c>electLeaders</c>' <em>wiring</em> — that its instance is built
    /// on <c>kafka_admin_ElectLeadersResult_get_error</c> rather than some other accessor.
    /// No <c>kafka_admin_ElectLeadersResult_t</c> is obtainable without a broker (Java's
    /// <c>MockAdminClient.electLeaders</c> throws
    /// <c>UnsupportedOperationException("Not implemented yet")</c> at
    /// <c>MockAdminClient.java:797</c>, and the core mirrors that), so the wiring is pinned
    /// structurally instead, by <see cref="AdminP4ReaderWiringTests"/>.
    /// </para>
    /// </remarks>
    private static readonly Func<IntPtr, int, KafkaException?> s_optionalError =
        AdminCallbacks.BorrowedOptionalError(NativeMethods.AlterPartitionReassignmentsResultGetError);

    private static int IndexOf(IntPtr result, TopicPartition partition)
    {
        int count = NativeMethods.AlterPartitionReassignmentsResultCount(result);
        for (int index = 0; index < count; index++)
        {
            if (AdminCallbacks.AlterPartitionReassignmentsKey(result, index).Equals(partition))
            {
                return index;
            }
        }

        Assert.Fail($"the result carried no entry for {partition}");
        return -1;
    }

    /// <summary>
    /// Creates a one-partition topic on the mock so a reassignment for it is in range, and
    /// returns its partition 0.
    /// </summary>
    private static TopicPartition SeedTopic(NativeAdminClient admin, string name, int partitions = 1)
    {
        TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(name, partitions, 1) }, options: null).All(), s_deadline)
            .GetAwaiter()
            .GetResult();
        return new TopicPartition(name, 0);
    }

    /// <summary>
    /// Submits <c>alter_partition_reassignments_async</c> directly for one succeeding and
    /// one failing partition, and hands the caller the resulting <b>owned</b> result root,
    /// which the capturing callback deliberately does not destroy — the callback owns it,
    /// and here that owner is the test.
    /// </summary>
    private static IntPtr SubmitAndCapture(NativeAdminClient admin, TopicPartition present, TopicPartition missing)
    {
        using Utf8Marshal.PinnedUtf8String presentTopic = Utf8Marshal.Pin(present.Topic);
        using Utf8Marshal.PinnedUtf8String missingTopic = Utf8Marshal.Pin(missing.Topic);

        int[] replicas = { 0 };
        GCHandle pinnedReplicas = GCHandle.Alloc(replicas, GCHandleType.Pinned);
        try
        {
            IntPtr ids = pinnedReplicas.AddrOfPinnedObject();
            Capture capture = Submit(
                userData => NativeMethods.AdminClientAlterPartitionReassignmentsAsync(
                    admin.Handle.DangerousGetHandle(),
                    new[] { presentTopic.Pointer, missingTopic.Pointer },
                    new[] { present.Partition, missing.Partition },
                    new[] { false, false },
                    new[] { ids, ids },
                    new[] { replicas.Length, replicas.Length },
                    2,
                    -1,
                    true,
                    s_capture,
                    userData));

            KafkaException? failure = KafkaException.FromHandle(capture.Error);
            if (failure is not null)
            {
                throw failure;
            }

            Assert.NotEqual(IntPtr.Zero, capture.Result);
            return capture.Result;
        }
        finally
        {
            pinnedReplicas.Free();
        }
    }

    /// <summary>
    /// Submits two entries — the first with replicas, the second with none — and chooses
    /// the second's <c>cancel</c> flag, returning whichever of result / error the callback
    /// was handed.
    /// </summary>
    private static Capture SubmitRaw(
        NativeAdminClient admin, TopicPartition first, TopicPartition second, bool cancelSecond)
    {
        using Utf8Marshal.PinnedUtf8String firstTopic = Utf8Marshal.Pin(first.Topic);
        using Utf8Marshal.PinnedUtf8String secondTopic = Utf8Marshal.Pin(second.Topic);

        int[] replicas = { 0 };
        GCHandle pinnedReplicas = GCHandle.Alloc(replicas, GCHandleType.Pinned);
        try
        {
            return Submit(
                userData => NativeMethods.AdminClientAlterPartitionReassignmentsAsync(
                    admin.Handle.DangerousGetHandle(),
                    new[] { firstTopic.Pointer, secondTopic.Pointer },
                    new[] { first.Partition, second.Partition },
                    new[] { false, cancelSecond },
                    new[] { pinnedReplicas.AddrOfPinnedObject(), IntPtr.Zero },
                    new[] { replicas.Length, 0 },
                    2,
                    -1,
                    true,
                    s_capture,
                    userData));
        }
        finally
        {
            pinnedReplicas.Free();
        }
    }

    /// <summary>
    /// Submits and waits for the capturing callback, freeing the rooting
    /// <see cref="GCHandle"/> <b>only once that callback has run</b>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The unconditional <c>finally</c> this replaces would turn a legible failure
    /// into a host abort (M15/P4 round 1, finding 70.7).</b> A callback that is merely
    /// <em>late</em> rather than absent still fires: freeing the handle on the timeout path
    /// hands the core's dispatcher a freed <see cref="GCHandle"/>, which it dereferences
    /// and writes through. Leaking one handle in a test that is already failing is the
    /// cheaper outcome, and it keeps the assertion message — which names the test — the
    /// thing the run reports. This is the same reasoning production's
    /// <c>AdminOperation.AbandonBeforeSubmit</c> is written from: free only where native
    /// provably cannot still be holding the pointer.
    /// </remarks>
    private static Capture Submit(Action<IntPtr> submit)
    {
        Capture capture = new Capture();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        bool fired = false;
        try
        {
            submit(GCHandle.ToIntPtr(gcHandle));
            fired = capture.Done.Wait(s_deadline);
        }
        finally
        {
            // Deliberately NOT freed when the callback has not fired — see the remarks.
            if (fired)
            {
                gcHandle.Free();
            }
        }

        Assert.True(fired, "the alterPartitionReassignments callback never fired");
        return capture;
    }

    private static void OnCapture(IntPtr result, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            Capture capture = (Capture)GCHandle.FromIntPtr(userData).Target!;
            capture.Result = result;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    private sealed class Capture
    {
        internal IntPtr Result;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);
    }
}
