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
/// Drives M15/P4 Stage 2's marshallers over <b>real</b> native result roots, and drives the
/// ABI's two documented rejection paths — which the managed API deliberately makes
/// unreachable.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests own the result handle.</b> The production trampoline destroys the
/// root in its <c>finally</c>, so it never lets a caller inspect the borrowed pointers
/// afterwards — and "everything was copied out before the root died" is exactly what has to
/// be proven. Each test submits the <c>_async</c> entry point directly with its own
/// capturing callback, walks the root with <em>production's</em> marshallers, and destroys
/// it exactly once.
/// </para>
/// <para>
/// ⚠⚠ <b>The leader epoch's PRESENT branch is unreachable through the mock</b> — the Rust
/// <c>MockAdminClient.list_offsets</c> builds every info with <c>None</c> — so it is
/// reached through <see cref="ListOffsetsResultInfoMarshal.CopyOut"/>'s injectable presence
/// accessor, the same A/B accommodation M15/P3 made for the authorized-operations gate.
/// The case that matters most is a <b>present epoch of <c>-1</c></b>: it is the one input
/// that a "negative means absent" implementation gets wrong while passing every other test.
/// </para>
/// <para>
/// ⚠ <b>The borrowed/owned split differs between the two RPCs.</b>
/// <c>listOffsets</c> has a <c>const</c>/borrowed per-partition <c>get_error</c> →
/// <see cref="KafkaException.FromBorrowedHandle"/>, never destroyed;
/// <c>listPartitionReassignments</c> declares no per-key error at all, so its only error is
/// the callback's own <b>owned</b> one.
/// </para>
/// </remarks>
public sealed class AdminP4Stage2ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <inheritdoc cref="s_captureOffsets"/>
    private static readonly AdminCallbacks.ListPartitionReassignmentsCallback s_captureReassignments =
        OnCaptureReassignments;

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.ListOffsetsCallback s_captureOffsets = OnCaptureOffsets;

    /// <summary>
    /// ⚠⚠ <b>THE discriminator for this stage's value decode: a leader epoch of
    /// <c>-1</c> that is PRESENT stays present.</b>
    /// </summary>
    /// <remarks>
    /// Walked as an A/B over one real root with everything but the presence accessor held
    /// identical, so a difference in outcome can only come from the accessor's <b>bool
    /// return</b>. An implementation reading "negative means absent" would return
    /// <see langword="null"/> for the first case and pass the second, which is why both
    /// halves are asserted together.
    /// </remarks>
    [Fact]
    public void LeaderEpoch_PresenceComesFromTheBoolReturn_NotFromTheValue()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition partition = SeedTopic(admin, "p4s2-epoch");

        IntPtr result = SubmitAndCaptureOffsets(admin, partition, OffsetSpec.Latest());
        try
        {
            IntPtr info = NativeMethods.ListOffsetsResultGetValue(result, 0);
            Assert.NotEqual(IntPtr.Zero, info);

            // The real accessor: the mock always reports Optional.empty().
            Assert.False(NativeMethods.ListOffsetsResultInfoLeaderEpoch(info, out int _));
            Assert.Null(ListOffsetsResultInfoMarshal.CopyOut(info)!.LeaderEpoch);

            // ---- (A) present, and NEGATIVE. The value a sentinel reading gets wrong. ----
            ListOffsetsResult.ListOffsetsResultInfo presentNegative =
                ListOffsetsResultInfoMarshal.CopyOut(info, PresenceStub(true, -1))!;
            Assert.Equal(-1, presentNegative.LeaderEpoch);

            // ---- (B) the SAME root and the SAME production accessors, with only the
            // presence stub flipped to absent — and the epoch it would have written left
            // identical, so nothing but the bool differs. ----
            ListOffsetsResult.ListOffsetsResultInfo absent =
                ListOffsetsResultInfoMarshal.CopyOut(info, PresenceStub(false, -1))!;
            Assert.Null(absent.LeaderEpoch);

            // …and an ordinary present epoch still round-trips.
            Assert.Equal(
                7, ListOffsetsResultInfoMarshal.CopyOut(info, PresenceStub(true, 7))!.LeaderEpoch);

            // The other two values come from the root either way.
            Assert.Equal(NativeMethods.ListOffsetsResultInfoOffset(info), presentNegative.Offset);
            Assert.Equal(NativeMethods.ListOffsetsResultInfoTimestamp(info), presentNegative.Timestamp);
        }
        finally
        {
            NativeMethods.ListOffsetsResultDestroy(result);
        }
    }

    /// <summary>
    /// <c>listOffsets</c>' per-partition error is <b>borrowed</b>, and its value is copied
    /// out before the root dies.
    /// </summary>
    /// <remarks>
    /// The mock supplies the mixed input for free: a <c>TimestampSpec</c> partition fails
    /// with <c>unsupported_version</c> while a <c>Latest</c> one succeeds, so one root
    /// carries both an error and a value.
    /// </remarks>
    [Fact]
    public async Task ListOffsets_TheBorrowedPerPartitionError_IsReadNeverDestroyed_AndValuesSurvive()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition good = SeedTopic(admin, "p4s2-borrow", partitions: 2);
        TopicPartition bad = new TopicPartition(good.Topic, 1);

        IntPtr result = SubmitAndCaptureOffsets(
            admin,
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [good] = OffsetSpec.Latest(),
                [bad] = OffsetSpec.ForTimestamp(1),
            });

        KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> operation =
            new KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>(
                "listOffsets", new[] { good, bad }, EqualityComparer<TopicPartition>.Default);
        try
        {
            Assert.Equal(2, NativeMethods.ListOffsetsResultCount(result));

            // Re-reading the borrowed error many times neither frees nor corrupts it; a
            // double free would abort the run rather than fail an assertion.
            int badIndex = IndexOf(result, bad);
            KafkaException first =
                KafkaException.FromBorrowedHandle(NativeMethods.ListOffsetsResultGetError(result, badIndex))!;
            for (int i = 0; i < 200; i++)
            {
                KafkaException again =
                    KafkaException.FromBorrowedHandle(
                        NativeMethods.ListOffsetsResultGetError(result, badIndex))!;
                Assert.Equal(first.Code, again.Code);
                Assert.Equal(first.Message, again.Message);
            }

            Assert.Equal(UnsupportedVersionCode, first.Code);

            // Walk with production's accessors and readers.
            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.ListOffsetsAccessors,
                operation,
                AdminCallbacks.ListOffsetsKey,
                AdminCallbacks.ListOffsetsInfoValue);
        }
        finally
        {
            NativeMethods.ListOffsetsResultDestroy(result);
        }

        // The root is gone; everything below reads owned managed state only.
        ListOffsetsResult.ListOffsetsResultInfo info =
            await TestTimeout.Run(() => operation.Tasks[good], s_deadline);
        Assert.Equal(-1, info.Offset);
        Assert.Null(info.LeaderEpoch);

        KafkaException faulted = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => operation.Tasks[bad]), s_deadline);
        Assert.Equal(UnsupportedVersionCode, faulted.Code);
    }

    /// <summary>
    /// A <see cref="PartitionReassignment"/>'s three broker lists are copied out and survive
    /// the root's destruction.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This is the test that catches a lazily-held borrowed pointer.</b> The six
    /// flattened accessors all read into the root; an implementation that stored the
    /// <see cref="IntPtr"/> and read on demand would pass every assertion made
    /// <em>before</em> the destroy.
    /// </remarks>
    [Fact]
    public async Task PartitionReassignment_ThreeListsAreCopiedOut_AndSurviveTheRootsDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(3);
        TopicPartition partition = SeedTopic(admin, "p4s2-lpr-marshal", replicationFactor: 2);

        await TestTimeout.Run(
            () => admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>
                {
                    [partition] = new NewPartitionReassignment(new[] { 0, 2 }),
                },
                options: null).All(),
            s_deadline);

        PartitionReassignment reassignment;
        IntPtr result = SubmitAndCaptureReassignments(admin, partition);
        try
        {
            Assert.Equal(1, NativeMethods.ListPartitionReassignmentsResultCount(result));
            Assert.Equal(partition, AdminCallbacks.ListPartitionReassignmentsKey(result, 0));
            reassignment = AdminCallbacks.PartitionReassignmentValue(result, 0);
        }
        finally
        {
            NativeMethods.ListPartitionReassignmentsResultDestroy(result);
        }

        // The root is gone. All three lists are owned managed state.
        Assert.NotEmpty(reassignment.Replicas);
        Assert.All(reassignment.AddingReplicas, broker => Assert.DoesNotContain(broker, reassignment.Replicas));
        Assert.All(reassignment.RemovingReplicas, broker => Assert.Contains(broker, reassignment.Replicas));

        // ⚠ The three lists are read through DIFFERENT accessor pairs, so a copy-paste that
        // pointed two of them at the same pair would make them equal. They are not.
        Assert.NotEqual(reassignment.Replicas, reassignment.AddingReplicas);
    }

    /// <summary>
    /// ⚠ The ABI <b>rejects</b> an unrecognised offset sentinel and an unknown isolation
    /// level, and fires its callback <b>inline</b> — driven here through the raw P/Invoke,
    /// because the managed surface makes both unreachable.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="OffsetSpec"/>'s closed hierarchy means every managed spec encodes to a
    /// recognised sentinel, and <c>ListOffsets</c> validates the isolation level before the
    /// P/Invoke (ffi §B5). Both guards are deliberate — and both make the ABI's own
    /// rejection unreachable from C#, so the only honest way to prove the ABI really
    /// rejects, and that the rejection arrives as an <b>owned</b> error rather than a
    /// result, is to call it directly. Same treatment Stage 1 gave the <c>cancel</c> flag.
    /// </para>
    /// <para>
    /// The control-positive is in the same test: the identical call with a <em>recognised</em>
    /// sentinel and a valid level returns a result and no error.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheAbiRejectsABadSentinelAndABadIsolationLevel_OnTheInlinePath()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition partition = SeedTopic(admin, "p4s2-reject");

        // ---- control-positive: a recognised sentinel and a valid level SUCCEED ----
        Capture ok = SubmitRawOffsets(admin, partition, isTimestamp: false, spec: -2, isolationLevel: 0);
        Assert.Equal(IntPtr.Zero, ok.Error);
        Assert.NotEqual(IntPtr.Zero, ok.Result);
        NativeMethods.ListOffsetsResultDestroy(ok.Result);

        // ---- an unrecognised sentinel with is_timestamp false is rejected ----
        Capture badSentinel =
            SubmitRawOffsets(admin, partition, isTimestamp: false, spec: -99, isolationLevel: 0);
        Assert.Equal(IntPtr.Zero, badSentinel.Result);
        Assert.NotEqual(IntPtr.Zero, badSentinel.Error);
        Assert.NotNull(KafkaException.FromHandle(badSentinel.Error));

        // …and the very same value IS accepted when flagged as a timestamp, which is the
        // flag's whole purpose.
        Capture asTimestamp =
            SubmitRawOffsets(admin, partition, isTimestamp: true, spec: -99, isolationLevel: 0);
        Assert.Equal(IntPtr.Zero, asTimestamp.Error);
        Assert.NotEqual(IntPtr.Zero, asTimestamp.Result);
        NativeMethods.ListOffsetsResultDestroy(asTimestamp.Result);

        // ---- an unknown isolation level is rejected ----
        Capture badLevel =
            SubmitRawOffsets(admin, partition, isTimestamp: false, spec: -2, isolationLevel: 7);
        Assert.Equal(IntPtr.Zero, badLevel.Result);
        Assert.NotEqual(IntPtr.Zero, badLevel.Error);
        Assert.NotNull(KafkaException.FromHandle(badLevel.Error));
    }

    private static ListOffsetsResultInfoMarshal.LeaderEpochAccessor PresenceStub(bool present, int epoch) =>
        (IntPtr info, out int written) =>
        {
            written = epoch;
            return present;
        };

    private static int IndexOf(IntPtr result, TopicPartition partition)
    {
        int count = NativeMethods.ListOffsetsResultCount(result);
        for (int index = 0; index < count; index++)
        {
            if (AdminCallbacks.ListOffsetsKey(result, index).Equals(partition))
            {
                return index;
            }
        }

        Assert.Fail($"the result carried no entry for {partition}");
        return -1;
    }

    private static TopicPartition SeedTopic(
        NativeAdminClient admin, string name, int partitions = 1, short replicationFactor = 1)
    {
        TestTimeout.Run(
            () => admin.CreateTopics(
                new[] { new NewTopic(name, partitions, replicationFactor) }, options: null).All(),
            s_deadline)
            .GetAwaiter()
            .GetResult();
        return new TopicPartition(name, 0);
    }

    private static IntPtr SubmitAndCaptureOffsets(
        NativeAdminClient admin, TopicPartition partition, OffsetSpec spec) =>
        SubmitAndCaptureOffsets(admin, new Dictionary<TopicPartition, OffsetSpec> { [partition] = spec });

    /// <summary>
    /// Submits through <b>production's</b> encoder, capturing the owned root instead of
    /// letting the trampoline destroy it.
    /// </summary>
    /// <remarks>
    /// The submit is production's <c>ListOffsets</c> with the native call left real and only
    /// the callback replaced, so the (flag, value) encoding under test is the shipped one.
    /// </remarks>
    private static IntPtr SubmitAndCaptureOffsets(
        NativeAdminClient admin, IReadOnlyDictionary<TopicPartition, OffsetSpec> request)
    {
        Capture capture = new Capture();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        try
        {
            admin.ListOffsets(
                request,
                options: null,
                (nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs,
                    isolationLevel, callback, userData) =>
                    NativeMethods.AdminClientListOffsetsAsync(
                        nativeHandle, topics, partitions, isTimestamp, specTimestamps, count, timeoutMs,
                        isolationLevel, s_captureOffsets, GCHandle.ToIntPtr(gcHandle)));

            Assert.True(capture.Done.Wait(s_deadline), "the listOffsets callback never fired");
        }
        finally
        {
            gcHandle.Free();
        }

        KafkaException? failure = KafkaException.FromHandle(capture.Error);
        if (failure is not null)
        {
            throw failure;
        }

        Assert.NotEqual(IntPtr.Zero, capture.Result);
        return capture.Result;
    }

    private static IntPtr SubmitAndCaptureReassignments(NativeAdminClient admin, TopicPartition partition)
    {
        Capture capture = new Capture();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        try
        {
            admin.ListPartitionReassignments(
                new[] { partition },
                options: null,
                (nativeHandle, allPartitions, topics, partitions2, count, timeoutMs, callback, userData) =>
                    NativeMethods.AdminClientListPartitionReassignmentsAsync(
                        nativeHandle, allPartitions, topics, partitions2, count, timeoutMs,
                        s_captureReassignments, GCHandle.ToIntPtr(gcHandle)));

            Assert.True(capture.Done.Wait(s_deadline), "the listPartitionReassignments callback never fired");
        }
        finally
        {
            gcHandle.Free();
        }

        KafkaException? failure = KafkaException.FromHandle(capture.Error);
        if (failure is not null)
        {
            throw failure;
        }

        Assert.NotEqual(IntPtr.Zero, capture.Result);
        return capture.Result;
    }

    /// <summary>
    /// Calls the ABI directly with an arbitrary (flag, value, level) triple — the only way
    /// to reach the rejections the managed guards prevent.
    /// </summary>
    private static Capture SubmitRawOffsets(
        NativeAdminClient admin, TopicPartition partition, bool isTimestamp, long spec, int isolationLevel)
    {
        using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partition.Topic);

        Capture capture = new Capture();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        bool fired = false;
        try
        {
            NativeMethods.AdminClientListOffsetsAsync(
                admin.Handle.DangerousGetHandle(),
                new[] { topic.Pointer },
                new[] { partition.Partition },
                new[] { isTimestamp },
                new[] { spec },
                1,
                -1,
                isolationLevel,
                s_captureOffsets,
                GCHandle.ToIntPtr(gcHandle));

            fired = capture.Done.Wait(s_deadline);
        }
        finally
        {
            // Freed only once the callback has run — a late callback would otherwise
            // dereference a freed handle and abort the host (M15/P4 round 1, 70.7).
            if (fired)
            {
                gcHandle.Free();
            }
        }

        Assert.True(fired, "the listOffsets callback never fired");
        return capture;
    }

    private static void OnCaptureOffsets(IntPtr result, IntPtr error, IntPtr userData) =>
        Capture.Record(result, error, userData);

    private static void OnCaptureReassignments(IntPtr result, IntPtr error, IntPtr userData) =>
        Capture.Record(result, error, userData);

    private sealed class Capture
    {
        internal IntPtr Result;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);

        /// <summary>A callback entered from native is a no-throw boundary even in a test.</summary>
        internal static void Record(IntPtr result, IntPtr error, IntPtr userData)
        {
            try
            {
                Capture capture = (Capture)GCHandle.FromIntPtr(userData).Target!;
                capture.Result = result;
                capture.Error = error;
                capture.Done.Set();
            }
            catch (Exception)
            {
                // Swallow: an escaping exception would unwind into Rust. The Wait then times
                // out and fails the test with a clear message.
            }
        }
    }
}
