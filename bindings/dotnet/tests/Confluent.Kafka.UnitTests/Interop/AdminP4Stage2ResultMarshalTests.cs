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
/// <b>Why these tests own the delivered handle.</b> The production trampoline destroys it
/// in its <c>finally</c>, so it never lets a caller inspect the borrowed pointers
/// afterwards — and "everything was copied out before it died" is exactly what has to be
/// proven. Each test submits the <c>_async</c> entry point directly with its own capturing
/// callback, reads with <em>production's</em> marshallers, and destroys exactly once.
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
/// ⚠ <b>Under M15/P9's per-key ABI <c>listOffsets</c>' error is OWNED</b> — there is no
/// result root to borrow it from, which inverts the shape-1 walk this file used to drive.
/// <c>listPartitionReassignments</c> is unchanged (shape 3): its only error is the
/// callback's own owned one.
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

        OffsetsCapture captured = Assert.Single(
            SubmitAndCaptureOffsets(admin, partition, OffsetSpec.Latest()));
        try
        {
            // Shape 4a: the per-key value IS the ListOffsetsResultInfo_t, owned outright.
            Assert.Equal(IntPtr.Zero, captured.Error);
            IntPtr info = captured.Value;
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
            NativeMethods.ListOffsetsResultInfoDestroy(captured.Value);
        }
    }

    /// <summary>
    /// <c>listOffsets</c>' per-partition error is <b>owned</b>, and its value is copied out
    /// before that value dies.
    /// </summary>
    /// <remarks>
    /// ⚠ The inverse of what this test asserted against the shape-1 walk: there is no
    /// result root, so <see cref="KafkaException.FromHandle"/> — which frees — is the only
    /// correct reader, and a <see cref="KafkaException.FromBorrowedHandle"/> would leak the
    /// error on every failing key. The mock supplies the mixed input for free: a
    /// <c>TimestampSpec</c> partition fails with <c>unsupported_version</c> while a
    /// <c>Latest</c> one succeeds, so the two callbacks differ in which slot is non-null.
    /// </remarks>
    [Fact]
    public async Task ListOffsets_ThePerKeyError_IsOwned_AndValuesSurviveTheValuesDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        TopicPartition good = SeedTopic(admin, "p4s2-owned", partitions: 2);
        TopicPartition bad = new TopicPartition(good.Topic, 1);

        IReadOnlyList<OffsetsCapture> captures = SubmitAndCaptureOffsets(
            admin,
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [good] = OffsetSpec.Latest(),
                [bad] = OffsetSpec.ForTimestamp(1),
            });
        Assert.Equal(2, captures.Count);

        KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> operation =
            new KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>(
                "listOffsets", new[] { good, bad }, EqualityComparer<TopicPartition>.Default);

        foreach (OffsetsCapture capture in captures)
        {
            TopicPartition key = new TopicPartition(capture.Topic!, capture.Partition);

            // Exactly one slot per key, and which one depends on the spec — the mixed input.
            if (key.Equals(bad))
            {
                Assert.Equal(IntPtr.Zero, capture.Value);
                Assert.NotEqual(IntPtr.Zero, capture.Error);
            }
            else
            {
                Assert.NotEqual(IntPtr.Zero, capture.Value);
                Assert.Equal(IntPtr.Zero, capture.Error);
            }

            // Production's reader and production's destroy, so the copy-out must complete
            // before the value handle dies.
            KeyedResultMarshal.CompleteKey(
                operation,
                key,
                capture.Value,
                capture.Error,
                AdminCallbacks.ListOffsetsInfoPerKeyValue,
                NativeMethods.ListOffsetsResultInfoDestroy);
        }

        // The values are gone; everything below reads owned managed state only.
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
        OffsetsCapture ok = SubmitRawOffsets(admin, partition, isTimestamp: false, spec: -2, isolationLevel: 0);
        Assert.Equal(IntPtr.Zero, ok.Error);
        Assert.NotEqual(IntPtr.Zero, ok.Value);
        NativeMethods.ListOffsetsResultInfoDestroy(ok.Value);

        // ---- an unrecognised sentinel with is_timestamp false is rejected ----
        OffsetsCapture badSentinel =
            SubmitRawOffsets(admin, partition, isTimestamp: false, spec: -99, isolationLevel: 0);
        Assert.Equal(IntPtr.Zero, badSentinel.Value);
        KafkaException rejected = KafkaException.FromHandle(badSentinel.Error)!;
        Assert.Contains("is not a ListOffsets timestamp sentinel", rejected.Message);

        // …and the very same value IS accepted as a spec when flagged as a timestamp, which
        // is the flag's whole purpose. Shape 4a leaves no result root to carry the mock's
        // own per-partition refusal, so the discriminator is now WHICH error arrives: the
        // mock's unsupported-version, not the argument rejection above.
        OffsetsCapture asTimestamp =
            SubmitRawOffsets(admin, partition, isTimestamp: true, spec: -99, isolationLevel: 0);
        Assert.Equal(IntPtr.Zero, asTimestamp.Value);
        KafkaException accepted = KafkaException.FromHandle(asTimestamp.Error)!;
        Assert.Equal(UnsupportedVersionCode, accepted.Code);
        Assert.NotEqual(rejected.Code, accepted.Code);

        // ---- an unknown isolation level is rejected ----
        OffsetsCapture badLevel =
            SubmitRawOffsets(admin, partition, isTimestamp: false, spec: -2, isolationLevel: 7);
        Assert.Equal(IntPtr.Zero, badLevel.Value);
        Assert.Contains("isolation", KafkaException.FromHandle(badLevel.Error)!.Message);
    }

    private static ListOffsetsResultInfoMarshal.LeaderEpochAccessor PresenceStub(bool present, int epoch) =>
        (IntPtr info, out int written) =>
        {
            written = epoch;
            return present;
        };

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

    private static IReadOnlyList<OffsetsCapture> SubmitAndCaptureOffsets(
        NativeAdminClient admin, TopicPartition partition, OffsetSpec spec) =>
        SubmitAndCaptureOffsets(admin, new Dictionary<TopicPartition, OffsetSpec> { [partition] = spec });

    /// <summary>
    /// Submits through <b>production's</b> encoder, capturing each owned per-key value
    /// instead of letting the trampoline destroy it.
    /// </summary>
    /// <remarks>
    /// The submit is production's <c>ListOffsets</c> with the native call left real and only
    /// the callback replaced, so the (flag, value) encoding under test is the shipped one.
    /// </remarks>
    private static IReadOnlyList<OffsetsCapture> SubmitAndCaptureOffsets(
        NativeAdminClient admin, IReadOnlyDictionary<TopicPartition, OffsetSpec> request)
    {
        OffsetsCaptureSet captures = new OffsetsCaptureSet(request.Count);
        GCHandle gcHandle = GCHandle.Alloc(captures, GCHandleType.Normal);
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

            Assert.True(captures.Done.Wait(s_deadline), "the listOffsets callbacks never fired");
        }
        finally
        {
            gcHandle.Free();
        }

        return captures.Captured;
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
    private static OffsetsCapture SubmitRawOffsets(
        NativeAdminClient admin, TopicPartition partition, bool isTimestamp, long spec, int isolationLevel)
    {
        using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partition.Topic);

        OffsetsCaptureSet captures = new OffsetsCaptureSet(1);
        GCHandle gcHandle = GCHandle.Alloc(captures, GCHandleType.Normal);
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

            fired = captures.Done.Wait(s_deadline);
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
        return Assert.Single(captures.Captured);
    }

    private static void OnCaptureOffsets(
        IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData) =>
        OffsetsCaptureSet.Record(topic, partition, value, error, userData);

    private static void OnCaptureReassignments(IntPtr result, IntPtr error, IntPtr userData) =>
        Capture.Record(result, error, userData);

    /// <summary>One shape-4a per-key callback: the key, plus the two owned slots.</summary>
    private sealed class OffsetsCapture
    {
        internal string? Topic;

        internal int Partition;

        internal IntPtr Value;

        internal IntPtr Error;
    }

    private sealed class OffsetsCaptureSet
    {
        private readonly List<OffsetsCapture> _captured = new List<OffsetsCapture>();

        private readonly int _expected;

        internal OffsetsCaptureSet(int expected) => _expected = expected;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);

        internal IReadOnlyList<OffsetsCapture> Captured
        {
            get
            {
                lock (_captured)
                {
                    return _captured.ToArray();
                }
            }
        }

        /// <summary>A callback entered from native is a no-throw boundary even in a test.</summary>
        internal static void Record(
            IntPtr topic, int partition, IntPtr value, IntPtr error, IntPtr userData)
        {
            try
            {
                OffsetsCaptureSet set = (OffsetsCaptureSet)GCHandle.FromIntPtr(userData).Target!;

                // The topic borrows for the call only, so copy it out here.
                OffsetsCapture capture = new OffsetsCapture
                {
                    Topic = KeyedResultMarshal.ReadStringKey(topic),
                    Partition = partition,
                    Value = value,
                    Error = error,
                };

                bool complete;
                lock (set._captured)
                {
                    set._captured.Add(capture);
                    complete = set._captured.Count >= set._expected;
                }

                if (complete)
                {
                    set.Done.Set();
                }
            }
            catch (Exception)
            {
                // Swallow: an escaping exception would unwind into Rust. The Wait then times
                // out and fails the test with a clear message.
            }
        }
    }

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
