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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives the M15/P2b walkers over a <b>real</b> native <c>DeleteRecordsResult_t</c> —
/// the composite-key / inline-scalar sub-shape, which is the highest-risk surface in this
/// phase.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests own the result handle instead of going through the mock's success
/// path.</b> Two reasons, both forced. The production trampoline destroys the result root
/// in its <c>finally</c> — correctly — so it never lets a caller inspect the borrowed
/// pointers afterwards. And the Rust <c>MockAdminClient</c> has <b>no</b> success path for
/// a non-empty <c>deleteRecords</c>: it completes every key with
/// <c>unsupported_version("Not implemented yet")</c>, faithfully mirroring Java's own
/// <c>MockAdminClient.java:631-638</c>. So the test submits <c>delete_records_async</c>
/// directly with its own capturing callback, keeping the root alive and walking it with
/// <em>production's</em> readers (<c>definition-of-done.md</c> §12), then destroys it
/// exactly once.
/// </para>
/// <para>
/// ⚠ <b>The <c>-1</c> watermark is the phase's central correctness claim, and it is tested
/// as an A/B over one root.</b> The header says <c>get_low_watermark</c> returns <c>-1</c>
/// "if that partition failed … or <c>index</c> is out of range" — and <c>-1</c> is also a
/// legitimate watermark, so the number cannot be a verdict. The mock hands us a root whose
/// every entry has a non-null error <em>and</em> a <c>-1</c> watermark: exactly the
/// ambiguous input. Walking it twice — once with production's real error accessor, once
/// with the error accessor stubbed to null and <b>everything else identical</b> —
/// isolates the signal under test. An implementation that read <c>-1</c> as failure would
/// fault in both walks.
/// </para>
/// </remarks>
public sealed class AdminP2bResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The C code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws and the Rust mock
    /// translates (<c>MockAdminClient.java:631-638</c>).
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.DeleteRecordsCallback s_capture = OnCapture;

    /// <summary>
    /// ⚠ <b>THE discriminator for this phase.</b> A <c>-1</c> low watermark whose entry
    /// has a <b>null</b> error is a <b>success carrying -1</b>, not a failure.
    /// </summary>
    [Fact]
    public async Task MinusOneWatermark_WithANullError_IsASuccess_NotAFailure()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition partition = new TopicPartition("p2b-sentinel", 0);
        IntPtr result = SubmitAndCaptureResult(admin, partition);

        try
        {
            // The input really is the ambiguous one: a non-null error AND a -1 watermark
            // on the same entry. Without this the A/B below would prove nothing.
            Assert.Equal(1, NativeMethods.DeleteRecordsResultCount(result));
            Assert.NotEqual(IntPtr.Zero, NativeMethods.DeleteRecordsResultGetError(result, 0));
            Assert.Equal(-1L, NativeMethods.DeleteRecordsResultGetLowWatermark(result, 0));

            // ---- (A) production's real accessors: the error decides, so this faults ----
            KeyedAdminOperation<TopicPartition, DeletedRecords> withError = NewOperation(partition);
            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.DeleteRecordsAccessors,
                withError,
                AdminCallbacks.DeleteRecordsKey,
                AdminCallbacks.DeletedRecordsValue);

            Assert.True(withError.Tasks[partition].IsFaulted);

            // ---- (B) the SAME root, the SAME production key and value readers, with only
            // the error accessor stubbed to null. Nothing else differs, so a difference in
            // outcome can only come from the error signal. ----
            KeyedResultMarshal.Accessors noError = new KeyedResultMarshal.Accessors(
                NativeMethods.DeleteRecordsResultCount,
                static (_, _) => IntPtr.Zero);

            KeyedAdminOperation<TopicPartition, DeletedRecords> withoutError = NewOperation(partition);
            KeyedResultMarshal.Complete(
                result,
                noError,
                withoutError,
                AdminCallbacks.DeleteRecordsKey,
                AdminCallbacks.DeletedRecordsValue);

            Assert.Equal(TaskStatus.RanToCompletion, withoutError.Tasks[partition].Status);

            DeletedRecords deleted = await withoutError.Tasks[partition];
            Assert.Equal(-1L, deleted.LowWatermark);
        }
        finally
        {
            NativeMethods.DeleteRecordsResultDestroy(result);
        }
    }

    /// <summary>
    /// The per-partition error is <b>borrowed</b> from the result root: reading it leaves
    /// it alive, so the single root destroy that follows is the only free — and the
    /// message is the mock's, asserted exactly (<c>definition-of-done.md</c> §3).
    /// </summary>
    /// <remarks>
    /// ⚠ THE INJECTION POINT for <c>deleteRecords</c>. Under the injection this guards
    /// against — teaching <see cref="KafkaException.FromBorrowedHandle"/> to destroy — the
    /// re-read below becomes a use-after-free and the destroy a double free, which aborts
    /// the test host rather than failing an assertion.
    /// </remarks>
    [Fact]
    public void PerPartitionError_IsBorrowed_AndSurvivesTheWalk()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition partition = new TopicPartition("p2b-borrowed", 3);
        IntPtr result = SubmitAndCaptureResult(admin, partition);

        try
        {
            KeyedAdminOperation<TopicPartition, DeletedRecords> operation = NewOperation(partition);
            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.DeleteRecordsAccessors,
                operation,
                AdminCallbacks.DeleteRecordsKey,
                AdminCallbacks.DeletedRecordsValue);

            KafkaException failure = Assert.IsType<KafkaException>(
                operation.Tasks[partition].Exception!.InnerException);
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);

            // The borrowed pointer must still resolve to the same values after the walk.
            IntPtr borrowed = NativeMethods.DeleteRecordsResultGetError(result, 0);
            Assert.NotEqual(IntPtr.Zero, borrowed);
            Assert.Equal(UnsupportedVersionCode, NativeMethods.Code(borrowed));
            Assert.Equal(NotImplemented, Utf8Marshal.PtrToString(NativeMethods.Message(borrowed)));
        }
        finally
        {
            // The ONLY free of the borrowed error, via its owning root — exactly once.
            NativeMethods.DeleteRecordsResultDestroy(result);
        }
    }

    /// <summary>
    /// The composite key round-trips: <c>(get_topic(i), get_partition(i))</c> becomes the
    /// right <see cref="TopicPartition"/>, including two partitions of the <b>same
    /// topic</b> and the <b>same partition index</b> across two topics — the two shapes
    /// that collapse if either half of the key is dropped.
    /// </summary>
    /// <remarks>
    /// A key reader that ignored the partition would collapse <c>a-0</c> and <c>a-1</c>;
    /// one that ignored the topic would collapse <c>a-0</c> and <c>b-0</c>. Both would then
    /// surface as a "result contained no entry" fault from <c>FailUncompleted</c>, which is
    /// asserted against here.
    /// </remarks>
    [Fact]
    public void CompositeKey_DistinguishesPartitionsWithinATopic_AndTopicsAtOnePartition()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition a0 = new TopicPartition("p2b-composite-a", 0);
        TopicPartition a1 = new TopicPartition("p2b-composite-a", 1);
        TopicPartition b0 = new TopicPartition("p2b-composite-b", 0);

        IntPtr result = SubmitAndCaptureResult(admin, a0, a1, b0);

        try
        {
            Assert.Equal(3, NativeMethods.DeleteRecordsResultCount(result));

            KeyedAdminOperation<TopicPartition, DeletedRecords> operation = NewOperation(a0, a1, b0);
            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.DeleteRecordsAccessors,
                operation,
                AdminCallbacks.DeleteRecordsKey,
                AdminCallbacks.DeletedRecordsValue);

            // Every requested key was accounted for by the walk itself — so none of them
            // needs FailUncompleted, which is what a collapsed key would trigger.
            foreach (TopicPartition key in new[] { a0, a1, b0 })
            {
                Assert.True(operation.Tasks[key].IsCompleted, $"{key} was not accounted for by the walk");
                Assert.Equal(
                    NotImplemented,
                    Assert.IsType<KafkaException>(operation.Tasks[key].Exception!.InnerException).Message);
            }

            // And the three keys really are three distinct dictionary entries.
            Assert.Equal(3, operation.Tasks.Count);
            Assert.Equal(3, operation.Tasks.Keys.Distinct().Count());

            // Read straight out of the root, so the assertion is about the ABI's own
            // pairing rather than about the request we made.
            HashSet<TopicPartition> fromResult = new HashSet<TopicPartition>();
            for (int index = 0; index < 3; index++)
            {
                fromResult.Add(AdminCallbacks.DeleteRecordsKey(result, index));
            }

            Assert.Equal(new HashSet<TopicPartition> { a0, a1, b0 }, fromResult);
        }
        finally
        {
            NativeMethods.DeleteRecordsResultDestroy(result);
        }
    }

    /// <summary>
    /// Result <b>shape 3</b>: a failure faults the <b>one</b> task, because there is no
    /// per-key error channel to fault into.
    /// </summary>
    /// <remarks>
    /// The submit is injected so the callback's <c>error</c> parameter — the only failure
    /// channel <c>listTopics</c> has — can be driven deterministically. The error handle is
    /// <b>owned</b>; the trampoline frees it via <see cref="KafkaException.FromHandle"/>,
    /// which is the mirror image of the borrowed per-key errors above.
    /// </remarks>
    [Fact]
    public async Task ListTopics_AggregateFailure_FaultsTheOneTask()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr capturedUserData = IntPtr.Zero;
        ListTopicsResult result = admin.ListTopics(
            options: null,
            (nativeHandle, timeoutMs, listInternal, callback, userData) => capturedUserData = userData);

        Assert.NotEqual(IntPtr.Zero, capturedUserData);
        Assert.False(result.NamesToListings().IsCompleted);

        AdminCallbacks.ListTopics(IntPtr.Zero, MakeError(42, "could not submit"), capturedUserData);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.NamesToListings(), s_deadline));
        Assert.Equal(42, failure.Code);
        Assert.Equal("could not submit", failure.Message);

        // The projections are derived from the same future, so they carry the same
        // failure rather than hanging or reporting an empty map.
        KafkaException fromListings = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Listings(), s_deadline));
        Assert.Equal("could not submit", fromListings.Message);

        KafkaException fromNames = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Names(), s_deadline));
        Assert.Equal("could not submit", fromNames.Message);
    }

    /// <summary>
    /// Result shape 3 must not hang: an awaiter nothing resolved is <b>faulted</b> by the
    /// trampoline's <c>finally</c>, exactly as the keyed bridge faults a key the result did
    /// not account for.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Driven directly rather than through the trampoline, and that is not a
    /// shortcut.</b> The only way to reach the trampoline with nothing resolved would be a
    /// callback carrying <em>neither</em> a result nor an error — which the ABI excludes
    /// outright ("Exactly one of <c>result</c> / <c>error</c> is non-null", the
    /// <c>list_topics_callback_t</c> typedef). Synthesising that pair does not test a
    /// reachable path; it feeds <c>ListTopicsResult_count</c> a NULL the header forbids
    /// and <b>aborts the test host</b> — which is what happened when this test first tried
    /// it, and is exactly why an aborted run must never be read as a pass. So the sweep is
    /// exercised where it is real: on the bridge itself, the same way P1's
    /// <c>AKeyMissingFromTheResult_IsFaulted_NotLeftHanging</c> exercises its keyed twin.
    /// </remarks>
    [Fact]
    public async Task ListTopics_AnUnresolvedAwaiter_IsFaulted_NotLeftHanging()
    {
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation =
            new SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>("listTopics");

        Assert.False(operation.Task.IsCompleted);

        operation.FailUncompleted();

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => operation.Task, s_deadline));
        Assert.Equal("The listTopics call completed without delivering a result.", failure.Message);

        // Idempotent: the trampoline calls it on every path, including after a successful
        // walk, so a second call must not disturb a settled result.
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> settled =
            new SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>("listTopics");
        settled.SetResult(new Dictionary<string, TopicListing>());
        settled.FailUncompleted();
        Assert.Equal(TaskStatus.RanToCompletion, settled.Task.Status);
    }

    private static KeyedAdminOperation<TopicPartition, DeletedRecords> NewOperation(
        params TopicPartition[] partitions) =>
        new KeyedAdminOperation<TopicPartition, DeletedRecords>(
            "deleteRecords", partitions, EqualityComparer<TopicPartition>.Default);

    /// <summary>
    /// Submits <c>delete_records_async</c> directly and hands the caller the resulting
    /// <b>owned</b> result root, which the callback deliberately does not destroy — the
    /// callback owns it, and here that owner is the test.
    /// </summary>
    private static IntPtr SubmitAndCaptureResult(NativeAdminClient admin, params TopicPartition[] partitions)
    {
        Capture capture = new Capture();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(partitions.Length);
        try
        {
            IntPtr[] topics = new IntPtr[partitions.Length];
            int[] indices = new int[partitions.Length];
            long[] beforeOffsets = new long[partitions.Length];
            for (int i = 0; i < partitions.Length; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partitions[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                indices[i] = partitions[i].Partition;
                beforeOffsets[i] = 0;
            }

            NativeMethods.AdminClientDeleteRecordsAsync(
                admin.Handle.DangerousGetHandle(),
                topics,
                indices,
                beforeOffsets,
                partitions.Length,
                -1,
                s_capture,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(capture.Done.Wait(s_deadline), "the deleteRecords callback never fired");
        }
        finally
        {
            foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
            {
                topic.Dispose();
            }

            gcHandle.Free();
        }

        KafkaException? submitFailure = KafkaException.FromHandle(capture.Error);
        if (submitFailure is not null)
        {
            throw submitFailure;
        }

        Assert.NotEqual(IntPtr.Zero, capture.Result);
        return capture.Result;
    }

    /// <summary>
    /// Builds an <b>owned</b> <c>kafka_common_KafkaError_t</c> to stand in for the one
    /// native would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
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
