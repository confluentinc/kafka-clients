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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The completion pump's <c>SLOT_CAPACITY</c> constant — originally M11/P3.1 slice S8a's <b>drain
/// cap</b> (§12.2), <b>MIGRATED</b> by M11/P3.2 slice S3 to the constant's new job (§3B.4): the
/// capacity of the three reused <c>get_all</c> marshalling arrays, and the bound
/// <c>ProcessGroup</c> splits an oversized group by.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why migrated rather than deleted (M11/P3.2 §6, test 22).</b> The queue now holds one entry per
/// <c>send_batch</c> <b>group</b> and a pass takes exactly one group, so "a drain of 1800 becomes
/// two passes of ≤1100" is no longer a statement about anything — but the three properties those
/// tests actually protected all survive with the constant, in the grouped shape, and each is
/// re-expressed below rather than dropped:
/// </para>
/// <list type="number">
/// <item><b>The cap still bounds a marshalling pass</b> — now as the oversized-group split
/// (<see cref="AGroupOverTheCap_IsSplitIntoMultiplePassesOfAtMostTheCap"/>). The pre-S3 form drove
/// it by queueing more than the cap; the grouped form drives it by queueing ONE group larger than
/// the cap, which is the only way a pass can be over-large now.</item>
/// <item><b>The loop never strands a known leftover</b>
/// (<see cref="SeveralGroups_ThenNoFurtherEnqueue_StillResolvesEveryCompletion"/>). The hang shape
/// is unchanged in kind — a pass takes one group, so leaving the rest behind while the signal is
/// already <c>Reset</c> hangs them forever — only the leftover is now a queued group rather than
/// a drain remainder.</item>
/// <item><b>The terminal drain is uncapped</b>
/// (<see cref="TeardownWithSeveralGroupsQueued_FaultsEverySendInEveryGroup"/>), and
/// <b>the reused arrays are read by count, never by <c>Length</c></b>
/// (<see cref="ALargeGroupThenASmallGroup_FreesOnlyTheSmallGroupsHandles"/>). Both are the same
/// properties, over groups.</item>
/// </list>
/// <para>
/// <b>Nothing was removed.</b> One assertion changed meaning rather than being dropped:
/// <c>ProcessedBatchCount >= 2</c> for a queue holding more than the cap is now
/// <c>== ceil(count / DrainCap)</c> for a single oversized group — a stronger claim, because the
/// pass count is deterministic once a pass is one group.
/// </para>
/// <para>
/// <b>A <see cref="IntPtr.Zero"/> future is a legitimate driver here.</b> The ABI's <c>get_all</c>
/// writes a null metadata and an <c>InvalidRequest</c> error for a null future rather than
/// asserting, so every enqueued send still <b>settles</b> — which is all these tests need, and it
/// lets them exercise the pump's loop with no producer and no broker at all. The
/// pre-existing gate tests use the same stand-in.
/// </para>
/// <para>
/// The assertions are on the pump's own counters, not on "the completions resolved": an
/// <em>ungrouped</em> pump also resolves every completion, so resolution alone cannot tell whether
/// a pass carries one send call's records.
/// </para>
/// </remarks>
public sealed class SendCompletionPumpDrainCapTests
{
    // Hard deadlines. The hang-regression test below fails DETERMINISTICALLY without the inner group
    // loop, so it must fail rather than hang the suite (§12.2.1).
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void AGroupOverTheCap_IsSplitIntoMultiplePassesOfAtMostTheCap()
    {
        // Migrated test 24. ONE group larger than the three marshalling arrays must become MULTIPLE
        // ProcessBatch passes, each of at most 1100 (§3B.3). Before S3 the driver was "queue more
        // than the cap"; a queue of records no longer exists, so the driver is now the only shape
        // that can produce an over-large pass — a single over-large group.
        //
        // Asserted by pass count and pass size: an unsplit group would fault every send in it
        // (Array.Clear over the arrays' length), which "the completions settled" cannot see, because
        // these stand-in sends settle as FAULTED either way.
        const int Total = SendCompletionPump.DrainCap + 700;

        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] completions = EnqueueOneGroup(pump, Total);

            TestTimeout.Run(() => Task.WaitAll(Settled(completions), s_deadline), s_deadline);

            Assert.True(
                pump.LargestProcessedBatch <= SendCompletionPump.DrainCap,
                $"a pass carried {pump.LargestProcessedBatch} completions, above the " +
                $"{SendCompletionPump.DrainCap} array capacity");
            Assert.Equal(2, pump.ProcessedBatchCount);
        }
        finally
        {
            pump.Stop();
        }
    }

    [Fact]
    public void SeveralGroups_ThenNoFurtherEnqueue_StillResolvesEveryCompletion()
    {
        // ⚠ MIGRATED TEST 25 — THE HANG REGRESSION, the single most important test of S8a (§12.2.1),
        // and its shape is unchanged by grouping: a pass takes ONE group, so a loop without the
        // inner group loop strands every group after the first DETERMINISTICALLY. Three groups
        // queued, one processed, two left, and the next iteration blocks on _signal.Wait() — reset
        // before the pass and only Set by a new Enqueue. With no further sends those two groups'
        // completions and every awaiting Task hang FOREVER. It is not a race and needs no
        // concurrency to reproduce, which is exactly why "stop enqueuing entirely" is the whole
        // point of the test.
        //
        // The hard deadline is load-bearing: without the inner loop this must FAIL, not hang the
        // suite.
        const int Groups = 3;
        const int PerGroup = 1000;

        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] completions =
                EnqueueGroups(pump, Groups, PerGroup);

            // Nothing further is enqueued from here on — no Set can arrive to rescue a stranded
            // remainder.
            TestTimeout.Run(() => Task.WaitAll(Settled(completions), s_deadline), s_deadline);

            foreach (TaskCompletionSource<RecordMetadata> completion in completions)
            {
                Assert.True(completion.Task.IsCompleted, "a completion in a later group was stranded");
            }

            Assert.Equal(Groups, pump.ProcessedBatchCount);
        }
        finally
        {
            pump.Stop();
        }
    }

    [Fact]
    public void TeardownWithSeveralGroupsQueued_FaultsEverySendInEveryGroup()
    {
        // Migrated test 26, and M11/P3.2 §6 test 14. Stop's terminal drain is deliberately UNCAPPED
        // and now runs per group (§3B.5): capping it — at one group, or at DrainCap — would strand
        // every send past the cap, TCSes never completed, awaiters hanging, future handles never
        // destroyed. The COUNT is the assertion (never a message — a faulted teardown send and an
        // accepted residual share an identical one, STATUS.md:20).
        //
        // ⚠ THE INJECTION IS WHAT MAKES THIS DETERMINISTIC, and the pre-S3 form was not. That form
        // filled a pump and immediately Stop()ed it, hoping the loop had not drained the queue
        // first — but RunLoop's INNER loop drains to empty without re-checking _stopping, so once
        // the pump wakes it takes everything and the terminal drain gets nothing. Measured against
        // the "cap it at one group" mutation, that shape failed 1 run in 3: a coin-flip guard on the
        // §12.2.2 property, i.e. no guard at all on the other two runs.
        //
        // Setting _stopping BEFORE enqueuing closes it by construction: the pump wakes on the first
        // Enqueue's Set, reads the volatile flag at its loop top, breaks, and exits — so every group
        // is still queued when Stop's terminal drain runs, on every run. There is no production
        // state that produces this (Stop sets the flag and joins in the same call), and producing it
        // IS the injected condition — the same warrant SendAccumulatorTests' reflection injections
        // carry. Now fails the mutation 3 runs in 3.
        const int Groups = 3;
        const int PerGroup = SendCompletionPump.DrainCap + 13;
        const int Total = Groups * PerGroup;

        SendCompletionPump pump = new SendCompletionPump();
        StopTheLoopWithoutDraining(pump);

        TaskCompletionSource<RecordMetadata>[] completions = EnqueueGroups(pump, Groups, PerGroup);
        pump.Stop();

        int settled = 0;
        foreach (TaskCompletionSource<RecordMetadata> completion in completions)
        {
            if (completion.Task.IsCompleted)
            {
                settled++;
            }
        }

        Assert.Equal(Total, settled);
    }

    /// <summary>
    /// Sets the pump's <c>_stopping</c> flag without joining, so the loop exits at its next top and
    /// leaves everything subsequently enqueued for <c>Stop</c>'s terminal drain.
    /// </summary>
    /// <remarks>
    /// The field is private to <see cref="SendCompletionPump"/> and no production path sets it
    /// without also joining in the same call, so this state cannot be reached from the outside —
    /// which is exactly the warrant the fixture injections in <c>SendAccumulatorTests</c> carry.
    /// Everything else in the test stays production's own primitive: the enqueue, the terminal
    /// drain and <see cref="SendCompletionPump.Stop"/> itself.
    /// </remarks>
    private static void StopTheLoopWithoutDraining(SendCompletionPump pump)
    {
        FieldInfo stopping = typeof(SendCompletionPump).GetField(
            "_stopping", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                "SendCompletionPump no longer exposes a _stopping flag — this injection needs to " +
                "be re-derived against the new shape rather than silently skipped.");

        Assert.False((bool)stopping.GetValue(pump)!, "the pump was already stopping");
        stopping.SetValue(pump, true);
    }

    [Fact]
    public void ALargeGroupThenASmallGroup_FreesOnlyTheSmallGroupsHandles()
    {
        // ⚠ MIGRATED TEST 27 (and M11/P3.2 §6 test 13) — the STALE-SLOT / DOUBLE-FREE guard for the
        // reused marshalling arrays (§12.3). A reused array keeps the previous pass's handle values
        // past the current pass's count, so a loop bounded by Length instead of count would hand
        // `destroy_all` (or the finally sweep) the PREVIOUS pass's already-freed handles — a double
        // free. S3 REWROTE exactly those loops, which is why this shape had to survive verbatim in
        // meaning.
        //
        // It needs REAL futures: with the null-future stand-in the other tests use there is nothing
        // to double-free, so the bug would pass unnoticed. A large group at exactly the array
        // capacity fills every slot, then a 2-record group must touch only its own two. A double
        // free of a FutureRecordMetadata_t corrupts the allocator, so — as with the other
        // use-after-free guards in this project — the assertion is that this completes at all, plus
        // that both groups settled.
        const int Large = SendCompletionPump.DrainCap;
        const int Small = 2;

        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] large = EnqueueRealSends(producer, pump, Large);
            TestTimeout.Run(() => Task.WaitAll(Tasks(large), s_deadline), s_deadline);

            // The arrays now hold Large live-looking values in every slot.
            TaskCompletionSource<RecordMetadata>[] small = EnqueueRealSends(producer, pump, Small);
            TestTimeout.Run(() => Task.WaitAll(Tasks(small), s_deadline), s_deadline);

            foreach (TaskCompletionSource<RecordMetadata> completion in large)
            {
                Assert.Equal(TaskStatus.RanToCompletion, completion.Task.Status);
            }

            foreach (TaskCompletionSource<RecordMetadata> completion in small)
            {
                Assert.Equal(TaskStatus.RanToCompletion, completion.Task.Status);
            }

            Assert.True(pump.LargestProcessedBatch <= SendCompletionPump.DrainCap);

            // The grouped witness the pre-S3 form could not make: two groups in, two passes out.
            // A pass that mixed them would read 1.
            Assert.Equal(2, pump.ProcessedBatchCount);
        }
        finally
        {
            pump.Stop();
        }
    }

    /// <summary>
    /// Sends <paramref name="count"/> real records through <c>send_batch</c> and hands the resulting
    /// futures to the pump as ONE group — the shape the accumulator's batch thread produces.
    /// </summary>
    private static TaskCompletionSource<RecordMetadata>[] EnqueueRealSends(
        NativeProducer producer, SendCompletionPump pump, int count)
    {
        ProducerRecordNative[] records = new ProducerRecordNative[count];
        IntPtr[] futures = new IntPtr[count];
        IntPtr[] errors = new IntPtr[count];

        using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin("pump-cap-topic");
        GCHandle valuePin = GCHandle.Alloc(new byte[] { 1, 2, 3 }, GCHandleType.Pinned);
        try
        {
            for (int i = 0; i < count; i++)
            {
                records[i].Topic = topic.Pointer;
                records[i].Partition = -1;
                records[i].Timestamp = -1;
                records[i].Key = IntPtr.Zero;
                records[i].KeyLength = -1;
                records[i].Value = valuePin.AddrOfPinnedObject();
                records[i].ValueLength = 3;
            }

            int accepted = ProducerSendBatchMarshal.SendBatch(
                producer.Handle, records, offset: 0, count: count, futures, errors);
            Assert.Equal(count, accepted);
        }
        finally
        {
            valuePin.Free();
        }

        TaskCompletionSource<RecordMetadata>[] completions = NewCompletions(count);
        pump.Enqueue(futures, completions, new DeliveryRegistration?[count], count);
        return completions;
    }

    private static Task[] Tasks(TaskCompletionSource<RecordMetadata>[] completions)
    {
        Task[] tasks = new Task[completions.Length];
        for (int i = 0; i < completions.Length; i++)
        {
            tasks[i] = completions[i].Task;
        }

        return tasks;
    }

    /// <summary>
    /// Enqueues <paramref name="count"/> stand-in sends as a <b>single</b> group — one
    /// <c>send_batch</c> call's worth.
    /// </summary>
    private static TaskCompletionSource<RecordMetadata>[] EnqueueOneGroup(
        SendCompletionPump pump, int count)
    {
        TaskCompletionSource<RecordMetadata>[] completions = NewCompletions(count);

        // Null futures: get_all reports InvalidRequest for each, so every send settles (faulted)
        // without a producer, and destroy_all skips null entries.
        pump.Enqueue(new IntPtr[count], completions, new DeliveryRegistration?[count], count);
        return completions;
    }

    /// <summary>
    /// Enqueues <paramref name="groups"/> separate groups of <paramref name="perGroup"/> stand-in
    /// sends each, returning every completion in enqueue order.
    /// </summary>
    private static TaskCompletionSource<RecordMetadata>[] EnqueueGroups(
        SendCompletionPump pump, int groups, int perGroup)
    {
        TaskCompletionSource<RecordMetadata>[] all =
            new TaskCompletionSource<RecordMetadata>[groups * perGroup];

        for (int g = 0; g < groups; g++)
        {
            TaskCompletionSource<RecordMetadata>[] group = EnqueueOneGroup(pump, perGroup);
            Array.Copy(group, 0, all, g * perGroup, perGroup);
        }

        return all;
    }

    private static TaskCompletionSource<RecordMetadata>[] NewCompletions(int count)
    {
        TaskCompletionSource<RecordMetadata>[] completions = new TaskCompletionSource<RecordMetadata>[count];
        for (int i = 0; i < count; i++)
        {
            completions[i] = new TaskCompletionSource<RecordMetadata>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        }

        return completions;
    }

    /// <summary>
    /// The tasks to wait on, with their faults observed — every send here settles as FAULTED (the
    /// null-future <c>InvalidRequest</c>), so awaiting them raw would surface that as the test's own
    /// failure instead of the property under test.
    /// </summary>
    private static Task[] Settled(TaskCompletionSource<RecordMetadata>[] completions)
    {
        Task[] settled = new Task[completions.Length];
        for (int i = 0; i < completions.Length; i++)
        {
            settled[i] = completions[i].Task.ContinueWith(
                static task => _ = task.Exception,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        return settled;
    }
}
