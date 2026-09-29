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
/// M17/P1 S4 — the completion barrier in isolation (D4): <c>SendCompletionPump.EnqueueBarrier()</c>
/// and the barrier form of the pump's queue entry.
/// </summary>
/// <remarks>
/// <para>
/// <b>The invariant (D4), while the pump runs.</b> While the loop runs, a barrier completes only
/// after every group enqueued before it has had each of its
/// <see cref="TaskCompletionSource{TResult}"/>s set or faulted, and each delivery callback its
/// passes invoked has returned; and no <c>get_all</c> pass spans a barrier.
/// </para>
/// <para>
/// <b>Stop and the closed gate complete it without that ordering.</b> <c>Stop</c>'s drain faults
/// the sends still queued ahead of a barrier, with no <c>get_all</c> read and no delivery callback,
/// and completes the barrier with success. After <c>CloseGate</c> or <c>Stop</c>,
/// <c>EnqueueBarrier</c> queues nothing and returns a completed task, whether or not a group ahead
/// of it is still unresolved. The stop and gate tests below assert those paths.
/// </para>
/// <para>
/// <b>The order is chosen by the test.</b> The futures come from a manual
/// (<c>autoComplete: false</c>) mock, whose pending completions are FIFO:
/// <c>src/producer/mock_producer.rs</c> holds them in a <c>VecDeque</c>, a send pushes the back,
/// and <c>complete_next</c> pops the front (through <c>error_next</c>). <c>get_all</c> blocks until
/// every future it was given has resolved. So while the first queued group's records are
/// unresolved, the loop is inside that group's <c>get_all</c> or has not taken it yet, and every
/// entry queued after it is still in the queue when the loop comes back for it. The tests that
/// need a queued-behind state build it that way; none of them waits a fixed time.
/// </para>
/// <para>
/// <b>Null futures stand in where a group only has to settle</b>, as in
/// <c>SendCompletionPumpGateTests</c> and <c>SendCompletionPumpDrainCapTests</c>: <c>get_all</c>
/// reports <c>InvalidRequest</c> for each, so the group faults, and <c>destroy_all</c> skips null
/// entries.
/// </para>
/// <para>
/// <b>Holds outlast the waits.</b> A callback or continuation that blocks waits up to
/// <see cref="s_hold"/>, twice <see cref="s_deadline"/>, so a mutation that stalls the loop
/// behind one fails at the deadline, with the hold still in force. The tests that use a manual
/// mock release their holds and resolve their pending records in <c>finally</c>, then stop the
/// pump before the producer that owns the futures is disposed.
/// </para>
/// <para>
/// <b>Mutations.</b> Each test's remarks name the mutations of <c>SendCompletionPump</c> that fail
/// it; the runs are recorded in
/// <c>design/history/M17/P1-producer-transactions/gate/CP3.txt</c>.
/// </para>
/// </remarks>
public sealed class SendCompletionPumpBarrierTests
{
    private const string Topic = "pump-barrier-topic";

    // The name SendCompletionPump's constructor gives its thread.
    private const string PumpThreadName = "confluent-kafka-producer-send-pump";

    // SendCompletionPump.TeardownException's message.
    private const string TeardownMessage = "The producer was closed before the send completed.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_hold = TimeSpan.FromSeconds(60);

    // Set only while this thread is inside SendCompletionPump.Stop (StopMarkingTheThread), so a
    // continuation that reads it true ran inline, inside Stop's terminal drain.
    [ThreadStatic]
    private static bool s_insideStop;

    /// <summary>
    /// S4: on an idle pump, the barrier completes and moves neither
    /// <c>ProcessedBatchCount</c> nor <c>DrainedSendCount</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Checked twice: on a fresh pump, where both counters are 0, and again once the pump has
    /// processed one group of two sends, where they are 1 and 2. Each barrier is awaited before the
    /// counters are read.
    /// </para>
    /// <para>
    /// Mutations that fail it: counting a barrier as a pass, or as a send, fails a counter
    /// assertion. Removing the loop's barrier branch, or not waking the loop when a barrier is
    /// queued, leaves the first barrier pending, and its await fails at the deadline.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task EnqueueBarrier_OnAnIdlePump_CompletesAndLeavesBothCountersUnchanged()
    {
        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            Task first = pump.EnqueueBarrier();
            await TestTimeout.Run(() => first, s_deadline);

            Assert.Equal(TaskStatus.RanToCompletion, first.Status);
            Assert.Equal(0, pump.ProcessedBatchCount);
            Assert.Equal(0, pump.DrainedSendCount);

            TaskCompletionSource<RecordMetadata>[] group = EnqueueNullFutures(pump, 2);
            await TestTimeout.Run(() => Task.WhenAll(Settled(group)), s_deadline);
            Assert.Equal(1, pump.ProcessedBatchCount);
            Assert.Equal(2, pump.DrainedSendCount);

            Task second = pump.EnqueueBarrier();
            await TestTimeout.Run(() => second, s_deadline);

            Assert.Equal(TaskStatus.RanToCompletion, second.Status);
            Assert.Equal(1, pump.ProcessedBatchCount);
            Assert.Equal(2, pump.DrainedSendCount);
        }
        finally
        {
            TestTimeout.Run(pump.Stop, s_deadline);
        }
    }

    /// <summary>
    /// S4: behind three groups, the barrier completes only after every completion in them is set
    /// and every delivery callback they invoked has returned.
    /// </summary>
    /// <remarks>
    /// <para>
    /// G1 (two real records), G2 (two null futures, which fault) and G3 (four real records)
    /// register one <see cref="ProbeCallback"/> on every send, eight in all, and the eighth
    /// invocation — G3's last send — blocks until the test releases it. G1's records are
    /// unresolved while G2, G3 and the barrier are queued, so all three are behind G1 when the loop
    /// first comes back to the queue.
    /// </para>
    /// <para>Three assertions, each on a different point:</para>
    /// <list type="bullet">
    /// <item>the barrier is pending right after it is enqueued, with G1 unresolved;</item>
    /// <item>the barrier is pending while the eighth callback is blocked, that is, while the loop
    /// is inside a callback of a group queued before it;</item>
    /// <item>a probe — a continuation on the barrier, registered while the barrier was pending —
    /// sees all eight completions set and all eight callbacks returned when it runs.</item>
    /// </list>
    /// <para>
    /// Mutations that fail it: completing the barrier when the loop takes the group queued just
    /// before it, before that group's pass, fails the second assertion; completing it at enqueue
    /// fails the first. Removing the loop's barrier branch leaves it pending, and its await fails
    /// at the deadline. Counting a barrier as a pass, or as a send, fails a count at the end.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task EnqueueBarrier_BehindSeveralGroups_CompletesOnlyAfterTheirCompletionsAndCallbacks()
    {
        const int Callbacks = 8;

        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        ProbeCallback callback = new ProbeCallback(blockOnCall: Callbacks);
        try
        {
            TaskCompletionSource<RecordMetadata>[] g1 =
                Enqueue(pump, SendPending(producer, 2), callback);
            TaskCompletionSource<RecordMetadata>[] g2 = Enqueue(pump, new IntPtr[2], callback);
            TaskCompletionSource<RecordMetadata>[] g3 =
                Enqueue(pump, SendPending(producer, 4), callback);
            Task barrier = pump.EnqueueBarrier();
            TaskCompletionSource<RecordMetadata>[] all = Concat(g1, g2, g3);

            Assert.False(
                barrier.IsCompleted, "the barrier completed while G1's records were unresolved");

            // Registered while the barrier is pending, so it observes the state after completion.
            Task<Observation> probe = barrier.ContinueWith(
                _ => new Observation(CountCompleted(all), callback.Invoked, callback.Returned),
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);

            // Resolve G1's two records and G3's four, in send order. The loop then runs G1, G2 and
            // G3, and parks inside the eighth callback.
            CompleteNext(producer, 6);
            Assert.True(
                callback.Entered.Wait(s_deadline), "the eighth delivery callback never ran");

            Assert.False(
                barrier.IsCompleted,
                "the barrier completed while a delivery callback of a group queued before it was " +
                "still running");

            callback.Release.Set();
            await TestTimeout.Run(() => barrier, s_deadline);
            Observation seen = await TestTimeout.Run(() => probe, s_deadline);

            Assert.Equal(TaskStatus.RanToCompletion, barrier.Status);
            Assert.Equal(all.Length, seen.Completed);
            Assert.Equal(Callbacks, seen.Invoked);
            Assert.Equal(Callbacks, seen.Returned);

            AssertRanToCompletion(g1);
            AssertRanToCompletion(g3);
            foreach (TaskCompletionSource<RecordMetadata> completion in g2)
            {
                Assert.Equal(TaskStatus.Faulted, completion.Task.Status);
                Assert.IsType<KafkaException>(completion.Task.Exception!.InnerException);
            }

            // One pass per group; the barrier ran none and added no send.
            Assert.Equal(3, pump.ProcessedBatchCount);
            Assert.Equal(all.Length, pump.DrainedSendCount);
        }
        finally
        {
            callback.Release.Set();
            Cleanup(producer, pump);
        }
    }

    /// <summary>
    /// S4: the barrier is a coalescing boundary. Queued as group A, barrier, group B, it completes
    /// once A is resolved while B's records are still withheld.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One <c>get_all</c> pass over A and B together could not return while B's records are
    /// unresolved, so neither A's completions nor the barrier could be set before B is resolved. A
    /// hold group H of one record is queued first and resolved together with A, after A, the
    /// barrier and B are all queued, so the loop comes back for A with the barrier and B behind it.
    /// The assertions: the barrier and every send of H and A ran to completion while every send of
    /// B is pending. B is resolved afterwards, for cleanup, and each of the three groups then
    /// accounts for one pass.
    /// </para>
    /// <para>
    /// Mutations that fail it: a pass that carries A and B together, completing the barrier after
    /// it, leaves the barrier pending behind B's records, and its await fails at the deadline. So
    /// do completing barriers only once the queue comes back empty, and removing the loop's
    /// barrier branch. Counting a barrier as a pass fails the final count.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task EnqueueBarrier_BetweenAResolvedAndAWithheldGroup_CompletesWhileTheWithheldGroupIsPending()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] hold =
                Enqueue(pump, SendPending(producer, 1), callback: null);
            TaskCompletionSource<RecordMetadata>[] a =
                Enqueue(pump, SendPending(producer, 2), callback: null);
            Task barrier = pump.EnqueueBarrier();
            TaskCompletionSource<RecordMetadata>[] b =
                Enqueue(pump, SendPending(producer, 2), callback: null);

            // H's record, then A's two, in send order. B's two stay pending.
            CompleteNext(producer, 3);
            await TestTimeout.Run(() => barrier, s_deadline);

            Assert.Equal(TaskStatus.RanToCompletion, barrier.Status);
            AssertRanToCompletion(hold);
            AssertRanToCompletion(a);
            foreach (TaskCompletionSource<RecordMetadata> completion in b)
            {
                Assert.False(
                    completion.Task.IsCompleted,
                    "a send in B completed while its record was withheld");
            }

            CompleteNext(producer, 2);
            await TestTimeout.Run(() => Task.WhenAll(Tasks(b)), s_deadline);
            AssertRanToCompletion(b);

            Assert.Equal(3, pump.ProcessedBatchCount);
        }
        finally
        {
            Cleanup(producer, pump);
        }
    }

    /// <summary>
    /// S4, the stop path: a barrier still queued when <see cref="SendCompletionPump.Stop"/> runs
    /// completes with success in the terminal drain, after the sends queued ahead of it fault and
    /// before the sends queued after it do.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The loop is stopped without draining (the <c>_stopping</c> injection
    /// <c>SendCompletionPumpDrainCapTests</c> also uses), so a group of three null futures, the
    /// barrier and a group of two are all still queued when <c>Stop</c> drains. The assertions: the
    /// barrier is pending before <c>Stop</c> and ran to completion after it; each of the five sends
    /// faulted with the teardown message; the drain took all three entries, for a
    /// <c>DrainedSendCount</c> of 5; and no pass ran.
    /// </para>
    /// <para>
    /// <b>The order within the drain.</b> The five sends' completions are created without
    /// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>, and each has a
    /// continuation registered with <see cref="TaskContinuationOptions.ExecuteSynchronously"/>, so
    /// it runs inline where the drain faults that send and records whether the barrier had
    /// completed by then: not yet for the three ahead of it, already for the two after it. Each
    /// reading also records <see cref="s_insideStop"/>, so a continuation that did not run inline
    /// reads as <see langword="null"/> and fails the check rather than passing it.
    /// </para>
    /// <para>
    /// <c>DestroyFutures</c> is not called for the barrier, but nothing here observes that: the
    /// barrier holds no future handle, so the call would have nothing to free. A mutation that adds
    /// the call leaves every test in this class passing.
    /// </para>
    /// <para>
    /// Mutations that fail it: completing the barrier with the teardown fault instead of success
    /// (the barrier is Faulted); removing the drain's barrier branch (it stays pending); completing
    /// every queued barrier before the drain faults any group (a send ahead of it reads true);
    /// counting a barrier as a send (the count is 6); and completing it at enqueue (it is complete
    /// before <c>Stop</c>).
    /// </para>
    /// </remarks>
    [Fact]
    public async Task Stop_WithABarrierQueued_CompletesItWithSuccessBetweenTheSendsAroundIt()
    {
        SendCompletionPump pump = new SendCompletionPump();
        StopTheLoopWithoutDraining(pump);

        TaskCompletionSource<RecordMetadata>[] before =
            NewCompletions(3, TaskCreationOptions.None);
        EnqueueGroup(pump, new IntPtr[3], before, callback: null);
        Task barrier = pump.EnqueueBarrier();
        TaskCompletionSource<RecordMetadata>[] after = NewCompletions(2, TaskCreationOptions.None);
        EnqueueGroup(pump, new IntPtr[2], after, callback: null);

        Assert.False(barrier.IsCompleted, "the barrier completed before Stop drained the queue");

        Task<bool?>[] beforeReadings = BarrierStateWhenFaulted(before, barrier);
        Task<bool?>[] afterReadings = BarrierStateWhenFaulted(after, barrier);

        TestTimeout.Run(() => StopMarkingTheThread(pump), s_deadline);

        Assert.Equal(TaskStatus.RanToCompletion, barrier.Status);
        AssertTeardownFault(before);
        AssertTeardownFault(after);
        Assert.Equal(5, pump.DrainedSendCount);
        Assert.Equal(0, pump.ProcessedBatchCount);

        bool?[] ahead = await TestTimeout.Run(() => Task.WhenAll(beforeReadings), s_deadline);
        bool?[] behind = await TestTimeout.Run(() => Task.WhenAll(afterReadings), s_deadline);
        foreach (bool? reading in ahead)
        {
            Assert.Equal(false, reading);
        }

        foreach (bool? reading in behind)
        {
            Assert.Equal(true, reading);
        }
    }

    /// <summary>
    /// S4, the gate: after <see cref="SendCompletionPump.CloseGate"/>, <c>EnqueueBarrier</c>
    /// returns a task that has already run to completion, where <c>Enqueue</c> faults the group in
    /// place.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A hold group of one unresolved record is queued first, and <c>CloseGate</c> leaves the loop
    /// running, so a barrier queued here would sit behind the hold, pending, until the test
    /// resolves it at the end. The control: <c>Enqueue</c> after <c>CloseGate</c> returns with its
    /// send already faulted with the teardown message.
    /// </para>
    /// <para>
    /// Mutations that fail it: returning a faulted task where <c>Enqueue</c> faults (the task is
    /// Faulted), and removing the gate check (the barrier is queued behind the hold and is
    /// pending).
    /// </para>
    /// </remarks>
    [Fact]
    public async Task EnqueueBarrier_AfterCloseGate_ReturnsACompletedTaskWhereEnqueueFaults()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            TaskCompletionSource<RecordMetadata>[] hold =
                Enqueue(pump, SendPending(producer, 1), callback: null);

            pump.CloseGate();

            AssertTeardownFault(EnqueueNullFutures(pump, 1));

            Task barrier = pump.EnqueueBarrier();
            Assert.Equal(TaskStatus.RanToCompletion, barrier.Status);

            CompleteNext(producer, 1);
            await TestTimeout.Run(() => Task.WhenAll(Tasks(hold)), s_deadline);
            AssertRanToCompletion(hold);
        }
        finally
        {
            Cleanup(producer, pump);
        }
    }

    /// <summary>
    /// S4, stop: after <see cref="SendCompletionPump.Stop"/>, <c>EnqueueBarrier</c> returns a task
    /// that has already run to completion, where <c>Enqueue</c> faults the group in place.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The control: <c>Enqueue</c> after <c>Stop</c> returns with its send already faulted with the
    /// teardown message. <c>Stop</c> has joined the loop, so nothing could complete a queued
    /// barrier afterwards.
    /// </para>
    /// <para>
    /// Mutations that fail it: returning a faulted task where <c>Enqueue</c> faults (the task is
    /// Faulted), and removing the gate check (the barrier is queued into the stopped pump and is
    /// pending).
    /// </para>
    /// </remarks>
    [Fact]
    public void EnqueueBarrier_AfterStop_ReturnsACompletedTaskWhereEnqueueFaults()
    {
        SendCompletionPump pump = new SendCompletionPump();
        TestTimeout.Run(pump.Stop, s_deadline);

        AssertTeardownFault(EnqueueNullFutures(pump, 1));

        Task barrier = pump.EnqueueBarrier();
        Assert.Equal(TaskStatus.RanToCompletion, barrier.Status);
    }

    /// <summary>
    /// S4: a continuation on the barrier that blocks does not stall the pump — a group queued after
    /// the barrier still completes while the continuation is blocked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The continuation is registered while the barrier is pending (a hold group of one unresolved
    /// record is ahead of it) with <see cref="TaskContinuationOptions.ExecuteSynchronously"/>,
    /// which asks to run it on the thread that completes the barrier. It records its thread's name,
    /// then blocks. The assertions: that name is not the pump thread's, and the later group L's two
    /// sends run to completion while the continuation is still blocked.
    /// </para>
    /// <para>
    /// Mutations that fail it: a barrier task source built without
    /// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> runs the continuation on
    /// the pump thread, which fails the thread-name assertion. Completing the barrier at enqueue
    /// fails the pending assertion, and removing the loop's barrier branch leaves the continuation
    /// unrun.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task EnqueueBarrier_WithABlockingContinuation_DoesNotStallALaterGroup()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        ManualResetEventSlim entered = new ManualResetEventSlim(false);
        ManualResetEventSlim release = new ManualResetEventSlim(false);
        try
        {
            TaskCompletionSource<RecordMetadata>[] hold =
                Enqueue(pump, SendPending(producer, 1), callback: null);
            Task barrier = pump.EnqueueBarrier();
            Assert.False(
                barrier.IsCompleted, "the barrier completed while the hold group was unresolved");

            string? continuationThread = null;
            Task blocked = barrier.ContinueWith(
                antecedent =>
                {
                    continuationThread = Thread.CurrentThread.Name;
                    entered.Set();
                    _ = release.Wait(s_hold);
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            TaskCompletionSource<RecordMetadata>[] later =
                Enqueue(pump, SendPending(producer, 2), callback: null);

            // The hold's record, then L's two, in send order.
            CompleteNext(producer, 3);
            Assert.True(entered.Wait(s_deadline), "the barrier's continuation never ran");
            Assert.NotEqual(PumpThreadName, continuationThread);

            await TestTimeout.Run(() => Task.WhenAll(Tasks(later)), s_deadline);
            AssertRanToCompletion(hold);
            AssertRanToCompletion(later);
            Assert.False(
                blocked.IsCompleted,
                "the continuation returned before the later group was checked");

            release.Set();
            await TestTimeout.Run(() => blocked, s_deadline);
        }
        finally
        {
            release.Set();
            Cleanup(producer, pump);
        }
    }

    /// <summary>
    /// Sends <paramref name="count"/> records through <c>send_batch</c> on the manual mock and
    /// returns their futures, which stay unresolved until <see cref="CompleteNext"/> resolves them
    /// in send order.
    /// </summary>
    private static IntPtr[] SendPending(NativeProducer producer, int count)
    {
        ProducerRecordNative[] records = new ProducerRecordNative[count];
        IntPtr[] futures = new IntPtr[count];
        IntPtr[] errors = new IntPtr[count];

        using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(Topic);
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

        return futures;
    }

    /// <summary>
    /// Enqueues <paramref name="futures"/> as one group, with new completions created with
    /// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>, and with
    /// <paramref name="callback"/> registered on every send when it is not null. The pump owns the
    /// futures from here.
    /// </summary>
    private static TaskCompletionSource<RecordMetadata>[] Enqueue(
        SendCompletionPump pump, IntPtr[] futures, IDeliveryCallback? callback)
    {
        TaskCompletionSource<RecordMetadata>[] completions =
            NewCompletions(futures.Length, TaskCreationOptions.RunContinuationsAsynchronously);
        EnqueueGroup(pump, futures, completions, callback);
        return completions;
    }

    /// <summary>
    /// Enqueues <paramref name="futures"/> as one group with the given completions, with
    /// <paramref name="callback"/> registered on every send when it is not null.
    /// </summary>
    private static void EnqueueGroup(
        SendCompletionPump pump,
        IntPtr[] futures,
        TaskCompletionSource<RecordMetadata>[] completions,
        IDeliveryCallback? callback)
    {
        int count = futures.Length;
        DeliveryRegistration?[] deliveries = new DeliveryRegistration?[count];
        if (callback is not null)
        {
            for (int i = 0; i < count; i++)
            {
                deliveries[i] = new DeliveryRegistration(callback, Topic, -1);
            }
        }

        pump.Enqueue(futures, completions, deliveries, count);
    }

    /// <summary>Enqueues one group of <paramref name="count"/> null futures.</summary>
    private static TaskCompletionSource<RecordMetadata>[] EnqueueNullFutures(
        SendCompletionPump pump, int count) =>
        Enqueue(pump, new IntPtr[count], callback: null);

    private static void CompleteNext(NativeProducer producer, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.True(producer.MockCompleteNext(), $"the mock had no pending record left at {i}");
        }
    }

    /// <summary>
    /// Resolves every record the test left pending, so the loop is not inside <c>get_all</c>, then
    /// stops the pump, which joins its thread — before the caller disposes the producer whose
    /// futures the pump holds. The caller releases its holds first.
    /// </summary>
    private static void Cleanup(NativeProducer producer, SendCompletionPump pump)
    {
        while (producer.MockCompleteNext())
        {
            // Resolved one more pending record.
        }

        TestTimeout.Run(pump.Stop, s_deadline);
    }

    /// <summary>
    /// Sets the pump's <c>_stopping</c> flag without joining, so the loop exits at its next top and
    /// leaves everything enqueued afterwards for <c>Stop</c>'s terminal drain.
    /// </summary>
    /// <remarks>
    /// A copy of <c>SendCompletionPumpDrainCapTests.StopTheLoopWithoutDraining</c>, kept private to
    /// this class like the original. The field is private to <see cref="SendCompletionPump"/> and
    /// <see cref="SendCompletionPump.Stop"/> sets it and joins in the same call, so this state is
    /// reachable only by injection.
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

    private static void AssertRanToCompletion(TaskCompletionSource<RecordMetadata>[] completions)
    {
        foreach (TaskCompletionSource<RecordMetadata> completion in completions)
        {
            Assert.Equal(TaskStatus.RanToCompletion, completion.Task.Status);
        }
    }

    private static void AssertTeardownFault(TaskCompletionSource<RecordMetadata>[] completions)
    {
        foreach (TaskCompletionSource<RecordMetadata> completion in completions)
        {
            Assert.Equal(TaskStatus.Faulted, completion.Task.Status);
            KafkaException fault =
                Assert.IsType<KafkaException>(completion.Task.Exception!.InnerException);
            Assert.Equal(TeardownMessage, fault.Message);
        }
    }

    private static int CountCompleted(TaskCompletionSource<RecordMetadata>[] completions)
    {
        int completed = 0;
        foreach (TaskCompletionSource<RecordMetadata> completion in completions)
        {
            if (completion.Task.IsCompleted)
            {
                completed++;
            }
        }

        return completed;
    }

    private static TaskCompletionSource<RecordMetadata>[] Concat(
        params TaskCompletionSource<RecordMetadata>[][] groups)
    {
        int total = 0;
        foreach (TaskCompletionSource<RecordMetadata>[] group in groups)
        {
            total += group.Length;
        }

        TaskCompletionSource<RecordMetadata>[] all =
            new TaskCompletionSource<RecordMetadata>[total];
        int offset = 0;
        foreach (TaskCompletionSource<RecordMetadata>[] group in groups)
        {
            Array.Copy(group, 0, all, offset, group.Length);
            offset += group.Length;
        }

        return all;
    }

    private static TaskCompletionSource<RecordMetadata>[] NewCompletions(
        int count, TaskCreationOptions options)
    {
        TaskCompletionSource<RecordMetadata>[] completions =
            new TaskCompletionSource<RecordMetadata>[count];
        for (int i = 0; i < count; i++)
        {
            completions[i] = new TaskCompletionSource<RecordMetadata>(options);
        }

        return completions;
    }

    /// <summary>
    /// Registers, on each completion, a continuation that runs where the completion is faulted and
    /// reads whether <paramref name="barrier"/> had completed by then — or <see langword="null"/>
    /// when it did not run inside <see cref="SendCompletionPump.Stop"/> on the thread that called
    /// it. The completions must not be created with
    /// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>, which would move the
    /// continuation off that thread.
    /// </summary>
    private static Task<bool?>[] BarrierStateWhenFaulted(
        TaskCompletionSource<RecordMetadata>[] completions, Task barrier)
    {
        Task<bool?>[] readings = new Task<bool?>[completions.Length];
        for (int i = 0; i < completions.Length; i++)
        {
            readings[i] = completions[i].Task.ContinueWith(
                task =>
                {
                    _ = task.Exception;
                    return s_insideStop ? barrier.IsCompleted : (bool?)null;
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        return readings;
    }

    /// <summary>
    /// Calls <see cref="SendCompletionPump.Stop"/> with <see cref="s_insideStop"/> set on this
    /// thread for the duration of the call.
    /// </summary>
    private static void StopMarkingTheThread(SendCompletionPump pump)
    {
        s_insideStop = true;
        try
        {
            pump.Stop();
        }
        finally
        {
            s_insideStop = false;
        }
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
    /// The tasks to wait on, with their faults observed, for groups of null futures that settle as
    /// faulted.
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

    /// <summary>
    /// A delivery callback that counts its invocations and its returns, and blocks inside the
    /// invocation numbered <c>blockOnCall</c> until <see cref="Release"/> is set, for at most
    /// <see cref="s_hold"/>. It runs on the pump thread, inside
    /// <see cref="DeliveryRegistration.Fire"/>'s no-throw boundary.
    /// </summary>
    private sealed class ProbeCallback : IDeliveryCallback
    {
        private readonly int _blockOnCall;
        private int _invoked;
        private int _returned;

        internal ProbeCallback(int blockOnCall)
        {
            _blockOnCall = blockOnCall;
        }

        internal ManualResetEventSlim Entered { get; } = new ManualResetEventSlim(false);

        internal ManualResetEventSlim Release { get; } = new ManualResetEventSlim(false);

        internal int Invoked => Volatile.Read(ref _invoked);

        internal int Returned => Volatile.Read(ref _returned);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            if (Interlocked.Increment(ref _invoked) == _blockOnCall)
            {
                Entered.Set();
                _ = Release.Wait(s_hold);
            }

            Interlocked.Increment(ref _returned);
        }
    }

    /// <summary>What the probe continuation saw when it ran.</summary>
    private sealed class Observation
    {
        internal Observation(int completed, int invoked, int returned)
        {
            Completed = completed;
            Invoked = invoked;
            Returned = returned;
        }

        internal int Completed { get; }

        internal int Invoked { get; }

        internal int Returned { get; }
    }
}
