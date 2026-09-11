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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P3.1 slice S5 — the teardown handshake (§3.8) as seen from the <b>public</b> surface, across
/// all four teardown flavors. The accumulator inserts a third party into what used to be a two-party
/// teardown: it holds records that have not reached the core at all, so they are not futures and the
/// completion pump knows nothing about them.
/// </summary>
/// <remarks>
/// The property under test is <b>the ordering</b>: the accumulator is closed and drained
/// <em>before</em> the pump's gate closes, so its final drain's futures arrive at an OPEN gate and
/// are resolved normally. Getting those two the wrong way round is not a crash — it is silent
/// completion loss dressed up as an accepted residual (the pump's fault-in-place branch fires no
/// delivery callback), which is exactly the kind of defect a "teardown returned" assertion misses.
/// So these tests assert what the records DID, not merely that <c>Dispose</c> came back.
/// <para>
/// ⚠ <b>And "what the records did" has to mean something the inverted ordering cannot also
/// produce.</b> It did not, at first: the sends' own outcomes are indistinguishable between the two
/// orderings, because both can fault with the same teardown exception — so the whole suite passed
/// with the ordering inverted. The witness is now <c>DrainedSendCount</c>; see
/// <see cref="AssertDrainedIntoTheOpenGate"/> for why it is the only deterministic one.
/// </para>
/// <para>
/// ⚠ <b>M11/P3.2 S4 joins this file for a different property, and deliberately so</b> —
/// <see cref="Dispose_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem"/>. The four
/// flavor tests above guard the §3.8 <em>ordering</em> and accept "faulted with a message containing
/// <c>closed</c>" as one legitimate outcome, so they are structurally blind to whether the queued
/// group was completed or faulted. S4's production call sites
/// (<c>NativeProducer.StopPump</c> / <c>StopPumpAsync</c>) therefore need their own guard here, on
/// the <b>public</b> surface, because the S4 unit test drives the interop harness's fixture teardown
/// instead — deleting the two production lines alone left the whole suite green (DoD §12: a
/// fixture-only guard is a proof about the fixture).
/// </para>
/// </remarks>
public sealed class PublicProducerAccumulatorTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "accumulator-teardown-topic";

    private const int SendCount = 24;

    [Fact]
    public void Dispose_DrainsAccumulatorRecords_IntoTheStillOpenPumpGate()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);

        TestTimeout.Run(producer.Dispose, s_deadline);

        AssertDrainedIntoTheOpenGate(producer, sends, nameof(producer.Dispose));
    }

    [Fact]
    public async Task DisposeAsync_DrainsAccumulatorRecords_IntoTheStillOpenPumpGate()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        AssertDrainedIntoTheOpenGate(producer, sends, nameof(producer.DisposeAsync));
    }

    [Fact]
    public async Task Close_DrainsAccumulatorRecords_IntoTheStillOpenPumpGate()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);

        await TestTimeout.Run(() => producer.Close(), s_deadline);

        AssertDrainedIntoTheOpenGate(producer, sends, nameof(producer.Close));
    }

    [Fact]
    public async Task CloseWithCancellationToken_DrainsAccumulatorRecords_IntoTheStillOpenPumpGate()
    {
        // The fourth flavor: Close(CancellationToken) — the same worker, reached with a live token.
        using CancellationTokenSource cts = new CancellationTokenSource();
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);

        await TestTimeout.Run(() => producer.Close(cts.Token), s_deadline);

        AssertDrainedIntoTheOpenGate(producer, sends, "Close(CancellationToken)");
    }

    [Fact]
    public void Dispose_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem()
    {
        // ⚠ M11/P3.2 S4 — THE PRODUCTION CALL SITES. The twin of
        // SendCompletionPumpPreStopDrainTests.Teardown_CompletesSendsAlreadyQueuedToThePump_...
        // (§6 test 17), which drives the interop harness's own fixture teardown; this one drives
        // NativeProducer.StopPump through the PUBLIC Dispose, so the two
        // `pump?.WaitForQueueDrain(s_pumpDrainTimeout)` calls that actually ship are guarded.
        // Without it, deleting BOTH production lines and keeping the fixture's left the entire
        // suite green — the §3.8 flavor tests above cannot see it (they accept a fault whose
        // message contains "closed", and DrainedSendCount rises through either consumer), and test
        // 17 cannot see it either because it never enters NativeProducer. That is the DoD §12
        // failure class from the other side: the fixture mirrors production faithfully, but nothing
        // held production to the mirror.
        //
        // THE ASSERTION IS RanToCompletion, not a counter. ProcessedBatchCount — test 17's witness
        // for "the pump resolved it" — is not surfaced past SendCompletionPump (NativeProducer
        // exposes only DrainedSendCount), and DrainedSendCount is incremented by BOTH the pump loop
        // and Stop's terminal fault drain, so it cannot separate them. What S4 makes deterministic
        // on this surface is that every send the core already accepted is COMPLETED; the pre-S4
        // behaviour faults some of them, and a fault is not RanToCompletion.
        //
        // ⚠ A K-BURST WITH A FRESH PRODUCER PER ROUND, for test 17's reason: the defect is a
        // SCHEDULING race (does the pump thread wake inside a window made of one CloseGate plus one
        // Producer_flush?), so a single round is a coin flip and no guard at all. Each round is an
        // independent trial and the mutation must win all of them.
        //
        // SYNCHRONOUS Dispose deliberately — the DisposeAsync twin's awaited flush hands the pump an
        // extra scheduling opportunity, which makes it detect the deletion only intermittently at
        // this round count. Both flavors share the same `pump?.WaitForQueueDrain` step immediately
        // before `pump?.Stop()`, so the sync path is the reliable instrument for it.
        //
        // Mutation that must fail this, ON ITS OWN: delete the two
        // `pump?.WaitForQueueDrain(s_pumpDrainTimeout)` lines from NativeProducer.StopPump and
        // StopPumpAsync, leaving the harness fixture's call in place. RunLoop then breaks on
        // _stopping with the drained group still queued, DrainAndFaultRemaining faults it, and a
        // round fails on RanToCompletion.
        const int Rounds = 48;

        for (int round = 0; round < Rounds; round++)
        {
            AsyncMockProducer<byte[], byte[]> producer =
                new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

            // Left in the accumulator: teardown's own StopAccumulator is what drains them, so the
            // group reaches the pump's queue in exactly the F4 window — microseconds before
            // _stopping — with the teardown flush having already resolved what the pump will read.
            Task<RecordMetadata>[] sends = Fire(producer);

            TestTimeout.Run(producer.Dispose, s_deadline);

            for (int i = 0; i < sends.Length; i++)
            {
                Assert.Equal(TaskStatus.RanToCompletion, sends[i].Status);
            }
        }
    }

    [Fact]
    public void Dispose_WithManualMockAndUndrainedRecords_Returns()
    {
        // A manual-completion mock: the records reach the core but never resolve on their own, so
        // the completion pump's blocking get_all cannot return until the teardown flush drives them.
        // The accumulator drain must therefore not be what hangs — it hands the records over and
        // exits, and the pre-existing flush-before-join ordering does the rest.
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata>[] sends = Fire(producer);

        TestTimeout.Run(producer.Dispose, s_deadline);

        // Each send SETTLED one way or the other — none is left pending, which is the shape that
        // hangs an awaiting caller forever.
        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.True(send.IsCompleted, "a send was left pending after teardown");
        }
    }

    [Fact]
    public void ClearWithRecordsInTheAccumulator_TheDrainStillCompletesPromptly()
    {
        // The §6.3 re-enumeration, scoped to what this phase owns. The recorded pump-orphan finding
        // is that MockProducer.clear() drops the core's pending completions WITHOUT completing them,
        // which breaks the "flush resolves every pending send" premise the pump's flush-before-join
        // fix rests on. The question this phase has to answer is narrower: does the ACCUMULATOR add
        // a second way to hang? It does not — its drain hands the records to the core and returns,
        // whatever the core then does with them.
        //
        // ⚠ RECORDED FINDING, NOT FIXED HERE: when Clear() lands AFTER a drain has already handed the
        // records over, the pump is blocked in an uninterruptible get_all on futures the core has
        // forgotten, the teardown flush has nothing left to resolve, and SendCompletionPump.Stop's
        // UNBOUNDED _thread.Join() never returns. That is pre-existing (it predates the accumulator),
        // it is a property of SendCompletionPump.Stop, and SendCompletionPump.Stop is explicitly out
        // of scope for this phase (§1.5's carve-out table lists "Changing Stop / CloseGate / Enqueue
        // / _stopLock semantics" as forbidden). Asserting teardown here would therefore be asserting
        // a fix this slice is not allowed to make — so this test stops at the accumulator's own
        // boundary and the finding is reported rather than papered over.
        // An AUTO-complete mock, deliberately: it isolates the accumulator's own behaviour from the
        // out-of-scope pump hang above, which needs a manual mock to reach. Running this on a manual
        // mock would make the test's outcome depend on whether Clear() beat the 10 ms drain window —
        // i.e. on which side of the pre-existing race won, which is not a contract.
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);
        producer.Clear();

        // The drain must return promptly — a hang here would be the accumulator's own, and the
        // deadline is the assertion.
        TestTimeout.Run(() => producer.WaitForSendsToReachCore(s_deadline), s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);

        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.True(send.IsCompleted, "a send was left pending after a Clear + drain + teardown");
        }
    }

    [Fact]
    public void ConcurrentSendAndDispose_WithTheAccumulator_DoesNotHangOrCrash()
    {
        // Churn: senders racing teardown must all SETTLE, and teardown must return. The accumulator
        // adds two new race surfaces — an append landing as the accumulator closes (refused, pins
        // returned) and a drained future landing as the pump's gate closes (faulted in place).
        for (int round = 0; round < 8; round++)
        {
            AsyncMockProducer<byte[], byte[]> producer =
                new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

            Task sender = Task.Run(() =>
            {
                for (int i = 0; i < 64; i++)
                {
                    try
                    {
                        _ = producer.Send(NewRecord(i));
                    }
                    catch (ObjectDisposedException)
                    {
                        // Expected once teardown wins: nothing was sent.
                        return;
                    }
                }
            });

            TestTimeout.Run(producer.Dispose, s_deadline);
            TestTimeout.Run(() => sender.GetAwaiter().GetResult(), s_deadline);
        }
    }

    [Fact]
    public void CreateSendDisposeMany_WithTheAccumulator_NoLeakOrCrash()
    {
        // Every producer starts a batch thread AND a pump thread now, so a teardown that failed to
        // join either would leak two threads per iteration rather than one.
        for (int i = 0; i < 24; i++)
        {
            AsyncMockProducer<byte[], byte[]> producer =
                new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            _ = producer.Send(NewRecord(i));
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void SendAfterDispose_IsRefusedByTheDisposedGuard()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        _ = producer.Send(NewRecord(0));
        TestTimeout.Run(producer.Dispose, s_deadline);

        // Synchronously — the closed latch is checked before anything is pinned or appended, so a
        // post-teardown send cannot enter the accumulator through any window. Captured into a local
        // so the assertion is about the SYNCHRONOUS throw, not about awaiting a faulted Task.
        Func<object> send = () => producer.Send(NewRecord(1));
        Assert.Throws<ObjectDisposedException>(send);
    }

    private static Task<RecordMetadata>[] Fire(AsyncMockProducer<byte[], byte[]> producer)
    {
        // Fired and NOT awaited, so they are still sitting in the accumulator (the default 10 ms
        // window has barely started) when teardown begins — which is the state under test.
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[SendCount];
        for (int i = 0; i < SendCount; i++)
        {
            sends[i] = producer.Send(NewRecord(i));
        }

        return sends;
    }

    private static ProducerRecord<byte[], byte[]> NewRecord(int index) =>
        new ProducerRecord<byte[], byte[]>(
            Topic, Encoding.UTF8.GetBytes($"value-{index}"), partition: 0);

    /// <summary>
    /// Asserts the two properties the §3.8 handshake adds: every record the accumulator accepted
    /// <b>reached the pump's still-open gate</b>, and none of them is left pending.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>The ordering assertion is the count, NOT the sends' outcomes — and that is the whole
    /// point.</b> Under the inverted ordering (gate closed first) every drained future takes
    /// <c>Enqueue</c>'s fault-in-place branch, which faults the send with
    /// <c>SendCompletionPump.TeardownException()</c>: <i>"The producer was closed before the send
    /// completed."</i> Under the CORRECT ordering a send can fault with that <em>identical</em>
    /// exception, because the pump's loop may lose the race to <c>Stop</c>'s terminal drain — an
    /// out-of-scope, pre-existing property of <c>SendCompletionPump.Stop</c> (§1.5's carve-out
    /// table). So the accepted outcome and the defect share an observable, and no assertion on the
    /// <see cref="Task"/>s can separate them: this file's earlier "contains 'closed'" tolerance
    /// accepted both, and the whole suite passed with the ordering inverted.
    /// </para>
    /// <para>
    /// <c>DrainedSendCount</c> separates them, and does so <b>deterministically</b>. It counts sends
    /// taken off the pump's queue, so it counts exactly those that reached <c>Enqueue</c> while the
    /// gate was open; every queued send is dequeued exactly once by one of the pump's two
    /// consumers, whichever won that race. Correct ordering ⇒ all of them; inverted ⇒ none were
    /// ever queued.
    /// </para>
    /// </remarks>
    private static void AssertDrainedIntoTheOpenGate(
        AsyncMockProducer<byte[], byte[]> producer, Task<RecordMetadata>[] sends, string flavor)
    {
        Assert.Equal(sends.Length, producer.DrainedSendCount);

        for (int i = 0; i < sends.Length; i++)
        {
            Task<RecordMetadata> send = sends[i];
            Assert.True(
                send.IsCompleted,
                $"{flavor} left send {i} pending — the accumulator's records were abandoned");

            if (send.Status == TaskStatus.RanToCompletion)
            {
                Assert.Equal(Topic, send.Result.Topic);
            }
            else
            {
                // Faulted is acceptable ONLY as the out-of-scope pump race above; anything else
                // means the record failed for a reason this teardown path should not produce.
                KafkaException failure = Assert.IsType<KafkaException>(send.Exception?.InnerException);
                Assert.Contains("closed", failure.Message, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
