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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P7 — the managed in-flight cap on the producer <b>async</b> send path under the
/// <b>blocking gate</b> (PLAN §6). The cap is a max-count <see cref="SemaphoreSlim"/> acquired before
/// <c>Producer_send</c> and released 1:1 with the pump's exactly-once future-destroy; the slow
/// (cap-engaged) path acquires it with a <b>synchronous blocking</b>
/// <see cref="SemaphoreSlim.Wait(int, CancellationToken)"/>, so an un-awaited overflow <c>Send</c>
/// <b>blocks the calling thread</b> (Java-faithful backpressure) rather than piling up an incomplete
/// <see cref="Task"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Blocking contract ⇒ helper threads.</b> Because a "parked" overflow send is now a
/// <b>blocked thread</b> (not an incomplete <see cref="Task"/>), every test that drives an
/// overflow/parked <c>Send</c> runs it on a <b>helper thread</b> (<see cref="HelperSend"/>) — driving
/// it inline would block the test thread and hang the suite. A blocked send is observed as
/// "the helper has not returned yet" (<see cref="HelperSend.HasReturned"/>) with slot count /
/// <c>HistoryCount()</c> not advancing; its terminal outcome (canceled / faulted / completed) is
/// observed by awaiting the <see cref="Task"/> the helper eventually returns.
/// </para>
/// <para>
/// <b>Test seam (PLAN §6).</b> A <b>small cap</b> (<see cref="Cap"/> = 4) + a per-test
/// <c>max.block.ms</c> are supplied via the internal <see cref="AsyncMockProducer{TKey, TValue}"/>
/// test-seam ctor (visible through <c>InternalsVisibleTo</c>) — NOT the process-global env var, which
/// would race parallel tests. Tests that block-then-externally-unblock use a <b>long</b>
/// <c>max.block.ms</c> so the block never times out mid-test; only the timeout test (§6.b) uses a
/// short one. <c>s_cap</c> reads the <b>effective</b> per-instance cap via the white-box accessor
/// <c>producer.Native.MaxInflightSlots</c>.
/// </para>
/// </remarks>
public sealed class PublicProducerInflightCapTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "inflight-cap-topic";

    // A small deterministic cap for the test seam (PLAN §6): fills fast, makes the fast/slow-path
    // boundary and the blocking behavior trivial to hit without filling the default 5000 slots.
    private const int Cap = 4;

    // Long enough that a blocking slow-path acquire never times out during a test that unblocks it by
    // teardown / cancel / a freed slot; the §6.b timeout test overrides this with ShortMaxBlockMs.
    private const int LongMaxBlockMs = 60_000;

    // Short budget for the §6.b max.block.ms timeout test — a blocked overflow send with no completer
    // throws after ~this long.
    private const int ShortMaxBlockMs = 500;

    // A shared value buffer — content is irrelevant to the cap; reusing it avoids per-send allocation
    // in the M >> cap loop.
    private static readonly byte[] s_value = Encoding.UTF8.GetBytes("v");

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, s_value, partition: 0);

    // A manual mock (autoComplete: false) with the small test-seam cap + the given max.block.ms.
    private static AsyncMockProducer<byte[], byte[]> NewManualMock(int maxBlockMs = LongMaxBlockMs) =>
        new AsyncMockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false, maxInflightSends: Cap, maxBlockMs: maxBlockMs);

    // ---- 6.1 Slot-leak regression: M >> cap all complete AND the semaphore returns to exactly N ----

    [Fact]
    public async Task Cap_SlotLeak_ManySends_AllComplete_AndSlotsReturnToBaseline()
    {
        using AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        int cap = producer.Native.MaxInflightSlots;
        int m = cap * 50; // M >> cap: most sends traverse the contended slow (blocking) path.

        // Completer runs concurrently, so each contended Send blocks only briefly (until a slot frees)
        // rather than deadlocking — driving them inline on the test thread is safe here.
        using CancellationTokenSource driverStop = new CancellationTokenSource();
        Thread driver = StartCompleter(producer, driverStop.Token);

        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[m];
        for (int i = 0; i < m; i++)
        {
            sends[i] = producer.Send(Record());
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        driverStop.Cancel();
        driver.Join();

        // (a) All M sends completed successfully.
        Assert.All(sends, t => Assert.Equal(TaskStatus.RanToCompletion, t.Status));

        // (b) Every acquired slot was released exactly once → back to exactly N free. The release runs
        // in the pump's finally just after the TCS is completed, so spin briefly to let the last
        // batch's release land, then confirm the count is stable at N (a leak would stall below N).
        Assert.True(
            SpinWait.SpinUntil(() => producer.Native.InflightSlotsAvailable == cap, s_deadline),
            $"in-flight slots did not return to baseline {cap} — last read {producer.Native.InflightSlotsAvailable} (slot leak).");
        Assert.Equal(cap, producer.Native.InflightSlotsAvailable);
    }

    // ---- 6.2 Deadlock / Dispose regression: fill N + BLOCK waiters on helper threads, then teardown
    // returns and every task settles (never hangs, never crashes). Both sync Dispose and DisposeAsync.
    // Under the blocking gate a parked waiter is a blocked thread → helper threads (PLAN §6). ----

    [Fact]
    public async Task Cap_Deadlock_SyncDispose_WithFilledCapAndBlockedWaiters_ReturnsAndTasksSettle()
    {
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        Task<RecordMetadata>[] filled = FillCap(producer);

        // Two overflow sends BLOCK in the gate (Wait(0) fails → slow-path blocking Wait). On helper
        // threads so they don't block the test thread.
        HelperSend blocked1 = new HelperSend(producer, Record());
        HelperSend blocked2 = new HelperSend(producer, Record());
        AssertBlocked(producer, blocked1, blocked2);

        // Dispose with N filled + 2 blocked in flight MUST return without hanging (the deadlock guard):
        // _sendGate.Cancel() wakes the blocked waiters (OCE); the teardown flush resolves the pending
        // filled sends so the pump's get_all returns and the join completes.
        TestTimeout.Run(producer.Dispose, s_deadline);

        Task[] all = Combine(filled, blocked1.AwaitReturn(s_deadline), blocked2.AwaitReturn(s_deadline));
        await ObserveAll(all);
        Assert.All(all, t => Assert.True(t.IsCompleted));

        // The two blocked overflow sends were canceled by teardown (acquired no slot).
        await Assert.ThrowsAsync<OperationCanceledException>(() => blocked1.AwaitReturn(s_deadline));
        await Assert.ThrowsAsync<OperationCanceledException>(() => blocked2.AwaitReturn(s_deadline));
    }

    [Fact]
    public async Task Cap_Deadlock_DisposeAsync_WithFilledCapAndBlockedWaiters_ReturnsAndTasksSettle()
    {
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        Task<RecordMetadata>[] filled = FillCap(producer);

        HelperSend blocked1 = new HelperSend(producer, Record());
        HelperSend blocked2 = new HelperSend(producer, Record());
        AssertBlocked(producer, blocked1, blocked2);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        Task[] all = Combine(filled, blocked1.AwaitReturn(s_deadline), blocked2.AwaitReturn(s_deadline));
        await ObserveAll(all);
        Assert.All(all, t => Assert.True(t.IsCompleted));

        await Assert.ThrowsAsync<OperationCanceledException>(() => blocked1.AwaitReturn(s_deadline));
        await Assert.ThrowsAsync<OperationCanceledException>(() => blocked2.AwaitReturn(s_deadline));
    }

    // ---- 6.3 Over-release guard: the max-count ctor makes any extra Release throw
    // SemaphoreFullException; and after N complete, exactly N (not more) slots are free. ----

    [Fact]
    public void Cap_OverRelease_ExtraReleaseThrowsSemaphoreFull()
    {
        // autoComplete: true — no pending sends; a fresh producer sits at the max count.
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(
                Serdes.ByteArray, Serdes.ByteArray, autoComplete: true, maxInflightSends: Cap, maxBlockMs: LongMaxBlockMs);

        int cap = producer.Native.MaxInflightSlots;

        // An extra Release exceeds the max → the built-in release <= acquire guard fires. This is what
        // makes any slot-accounting bug surface loudly instead of silently over-counting.
        Assert.Equal(cap, producer.Native.InflightSlotsAvailable);
        Assert.Throws<SemaphoreFullException>(() => producer.Native.ReleaseInflightSlotForTest());

        // The failed release did not change the count.
        Assert.Equal(cap, producer.Native.InflightSlotsAvailable);
    }

    [Fact]
    public async Task Cap_AfterNSendsComplete_ExactlyNSlotsFree_NotMore()
    {
        using AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        int cap = producer.Native.MaxInflightSlots;

        using CancellationTokenSource driverStop = new CancellationTokenSource();
        Thread driver = StartCompleter(producer, driverStop.Token);

        // Exactly N sends → all take the fast path (Wait(0)); no blocking.
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[cap];
        for (int i = 0; i < cap; i++)
        {
            sends[i] = producer.Send(Record());
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        driverStop.Cancel();
        driver.Join();

        Assert.True(
            SpinWait.SpinUntil(() => producer.Native.InflightSlotsAvailable == cap, s_deadline),
            $"in-flight slots did not return to exactly {cap} — last read {producer.Native.InflightSlotsAvailable}.");

        // Exactly N and not more: an extra release still throws, proving no phantom slots were added
        // (a double-release bug would have raised the count above N and this would NOT throw).
        Assert.Throws<SemaphoreFullException>(() => producer.Native.ReleaseInflightSlotForTest());
    }

    // ---- 6.4 Cancellation while blocked for a slot: a blocked Send(ct) faults canceled, no slot
    // leaked or spuriously acquired. Under the blocking gate the Send blocks → helper thread. ----

    [Fact]
    public async Task Cap_CancelWhileWaitingForSlot_FaultsCanceled_NoSlotLeaked()
    {
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        Task<RecordMetadata>[] filled = FillCap(producer);

        // One more with a cancelable token → BLOCKS in the slow-path Wait (Wait(0) already failed).
        using CancellationTokenSource cts = new CancellationTokenSource();
        HelperSend blocked = new HelperSend(producer, Record(), cts.Token);
        AssertBlocked(producer, blocked);

        // Cancel the wait → the blocking Wait throws OCE; it acquired no slot.
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => blocked.AwaitReturn(s_deadline));

        // No slot leaked or spuriously acquired: the cap is still fully held by the N filled sends
        // (the canceled waiter neither acquired nor released a slot).
        Assert.Equal(0, producer.Native.InflightSlotsAvailable);

        // Cleanup: teardown flushes the pending filled sends and joins the pump (no hang); observe.
        TestTimeout.Run(producer.Dispose, s_deadline);
        await ObserveAll(filled);
    }

    [Fact]
    public async Task Cap_PreCanceledToken_OnSlowPath_ThrowsSynchronously_NoSlotConsumed()
    {
        // The token is already canceled AND the cap is full, so the send would take the slow path —
        // but the synchronous ThrowIfCancellationRequested precondition in SendViaPump (before the cap
        // acquire, and NOT inside an async method) still fires first, so no slot is consumed and the
        // producer stays healthy. Runs inline (throws synchronously, does not block).
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        Task<RecordMetadata>[] filled = FillCap(producer);

        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => producer.Send(Record(), cts.Token));

        // The cap is untouched by the rejected send.
        Assert.Equal(0, producer.Native.InflightSlotsAvailable);

        TestTimeout.Run(producer.Dispose, s_deadline);
        await ObserveAll(filled);
    }

    // ---- 6.5 Backpressure engages: the (N+1)-th send does not reach Producer_send until a slot
    // frees (observed via the mock's HistoryCount, which increments on the inline Producer_send).
    // Under the blocking gate the overflow send blocks → helper thread. ----

    [Fact]
    public async Task Cap_BackpressureEngages_OverflowSendDoesNotSendUntilSlotFrees()
    {
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        int cap = producer.Native.MaxInflightSlots;

        // Fire N sends (fast path): each calls Producer_send inline → HistoryCount == N; slots at 0.
        Task<RecordMetadata>[] filled = FillCap(producer);
        Assert.Equal(cap, producer.HistoryCount());

        // The (N+1)-th send BLOCKS in the gate BEFORE Producer_send — backpressure engaged.
        HelperSend overflow = new HelperSend(producer, Record());
        AssertBlocked(producer, overflow);
        Assert.Equal(cap, producer.HistoryCount()); // did NOT reach Producer_send

        // It stays blocked while the cap is full (no slot frees on its own).
        await Task.Delay(50);
        Assert.Equal(cap, producer.HistoryCount());
        Assert.False(overflow.HasReturned);

        // Free slots by driving completions: the blocked send then acquires a slot, reaches
        // Producer_send (HistoryCount → N+1) and completes.
        using CancellationTokenSource driverStop = new CancellationTokenSource();
        Thread driver = StartCompleter(producer, driverStop.Token);

        Task[] all = Combine(filled, overflow.AwaitReturn(s_deadline));
        await TestTimeout.Run(() => Task.WhenAll(all), s_deadline);

        driverStop.Cancel();
        driver.Join();

        // The overflow send did reach Producer_send once a slot freed.
        Assert.Equal(cap + 1, producer.HistoryCount());

        producer.Dispose();
    }

    // ---- 6.a NEW — matrix condition #2: teardown wakes a BLOCKED caller (no hang, OCE, no slot leak).
    // Both sync Dispose and async DisposeAsync variants. ----

    [Fact]
    public async Task Cap_Teardown_WakesBlockedCaller_SyncDispose_NoHang_OceThrown_NoSlotLeak()
    {
        await Cap_Teardown_WakesBlockedCaller(disposeAsync: false);
    }

    [Fact]
    public async Task Cap_Teardown_WakesBlockedCaller_DisposeAsync_NoHang_OceThrown_NoSlotLeak()
    {
        await Cap_Teardown_WakesBlockedCaller(disposeAsync: true);
    }

    private static async Task Cap_Teardown_WakesBlockedCaller(bool disposeAsync)
    {
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock();

        int cap = producer.Native.MaxInflightSlots;

        Task<RecordMetadata>[] filled = FillCap(producer);

        // One overflow send BLOCKS inside the gate on a helper thread.
        HelperSend blocked = new HelperSend(producer, Record());
        AssertBlocked(producer, blocked);

        // Teardown from the test thread MUST return within the deadline (no hang): _sendGate.Cancel()
        // (the first teardown action) fires the linked token → the blocked Wait throws OCE.
        if (disposeAsync)
        {
            await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
        }
        else
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }

        // The blocked caller's Send was canceled (it acquired nothing).
        await Assert.ThrowsAsync<OperationCanceledException>(() => blocked.AwaitReturn(s_deadline));

        // No slot leaked: the N filled slots were all released by the teardown flush, and the blocked
        // waiter acquired none → back to exactly N free (a leak would leave it below N).
        Assert.True(
            SpinWait.SpinUntil(() => producer.Native.InflightSlotsAvailable == cap, s_deadline),
            $"in-flight slots did not return to baseline {cap} — last read {producer.Native.InflightSlotsAvailable} (slot leak).");

        await ObserveAll(filled);
    }

    // ---- 6.b NEW — matrix condition #9: max.block.ms timeout. A blocked overflow send with NO
    // completer throws KafkaException after ~max.block.ms; assert the message content + no slot leak.

    [Fact]
    public async Task Cap_MaxBlockMs_Timeout_ThrowsKafkaExceptionWithMessage_NoSlotLeak()
    {
        // Short max.block.ms via the test seam so the timeout fires quickly.
        AsyncMockProducer<byte[], byte[]> producer = NewManualMock(maxBlockMs: ShortMaxBlockMs);

        Task<RecordMetadata>[] filled = FillCap(producer);

        // Overflow send BLOCKS in the gate; NO completer runs, so no slot ever frees → it times out.
        Stopwatch sw = Stopwatch.StartNew();
        HelperSend blocked = new HelperSend(producer, Record());

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(() => blocked.AwaitReturn(s_deadline));
        sw.Stop();

        // Threw after roughly the max.block.ms budget (proves it actually blocked, not fast-failed).
        Assert.True(
            sw.Elapsed >= TimeSpan.FromMilliseconds(ShortMaxBlockMs / 2),
            $"timeout fired too early ({sw.ElapsedMilliseconds} ms) for a {ShortMaxBlockMs} ms max.block.ms budget.");

        // Message content is part of the behavioral contract (DoD §3): Java-faithful wording that
        // names the elapsed ms and that it is the max.block.ms bound.
        Assert.Contains("Failed to allocate an in-flight send slot", ex.Message);
        Assert.Contains("max blocking time", ex.Message);
        Assert.Contains($"{ShortMaxBlockMs} ms", ex.Message);
        Assert.Contains("max.block.ms", ex.Message);

        // No slot leaked or spuriously acquired: the timed-out waiter owes nothing → cap still fully
        // held by the N filled sends.
        Assert.Equal(0, producer.Native.InflightSlotsAvailable);

        // Cleanup.
        TestTimeout.Run(producer.Dispose, s_deadline);
        await ObserveAll(filled);
    }

    // ---- Env-var / config resolver unit tests (pure functions; PLAN §7 D2/D3). Deterministic,
    // broker-free, and race-free (they do NOT touch the process environment). ----

    [Theory]
    [InlineData(null, NativeProducer.DefaultMaxInflightSends)]     // unset
    [InlineData("", NativeProducer.DefaultMaxInflightSends)]       // empty
    [InlineData("   ", NativeProducer.DefaultMaxInflightSends)]    // whitespace (non-numeric)
    [InlineData("abc", NativeProducer.DefaultMaxInflightSends)]    // non-numeric
    [InlineData("12.5", NativeProducer.DefaultMaxInflightSends)]   // not an integer
    [InlineData("0", NativeProducer.DefaultMaxInflightSends)]      // <= 0 → default
    [InlineData("-7", NativeProducer.DefaultMaxInflightSends)]     // negative → default
    [InlineData("1", 1)]                                           // valid minimum
    [InlineData("250", 250)]                                       // valid override
    [InlineData("10000", 10000)]                                   // valid override
    public void ResolveMaxInflightSends_FallsBackToDefaultUnlessPositiveInteger(string? envValue, int expected)
    {
        Assert.Equal(expected, NativeProducer.ResolveMaxInflightSends(envValue));
    }

    [Fact]
    public void ResolveMaxBlockMs_AbsentKey_UsesDefault()
    {
        Assert.Equal(NativeProducer.DefaultMaxBlockMs,
            NativeProducer.ResolveMaxBlockMs(new Dictionary<string, string>()));
    }

    [Theory]
    [InlineData("abc", NativeProducer.DefaultMaxBlockMs)]  // non-numeric → default
    [InlineData("-1", NativeProducer.DefaultMaxBlockMs)]   // negative → default
    [InlineData("0", 0)]                                   // zero is HONORED (fail-fast, matches core)
    [InlineData("500", 500)]                               // valid override
    [InlineData("120000", 120000)]                         // valid override
    public void ResolveMaxBlockMs_ParsesConfigValueElseDefault(string value, int expected)
    {
        Dictionary<string, string> config = new Dictionary<string, string>
        {
            [NativeProducer.MaxBlockMsConfigKey] = value,
        };

        Assert.Equal(expected, NativeProducer.ResolveMaxBlockMs(config));
    }

    // ---- Helpers ----

    /// <summary>
    /// Fills the cap: fires exactly N sends on a manual mock, each acquiring a slot via the fast path
    /// (nothing completes → the semaphore drops to 0). Returns the in-flight send tasks. All fast-path
    /// (Wait(0) succeeds while slots remain) → no blocking, safe to run inline on the test thread.
    /// </summary>
    private static Task<RecordMetadata>[] FillCap(AsyncMockProducer<byte[], byte[]> producer)
    {
        int cap = producer.Native.MaxInflightSlots;
        Task<RecordMetadata>[] filled = new Task<RecordMetadata>[cap];
        for (int i = 0; i < cap; i++)
        {
            filled[i] = producer.Send(Record());
        }

        // Deterministic on a manual mock: N fast-path acquires with no completion → 0 slots left.
        Assert.Equal(0, producer.Native.InflightSlotsAvailable);
        return filled;
    }

    /// <summary>
    /// Asserts each helper send is blocked in the gate: after a short settle it has NOT returned, and
    /// no slot is free (the cap is still fully held). Confirms the overflow send parked <b>before</b>
    /// <c>Producer_send</c> rather than the helper thread merely not having started.
    /// </summary>
    private static void AssertBlocked(AsyncMockProducer<byte[], byte[]> producer, params HelperSend[] blocked)
    {
        // Give the helper thread(s) a moment to reach the blocking Wait.
        Assert.True(
            SpinWait.SpinUntil(() => producer.Native.InflightSlotsAvailable == 0, TimeSpan.FromSeconds(5)),
            "the cap did not stay fully held (expected all slots taken by the filled sends).");
        Thread.Sleep(50);

        foreach (HelperSend h in blocked)
        {
            Assert.False(h.HasReturned, "overflow Send returned early — it should be blocked in the gate.");
        }

        Assert.Equal(0, producer.Native.InflightSlotsAvailable);
    }

    /// <summary>
    /// Starts a background thread that resolves pending mock sends (<c>CompleteNext</c>) until stopped —
    /// the cross-thread drive the manual mock needs to unblock the pump's batched <c>get_all</c> and
    /// free slots. Join it BEFORE disposing (a post-dispose <c>CompleteNext</c> throws
    /// <see cref="ObjectDisposedException"/>, swallowed here as a benign teardown race).
    /// </summary>
    private static Thread StartCompleter(AsyncMockProducer<byte[], byte[]> producer, CancellationToken stop)
    {
        Thread thread = new Thread(() =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    if (!producer.CompleteNext())
                    {
                        Thread.Sleep(1);
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // The producer was disposed before the completer stopped — benign teardown race.
            }
        })
        {
            IsBackground = true,
            Name = "inflight-cap-test-completer",
        };
        thread.Start();
        return thread;
    }

    /// <summary>
    /// Awaits every task to settlement under the hang guard, swallowing faults/cancellations so the
    /// (deliberately faulted teardown / canceled parked) tasks are observed — no unobserved-exception
    /// crash — without asserting their outcome. The settlement itself is asserted by the caller.
    /// </summary>
    private static async Task ObserveAll(Task[] tasks)
    {
        await TestTimeout.Run(
            async () =>
            {
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch
                {
                    // Some tasks fault (teardown) or cancel; observing them here is the point.
                }
            },
            s_deadline);
    }

    private static Task[] Combine(Task<RecordMetadata>[] head, params Task[] tail)
    {
        Task[] all = new Task[head.Length + tail.Length];
        Array.Copy(head, all, head.Length);
        for (int i = 0; i < tail.Length; i++)
        {
            all[head.Length + i] = tail[i];
        }

        return all;
    }

    /// <summary>
    /// Drives one <c>Send</c> on a dedicated helper thread — the mechanism the blocking gate forces:
    /// an overflow/parked send now <b>blocks the calling thread</b> inside
    /// <see cref="SemaphoreSlim.Wait(int, CancellationToken)"/>, so driving it inline would hang the
    /// test. The helper captures the <see cref="Task"/> that <c>Send</c> eventually returns once the
    /// block ends (slot acquired / canceled / timed out); a slow-path throw is delivered on that
    /// <see cref="Task"/> (async method), not synchronously — a synchronous throw is captured too,
    /// defensively.
    /// </summary>
    private sealed class HelperSend
    {
        private readonly Thread _thread;

        // Plain int (not volatile): the Volatile.Read/Write calls below supply the acquire/release
        // fences that publish _task / _syncError — matching NativeProducer._closed. Declaring it
        // volatile AND passing it by ref to Volatile.* trips CS0420.
        private int _returned;
        private Task<RecordMetadata>? _task;
        private Exception? _syncError;

        public HelperSend(
            AsyncMockProducer<byte[], byte[]> producer,
            ProducerRecord<byte[], byte[]> record,
            CancellationToken cancellationToken = default)
        {
            _thread = new Thread(() =>
            {
                try
                {
                    _task = producer.Send(record, cancellationToken);
                }
                catch (Exception ex)
                {
                    // The slow path delivers throws on the returned Task; a synchronous throw would be
                    // a precondition (closed / pre-canceled) — captured here for completeness.
                    _syncError = ex;
                }
                finally
                {
                    // Release fence: publishes _task / _syncError to a reader that observes HasReturned.
                    Volatile.Write(ref _returned, 1);
                }
            })
            {
                IsBackground = true,
                Name = "inflight-cap-overflow-send",
            };
            _thread.Start();
        }

        /// <summary><see langword="true"/> once the <c>Send</c> call has returned (the block ended).</summary>
        public bool HasReturned => Volatile.Read(ref _returned) == 1;

        /// <summary>
        /// Waits until the <c>Send</c> call returns (the block ends), joins the helper thread, then
        /// returns the <see cref="Task"/> it produced (or rethrows a synchronous precondition throw).
        /// </summary>
        public Task<RecordMetadata> AwaitReturn(TimeSpan deadline)
        {
            Assert.True(
                SpinWait.SpinUntil(() => HasReturned, deadline),
                "overflow Send did not return within the deadline (still blocked?).");
            Assert.True(_thread.Join(deadline), "overflow send helper thread did not finish.");

            if (_syncError is not null)
            {
                throw _syncError;
            }

            return _task!;
        }
    }
}
