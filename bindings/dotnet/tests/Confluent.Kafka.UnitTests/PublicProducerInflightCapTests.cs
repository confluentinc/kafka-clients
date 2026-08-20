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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P6 — the managed in-flight cap on the producer <b>async</b> send path (PLAN §6). The cap is a
/// max-count <see cref="SemaphoreSlim"/> of N = 1000 acquired before <c>Producer_send</c> and released
/// 1:1 with the pump's exactly-once future-destroy. These tests drive the cap deterministically
/// against a manual <see cref="AsyncMockProducer{TKey, TValue}"/> (<c>autoComplete: false</c>),
/// completing pending sends from a helper thread (<c>CompleteNext</c>), and use the test-only
/// white-box accessors (<see cref="AsyncMockProducer{TKey, TValue}.Native"/> →
/// <c>InflightSlotsAvailable</c> / <c>MaxInflightSlots</c> / <c>ReleaseInflightSlotForTest</c>) to
/// assert slot accounting directly.
/// </summary>
/// <remarks>
/// The five hard gates (PLAN §6.1–§6.5): (1) slot-leak — M ≫ cap sends all complete and the semaphore
/// returns to exactly N; (2) deadlock/Dispose — fill N + park a waiter, teardown returns without
/// hanging and every task settles; (3) over-release — an extra <c>Release()</c> throws
/// <see cref="SemaphoreFullException"/> (the max-count guard), and exactly N slots free after N
/// complete; (4) cancellation while waiting — a parked <c>Send(ct)</c> faults canceled with no slot
/// leaked; (5) backpressure engages — the (N+1)-th send does not reach <c>Producer_send</c> until a
/// slot frees. Every awaited op / teardown runs under a <see cref="TestTimeout"/> hang guard.
/// </remarks>
public sealed class PublicProducerInflightCapTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "inflight-cap-topic";

    // The cap N (1000), read from the binding so the tests track the const rather than hard-coding it.
    private static readonly int s_cap = NativeProducer.MaxInflightSlots;

    // A shared value buffer — content is irrelevant to the cap; reusing it avoids per-send allocation
    // in the M ≫ cap loop.
    private static readonly byte[] s_value = Encoding.UTF8.GetBytes("v");

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, s_value, partition: 0);

    // ---- 6.1 Slot-leak regression: M ≫ cap all complete AND the semaphore returns to exactly N ----

    [Fact]
    public async Task Cap_SlotLeak_ManySends_AllComplete_AndSlotsReturnToBaseline()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        const int M = 5000; // M >> cap (1000): ~4000 sends traverse the contended slow (WaitAsync) path.

        using CancellationTokenSource driverStop = new CancellationTokenSource();
        Thread driver = StartCompleter(producer, driverStop.Token);

        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[M];
        for (int i = 0; i < M; i++)
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
            SpinWait.SpinUntil(() => producer.Native.InflightSlotsAvailable == s_cap, s_deadline),
            $"in-flight slots did not return to baseline {s_cap} — last read {producer.Native.InflightSlotsAvailable} (slot leak).");
        Assert.Equal(s_cap, producer.Native.InflightSlotsAvailable);
    }

    // ---- 6.2 Deadlock / Dispose regression: fill N + park a waiter, then teardown returns and every
    // task settles (never hangs, never crashes). Both sync Dispose and DisposeAsync. ----

    [Fact]
    public async Task Cap_Deadlock_SyncDispose_WithFilledCapAndParkedWaiters_ReturnsAndTasksSettle()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata>[] filled = FillCap(producer);

        // Park two waiters (Wait(0) fails → SendAfterWaitAsync suspends at WaitAsync).
        Task<RecordMetadata> parked1 = producer.Send(Record());
        Task<RecordMetadata> parked2 = producer.Send(Record());
        Assert.False(parked1.IsCompleted);
        Assert.False(parked2.IsCompleted);

        // Dispose with N filled + 2 parked in flight MUST return without hanging (the deadlock guard):
        // _sendGate.Cancel() wakes the parked waiters (OCE); the teardown flush resolves the pending
        // filled sends so the pump's get_all returns and the join completes.
        TestTimeout.Run(producer.Dispose, s_deadline);

        Task[] all = Combine(filled, parked1, parked2);
        await ObserveAll(all);
        Assert.All(all, t => Assert.True(t.IsCompleted));
    }

    [Fact]
    public async Task Cap_Deadlock_DisposeAsync_WithFilledCapAndParkedWaiters_ReturnsAndTasksSettle()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata>[] filled = FillCap(producer);

        Task<RecordMetadata> parked1 = producer.Send(Record());
        Task<RecordMetadata> parked2 = producer.Send(Record());
        Assert.False(parked1.IsCompleted);
        Assert.False(parked2.IsCompleted);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        Task[] all = Combine(filled, parked1, parked2);
        await ObserveAll(all);
        Assert.All(all, t => Assert.True(t.IsCompleted));
    }

    // ---- 6.3 Over-release guard: the max-count ctor makes any extra Release throw
    // SemaphoreFullException; and after N complete, exactly N (not more) slots are free. ----

    [Fact]
    public void Cap_OverRelease_ExtraReleaseThrowsSemaphoreFull()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // A fresh producer sits at the max count: an extra Release exceeds the max → the built-in
        // release <= acquire guard fires. This is what makes any slot-accounting bug surface loudly
        // instead of silently over-counting.
        Assert.Equal(s_cap, producer.Native.InflightSlotsAvailable);
        Assert.Throws<SemaphoreFullException>(() => producer.Native.ReleaseInflightSlotForTest());

        // The failed release did not change the count.
        Assert.Equal(s_cap, producer.Native.InflightSlotsAvailable);
    }

    [Fact]
    public async Task Cap_AfterNSendsComplete_ExactlyNSlotsFree_NotMore()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        using CancellationTokenSource driverStop = new CancellationTokenSource();
        Thread driver = StartCompleter(producer, driverStop.Token);

        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[s_cap];
        for (int i = 0; i < s_cap; i++)
        {
            sends[i] = producer.Send(Record());
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        driverStop.Cancel();
        driver.Join();

        Assert.True(
            SpinWait.SpinUntil(() => producer.Native.InflightSlotsAvailable == s_cap, s_deadline),
            $"in-flight slots did not return to exactly {s_cap} — last read {producer.Native.InflightSlotsAvailable}.");

        // Exactly N and not more: an extra release still throws, proving no phantom slots were added
        // (a double-release bug would have raised the count above N and this would NOT throw).
        Assert.Throws<SemaphoreFullException>(() => producer.Native.ReleaseInflightSlotForTest());
    }

    // ---- 6.4 Cancellation while waiting for a slot: a parked Send(ct) faults canceled, no slot
    // leaked or spuriously acquired. ----

    [Fact]
    public async Task Cap_CancelWhileWaitingForSlot_FaultsCanceled_NoSlotLeaked()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata>[] filled = FillCap(producer);

        // One more with a cancelable token → parks at WaitAsync (Wait(0) already failed).
        using CancellationTokenSource cts = new CancellationTokenSource();
        Task<RecordMetadata> parked = producer.Send(Record(), cts.Token);
        Assert.False(parked.IsCompleted);

        // Cancel the wait → the task faults OCE/TaskCanceledException; WaitAsync acquired no slot.
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => WithTimeout(parked));

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
        // Fast-path pre-canceled token is covered by PublicProducerSendTests; here the token is
        // already canceled AND the cap is full, so the send would take the slow path — but the
        // synchronous ThrowIfCancellationRequested precondition (before the cap acquire) still fires
        // first, so no slot is consumed and the producer stays healthy.
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

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
    // frees (observed via the mock's HistoryCount, which increments on the inline Producer_send). ----

    [Fact]
    public async Task Cap_BackpressureEngages_OverflowSendDoesNotSendUntilSlotFrees()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        // Fire N sends (fast path): each calls Producer_send inline → HistoryCount == N; slots at 0.
        Task<RecordMetadata>[] filled = FillCap(producer);
        Assert.Equal(s_cap, producer.HistoryCount());

        // The (N+1)-th send parks in WaitAsync BEFORE Producer_send — backpressure engaged.
        Task<RecordMetadata> overflow = producer.Send(Record());
        Assert.False(overflow.IsCompleted);
        Assert.Equal(s_cap, producer.HistoryCount()); // did NOT reach Producer_send

        // It stays parked while the cap is full (no slot frees on its own).
        await Task.Delay(50);
        Assert.Equal(s_cap, producer.HistoryCount());
        Assert.False(overflow.IsCompleted);

        // Free slots by driving completions: the parked send then acquires a slot, reaches
        // Producer_send (HistoryCount → N+1) and completes.
        using CancellationTokenSource driverStop = new CancellationTokenSource();
        Thread driver = StartCompleter(producer, driverStop.Token);

        Task[] all = Combine(filled, overflow);
        await TestTimeout.Run(() => Task.WhenAll(all), s_deadline);

        driverStop.Cancel();
        driver.Join();

        // The overflow send did reach Producer_send once a slot freed.
        Assert.Equal(s_cap + 1, producer.HistoryCount());

        producer.Dispose();
    }

    // ---- Helpers ----

    /// <summary>
    /// Fills the cap: fires exactly N sends on a manual mock, each acquiring a slot via the fast path
    /// (nothing completes → the semaphore drops to 0). Returns the in-flight send tasks.
    /// </summary>
    private static Task<RecordMetadata>[] FillCap(AsyncMockProducer<byte[], byte[]> producer)
    {
        Task<RecordMetadata>[] filled = new Task<RecordMetadata>[s_cap];
        for (int i = 0; i < s_cap; i++)
        {
            filled[i] = producer.Send(Record());
        }

        // Deterministic on a manual mock: N fast-path acquires with no completion → 0 slots left.
        Assert.Equal(0, producer.Native.InflightSlotsAvailable);
        return filled;
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

    private static async Task<RecordMetadata> WithTimeout(Task<RecordMetadata> task)
    {
        Task winner = await Task.WhenAny(task, Task.Delay(s_deadline)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException("Send did not complete within the deadline — treated as a hang.");
        }

        return await task.ConfigureAwait(false);
    }
}
