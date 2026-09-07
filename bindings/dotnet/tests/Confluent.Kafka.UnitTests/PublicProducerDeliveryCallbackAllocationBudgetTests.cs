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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, and this is a runtime-behavior
// test (net462 runs are Windows/CI-only) — so the whole class is net8.0+ only, keeping the net462
// TFM smoke leg compiling. Same gate as PublicProducerSendAllocationBudgetTests.
#if NET8_0_OR_GREATER

/// <summary>
/// M14/P1 — the DoD §10 hot-path allocation audit for the delivery-callback overload (decision
/// D11). Two obligations, one per direction:
/// <list type="bullet">
/// <item>the <b>plain</b> <c>Send(record)</c> path must not regress — widening
/// <c>SendCompletionPump.PendingSend</c> with one nullable reference field must cost nothing,
/// because a <c>ConcurrentQueue&lt;T&gt;</c> stores its items in segment arrays and a null field
/// adds nothing at all;</item>
/// <item>the <b>callback</b> path must add only the one <c>DeliveryRegistration</c> — no captured
/// closure, no captured <c>ProducerRecord</c>, and no eagerly-built placeholder
/// <see cref="RecordMetadata"/> (which is failure-path-only).</item>
/// </list>
/// </summary>
/// <remarks>
/// Uses the <b>per-thread</b> <see cref="GC.GetAllocatedBytesForCurrentThread"/> counter (M7/P1),
/// measured around the synchronous <c>Send</c> calls on this thread — so the completion pump's own
/// allocations are excluded and the measurement is immune to concurrently-running tests
/// (the assembly runs in parallel). The measured region never <c>await</c>s. The value / key
/// buffers and the callback instance are allocated <b>once</b> outside every measured loop, so what
/// is left is attributable to the send path itself.
/// <para>
/// Budgets were set from measurements on this branch, with generous headroom over the observed
/// values but far below the failure signal each one guards: a per-send value-sized copy would add
/// ~<see cref="LargeValueSize"/> bytes, and a per-send closure / record capture / eager placeholder
/// would push the callback-path delta well past <see cref="RegistrationDeltaBudgetBytes"/>.
/// </para>
/// </remarks>
public sealed class PublicProducerDeliveryCallbackAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "delivery-alloc-topic";

    private const int SendCount = 64;

    private const int SmallValueSize = 64;

    private const int LargeValueSize = 512 * 1024;

    // MEASURED on this branch (net10.0, arm64): the plain path costs ~280 B/send on the caller
    // thread — the ProducerRecord and the TCS + its Task. The ceiling leaves headroom for runtime /
    // TFM variation while still catching what it guards: a value-sized managed copy would add
    // ~LargeValueSize bytes per send, and a Task-scoped pin would show here too.
    //
    // RE-BASELINED for M11/P3.1 (the DoD §10 obligation the accumulator brings). Two send-path costs
    // moved and the budget still holds at 512 B:
    //   * GONE — the per-send topic pin. The async path interns ONE permanently-pinned buffer per
    //     DISTINCT topic (§4.1), so a steady-state send allocates nothing for its topic at all.
    //   * NEW — the accumulator node's parallel arrays, which are a per-NODE cost the caller thread
    //     pays when it grows or allocates one. Two things keep it off the per-send budget: the
    //     record is marshalled into its blittable slot at append time rather than stored (so the
    //     node has no per-slot SerializedProducerRecord, its widest field), and a drained node is
    //     RECYCLED rather than re-allocated — so at steady state the node contributes zero.
    // The remaining measurement is therefore essentially unchanged from Option C, which is why this
    // number did not have to move.
    private const long PlainPerSendBudgetBytes = 512;

    // MEASURED: the callback path costs 320 B/send — a delta of EXACTLY 40 B, which is one
    // DeliveryRegistration (three fields, one object). The ceiling is deliberately tight rather
    // than generous, because sensitivity is the whole point, and it was verified BOTH ways:
    // injecting a second per-send object (an eagerly-built placeholder RecordMetadata held on the
    // registration) took the measured delta from 40 B to 96 B and turned this test red. A captured
    // closure or a captured ProducerRecord would too.
    private const long RegistrationDeltaBudgetBytes = 64;

    [Fact]
    public async Task PlainSend_PerRecordAllocation_IsNotRegressedByTheCallbackField()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);

        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Warmup(producer, value, key, callback: null);

        long bytes = FireAndMeasure(producer, value, key, callback: null, out Task<RecordMetadata>[] sends);
        await AwaitAll(sends);

        long perSend = bytes / SendCount;
        Assert.True(
            perSend <= PlainPerSendBudgetBytes,
            $"Plain Send per-send caller-thread allocation {perSend} B exceeded the budget " +
            $"{PlainPerSendBudgetBytes} B ({bytes} B over {SendCount} sends) — the delivery-callback " +
            "field must cost the plain path nothing.");
    }

    [Fact]
    public async Task CallbackSend_AddsOnlyTheRegistration()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Warmup(producer, value, key, callback: null);
        await Warmup(producer, value, key, callback);

        long plainBytes = FireAndMeasure(producer, value, key, callback: null, out Task<RecordMetadata>[] plainSends);
        await AwaitAll(plainSends);

        long callbackBytes = FireAndMeasure(producer, value, key, callback, out Task<RecordMetadata>[] callbackSends);
        await AwaitAll(callbackSends);

        long perSendDelta = (callbackBytes - plainBytes) / SendCount;
        Assert.True(
            perSendDelta <= RegistrationDeltaBudgetBytes,
            $"Callback-path per-send allocation delta {perSendDelta} B exceeded the budget " +
            $"{RegistrationDeltaBudgetBytes} B (plain={plainBytes} B, callback={callbackBytes} B over " +
            $"{SendCount} sends) — only ONE DeliveryRegistration may be added per send.");
    }

    [Fact]
    public async Task CallbackSend_HasNoValueSizedCopy()
    {
        // The callback path is still a hot path: a large value must cost the same as a small one, so
        // the key/value bytes are still pinned and copied by the core during the call rather than
        // being copied onto the managed heap (the DoD §10 obligation the plain path already carries).
        byte[] key = MakeBytes(16, 0xAB);
        byte[] smallValue = MakeBytes(SmallValueSize, 0xCD);
        byte[] largeValue = MakeBytes(LargeValueSize, 0xCD);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Warmup(producer, smallValue, key, callback);

        long smallBytes = FireAndMeasure(producer, smallValue, key, callback, out Task<RecordMetadata>[] smallSends);
        await AwaitAll(smallSends);

        long largeBytes = FireAndMeasure(producer, largeValue, key, callback, out Task<RecordMetadata>[] largeSends);
        await AwaitAll(largeSends);

        long perSendMarginal = (largeBytes - smallBytes) / SendCount;
        Assert.True(
            perSendMarginal <= PlainPerSendBudgetBytes,
            $"Callback-path per-send value-size marginal {perSendMarginal} B exceeded the budget " +
            $"{PlainPerSendBudgetBytes} B (small={smallBytes} B, large={largeBytes} B over {SendCount} " +
            "sends) — a value-sized managed copy or a Task-scoped pin would show here.");
    }

    private static async Task Warmup(
        AsyncMockProducer<byte[], byte[]> producer, byte[] value, byte[] key, IDeliveryCallback? callback)
    {
        // Warm the send path (JIT + the first pump start) so the measured runs are steady-state.
        for (int i = 0; i < 3; i++)
        {
            _ = FireAndMeasure(producer, value, key, callback, out Task<RecordMetadata>[] sends);
            await AwaitAll(sends);
        }
    }

    private static long FireAndMeasure(
        AsyncMockProducer<byte[], byte[]> producer,
        byte[] value,
        byte[] key,
        IDeliveryCallback? callback,
        out Task<RecordMetadata>[] sends)
    {
        // Pre-allocate the task array OUTSIDE the measured window (its allocation is not per-send).
        Task<RecordMetadata>[] tasks = new Task<RecordMetadata>[SendCount];

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            // Same `value` / `key` / `callback` references across sends — nothing here is
            // re-allocated per send except what the send path itself allocates.
            ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0);
            tasks[i] = callback is null
                ? producer.Send(record)
                : producer.Send(record, callback);
        }

        long after = GC.GetAllocatedBytesForCurrentThread();

        sends = tasks;
        return after - before;
    }

    private static async Task AwaitAll(Task<RecordMetadata>[] sends) =>
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

    private static byte[] MakeBytes(int size, byte fill)
    {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++)
        {
            bytes[i] = fill;
        }

        return bytes;
    }

    /// <summary>
    /// A do-nothing delivery callback — allocation tests must not have the callback itself allocate.
    /// </summary>
    private sealed class CountingDeliveryCallback : IDeliveryCallback
    {
        private int _count;

        /// <summary>Read only so the counter is not an unused field; the tests assert bytes, not counts.</summary>
        internal int Count => _count;

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception) => _count++;
    }
}

#endif
