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

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0+ only.
#if NET8_0_OR_GREATER

/// <summary>
/// The send-path allocation audit for the delivery-callback overload: the plain Send must not regress, and the
/// callback Send must add no per-send closure, record capture, eager placeholder metadata or value-sized copy.
/// Measured with the per-thread allocation counter around the synchronous part of Send, best of N attempts.
/// </summary>
public sealed class PublicProducerDeliveryCallbackAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "delivery-alloc-topic";

    private const int SendCount = 64;

    private const int SmallValueSize = 64;

    private const int LargeValueSize = 512 * 1024;

    // The ceiling catches a per-send value-sized copy (~LargeValueSize bytes) or a Task-scoped pin, with headroom
    // for runtime variation.
    private const long PlainPerSendBudgetBytes = 512;

    // The ceiling on what the callback adds per send, kept tight because sensitivity is the point.
    private const long RegistrationDeltaBudgetBytes = 64;

    // Each measurement is repeated and the lowest result taken: a one-off allocation in one attempt is noise, while a
    // real per-send regression raises every attempt.
    private const int MeasurementAttempts = 4;

    // The absolute per-send budget of the callback overload: the measured 424 B (net8.0, net10.0; the callback is
    // a field of the delivery state) plus 16 B, less than one more object (24 B).
    private const long CallbackFastPathPerSendBudgetBytes = 440;

    // More attempts than the paired tests, as an absolute figure has no matched pair to cancel a one-off allocation.
    private const int AbsoluteMeasurementAttempts = 8;

    [Fact]
    public async Task PlainSend_PerRecordAllocation_IsNotRegressedByTheCallbackField()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);

        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Warmup(producer, value, key, callback: null);

        // Best of N: a genuine per-send regression raises every attempt, the minimum included.
        long bytes = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            long measured = FireAndMeasure(producer, value, key, callback: null, out Task<RecordMetadata>[] sends);
            await AwaitAll(sends);
            bytes = Math.Min(bytes, measured);
        }

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

        // Best of N over matched plain/callback pairs, so a one-off allocation cancels within an attempt.
        long plainBytes = 0;
        long callbackBytes = 0;
        long perSendDelta = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            long plain = FireAndMeasure(producer, value, key, callback: null, out Task<RecordMetadata>[] plainSends);
            await AwaitAll(plainSends);

            long withCallback = FireAndMeasure(producer, value, key, callback, out Task<RecordMetadata>[] callbackSends);
            await AwaitAll(callbackSends);

            long delta = (withCallback - plain) / SendCount;
            if (delta < perSendDelta)
            {
                perSendDelta = delta;
                plainBytes = plain;
                callbackBytes = withCallback;
            }
        }
        Assert.True(
            perSendDelta <= RegistrationDeltaBudgetBytes,
            $"Callback-path per-send allocation delta {perSendDelta} B exceeded the budget " +
            $"{RegistrationDeltaBudgetBytes} B (plain={plainBytes} B, callback={callbackBytes} B over " +
            $"{SendCount} sends) — the callback overload may add at most {RegistrationDeltaBudgetBytes} B per send.");
    }

    [Fact]
    public async Task CallbackSend_HasNoValueSizedCopy()
    {
        // A large value must cost the same as a small one: the key and value are pinned, never copied onto the heap.
        byte[] key = MakeBytes(16, 0xAB);
        byte[] smallValue = MakeBytes(SmallValueSize, 0xCD);
        byte[] largeValue = MakeBytes(LargeValueSize, 0xCD);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Warmup(producer, smallValue, key, callback);

        // Best of N over matched small/large pairs.
        long smallBytes = 0;
        long largeBytes = 0;
        long perSendMarginal = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            long small = FireAndMeasure(producer, smallValue, key, callback, out Task<RecordMetadata>[] smallSends);
            await AwaitAll(smallSends);

            long large = FireAndMeasure(producer, largeValue, key, callback, out Task<RecordMetadata>[] largeSends);
            await AwaitAll(largeSends);

            long marginal = (large - small) / SendCount;
            if (marginal < perSendMarginal)
            {
                perSendMarginal = marginal;
                smallBytes = small;
                largeBytes = large;
            }
        }
        Assert.True(
            perSendMarginal <= PlainPerSendBudgetBytes,
            $"Callback-path per-send value-size marginal {perSendMarginal} B exceeded the budget " +
            $"{PlainPerSendBudgetBytes} B (small={smallBytes} B, large={largeBytes} B over {SendCount} " +
            "sends) — a value-sized managed copy or a Task-scoped pin would show here.");
    }

    [Fact]
    public async Task CallbackSend_FastPath_PerRecordAllocation_StaysWithinItsMeasuredBudget()
    {
        // Measured absolutely (bytes per Send over 64 sends, best of 8), against a budget that one more Task-sized
        // allocation per send exceeds.
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Warmup(producer, value, key, callback);

        long bytes = long.MaxValue;
        for (int attempt = 0; attempt < AbsoluteMeasurementAttempts; attempt++)
        {
            long measured = FireAndMeasure(producer, value, key, callback, out Task<RecordMetadata>[] sends);
            await AwaitAll(sends);
            bytes = Math.Min(bytes, measured);
        }

        long perSend = bytes / SendCount;
        Assert.True(
            perSend <= CallbackFastPathPerSendBudgetBytes,
            $"Fast-path callback Send per-send caller-thread allocation {perSend} B exceeded the budget " +
            $"{CallbackFastPathPerSendBudgetBytes} B ({bytes} B over {SendCount} sends) — the first stage " +
            "must stay a struct over the delivery task when a permit is free.");
    }

    private static async Task Warmup(
        AsyncMockProducer<byte[], byte[]> producer, byte[] value, byte[] key, IDeliveryCallback? callback)
    {
        // Warm up so the measured runs are steady-state.
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
                ? producer.Send(record).Delivery()
                : producer.Send(record, callback).Delivery();
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

        // Read only so the counter is not an unused field; the tests assert bytes, not counts.
        internal int Count => _count;

        public void OnCompletion(RecordMetadata metadata, Exception? exception) => _count++;
    }
}

#endif
