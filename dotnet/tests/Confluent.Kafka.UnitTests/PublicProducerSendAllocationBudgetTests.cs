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
/// The send-path allocation audit for the async <c>Send</c>: a large value costs the same as a small one (the key
/// and value are pinned, never copied onto the heap), and the absolute per-send caller-thread cost stays within
/// its budget. Measured with the per-thread allocation counter around the synchronous part of Send, best of N.
/// </summary>
public sealed class PublicProducerSendAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "send-alloc-topic";

    private const int SendCount = 64;
    private const int SmallValueSize = 64;
    private const int LargeValueSize = 512 * 1024;

    // A generous ceiling on the value-size-independent per-send cost that still catches a value-sized copy
    // (~LargeValueSize bytes per send) or a Task-scoped pin.
    private const long PerSendBudgetBytes = 512;

    // Each measurement is repeated and the lowest result taken.
    private const int MeasurementAttempts = 4;

    // The absolute per-send budget, which the marginal budget cannot see: the measured 424 B (net8.0, net10.0; the
    // delivery state, its KafkaFutureImpl, the delivery task and the record) plus 16 B, less than one more object.
    private const long FastPathPerSendBudgetBytes = 440;

    // More attempts than the marginal test, as an absolute figure has no matched pair to cancel a one-off allocation.
    private const int AbsoluteMeasurementAttempts = 8;

    [Fact]
    public async Task Send_PerRecordAllocation_HasNoValueSizedCopy()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] smallValue = MakeBytes(SmallValueSize, 0xCD);
        byte[] largeValue = MakeBytes(LargeValueSize, 0xCD);

        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // Warm up so the measured runs are steady-state.
        for (int i = 0; i < 3; i++)
        {
            _ = FireAndMeasure(producer, smallValue, key, out Task<RecordMetadata>[] warmup);
            await AwaitAll(warmup);
        }

        // Best of N over matched small/large pairs: a one-off allocation is noise, while a value-sized copy would add
        // ~LargeValueSize bytes to every attempt.
        long smallBytes = 0;
        long largeBytes = 0;
        long perSendMarginal = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            long small = FireAndMeasure(producer, smallValue, key, out Task<RecordMetadata>[] smallSends);
            await AwaitAll(smallSends);

            long large = FireAndMeasure(producer, largeValue, key, out Task<RecordMetadata>[] largeSends);
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
            perSendMarginal <= PerSendBudgetBytes,
            $"Per-send caller-thread allocation marginal {perSendMarginal} B exceeded the budget " +
            $"{PerSendBudgetBytes} B (small={smallBytes} B, large={largeBytes} B over {SendCount} sends) — " +
            "a value-sized managed copy or a Task-scoped pin would show here.");
    }

    [Fact]
    public async Task Send_FastPath_PerRecordAllocation_StaysWithinItsMeasuredBudget()
    {
        // Send(record) allocates nothing for the first stage. Measured absolutely (bytes per Send over 64 sends, no
        // token, no callback, best of 8), against a budget that one more Task-sized allocation per send exceeds.
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);

        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        for (int i = 0; i < 3; i++)
        {
            _ = FireAndMeasure(producer, value, key, out Task<RecordMetadata>[] warmup);
            await AwaitAll(warmup);
        }

        // Best of N: a per-send regression raises every attempt, the minimum included.
        long bytes = long.MaxValue;
        for (int attempt = 0; attempt < AbsoluteMeasurementAttempts; attempt++)
        {
            long measured = FireAndMeasure(producer, value, key, out Task<RecordMetadata>[] sends);
            await AwaitAll(sends);
            bytes = Math.Min(bytes, measured);
        }

        long perSend = bytes / SendCount;
        Assert.True(
            perSend <= FastPathPerSendBudgetBytes,
            $"Fast-path Send per-send caller-thread allocation {perSend} B exceeded the budget " +
            $"{FastPathPerSendBudgetBytes} B ({bytes} B over {SendCount} sends) — the first stage must " +
            "stay a struct over the delivery task when a permit is free.");
    }

    private static long FireAndMeasure(AsyncMockProducer<byte[], byte[]> producer, byte[] value, byte[] key, out Task<RecordMetadata>[] sends)
    {
        // Pre-allocate the task array OUTSIDE the measured window (its allocation is not per-send).
        Task<RecordMetadata>[] tasks = new Task<RecordMetadata>[SendCount];

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            // Same `value` / `key` references across sends — the value buffer is NOT re-allocated
            // per send, so any value-sized allocation here would come from the send path itself.
            tasks[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0)).Delivery();
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
}

#endif
