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
// TFM smoke leg compiling.
#if NET8_0_OR_GREATER

/// <summary>
/// M11/P3 — the send path is a hot path, so the DoD §10 hot-path allocation audit applies in full
/// (ffi §A4, PLAN §7). Asserts that <see cref="IAsyncProducer.Send"/> adds <b>no value-sized
/// managed allocation</b> on the caller thread: a large value costs the same as a small one, proving
/// the key/value bytes are pinned (via <c>fixed</c>) and copied by the core during the call, never
/// copied onto the managed heap (an intermediate copy / a Task-scoped pin would show here).
/// </summary>
/// <remarks>
/// Uses the <b>per-thread</b> <see cref="GC.GetAllocatedBytesForCurrentThread"/> counter (M7/P1),
/// measured around the synchronous <see cref="IAsyncProducer.Send"/> calls on this thread — so the
/// completion pump's allocations (on its own thread) are excluded, and the measurement is immune to
/// concurrently-running tests on other threads. The value buffer is allocated <b>once</b> outside
/// the measured loop and the same reference is reused across sends, so the only per-send caller-thread
/// allocations are value-size-independent (the record object, the TCS, the small topic pin); the
/// marginal (large − small) subtraction cancels them. The fired tasks are awaited <b>after</b> the
/// measurement window (auto-complete mock) so no send task goes unobserved.
/// </remarks>
public sealed class PublicProducerSendAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "send-alloc-topic";

    private const int SendCount = 64;
    private const int SmallValueSize = 64;
    private const int LargeValueSize = 512 * 1024;

    // The per-send caller-thread cost is a fixed record object + TCS + small topic pin — all
    // value-size-independent. A generous ceiling that still catches a value-sized copy (which would
    // add ~LargeValueSize bytes per send) or a Task-scoped pin.
    private const long PerSendBudgetBytes = 512;

    // Repetitions before the BEST (lowest) result is taken — see the rationale at the use site.
    private const int MeasurementAttempts = 4;

    [Fact]
    public async Task Send_PerRecordAllocation_HasNoValueSizedCopy()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] smallValue = MakeBytes(SmallValueSize, 0xCD);
        byte[] largeValue = MakeBytes(LargeValueSize, 0xCD);

        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Warm up the send path (JIT + first pump start) so the measured runs are steady-state.
        for (int i = 0; i < 3; i++)
        {
            _ = FireAndMeasure(producer, smallValue, key, out Task<RecordMetadata>[] warmup);
            await AwaitAll(warmup);
        }

        // BEST OF N (M11/P3.1): the send accumulator charges a node allocation (or growth) to
        // whichever caller thread needs a fresh node, so a drain landing inside a measured window
        // makes that run pay for one. The minimum over matched small/large pairs is the drain-free
        // measurement; a real value-sized copy would add ~LargeValueSize bytes to EVERY attempt.
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
            tasks[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0));
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
