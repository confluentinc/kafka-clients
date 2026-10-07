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

    // M11/P3.5 T21 (ii) — the ABSOLUTE per-send budget of the two-stage Send's fast path. The marginal
    // budget above subtracts a constant per-send cost away, so it cannot see one: a first stage that
    // stopped being a struct over the delivery task (one more Task<T>, 72 B) passes it unchanged.
    // MEASURED: 160 B/send on BOTH net8.0 and net10.0 — the ProducerRecord, the completion source and
    // its task — at M11/P3.5 S3, and again, unchanged, at M11/P3.6 S2 (3 runs per TFM) once the first
    // stage yields an AsyncKafkaFuture<RecordMetadata>, a struct over that same task. The budget is
    // that plus 16 B (M11/P3.6 D11 (a)), which is less than one boxed AsyncKafkaFuture<RecordMetadata>
    // (24 B) and so less than one more Task<T> (72 B). History: P3.5's budget was that plus 32 B (192),
    // verified red then by injecting one more Task<T> (a Task.FromResult first stage on the fast path).
    private const long FastPathPerSendBudgetBytes = 176;

    // More attempts than the marginal test: an absolute figure has no matched pair to cancel a node
    // allocation within an attempt, so it needs a drain-free run among them.
    private const int AbsoluteMeasurementAttempts = 8;

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

    [Fact]
    public async Task Send_FastPath_PerRecordAllocation_StaysWithinItsMeasuredBudget()
    {
        // M11/P3.5 T21 (ii) — with a permit free, Send(record) returns a ValueTask over the record's
        // AsyncKafkaFuture, whose Get() is the delivery task itself, and allocates nothing for the first
        // stage. Measured absolutely (caller-thread bytes per Send over 64 sends, no token, no callback,
        // best of 8), against a budget tight enough that one extra Task-sized allocation per send turns
        // it red.
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);

        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 3; i++)
        {
            _ = FireAndMeasure(producer, value, key, out Task<RecordMetadata>[] warmup);
            await AwaitAll(warmup);
        }

        // BEST OF N, as above: a run whose window caught a drain pays for a node; the minimum is the
        // run that did not, and a per-send regression raises every run, the minimum included.
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
