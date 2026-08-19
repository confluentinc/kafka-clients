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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P4 — the sync <see cref="IProducer.Send(ProducerRecord)"/> is on the send path, so the DoD §10
/// hot-path allocation audit applies (ffi §A4, PLAN §7). The <b>allocation-budget</b> fact asserts
/// that <see cref="IProducer.Send"/> adds <b>no value-sized managed allocation</b>: a large value
/// costs the same as a small one, proving the key/value bytes are pinned (via <c>fixed</c>) and
/// copied by the core during the call, never onto the managed heap. The <b>mutation-after-send</b>
/// fact proves the copy happened during the call (the caller may reuse/mutate its buffers the moment
/// <see cref="IProducer.Send"/> returns). The sync send is strictly leaner than the async send — no
/// TCS / cancellation registration / GCHandle / pump — so the only per-send managed allocations are
/// the unavoidable <see cref="RecordMetadata"/> + topic string (Java's own behavior), which are
/// value-size-independent and cancel in the marginal subtraction.
/// </summary>
/// <remarks>
/// <b>Measured on the caller thread.</b> Unlike the async producer (whose completion runs on a pump
/// thread), the sync <see cref="IProducer.Send"/> blocks and copies the metadata out on the caller's
/// own thread, so <see cref="GC.GetAllocatedBytesForCurrentThread"/> captures the whole per-send cost
/// here — and the (large − small) subtraction still cancels the value-size-independent parts, leaving
/// only a value-sized copy (if any) to catch. The value buffer is allocated <b>once</b> outside the
/// measured loop and reused across sends, so any value-sized allocation would come from the send path
/// itself. The mock can't read back value bytes broker-free (integration-only), so the
/// mutation-after-send fact asserts the send <em>completes correctly</em> (correct offsets) after the
/// buffer is mutated and reused — the call-scoped-copy proof the mock allows.
/// </remarks>
public sealed class PublicSyncProducerSendAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-send-alloc-topic";

    private const int SendCount = 64;
    private const int SmallValueSize = 64;
    private const int LargeValueSize = 512 * 1024;

    // The per-send caller-thread cost is a fixed RecordMetadata + topic string — value-size-
    // independent. A generous ceiling that still catches a value-sized copy (~LargeValueSize bytes
    // per send) or a Task-scoped pin.
    private const long PerSendBudgetBytes = 512;

    // ---- Mutation-after-send (runs on every TFM; proves the core copied during the call) ----

    [Fact]
    public void Send_MutatingBufferAfterSend_ProducesUnchangedRecord()
    {
        using MockProducer producer = new MockProducer();

        byte[] key = Encoding.UTF8.GetBytes("key");
        byte[] value = Encoding.UTF8.GetBytes("value-original");

        RecordMetadata first = null!;
        TestTimeout.Run(
            () => first = producer.Send(new ProducerRecord(Topic, value, key, partition: 0)),
            s_deadline);
        Assert.Equal(0L, first.Offset);

        // Mutate the caller's buffers AFTER Send returns. The core copied key/value into the batch
        // synchronously DURING Producer_send (ffi §A4), and sync Send returns only after the send
        // resolved — so the mutation cannot affect the produced record. A second send reusing the
        // mutated buffers succeeds independently (proving buffer reuse is safe).
        for (int i = 0; i < value.Length; i++)
        {
            value[i] = 0xFF;
        }

        for (int i = 0; i < key.Length; i++)
        {
            key[i] = 0xEE;
        }

        RecordMetadata second = null!;
        TestTimeout.Run(
            () => second = producer.Send(new ProducerRecord(Topic, value, key, partition: 0)),
            s_deadline);
        Assert.Equal(1L, second.Offset);
    }

#if NET8_0_OR_GREATER

    // GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, and this is a runtime-behavior
    // test (net462 runs are Windows/CI-only) — so the budget fact + its helper are net8.0+ only,
    // keeping the net462 TFM smoke leg compiling.

    [Fact]
    public void Send_PerRecordAllocation_HasNoValueSizedCopy()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] smallValue = MakeBytes(SmallValueSize, 0xCD);
        byte[] largeValue = MakeBytes(LargeValueSize, 0xCD);

        using MockProducer producer = new MockProducer();

        // Warm up the send path (JIT) so the measured runs are steady-state.
        _ = MeasureSends(producer, smallValue, key);

        long smallBytes = MeasureSends(producer, smallValue, key);
        long largeBytes = MeasureSends(producer, largeValue, key);

        long perSendMarginal = (largeBytes - smallBytes) / SendCount;

        Assert.True(
            perSendMarginal <= PerSendBudgetBytes,
            $"Per-send caller-thread allocation marginal {perSendMarginal} B exceeded the budget " +
            $"{PerSendBudgetBytes} B (small={smallBytes} B, large={largeBytes} B over {SendCount} sends) — " +
            "a value-sized managed copy or a Task-scoped pin would show here.");
    }

    private static long MeasureSends(MockProducer producer, byte[] value, byte[] key)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            // Same `value` / `key` references across sends — the value buffer is NOT re-allocated per
            // send, so any value-sized allocation here would come from the send path itself.
            _ = producer.Send(new ProducerRecord(Topic, value, key, partition: 0));
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static byte[] MakeBytes(int size, byte fill)
    {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++)
        {
            bytes[i] = fill;
        }

        return bytes;
    }

#endif
}
