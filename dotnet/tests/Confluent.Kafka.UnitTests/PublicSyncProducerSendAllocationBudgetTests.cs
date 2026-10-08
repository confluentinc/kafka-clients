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
#if NET8_0_OR_GREATER
using Xunit.Abstractions;
#endif

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P4 — the sync <see cref="IProducer.Send(ProducerRecord)"/> is on the send path, so the DoD §10
/// hot-path allocation audit applies (ffi §A4, PLAN §7). The <b>marginal</b> fact asserts that
/// <see cref="IProducer.Send"/> adds <b>no value-sized managed allocation</b>: a large value costs the
/// same as a small one, proving the key/value bytes are pinned (via <c>fixed</c>) and copied by the core
/// during the call, never onto the managed heap. The <b>mutation-after-send</b> fact proves the copy
/// happened during the call (the caller may reuse/mutate its buffers the moment
/// <see cref="IProducer.Send"/> returns). The <b>absolute</b> fact (M11/P4.2 D11) pins the whole
/// per-send caller-thread cost, which the marginal subtraction cannot see.
/// </summary>
/// <remarks>
/// <para>
/// <b>What the caller thread allocates per send (M11/P4.2).</b> <see cref="IProducer.Send"/> returns once
/// the core has accepted the record, with a <see cref="KafkaFuture{T}"/> — a struct, so it allocates
/// nothing itself. The caller's per-send allocations are the <see cref="ProducerRecord{TKey, TValue}"/>
/// (the test's own), the send's <c>SyncCompletion</c> latch, the pump's <c>PendingSyncSend</c> entry and
/// the topic's call-scoped UTF-8 pin, all value-size-independent. The <see cref="RecordMetadata"/> and its strings are copied out on the
/// send-completion pump, so the caller-thread figure does not include them; the absolute fact also
/// reports a caller + pump figure as a second definition, without asserting it.
/// </para>
/// <para>
/// <b>Measured on the caller thread</b> with <see cref="GC.GetAllocatedBytesForCurrentThread"/>. The
/// value buffer is allocated <b>once</b> outside the measured loop and reused across sends, so any
/// value-sized allocation would come from the send path itself, and the (large − small) subtraction
/// cancels everything value-size-independent. The mock can't read back value bytes broker-free
/// (integration-only), so the mutation-after-send fact asserts the send <em>completes correctly</em>
/// (correct offsets) after the buffer is mutated and reused — the call-scoped-copy proof the mock allows.
/// </para>
/// </remarks>
public sealed class PublicSyncProducerSendAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-send-alloc-topic";

    private const int SendCount = 64;
    private const int SmallValueSize = 64;
    private const int LargeValueSize = 512 * 1024;

    // The per-send caller-thread cost is a fixed record + latch + pump entry — value-size-independent
    // (see the class remarks). A generous ceiling that still catches a value-sized copy (~LargeValueSize
    // bytes per send) or a Task-scoped pin.
    private const long PerSendBudgetBytes = 512;

    // ---- Mutation-after-send (runs on every TFM; proves the core copied during the call) ----

    [Fact]
    public void Send_MutatingBufferAfterSend_ProducesUnchangedRecord()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        byte[] key = Encoding.UTF8.GetBytes("key");
        byte[] value = Encoding.UTF8.GetBytes("value-original");

        KafkaFuture<RecordMetadata> firstFuture = default;
        TestTimeout.Run(
            () => firstFuture = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0)),
            s_deadline);

        // Mutate the caller's buffers AFTER Send returns and BEFORE its Get. The core copied key/value
        // into the batch synchronously DURING Producer_send (ffi §A4), so the buffers are reusable the
        // moment Send returns — the mutation cannot affect the produced record. A second send reusing
        // the mutated buffers succeeds independently (proving buffer reuse is safe).
        for (int i = 0; i < value.Length; i++)
        {
            value[i] = 0xFF;
        }

        for (int i = 0; i < key.Length; i++)
        {
            key[i] = 0xEE;
        }

        RecordMetadata first = null!;
        TestTimeout.Run(() => first = firstFuture.Get(), s_deadline);
        Assert.Equal(0L, first.Offset);

        RecordMetadata second = null!;
        TestTimeout.Run(
            () => second = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0)).Get(),
            s_deadline);
        Assert.Equal(1L, second.Offset);
    }

#if NET8_0_OR_GREATER

    // GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, and this is a runtime-behavior
    // test (net462 runs are Windows/CI-only) — so the budget facts + their helpers are net8.0+ only,
    // keeping the net462 TFM smoke leg compiling.

    // M11/P4.2 D11 — the ABSOLUTE per-send caller-thread budget of the sync Send. Definition: the bytes
    // GC.GetAllocatedBytesForCurrentThread charges to the sending thread per plain Send(record) (no
    // callback) on an auto-completing MockProducer, over SendCount sends with a 64 B value, best of
    // AbsoluteMeasurementAttempts; each attempt's futures are waited on outside the measured window.
    // MEASURED: 272 B/send on BOTH net8.0 and net10.0 (17408 B over 64 sends, 3 runs per TFM, every run
    // identical) at M11/P4.2 S4 — the ProducerRecord (64 B), the SyncCompletion latch (48 B), the topic's
    // call-scoped UTF-8 pin for this 21-character topic (120 B: Utf8Marshal.Pin's encoded bytes plus
    // their NUL-terminated copy) and, by difference, the pump's PendingSyncSend entry (40 B); the first
    // three were measured one by one on net8.0. RecordMetadata is no longer here: the pump copies it out,
    // which only the caller + pump figure the test reports can see. The budget is that plus 16 B (M11/P4.2 D11, the P3.6 D11 (a) precedent), which is less than one boxed
    // KafkaFuture<RecordMetadata> (24 B). The two TFMs agree, so one constant serves both.
    private const long AbsolutePerSendBudgetBytes = 288;

    // Best of N, as the async fast-path budget (PublicProducerSendAllocationBudgetTests): an absolute
    // figure has no matched pair to cancel an occasional growth allocation on the caller thread (the
    // pump queue's ConcurrentQueue segment), so it needs a growth-free run among the attempts.
    private const int AbsoluteMeasurementAttempts = 8;

    private readonly ITestOutputHelper _output;

    public PublicSyncProducerSendAllocationBudgetTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Send_AbsoluteCallerThreadBudget()
    {
        // The marginal fact below subtracts every value-size-independent per-send cost away, so it cannot
        // see one more object per send — a boxed KafkaFuture<RecordMetadata> (24 B), for instance. This
        // one can: it is the whole caller-thread cost, against a budget of the measured figure + 16 B.
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);

        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Pre-allocated OUTSIDE every measured window; storing a struct into it allocates nothing.
        KafkaFuture<RecordMetadata>[] futures = new KafkaFuture<RecordMetadata>[SendCount];

        // Warm up (JIT, the pump's start, the queue's first segments) so the measured runs are steady-state.
        for (int i = 0; i < 3; i++)
        {
            _ = SendAndMeasureCallerThread(producer, value, key, futures);
            GetAll(futures);
        }

        long callerBytes = long.MaxValue;
        for (int attempt = 0; attempt < AbsoluteMeasurementAttempts; attempt++)
        {
            long measured = SendAndMeasureCallerThread(producer, value, key, futures);
            GetAll(futures);
            callerBytes = Math.Min(callerBytes, measured);
        }

        // Second definition, REPORTED ONLY (not asserted): every thread's allocations — caller + pump
        // (the pump copies RecordMetadata and its strings out) — from before the first send to after the
        // last Get, best of the same N. Process-wide, so a concurrently running test can inflate an
        // attempt; the minimum is the quiet one.
        long totalBytes = long.MaxValue;
        for (int attempt = 0; attempt < AbsoluteMeasurementAttempts; attempt++)
        {
            totalBytes = Math.Min(totalBytes, SendAndMeasureCallerAndPump(producer, value, key, futures));
        }

        long perSend = callerBytes / SendCount;
        _output.WriteLine(
            $"A1 caller-thread: {perSend} B/send ({callerBytes} B over {SendCount} sends, best of " +
            $"{AbsoluteMeasurementAttempts}); caller + pump (GC.GetTotalAllocatedBytes(precise: true), " +
            $"report only): {totalBytes / SendCount} B/send ({totalBytes} B over {SendCount} sends).");

        Assert.True(
            perSend <= AbsolutePerSendBudgetBytes,
            $"Sync Send per-send caller-thread allocation {perSend} B exceeded the budget " +
            $"{AbsolutePerSendBudgetBytes} B ({callerBytes} B over {SendCount} sends, best of " +
            $"{AbsoluteMeasurementAttempts}) — the caller's per-send allocations are the record, the latch, " +
            "the pump entry and the topic pin; KafkaFuture<RecordMetadata> must stay an unboxed struct.");
    }

    [Fact]
    public void Send_PerRecordAllocation_HasNoValueSizedCopy()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] smallValue = MakeBytes(SmallValueSize, 0xCD);
        byte[] largeValue = MakeBytes(LargeValueSize, 0xCD);

        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

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

    private static long MeasureSends(MockProducer<byte[], byte[]> producer, byte[] value, byte[] key)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            // Same `value` / `key` references across sends — the value buffer is NOT re-allocated per
            // send, so any value-sized allocation here would come from the send path itself. The
            // future is discarded: the measurement is the send itself (its Get is not on this path).
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0));
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long SendAndMeasureCallerThread(
        MockProducer<byte[], byte[]> producer, byte[] value, byte[] key, KafkaFuture<RecordMetadata>[] futures)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            futures[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0));
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long SendAndMeasureCallerAndPump(
        MockProducer<byte[], byte[]> producer, byte[] value, byte[] key, KafkaFuture<RecordMetadata>[] futures)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < SendCount; i++)
        {
            futures[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0));
        }

        // The pump copies each send's metadata out BEFORE it opens that send's latch, so once the last
        // Get returns every pump-side allocation for these sends has happened.
        GetAll(futures);
        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private static void GetAll(KafkaFuture<RecordMetadata>[] futures)
    {
        // Bounded: the auto-completing mock resolves every send, so a hang here is a regression.
        TestTimeout.Run(
            () =>
            {
                foreach (KafkaFuture<RecordMetadata> future in futures)
                {
                    _ = future.Get();
                }
            },
            s_deadline);
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
