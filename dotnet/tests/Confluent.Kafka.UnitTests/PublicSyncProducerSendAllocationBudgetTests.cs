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
/// The send-path allocation audit for the sync <c>IProducer.Send</c>: a large value costs the same as a small one
/// (the key and value are pinned for the call, never copied onto the heap), the caller may mutate its buffers once
/// Send returns, and the absolute per-send caller-thread cost stays within its budget.
/// </summary>
public sealed class PublicSyncProducerSendAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-send-alloc-topic";

    private const int SendCount = 64;
    private const int SmallValueSize = 64;
    private const int LargeValueSize = 512 * 1024;

    // A generous ceiling on the value-size-independent per-send cost that still catches a value-sized copy
    // (~LargeValueSize bytes per send) or a Task-scoped pin.
    private const long PerSendBudgetBytes = 512;

    [Fact]
    public void Send_MutatingBufferAfterSend_ProducesUnchangedRecord()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        byte[] key = Encoding.UTF8.GetBytes("key");
        byte[] value = Encoding.UTF8.GetBytes("value-original");

        KafkaFuture<RecordMetadata> firstFuture = default;
        TestTimeout.Run(
            () => firstFuture = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0)),
            s_deadline);

        // Mutate the buffers after Send returns and before Get: the core read them during the call, so the record is
        // unchanged, and a second send reusing them succeeds.
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

    // GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the budget facts are net8.0+ only.

    // The absolute per-send caller-thread budget of a plain Send(record) on an auto-completing mock, over SendCount
    // sends with a 64 B value, best of AbsoluteMeasurementAttempts, each attempt's futures waited on outside the
    // measured window.
    private const long AbsolutePerSendBudgetBytes = 288;

    // Best of N: an absolute figure has no matched pair to cancel an occasional growth allocation, so it needs a
    // growth-free run among the attempts.
    private const int AbsoluteMeasurementAttempts = 8;

    private readonly ITestOutputHelper _output;

    public PublicSyncProducerSendAllocationBudgetTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Send_AbsoluteCallerThreadBudget()
    {
        // The marginal fact cancels every value-size-independent cost, so it cannot see one more object per send;
        // this one measures the whole caller-thread cost.
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(SmallValueSize, 0xCD);

        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // Pre-allocated OUTSIDE every measured window; storing a struct into it allocates nothing.
        KafkaFuture<RecordMetadata>[] futures = new KafkaFuture<RecordMetadata>[SendCount];

        // Warm up so the measured runs are steady-state.
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

        // A second definition, reported and not asserted: every thread's allocations, the completion thread's
        // included, from before the first send to after the last Get, best of N. Process-wide, so the minimum is the
        // quiet attempt.
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

        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

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

        // Each send's metadata is copied out before its future completes, so the last Get returns after those copies.
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
