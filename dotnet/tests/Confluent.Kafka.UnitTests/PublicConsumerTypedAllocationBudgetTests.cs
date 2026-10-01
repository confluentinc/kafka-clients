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

using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, and this is a runtime-behavior
// test (net462 runs are Windows/CI-only) — so the whole class is net8.0+ only, keeping the net462
// TFM smoke leg compiling.
#if NET8_0_OR_GREATER

/// <summary>
/// M6/P1b typed-poll allocation budget (PLAN §5/§9, DoD §10/§11) — proves the typed receive
/// path is <b>zero-copy on the key/value</b>: it deserializes from a span directly over the
/// native batch with <b>no intermediate per-record <c>byte[]</c></b>. A large value is decoded
/// to a tiny <c>int</c> (its span length) via <see cref="SpanLengthDeserializer"/>; the marginal
/// per-record allocation between a 16-byte value and a 64 KiB value stays near zero — an
/// intermediate value-sized <c>byte[]</c> (as the raw-byte copy-out path makes) would show the
/// full value-size delta here.
/// </summary>
/// <remarks>
/// Driven via the <b>sync</b> typed <see cref="MockConsumer{TKey, TValue}"/> (M7/P1), whose typed
/// copy-out runs the shared <c>ConsumerRecordsMarshal.CopyOut&lt;K,V&gt;</c> on the caller's (this)
/// thread — the identical marshaller the async poll runs on its foreign dispatcher thread. So a
/// <b>per-thread</b> <see cref="GC.GetAllocatedBytesForCurrentThread"/> count captures exactly this
/// poll's allocation, is <b>immune to allocations by concurrently-running tests on other
/// threads</b> (robust under parallel execution, unlike the process-wide
/// <see cref="GC.GetTotalAllocatedBytes(bool)"/>), and — because the marshaller is shared — fully
/// covers the async typed per-record budget too. The marginal (large − small) subtraction cancels
/// the fixed per-poll and per-record overhead (record object, topic string, list slot — identical
/// in both measurements).
/// </remarks>
/// <remarks>
/// ⚠ <b>This test is structurally blind to the topic name, by construction.</b> The record count
/// and the topic are identical in both measurements, so the topic strings appear on both sides of
/// the subtraction and <b>cancel exactly</b> — it can never fail on a per-record topic decode, in
/// either direction. That clause (<c>ffi §B4</c>: "no allocation attributable to <b>topic
/// name</b>") is covered by <see cref="PublicConsumerTopicAllocationBudgetTests"/>, which holds the
/// record count fixed and varies only the topic-name length. Do not read the "topic string cancels"
/// note above as evidence that the topic clause is tested here.
/// </remarks>
public sealed class PublicConsumerTypedAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "typed-alloc-topic";
    private const int Partition = 0;

    private const int RecordCount = 64;
    private const int SmallValueSize = 16;
    private const int LargeValueSize = 64 * 1024;

    // The value-size delta per record is (LargeValueSize - SmallValueSize) ~= 64 KiB. Zero-copy
    // deserialize-to-int adds nothing per record beyond the int (no heap); a budget far below
    // one value-sized copy still catches an intermediate byte[] on the value path.
    private const long MarginalPerRecordBudgetBytes = 1024;

    [Fact]
    public void TypedPoll_LargeValue_AddsNoValueSizedIntermediateAllocation()
    {
        // Warmup — JIT + first-poll fixed costs out of the way.
        for (int i = 0; i < 5; i++)
        {
            MeasurePoll(RecordCount, SmallValueSize);
        }

        long smallBytes = MeasurePoll(RecordCount, SmallValueSize);
        long largeBytes = MeasurePoll(RecordCount, LargeValueSize);

        long marginalPerRecord = (largeBytes - smallBytes) / RecordCount;

        Assert.True(
            marginalPerRecord <= MarginalPerRecordBudgetBytes,
            $"Typed poll added {marginalPerRecord} B/record going from a {SmallValueSize} B value to a " +
            $"{LargeValueSize} B value (budget {MarginalPerRecordBudgetBytes} B; small={smallBytes} B, " +
            $"large={largeBytes} B over {RecordCount} records). A per-record intermediate value-sized " +
            "byte[] on the key/value path would show ~the value-size delta here — the deserialize is NOT zero-copy.");
    }

    private static long MeasurePoll(int recordCount, int valueSize)
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        byte[] key = new byte[8];
        byte[] value = new byte[valueSize];

        // The SYNC typed consumer runs CopyOut<byte[], int> on THIS thread (M7/P1) — the same
        // marshaller the async poll runs on its foreign dispatcher thread — so the per-thread
        // counter below measures exactly the typed key/value path.
        using MockConsumer<byte[], int> consumer =
            new MockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        for (int i = 0; i < recordCount; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, key, value);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        ConsumerRecords<byte[], int> records = consumer.Poll(s_pollTimeout);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(recordCount, records.Count);
        // The value decoded to its span length — proving the deserializer saw the whole
        // value through the span, without a copy.
        foreach (ConsumerRecord<byte[], int> record in records)
        {
            Assert.Equal(valueSize, record.Value);
        }

        return after - before;
    }
}

#endif
