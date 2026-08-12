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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetTotalAllocatedBytes has no net462 equivalent, and this is a runtime-behavior test
// (net462 runs are Windows/CI-only) — so the whole class is net8.0+ only, keeping the net462
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
/// The copy-out runs on the foreign dispatcher thread (async poll), so a per-thread counter
/// would miss it — use the process-wide precise <see cref="GC.GetTotalAllocatedBytes(bool)"/>
/// and the marginal (large − small) subtraction to cancel the fixed per-poll and per-record
/// overhead (record object, topic string, list slot — identical in both measurements).
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
    public async Task TypedPoll_LargeValue_AddsNoValueSizedIntermediateAllocation()
    {
        // Warmup — JIT + first-poll fixed costs out of the way.
        for (int i = 0; i < 5; i++)
        {
            await MeasurePoll(RecordCount, SmallValueSize);
        }

        long smallBytes = await MeasurePoll(RecordCount, SmallValueSize);
        long largeBytes = await MeasurePoll(RecordCount, LargeValueSize);

        long marginalPerRecord = (largeBytes - smallBytes) / RecordCount;

        Assert.True(
            marginalPerRecord <= MarginalPerRecordBudgetBytes,
            $"Typed poll added {marginalPerRecord} B/record going from a {SmallValueSize} B value to a " +
            $"{LargeValueSize} B value (budget {MarginalPerRecordBudgetBytes} B; small={smallBytes} B, " +
            $"large={largeBytes} B over {RecordCount} records). A per-record intermediate value-sized " +
            "byte[] on the key/value path would show ~the value-size delta here — the deserialize is NOT zero-copy.");
    }

    private static async Task<long> MeasurePoll(int recordCount, int valueSize)
    {
        SpanLengthDeserializer valueDeserializer = new SpanLengthDeserializer();
        byte[] key = new byte[8];
        byte[] value = new byte[valueSize];

        AsyncMockConsumer<byte[], int> consumer =
            new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, valueDeserializer);
        try
        {
            await consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
            consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
            for (int i = 0; i < recordCount; i++)
            {
                consumer.AddRecord(Topic, Partition, offset: i, key, value);
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long before = GC.GetTotalAllocatedBytes(precise: true);
            ConsumerRecords<byte[], int> records = await consumer.Poll(s_pollTimeout);
            long after = GC.GetTotalAllocatedBytes(precise: true);

            Assert.Equal(recordCount, records.Count);
            // The value decoded to its span length — proving the deserializer saw the whole
            // value through the span, without a copy.
            foreach (ConsumerRecord<byte[], int> record in records)
            {
                Assert.Equal(valueSize, record.Value);
            }

            return after - before;
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }
}

#endif
