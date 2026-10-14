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
/// M9/P4 M6 — the receive path must NOT allocate a managed <see cref="string"/> for the topic on
/// every record. <c>ffi-marshalling.md §B4</c>'s allocation budget requires "no allocation
/// attributable to <b>topic name</b>", and <c>consumer-threading.md §27</c> names the per-record
/// topic decode verbatim as an anti-pattern.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this test exists rather than leaning on the two pre-existing budget tests: both are
/// structurally blind to the topic.</b> <c>PublicConsumerTypedAllocationBudgetTests</c> varies only
/// the value size at a fixed record count and a fixed topic, so its topic strings appear
/// identically on both sides of the subtraction and <b>cancel exactly</b>.
/// <c>PublicSyncConsumerAllocationBudgetTests</c> does include the topic string, but its ceiling
/// was ~4x the actual cost, so removing ~56 B/record moved the measurement well inside a bound it
/// was never close to — it could not fail on this, before or after.
/// </para>
/// <para>
/// <b>The construction that CAN fail.</b> Hold the record count fixed and vary <b>only the topic
/// name length</b>. A per-record decode costs one managed string per record, so the delta between
/// an 8-character topic and a 1008-character topic is <c>recordCount x 2 x 1000</c> bytes (managed
/// strings are UTF-16) — about 2000 B/record. With one decode per distinct topic per batch, the
/// delta collapses to a single string for the whole batch. Measured on this branch:
/// <b>2000 B/record before the memo, 7 B/record after</b> (256 records, so the one remaining
/// ~2 KB string amortizes to ~8 B). The 32 B/record bound therefore fails by ~62x on the bug and
/// passes with ~4x headroom after it.
/// </para>
/// <para>
/// ⚠ <b><see cref="GC.GetAllocatedBytesForCurrentThread"/> is load-bearing here, not
/// defensive.</b> <c>DisableTestParallelization</c> is <c>false</c>, so this test genuinely runs
/// concurrently with the rest of the assembly; the process-wide
/// <see cref="GC.GetTotalAllocatedBytes(bool)"/> would pick up other tests' allocations and flake.
/// For the same reason the measured region must not <c>await</c> — a continuation can resume on a
/// different thread and silently split the measurement. Any future allocation assertion must
/// preserve both properties.
/// </para>
/// </remarks>
public sealed class PublicConsumerTopicAllocationBudgetTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const int Partition = 0;
    private const int RecordCount = 256;

    private const int ShortTopicLength = 8;
    private const int LongTopicLength = 1008;

    private const int KeySize = 16;
    private const int ValueSize = 64;

    // Measured on this branch: ~2000 B/record with a per-record decode, ~7 B/record with the
    // one-decode-per-distinct-topic memo (the single remaining ~2 KB string amortized over 256
    // records). 32 B/record sits ~4x above the post-fix value and ~62x below the pre-fix value,
    // so it is a real gate with room for runtime/TFM variation.
    private const long PerRecordTopicBudgetBytes = 32;

    [Fact]
    public void Poll_LongerTopicName_AddsNoPerRecordTopicAllocation()
    {
        string shortTopic = new string('s', ShortTopicLength);
        string longTopic = new string('l', LongTopicLength);
        byte[] key = MakeBytes(KeySize, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);

        // Warmup — JIT + first-poll fixed costs out of the way before measuring.
        for (int i = 0; i < 5; i++)
        {
            MeasurePoll(shortTopic, RecordCount, key, value);
        }

        long shortBytes = MeasurePoll(shortTopic, RecordCount, key, value);
        long longBytes = MeasurePoll(longTopic, RecordCount, key, value);

        long perRecord = (longBytes - shortBytes) / RecordCount;

        Assert.True(
            perRecord <= PerRecordTopicBudgetBytes,
            $"Poll added {perRecord} B/record going from a {ShortTopicLength}-char topic to a " +
            $"{LongTopicLength}-char topic (budget {PerRecordTopicBudgetBytes} B; short={shortBytes} B, " +
            $"long={longBytes} B over {RecordCount} records). A per-record managed topic string would " +
            $"show ~{2 * (LongTopicLength - ShortTopicLength)} B/record here — the topic decode is NOT " +
            "shared across the batch (ffi §B4, consumer-threading §27).");
    }

    private static long MeasurePoll(string topic, int recordCount, byte[] key, byte[] value)
    {
        // Assign + Seek the SAME topic that is AddRecord'ed: the Rust mock rejects a record on an
        // unassigned partition, and the seek is what makes a broker-free poll return the records.
        using MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(topic, Partition) });
        consumer.Seek(new TopicPartition(topic, Partition), offset: 0);
        for (int i = 0; i < recordCount; i++)
        {
            consumer.AddRecord(topic, Partition, offset: i, key, value);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // No await inside the measured region (see the class remarks).
        long before = GC.GetAllocatedBytesForCurrentThread();
        ConsumerRecords<byte[], byte[]> records = consumer.Poll(s_pollTimeout);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(recordCount, records.Count);
        return after - before;
    }

    [Fact]
    public void Poll_SharedTopicString_IsReferenceEqualAcrossRecordsOfOneBatch()
    {
        // The direct, allocation-counter-free statement of the same fact: within one batch every
        // record of a partition group carries the SAME string instance, exactly as the Rust core
        // shares one Arc<str> and Java shares one topic string per partition group. This is what
        // makes the byte-comparison fallback observable on the mock path, where every add_record
        // allocates a fresh Arc<str> so the pointers all differ.
        string topic = new string('r', ShortTopicLength);
        byte[] value = MakeBytes(ValueSize, 0xEE);

        using MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(topic, Partition) });
        consumer.Seek(new TopicPartition(topic, Partition), offset: 0);
        for (int i = 0; i < 8; i++)
        {
            consumer.AddRecord(topic, Partition, offset: i, null, value);
        }

        ConsumerRecords<byte[], byte[]> records = consumer.Poll(s_pollTimeout);
        Assert.Equal(8, records.Count);

        string? first = null;
        foreach (ConsumerRecord<byte[], byte[]> record in records)
        {
            Assert.Equal(topic, record.Topic);
            if (first is null)
            {
                first = record.Topic;
            }
            else
            {
                // ReferenceEquals, not Equal: a per-record decode would produce equal-but-distinct
                // instances and pass an equality check while still allocating one string per record.
                Assert.True(
                    ReferenceEquals(first, record.Topic),
                    "Each record decoded its own topic string — the per-batch topic memo is not being hit.");
            }
        }
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
}

#endif
