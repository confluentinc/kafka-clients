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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// Optional post-run correctness check for the producer benchmark (<c>VERIFY_CONSUMED</c>, off by
/// default) — the C# analog of <c>producer_performance_test.py</c>'s <c>get_topic_end_offsets</c> +
/// <c>verify_consumed_messages</c>. Captures the topic's per-partition end offsets before the run, then
/// consumes everything produced during the run from those baselines, asserting the count matches and (for
/// keyed records) that each landed in the partition <see cref="Crc32"/> would have chosen — the v3
/// producer's actual default partitioner (<c>design/current/partitioner.md</c>); there is no ABI surface to
/// select anything else. Uses our binding's <see cref="KafkaConsumer{TKey, TValue}"/> query APIs
/// (<c>PartitionsFor</c> / <c>BeginningOffsets</c> / <c>EndOffsets</c> / <c>Assign</c> / <c>Seek</c> / <c>Poll</c>).
/// </summary>
internal static class VerifyConsumed
{
    /// <summary>
    /// Captures <c>{partition: high_watermark}</c> for <paramref name="topic"/> before the run, or an empty
    /// map if the topic has no partitions yet (treated as all starting at 0). Throws on a transport/query
    /// failure — the caller catches it and skips verification (matching Python's baseline try/except).
    /// </summary>
    internal static Dictionary<int, long> CaptureBaseline(string bootstrapServers, string topic)
    {
        using var consumer = NewVerifier(bootstrapServers, "perf-baseline");
        IReadOnlyList<PartitionInfo> partitions = consumer.PartitionsFor(topic);
        var baseline = new Dictionary<int, long>();
        if (partitions.Count == 0)
        {
            return baseline;
        }

        var tps = new List<TopicPartition>(partitions.Count);
        foreach (PartitionInfo p in partitions)
        {
            tps.Add(new TopicPartition(topic, p.Partition));
        }

        IReadOnlyDictionary<TopicPartition, long> ends = consumer.EndOffsets(tps);
        foreach (TopicPartition tp in tps)
        {
            baseline[tp.Partition] = ends.TryGetValue(tp, out long high) ? high : 0;
        }

        return baseline;
    }

    /// <summary>
    /// Consumes everything produced during the run (from <paramref name="baseline"/> offsets), asserts the
    /// count equals <paramref name="expectedCount"/>, and — when <paramref name="hasKeys"/> — that every
    /// record landed in the CRC-32-chosen partition. Returns 0 on success, 1 on any verification failure.
    /// </summary>
    internal static int VerifyConsumedMessages(string bootstrapServers, string topic, Dictionary<int, long> baseline, long expectedCount, bool hasKeys)
    {
        long consumedCount = 0;
        long mismatchCount = 0;
        var sampleMismatches = new List<string>();

        // Scoped (not `using var`) so the consumer is closed BEFORE the summary is printed below —
        // matching Python's `finally: consumer.close()` running before its trailing `print()` blank
        // line and the verification summary, rather than after (a `using var` here would defer Dispose
        // to the method's end, i.e. after those prints).
        using (KafkaConsumer<byte[], byte[]> consumer = NewVerifier(bootstrapServers, "perf-verify"))
        {
            IReadOnlyList<PartitionInfo> partitions = consumer.PartitionsFor(topic);
            int numPartitions = partitions.Count;
            if (numPartitions == 0)
            {
                Console.WriteLine($"Verification: topic {topic} has no partitions");
                return 1;
            }

            var tps = new List<TopicPartition>(numPartitions);
            var partitionIds = new List<int>(numPartitions);
            foreach (PartitionInfo p in partitions)
            {
                tps.Add(new TopicPartition(topic, p.Partition));
                partitionIds.Add(p.Partition);
            }

            IReadOnlyDictionary<TopicPartition, long> lows = consumer.BeginningOffsets(tps);
            IReadOnlyDictionary<TopicPartition, long> highs = consumer.EndOffsets(tps);

            var targets = new Dictionary<int, long>();
            var current = new Dictionary<int, long>();
            consumer.Assign(tps);
            foreach (TopicPartition tp in tps)
            {
                long low = lows.TryGetValue(tp, out long l) ? l : 0;
                long start = Math.Max(low, baseline.TryGetValue(tp.Partition, out long b) ? b : 0);
                targets[tp.Partition] = highs.TryGetValue(tp, out long h) ? h : 0;
                current[tp.Partition] = start;
                consumer.Seek(tp, start);
            }

            const double EmptyBudgetSeconds = 60.0;
            double lastProgress = Monotonic();
            var pollTimeout = TimeSpan.FromSeconds(2);

            // Rolling progress line (Python's `progress_print_at` / `progress_print_step`): printed at
            // most once per step rather than per record, and overwritten in place via a trailing '\r'
            // (no newline) so a long verification run doesn't scroll the terminal.
            long progressPrintAt = 0;
            long progressPrintStep = Math.Max(10000, expectedCount / 20);

            while (!CaughtUp(partitionIds, current, targets))
            {
                ConsumerRecords<byte[], byte[]> records = consumer.Poll(pollTimeout);
                if (records.Count == 0)
                {
                    if (Monotonic() - lastProgress > EmptyBudgetSeconds)
                    {
                        break;
                    }

                    continue;
                }

                foreach (ConsumerRecord<byte[], byte[]> r in records)
                {
                    consumedCount++;
                    int p = r.Partition;
                    current[p] = Math.Max(current.TryGetValue(p, out long c) ? c : 0, r.Offset + 1);
                    if (hasKeys)
                    {
                        CheckKeyPartition(r, p, numPartitions, ref mismatchCount, sampleMismatches);
                    }

                    if (consumedCount >= progressPrintAt)
                    {
                        Console.Write($"Verification: consumed {consumedCount} messages so far\r");
                        progressPrintAt = consumedCount + progressPrintStep;
                    }
                }

                lastProgress = Monotonic();
            }
        }

        Console.WriteLine();
        bool countOk = consumedCount == expectedCount;
        bool partitionsOk = !hasKeys || mismatchCount == 0;

        Console.WriteLine($"Consumer verification: consumed={consumedCount} expected={expectedCount} (count_ok={countOk})");
        if (hasKeys)
        {
            Console.WriteLine($"Partition verification: mismatches={mismatchCount}/{consumedCount} (partitions_ok={partitionsOk})");
            if (sampleMismatches.Count > 0)
            {
                Console.WriteLine("First mismatches:");
                foreach (string s in sampleMismatches)
                {
                    Console.WriteLine($"  {s}");
                }
            }
        }
        else
        {
            Console.WriteLine("Partition verification: skipped (no keys)");
        }

        return countOk && partitionsOk ? 0 : 1;
    }

    private static void CheckKeyPartition(ConsumerRecord<byte[], byte[]> r, int partition, int numPartitions, ref long mismatchCount, List<string> samples)
    {
        byte[]? key = r.Key;
        if (key is null)
        {
            mismatchCount++;
            if (samples.Count < 5)
            {
                samples.Add($"partition={partition} offset={r.Offset} key=null (expected non-null)");
            }

            return;
        }

        int expectedPartition = Crc32.PartitionForKey(key, numPartitions);
        if (expectedPartition != partition)
        {
            mismatchCount++;
            if (samples.Count < 5)
            {
                samples.Add($"partition={partition} expected={expectedPartition} offset={r.Offset}");
            }
        }
    }

    private static bool CaughtUp(List<int> partitionIds, Dictionary<int, long> current, Dictionary<int, long> targets)
    {
        foreach (int p in partitionIds)
        {
            long cur = current.TryGetValue(p, out long c) ? c : 0;
            long target = targets.TryGetValue(p, out long t) ? t : 0;
            if (cur < target)
            {
                return false;
            }
        }

        return true;
    }

    private static KafkaConsumer<byte[], byte[]> NewVerifier(string bootstrapServers, string groupPrefix)
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = bootstrapServers,
            ["group.id"] = $"{groupPrefix}-{Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture)}",
            ["group.protocol"] = "consumer",
            ["enable.auto.commit"] = "false",
            ["auto.offset.reset"] = "earliest",
            ["session.timeout.ms"] = "10000",
            ["check.crcs"] = "true",
        };
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
        {
            config[kv.Key] = kv.Value;
        }

        return new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
    }

    private static double Monotonic() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;
}
