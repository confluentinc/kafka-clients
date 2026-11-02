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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// The optional <c>VERIFY_CONSUMED</c> check, as Python's <c>topic_end_offsets</c> and <c>verify_consumed_messages</c>:
/// it captures the end offsets before the run, then consumes what the run produced and checks the count and, for keyed
/// records, that each landed where <see cref="Crc32"/>, the core's default partitioner, puts it.
/// </summary>
internal static class VerifyConsumed
{
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

    internal static int VerifyConsumedMessages(string bootstrapServers, string topic, Dictionary<int, long> baseline, long expectedCount, bool hasKeys)
    {
        long consumedCount = 0;
        long mismatchCount = 0;
        var sampleMismatches = new List<string>();

        // Scoped, so the consumer closes before the summary prints, as Python's finally does.
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
