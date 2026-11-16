// Copyright 2026 Confluent Inc.
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

// Lists a group's committed offsets with IAdmin.ListConsumerGroupOffsets. Without partitions it lists every
// partition the group has committed an offset for; with partitions it lists those, reporting the ones
// without a committed offset. With --require-stable a partition with an offset commit in a transaction that
// is still open is left out of the listing, also when it was asked for by name; without it, that partition
// is listed with its last committed offset. The call does not wait for the transaction either way.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListConsumerGroupOffsets orders-app \
//         orders 0 orders 1

using System;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

/// <summary>The ListConsumerGroupOffsets example.</summary>
internal static class ListConsumerGroupOffsets
{
    public const string Arguments = "[--require-stable] <group> [<topic> <partition>]...";

    public static void Run(IAdmin admin, Args args)
    {
        bool requireStable = args.Flag("require-stable");
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no group given");
        }

        string group = positional[0];
        List<TopicPartition> partitions = Cli.ParseTopicPartitions(positional[1..], required: false);

        // No partitions (a null collection) means every partition with a committed offset.
        ListConsumerGroupOffsetsSpec spec =
            new ListConsumerGroupOffsetsSpec().TopicPartitions(partitions.Count == 0 ? null : partitions);
        Dictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs = new() { [group] = spec };
        ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            groupSpecs, new ListConsumerGroupOffsetsOptions().RequireStable(requireStable));

        // One future for the group: a failure is thrown by Get(), and reported by Program.cs.
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            result.PartitionsToOffsetAndMetadata(group).Get();
        Console.WriteLine($"Group {group}: {offsets.Count} partitions");
        foreach (TopicPartition partition in Cli.Sorted(offsets.Keys))
        {
            if (offsets[partition] is { } offset)
            {
                Console.WriteLine(
                    $"  {partition}: offset {offset.Offset()}, leader epoch {Cli.OrNone(offset.LeaderEpoch())}, " +
                    $"metadata \"{offset.Metadata()}\"");
            }
            else
            {
                Console.WriteLine($"  {partition}: no committed offset");
            }
        }
    }
}
