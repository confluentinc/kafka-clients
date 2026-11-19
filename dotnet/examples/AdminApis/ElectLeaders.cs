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

// Elects partition leaders with IAdmin.ElectLeaders. A preferred election moves leadership back to the first
// replica of each partition; an unclean election picks a leader among out-of-sync replicas when no in-sync
// replica is alive, which may lose records. Without partitions it runs on every partition of the cluster.
//
// A partition whose preferred replica already leads fails with ElectionNotNeededException, which only means
// there was nothing to do; when no partition is given, the broker leaves such partitions out of the result.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ElectLeaders preferred orders 0 orders 1

using System;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The ElectLeaders example.</summary>
internal static class ElectLeaders
{
    public const string Arguments = "<preferred|unclean> [<topic> <partition>]...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no election type given");
        }

        ElectionType electionType = Cli.Choose(
            "election type",
            positional[0],
            ("preferred", ElectionType.PREFERRED),
            ("unclean", ElectionType.UNCLEAN));
        List<TopicPartition> partitions = Cli.ParseTopicPartitions(positional[1..], required: false);

        // Null partitions elects on every partition; an empty collection would elect on none.
        ElectLeadersResult result = admin.ElectLeaders(electionType, partitions.Count == 0 ? null : partitions);

        // One future for the whole election: a partition's own failure is a value in its map.
        IReadOnlyDictionary<TopicPartition, Exception?> outcomes = result.Partitions().Get();

        // Asked for every partition, the broker leaves out those already led by their preferred replica.
        if (outcomes.Count == 0)
        {
            Console.WriteLine("No leader election needed");
            return;
        }

        foreach (TopicPartition partition in Cli.Sorted(outcomes.Keys))
        {
            if (outcomes[partition] is { } error)
            {
                Cli.Fail($"Failed to elect leader for {partition}", error);
            }
            else
            {
                Console.WriteLine($"Leader elected for {partition}");
            }
        }
    }
}
