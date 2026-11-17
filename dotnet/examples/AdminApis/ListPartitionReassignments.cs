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

// Lists the ongoing partition reassignments with IAdmin.ListPartitionReassignments: per partition its current
// replicas and the replicas being added and removed. Without partitions it lists every reassignment in the
// cluster.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListPartitionReassignments orders 0 orders 1

using System;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The ListPartitionReassignments example.</summary>
internal static class ListPartitionReassignments
{
    public const string Arguments = "[<topic> <partition>]...";

    public static void Run(IAdmin admin, Args args)
    {
        List<TopicPartition> partitions = Cli.ParseTopicPartitions(args.Positional(), required: false);

        // No partitions asks for every reassignment; an empty collection would ask for none.
        ListPartitionReassignmentsResult result = partitions.Count == 0
            ? admin.ListPartitionReassignments()
            : admin.ListPartitionReassignments(partitions);
        IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments = result.Reassignments().Get();
        if (reassignments.Count == 0)
        {
            Console.WriteLine("No ongoing reassignments");
            return;
        }

        foreach (TopicPartition partition in Cli.Sorted(reassignments.Keys))
        {
            PartitionReassignment reassignment = reassignments[partition];
            Console.WriteLine(
                $"Partition {partition}: replicas {Cli.Ids(reassignment.Replicas())}, " +
                $"adding {Cli.Ids(reassignment.AddingReplicas())}, " +
                $"removing {Cli.Ids(reassignment.RemovingReplicas())}");
        }
    }
}
