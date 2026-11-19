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

// Starts or cancels partition reassignments with IAdmin.AlterPartitionReassignments. Each partition is given
// as topic and partition followed by its new replicas, a comma-separated list of broker ids whose first entry
// becomes the preferred leader, or by cancel to stop an ongoing reassignment. The controller moves the
// replicas in the background; the ListPartitionReassignments example shows the ones still in progress.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 AlterPartitionReassignments \
//         orders 0 1,2,3 orders 1 cancel

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The AlterPartitionReassignments example.</summary>
internal static class AlterPartitionReassignments
{
    public const string Arguments = "<topic> <partition> <broker_id,...|cancel>...";

    public static void Run(IAdmin admin, Args args)
    {
        List<TopicPartition> partitions = [];
        Dictionary<TopicPartition, NewPartitionReassignment?> reassignments = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 3, required: true))
        {
            TopicPartition partition = Cli.ParseTopicPartition(group);

            // A null reassignment cancels the partition's reassignment.
            NewPartitionReassignment? reassignment = null;
            if (group[2] != "cancel")
            {
                List<int> replicas = [];
                foreach (string text in Cli.CommaList(group[2]))
                {
                    replicas.Add(Cli.Parse<int>("broker id", text));
                }

                if (replicas.Count == 0)
                {
                    throw new UsageException($"no replicas given for partition {partition}");
                }

                reassignment = new NewPartitionReassignment(replicas);
            }

            // One reassignment per partition: a later one for the same partition wins.
            if (!reassignments.ContainsKey(partition))
            {
                partitions.Add(partition);
            }

            reassignments[partition] = reassignment;
        }

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(reassignments);
        foreach (TopicPartition partition in partitions)
        {
            NewPartitionReassignment? reassignment = reassignments[partition];
            try
            {
                result.Values()[partition].Get();
                Console.WriteLine(
                    reassignment is null
                        ? $"Reassignment of partition {partition} cancelled"
                        : $"Reassignment of partition {partition} to {Cli.Ids(reassignment.TargetReplicas())} started");
            }
            catch (ExecutionException e)
            {
                Cli.Fail(
                    reassignment is null
                        ? $"Failed to cancel the reassignment of partition {partition}"
                        : $"Failed to reassign partition {partition} to {Cli.Ids(reassignment.TargetReplicas())}",
                    e);
            }
        }
    }
}
