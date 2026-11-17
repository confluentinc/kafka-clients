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

// Moves replicas between the log directories of their broker with IAdmin.AlterReplicaLogDirs. Each replica
// is given as topic, partition and broker id, followed by the absolute path of the target directory, which
// must be one of the broker's log.dirs. The broker copies the replica in the background; the
// DescribeReplicaLogDirs example shows the progress.
//
// Needs a broker configured with several log.dirs.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 AlterReplicaLogDirs \
//         orders 0 1 /var/lib/kafka/data-2

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The AlterReplicaLogDirs example.</summary>
internal static class AlterReplicaLogDirs
{
    public const string Arguments = "<topic> <partition> <broker_id> <log_dir>...";

    public static void Run(IAdmin admin, Args args)
    {
        List<TopicPartitionReplica> replicas = [];
        Dictionary<TopicPartitionReplica, string> assignment = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 4, required: true))
        {
            TopicPartitionReplica replica = new(
                group[0], Cli.Parse<int>("partition", group[1]), Cli.Parse<int>("broker id", group[2]));

            // One move per replica: a later directory for the same replica wins.
            if (!assignment.ContainsKey(replica))
            {
                replicas.Add(replica);
            }

            assignment[replica] = group[3];
        }

        AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(assignment);
        foreach (TopicPartitionReplica replica in replicas)
        {
            string logDir = assignment[replica];
            try
            {
                result.Values()[replica].Get();
                Console.WriteLine($"Replica {replica} moving to {logDir}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to move replica {replica} to {logDir}", e);
            }
        }
    }
}
