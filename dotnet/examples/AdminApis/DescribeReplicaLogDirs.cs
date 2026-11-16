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

// Describes where replicas live with IAdmin.DescribeReplicaLogDirs: for each replica, given as topic,
// partition and broker id, the log directory that holds it and, while the broker moves it, the directory it
// moves to.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeReplicaLogDirs orders 0 1 orders 1 2

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeReplicaLogDirs example.</summary>
internal static class DescribeReplicaLogDirs
{
    public const string Arguments = "<topic> <partition> <broker_id>...";

    public static void Run(IAdmin admin, Args args)
    {
        List<TopicPartitionReplica> replicas = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 3, required: true))
        {
            TopicPartitionReplica replica = new(
                group[0], Cli.Parse<int>("partition", group[1]), Cli.Parse<int>("broker id", group[2]));
            if (!replicas.Contains(replica))
            {
                replicas.Add(replica);
            }
        }

        DescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(replicas);
        foreach (TopicPartitionReplica replica in replicas)
        {
            try
            {
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = result.Values()[replica].Get();
                string line =
                    $"Replica {replica}: log dir {info.GetCurrentReplicaLogDir() ?? "none"}, " +
                    $"offset lag {info.GetCurrentReplicaOffsetLag()}";
                if (info.GetFutureReplicaLogDir() is { } futureDir)
                {
                    line += $", moving to {futureDir}, future offset lag {info.GetFutureReplicaOffsetLag()}";
                }

                Console.WriteLine(line);
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe replica {replica}", e);
            }
        }
    }
}
