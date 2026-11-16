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

// Adds partitions to existing topics with IAdmin.CreatePartitions. Each topic is followed by the total
// number of partitions it should have, which must be larger than its current count: Kafka never removes
// partitions. The broker places the new partitions' replicas.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 CreatePartitions orders 6 payments 4

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The CreatePartitions example.</summary>
internal static class CreatePartitions
{
    public const string Arguments = "<topic> <total_partitions>...";

    public static void Run(IAdmin admin, Args args)
    {
        List<(string Topic, int Total)> topics = [];
        Dictionary<string, NewPartitions> newPartitions = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 2, required: true))
        {
            int total = Cli.Parse<int>("partition count", group[1]);
            topics.Add((group[0], total));
            newPartitions[group[0]] = NewPartitions.IncreaseTo(total);
        }

        CreatePartitionsResult result = admin.CreatePartitions(newPartitions);
        foreach ((string topic, int total) in topics)
        {
            try
            {
                result.Values()[topic].Get();
                Console.WriteLine($"Topic {topic} now has {total} partitions");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to add partitions to topic {topic}", e);
            }
        }
    }
}
