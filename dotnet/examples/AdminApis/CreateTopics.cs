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

// Creates topics with IAdmin.CreateTopics.
//
// Each topic takes an optional partition count and replication factor; a missing one uses the broker's
// default (num.partitions, default.replication.factor). With --validate-only the broker checks the request
// without creating anything.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 CreateTopics orders:3:1 payments

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The CreateTopics example.</summary>
internal static class CreateTopics
{
    public const string Arguments = "[--validate-only] <topic>[:partitions[:replication_factor]]...";

    public static void Run(IAdmin admin, Args args)
    {
        bool validateOnly = args.Flag("validate-only");
        string[] specs = args.Positional();
        if (specs.Length == 0)
        {
            throw new UsageException("no topic given");
        }

        List<NewTopic> newTopics = [];
        foreach (string spec in specs)
        {
            string[] parts = spec.Split(':');
            int? numPartitions = parts.Length > 1 ? Cli.Parse<int>("partition count", parts[1]) : null;
            short? replicationFactor = parts.Length > 2 ? Cli.Parse<short>("replication factor", parts[2]) : null;
            if (parts[0].Length == 0 || parts.Length > 3)
            {
                throw new UsageException($"invalid topic spec \"{spec}\"");
            }

            try
            {
                newTopics.Add(new NewTopic(parts[0], numPartitions, replicationFactor));
            }
            catch (ArgumentOutOfRangeException e)
            {
                // NewTopic refuses a count below -1 itself; the other topics are still created.
                Cli.Fail($"Failed to create topic {parts[0]}", e);
            }
        }

        // The call returns at once; each topic's outcome arrives on its own future.
        CreateTopicsResult result = admin.CreateTopics(newTopics, new CreateTopicsOptions().ValidateOnly(validateOnly));
        string verb = validateOnly ? "validated" : "created";
        foreach (NewTopic newTopic in newTopics)
        {
            string name = newTopic.Name();
            try
            {
                result.Values()[name].Get();
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to create topic {name}", e);
                continue;
            }

            // The broker also returns what it created: these refinements of the topic's future fail on their
            // own when it withholds them, for example from a caller without DESCRIBE_CONFIGS on the topic,
            // which is still created.
            try
            {
                Uuid topicId = result.TopicId(name).Get();
                int partitions = result.NumPartitions(name).Get();
                int replicas = result.ReplicationFactor(name).Get();
                Console.WriteLine(
                    $"Topic {name} {verb}: id {topicId}, {partitions} partitions, replication factor {replicas}");
            }
            catch (ExecutionException e)
            {
                Console.WriteLine($"Topic {name} {verb}; its details were not returned: {Cli.Describe(e)}");
            }
        }
    }
}
