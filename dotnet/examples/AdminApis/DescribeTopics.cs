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

// Describes topics with IAdmin.DescribeTopics: each topic's id and its partitions, with their leader,
// replicas and in-sync replicas. With --authorized-operations the broker also returns the operations the
// caller may perform on each topic.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeTopics --authorized-operations orders

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;

/// <summary>The DescribeTopics example.</summary>
internal static class DescribeTopics
{
    public const string Arguments = "[--authorized-operations] <topic>...";

    public static void Run(IAdmin admin, Args args)
    {
        bool includeAuthorizedOperations = args.Flag("authorized-operations");
        string[] topics = args.Positional();
        if (topics.Length == 0)
        {
            throw new UsageException("no topic given");
        }

        DescribeTopicsResult result = admin.DescribeTopics(
            topics, new DescribeTopicsOptions().IncludeAuthorizedOperations(includeAuthorizedOperations));

        // A by-name call keys its futures by topic name.
        if (result.TopicNameValues() is not { } futures)
        {
            return;
        }

        foreach (string topic in topics)
        {
            try
            {
                TopicDescription description = futures[topic].Get();
                Console.WriteLine(
                    $"Topic {description.Name()} (id {description.TopicId()}){(description.IsInternal() ? " internal" : "")}");
                foreach (TopicPartitionInfo partition in description.Partitions())
                {
                    Console.WriteLine(
                        $"  partition {partition.Partition()}: leader {Cli.OrNone(partition.Leader()?.Id())}, " +
                        $"replicas {Ids(partition.Replicas())}, isr {Ids(partition.Isr())}");
                }

                // Absent unless the operations were asked for.
                if (description.AuthorizedOperations() is { } operations)
                {
                    Console.WriteLine($"  authorized operations: {string.Join(", ", operations.Order())}");
                }
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe topic {topic}", e);
            }
        }
    }

    private static string Ids(IEnumerable<Node> nodes)
    {
        return Cli.Ids(nodes.Select(node => node.Id()));
    }
}
