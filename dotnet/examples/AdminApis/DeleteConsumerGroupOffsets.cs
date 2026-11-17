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

// Deletes a group's committed offsets for the given partitions with IAdmin.DeleteConsumerGroupOffsets. The
// broker refuses to delete the offsets of a topic the group is still subscribed to.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DeleteConsumerGroupOffsets orders-app \
//         orders 0 orders 1

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DeleteConsumerGroupOffsets example.</summary>
internal static class DeleteConsumerGroupOffsets
{
    public const string Arguments = "<group> (<topic> <partition>)...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no group given");
        }

        string group = positional[0];

        // Each partition once, in command-line order.
        List<TopicPartition> partitions = [.. Cli.ParseTopicPartitions(positional[1..], required: true).Distinct()];

        DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(group, partitions);
        foreach (TopicPartition partition in partitions)
        {
            try
            {
                result.PartitionResult(partition).Get();
                Console.WriteLine($"Group {group}: {partition} offset deleted");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to delete group {group} offset for {partition}", e);
            }
        }
    }
}
