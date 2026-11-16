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

// Sets a group's committed offsets with IAdmin.AlterConsumerGroupOffsets. The group must have no active
// members: the broker rejects the change while consumers are in the group. A group that does not exist yet
// is created with the given offsets.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 AlterConsumerGroupOffsets orders-app \
//         orders 0 42 orders 1 17

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

/// <summary>The AlterConsumerGroupOffsets example.</summary>
internal static class AlterConsumerGroupOffsets
{
    public const string Arguments = "<group> (<topic> <partition> <offset>)...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no group given");
        }

        string group = positional[0];
        List<TopicPartition> partitions = [];
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = [];
        foreach (string[] spec in Cli.Groups(positional[1..], 3, required: true))
        {
            TopicPartition partition = Cli.ParseTopicPartition(spec);
            OffsetAndMetadata offset = new(Cli.Parse<long>("offset", spec[2]));

            // One offset per partition: a later one for the same partition wins.
            if (!offsets.ContainsKey(partition))
            {
                partitions.Add(partition);
            }

            offsets[partition] = offset;
        }

        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(group, offsets);
        foreach (TopicPartition partition in partitions)
        {
            try
            {
                result.PartitionResult(partition).Get();
                Console.WriteLine($"Group {group}: {partition} offset set to {offsets[partition].Offset()}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to alter group {group} offset for {partition}", e);
            }
        }
    }
}
