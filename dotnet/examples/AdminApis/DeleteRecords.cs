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

// Deletes the records of partitions up to an offset with IAdmin.DeleteRecords. Every record before the
// given offset is removed and the partition's low watermark moves to it; offset -1 deletes up to the high
// watermark, that is every record currently in the partition.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DeleteRecords orders 0 100 orders 1 -1

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DeleteRecords example.</summary>
internal static class DeleteRecords
{
    public const string Arguments = "<topic> <partition> <offset>...";

    public static void Run(IAdmin admin, Args args)
    {
        List<TopicPartition> partitions = [];
        Dictionary<TopicPartition, RecordsToDelete> recordsToDelete = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 3, required: true))
        {
            TopicPartition partition = Cli.ParseTopicPartition(group);
            long offset = Cli.Parse<long>("offset", group[2]);
            partitions.Add(partition);
            recordsToDelete[partition] = RecordsToDelete.BeforeOffset(offset);
        }

        DeleteRecordsResult result = admin.DeleteRecords(recordsToDelete);
        foreach (TopicPartition partition in partitions)
        {
            try
            {
                DeletedRecords deleted = result.LowWatermarks()[partition].Get();
                Console.WriteLine($"Deleted records of partition {partition}: low watermark {deleted.LowWatermark()}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to delete records of partition {partition}", e);
            }
        }
    }
}
