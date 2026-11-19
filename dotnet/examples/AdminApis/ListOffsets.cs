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

// Looks up partition offsets with IAdmin.ListOffsets. Each partition is given as topic and partition
// followed by what to look up: earliest, latest, max_timestamp (the record with the highest timestamp),
// earliest_local and latest_tiered (tiered storage), earliest_pending_upload, or timestamp:<ms> (the first
// record at or after that time). With read_committed the latest offset stops at the last stable offset,
// before any open transaction.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListOffsets read_uncommitted \
//         orders 0 earliest orders 1 timestamp:1700000000000

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The ListOffsets example.</summary>
internal static class ListOffsets
{
    public const string Arguments =
        "<read_committed|read_uncommitted> <topic> <partition> " +
        "<earliest|latest|max_timestamp|earliest_local|latest_tiered|earliest_pending_upload|timestamp:ms>...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no isolation level given");
        }

        IsolationLevel isolationLevel = Cli.Choose(
            "isolation level",
            positional[0],
            ("read_committed", IsolationLevel.READ_COMMITTED),
            ("read_uncommitted", IsolationLevel.READ_UNCOMMITTED));

        List<TopicPartition> partitions = [];
        Dictionary<TopicPartition, OffsetSpec> offsetSpecs = [];
        foreach (string[] group in Cli.Groups(positional[1..], 3, required: true))
        {
            TopicPartition partition = Cli.ParseTopicPartition(group);

            // One lookup per partition: a later spec for the same partition wins.
            if (!offsetSpecs.ContainsKey(partition))
            {
                partitions.Add(partition);
            }

            offsetSpecs[partition] = ParseOffsetSpec(group[2]);
        }

        ListOffsetsResult result = admin.ListOffsets(offsetSpecs, new ListOffsetsOptions(isolationLevel));
        foreach (TopicPartition partition in partitions)
        {
            try
            {
                ListOffsetsResult.ListOffsetsResultInfo info = result.PartitionResult(partition).Get();
                Console.WriteLine(
                    $"{partition}: offset {info.Offset()}, timestamp {info.Timestamp()}, " +
                    $"leader epoch {Cli.OrNone(info.LeaderEpoch())}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to list offsets of {partition}", e);
            }
        }
    }

    private static OffsetSpec ParseOffsetSpec(string text)
    {
        const string Timestamp = "timestamp:";
        if (text.StartsWith(Timestamp, StringComparison.Ordinal))
        {
            return OffsetSpec.ForTimestamp(Cli.Parse<long>("timestamp", text[Timestamp.Length..]));
        }

        return Cli.Choose(
            "offset spec",
            text,
            ("earliest", OffsetSpec.Earliest()),
            ("latest", OffsetSpec.Latest()),
            ("max_timestamp", OffsetSpec.MaxTimestamp()),
            ("earliest_local", OffsetSpec.EarliestLocal()),
            ("latest_tiered", OffsetSpec.LatestTiered()),
            ("earliest_pending_upload", OffsetSpec.EarliestPendingUpload()));
    }
}
