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

// Describes the log directories of brokers with IAdmin.DescribeLogDirs: per directory its total and usable
// bytes and, per replica it holds, the replica's size and offset lag. A future replica is one the broker is
// moving into that directory.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeLogDirs 0 1 2

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeLogDirs example.</summary>
internal static class DescribeLogDirs
{
    public const string Arguments = "<broker_id>...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no broker given");
        }

        List<int> brokers = [];
        foreach (string text in positional)
        {
            int broker = Cli.Parse<int>("broker id", text);
            if (!brokers.Contains(broker))
            {
                brokers.Add(broker);
            }
        }

        DescribeLogDirsResult result = admin.DescribeLogDirs(brokers);
        foreach (int broker in brokers)
        {
            IReadOnlyDictionary<string, LogDirDescription> descriptions;
            try
            {
                descriptions = result.Descriptions()[broker].Get();
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe log dirs of broker {broker}", e);
                continue;
            }

            Console.WriteLine($"Broker {broker}: {descriptions.Count} log dirs");
            foreach ((string dir, LogDirDescription description) in
                descriptions.OrderBy(entry => entry.Key, StringComparer.Ordinal))
            {
                // An offline or unreadable directory: the broker's future succeeded, the directory has an error.
                if (description.Error() is { } error)
                {
                    Cli.Fail($"Failed to describe log dir {dir} of broker {broker}", error);
                    continue;
                }

                Console.WriteLine(
                    $"  log dir {dir}: total {Bytes(description.TotalBytes())} bytes, " +
                    $"usable {Bytes(description.UsableBytes())} bytes{(description.IsCordoned() ? ", cordoned" : "")}");
                IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicas = description.ReplicaInfos();
                foreach (TopicPartition partition in Cli.Sorted(replicas.Keys))
                {
                    ReplicaInfo info = replicas[partition];
                    Console.WriteLine(
                        $"    replica {partition}: size {info.Size()}, offset lag {info.OffsetLag()}" +
                        (info.IsFuture() ? ", future" : ""));
                }
            }
        }
    }

    // The size, or unknown when the broker did not report it.
    private static string Bytes(long? value)
    {
        return value?.ToString(CultureInfo.InvariantCulture) ?? "unknown";
    }
}
