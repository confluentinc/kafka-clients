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

// Deletes consumer groups, with their committed offsets, with IAdmin.DeleteConsumerGroups. Only a group
// without active members can be deleted.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DeleteConsumerGroups orders-app payments-app

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DeleteConsumerGroups example.</summary>
internal static class DeleteConsumerGroups
{
    public const string Arguments = "<group>...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] groups = args.Positional();
        if (groups.Length == 0)
        {
            throw new UsageException("no group given");
        }

        IReadOnlyDictionary<string, KafkaFuture> futures = admin.DeleteConsumerGroups(groups).DeletedGroups();
        foreach (string group in groups)
        {
            try
            {
                futures[group].Get();
                Console.WriteLine($"Group {group} deleted");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to delete group {group}", e);
            }
        }
    }
}
