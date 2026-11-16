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

// Describes classic groups with IAdmin.DescribeClassicGroups: the group's state, protocol type, protocol and
// coordinator, and per member its ids, host and assigned partitions. With --authorized-operations the broker
// also returns the operations the caller may perform on each group.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeClassicGroups \
//         --authorized-operations legacy-app

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeClassicGroups example.</summary>
internal static class DescribeClassicGroups
{
    public const string Arguments = "[--authorized-operations] <group>...";

    public static void Run(IAdmin admin, Args args)
    {
        bool includeAuthorizedOperations = args.Flag("authorized-operations");
        string[] groups = args.Positional();
        if (groups.Length == 0)
        {
            throw new UsageException("no group given");
        }

        DescribeClassicGroupsResult result = admin.DescribeClassicGroups(
            groups, new DescribeClassicGroupsOptions().IncludeAuthorizedOperations(includeAuthorizedOperations));
        IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>> futures = result.DescribedGroups();
        foreach (string group in groups)
        {
            try
            {
                ClassicGroupDescription description = futures[group].Get();

                // Protocol() is the protocol type ("consumer" for consumer groups), ProtocolData() the chosen
                // protocol (the assignor).
                Console.WriteLine(
                    $"Group {description.GroupId()}: " +
                    $"state {ClassicGroupStateMethods.ToString(description.State())}, " +
                    $"protocol \"{description.Protocol()}\", protocol data \"{description.ProtocolData()}\", " +
                    $"coordinator {Cli.OrNone(description.Coordinator()?.Id())}, " +
                    $"simple {(description.IsSimpleConsumerGroup() ? "true" : "false")}");
                foreach (MemberDescription member in description.Members())
                {
                    Console.WriteLine(
                        $"  member {member.ConsumerId()}: group instance id {member.GroupInstanceId() ?? "none"}, " +
                        $"client id \"{member.ClientId()}\", host {member.Host()}, " +
                        $"assignment {Partitions(member.Assignment())}");
                }

                // Absent unless the operations were asked for.
                if (description.AuthorizedOperations() is { } operations)
                {
                    Console.WriteLine($"  authorized operations: {string.Join(", ", operations.Order())}");
                }
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe group {group}", e);
            }
        }
    }

    private static string Partitions(MemberAssignment assignment)
    {
        return $"[{string.Join(",", Cli.Sorted(assignment.TopicPartitions()))}]";
    }
}
