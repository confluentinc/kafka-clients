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

// Describes consumer groups with IAdmin.DescribeConsumerGroups: the group's type, state, partition assignor
// and coordinator, and per member its ids, host and assigned partitions. Both consumer (KIP-848) and classic
// groups are described. With --authorized-operations the broker also returns the operations the caller may
// perform on each group.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeConsumerGroups \
//         --authorized-operations orders-app

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeConsumerGroups example.</summary>
internal static class DescribeConsumerGroups
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

        DescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(
            groups, new DescribeConsumerGroupsOptions().IncludeAuthorizedOperations(includeAuthorizedOperations));
        IReadOnlyDictionary<string, KafkaFuture<ConsumerGroupDescription>> futures = result.DescribedGroups();
        foreach (string group in groups)
        {
            try
            {
                ConsumerGroupDescription description = futures[group].Get();
                Console.WriteLine(
                    $"Group {description.GroupId()}: type {GroupTypeMethods.ToString(description.Type())}, " +
                    $"state {GroupStateMethods.ToString(description.GroupState())}, " +
                    $"assignor \"{description.PartitionAssignor()}\", " +
                    $"coordinator {Cli.OrNone(description.Coordinator()?.Id())}, " +
                    $"simple {(description.IsSimpleConsumerGroup() ? "true" : "false")}");

                // Only consumer (KIP-848) groups have epochs.
                if (description.GroupEpoch() is { } groupEpoch
                    && description.TargetAssignmentEpoch() is { } targetAssignmentEpoch)
                {
                    Console.WriteLine(
                        $"  group epoch {groupEpoch}, target assignment epoch {targetAssignmentEpoch}");
                }

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
