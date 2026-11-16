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

// Removes members from a classic consumer group with IAdmin.RemoveMembersFromConsumerGroup. Given group
// instance ids, it removes those static members; given none, it removes every member of the group, failing
// when the group has none. --reason is sent to the broker with the request.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 RemoveMembersFromConsumerGroup \
//         --reason=maintenance orders-app instance-1

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The RemoveMembersFromConsumerGroup example.</summary>
internal static class RemoveMembersFromConsumerGroup
{
    public const string Arguments = "[--reason=text] <group> [<instance_id>]...";

    public static void Run(IAdmin admin, Args args)
    {
        string? reason = args.Value("reason");
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no group given");
        }

        string group = positional[0];
        List<MemberToRemove> members = [.. positional[1..].Select(instanceId => new MemberToRemove(instanceId))];

        // The parameterless options remove every member of the group.
        RemoveMembersFromConsumerGroupOptions options = members.Count == 0
            ? new RemoveMembersFromConsumerGroupOptions()
            : new RemoveMembersFromConsumerGroupOptions(members);
        options.Reason(reason);

        RemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(group, options);
        if (members.Count == 0)
        {
            try
            {
                result.All().Get();
                Console.WriteLine($"Group {group}: all members removed");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to remove all members from group {group}", e);
            }

            return;
        }

        foreach (MemberToRemove member in members)
        {
            string instanceId = member.GroupInstanceId();
            try
            {
                result.MemberResult(member).Get();
                Console.WriteLine($"Group {group}: member {instanceId} removed");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to remove member {instanceId} from group {group}", e);
            }
        }
    }
}
