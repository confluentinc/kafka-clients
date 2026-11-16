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

// One example per Admin API. The first argument is the brokers, the second names the example to run, and
// the rest are that example's own:
//
//     dotnet run -c Release --project AdminApis -- <bootstrap-brokers> <example> [-X key=value]... [options] <args>
//     dotnet run -c Release --project AdminApis -- <bootstrap-brokers> <example> --help
//
// Each example is one file, named after the IAdmin method it calls. Every admin call returns its Result at
// once; the Result's KafkaFutures are read with Get(), which throws ExecutionException for a failed one.
//
// Exit codes: 0 success, 1 the client was not created or an operation failed, 2 a mistake on the command
// line.

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;

Example[] examples =
[
    // Topics
    new("CreateTopics", CreateTopics.Arguments, CreateTopics.Run),
    new("ListTopics", ListTopics.Arguments, ListTopics.Run),
    new("DescribeTopics", DescribeTopics.Arguments, DescribeTopics.Run),
    new("DeleteTopics", DeleteTopics.Arguments, DeleteTopics.Run),
    new("CreatePartitions", CreatePartitions.Arguments, CreatePartitions.Run),

    // Records and offsets
    new("DeleteRecords", DeleteRecords.Arguments, DeleteRecords.Run),
    new("ListOffsets", ListOffsets.Arguments, ListOffsets.Run),

    // Cluster and configs
    new("DescribeCluster", DescribeCluster.Arguments, DescribeCluster.Run),
    new("DescribeConfigs", DescribeConfigs.Arguments, DescribeConfigs.Run),
    new("IncrementalAlterConfigs", IncrementalAlterConfigs.Arguments, IncrementalAlterConfigs.Run),
    new("ListConfigResources", ListConfigResources.Arguments, ListConfigResources.Run),
    new("DescribeFeatures", DescribeFeatures.Arguments, DescribeFeatures.Run),
    new("UpdateFeatures", UpdateFeatures.Arguments, UpdateFeatures.Run),
    new("UnregisterController", UnregisterController.Arguments, UnregisterController.Run),

    // Replicas
    new("DescribeLogDirs", DescribeLogDirs.Arguments, DescribeLogDirs.Run),
    new("DescribeReplicaLogDirs", DescribeReplicaLogDirs.Arguments, DescribeReplicaLogDirs.Run),
    new("AlterReplicaLogDirs", AlterReplicaLogDirs.Arguments, AlterReplicaLogDirs.Run),
    new("AlterPartitionReassignments", AlterPartitionReassignments.Arguments, AlterPartitionReassignments.Run),
    new("ListPartitionReassignments", ListPartitionReassignments.Arguments, ListPartitionReassignments.Run),
    new("ElectLeaders", ElectLeaders.Arguments, ElectLeaders.Run),

    // Groups
    new("ListGroups", ListGroups.Arguments, ListGroups.Run),
    new("DescribeConsumerGroups", DescribeConsumerGroups.Arguments, DescribeConsumerGroups.Run),
    new("DescribeClassicGroups", DescribeClassicGroups.Arguments, DescribeClassicGroups.Run),
    new("ListConsumerGroupOffsets", ListConsumerGroupOffsets.Arguments, ListConsumerGroupOffsets.Run),
    new("AlterConsumerGroupOffsets", AlterConsumerGroupOffsets.Arguments, AlterConsumerGroupOffsets.Run),
    new("DeleteConsumerGroupOffsets", DeleteConsumerGroupOffsets.Arguments, DeleteConsumerGroupOffsets.Run),
    new("DeleteConsumerGroups", DeleteConsumerGroups.Arguments, DeleteConsumerGroups.Run),
    new("RemoveMembersFromConsumerGroup", RemoveMembersFromConsumerGroup.Arguments, RemoveMembersFromConsumerGroup.Run),

    // ACLs
    new("CreateAcls", CreateAcls.Arguments, CreateAcls.Run),
    new("DescribeAcls", DescribeAcls.Arguments, DescribeAcls.Run),
    new("DeleteAcls", DeleteAcls.Arguments, DeleteAcls.Run),

    // Quotas and SCRAM
    new("DescribeClientQuotas", DescribeClientQuotas.Arguments, DescribeClientQuotas.Run),
    new("AlterClientQuotas", AlterClientQuotas.Arguments, AlterClientQuotas.Run),
    new("DescribeUserScramCredentials", DescribeUserScramCredentials.Arguments, DescribeUserScramCredentials.Run),
    new("AlterUserScramCredentials", AlterUserScramCredentials.Arguments, AlterUserScramCredentials.Run),

    // Delegation tokens
    new("CreateDelegationToken", CreateDelegationToken.Arguments, CreateDelegationToken.Run),
    new("DescribeDelegationToken", DescribeDelegationToken.Arguments, DescribeDelegationToken.Run),
    new("RenewDelegationToken", RenewDelegationToken.Arguments, RenewDelegationToken.Run),
    new("ExpireDelegationToken", ExpireDelegationToken.Arguments, ExpireDelegationToken.Run),

    // Transactions
    new("ListTransactions", ListTransactions.Arguments, ListTransactions.Run),
    new("DescribeTransactions", DescribeTransactions.Arguments, DescribeTransactions.Run),
    new("DescribeProducers", DescribeProducers.Arguments, DescribeProducers.Run),
    new("AbortTransaction", AbortTransaction.Arguments, AbortTransaction.Run),
    new("FenceProducers", FenceProducers.Arguments, FenceProducers.Run),
    new("ForceTerminateTransaction", ForceTerminateTransaction.Arguments, ForceTerminateTransaction.Run),
];

string general =
    "Usage: AdminApis <bootstrap-brokers> <example> [-X key=value]... [options] <args>\n" +
    "       AdminApis <bootstrap-brokers> <example> --help\n\n" +
    $"Examples:\n  {string.Join("\n  ", examples.Select(e => e.Name))}";

if (args.Length < 2)
{
    if (args.Contains("--help"))
    {
        Console.WriteLine(general);
        return 0;
    }

    Console.Error.WriteLine(general);
    return 2;
}

// The example's name, in any case and with or without separators: CreateTopics, create_topics, create-topics.
string wanted = args[1].Replace("_", "").Replace("-", "");
Example? example = examples.FirstOrDefault(e => e.Name.Equals(wanted, StringComparison.OrdinalIgnoreCase));
if (example is null)
{
    Console.Error.WriteLine($"unknown example \"{args[1]}\"\n\n{general}");
    return 2;
}

string usage = $"Usage: AdminApis <bootstrap-brokers> {example.Name} [-X key=value]... {example.Arguments}";
try
{
    Args parsed = Args.Parse(args[2..]);
    if (parsed.Help)
    {
        Console.WriteLine(usage);
        return 0;
    }

    // Admin configuration: the brokers, then each -X property.
    // The keys are the Java client's: https://kafka.apache.org/documentation/#adminclientconfigs
    Dictionary<string, object?> conf = new() { ["bootstrap.servers"] = args[0] };
    foreach ((string key, string value) in parsed.Config)
    {
        conf[key] = value;
    }

    // Leaving the using block closes the client.
    using (IAdmin admin = Admin.Create(conf))
    {
        example.Run(admin, parsed);
    }
}
catch (UsageException e)
{
    Console.Error.WriteLine($"{e.Message}\n\n{usage}");
    return 2;
}
catch (Exception e)
{
    Console.Error.WriteLine($"Error: {Cli.Describe(e)}");
    return 1;
}

return Cli.AnyFailed ? 1 : 0;

/// <summary>One example: its name, the arguments it takes, and its body.</summary>
internal sealed record Example(string Name, string Arguments, Action<IAdmin, Args> Run);

/// <summary>A mistake on the command line: the example prints the message and its usage, and exits with code 2.</summary>
internal sealed class UsageException(string message) : Exception(message);
