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

// Describes the cluster with IAdmin.DescribeCluster: its id, the controller and the brokers. With
// --authorized-operations the broker also returns the operations the caller may perform on the cluster,
// and with --fenced the brokers list includes the fenced ones.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeCluster --authorized-operations

using System;
using System.Linq;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeCluster example.</summary>
internal static class DescribeCluster
{
    public const string Arguments = "[--authorized-operations] [--fenced]";

    public static void Run(IAdmin admin, Args args)
    {
        bool includeAuthorizedOperations = args.Flag("authorized-operations");
        bool includeFencedBrokers = args.Flag("fenced");
        if (args.Positional().Length > 0)
        {
            throw new UsageException("unexpected argument");
        }

        DescribeClusterResult result = admin.DescribeCluster(
            new DescribeClusterOptions()
                .IncludeAuthorizedOperations(includeAuthorizedOperations)
                .IncludeFencedBrokers(includeFencedBrokers));

        Console.WriteLine($"Cluster id: {result.ClusterId().Get()}");
        Console.WriteLine(
            result.Controller().Get() is { } controller ? $"Controller: {NodeText(controller)}" : "Controller: none");
        foreach (Node broker in result.Nodes().Get().OrderBy(node => node.Id()))
        {
            Console.WriteLine($"Broker {NodeText(broker)}");
        }

        // Absent unless the operations were asked for.
        if (result.AuthorizedOperations().Get() is { } operations)
        {
            Console.WriteLine($"Authorized operations: {string.Join(", ", operations.Order())}");
        }
    }

    private static string NodeText(Node node)
    {
        string rack = node.Rack() is { } name ? $", rack {name}" : "";
        return $"{node.Id()} at {node.Host()}:{node.Port()}{rack}{(node.IsFenced() ? ", fenced" : "")}";
    }
}
