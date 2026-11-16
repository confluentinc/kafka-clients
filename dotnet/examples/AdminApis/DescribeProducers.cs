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

// Describes the active idempotent and transactional producers of partitions with IAdmin.DescribeProducers.
// The partition leader answers unless --broker names another replica.
//
// A producer with a current transaction start offset has a transaction open on the partition. Its producer
// id, producer epoch and coordinator epoch are what the AbortTransaction example takes to abort a hanging
// transaction; an absent coordinator epoch or start offset prints as -1.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeProducers orders 0 orders 1

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeProducers example.</summary>
internal static class DescribeProducers
{
    public const string Arguments = "[--broker=<id>] (<topic> <partition>)...";

    public static void Run(IAdmin admin, Args args)
    {
        int? brokerId = args.Value("broker") is { } broker ? Cli.Parse<int>("broker id", broker) : null;
        List<TopicPartition> partitions = Cli.ParseTopicPartitions(args.Positional(), required: true);

        DescribeProducersOptions options = new();
        if (brokerId is { } id)
        {
            options.BrokerId(id);
        }

        DescribeProducersResult result = admin.DescribeProducers(partitions, options);
        foreach (TopicPartition partition in partitions)
        {
            try
            {
                DescribeProducersResult.PartitionProducerState state = result.PartitionResult(partition).Get();
                IReadOnlyList<ProducerState> producers = state.ActiveProducers();
                Console.WriteLine($"Partition {partition}: {producers.Count} active producers");
                foreach (ProducerState producer in producers)
                {
                    Console.WriteLine(
                        $"  producer id {producer.ProducerId()}, producer epoch {producer.ProducerEpoch()}, " +
                        $"coordinator epoch {producer.CoordinatorEpoch() ?? -1}, " +
                        $"last sequence {producer.LastSequence()}, last timestamp {producer.LastTimestamp()}, " +
                        $"current transaction start offset " +
                        $"{producer.CurrentTransactionStartOffset() ?? -1}");
                }
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe producers of {partition}", e);
            }
        }
    }
}
