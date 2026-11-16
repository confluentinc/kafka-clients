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

// Describes transactional ids with IAdmin.DescribeTransactions: each id's coordinator, state, producer id and
// epoch, timeout, and the partitions of its current transaction. Each id is asked of its own transaction
// coordinator; an id the coordinator does not know fails with TransactionalIdNotFoundException.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeTransactions my-transactional-id

using System;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The DescribeTransactions example.</summary>
internal static class DescribeTransactions
{
    public const string Arguments = "<transactional_id>...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] transactionalIds = args.Positional();
        if (transactionalIds.Length == 0)
        {
            throw new UsageException("no transactional id given");
        }

        DescribeTransactionsResult result = admin.DescribeTransactions(transactionalIds);
        foreach (string transactionalId in transactionalIds)
        {
            try
            {
                // Null only when the client delivers no description.
                if (result.Description(transactionalId).Get() is not { } description)
                {
                    Console.WriteLine($"Transaction {transactionalId}: no description");
                    continue;
                }

                Console.WriteLine(
                    $"Transaction {transactionalId}: " +
                    $"state {TransactionStateMethods.ToString(description.State())}, " +
                    $"coordinator {description.CoordinatorId()}, producer id {description.ProducerId()}, " +
                    $"producer epoch {description.ProducerEpoch()}, " +
                    $"timeout {description.TransactionTimeoutMs()} ms, " +
                    $"start time {Cli.OrNone(description.TransactionStartTimeMs())}");
                foreach (TopicPartition partition in Cli.Sorted(description.TopicPartitions()))
                {
                    Console.WriteLine($"  partition {partition}");
                }
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe transaction {transactionalId}", e);
            }
        }
    }
}
