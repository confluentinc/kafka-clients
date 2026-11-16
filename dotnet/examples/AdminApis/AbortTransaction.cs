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

// Aborts a hanging transaction on one partition with IAdmin.AbortTransaction. The partition leader writes an
// abort marker for the producer, so consumers reading committed records can move past the transaction and
// the last stable offset advances. It is a recovery tool for a transaction its coordinator no longer tracks,
// and needs CLUSTER_ACTION permission.
//
// Take the producer id, producer epoch and coordinator epoch from the partition's producer with a current
// transaction start offset in the DescribeProducers example; a negative coordinator epoch is sent as 0, as
// Kafka's kafka-transactions.sh does.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 AbortTransaction orders 0 4001 0 5

using System;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The AbortTransaction example.</summary>
internal static class AbortTransaction
{
    public const string Arguments = "<topic> <partition> <producer_id> <producer_epoch> <coordinator_epoch>";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length != 5)
        {
            throw new UsageException(
                "expected a topic, a partition, a producer id, a producer epoch and a coordinator epoch");
        }

        TopicPartition partition = Cli.ParseTopicPartition(positional);
        long producerId = Cli.Parse<long>("producer id", positional[2]);
        short producerEpoch = Cli.Parse<short>("producer epoch", positional[3]);
        int coordinatorEpoch = Cli.Parse<int>("coordinator epoch", positional[4]);

        AbortTransactionSpec spec = new(partition, producerId, producerEpoch, Math.Max(coordinatorEpoch, 0));
        admin.AbortTransaction(spec).All().Get();
        Console.WriteLine(
            $"Aborted transaction of producer id {producerId}, producer epoch {producerEpoch} on {partition}");
    }
}
