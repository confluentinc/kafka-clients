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

// Example async high-level Kafka balanced consumer (KIP-848 group protocol).
// Prints every message it reads, and every change to its assignment. Leaving the await using block closes
// the consumer.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: AsyncConsumer <bootstrap-brokers> <group> <topic1> <topic2> ..");
    return 1;
}

string broker = args[0];
string group = args[1];
string[] topics = args[2..];

// Consumer configuration.
// The keys are the Java client's: https://kafka.apache.org/documentation/#consumerconfigs
Dictionary<string, object?> conf = new()
{
    ["bootstrap.servers"] = broker,
    ["group.id"] = group,
    // This client speaks only the KIP-848 consumer group protocol.
    ["group.protocol"] = "consumer",
    ["auto.offset.reset"] = "earliest",
    // With auto-commit (the default), the consumer commits the offsets of the records Poll() returned every
    // auto.commit.interval.ms (5 s), before a rebalance takes partitions away, and on Close(). Processing
    // each batch before the next Poll() gives at-least-once delivery.
};

// Ctrl+C cancels the token given to Poll(): the poll ends, and its task throws OperationCanceledException.
// Cancelling the event keeps the process running, so that the consumer is closed.
CancellationTokenSource cancellation = new();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cancellation.Cancel();
};

// The deserializers decode keys and values as UTF-8.
await using (AsyncKafkaConsumer<string, string> consumer = await AsyncKafkaConsumer<string, string>.Create(conf, new AsyncStringDeserializer(), new AsyncStringDeserializer()))
{
    await consumer.Subscribe(topics, new PrintRebalance(consumer));

    // Read messages from Kafka, print to stdout.
    try
    {
        while (true)
        {
            // Poll() returns a batch of records, possibly empty. A failed Poll() throws a KafkaException.
            ConsumerRecords<string, string> records = await consumer.Poll(TimeSpan.FromSeconds(1), cancellation.Token);
            foreach (ConsumerRecord<string, string> record in records)
            {
                Console.Error.WriteLine(
                    $"% {record.Topic()} [{record.Partition()}] at offset {record.Offset()} with key {record.Key() ?? "null"}:");
                Console.WriteLine(record.Value());
            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.Error.WriteLine("% Aborted by user");
    }
}
// Leaving the block closed the consumer: it committed the final offsets and left the group.

return 0;

/// <summary>
/// Prints each change to the consumer's assignment. On the async consumer the methods return tasks: they run on
/// the consumer's own thread while Poll() runs, and the rebalance waits for their tasks to end.
/// </summary>
internal sealed class PrintRebalance(IAsyncConsumer<string, string> consumer)
    : IAsyncConsumerRebalanceListenerWithPartitionsLost
{
    public async Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
        // A listener may call back into its consumer.
        await Console.Error.WriteLineAsync(
            $"% Assigned: {PartitionList(partitions)}; assignment now: {PartitionList(consumer.Assignment())}");
    }

    public async Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
        await Console.Error.WriteLineAsync($"% Revoked: {PartitionList(partitions)}");
    }

    public async Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
    {
        await Console.Error.WriteLineAsync($"% Lost: {PartitionList(partitions)}");
    }

    private static string PartitionList(IEnumerable<TopicPartition> partitions)
    {
        string list = string.Join(
            ", ", partitions.OrderBy(p => p.Topic(), StringComparer.Ordinal).ThenBy(p => p.Partition()));
        return list.Length == 0 ? "(none)" : list;
    }
}
