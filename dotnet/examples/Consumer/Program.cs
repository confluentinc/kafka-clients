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

// Example high-level Kafka balanced consumer (KIP-848 group protocol).
// Prints every message it reads, and every change to its assignment.

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: Consumer <bootstrap-brokers> <group> <topic1> <topic2> ..");
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

// Create the consumer. The deserializers decode keys and values as UTF-8.
KafkaConsumer<string, string> consumer = new(conf, new StringDeserializer(), new StringDeserializer());

// Ctrl+C wakes the consumer up: the Poll() in progress, or the next one, throws WakeupException.
// Cancelling the event keeps the process running, so that Close() runs.
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    consumer.Wakeup();
};

// Read messages from Kafka, print to stdout.
try
{
    // Subscribe to the topics. Inside the try, so that a failed Subscribe() still closes the consumer.
    consumer.Subscribe(topics, new PrintRebalance(consumer));

    while (true)
    {
        // Poll() returns a batch of records, possibly empty. A failed Poll() throws a KafkaException.
        ConsumerRecords<string, string> records = consumer.Poll(TimeSpan.FromSeconds(1));
        foreach (ConsumerRecord<string, string> record in records)
        {
            Console.Error.WriteLine(
                $"% {record.Topic()} [{record.Partition()}] at offset {record.Offset()} with key {record.Key() ?? "null"}:");
            Console.WriteLine(record.Value());
        }
    }
}
catch (WakeupException)
{
    Console.Error.WriteLine("% Aborted by user");
}
finally
{
    // Close the consumer: it commits the final offsets and leaves the group.
    consumer.Close();
}

return 0;

/// <summary>
/// Prints each change to the consumer's assignment. The methods run on the thread calling Poll(), and the
/// rebalance waits for them to return.
/// </summary>
internal sealed class PrintRebalance(IConsumer<string, string> consumer) : IConsumerRebalanceListenerWithPartitionsLost
{
    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
        // A listener may call back into its consumer.
        Console.Error.WriteLine(
            $"% Assigned: {PartitionList(partitions)}; assignment now: {PartitionList(consumer.Assignment())}");
    }

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
        Console.Error.WriteLine($"% Revoked: {PartitionList(partitions)}");
    }

    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
    {
        Console.Error.WriteLine($"% Lost: {PartitionList(partitions)}");
    }

    private static string PartitionList(IEnumerable<TopicPartition> partitions)
    {
        string list = string.Join(
            ", ", partitions.OrderBy(p => p.Topic(), StringComparer.Ordinal).ThenBy(p => p.Partition()));
        return list.Length == 0 ? "(none)" : list;
    }
}
