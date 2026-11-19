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

// Example Kafka producer.
// Reads lines from stdin and sends them to Kafka.

using System;
using System.Collections.Generic;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

if (args.Length != 2)
{
    Console.Error.WriteLine("Usage: Producer <bootstrap-brokers> <topic>");
    return 1;
}

string broker = args[0];
string topic = args[1];

// Producer configuration.
// The keys are the Java client's: https://kafka.apache.org/documentation/#producerconfigs
Dictionary<string, object?> conf = new() { ["bootstrap.servers"] = broker };

// Create the producer. The serializers encode keys and values as UTF-8; the records sent here have no key.
KafkaProducer<string, string> producer = KafkaProducer<string, string>.Create(conf, new StringSerializer(), new StringSerializer());

try
{
    // Read lines from stdin and send each one (without its newline).
    // Send() does not throw when the local buffer is full: it waits for space, up to max.block.ms.
    string? line;
    while ((line = Console.In.ReadLine()) is not null)
    {
        ProducerRecord<string, string> record = new(topic, line.TrimEnd());
        producer.Send(record, new DeliveryCallback(record));
    }

    // Wait until every message has been delivered or has failed.
    Console.Error.WriteLine("% Waiting for outstanding deliveries");
    producer.Flush();
}
finally
{
    producer.Close();
}

return 0;

/// <summary>
/// Per-message delivery callback, run on the producer's background completion thread once the message is
/// delivered or has permanently failed (after retries). It holds its record, since the metadata carries no value.
/// </summary>
internal sealed class DeliveryCallback(ProducerRecord<string, string> record) : ICallback
{
    public void OnCompletion(RecordMetadata metadata, Exception? exception)
    {
        if (exception is not null)
        {
            Console.Error.WriteLine($"% Message failed delivery: {record.Value()}: {exception.Message}");
        }
        else
        {
            Console.Error.WriteLine(
                $"% Message delivered to {metadata.Topic()} [{metadata.Partition()}] @ {metadata.Offset()}: {record.Value()}");
        }
    }
}
