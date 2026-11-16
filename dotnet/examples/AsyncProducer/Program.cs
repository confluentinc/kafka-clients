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

// Example async Kafka producer.
// Reads lines from stdin and sends them to Kafka. Leaving the await using block closes the producer, which
// first waits for every delivery.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

if (args.Length != 2)
{
    Console.Error.WriteLine("Usage: AsyncProducer <bootstrap-brokers> <topic>");
    return 1;
}

string broker = args[0];
string topic = args[1];

// Producer configuration.
// The keys are the Java client's: https://kafka.apache.org/documentation/#producerconfigs
Dictionary<string, object?> conf = new() { ["bootstrap.servers"] = broker };

// The serializers encode keys and values as UTF-8; the records sent here have no key.
await using (AsyncKafkaProducer<string, string> producer = new(conf, new StringSerializer(), new StringSerializer()))
{
    string? line;
    while ((line = await Console.In.ReadLineAsync()) is not null)
    {
        ProducerRecord<string, string> record = new(topic, line.TrimEnd());
        // Awaiting Send() waits for buffer space, and returns the future of the delivery;
        // `await (await producer.Send(record)).Get()` would give its metadata.
        await producer.Send(record, new DeliveryCallback(record));
    }

    Console.Error.WriteLine("% Waiting for outstanding deliveries");
}
// Leaving the block waited for every delivery, then closed the producer.

return 0;

/// <summary>
/// Per-message delivery callback, run on the producer's background completion thread once the message is
/// delivered or has permanently failed (after retries). It holds its record, since the metadata carries no value.
/// The producer handles its next completion once the returned task has ended.
/// </summary>
internal sealed class DeliveryCallback(ProducerRecord<string, string> record) : IAsyncCallback
{
    public async Task OnCompletion(RecordMetadata metadata, Exception? exception)
    {
        if (exception is not null)
        {
            await Console.Error.WriteLineAsync($"% Message failed delivery: {record.Value()}: {exception.Message}");
        }
        else
        {
            await Console.Error.WriteLineAsync(
                $"% Message delivered to {metadata.Topic()} [{metadata.Partition()}] @ {metadata.Offset()}: {record.Value()}");
        }
    }
}
