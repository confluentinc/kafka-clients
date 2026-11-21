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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;

using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// A round trip through clients built by <c>Create(configs)</c> alone, against a broker: the configuration holds a
/// function for each serializer and deserializer, which <c>Create</c> calls once and configures with that
/// configuration, on the sync and the async clients. The key travels as UTF-16LE, an encoding both sides read from the
/// configuration, and the value's bytes are consumed as they are, so they show the encoding the value's serializer
/// was configured with.
/// </summary>
[Collection(BrokerCollection.Name)]
public sealed class SerdeConfigCreateBrokerTests
{
    private const string Key = "clé";

    private const string Value = "valeur été";

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_poll = TimeSpan.FromSeconds(0.5);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public SerdeConfigCreateBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task KafkaProducerAndKafkaConsumer_BuiltFromTheirConfigurationAlone_RoundTripARecord()
    {
        string topic = await TopicAsync();
        Calls calls = new Calls();
        Dictionary<string, object?> producerConfigs = ProducerConfigs();
        producerConfigs["key.serializer"] = new Func<ISerializer<string>>(() => calls.Counted(new StringSerializer()));
        producerConfigs["value.serializer"] = new Func<StringSerializer>(() => calls.Counted(new StringSerializer()));
        using (KafkaProducer<string, string> producer = KafkaProducer<string, string>.Create(producerConfigs))
        {
            _ = producer.Send(new ProducerRecord<string, string>(topic, value: Value, key: Key, partition: 0)).Get(s_wait);
        }

        Dictionary<string, object?> consumerConfigs = ConsumerConfigs();
        consumerConfigs["key.deserializer"] = new Func<IDeserializer<string>>(() => calls.Counted(new StringDeserializer()));
        consumerConfigs["value.deserializer"] = new Func<IDeserializer<byte[]>>(() => calls.Counted(new ByteArrayDeserializer()));
        using KafkaConsumer<string, byte[]> consumer = KafkaConsumer<string, byte[]>.Create(consumerConfigs);
        consumer.Assign(new[] { new TopicPartition(topic, 0) });
        ConsumerRecord<string, byte[]> record = PollOne(consumer);

        AssertRoundTrip(calls, record);
    }

    [SkippableFact]
    public async Task AsyncKafkaProducerAndAsyncKafkaConsumer_BuiltFromTheirConfigurationAlone_RoundTripARecord()
    {
        string topic = await TopicAsync();
        Calls calls = new Calls();
        Dictionary<string, object?> producerConfigs = ProducerConfigs();
        producerConfigs["key.serializer"] =
            new Func<IAsyncSerializer<string>>(() => calls.Counted(new AsyncStringSerializer()));
        producerConfigs["value.serializer"] =
            new Func<AsyncStringSerializer>(() => calls.Counted(new AsyncStringSerializer()));
        await using (AsyncKafkaProducer<string, string> producer =
            await AsyncKafkaProducer<string, string>.Create(producerConfigs))
        {
            AsyncKafkaFuture<RecordMetadata> future =
                await producer.Send(new ProducerRecord<string, string>(topic, value: Value, key: Key, partition: 0));
            _ = await future.Get().WaitAsync(s_wait);
        }

        Dictionary<string, object?> consumerConfigs = ConsumerConfigs();
        consumerConfigs["key.deserializer"] =
            new Func<IAsyncDeserializer<string>>(() => calls.Counted(new AsyncStringDeserializer()));
        consumerConfigs["value.deserializer"] =
            new Func<IAsyncDeserializer<byte[]>>(() => calls.Counted(new AsyncByteArrayDeserializer()));
        await using AsyncKafkaConsumer<string, byte[]> consumer =
            await AsyncKafkaConsumer<string, byte[]>.Create(consumerConfigs);
        await consumer.Assign(new[] { new TopicPartition(topic, 0) });
        ConsumerRecord<string, byte[]> record = await PollOneAsync(consumer);

        AssertRoundTrip(calls, record);
    }

    private static void AssertRoundTrip(Calls calls, ConsumerRecord<string, byte[]> record)
    {
        // Each of the four functions was called once.
        Assert.Equal(4, calls.Count);

        // The key's deserializer read the encoding the key's serializer wrote: both were configured as the key's.
        Assert.Equal(Key, record.Key());

        // The value's serializer was configured as the value's: these are not the UTF-8 bytes it writes by default.
        Assert.Equal(Encoding.BigEndianUnicode.GetBytes(Value), record.Value());
    }

    private static ConsumerRecord<string, byte[]> PollOne(KafkaConsumer<string, byte[]> consumer)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<string, byte[]> record in consumer.Poll(s_poll))
            {
                return record;
            }
        }

        throw new InvalidOperationException("the record was not consumed");
    }

    private static async Task<ConsumerRecord<string, byte[]>> PollOneAsync(AsyncKafkaConsumer<string, byte[]> consumer)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<string, byte[]> record in await consumer.Poll(s_poll))
            {
                return record;
            }
        }

        throw new InvalidOperationException("the record was not consumed");
    }

    private async Task<string> TopicAsync()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-serde-config-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }

    private Dictionary<string, object?> ProducerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["key.serializer.encoding"] = "UTF-16LE",
            ["value.serializer.encoding"] = "UTF-16BE",
        };

    private Dictionary<string, object?> ConsumerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["group.protocol"] = "consumer",
            ["group.id"] = "dotnet-serde-config-" + Guid.NewGuid().ToString("N").Substring(0, 12),
            ["auto.offset.reset"] = "earliest",
            ["enable.auto.commit"] = "false",
            ["key.deserializer.encoding"] = "UTF-16LE",
        };

    /// <summary>Counts the calls of the functions the configuration holds.</summary>
    private sealed class Calls
    {
        internal int Count { get; private set; }

        internal T Counted<T>(T serde)
        {
            Count++;
            return serde;
        }
    }
}
