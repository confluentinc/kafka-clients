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
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;

using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// The headers forms against a broker: the key's and the value's serializer's headers forms each add a header to the
/// record's own headers, which the broker receives, and a real consumer's key and value deserializers read them from
/// the headers the record then carries, on the sync and the async clients; a deserializer that throws gives the
/// <see cref="RecordDeserializationException"/> the headers it was given.
/// </summary>
[Collection(BrokerCollection.Name)]
public sealed class SerdeHeadersBrokerTests
{
    private const string SchemaId = "schema-id";

    private const string KeyId = "key-id";

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_poll = TimeSpan.FromSeconds(0.5);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public SerdeHeadersBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task KafkaProducerAndKafkaConsumer_CallTheHeadersForms_WithTheRecordsHeaders()
    {
        string topic = await TopicAsync();
        HeaderAddingSerializer keySerializer = new HeaderAddingSerializer(KeyId);
        HeaderAddingSerializer serializer = new HeaderAddingSerializer();
        using (KafkaProducer<string, string> producer =
            new KafkaProducer<string, string>(ProducerConfigs(), keySerializer, serializer))
        {
            _ = producer.Send(new ProducerRecord<string, string>(topic, "v", "k", partition: 0)).Get(s_wait);
        }

        HeaderReadingDeserializer keyDeserializer = new HeaderReadingDeserializer(KeyId);
        HeaderReadingDeserializer deserializer = new HeaderReadingDeserializer();
        using KafkaConsumer<string, string> consumer =
            new KafkaConsumer<string, string>(ConsumerConfigs(), keyDeserializer, deserializer);
        consumer.Assign(new[] { new TopicPartition(topic, 0) });
        ConsumerRecord<string, string> record = PollOne(() => consumer.Poll(s_poll));

        AssertRoundTrip(keySerializer, serializer, keyDeserializer, deserializer, record);
    }

    [SkippableFact]
    public async Task AsyncKafkaProducerAndAsyncKafkaConsumer_CallTheHeadersForms_WithTheRecordsHeaders()
    {
        string topic = await TopicAsync();
        HeaderAddingSerializer keySerializer = new HeaderAddingSerializer(KeyId);
        HeaderAddingSerializer serializer = new HeaderAddingSerializer();
        await using (AsyncKafkaProducer<string, string> producer =
            new AsyncKafkaProducer<string, string>(ProducerConfigs(), keySerializer, serializer))
        {
            AsyncKafkaFuture<RecordMetadata> future =
                await producer.Send(new ProducerRecord<string, string>(topic, "v", "k", partition: 0));
            _ = await future.Get().WaitAsync(s_wait);
        }

        HeaderReadingDeserializer keyDeserializer = new HeaderReadingDeserializer(KeyId);
        HeaderReadingDeserializer deserializer = new HeaderReadingDeserializer();
        await using AsyncKafkaConsumer<string, string> consumer =
            new AsyncKafkaConsumer<string, string>(ConsumerConfigs(), keyDeserializer, deserializer);
        await consumer.Assign(new[] { new TopicPartition(topic, 0) });
        ConsumerRecord<string, string> record = await PollOneAsync(() => consumer.Poll(s_poll));

        AssertRoundTrip(keySerializer, serializer, keyDeserializer, deserializer, record);
    }

    [SkippableFact]
    public async Task KafkaConsumer_AHeadersFormThatThrows_GivesTheExceptionTheHeadersItWasGiven()
    {
        string topic = await ProducedAsync();
        HeaderReadingDeserializer deserializer = new HeaderReadingDeserializer(fail: true);
        using KafkaConsumer<string, string> consumer =
            new KafkaConsumer<string, string>(ConsumerConfigs(), Serdes.String().Deserializer(), deserializer);
        consumer.Assign(new[] { new TopicPartition(topic, 0) });

        RecordDeserializationException e = PollFailure(() => consumer.Poll(s_poll));

        AssertTheFailureCarriesTheHeaders(deserializer, e);
    }

    [SkippableFact]
    public async Task AsyncKafkaConsumer_AHeadersFormThatThrows_GivesTheExceptionTheHeadersItWasGiven()
    {
        string topic = await ProducedAsync();
        HeaderReadingDeserializer deserializer = new HeaderReadingDeserializer(fail: true);
        await using AsyncKafkaConsumer<string, string> consumer =
            new AsyncKafkaConsumer<string, string>(ConsumerConfigs(), Serdes.String().Deserializer(), deserializer);
        await consumer.Assign(new[] { new TopicPartition(topic, 0) });

        RecordDeserializationException e = await PollFailureAsync(() => consumer.Poll(s_poll));

        AssertTheFailureCarriesTheHeaders(deserializer, e);
    }

    private static void AssertRoundTrip(
        HeaderAddingSerializer keySerializer,
        HeaderAddingSerializer serializer,
        HeaderReadingDeserializer keyDeserializer,
        HeaderReadingDeserializer deserializer,
        ConsumerRecord<string, string> record)
    {
        Assert.Same(Assert.Single(keySerializer.Seen), Assert.Single(serializer.Seen));
        Assert.Equal("k|1", record.Key());
        Assert.Equal("v|1", record.Value());
        Assert.Same(record.Headers(), Assert.Single(keyDeserializer.Seen));
        Assert.Same(record.Headers(), Assert.Single(deserializer.Seen));
        Assert.Equal(Encoding.UTF8.GetBytes("1"), record.Headers().LastHeader(KeyId)?.Value());
        Assert.Equal(Encoding.UTF8.GetBytes("1"), record.Headers().LastHeader(SchemaId)?.Value());
    }

    private static void AssertTheFailureCarriesTheHeaders(HeaderReadingDeserializer deserializer, RecordDeserializationException e)
    {
        Assert.Equal(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, e.Origin());
        Headers seen = Assert.Single(deserializer.Seen);
        Assert.Same(seen, e.Headers());
        Assert.Equal(Encoding.UTF8.GetBytes("1"), seen.LastHeader(SchemaId)?.Value());
        Assert.Equal("bad v", e.InnerException?.Message);
    }

    private static ConsumerRecord<string, string> PollOne(Func<ConsumerRecords<string, string>> poll)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<string, string> record in poll())
            {
                return record;
            }
        }

        throw new InvalidOperationException("the record was not consumed");
    }

    private static async Task<ConsumerRecord<string, string>> PollOneAsync(Func<Task<ConsumerRecords<string, string>>> poll)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<string, string> record in await poll())
            {
                return record;
            }
        }

        throw new InvalidOperationException("the record was not consumed");
    }

    private static RecordDeserializationException PollFailure(Func<ConsumerRecords<string, string>> poll)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_wait)
        {
            try
            {
                Assert.Empty(poll());
            }
            catch (RecordDeserializationException e)
            {
                return e;
            }
        }

        throw new InvalidOperationException("the record was not consumed");
    }

    private static async Task<RecordDeserializationException> PollFailureAsync(Func<Task<ConsumerRecords<string, string>>> poll)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_wait)
        {
            try
            {
                Assert.Empty(await poll());
            }
            catch (RecordDeserializationException e)
            {
                return e;
            }
        }

        throw new InvalidOperationException("the record was not consumed");
    }

    private async Task<string> ProducedAsync()
    {
        string topic = await TopicAsync();
        using KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            ProducerConfigs(), Serdes.String().Serializer(), new HeaderAddingSerializer());
        _ = producer.Send(new ProducerRecord<string, string>(topic, "v", "k", partition: 0)).Get(s_wait);
        return topic;
    }

    private async Task<string> TopicAsync()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-serde-headers-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }

    private Dictionary<string, object?> ProducerConfigs() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = _broker.ExternalBootstrap! };

    private Dictionary<string, object?> ConsumerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["group.protocol"] = "consumer",
            ["group.id"] = "dotnet-serde-headers-" + Guid.NewGuid().ToString("N").Substring(0, 12),
            ["auto.offset.reset"] = "earliest",
            ["enable.auto.commit"] = "false",
        };

    /// <summary>
    /// A string serializer whose headers form adds a header (the schema id unless another is named) and records the
    /// headers it was given.
    /// </summary>
    private sealed class HeaderAddingSerializer : ISerializerWithHeaders<string>
    {
        private readonly string _header;

        internal HeaderAddingSerializer(string header = SchemaId) => _header = header;

        internal List<Headers> Seen { get; } = new List<Headers>();

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) =>
            throw new InvalidOperationException("the producer called the form without headers");

        public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, string? data)
        {
            Seen.Add(headers);
            headers.Add(_header, Encoding.UTF8.GetBytes("1"));
            return data is null ? (ReadOnlyMemory<byte>?)null : Encoding.UTF8.GetBytes(data);
        }
    }

    /// <summary>
    /// A string deserializer whose headers form appends the value of a header (the schema id unless another is named)
    /// and records the headers it was given, or throws after recording them.
    /// </summary>
    private sealed class HeaderReadingDeserializer : IDeserializerWithHeaders<string>
    {
        private readonly string _header;

        private readonly bool _fail;

        internal HeaderReadingDeserializer(string header = SchemaId, bool fail = false)
        {
            _header = header;
            _fail = fail;
        }

        internal List<Headers> Seen { get; } = new List<Headers>();

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) =>
            throw new InvalidOperationException("the consumer called the form without headers");

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data)
        {
            Seen.Add(headers);
            string text = Encoding.UTF8.GetString(data.ToArray());
            if (_fail)
            {
                throw new FormatException("bad " + text);
            }

            byte[]? id = headers.LastHeader(_header)?.Value();
            return text + "|" + (id is null ? "none" : Encoding.UTF8.GetString(id));
        }
    }
}
