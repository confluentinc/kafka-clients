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
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;
using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// A client's <c>Metrics()</c> against a broker: a map taken before the first record is a live view that gains the
/// topic's metrics once the client produces or fetches, and a metric kept from it counts the records, as Java's
/// <c>metrics()</c> does.
/// </summary>
[Collection(BrokerCollection.Name)]
public sealed class MetricsBrokerTests
{
    private const int RecordCount = 3;

    private static readonly TimeSpan s_get = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_consumeWait = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_poll = TimeSpan.FromSeconds(0.5);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public MetricsBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task Producer_AMapTakenBeforeTheFirstSend_GainsTheTopicsMetrics_AndAKeptCounterReadsTheRecordsSent()
    {
        string topic = await TopicAsync();
        using KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            new Dictionary<string, object?> { ["bootstrap.servers"] = _broker.ExternalBootstrap! },
            Serdes.ByteArray().Serializer(),
            Serdes.ByteArray().Serializer());
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        KafkaMetric sent = Find(metrics, "producer-metrics", "record-send-total", topic: null);
        Assert.Equal(0.0, sent.MetricValue());
        Assert.Null(FindOrNull(metrics, "producer-topic-metrics", "record-send-total", topic));

        for (int i = 0; i < RecordCount; i++)
        {
            producer.Send(new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v" + i))).Get(s_get);
        }

        Assert.Equal((double)RecordCount, sent.MetricValue());
        Assert.Equal((double)RecordCount, Find(metrics, "producer-topic-metrics", "record-send-total", topic).MetricValue());
    }

    [SkippableFact]
    public async Task Consumer_AMapTakenBeforeTheFirstPoll_GainsTheTopicsMetrics_AndAKeptCounterReadsTheRecordsConsumed()
    {
        string topic = await TopicAsync();
        using (KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            new Dictionary<string, object?> { ["bootstrap.servers"] = _broker.ExternalBootstrap! },
            Serdes.ByteArray().Serializer(),
            Serdes.ByteArray().Serializer()))
        {
            for (int i = 0; i < RecordCount; i++)
            {
                producer.Send(new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v" + i))).Get(s_get);
            }
        }

        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = _broker.ExternalBootstrap!,
                ["group.protocol"] = "consumer",
                ["group.id"] = "dotnet-metrics-" + Guid.NewGuid().ToString("N").Substring(0, 12),
                ["enable.auto.commit"] = "false",
            },
            new AsyncByteArrayDeserializer(),
            new AsyncByteArrayDeserializer());
        await using IAsyncDisposable consumerDisposal = consumer;
        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        KafkaMetric consumed = Find(metrics, "consumer-fetch-manager-metrics", "records-consumed-total", topic: null);
        Assert.Equal(0.0, consumed.MetricValue());
        Assert.Null(FindOrNull(metrics, "consumer-fetch-manager-metrics", "records-consumed-total", topic));

        TopicPartition partition = new TopicPartition(topic, 0);
        await consumer.Assign(new[] { partition });
        await consumer.Seek(partition, 0L);
        int received = 0;
        Stopwatch elapsed = Stopwatch.StartNew();
        while (received < RecordCount && elapsed.Elapsed < s_consumeWait)
        {
            received += (await consumer.Poll(s_poll)).Count();
        }

        Assert.Equal(RecordCount, received);
        Assert.Equal((double)RecordCount, consumed.MetricValue());
        Assert.Equal(
            (double)RecordCount,
            Find(metrics, "consumer-fetch-manager-metrics", "records-consumed-total", topic).MetricValue());
    }

    private static KafkaMetric Find(IReadOnlyDictionary<MetricName, IMetric> metrics, string group, string name, string? topic) =>
        FindOrNull(metrics, group, name, topic)
            ?? throw new InvalidOperationException($"no metric {group}:{name} for topic {topic ?? "(none)"}");

    // The client-level metric has no topic tag; the topic's own has the topic's.
    private static KafkaMetric? FindOrNull(
        IReadOnlyDictionary<MetricName, IMetric> metrics, string group, string name, string? topic)
    {
        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName key = entry.Key;
            bool hasTopic = key.Tags().TryGetValue("topic", out string? tagged);
            if (key.Group() == group && key.Name() == name && (topic is null ? !hasTopic : tagged == topic))
            {
                return Assert.IsType<KafkaMetric>(entry.Value);
            }
        }

        return null;
    }

    private async Task<string> TopicAsync()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-metrics-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }
}
