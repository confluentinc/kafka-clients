// Copyright 2025 Confluent Inc.
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
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Performance.Tests;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// The producer's send against a broker: a record taken on the calling thread once its topic's metadata is
/// cached, and a record taken at once while another waits for a partition the topic does not have yet.
/// </summary>
public sealed class KafkaProducerBrokerTests : IClassFixture<KafkaBrokerFixture>
{
    private static readonly TimeSpan s_get = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_partitionWait = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_readBackWindow = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan s_readBackPoll = TimeSpan.FromSeconds(0.5);
    private static readonly TimeSpan s_firstSendStarts = TimeSpan.FromSeconds(1);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public KafkaProducerBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task AsyncSend_WithCachedMetadata_CompletesTheFirstStageInsideTheCall()
    {
        string topic = await TopicAsync();
        await using AsyncKafkaProducer<byte[], byte[]> producer =
            new AsyncKafkaProducer<byte[], byte[]>(Configs(), Serdes.ByteArray, Serdes.ByteArray);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(Record(topic, 0, "a"));
        Assert.False(firstStage.IsCompleted, "the first record was taken inside the call");
        AsyncKafkaFuture<RecordMetadata> first = await firstStage;
        Assert.Equal(0L, (await first.Get(s_get)).Offset);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> secondStage = producer.Send(Record(topic, 0, "b"));
        Assert.True(secondStage.IsCompletedSuccessfully, "the second record was not taken inside the call");
        AsyncKafkaFuture<RecordMetadata> second = await secondStage;
        Assert.Equal(1L, (await second.Get(s_get)).Offset);
    }

    [SkippableFact]
    public async Task ARecordTakenAtOnce_IsNotHeldBehindOneWaitingOnAnotherThread()
    {
        string topic = await TopicAsync();
        using KafkaProducer<byte[], byte[]> producer =
            new KafkaProducer<byte[], byte[]>(Configs(maxBlockMs: 60000), Serdes.ByteArray, Serdes.ByteArray);

        KafkaFuture<RecordMetadata> warmUp = producer.Send(Record(topic, 0, "warm-up"));
        Assert.Equal(0L, warmUp.Get(s_get).Offset);

        KafkaFuture<RecordMetadata>? first = null;
        Exception? firstFailure = null;
        Thread waiting = new Thread(() =>
        {
            try
            {
                first = producer.Send(Record(topic, 1, "a"));
            }
            catch (Exception e)
            {
                firstFailure = e;
            }
        });
        waiting.Start();

        // The blocking Send gives no signal that its record is queued, so the first send is given time to start.
        await Task.Delay(s_firstSendStarts);
        KafkaFuture<RecordMetadata> second = producer.Send(Record(topic, 0, "b"));
        Assert.Equal(1L, second.Get(s_get).Offset);
        Assert.Equal(new (long, string?)[] { (1L, "b") }, RecordsOf(topic, 0, fromOffset: 1));
        Assert.True(waiting.IsAlive, "the first Send no longer waits for its partition");

        await _broker.AddPartitionsAsync(topic, 2);
        Assert.True(waiting.Join(s_partitionWait), "the first Send still waits after the partition appeared");
        Assert.Null(firstFailure);
        Assert.Equal(1, Assert.NotNull(first).Get(s_partitionWait).Partition);
    }

    [SkippableFact]
    public async Task AsyncSend_ARecordTakenAtOnce_IsNotHeldBehindOneWaitingOnAnotherThread()
    {
        string topic = await TopicAsync();
        await using AsyncKafkaProducer<byte[], byte[]> producer =
            new AsyncKafkaProducer<byte[], byte[]>(Configs(maxBlockMs: 60000), Serdes.ByteArray, Serdes.ByteArray);

        AsyncKafkaFuture<RecordMetadata> warmUp = await producer.Send(Record(topic, 0, "warm-up"));
        Assert.Equal(0L, (await warmUp.Get(s_get)).Offset);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(Record(topic, 1, "a"));
        Assert.False(firstStage.IsCompleted, "the first record was taken inside the call");
        ValueTask<AsyncKafkaFuture<RecordMetadata>> secondStage = producer.Send(Record(topic, 0, "b"));
        Assert.True(secondStage.IsCompletedSuccessfully, "the second record was held behind the first");
        AsyncKafkaFuture<RecordMetadata> second = await secondStage;
        Assert.Equal(1L, (await second.Get(s_get)).Offset);
        Assert.Equal(new (long, string?)[] { (1L, "b") }, RecordsOf(topic, 0, fromOffset: 1));
        Assert.False(firstStage.IsCompleted, "the first record no longer waits for its partition");

        await _broker.AddPartitionsAsync(topic, 2);
        AsyncKafkaFuture<RecordMetadata> first = await firstStage.AsTask().WaitAsync(s_partitionWait);
        Assert.Equal(1, (await first.Get(s_partitionWait)).Partition);
    }

    private static ProducerRecord<byte[], byte[]> Record(string topic, int partition, string value) =>
        new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes(value), key: null, partition: partition);

    private async Task<string> TopicAsync()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-producer-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }

    private Dictionary<string, string> Configs(int? maxBlockMs = null)
    {
        Dictionary<string, string> configs = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
        };
        if (maxBlockMs is int ms)
        {
            configs["max.block.ms"] = ms.ToString(CultureInfo.InvariantCulture);
        }

        return configs;
    }

    private List<(long Offset, string? Value)> RecordsOf(string topic, int partition, long fromOffset)
    {
        Dictionary<string, string> configs = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["group.protocol"] = "consumer",
            ["group.id"] = "dotnet-records-" + Guid.NewGuid().ToString("N").Substring(0, 12),
            ["enable.auto.commit"] = "false",
        };
        using KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<byte[], byte[]>(configs, Serdes.ByteArray, Serdes.ByteArray);
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.Assign(new[] { tp });
        consumer.Seek(tp, fromOffset);
        List<(long Offset, string? Value)> records = new List<(long Offset, string? Value)>();
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_readBackWindow)
        {
            foreach (ConsumerRecord<byte[], byte[]> record in consumer.Poll(s_readBackPoll))
            {
                records.Add((record.Offset, record.Value is null ? null : Encoding.UTF8.GetString(record.Value)));
            }
        }

        return records;
    }
}
